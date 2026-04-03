package org.taotree;

import org.taotree.internal.NodePtr;

import org.taotree.internal.Node4;
import org.taotree.internal.Node16;
import org.taotree.internal.Node48;
import org.taotree.internal.Node256;
import org.taotree.internal.NodeConstants;
import org.taotree.internal.BumpAllocator;
import org.taotree.internal.PrefixNode;
import org.taotree.internal.SlabAllocator;

import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * Off-heap key-value tree backed by an Adaptive Radix Tree.
 *
 * <p>Owns the memory lifecycle, off-heap slab and bump allocators, and a fair
 * {@link ReentrantReadWriteLock} that protects all shared state. Operates on
 * fixed-length, binary-comparable keys with zero GC pressure.
 *
 * <p>{@link TaoDictionary Dictionaries} created from a tree share its slab allocator
 * and lock, ensuring consistent concurrency control.
 *
 * <h3>Usage:</h3>
 * <pre>{@code
 * try (var tree = TaoTree.open(16, 24)) {
 *     var dict = TaoDictionary.u16(tree);
 *
 *     try (var w = tree.write()) {
 *         long leaf = w.getOrCreate(key, 0);
 *         w.leafValue(leaf).set(ValueLayout.JAVA_LONG, 0, 42L);
 *     }
 *
 *     try (var r = tree.read()) {
 *         long leaf = r.lookup(key);
 *         long value = r.leafValue(leaf).get(ValueLayout.JAVA_LONG, 0);
 *     }
 * }
 * }</pre>
 *
 * <h3>Threading model:</h3>
 * <p>Safe for one writer + many readers across threads, protected by a fair RW lock.
 * Use {@link #read()} / {@link #write()} for scoped access.
 *
 * <p><b>Important:</b> Leaf references ({@code long} values) and {@link MemorySegment}
 * references returned by a scope are valid only while that scope is open. Do not retain
 * them after {@link ReadScope#close()} or {@link WriteScope#close()}.
 */
public final class TaoTree implements AutoCloseable {

    /**
     * Sentinel value returned by {@link ReadScope#lookup} when a key is not found.
     * Equivalent to {@code 0L}.
     */
    public static final long NOT_FOUND = 0L;

    // -- Shared infrastructure --
    private final Arena arena;
    private final SlabAllocator slab;
    private final BumpAllocator bump;
    private final ReentrantReadWriteLock lock;
    private final boolean ownsArena;

    /** Package-private: provides overflow storage for {@link TaoString}. */
    BumpAllocator bump() { return bump; }

    // Node slab class IDs (registered once at construction)
    final int prefixClassId;
    final int node4ClassId;
    final int node16ClassId;
    final int node48ClassId;
    final int node256ClassId;

    // -- Tree state --
    private final int keyLen;
    private final int keySlotSize;      // keyLen rounded up to 8-byte alignment
    private final int[] leafClassIds;   // slab class IDs for each user leaf class
    private final int[] leafValueSizes; // value size per leaf class
    private final int leafClassCount;

    private long root = NodePtr.EMPTY_PTR;
    long size = 0; // package-private for TaoDictionary

    // -----------------------------------------------------------------------
    // Root tree constructor (owns arena)
    // -----------------------------------------------------------------------

    private TaoTree(int slabSize, int keyLen, int[] leafValueSizes) {
        this.arena = Arena.ofShared();
        this.slab = new SlabAllocator(arena, slabSize);
        this.bump = new BumpAllocator(arena);
        this.lock = new ReentrantReadWriteLock(true); // fair
        this.ownsArena = true;

        // Register node slab classes
        this.prefixClassId  = slab.registerClass(NodeConstants.PREFIX_SIZE);
        this.node4ClassId   = slab.registerClass(NodeConstants.NODE4_SIZE);
        this.node16ClassId  = slab.registerClass(NodeConstants.NODE16_SIZE);
        this.node48ClassId  = slab.registerClass(NodeConstants.NODE48_SIZE);
        this.node256ClassId = slab.registerClass(NodeConstants.NODE256_SIZE);

        // Tree params
        this.keyLen = keyLen;
        this.keySlotSize = keyLen > 0 ? (keyLen + 7) & ~7 : 0;
        this.leafValueSizes = leafValueSizes != null ? leafValueSizes.clone() : new int[0];
        this.leafClassCount = this.leafValueSizes.length;
        this.leafClassIds = new int[leafClassCount];
        for (int i = 0; i < leafClassCount; i++) {
            leafClassIds[i] = slab.registerClass(keySlotSize + this.leafValueSizes[i]);
        }
    }

    // -----------------------------------------------------------------------
    // Child tree constructor (shares parent's infrastructure)
    // -----------------------------------------------------------------------

    /**
     * Package-private: creates a child tree that shares the parent's arena,
     * slab allocator, bump allocator, lock, and node class IDs.
     * Used internally by {@link TaoDictionary}.
     */
    TaoTree(TaoTree parent, int keyLen, int[] leafValueSizes) {
        if (keyLen <= 0) throw new IllegalArgumentException("keyLen must be positive: " + keyLen);
        if (leafValueSizes == null || leafValueSizes.length == 0) {
            throw new IllegalArgumentException("At least one leaf value size required");
        }

        // Share parent's infrastructure
        this.arena = parent.arena;
        this.slab = parent.slab;
        this.bump = parent.bump;
        this.lock = parent.lock;
        this.ownsArena = false;

        // Copy node class IDs
        this.prefixClassId  = parent.prefixClassId;
        this.node4ClassId   = parent.node4ClassId;
        this.node16ClassId  = parent.node16ClassId;
        this.node48ClassId  = parent.node48ClassId;
        this.node256ClassId = parent.node256ClassId;

        // Tree params
        this.keyLen = keyLen;
        this.keySlotSize = (keyLen + 7) & ~7;
        this.leafValueSizes = leafValueSizes.clone();
        this.leafClassCount = leafValueSizes.length;
        this.leafClassIds = new int[leafClassCount];

        // Registration mutates shared slab state — take write lock
        lock.writeLock().lock();
        try {
            for (int i = 0; i < leafClassCount; i++) {
                leafClassIds[i] = slab.registerClass(keySlotSize + this.leafValueSizes[i]);
            }
        } finally {
            lock.writeLock().unlock();
        }
    }

    // -----------------------------------------------------------------------
    // Static factory methods
    // -----------------------------------------------------------------------

    /**
     * Create an infrastructure-only tree (no primary data tree).
     *
     * <p>Use this when you only need dictionaries and don't have a primary data tree.
     * Dictionaries created from this tree share its slab allocator and lock.
     *
     * @param slabSize slab size in bytes
     */
    public static TaoTree forDictionaries(int slabSize) {
        return new TaoTree(slabSize, 0, null);
    }

    /** Create an infrastructure-only tree with the default slab size (1 MB). */
    public static TaoTree forDictionaries() {
        return new TaoTree(SlabAllocator.DEFAULT_SLAB_SIZE, 0, null);
    }

    /**
     * Create a data tree with a single leaf class.
     *
     * @param keyLen    fixed key length in bytes (must be positive)
     * @param valueSize size of the leaf value region in bytes
     */
    public static TaoTree open(int keyLen, int valueSize) {
        if (keyLen <= 0) throw new IllegalArgumentException("keyLen must be positive: " + keyLen);
        return new TaoTree(SlabAllocator.DEFAULT_SLAB_SIZE, keyLen, new int[]{valueSize});
    }

    /**
     * Create a data tree with a single leaf class and custom slab size.
     *
     * @param keyLen    fixed key length in bytes (must be positive)
     * @param valueSize size of the leaf value region in bytes
     * @param slabSize  slab size in bytes
     */
    public static TaoTree open(int keyLen, int valueSize, int slabSize) {
        if (keyLen <= 0) throw new IllegalArgumentException("keyLen must be positive: " + keyLen);
        return new TaoTree(slabSize, keyLen, new int[]{valueSize});
    }

    /**
     * Create a data tree with multiple leaf classes.
     *
     * @param keyLen         fixed key length in bytes (must be positive)
     * @param leafValueSizes value sizes for each leaf class (index = leaf class ID)
     */
    public static TaoTree open(int keyLen, int[] leafValueSizes) {
        if (keyLen <= 0) throw new IllegalArgumentException("keyLen must be positive: " + keyLen);
        if (leafValueSizes == null || leafValueSizes.length == 0) {
            throw new IllegalArgumentException("At least one leaf value size required");
        }
        return new TaoTree(SlabAllocator.DEFAULT_SLAB_SIZE, keyLen, leafValueSizes);
    }

    /**
     * Create a data tree with multiple leaf classes and custom slab size.
     *
     * @param keyLen         fixed key length in bytes (must be positive)
     * @param leafValueSizes value sizes for each leaf class (index = leaf class ID)
     * @param slabSize       slab size in bytes
     */
    public static TaoTree open(int keyLen, int[] leafValueSizes, int slabSize) {
        if (keyLen <= 0) throw new IllegalArgumentException("keyLen must be positive: " + keyLen);
        if (leafValueSizes == null || leafValueSizes.length == 0) {
            throw new IllegalArgumentException("At least one leaf value size required");
        }
        return new TaoTree(slabSize, keyLen, leafValueSizes);
    }

    /** Fixed key length in bytes. */
    public int keyLen() { return keyLen; }

    // -----------------------------------------------------------------------
    // Stats (public, self-locking)
    // -----------------------------------------------------------------------

    /** Total bytes allocated by the slab allocator (slab-level granularity). */
    public long totalSlabBytes() {
        lock.readLock().lock();
        try { return slab.totalAllocatedBytes(); }
        finally { lock.readLock().unlock(); }
    }

    /** Total segments currently in use across all slab classes. */
    public long totalSegmentsInUse() {
        lock.readLock().lock();
        try { return slab.totalSegmentsInUse(); }
        finally { lock.readLock().unlock(); }
    }

    /** Total bytes committed by the bump allocator. */
    public long totalOverflowBytes() {
        lock.readLock().lock();
        try { return bump.totalCommittedBytes(); }
        finally { lock.readLock().unlock(); }
    }

    /** Number of bump allocator pages. */
    public int overflowPageCount() {
        lock.readLock().lock();
        try { return bump.pageCount(); }
        finally { lock.readLock().unlock(); }
    }

    // -----------------------------------------------------------------------
    // Lock operations (package-private, used by TaoDictionary)
    // -----------------------------------------------------------------------

    /**
     * Fail fast if the current thread holds a read lock but not the write lock.
     * Prevents read→write upgrade deadlocks.
     */
    void checkCanAcquireWriteLock() {
        if (lock.getReadHoldCount() > 0 && !lock.isWriteLockedByCurrentThread()) {
            throw new IllegalStateException(
                "Cannot acquire write scope while holding only a read scope. " +
                "Release the read scope first, or use a write scope from the start.");
        }
    }

    void acquireReadLock() {
        lock.readLock().lock();
    }

    void releaseReadLock() {
        lock.readLock().unlock();
    }

    void acquireWriteLock() {
        checkCanAcquireWriteLock();
        lock.writeLock().lock();
    }

    void releaseWriteLock() {
        lock.writeLock().unlock();
    }

    boolean isWriteLockedByCurrentThread() {
        return lock.isWriteLockedByCurrentThread();
    }

    // -----------------------------------------------------------------------
    // Lifecycle
    // -----------------------------------------------------------------------

    /**
     * Close this tree, releasing the underlying arena if this is a root tree.
     * Child trees (created internally by dictionaries) do not close the arena.
     */
    @Override
    public void close() {
        if (ownsArena) {
            arena.close();
        }
    }

    // -----------------------------------------------------------------------
    // Scoped access — the primary public API
    // -----------------------------------------------------------------------

    /**
     * Acquire a read scope. Holds the read lock until closed.
     *
     * <p>Use within try-with-resources:
     * <pre>{@code
     * try (var r = tree.read()) {
     *     long leaf = r.lookup(key);
     *     // ... read leaf data ...
     * }
     * }</pre>
     */
    public ReadScope read() {
        acquireReadLock();
        return new ReadScope();
    }

    /**
     * Acquire a write scope. Holds the write lock until closed.
     *
     * <p>Use within try-with-resources:
     * <pre>{@code
     * try (var w = tree.write()) {
     *     long leaf = w.getOrCreate(key, 0);
     *     w.leafValue(leaf).set(ValueLayout.JAVA_LONG, 0, value);
     * }
     * }</pre>
     *
     * @throws IllegalStateException if the current thread holds a read scope but not
     *                               a write scope (read→write upgrade is not supported)
     */
    public WriteScope write() {
        acquireWriteLock();
        return new WriteScope();
    }

    // -----------------------------------------------------------------------
    // ReadScope — read-only scoped access
    // -----------------------------------------------------------------------

    /**
     * Read-only scope of the TaoTree. Holds the read lock.
     *
     * <p>All returned leaf references and {@link MemorySegment} slices are
     * valid only while this scope is open.
     */
    public final class ReadScope implements AutoCloseable {

        private boolean closed;
        private final Thread ownerThread = Thread.currentThread();

        private ReadScope() {}

        /**
         * Point lookup.
         * @return a leaf reference, or {@link TaoTree#NOT_FOUND} if not found
         */
        public long lookup(MemorySegment key, int keyLen) {
            checkAccess();
            validateKeyLen(keyLen);
            return lookupImpl(key, keyLen);
        }

        /** Point lookup (uses the tree's key length). */
        public long lookup(MemorySegment key) {
            checkAccess();
            return lookupImpl(key, TaoTree.this.keyLen);
        }

        public long lookup(byte[] key) {
            checkAccess();
            validateKeyLen(key.length);
            return lookupImpl(MemorySegment.ofArray(key), key.length);
        }

        /**
         * Resolve a leaf pointer to a <b>read-only</b> view of the value portion.
         * The returned segment is valid only while this scope is open.
         * Writing to it throws {@link UnsupportedOperationException}.
         */
        public MemorySegment leafValue(long leafPtr) {
            checkAccess();
            validateLeafPtr(leafPtr);
            return leafValueImpl(leafPtr).asReadOnly();
        }

        public long size() { checkAccess(); return TaoTree.this.size; }
        public boolean isEmpty() { checkAccess(); return TaoTree.this.size == 0; }

        @Override
        public void close() {
            if (!closed) {
                checkThread();
                closed = true;
                releaseReadLock();
            }
        }

        private void checkAccess() {
            if (closed) throw new IllegalStateException("ReadScope is closed");
            checkThread();
        }

        private void checkThread() {
            if (Thread.currentThread() != ownerThread) {
                throw new IllegalStateException(
                    "Scope must be used by the thread that created it (owner="
                    + ownerThread.getName() + ", current=" + Thread.currentThread().getName() + ")");
            }
        }
    }

    // -----------------------------------------------------------------------
    // WriteScope — read+write scoped access
    // -----------------------------------------------------------------------

    /**
     * Read-write scope of the TaoTree. Holds the write lock.
     *
     * <p>All returned leaf references and {@link MemorySegment} slices are
     * valid only while this scope is open.
     */
    public final class WriteScope implements AutoCloseable {

        private boolean closed;
        private final Thread ownerThread = Thread.currentThread();

        private WriteScope() {}

        // -- Read operations (also available under write lock) --

        public long lookup(MemorySegment key, int keyLen) {
            checkAccess();
            validateKeyLen(keyLen);
            return lookupImpl(key, keyLen);
        }

        /** Point lookup (uses the tree's key length). */
        public long lookup(MemorySegment key) {
            checkAccess();
            return lookupImpl(key, TaoTree.this.keyLen);
        }

        public long lookup(byte[] key) {
            checkAccess();
            validateKeyLen(key.length);
            return lookupImpl(MemorySegment.ofArray(key), key.length);
        }

        /**
         * Resolve a leaf pointer to a <b>writable</b> view of the value portion.
         * The returned segment is valid only while this scope is open.
         */
        public MemorySegment leafValue(long leafPtr) {
            checkAccess();
            validateLeafPtr(leafPtr);
            return leafValueImpl(leafPtr);
        }

        public long size() { checkAccess(); return TaoTree.this.size; }
        public boolean isEmpty() { checkAccess(); return TaoTree.this.size == 0; }

        // -- Write operations --

        /**
         * Insert a key or return the existing leaf.
         *
         * @param key       the key (binary-comparable, length must equal this tree's keyLen)
         * @param keyLen    key length in bytes
         * @param leafClass the leaf class index (0-based)
         * @return a leaf reference (existing or newly created with zeroed value)
         */
        public long getOrCreate(MemorySegment key, int keyLen, int leafClass) {
            checkAccess();
            validateKeyLen(keyLen);
            validateLeafClass(leafClass);
            return getOrCreateImpl(key, keyLen, leafClass);
        }

        /** Insert or lookup using the tree's key length and the default leaf class (0). */
        public long getOrCreate(MemorySegment key) {
            checkAccess();
            return getOrCreateImpl(key, TaoTree.this.keyLen, 0);
        }

        public long getOrCreate(byte[] key, int leafClass) {
            checkAccess();
            validateKeyLen(key.length);
            validateLeafClass(leafClass);
            return getOrCreateImpl(MemorySegment.ofArray(key), key.length, leafClass);
        }

        /** Insert or lookup using the default leaf class (0). */
        public long getOrCreate(byte[] key) {
            checkAccess();
            validateKeyLen(key.length);
            return getOrCreateImpl(MemorySegment.ofArray(key), key.length, 0);
        }

        /**
         * Delete a key.
         * @return true if the key existed and was removed
         */
        public boolean delete(MemorySegment key, int keyLen) {
            checkAccess();
            validateKeyLen(keyLen);
            return deleteImpl(key, keyLen);
        }

        public boolean delete(byte[] key) {
            checkAccess();
            validateKeyLen(key.length);
            return deleteImpl(MemorySegment.ofArray(key), key.length);
        }

        @Override
        public void close() {
            if (!closed) {
                checkThread();
                closed = true;
                releaseWriteLock();
            }
        }

        private void checkAccess() {
            if (closed) throw new IllegalStateException("WriteScope is closed");
            checkThread();
        }

        private void checkThread() {
            if (Thread.currentThread() != ownerThread) {
                throw new IllegalStateException(
                    "Scope must be used by the thread that created it (owner="
                    + ownerThread.getName() + ", current=" + Thread.currentThread().getName() + ")");
            }
        }
    }

    // -----------------------------------------------------------------------
    // Validation
    // -----------------------------------------------------------------------

    private void validateKeyLen(int len) {
        if (len != this.keyLen) {
            throw new IllegalArgumentException(
                "Key length " + len + " does not match TaoTree key length " + this.keyLen);
        }
    }

    private void validateLeafClass(int leafClass) {
        if (leafClass < 0 || leafClass >= leafClassCount) {
            throw new IllegalArgumentException(
                "Leaf class " + leafClass + " out of range [0, " + leafClassCount + ")");
        }
    }

    private void validateLeafPtr(long ptr) {
        if (NodePtr.isEmpty(ptr)) {
            throw new IllegalArgumentException("Cannot resolve EMPTY_PTR as a leaf");
        }
        if (!NodePtr.isLeaf(ptr)) {
            throw new IllegalArgumentException(
                "Pointer is not a leaf (nodeType=" + NodePtr.nodeType(ptr) + ")");
        }
        int classId = NodePtr.slabClassId(ptr);
        boolean valid = false;
        for (int i = 0; i < leafClassCount; i++) {
            if (leafClassIds[i] == classId) { valid = true; break; }
        }
        if (!valid) {
            throw new IllegalArgumentException(
                "Leaf pointer slabClassId=" + classId + " does not belong to this TaoTree");
        }
    }

    // =======================================================================
    // Package-private implementation — no locking, called by views and TaoDictionary
    // =======================================================================

    long lookupImpl(MemorySegment key, int keyLen) {
        long node = root;
        int depth = 0;

        while (!NodePtr.isEmpty(node)) {
            int type = NodePtr.nodeType(node);

            if (type == NodePtr.LEAF) {
                return leafKeyMatches(node, key, keyLen) ? node : NodePtr.EMPTY_PTR;
            }

            if (type == NodePtr.PREFIX) {
                MemorySegment prefSeg = slab.resolve(node);
                int prefLen = PrefixNode.count(prefSeg);
                int matched = PrefixNode.matchKey(prefSeg, key, keyLen, depth);
                if (matched < prefLen) {
                    return NodePtr.EMPTY_PTR;
                }
                depth += prefLen;
                node = PrefixNode.child(prefSeg);
                continue;
            }

            if (depth >= keyLen) return NodePtr.EMPTY_PTR;
            byte keyByte = key.get(ValueLayout.JAVA_BYTE, depth);
            node = findChild(node, type, keyByte);
            depth++;
        }
        return NodePtr.EMPTY_PTR;
    }

    long getOrCreateImpl(MemorySegment key, int keyLen, int leafClass) {
        if (NodePtr.isEmpty(root)) {
            long leafPtr = allocateLeaf(key, keyLen, leafClass);
            root = wrapInPrefix(key, 0, keyLen, leafPtr);
            size++;
            return leafPtr;
        }
        return doInsert(key, keyLen, leafClass);
    }

    private boolean deleteImpl(MemorySegment key, int keyLen) {
        if (NodePtr.isEmpty(root)) return false;
        boolean[] deleted = {false};
        root = doDelete(root, key, keyLen, 0, deleted);
        if (deleted[0]) size--;
        return deleted[0];
    }

    MemorySegment leafValueImpl(long leafPtr) {
        MemorySegment full = slab.resolve(leafPtr);
        return full.asSlice(keySlotSize);
    }

    // =======================================================================
    // Internal: Insert
    // =======================================================================

    private long doInsert(MemorySegment key, int keyLen, int leafClass) {
        long nodePtr = root;
        MemorySegment parentSeg = null;
        long parentOff = 0;
        int depth = 0;

        while (true) {
            int type = NodePtr.nodeType(nodePtr);

            if (type == NodePtr.LEAF) {
                if (leafKeyMatches(nodePtr, key, keyLen)) {
                    return nodePtr;
                }
                long newNode = expandLeaf(nodePtr, key, keyLen, depth, leafClass);
                writeBack(parentSeg, parentOff, newNode);
                return findInsertedLeaf(newNode, key, keyLen, depth);
            }

            if (type == NodePtr.PREFIX) {
                MemorySegment prefSeg = slab.resolve(nodePtr);
                int prefLen = PrefixNode.count(prefSeg);
                int matched = PrefixNode.matchKey(prefSeg, key, keyLen, depth);

                if (matched < prefLen) {
                    long newNode = splitPrefix(nodePtr, prefSeg, key, keyLen,
                                               depth, matched, leafClass);
                    writeBack(parentSeg, parentOff, newNode);
                    size++;
                    return findInsertedLeaf(newNode, key, keyLen, depth);
                }

                depth += prefLen;
                parentSeg = prefSeg;
                parentOff = PrefixNode.OFF_CHILD;
                nodePtr = PrefixNode.child(prefSeg);
                continue;
            }

            if (depth >= keyLen) {
                throw new IllegalStateException("Key exhausted at inner node (depth=" + depth + ")");
            }
            byte keyByte = key.get(ValueLayout.JAVA_BYTE, depth);
            long child = findChild(nodePtr, type, keyByte);

            if (NodePtr.isEmpty(child)) {
                long leafPtr = allocateLeaf(key, keyLen, leafClass);
                long wrappedLeaf = wrapInPrefix(key, depth + 1, keyLen, leafPtr);
                insertChildIntoNode(nodePtr, type, keyByte, wrappedLeaf, parentSeg, parentOff);
                size++;
                return leafPtr;
            }

            MemorySegment nodeSeg = slab.resolve(nodePtr);
            parentSeg = nodeSeg;
            parentOff = childOffset(type, nodeSeg, keyByte);
            nodePtr = child;
            depth++;
        }
    }

    private long expandLeaf(long existingLeafPtr, MemorySegment newKey, int newKeyLen,
                            int depth, int leafClass) {
        MemorySegment existingKey = slab.resolve(existingLeafPtr, keyLen);

        int mismatch = depth;
        while (mismatch < newKeyLen &&
               existingKey.get(ValueLayout.JAVA_BYTE, mismatch) ==
               newKey.get(ValueLayout.JAVA_BYTE, mismatch)) {
            mismatch++;
        }

        if (mismatch >= newKeyLen) {
            throw new IllegalStateException("Duplicate key in expandLeaf");
        }

        long newLeafPtr = allocateLeaf(newKey, newKeyLen, leafClass);
        size++;

        long n4Ptr = slab.allocate(node4ClassId, NodePtr.NODE_4);
        MemorySegment n4Seg = slab.resolve(n4Ptr);
        Node4.init(n4Seg);

        byte existingByte = existingKey.get(ValueLayout.JAVA_BYTE, mismatch);
        byte newByte = newKey.get(ValueLayout.JAVA_BYTE, mismatch);

        long existingWrapped = wrapInPrefix(existingKey, mismatch + 1, keyLen, existingLeafPtr);
        long newWrapped = wrapInPrefix(newKey, mismatch + 1, newKeyLen, newLeafPtr);

        Node4.insertChild(n4Seg, existingByte, existingWrapped);
        Node4.insertChild(n4Seg, newByte, newWrapped);

        return wrapInPrefix(newKey, depth, mismatch, n4Ptr);
    }

    private long splitPrefix(long prefixPtr, MemorySegment prefSeg,
                             MemorySegment key, int keyLen,
                             int depth, int matchedCount, int leafClass) {
        int prefLen = PrefixNode.count(prefSeg);
        long prefChild = PrefixNode.child(prefSeg);

        long newLeafPtr = allocateLeaf(key, keyLen, leafClass);

        long n4Ptr = slab.allocate(node4ClassId, NodePtr.NODE_4);
        MemorySegment n4Seg = slab.resolve(n4Ptr);
        Node4.init(n4Seg);

        byte existingByte = PrefixNode.keyAt(prefSeg, matchedCount);
        byte newByte = key.get(ValueLayout.JAVA_BYTE, depth + matchedCount);

        long existingChild;
        int remainingPrefixLen = prefLen - matchedCount - 1;
        if (remainingPrefixLen > 0) {
            long newPrefPtr = slab.allocate(prefixClassId, NodePtr.PREFIX);
            MemorySegment newPrefSeg = slab.resolve(newPrefPtr);
            byte[] remaining = new byte[remainingPrefixLen];
            for (int i = 0; i < remainingPrefixLen; i++) {
                remaining[i] = PrefixNode.keyAt(prefSeg, matchedCount + 1 + i);
            }
            PrefixNode.init(newPrefSeg, remaining, 0, remainingPrefixLen, prefChild);
            existingChild = newPrefPtr;
        } else {
            existingChild = prefChild;
        }

        long newWrapped = wrapInPrefix(key, depth + matchedCount + 1, keyLen, newLeafPtr);

        Node4.insertChild(n4Seg, existingByte, existingChild);
        Node4.insertChild(n4Seg, newByte, newWrapped);

        slab.free(prefixPtr);

        if (matchedCount > 0) {
            long wrapPtr = slab.allocate(prefixClassId, NodePtr.PREFIX);
            MemorySegment wrapSeg = slab.resolve(wrapPtr);
            byte[] shared = new byte[matchedCount];
            for (int i = 0; i < matchedCount; i++) {
                shared[i] = key.get(ValueLayout.JAVA_BYTE, depth + i);
            }
            PrefixNode.init(wrapSeg, shared, 0, matchedCount, n4Ptr);
            return wrapPtr;
        }
        return n4Ptr;
    }

    private void insertChildIntoNode(long nodePtr, int type, byte keyByte, long childPtr,
                                     MemorySegment parentSeg, long parentOff) {
        MemorySegment seg = slab.resolve(nodePtr);

        switch (type) {
            case NodePtr.NODE_4 -> {
                if (Node4.isFull(seg)) {
                    long n16Ptr = slab.allocate(node16ClassId, NodePtr.NODE_16);
                    MemorySegment n16Seg = slab.resolve(n16Ptr);
                    Node16.init(n16Seg);
                    Node16.copyFromNode4(n16Seg, seg);
                    Node16.insertChild(n16Seg, keyByte, childPtr);
                    slab.free(nodePtr);
                    writeBack(parentSeg, parentOff, n16Ptr);
                } else {
                    Node4.insertChild(seg, keyByte, childPtr);
                }
            }
            case NodePtr.NODE_16 -> {
                if (Node16.isFull(seg)) {
                    long n48Ptr = slab.allocate(node48ClassId, NodePtr.NODE_48);
                    MemorySegment n48Seg = slab.resolve(n48Ptr);
                    Node48.init(n48Seg);
                    Node48.copyFromNode16(n48Seg, seg);
                    Node48.insertChild(n48Seg, keyByte, childPtr);
                    slab.free(nodePtr);
                    writeBack(parentSeg, parentOff, n48Ptr);
                } else {
                    Node16.insertChild(seg, keyByte, childPtr);
                }
            }
            case NodePtr.NODE_48 -> {
                if (Node48.isFull(seg)) {
                    long n256Ptr = slab.allocate(node256ClassId, NodePtr.NODE_256);
                    MemorySegment n256Seg = slab.resolve(n256Ptr);
                    Node256.init(n256Seg);
                    Node256.copyFromNode48(n256Seg, seg);
                    Node256.insertChild(n256Seg, keyByte, childPtr);
                    slab.free(nodePtr);
                    writeBack(parentSeg, parentOff, n256Ptr);
                } else {
                    Node48.insertChild(seg, keyByte, childPtr);
                }
            }
            case NodePtr.NODE_256 -> {
                Node256.insertChild(seg, keyByte, childPtr);
            }
            default -> throw new IllegalStateException("Not an inner node: " + type);
        }
    }

    // =======================================================================
    // Internal: Delete (recursive)
    // =======================================================================

    private long doDelete(long nodePtr, MemorySegment key, int keyLen,
                          int depth, boolean[] deleted) {
        if (NodePtr.isEmpty(nodePtr)) return NodePtr.EMPTY_PTR;

        int type = NodePtr.nodeType(nodePtr);

        if (type == NodePtr.LEAF) {
            if (leafKeyMatches(nodePtr, key, keyLen)) {
                slab.free(nodePtr);
                deleted[0] = true;
                return NodePtr.EMPTY_PTR;
            }
            return nodePtr;
        }

        if (type == NodePtr.PREFIX) {
            MemorySegment prefSeg = slab.resolve(nodePtr);
            int prefLen = PrefixNode.count(prefSeg);
            int matched = PrefixNode.matchKey(prefSeg, key, keyLen, depth);
            if (matched < prefLen) return nodePtr;

            long child = PrefixNode.child(prefSeg);
            long newChild = doDelete(child, key, keyLen, depth + prefLen, deleted);
            if (!deleted[0]) return nodePtr;

            if (NodePtr.isEmpty(newChild)) {
                slab.free(nodePtr);
                return NodePtr.EMPTY_PTR;
            }

            if (NodePtr.nodeType(newChild) == NodePtr.PREFIX) {
                return mergePrefixes(nodePtr, prefSeg, newChild);
            }

            PrefixNode.setChild(prefSeg, newChild);
            return nodePtr;
        }

        if (depth >= keyLen) return nodePtr;
        byte keyByte = key.get(ValueLayout.JAVA_BYTE, depth);
        long child = findChild(nodePtr, type, keyByte);
        if (NodePtr.isEmpty(child)) return nodePtr;

        long newChild = doDelete(child, key, keyLen, depth + 1, deleted);
        if (!deleted[0]) return nodePtr;

        MemorySegment seg = slab.resolve(nodePtr);

        if (NodePtr.isEmpty(newChild)) {
            removeChildFromNode(seg, type, keyByte);
            int count = nodeCount(seg, type);

            if (count == 0) {
                slab.free(nodePtr);
                return NodePtr.EMPTY_PTR;
            }
            if (count == 1) {
                return collapseSingleChild(nodePtr, seg, type);
            }
            return maybeShrink(nodePtr, seg, type, count);
        }

        updateChildInNode(seg, type, keyByte, newChild);
        return nodePtr;
    }

    private long mergePrefixes(long outerPtr, MemorySegment outerSeg, long innerPtr) {
        MemorySegment innerSeg = slab.resolve(innerPtr);
        int outerLen = PrefixNode.count(outerSeg);
        int innerLen = PrefixNode.count(innerSeg);
        long innerChild = PrefixNode.child(innerSeg);

        int totalLen = outerLen + innerLen;
        byte[] merged = new byte[totalLen];
        for (int i = 0; i < outerLen; i++) merged[i] = PrefixNode.keyAt(outerSeg, i);
        for (int i = 0; i < innerLen; i++) merged[outerLen + i] = PrefixNode.keyAt(innerSeg, i);

        slab.free(outerPtr);
        slab.free(innerPtr);

        return wrapInPrefix(merged, 0, totalLen, innerChild);
    }

    private long collapseSingleChild(long nodePtr, MemorySegment seg, int type) {
        byte[] singleKey = {0};
        long[] singleChild = {0};
        findSingleChild(seg, type, singleKey, singleChild);

        slab.free(nodePtr);

        byte[] prefixByte = {singleKey[0]};
        long child = singleChild[0];

        if (NodePtr.nodeType(child) == NodePtr.PREFIX) {
            MemorySegment childPrefSeg = slab.resolve(child);
            int childPrefLen = PrefixNode.count(childPrefSeg);
            byte[] m = new byte[1 + childPrefLen];
            m[0] = prefixByte[0];
            for (int i = 0; i < childPrefLen; i++) m[1 + i] = PrefixNode.keyAt(childPrefSeg, i);
            long grandChild = PrefixNode.child(childPrefSeg);
            slab.free(child);
            return wrapInPrefix(m, 0, m.length, grandChild);
        }

        long prefPtr = slab.allocate(prefixClassId, NodePtr.PREFIX);
        MemorySegment prefSeg = slab.resolve(prefPtr);
        PrefixNode.init(prefSeg, prefixByte, 0, 1, child);
        return prefPtr;
    }

    private long maybeShrink(long nodePtr, MemorySegment seg, int type, int count) {
        return switch (type) {
            case NodePtr.NODE_256 -> count <= NodeConstants.NODE256_SHRINK_THRESHOLD
                ? shrinkNode256ToNode48(nodePtr, seg) : nodePtr;
            case NodePtr.NODE_48 -> count <= NodeConstants.NODE48_SHRINK_THRESHOLD
                ? shrinkNode48ToNode16(nodePtr, seg) : nodePtr;
            case NodePtr.NODE_16 -> count <= NodeConstants.NODE16_SHRINK_THRESHOLD
                ? shrinkNode16ToNode4(nodePtr, seg) : nodePtr;
            default -> nodePtr;
        };
    }

    private long shrinkNode256ToNode48(long oldPtr, MemorySegment oldSeg) {
        long n48Ptr = slab.allocate(node48ClassId, NodePtr.NODE_48);
        MemorySegment n48Seg = slab.resolve(n48Ptr);
        Node48.init(n48Seg);
        Node256.forEach(oldSeg, (k, c) -> Node48.insertChild(n48Seg, k, c));
        slab.free(oldPtr);
        return n48Ptr;
    }

    private long shrinkNode48ToNode16(long oldPtr, MemorySegment oldSeg) {
        long n16Ptr = slab.allocate(node16ClassId, NodePtr.NODE_16);
        MemorySegment n16Seg = slab.resolve(n16Ptr);
        Node16.init(n16Seg);
        Node48.forEach(oldSeg, (k, c) -> Node16.insertChild(n16Seg, k, c));
        slab.free(oldPtr);
        return n16Ptr;
    }

    private long shrinkNode16ToNode4(long oldPtr, MemorySegment oldSeg) {
        long n4Ptr = slab.allocate(node4ClassId, NodePtr.NODE_4);
        MemorySegment n4Seg = slab.resolve(n4Ptr);
        Node4.init(n4Seg);
        int n = Node16.count(oldSeg);
        for (int i = 0; i < n; i++) {
            Node4.insertChild(n4Seg, Node16.keyAt(oldSeg, i), Node16.childAt(oldSeg, i));
        }
        slab.free(oldPtr);
        return n4Ptr;
    }

    // =======================================================================
    // Internal: helpers
    // =======================================================================

    private long findChild(long nodePtr, int type, byte keyByte) {
        MemorySegment seg = slab.resolve(nodePtr);
        return switch (type) {
            case NodePtr.NODE_4   -> Node4.findChild(seg, keyByte);
            case NodePtr.NODE_16  -> Node16.findChild(seg, keyByte);
            case NodePtr.NODE_48  -> Node48.findChild(seg, keyByte);
            case NodePtr.NODE_256 -> Node256.findChild(seg, keyByte);
            default -> NodePtr.EMPTY_PTR;
        };
    }

    private long childOffset(int type, MemorySegment seg, byte keyByte) {
        return switch (type) {
            case NodePtr.NODE_4 -> Node4.OFF_CHILDREN + (long) Node4.findPos(seg, keyByte) * 8;
            case NodePtr.NODE_16 -> Node16.OFF_CHILDREN + (long) Node16.findPos(seg, keyByte) * 8;
            case NodePtr.NODE_48 -> {
                int slot = Byte.toUnsignedInt(seg.get(ValueLayout.JAVA_BYTE,
                    Node48.OFF_CHILD_IDX + Byte.toUnsignedInt(keyByte)));
                yield Node48.OFF_CHILDREN + (long) slot * 8;
            }
            case NodePtr.NODE_256 -> Node256.OFF_CHILDREN + (long) Byte.toUnsignedInt(keyByte) * 8;
            default -> throw new IllegalStateException("Not an inner node");
        };
    }

    private int nodeCount(MemorySegment seg, int type) {
        return switch (type) {
            case NodePtr.NODE_4   -> Node4.count(seg);
            case NodePtr.NODE_16  -> Node16.count(seg);
            case NodePtr.NODE_48  -> Node48.count(seg);
            case NodePtr.NODE_256 -> Node256.count(seg);
            default -> 0;
        };
    }

    private void removeChildFromNode(MemorySegment seg, int type, byte keyByte) {
        switch (type) {
            case NodePtr.NODE_4   -> Node4.removeChild(seg, keyByte);
            case NodePtr.NODE_16  -> Node16.removeChild(seg, keyByte);
            case NodePtr.NODE_48  -> Node48.removeChild(seg, keyByte);
            case NodePtr.NODE_256 -> Node256.removeChild(seg, keyByte);
        }
    }

    private void updateChildInNode(MemorySegment seg, int type, byte keyByte, long newChild) {
        switch (type) {
            case NodePtr.NODE_4  -> Node4.setChildAt(seg, Node4.findPos(seg, keyByte), newChild);
            case NodePtr.NODE_16 -> Node16.setChildAt(seg, Node16.findPos(seg, keyByte), newChild);
            case NodePtr.NODE_48  -> Node48.updateChild(seg, keyByte, newChild);
            case NodePtr.NODE_256 -> Node256.updateChild(seg, keyByte, newChild);
        }
    }

    private void findSingleChild(MemorySegment seg, int type, byte[] outKey, long[] outChild) {
        switch (type) {
            case NodePtr.NODE_4 -> { outKey[0] = Node4.keyAt(seg, 0); outChild[0] = Node4.childAt(seg, 0); }
            case NodePtr.NODE_16 -> { outKey[0] = Node16.keyAt(seg, 0); outChild[0] = Node16.childAt(seg, 0); }
            case NodePtr.NODE_48 -> Node48.forEach(seg, (k, c) -> { outKey[0] = k; outChild[0] = c; });
            case NodePtr.NODE_256 -> Node256.forEach(seg, (k, c) -> { outKey[0] = k; outChild[0] = c; });
        }
    }

    private boolean leafKeyMatches(long leafPtr, MemorySegment key, int keyLen) {
        if (keyLen != this.keyLen) return false;
        MemorySegment leafKey = slab.resolve(leafPtr, this.keyLen);
        return leafKey.mismatch(key.asSlice(0, this.keyLen)) == -1;
    }

    private long allocateLeaf(MemorySegment key, int keyLen, int leafClass) {
        long ptr = slab.allocate(leafClassIds[leafClass]);
        MemorySegment seg = slab.resolve(ptr);
        // Copy key into the first keyLen bytes
        MemorySegment.copy(key, 0, seg, 0, this.keyLen);
        // Zero everything after the key (padding + value) so callers never see stale bytes
        seg.asSlice(this.keyLen).fill((byte) 0);
        return ptr;
    }

    private long wrapInPrefix(MemorySegment key, int from, int to, long child) {
        int remaining = to - from;
        if (remaining <= 0) return child;

        long current = child;
        int pos = to;
        while (pos > from) {
            int chunkLen = Math.min(pos - from, NodeConstants.PREFIX_CAPACITY);
            int chunkStart = pos - chunkLen;
            long prefPtr = slab.allocate(prefixClassId, NodePtr.PREFIX);
            MemorySegment prefSeg = slab.resolve(prefPtr);
            PrefixNode.init(prefSeg, key, chunkStart, chunkLen, current);
            current = prefPtr;
            pos = chunkStart;
        }
        return current;
    }

    private long wrapInPrefix(byte[] key, int from, int to, long child) {
        int remaining = to - from;
        if (remaining <= 0) return child;

        long current = child;
        int pos = to;
        while (pos > from) {
            int chunkLen = Math.min(pos - from, NodeConstants.PREFIX_CAPACITY);
            int chunkStart = pos - chunkLen;
            long prefPtr = slab.allocate(prefixClassId, NodePtr.PREFIX);
            MemorySegment prefSeg = slab.resolve(prefPtr);
            PrefixNode.init(prefSeg, key, chunkStart, chunkLen, current);
            current = prefPtr;
            pos = chunkStart;
        }
        return current;
    }

    private void writeBack(MemorySegment parentSeg, long parentOff, long newPtr) {
        if (parentSeg == null) {
            root = newPtr;
        } else {
            parentSeg.set(ValueLayout.JAVA_LONG, parentOff, newPtr);
        }
    }

    private long findInsertedLeaf(long node, MemorySegment key, int keyLen, int depth) {
        while (!NodePtr.isEmpty(node)) {
            int type = NodePtr.nodeType(node);
            if (type == NodePtr.LEAF) return node;
            if (type == NodePtr.PREFIX) {
                MemorySegment seg = slab.resolve(node);
                depth += PrefixNode.count(seg);
                node = PrefixNode.child(seg);
                continue;
            }
            if (depth >= keyLen) break;
            byte b = key.get(ValueLayout.JAVA_BYTE, depth);
            node = findChild(node, type, b);
            depth++;
        }
        throw new IllegalStateException("Could not find inserted leaf");
    }
}
