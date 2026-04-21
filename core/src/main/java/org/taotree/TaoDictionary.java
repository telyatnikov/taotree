package org.taotree;

import org.taotree.internal.art.NodePtr;
import org.taotree.internal.persist.Superblock;

import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReentrantLock;

/**
 * ART-backed intern table that maps variable-length strings to monotonic integer codes.
 *
 * <p>Uses {@link TaoKey#encodeString(String)} for binary-comparable encoding, then
 * pads to a fixed key length for the underlying tree. Each leaf stores a single
 * {@code int} code.
 *
 * <h3>Scoped access (consistent with {@link TaoTree}):</h3>
 * <pre>{@code
 * try (var w = dict.write()) {
 *     int code = w.intern("Animalia");
 * }
 * try (var r = dict.read()) {
 *     int code = r.resolve("Animalia");
 * }
 * }</pre>
 *
 * <h3>Convenience methods (self-locking):</h3>
 * <pre>{@code
 * int code = dict.intern("Animalia");   // acquires write lock
 * int code = dict.resolve("Animalia");  // acquires read lock
 * }</pre>
 *
 * <p>Self-locking methods are reentrant-safe and can be called from within an existing
 * {@link TaoTree.WriteScope} or {@link TaoTree.ReadScope} on the same tree.
 *
 * <p><b>Important:</b> All dictionaries used to encode keys for a given tree must
 * be created from that same tree.
 */
public final class TaoDictionary {

    /** Maximum encoded key length (bytes). Strings longer than this are rejected. */
    public static final int MAX_KEY_LEN = 128;

    private static final int CODE_SIZE = 4; // int32

    private final TaoTree owner;
    private final TaoTree tree;
    private final int maxCode;
    private volatile int nextCode;

    // Per-dictionary lock for COW mode: serializes dict writes without holding
    // the global tree lock. Multiple dicts can be interned concurrently.
    private final ReentrantLock dictLock = new ReentrantLock();

    // Lazy reverse cache: code → string. Populated on intern() and lazily
    // rebuilt via full tree scan on reverseLookup() cache miss.
    private final ConcurrentHashMap<Integer, String> reverseCache = new ConcurrentHashMap<>();

    /**
     * Create a dictionary.
     *
     * @param tree     the tree providing shared infrastructure (allocator, lock)
     * @param maxCode   the maximum code value (e.g., 0xFFFF for u16, Integer.MAX_VALUE for u32)
     * @param startCode the first code to assign (0 is typically the null sentinel)
     */
    public TaoDictionary(TaoTree tree, int maxCode, int startCode) {
        this.owner = tree;
        this.tree = new TaoTree(tree, MAX_KEY_LEN, new int[]{CODE_SIZE});
        this.maxCode = maxCode;
        this.nextCode = startCode;
        owner.registerDict(this);
    }

    /**
     * Package-private: restore a dictionary from persisted state.
     *
     * @param owner     the owning tree
     * @param childTree the pre-restored child tree (already has root, size, class IDs)
     * @param maxCode   maximum code value
     * @param nextCode  next code to assign
     */
    TaoDictionary(TaoTree owner, TaoTree childTree, int maxCode, int nextCode) {
        this.owner = owner;
        this.tree = childTree;
        this.maxCode = maxCode;
        this.nextCode = nextCode;
        // Don't call owner.registerDict — the caller (TaoTree.openFileBacked) handles this
    }

    /** Create a u16 dictionary (codes 1..65535, 0 reserved as null sentinel). */
    public static TaoDictionary u16(TaoTree tree) {
        return new TaoDictionary(tree, 0xFFFF, 1);
    }

    /** Create a u32 dictionary (codes 1..Integer.MAX_VALUE, 0 reserved as null sentinel). */
    public static TaoDictionary u32(TaoTree tree) {
        return new TaoDictionary(tree, Integer.MAX_VALUE, 1);
    }

    /** The tree this dictionary belongs to. */
    public TaoTree owner() { return owner; }

    // -----------------------------------------------------------------------
    // Scoped access
    // -----------------------------------------------------------------------

    /** Acquire a read scope. Lock-free (dict resolve is lock-free). */
    public ReadScope read() {
        return new ReadScope();
    }

    /**
     * Acquire a write scope. Acquires the per-dictionary lock.
     */
    public WriteScope write() {
        dictLock.lock();
        return new WriteScope();
    }

    // -----------------------------------------------------------------------
    // Convenience self-locking methods
    // -----------------------------------------------------------------------

    /**
     * Intern a string: return its code, assigning a new one if not yet known.
     *
     * <p>Acquires a per-dictionary lock (not the global tree lock),
     * allowing concurrent interning into different dictionaries.
     *
     * @param value the string to intern
     * @return the integer code (never 0 — zero is reserved as null sentinel)
     * @throws IllegalStateException if the dictionary is full
     */
    public int intern(String value) {
        byte[] padded = encodeAndPad(value);
        int existing = resolveEncoded(padded);
        if (existing != -1) {
            return existing;
        }
        dictLock.lock();
        try {
            // Re-check: another writer may have interned while we waited for the lock
            existing = resolveEncoded(padded);
            if (existing != -1) return existing;
            return internEncoded(value, padded);
        } finally {
            dictLock.unlock();
        }
    }

    /**
     * Resolve a string to its code without assigning. Returns -1 if not found.
     *
     * <p>Lock-free: reads the child tree's root via acquire semantics.
     */
    public int resolve(String value) {
        return resolveImpl(value);
    }

    /** Number of entries (lock-free). */
    public int size() {
        return (int) tree.publishedState().size();
    }

    /** The next code that will be assigned (lock-free). */
    public int nextCode() {
        return nextCode;
    }

    /**
     * Reverse-lookup: return the string for a given code.
     *
     * <p>Uses a lazy cache populated on {@link #intern(String)} and rebuilt on
     * cache miss by scanning the dict tree. Safe for concurrent use.
     *
     * @param code the integer code to look up
     * @return the interned string, or {@code null} if the code is unknown
     */
    public String reverseLookup(int code) {
        String cached = reverseCache.get(code);
        if (cached != null) return cached;
        rebuildReverseCache();
        return reverseCache.get(code);
    }

    private void rebuildReverseCache() {
        // Use the tree's internal scan to iterate all dict entries.
        // The dict tree stores encoded_string → code (int32).
        tree.scanAllLeaves((keySegment, valueSegment) -> {
            int code = valueSegment.get(ValueLayout.JAVA_INT_UNALIGNED, 0);
            if (code != 0) {
                byte[] keyBytes = new byte[MAX_KEY_LEN];
                MemorySegment.copy(keySegment, 0,
                    MemorySegment.ofArray(keyBytes), 0, MAX_KEY_LEN);
                String value = TaoKey.decodeString(keyBytes);
                if (value != null) {
                    reverseCache.putIfAbsent(code, value);
                }
            }
            return true;
        });
    }

    // -----------------------------------------------------------------------
    // ReadScope
    // -----------------------------------------------------------------------

    /** Read-only scope of the dictionary (lock-free). */
    public final class ReadScope implements AutoCloseable {
        /** Resolve a string to its code. Returns -1 if not found. */
        public int resolve(String value) { return resolveImpl(value); }

        /** Number of entries. */
        public int size() { return (int) tree.publishedState().size(); }

        @Override
        public void close() { /* lock-free — nothing to release */ }
    }

    // -----------------------------------------------------------------------
    // WriteScope
    // -----------------------------------------------------------------------

    /** Write scope of the dictionary. Holds the per-dictionary lock. */
    public final class WriteScope implements AutoCloseable {
        /** Intern a string: return its code, assigning a new one if not yet known. */
        public int intern(String value) { return internEncoded(value, encodeAndPad(value)); }

        /** Resolve a string to its code. Returns -1 if not found. */
        public int resolve(String value) { return resolveImpl(value); }

        /** Number of entries. */
        public int size() { return (int) tree.publishedState().size(); }

        @Override
        public void close() {
            dictLock.unlock();
        }
    }

    // -----------------------------------------------------------------------
    // Internal
    // -----------------------------------------------------------------------

    /** Package-private: export a dict descriptor for the superblock. */
    Superblock.DictDescriptor exportDescriptor(int treeIndex) {
        var desc = new Superblock.DictDescriptor();
        desc.maxCode = maxCode;
        desc.nextCode = nextCode;
        desc.treeIndex = treeIndex;
        return desc;
    }

    /** Package-private: the child tree backing this dictionary. */
    TaoTree childTree() { return tree; }

    /**
     * Copy all entries from the source dictionary into this dictionary.
     *
     * <p>This is a compaction-only operation: the target dictionary must be empty
     * and have the same {@code maxCode} as the source. After copy, the target's
     * {@code nextCode} matches the source's, so future interns continue the same
     * code sequence.
     *
     * <p>Acquires the per-dictionary lock for thread safety.
     *
     * @param source the source dictionary to copy from
     * @throws IllegalStateException if the target is not empty
     * @throws IllegalArgumentException if maxCode doesn't match
     */
    public void copyFrom(TaoDictionary source) {
        dictLock.lock();
        try {
            if (tree.size != 0) {
                throw new IllegalStateException(
                    "TaoDictionary.copyFrom() requires an empty target dictionary (size="
                    + tree.size + "). Use a freshly created dictionary.");
            }
            if (this.maxCode != source.maxCode) {
                throw new IllegalArgumentException(
                    "maxCode mismatch: source=" + source.maxCode + " target=" + this.maxCode);
            }
            var ws = tree.beginWrite();
            tree.copyFromImpl(source.tree, ws);
            tree.commitWrite(ws);
            this.nextCode = source.nextCode;
        } finally {
            dictLock.unlock();
        }
    }

    private int internImpl(String value) {
        return internEncoded(value, encodeAndPad(value));
    }

    private int internEncoded(String value, byte[] padded) {
        var ws = tree.beginWrite();
        long leafRef = tree.getOrCreateWith(ws, MemorySegment.ofArray(padded), MAX_KEY_LEN, 0);
        MemorySegment leaf = tree.leafValueImpl(leafRef);
        int existing = leaf.get(ValueLayout.JAVA_INT_UNALIGNED, 0);
        if (existing != 0) {
            // Key existed — no structural change, but still commit
            // (beginWrite snapshotted; commit restores and publishes)
            tree.commitWrite(ws);
            return existing;
        }

        if (nextCode > maxCode) {
            throw new IllegalStateException("TaoDictionary exhausted (max=" + maxCode + "): " + value);
        }
        int code = nextCode++;
        leaf.set(ValueLayout.JAVA_INT_UNALIGNED, 0, code);
        reverseCache.put(code, value);
        // Commit: publish new root + size, retire old nodes
        tree.commitWrite(ws);
        return code;
    }

    private int resolveImpl(String value) {
        return resolveEncoded(encodeAndPad(value));
    }

    private int resolveEncoded(byte[] padded) {
        // Lock-free lookup via acquire on published state
        long leafRef = tree.lookupLockFree(padded, MAX_KEY_LEN);
        if (NodePtr.isEmpty(leafRef)) return -1;
        return tree.leafValueImpl(leafRef).get(ValueLayout.JAVA_INT_UNALIGNED, 0);
    }

    private static final ThreadLocal<byte[]> PAD_BUFFER =
            ThreadLocal.withInitial(() -> new byte[MAX_KEY_LEN]);

    private byte[] encodeAndPad(String value) {
        byte[] padded = PAD_BUFFER.get();
        int encodedLen = TaoKey.encodeStringInto(value, padded);
        if (encodedLen < 0) {
            throw new IllegalArgumentException(
                "Encoded string too long (> " + MAX_KEY_LEN + "): " + value);
        }
        java.util.Arrays.fill(padded, encodedLen, MAX_KEY_LEN, (byte) 0);
        return padded;
    }
}
