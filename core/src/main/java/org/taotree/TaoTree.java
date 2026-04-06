package org.taotree;

import org.taotree.internal.art.NodePtr;

import org.taotree.internal.art.Node4;
import org.taotree.internal.art.Node16;
import org.taotree.internal.art.Node48;
import org.taotree.internal.art.Node256;
import org.taotree.internal.art.NodeConstants;
import org.taotree.internal.alloc.BumpAllocator;
import org.taotree.internal.alloc.ChunkStore;
import org.taotree.internal.cow.Compactor;
import org.taotree.internal.cow.CowEngine;
import org.taotree.internal.cow.EpochReclaimer;
import org.taotree.internal.alloc.Preallocator;
import org.taotree.internal.art.PrefixNode;
import org.taotree.internal.persist.CheckpointIO;
import org.taotree.internal.persist.CheckpointV2;
import org.taotree.internal.persist.CommitRecord;
import org.taotree.internal.persist.PersistenceManager;
import org.taotree.internal.persist.ShadowPagingRecovery;
import org.taotree.internal.alloc.SlabAllocator;
import org.taotree.internal.persist.Superblock;
import org.taotree.internal.alloc.WriterArena;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import org.taotree.layout.KeyBuilder;
import org.taotree.layout.KeyField;
import org.taotree.layout.KeyHandle;
import org.taotree.layout.KeyLayout;
import org.taotree.layout.LeafField;
import org.taotree.layout.LeafHandle;
import org.taotree.layout.LeafLayout;
import org.taotree.layout.QueryBuilder;

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
 * <p>Multiple concurrent readers, one writer at a time. Readers hold the
 * read lock; writers hold the write lock. Structural mutations use COW
 * (copy-on-write) path-copy via {@link CowEngine} with deferred publication —
 * the new tree root is published atomically when the write scope closes.
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
    private final ChunkStore chunkStore; // null for in-memory mode

    // -- Persistence coordination (file-backed mode) --
    private PersistenceManager persistence; // null for in-memory mode

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

    // -- COW infrastructure --
    // Volatile root pointer for lock-free dictionary resolve via VarHandle.
    // Writers update root (non-volatile) during mutations, then publish to
    // rootPtr via setRelease in commitWrite(). Dict resolvers read rootPtr
    // via getAcquire in lookupLockFree().
    @SuppressWarnings("unused") // accessed via ROOT_HANDLE VarHandle
    private volatile long rootPtr = NodePtr.EMPTY_PTR;

    private static final VarHandle ROOT_HANDLE;
    static {
        try {
            ROOT_HANDLE = MethodHandles.lookup()
                .findVarHandle(TaoTree.class, "rootPtr", long.class);
        } catch (ReflectiveOperationException e) {
            throw new ExceptionInInitializerError(e);
        }
    }

    // Atomic size counter for lock-free dictionary size queries
    private final AtomicLong atomicSize = new AtomicLong(0);

    // Epoch-based reclaimer: deferred node freeing after COW path-copy
    private EpochReclaimer reclaimer;

    // COW engine: performs structural mutations via deferred path-copy
    private CowEngine cowEngine;

    // Tracked dictionaries (for persistence)
    private final List<TaoDictionary> dicts = new ArrayList<>();

    // Bound key layout (set by layout-based factory methods; null for raw-API usage)
    private KeyLayout boundKeyLayout;

    // Bound leaf layout (set by layout-based factory methods; null for raw-API usage)
    private LeafLayout boundLeafLayout;

    // String layouts per slab class ID (for copyFrom out-of-line string handling)
    private final java.util.Map<Integer, TaoString.Layout> stringLayouts = new java.util.HashMap<>();

    // -----------------------------------------------------------------------
    // Root tree constructor (owns arena)
    // -----------------------------------------------------------------------

    private TaoTree(int slabSize, int keyLen, int[] leafValueSizes) {
        this.arena = Arena.ofShared();
        this.slab = new SlabAllocator(arena, slabSize);
        this.bump = new BumpAllocator(arena);
        this.lock = new ReentrantReadWriteLock(true); // fair
        this.ownsArena = true;
        this.chunkStore = null;

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

        // Init epoch reclaimer + COW engine
        this.reclaimer = new EpochReclaimer(slab);
        this.cowEngine = new CowEngine(slab, reclaimer,
            prefixClassId, node4ClassId, node16ClassId, node48ClassId, node256ClassId,
            keyLen, keySlotSize, leafClassIds);
    }

    // -----------------------------------------------------------------------
    // Child tree constructor (shares parent's infrastructure)
    // -----------------------------------------------------------------------

    /**
     * Package-private: creates a child tree that shares the parent's arena,
     * slab allocator, bump allocator, lock, epoch reclaimer, and node class IDs.
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
        this.chunkStore = null; // child trees don't own the chunk store

        // Share parent's epoch reclaimer (all trees sharing a slab should share reclamation)
        this.reclaimer = parent.reclaimer;

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

    /**
     * Create a data tree from a key layout and leaf layout.
     *
     * <p>Derives the key length from the key layout and the leaf value size from the
     * leaf layout. Automatically registers {@link TaoString.Layout}s for any
     * {@link LeafField.Str} or {@link LeafField.Json} fields so that
     * {@link #copyFrom} handles out-of-line strings correctly.
     *
     * @param keyLayout  the compound key layout
     * @param leafLayout the leaf value layout
     */
    public static TaoTree open(KeyLayout keyLayout, LeafLayout leafLayout) {
        var tree = new TaoTree(SlabAllocator.DEFAULT_SLAB_SIZE,
            keyLayout.totalWidth(), new int[]{leafLayout.totalWidth()});
        registerStringLayouts(tree, leafLayout, 0);
        tree.boundKeyLayout = bindDicts(tree, keyLayout);
        tree.boundLeafLayout = leafLayout;
        return tree;
    }

    /**
     * Create a data tree that shares infrastructure with an existing tree.
     *
     * <p>The child tree shares the parent's arena, slab allocator, bump allocator,
     * and lock. This is the preferred way to create a data tree when dictionaries
     * are created from a {@link #forDictionaries()} infrastructure tree.
     *
     * @param parent     the infrastructure tree (typically from {@link #forDictionaries()})
     * @param keyLayout  the compound key layout
     * @param leafLayout the leaf value layout
     */
    public static TaoTree open(TaoTree parent, KeyLayout keyLayout, LeafLayout leafLayout) {
        var tree = new TaoTree(parent, keyLayout.totalWidth(),
            new int[]{leafLayout.totalWidth()});
        registerStringLayouts(tree, leafLayout, 0);
        tree.boundKeyLayout = bindDicts(tree, keyLayout);
        tree.boundLeafLayout = leafLayout;
        return tree;
    }

    /**
     * Create a file-backed tree from a key layout and leaf layout.
     *
     * <p>Dictionaries are created automatically for any {@code dict16}/{@code dict32}
     * fields in the key layout.
     *
     * @param path       path to the store file (must not exist)
     * @param keyLayout  the compound key layout
     * @param leafLayout the leaf value layout
     */
    public static TaoTree create(Path path, KeyLayout keyLayout,
                                 LeafLayout leafLayout) throws IOException {
        var tree = createFileBacked(path, SlabAllocator.DEFAULT_SLAB_SIZE,
            keyLayout.totalWidth(), new int[]{leafLayout.totalWidth()},
            ChunkStore.DEFAULT_CHUNK_SIZE, Preallocator.isSupported());
        registerStringLayouts(tree, leafLayout, 0);
        tree.boundKeyLayout = bindDicts(tree, keyLayout);
        tree.boundLeafLayout = leafLayout;
        return tree;
    }

    /**
     * Create a file-backed tree from a key layout and leaf layout with custom storage options.
     *
     * @param path        path to the store file (must not exist)
     * @param keyLayout   the compound key layout
     * @param leafLayout  the leaf value layout
     * @param chunkSize   chunk size in bytes (must be a multiple of 4096)
     * @param preallocate if true, use OS-specific preallocation
     */
    public static TaoTree create(Path path, KeyLayout keyLayout, LeafLayout leafLayout,
                                 long chunkSize, boolean preallocate) throws IOException {
        var tree = createFileBacked(path, SlabAllocator.DEFAULT_SLAB_SIZE,
            keyLayout.totalWidth(), new int[]{leafLayout.totalWidth()},
            chunkSize, preallocate);
        registerStringLayouts(tree, leafLayout, 0);
        tree.boundKeyLayout = bindDicts(tree, keyLayout);
        tree.boundLeafLayout = leafLayout;
        return tree;
    }

    /**
     * Open an existing file-backed tree and bind key/leaf layouts.
     *
     * <p>The layouts provide typed handle access. Dictionary fields in the key layout
     * are bound to the restored dictionaries (matched by declaration order).
     *
     * @param path       path to the existing store file
     * @param keyLayout  the key layout (must match the schema used at creation)
     * @param leafLayout the leaf value layout (must match the schema used at creation)
     */
    public static TaoTree open(Path path, KeyLayout keyLayout,
                               LeafLayout leafLayout) throws IOException {
        var tree = openFileBacked(path);
        registerStringLayouts(tree, leafLayout, 0);
        tree.boundKeyLayout = rebindDicts(tree, keyLayout);
        tree.boundLeafLayout = leafLayout;
        return tree;
    }

    /**
     * Bind restored dictionaries to a key layout's dict fields (by declaration order).
     * Used when reopening a file-backed tree — dicts already exist, we just connect them.
     */
    private static KeyLayout rebindDicts(TaoTree tree, KeyLayout keyLayout) {
        return Binding.rebindDicts(tree, keyLayout);
    }

    private static void registerStringLayouts(TaoTree tree, LeafLayout leafLayout,
                                              int leafClassIndex) {
        Binding.registerStringLayouts(tree, leafLayout, leafClassIndex);
    }

    private static KeyLayout bindDicts(TaoTree tree, KeyLayout keyLayout) {
        return Binding.bindDicts(tree, keyLayout);
    }

    // -----------------------------------------------------------------------
    // Key handle factories
    // -----------------------------------------------------------------------

    /**
     * Returns the bound key layout (with dictionaries resolved).
     *
     * @throws IllegalStateException if the tree was not created with a KeyLayout
     */
    public KeyLayout keyLayout() {
        if (boundKeyLayout == null) {
            throw new IllegalStateException("No KeyLayout bound to this tree. "
                + "Use TaoTree.open(KeyLayout, LeafLayout) to bind a layout.");
        }
        return boundKeyLayout;
    }

    /**
     * Returns the bound leaf layout.
     *
     * @throws IllegalStateException if the tree was not created with a LeafLayout
     */
    public LeafLayout leafLayout() {
        if (boundLeafLayout == null) {
            throw new IllegalStateException("No LeafLayout bound to this tree. "
                + "Use TaoTree.open(KeyLayout, LeafLayout) to bind a layout.");
        }
        return boundLeafLayout;
    }

    /**
     * Create a new {@link KeyBuilder} for this tree's key layout.
     *
     * @param arena arena for allocating the reusable key buffer
     */
    public KeyBuilder newKeyBuilder(Arena arena) {
        return new KeyBuilder(keyLayout(), arena);
    }

    /**
     * Create a new {@link QueryBuilder} for this tree's key layout.
     *
     * <p>The query builder uses resolve-only dictionary access (read lock),
     * making it safe to use inside a {@link ReadScope}.
     *
     * @param arena arena for allocating the reusable key buffer
     */
    public QueryBuilder newQueryBuilder(Arena arena) {
        return new QueryBuilder(keyLayout(), arena);
    }

    // -----------------------------------------------------------------------
    // Leaf handle factories
    // -----------------------------------------------------------------------

    /** Derive an Int8 leaf handle. */
    public LeafHandle.Int8    leafInt8(String name)    { return leafLayout().int8(name); }
    /** Derive an Int16 leaf handle. */
    public LeafHandle.Int16   leafInt16(String name)   { return leafLayout().int16(name); }
    /** Derive an Int32 leaf handle. */
    public LeafHandle.Int32   leafInt32(String name)   { return leafLayout().int32(name); }
    /** Derive an Int64 leaf handle. */
    public LeafHandle.Int64   leafInt64(String name)   { return leafLayout().int64(name); }
    /** Derive a Float32 leaf handle. */
    public LeafHandle.Float32 leafFloat32(String name) { return leafLayout().float32(name); }
    /** Derive a Float64 leaf handle. */
    public LeafHandle.Float64 leafFloat64(String name) { return leafLayout().float64(name); }
    /** Derive a String leaf handle. */
    public LeafHandle.Str     leafString(String name)  { return leafLayout().string(name); }
    /** Derive a JSON leaf handle. */
    public LeafHandle.Json    leafJson(String name)    { return leafLayout().json(name); }
    /** Derive a Dict16 leaf handle. */
    public LeafHandle.Dict16  leafDict16(String name)  { return leafLayout().dict16(name); }
    /** Derive a Dict32 leaf handle. */
    public LeafHandle.Dict32  leafDict32(String name)  { return leafLayout().dict32(name); }

    /** Derive a Dict16 key handle for the named field. */
    public KeyHandle.Dict16 keyDict16(String name) {
        var kl = keyLayout();
        int i = kl.fieldIndex(name);
        var f = kl.field(i);
        if (!(f instanceof KeyField.DictU16 d) || d.dict() == null) {
            throw new IllegalArgumentException("Field '" + name + "' is not a bound dict16 field");
        }
        return new KeyHandle.Dict16(name, kl.offset(i), i, d.dict());
    }

    /** Derive a Dict32 key handle for the named field. */
    public KeyHandle.Dict32 keyDict32(String name) {
        var kl = keyLayout();
        int i = kl.fieldIndex(name);
        var f = kl.field(i);
        if (!(f instanceof KeyField.DictU32 d) || d.dict() == null) {
            throw new IllegalArgumentException("Field '" + name + "' is not a bound dict32 field");
        }
        return new KeyHandle.Dict32(name, kl.offset(i), i, d.dict());
    }

    /** Derive a UInt32 key handle for the named field. */
    public KeyHandle.UInt32 keyUint32(String name) {
        var kl = keyLayout();
        int i = kl.fieldIndex(name);
        return new KeyHandle.UInt32(name, kl.offset(i), i);
    }

    /** Derive a UInt16 key handle for the named field. */
    public KeyHandle.UInt16 keyUint16(String name) {
        var kl = keyLayout();
        int i = kl.fieldIndex(name);
        return new KeyHandle.UInt16(name, kl.offset(i), i);
    }

    /** Derive a UInt8 key handle for the named field. */
    public KeyHandle.UInt8 keyUint8(String name) {
        var kl = keyLayout();
        int i = kl.fieldIndex(name);
        return new KeyHandle.UInt8(name, kl.offset(i), i);
    }

    /** Derive a UInt64 key handle for the named field. */
    public KeyHandle.UInt64 keyUint64(String name) {
        var kl = keyLayout();
        int i = kl.fieldIndex(name);
        return new KeyHandle.UInt64(name, kl.offset(i), i);
    }

    /** Derive an Int64 key handle for the named field. */
    public KeyHandle.Int64 keyInt64(String name) {
        var kl = keyLayout();
        int i = kl.fieldIndex(name);
        return new KeyHandle.Int64(name, kl.offset(i), i);
    }

    /** Fixed key length in bytes. */
    public int keyLen() { return keyLen; }

    /** Returns true if this tree is file-backed. */
    public boolean isFileBacked() { return chunkStore != null; }

    /**
     * Returns the dictionary at the given index.
     *
     * <p>Dictionaries are indexed in the order they were created (for new trees)
     * or restored (for reopened trees).
     *
     * @throws IndexOutOfBoundsException if the index is out of range
     */
    public TaoDictionary dictionary(int index) {
        return dicts.get(index);
    }

    /** Returns the number of tracked dictionaries. */
    public int dictionaryCount() {
        return dicts.size();
    }

    /** Returns an unmodifiable view of all tracked dictionaries. */
    public List<TaoDictionary> dictionaries() {
        return Collections.unmodifiableList(dicts);
    }

    /** Package-private: register a dictionary for persistence tracking. */
    void registerDict(TaoDictionary dict) {
        dicts.add(dict);
    }

    /**
     * Register a string layout for a leaf class.
     *
     * <p>This tells {@link #copyFrom} how to find and copy out-of-line string data
     * within leaves of the given class. Without registration, leaf values are copied
     * as raw bytes (out-of-line pointers would become dangling in the target tree).
     *
     * @param leafClassIndex the leaf class index (0-based)
     * @param layout         the string layout descriptor
     */
    public void registerStringLayout(int leafClassIndex, TaoString.Layout layout) {
        stringLayouts.put(leafClassIds[leafClassIndex], layout);
    }

    // -----------------------------------------------------------------------
    // Copy / compaction
    // -----------------------------------------------------------------------

    /**
     * Copy all live entries from the source tree into this tree.
     *
     * <p>Walks the source tree and re-inserts every leaf into this tree. For leaves
     * with registered {@link TaoString.Layout}s, out-of-line string data is copied from
     * the source's bump allocator to this tree's bump allocator and the pointer is
     * patched in the target leaf.
     *
     * <p>The source tree is not modified. This tree must have the same key length
     * and compatible leaf classes as the source.
     *
     * <p><b>Typical use (copy-vacuum / compaction):</b>
     * <pre>{@code
     * // Compact a file-backed tree into a fresh file
     * try (var compacted = TaoTree.create(newPath, tree.keyLen(), valueSize)) {
     *     compacted.registerStringLayout(0, TaoString.STRING_LAYOUT);
     *     compacted.copyFrom(tree);
     * }
     * }</pre>
     *
     * <p><b>Threading:</b> Self-locking — acquires the write lock internally.
     * The source tree should not be concurrently modified (hold a read scope on
     * it, or ensure exclusive access).
     *
     * @param source the source tree to copy from
     * @throws IllegalArgumentException if key lengths, leaf class counts, or leaf value sizes don't match
     */
    public void copyFrom(TaoTree source) {
        lock.writeLock().lock();
        try {
            var ws = beginWrite();
            copyFromImpl(source, ws);
            commitWrite(ws);
        } finally {
            lock.writeLock().unlock();
        }
    }

    /**
     * Package-private implementation of copyFrom with explicit WriteState.
     */
    void copyFromImpl(TaoTree source, WriteState ws) {
        var copier = new Copier(source, this, ws);
        copier.validate();
        copier.copy();
    }

    /**
     * Package-private implementation of copyFrom. Creates own WriteState.
     */
    void copyFromImpl(TaoTree source) {
        var ws = beginWrite();
        copyFromImpl(source, ws);
        commitWrite(ws);
    }

    // -----------------------------------------------------------------------
    // File-backed factory methods
    // -----------------------------------------------------------------------

    /**
     * Create a new file-backed tree with a single leaf class.
     *
     * @param path      path to the store file (must not exist)
     * @param keyLen    fixed key length in bytes
     * @param valueSize size of the leaf value region in bytes
     */
    public static TaoTree create(Path path, int keyLen, int valueSize) throws IOException {
        if (keyLen <= 0) throw new IllegalArgumentException("keyLen must be positive: " + keyLen);
        return createFileBacked(path, SlabAllocator.DEFAULT_SLAB_SIZE, keyLen, new int[]{valueSize},
            ChunkStore.DEFAULT_CHUNK_SIZE, Preallocator.isSupported());
    }

    /**
     * Create a new file-backed tree with multiple leaf classes.
     *
     * @param path           path to the store file (must not exist)
     * @param keyLen         fixed key length in bytes
     * @param leafValueSizes value sizes for each leaf class
     */
    public static TaoTree create(Path path, int keyLen, int[] leafValueSizes) throws IOException {
        if (keyLen <= 0) throw new IllegalArgumentException("keyLen must be positive: " + keyLen);
        if (leafValueSizes == null || leafValueSizes.length == 0) {
            throw new IllegalArgumentException("At least one leaf value size required");
        }
        return createFileBacked(path, SlabAllocator.DEFAULT_SLAB_SIZE, keyLen, leafValueSizes,
            ChunkStore.DEFAULT_CHUNK_SIZE, Preallocator.isSupported());
    }

    /**
     * Create a new file-backed tree with a single leaf class and custom storage options.
     *
     * @param path        path to the store file (must not exist)
     * @param keyLen      fixed key length in bytes
     * @param valueSize   size of the leaf value region in bytes
     * @param chunkSize   chunk size in bytes (must be a multiple of 4096)
     * @param preallocate if true, use OS-specific preallocation to reserve physical blocks
     */
    public static TaoTree create(Path path, int keyLen, int valueSize,
                                 long chunkSize, boolean preallocate) throws IOException {
        if (keyLen <= 0) throw new IllegalArgumentException("keyLen must be positive: " + keyLen);
        return createFileBacked(path, SlabAllocator.DEFAULT_SLAB_SIZE, keyLen, new int[]{valueSize},
            chunkSize, preallocate);
    }

    /**
     * Open an existing file-backed tree.
     *
     * @param path path to the existing store file
     * @return the restored tree with all data intact
     */
    public static TaoTree open(Path path) throws IOException {
        return openFileBacked(path);
    }

    private static TaoTree createFileBacked(Path path, int slabSize, int keyLen,
                                            int[] leafValueSizes,
                                            long chunkSize, boolean preallocate) throws IOException {
        var arena = Arena.ofShared();
        var cs = ChunkStore.createV2(path, arena, chunkSize, preallocate);
        var slab = new SlabAllocator(arena, cs, slabSize);
        var bump = new BumpAllocator(arena, cs, BumpAllocator.DEFAULT_PAGE_SIZE);

        var tree = new TaoTree(arena, slab, bump, cs, keyLen, leafValueSizes);
        tree.persistence = new PersistenceManager(cs);
        // Write initial v2 checkpoint to slot A
        tree.persistence.writeCheckpoint(tree.gatherMetadata());
        // Reserve a page for the first commit record (at nextPage, so recovery finds it)
        tree.persistence.reserveNextCommitPage();
        return tree;
    }

    private static TaoTree openFileBacked(Path path) throws IOException {
        var arena = Arena.ofShared();

        // Open chunk store to read checkpoint slots.
        var channel = java.nio.channels.FileChannel.open(path,
            java.nio.file.StandardOpenOption.READ,
            java.nio.file.StandardOpenOption.WRITE);
        long fileSize = channel.size();
        channel.close();

        if (fileSize < (long) ChunkStore.V2_RESERVED_PAGES * ChunkStore.PAGE_SIZE) {
            throw new IOException("File too small to contain v2 checkpoint: " + path);
        }

        int totalPages = (int) (fileSize / ChunkStore.PAGE_SIZE);
        var cs = ChunkStore.open(path, arena, ChunkStore.DEFAULT_CHUNK_SIZE, totalPages, totalPages);

        // Read v2 mirrored checkpoints
        MemorySegment slotA = cs.checkpointSlotA();
        MemorySegment slotB = cs.checkpointSlotB();
        var cp = CheckpointV2.chooseBest(slotA, slotB);
        if (cp == null) {
            throw new IOException("No valid v2 checkpoint found in " + path);
        }
        var data = CheckpointIO.fromCheckpoint(cp);
        long cpGeneration = cp.generation;
        int cpActiveSlot = (cp.slotId == 0)
            ? CheckpointV2.SLOT_A_PAGE
            : CheckpointV2.SLOT_B_PAGE;

        // Scan forward for commit records written since this checkpoint
        var recovered = ShadowPagingRecovery.recover(
            cs,
            cpGeneration,
            data.trees.length > 0 ? data.trees[0].root : NodePtr.EMPTY_PTR,
            data.trees.length > 0 ? data.trees[0].size : 0,
            data.dicts.length,
            PersistenceManager.dictRootsFromData(data),
            PersistenceManager.dictNextCodesFromData(data),
            PersistenceManager.dictSizesFromData(data),
            data.nextPage,
            totalPages);

        // Apply recovered state (may update root, size, dict state, nextPage)
        if (recovered.generation > cpGeneration) {
            if (data.trees.length > 0) {
                data.trees[0].root = recovered.primaryRoot;
                data.trees[0].size = recovered.primarySize;
            }
            for (int i = 0; i < Math.min(recovered.dictionaryCount, data.dicts.length); i++) {
                if (1 + i < data.trees.length) {
                    data.trees[1 + i].root = recovered.dictRoots[i];
                    data.trees[1 + i].size = recovered.dictSizes[i];
                }
            }
            cpGeneration = recovered.generation;
        }

        // Reopen ChunkStore with the correct nextPage (from checkpoint or recovery)
        cs.close();
        cs = ChunkStore.open(path, arena, data.chunkSize,
            Math.max(data.totalPages, recovered.nextPage), recovered.nextPage);

        // Restore allocators
        var slab = new SlabAllocator(arena, cs, data.slabSize);
        for (var clsDesc : data.classes) {
            slab.restoreClass(clsDesc);
        }

        var bump = new BumpAllocator(arena, cs, data.bumpPageSize);
        for (int i = 0; i < data.bumpPageLocations.length; i++) {
            int sizeInPages = (i < data.bumpPageSizes.length && data.bumpPageSizes[i] > 0)
                ? data.bumpPageSizes[i]
                : data.bumpPageSize / ChunkStore.PAGE_SIZE;
            bump.restorePage(data.bumpPageLocations[i], sizeInPages);
        }
        bump.restoreState(data.bumpCurrentPage, data.bumpOffset, data.bumpBytesAllocated);

        // Restore the primary tree
        if (data.trees.length == 0) {
            throw new IOException("No tree descriptors found in checkpoint");
        }
        var treeDesc = data.trees[0];
        var tree = new TaoTree(arena, slab, bump, cs, treeDesc);
        tree.persistence = new PersistenceManager(cs, cpGeneration, cpActiveSlot);

        // Restore dictionaries
        for (var dictDesc : data.dicts) {
            var childTreeDesc = data.trees[dictDesc.treeIndex];
            var childTree = new TaoTree(tree, childTreeDesc);
            var dict = new TaoDictionary(tree, childTree, dictDesc.maxCode, dictDesc.nextCode);
            tree.dicts.add(dict);
        }

        // Reserve the next commit record page for shadow paging
        tree.persistence.reserveNextCommitPage();

        return tree;
    }

    // -----------------------------------------------------------------------
    // File-backed constructor (create new)
    // -----------------------------------------------------------------------

    private TaoTree(Arena arena, SlabAllocator slab, BumpAllocator bump,
                    ChunkStore chunkStore, int keyLen, int[] leafValueSizes) {
        this.arena = arena;
        this.slab = slab;
        this.bump = bump;
        this.lock = new ReentrantReadWriteLock(true);
        this.ownsArena = true;
        this.chunkStore = chunkStore;

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

        // COW always active: init epoch reclaimer + COW engine
        this.reclaimer = new EpochReclaimer(slab);
        var writerArena = new WriterArena(chunkStore);
        this.cowEngine = new CowEngine(slab, reclaimer, writerArena, chunkStore,
            prefixClassId, node4ClassId, node16ClassId, node48ClassId, node256ClassId,
            keyLen, keySlotSize, leafClassIds);
    }

    // -----------------------------------------------------------------------
    // File-backed constructor (restore from superblock)
    // -----------------------------------------------------------------------

    private TaoTree(Arena arena, SlabAllocator slab, BumpAllocator bump,
                    ChunkStore chunkStore, Superblock.TreeDescriptor desc) {
        this.arena = arena;
        this.slab = slab;
        this.bump = bump;
        this.lock = new ReentrantReadWriteLock(true);
        this.ownsArena = true;
        this.chunkStore = chunkStore;

        this.prefixClassId  = desc.prefixClassId;
        this.node4ClassId   = desc.node4ClassId;
        this.node16ClassId  = desc.node16ClassId;
        this.node48ClassId  = desc.node48ClassId;
        this.node256ClassId = desc.node256ClassId;

        this.keyLen = desc.keyLen;
        this.keySlotSize = keyLen > 0 ? (keyLen + 7) & ~7 : 0;
        this.leafValueSizes = desc.leafValueSizes.clone();
        this.leafClassCount = leafValueSizes.length;
        this.leafClassIds = desc.leafClassIds.clone();

        this.root = desc.root;
        this.size = desc.size;

        // COW always active: init epoch reclaimer + COW engine
        this.reclaimer = new EpochReclaimer(slab);
        var writerArena = new WriterArena(chunkStore);
        this.cowEngine = new CowEngine(slab, reclaimer, writerArena, chunkStore,
            prefixClassId, node4ClassId, node16ClassId, node48ClassId, node256ClassId,
            keyLen, keySlotSize, leafClassIds);
        ROOT_HANDLE.setRelease(this, root);
        atomicSize.set(size);
    }

    // -----------------------------------------------------------------------
    // Child tree restore constructor (shares parent's infrastructure)
    // -----------------------------------------------------------------------

    /**
     * Package-private: restore a child tree from a superblock descriptor.
     * Shares the parent's arena, slab, bump, lock, and epoch reclaimer.
     * Used to reconstruct TaoDictionary child trees on reopen.
     */
    TaoTree(TaoTree parent, Superblock.TreeDescriptor desc) {
        this.arena = parent.arena;
        this.slab = parent.slab;
        this.bump = parent.bump;
        this.lock = parent.lock;
        this.ownsArena = false;
        this.chunkStore = null;

        // Share parent's epoch reclaimer (all trees sharing a slab should share reclamation)
        this.reclaimer = parent.reclaimer;

        this.prefixClassId  = desc.prefixClassId;
        this.node4ClassId   = desc.node4ClassId;
        this.node16ClassId  = desc.node16ClassId;
        this.node48ClassId  = desc.node48ClassId;
        this.node256ClassId = desc.node256ClassId;

        this.keyLen = desc.keyLen;
        this.keySlotSize = keyLen > 0 ? (keyLen + 7) & ~7 : 0;
        this.leafValueSizes = desc.leafValueSizes.clone();
        this.leafClassCount = leafValueSizes.length;
        this.leafClassIds = desc.leafClassIds.clone();

        this.root = desc.root;
        this.size = desc.size;
        // Publish root for dict resolve (lock-free via rootPtr acquire)
        ROOT_HANDLE.setRelease(this, root);
        atomicSize.set(size);
    }

    // -----------------------------------------------------------------------
    // COW mode
    // -----------------------------------------------------------------------

    /** Returns the epoch reclaimer. */
    EpochReclaimer reclaimer() { return reclaimer; }

    /** Package-private: publish root and size for dict resolvers (via rootPtr/atomicSize). */
    void publishRoot() {
        ROOT_HANDLE.setRelease(this, root);
        atomicSize.set(size);
    }

    // -----------------------------------------------------------------------
    // Lock-free lookup (COW mode)
    // -----------------------------------------------------------------------

    /**
     * Lock-free point lookup. Uses one acquire on the root pointer, then
     * plain loads for the entire traversal.
     */
    long lookupLockFree(MemorySegment key, int keyLen) {
        long node = (long) ROOT_HANDLE.getAcquire(this);
        int depth = 0;

        while (!NodePtr.isEmpty(node)) {
            int type = NodePtr.nodeType(node);

            if (type == NodePtr.LEAF) {
                return leafKeyMatches(node, key, keyLen) ? node : NodePtr.EMPTY_PTR;
            }

            if (type == NodePtr.PREFIX) {
                MemorySegment prefSeg = resolveNode(node);
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

    /**
     * Compact the tree: walk all live nodes in post-order, copy into packed
     * slab layout, publish the new root, write a full checkpoint, and advance
     * {@code durableGeneration} to enable reclamation of old arena pages.
     *
     * <p>This is a blocking operation that acquires the write lock.
     *
     * @throws IOException if an I/O error occurs during checkpoint write
     */
    public void compact() throws IOException {
        if (persistence == null) return;
        lock.writeLock().lock();
        try {
            long currentRoot = root;
            if (NodePtr.isEmpty(currentRoot)) return;

            var compactor = new Compactor(slab, bump, reclaimer, chunkStore,
                prefixClassId, node4ClassId, node16ClassId, node48ClassId, node256ClassId,
                keyLen, keySlotSize, leafClassIds, leafValueSizes);

            var result = compactor.compact(currentRoot);

            // Publish the compacted root
            root = result.newRoot();
            publishRoot();

            // Write a full checkpoint with the compacted state
            persistence.writeCheckpoint(gatherMetadata());
            persistence.resetCommitCount();
            chunkStore.sync();

            // Advance durable generation — retired nodes older than this can now be freed
            if (reclaimer != null) {
                reclaimer.advanceDurableGeneration(persistence.generation());
                reclaimer.reclaim();
            }
        } finally {
            lock.writeLock().unlock();
        }
    }

    // -----------------------------------------------------------------------
    // Lock operations (package-private, used by TaoDictionary)
    // -----------------------------------------------------------------------

    boolean isWriteLockedByCurrentThread() {
        return lock.isWriteLockedByCurrentThread();
    }

    // -----------------------------------------------------------------------
    // Lifecycle
    // -----------------------------------------------------------------------

    /**
     * Flush all data to disk (file-backed mode only).
     *
     * <p>Writes a lightweight commit record (one page) and forces all mapped
     * regions. Every {@value #COMMITS_PER_CHECKPOINT} commits, also writes a
     * full mirrored checkpoint to bound recovery time and advances
     * {@code durableGeneration} for epoch reclamation. No-op for in-memory trees.
     *
     * @throws IOException if an I/O error occurs during sync
     */
    public void sync() throws IOException {
        if (persistence == null) return;
        lock.writeLock().lock();
        try {
            persistence.writeCommitRecord(buildCommitData());
            if (persistence.shouldCheckpoint()) {
                persistence.writeCheckpoint(gatherMetadata());
                persistence.resetCommitCount();
                chunkStore.sync();
                if (reclaimer != null) {
                    reclaimer.advanceDurableGeneration(persistence.generation());
                    reclaimer.reclaim();
                }
            } else {
                chunkStore.sync();
            }
        } finally {
            lock.writeLock().unlock();
        }
    }

    /**
     * Close this tree, releasing the underlying arena if this is a root tree.
     * For file-backed trees, writes a full checkpoint and syncs before closing.
     * Child trees (created internally by dictionaries) do not close the arena.
     */
    @Override
    public void close() {
        if (ownsArena) {
            if (persistence != null) {
                try {
                    persistence.writeCheckpoint(gatherMetadata());
                    chunkStore.sync();
                    if (reclaimer != null) {
                        reclaimer.advanceDurableGeneration(persistence.generation());
                        reclaimer.reclaim();
                    }
                    chunkStore.close();
                } catch (IOException e) {
                    throw new UncheckedIOException("Failed to sync/close chunk store", e);
                }
            }
            if (reclaimer != null) {
                reclaimer.close();
            }
            arena.close();
        }
    }

    // -----------------------------------------------------------------------
    // Persistence (package-private)
    // -----------------------------------------------------------------------

    /** Package-private: export a tree descriptor for the superblock. */
    Superblock.TreeDescriptor exportTreeDescriptor() {
        var desc = new Superblock.TreeDescriptor();
        desc.root = root;
        desc.size = size;
        desc.keyLen = keyLen;
        desc.prefixClassId = prefixClassId;
        desc.node4ClassId = node4ClassId;
        desc.node16ClassId = node16ClassId;
        desc.node48ClassId = node48ClassId;
        desc.node256ClassId = node256ClassId;
        desc.leafValueSizes = leafValueSizes.clone();
        desc.leafClassIds = leafClassIds.clone();
        return desc;
    }

    /**
     * Gather all tree metadata into a {@link Superblock.SuperblockData} structure.
     * Called by {@link PersistenceManager#writeCheckpoint}.
     */
    private Superblock.SuperblockData gatherMetadata() {
        var data = new Superblock.SuperblockData();
        data.slabSize = slab.slabSize();
        data.bumpPageSize = bump.pageSize();
        data.chunkSize = chunkStore.chunkSize();
        data.totalPages = chunkStore.totalPages();
        data.nextPage = chunkStore.nextPage();

        // Slab classes
        data.classes = new Superblock.SlabClassDescriptor[slab.classCount()];
        for (int i = 0; i < slab.classCount(); i++) {
            data.classes[i] = slab.exportClass(i);
        }

        // Bump allocator
        data.bumpPageCount = bump.pageCount();
        data.bumpCurrentPage = bump.currentPage();
        data.bumpOffset = bump.bumpOffset();
        data.bumpBytesAllocated = bump.bytesAllocated();
        data.bumpPageLocations = bump.exportPageLocations();
        data.bumpPageSizes = bump.exportPageSizes();

        // Tree descriptors: index 0 = primary tree, index 1+ = child trees for dicts
        int treeCount = 1 + dicts.size();
        data.trees = new Superblock.TreeDescriptor[treeCount];
        data.trees[0] = exportTreeDescriptor();
        for (int i = 0; i < dicts.size(); i++) {
            data.trees[1 + i] = dicts.get(i).childTree().exportTreeDescriptor();
        }

        // Dictionary descriptors
        data.dicts = new Superblock.DictDescriptor[dicts.size()];
        for (int i = 0; i < dicts.size(); i++) {
            data.dicts[i] = dicts.get(i).exportDescriptor(1 + i);
        }

        return data;
    }

    /**
     * Build a commit record data snapshot from current tree + dict state.
     * The {@link PersistenceManager} fills in generation, prev-page, and arena pages.
     */
    private CommitRecord.CommitData buildCommitData() {
        var cd = new CommitRecord.CommitData();
        cd.primaryRoot = root;
        cd.primarySize = size;
        cd.dictionaryCount = dicts.size();
        cd.dictRoots = new long[dicts.size()];
        cd.dictNextCodes = new long[dicts.size()];
        cd.dictSizes = new long[dicts.size()];
        for (int i = 0; i < dicts.size(); i++) {
            var dict = dicts.get(i);
            var childTree = dict.childTree();
            cd.dictRoots[i] = childTree.root;
            cd.dictNextCodes[i] = dict.nextCode();
            cd.dictSizes[i] = childTree.size;
        }
        return cd;
    }

    // -----------------------------------------------------------------------
    // Scoped access — the primary public API
    // -----------------------------------------------------------------------

    /**
     * Acquire a read scope. Lock-free: captures a snapshot of the current root
     * via acquire semantics and enters an epoch to protect against premature
     * node reclamation. Multiple readers can be active simultaneously without
     * blocking writers.
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
        return new ReadScope();
    }

    /**
     * Acquire a write scope. Acquires the write lock (serializes writers).
     *
     * <p>Use within try-with-resources:
     * <pre>{@code
     * try (var w = tree.write()) {
     *     long leaf = w.getOrCreate(key, 0);
     *     w.leafValue(leaf).set(ValueLayout.JAVA_LONG, 0, value);
     * }
     * }</pre>
     */
    public WriteScope write() {
        lock.writeLock().lock();
        ensureCowEngine();
        return new WriteScope();
    }

    /** Lazy-init CowEngine + reclaimer for child trees on first write(). */
    private void ensureCowEngine() {
        if (cowEngine != null) return;
        if (reclaimer == null) {
            reclaimer = new EpochReclaimer(slab);
        }
        cowEngine = new CowEngine(slab, reclaimer,
            prefixClassId, node4ClassId, node16ClassId, node48ClassId, node256ClassId,
            keyLen, keySlotSize, leafClassIds);
    }

    // -----------------------------------------------------------------------
    // ReadScope — read-only scoped access
    // -----------------------------------------------------------------------

    /**
     * Read-only scope of the TaoTree. Lock-free: holds a snapshot of the root
     * pointer (captured via acquire semantics) and an epoch slot that protects
     * referenced nodes from premature reclamation.
     *
     * <p>All returned leaf references and {@link MemorySegment} slices are
     * valid only while this scope is open.
     */
    public final class ReadScope implements AutoCloseable {

        private boolean closed;
        private final Thread ownerThread = Thread.currentThread();
        private final long snapshotRoot;
        private final int epochSlot;

        private ReadScope() {
            this.epochSlot = reclaimer.enterEpoch();
            this.snapshotRoot = (long) ROOT_HANDLE.getAcquire(TaoTree.this);
        }

        /**
         * Point lookup.
         * @return a leaf reference, or {@link TaoTree#NOT_FOUND} if not found
         */
        public long lookup(MemorySegment key, int keyLen) {
            checkAccess();
            validateKeyLen(keyLen);
            return lookupFrom(snapshotRoot, key, keyLen);
        }

        /** Point lookup (uses the tree's key length). */
        public long lookup(MemorySegment key) {
            checkAccess();
            return lookupFrom(snapshotRoot, key, TaoTree.this.keyLen);
        }

        public long lookup(byte[] key) {
            checkAccess();
            validateKeyLen(key.length);
            return lookupFrom(snapshotRoot, MemorySegment.ofArray(key), key.length);
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

        public long size() {
            checkAccess();
            return atomicSize.get();
        }
        public boolean isEmpty() {
            checkAccess();
            return atomicSize.get() == 0;
        }

        /**
         * Get a typed, read-only accessor for a leaf value.
         *
         * @param leafPtr the leaf pointer (from {@link #lookup})
         * @param layout  the leaf value layout
         * @return a read-only {@link LeafAccessor}
         */
        public LeafAccessor leaf(long leafPtr, LeafLayout layout) {
            return new LeafAccessor(leafValue(leafPtr), layout, TaoTree.this);
        }

        /**
         * Point lookup returning a typed, read-only accessor.
         *
         * @param key    the key (uses the tree's key length)
         * @param layout the leaf value layout
         * @return a read-only {@link LeafAccessor}, or {@code null} if not found
         */
        public LeafAccessor lookup(MemorySegment key, LeafLayout layout) {
            checkAccess();
            long ptr = lookupFrom(snapshotRoot, key, TaoTree.this.keyLen);
            if (ptr == NOT_FOUND) return null;
            return new LeafAccessor(leafValueImpl(ptr).asReadOnly(), layout, TaoTree.this);
        }

        /**
         * Point lookup from a {@link KeyBuilder}, returning a typed, read-only accessor.
         *
         * @param kb     the key builder (must have all fields set)
         * @param layout the leaf value layout
         * @return a read-only {@link LeafAccessor}, or {@code null} if not found
         */
        public LeafAccessor lookup(KeyBuilder kb, LeafLayout layout) {
            return lookup(kb.key(), layout);
        }

        /**
         * Point lookup from a {@link KeyBuilder}, using the tree's bound leaf layout.
         *
         * @param kb the key builder (must have all fields set)
         * @return a read-only {@link LeafAccessor}, or {@code null} if not found
         */
        public LeafAccessor lookup(KeyBuilder kb) {
            return lookup(kb.key(), leafLayout());
        }

        // -- Scan APIs --

        /**
         * Scan all leaves in lexicographic key order.
         *
         * <p>The {@link LeafAccessor} passed to the visitor is reusable — valid only
         * during the callback. Do not retain references.
         *
         * @param visitor receives each leaf; return {@code false} to stop early
         * @return {@code true} if all leaves were visited
         */
        public boolean forEach(LeafVisitor visitor) {
            checkAccess();
            var accessor = new LeafAccessor(null, boundLeafLayout, TaoTree.this);
            return walkLeaves(snapshotRoot, leafPtr -> {
                accessor.rebind(leafValueImpl(leafPtr).asReadOnly());
                return visitor.visit(accessor);
            });
        }

        /**
         * Prefix scan: iterate all leaves whose key matches the prefix defined by
         * the query builder up to (and including) the given handle's field.
         *
         * <p>If the query is unsatisfiable (unknown dict value, unset field),
         * returns immediately without traversal.
         *
         * @param qb    the query builder with prefix fields set
         * @param upTo  the last key handle in the prefix
         * @param visitor receives each matching leaf; return {@code false} to stop early
         * @return {@code true} if all matching leaves were visited
         */
        public boolean scan(QueryBuilder qb, KeyHandle upTo, LeafVisitor visitor) {
            checkAccess();
            if (!qb.isSatisfiable(upTo)) return true; // unsatisfiable → empty
            int prefixLen = qb.prefixLength(upTo);
            var accessor = new LeafAccessor(null, boundLeafLayout, TaoTree.this);
            return walkPrefixed(snapshotRoot, qb.key(), prefixLen, leafPtr -> {
                accessor.rebind(leafValueImpl(leafPtr).asReadOnly());
                return visitor.visit(accessor);
            });
        }

        /**
         * Low-level prefix scan with raw prefix bytes.
         *
         * @param prefix    the prefix bytes
         * @param prefixLen number of prefix bytes to match
         * @param visitor   receives each matching leaf; return {@code false} to stop early
         * @return {@code true} if all matching leaves were visited
         */
        public boolean scan(MemorySegment prefix, int prefixLen, LeafVisitor visitor) {
            checkAccess();
            var accessor = new LeafAccessor(null, boundLeafLayout, TaoTree.this);
            return walkPrefixed(snapshotRoot, prefix, prefixLen, leafPtr -> {
                accessor.rebind(leafValueImpl(leafPtr).asReadOnly());
                return visitor.visit(accessor);
            });
        }

        /** Package-private: access the underlying tree (for WriteScope.copyFrom). */
        TaoTree tree() { return TaoTree.this; }

        @Override
        public void close() {
            if (!closed) {
                checkThread();
                closed = true;
                reclaimer.exitEpoch(epochSlot);
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
     * Read-write scope of the TaoTree. Holds the write lock to serialize
     * writers. Mutations operate on a private {@link WriteState} snapshot;
     * the final root is committed back and published to readers on close.
     *
     * <p>All returned leaf references and {@link MemorySegment} slices are
     * valid only while this scope is open.
     */
    public final class WriteScope implements AutoCloseable {

        private boolean closed;
        private final Thread ownerThread = Thread.currentThread();
        private final WriteState ws;

        private WriteScope() {
            this.ws = new WriteState(root, size);
        }

        // -- Read operations (writer sees its own writes via workingRoot) --

        public long lookup(MemorySegment key, int keyLen) {
            checkAccess();
            validateKeyLen(keyLen);
            return lookupFrom(ws.root, key, keyLen);
        }

        /** Point lookup (uses the tree's key length). */
        public long lookup(MemorySegment key) {
            checkAccess();
            return lookupFrom(ws.root, key, TaoTree.this.keyLen);
        }

        public long lookup(byte[] key) {
            checkAccess();
            validateKeyLen(key.length);
            return lookupFrom(ws.root, MemorySegment.ofArray(key), key.length);
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

        public long size() {
            checkAccess();
            return ws.size;
        }
        public boolean isEmpty() {
            checkAccess();
            return ws.size == 0;
        }

        /**
         * Get a typed, writable accessor for a leaf value.
         *
         * @param leafPtr the leaf pointer (from {@link #lookup} or {@link #getOrCreate})
         * @param layout  the leaf value layout
         * @return a writable {@link LeafAccessor}
         */
        public LeafAccessor leaf(long leafPtr, LeafLayout layout) {
            return new LeafAccessor(leafValue(leafPtr), layout, TaoTree.this);
        }

        /**
         * Insert or lookup, returning a typed, writable accessor.
         *
         * <p>Uses the tree's key length and the default leaf class (0).
         *
         * @param key    the key
         * @param layout the leaf value layout
         * @return a writable {@link LeafAccessor} (existing or newly created with zeroed fields)
         */
        public LeafAccessor getOrCreate(MemorySegment key, LeafLayout layout) {
            checkAccess();
            long ptr = getOrCreateWith(ws, key, TaoTree.this.keyLen, 0);
            return new LeafAccessor(leafValueImpl(ptr), layout, TaoTree.this);
        }

        /**
         * Insert or lookup from a {@link KeyBuilder}, returning a typed, writable accessor.
         *
         * @param kb     the key builder (must have all fields set)
         * @param layout the leaf value layout
         * @return a writable {@link LeafAccessor} (existing or newly created with zeroed fields)
         */
        public LeafAccessor getOrCreate(KeyBuilder kb, LeafLayout layout) {
            return getOrCreate(kb.key(), layout);
        }

        /**
         * Insert or lookup from a {@link KeyBuilder}, using the tree's bound leaf layout.
         *
         * @param kb the key builder (must have all fields set)
         * @return a writable {@link LeafAccessor} (existing or newly created with zeroed fields)
         */
        public LeafAccessor getOrCreate(KeyBuilder kb) {
            return getOrCreate(kb.key(), leafLayout());
        }

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
            return getOrCreateWith(ws, key, keyLen, leafClass);
        }

        /** Insert or lookup using the tree's key length and the default leaf class (0). */
        public long getOrCreate(MemorySegment key) {
            checkAccess();
            return getOrCreateWith(ws, key, TaoTree.this.keyLen, 0);
        }

        public long getOrCreate(byte[] key, int leafClass) {
            checkAccess();
            validateKeyLen(key.length);
            validateLeafClass(leafClass);
            return getOrCreateWith(ws, MemorySegment.ofArray(key), key.length, leafClass);
        }

        /** Insert or lookup using the default leaf class (0). */
        public long getOrCreate(byte[] key) {
            checkAccess();
            validateKeyLen(key.length);
            return getOrCreateWith(ws, MemorySegment.ofArray(key), key.length, 0);
        }

        /**
         * Delete a key.
         * @return true if the key existed and was removed
         */
        public boolean delete(MemorySegment key, int keyLen) {
            checkAccess();
            validateKeyLen(keyLen);
            return deleteWith(ws, key, keyLen);
        }

        public boolean delete(byte[] key) {
            checkAccess();
            validateKeyLen(key.length);
            return deleteWith(ws, MemorySegment.ofArray(key), key.length);
        }

        /**
         * Copy all live entries from the source tree (held by a read scope) into this tree.
         *
         * @param sourceScope a read scope on the source tree
         * @see TaoTree#copyFrom(TaoTree)
         */
        public void copyFrom(ReadScope sourceScope) {
            checkAccess();
            sourceScope.checkAccess();
            TaoTree.this.copyFromImpl(sourceScope.tree(), ws);
        }

        @Override
        public void close() {
            if (!closed) {
                checkThread();
                closed = true;
                commitWrite(ws);
                lock.writeLock().unlock();
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
    // Internal: ordered subtree traversal (Layer 0)
    // -----------------------------------------------------------------------

    /**
     * Walk all leaves under the given node in lexicographic key order.
     *
     * @param nodePtr the subtree root
     * @param visitor receives raw leaf pointers; return false to stop early
     * @return true if all leaves were visited, false if stopped early
     */
    private boolean walkLeaves(long nodePtr, java.util.function.LongPredicate visitor) {
        if (NodePtr.isEmpty(nodePtr)) return true;
        int type = NodePtr.nodeType(nodePtr);

        if (type == NodePtr.LEAF) {
            return visitor.test(nodePtr);
        }

        if (type == NodePtr.PREFIX) {
            MemorySegment seg = resolveNode(nodePtr);
            return walkLeaves(PrefixNode.child(seg), visitor);
        }

        MemorySegment seg = resolveNode(nodePtr);
        return switch (type) {
            case NodePtr.NODE_4 -> {
                int n = Node4.count(seg);
                boolean cont = true;
                for (int i = 0; i < n && cont; i++) {
                    cont = walkLeaves(Node4.childAt(seg, i), visitor);
                }
                yield cont;
            }
            case NodePtr.NODE_16 -> {
                int n = Node16.count(seg);
                boolean cont = true;
                for (int i = 0; i < n && cont; i++) {
                    cont = walkLeaves(Node16.childAt(seg, i), visitor);
                }
                yield cont;
            }
            case NodePtr.NODE_48 -> {
                boolean[] cont = {true};
                Node48.forEach(seg, (k, child) -> {
                    if (cont[0]) cont[0] = walkLeaves(child, visitor);
                });
                yield cont[0];
            }
            case NodePtr.NODE_256 -> {
                boolean[] cont = {true};
                Node256.forEach(seg, (k, child) -> {
                    if (cont[0]) cont[0] = walkLeaves(child, visitor);
                });
                yield cont[0];
            }
            default -> true;
        };
    }

    /**
     * Descend the tree matching a prefix, then walk all leaves under the matching subtree.
     *
     * <p>Handles prefix-compressed nodes: if the prefix bytes span across or end inside
     * a compressed prefix node, the walker correctly finds the subtree.
     *
     * @param startRoot the root pointer to start traversal from
     * @param prefix    the prefix bytes to match
     * @param prefixLen number of prefix bytes
     * @param visitor   receives raw leaf pointers; return false to stop early
     * @return true if all matching leaves were visited, false if stopped early
     */
    private boolean walkPrefixed(long startRoot, MemorySegment prefix, int prefixLen,
                                 java.util.function.LongPredicate visitor) {
        if (NodePtr.isEmpty(startRoot)) return true;
        if (prefixLen == 0) return walkLeaves(startRoot, visitor);

        long node = startRoot;
        int depth = 0;

        while (depth < prefixLen) {
            if (NodePtr.isEmpty(node)) return true; // no match
            int type = NodePtr.nodeType(node);

            if (type == NodePtr.LEAF) {
                // Leaf: check if its key matches the prefix
                MemorySegment leafSeg = resolveNode(node);
                for (int i = depth; i < prefixLen; i++) {
                    if (leafSeg.get(ValueLayout.JAVA_BYTE, i) !=
                        prefix.get(ValueLayout.JAVA_BYTE, i)) {
                        return true; // prefix doesn't match this leaf
                    }
                }
                return visitor.test(node);
            }

            if (type == NodePtr.PREFIX) {
                MemorySegment seg = resolveNode(node);
                int prefLen = PrefixNode.count(seg);
                // Compare prefix bytes against compressed prefix bytes
                int toMatch = Math.min(prefLen, prefixLen - depth);
                for (int i = 0; i < toMatch; i++) {
                    if (PrefixNode.keyAt(seg, i) !=
                        prefix.get(ValueLayout.JAVA_BYTE, depth + i)) {
                        return true; // mismatch in compressed prefix
                    }
                }
                depth += prefLen;
                node = PrefixNode.child(seg);
                continue;
            }

            // Inner node: look up the child for the next prefix byte
            byte keyByte = prefix.get(ValueLayout.JAVA_BYTE, depth);
            MemorySegment seg = resolveNode(node);
            long child = switch (type) {
                case NodePtr.NODE_4 -> Node4.findChild(seg, keyByte);
                case NodePtr.NODE_16 -> Node16.findChild(seg, keyByte);
                case NodePtr.NODE_48 -> Node48.findChild(seg, keyByte);
                case NodePtr.NODE_256 -> Node256.findChild(seg, keyByte);
                default -> NodePtr.EMPTY_PTR;
            };
            if (NodePtr.isEmpty(child)) return true; // no match
            depth++;
            node = child;
        }

        // Fully matched the prefix — walk all leaves under this subtree
        return walkLeaves(node, visitor);
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
    // WriteState — writer-private root + size, separate from published state
    // =======================================================================

    /**
     * Mutable holder for the writer-private tree root and size.
     *
     * <p>Created by {@link WriteScope} at open time (snapshot of published state).
     * All mutations operate on these fields. On {@link WriteScope#close()}, the
     * final values are committed back to the published {@code root}/{@code size}
     * fields and made visible to readers via {@link #publishRoot()}.
     *
     * <p>Retirees are nodes from the published tree that were replaced by COW
     * path-copies. They are retired via the epoch reclaimer after publication
     * so that readers still traversing the old tree see consistent state.
     */
    static final class WriteState {
        long root;
        long size;
        final List<Long> retirees = new ArrayList<>();

        WriteState(long root, long size) {
            this.root = root;
            this.size = size;
        }
    }

    // =======================================================================
    // WriteState-aware mutation methods (deferred COW)
    // =======================================================================

    /** Create a detached WriteState from current published root/size. Requires CowEngine. */
    WriteState beginWrite() {
        ensureCowEngine();
        return new WriteState(root, size);
    }

    /** Commit a detached WriteState: publish root, retire old nodes. */
    void commitWrite(WriteState ws) {
        root = ws.root;
        size = ws.size;
        publishRoot();
        if (reclaimer != null) {
            for (long old : ws.retirees) {
                reclaimer.retire(old);
            }
            reclaimer.advanceGeneration();
        }
    }

    long getOrCreateWith(WriteState ws, MemorySegment key, int keyLen, int leafClass) {
        var result = cowEngine.deferredGetOrCreate(ws.root, key, keyLen, leafClass);
        if (result.mutated()) {
            ws.root = result.newRoot();
            ws.size += result.sizeDelta();
            ws.retirees.addAll(result.retirees());
        }
        return result.leafPtr();
    }

    boolean deleteWith(WriteState ws, MemorySegment key, int keyLen) {
        var result = cowEngine.deferredDelete(ws.root, key, keyLen);
        if (result.mutated()) {
            ws.root = result.newRoot();
            ws.size += result.sizeDelta();
            ws.retirees.addAll(result.retirees());
        }
        return result.mutated();
    }

    private long lookupFrom(long startRoot, MemorySegment key, int keyLen) {
        long node = startRoot;
        int depth = 0;

        while (!NodePtr.isEmpty(node)) {
            int type = NodePtr.nodeType(node);

            if (type == NodePtr.LEAF) {
                return leafKeyMatches(node, key, keyLen) ? node : NodePtr.EMPTY_PTR;
            }

            if (type == NodePtr.PREFIX) {
                MemorySegment prefSeg = resolveNode(node);
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

    // =======================================================================
    // Package-private implementation — no locking, called by TaoDictionary and copy
    // =======================================================================

    long lookupImpl(MemorySegment key, int keyLen) {
        return lookupFrom(root, key, keyLen);
    }

    MemorySegment leafValueImpl(long leafPtr) {
        MemorySegment full = resolveNode(leafPtr);
        return full.asSlice(keySlotSize);
    }

    /**
     * Resolve a NodePtr to a MemorySegment, handling both slab-allocated and
     * arena-allocated pointers.
     */
    private MemorySegment resolveNode(long ptr) {
        if (chunkStore != null && WriterArena.isArenaAllocated(ptr)) {
            int classId = NodePtr.slabClassId(ptr);
            return WriterArena.resolve(chunkStore, ptr, slab.segmentSize(classId));
        }
        return slab.resolve(ptr);
    }

    /**
     * Resolve a NodePtr with an explicit length.
     */
    private MemorySegment resolveNode(long ptr, int length) {
        if (chunkStore != null && WriterArena.isArenaAllocated(ptr)) {
            return WriterArena.resolve(chunkStore, ptr, length);
        }
        return slab.resolve(ptr, length);
    }

    // =======================================================================
    // Internal: helpers
    // =======================================================================

    private long findChild(long nodePtr, int type, byte keyByte) {
        MemorySegment seg = resolveNode(nodePtr);
        return switch (type) {
            case NodePtr.NODE_4   -> Node4.findChild(seg, keyByte);
            case NodePtr.NODE_16  -> Node16.findChild(seg, keyByte);
            case NodePtr.NODE_48  -> Node48.findChild(seg, keyByte);
            case NodePtr.NODE_256 -> Node256.findChild(seg, keyByte);
            default -> NodePtr.EMPTY_PTR;
        };
    }

    private boolean leafKeyMatches(long leafPtr, MemorySegment key, int keyLen) {
        if (keyLen != this.keyLen) return false;
        MemorySegment leafKey = resolveNode(leafPtr, this.keyLen);
        return leafKey.mismatch(key.asSlice(0, this.keyLen)) == -1;
    }

    // =======================================================================
    // Private nested helpers
    // =======================================================================

    /**
     * Walks a source tree and re-inserts every leaf into a target tree.
     * Handles out-of-line string data migration (overflow copy + pointer patching).
     */
    private static final class Copier {

        private final TaoTree source;
        private final TaoTree target;
        private final WriteState ws;

        Copier(TaoTree source, TaoTree target, WriteState ws) {
            this.source = source;
            this.target = target;
            this.ws = ws;
        }

        void validate() {
            if (source.keyLen != target.keyLen)
                throw new IllegalArgumentException(
                    "Key length mismatch: source=" + source.keyLen + " target=" + target.keyLen);
            if (source.leafClassCount != target.leafClassCount)
                throw new IllegalArgumentException(
                    "Leaf class count mismatch: source=" + source.leafClassCount
                    + " target=" + target.leafClassCount);
            for (int i = 0; i < source.leafClassCount; i++)
                if (source.leafValueSizes[i] != target.leafValueSizes[i])
                    throw new IllegalArgumentException(
                        "Leaf value size mismatch at class " + i + ": source="
                        + source.leafValueSizes[i] + " target=" + target.leafValueSizes[i]);
        }

        void copy() {
            if (NodePtr.isEmpty(source.root)) return;
            copyNode(source.root);
        }

        private void copyNode(long nodePtr) {
            if (NodePtr.isEmpty(nodePtr)) return;
            int type = NodePtr.nodeType(nodePtr);

            if (type == NodePtr.LEAF) { copyLeaf(nodePtr); return; }

            if (type == NodePtr.PREFIX) {
                MemorySegment prefSeg = source.resolveNode(nodePtr);
                copyNode(PrefixNode.child(prefSeg));
                return;
            }

            MemorySegment seg = source.resolveNode(nodePtr);
            switch (type) {
                case NodePtr.NODE_4 -> {
                    int n = Node4.count(seg);
                    for (int i = 0; i < n; i++) copyNode(Node4.childAt(seg, i));
                }
                case NodePtr.NODE_16 -> {
                    int n = Node16.count(seg);
                    for (int i = 0; i < n; i++) copyNode(Node16.childAt(seg, i));
                }
                case NodePtr.NODE_48 ->
                    Node48.forEach(seg, (k, child) -> copyNode(child));
                case NodePtr.NODE_256 ->
                    Node256.forEach(seg, (k, child) -> copyNode(child));
            }
        }

        private void copyLeaf(long srcLeafPtr) {
            MemorySegment srcFull = source.resolveNode(srcLeafPtr);
            MemorySegment srcKey = srcFull.asSlice(0, source.keyLen);

            int srcSlabClassId = NodePtr.slabClassId(srcLeafPtr);
            int leafClassIdx = -1;
            for (int i = 0; i < source.leafClassCount; i++) {
                if (source.leafClassIds[i] == srcSlabClassId) { leafClassIdx = i; break; }
            }
            if (leafClassIdx < 0 || leafClassIdx >= target.leafClassCount)
                throw new IllegalStateException(
                    "Source leaf class index " + leafClassIdx + " not compatible with target tree");

            long tgtLeafPtr = target.getOrCreateWith(ws, srcKey, source.keyLen, leafClassIdx);

            int tgtSlabClassId = NodePtr.slabClassId(tgtLeafPtr);
            if (tgtSlabClassId != target.leafClassIds[leafClassIdx])
                throw new IllegalStateException(
                    "Leaf class conflict for existing key: target slab class " + tgtSlabClassId
                    + " != expected " + target.leafClassIds[leafClassIdx]
                    + " (leaf class index " + leafClassIdx + ").");

            MemorySegment srcValue = srcFull.asSlice(source.keySlotSize);
            MemorySegment tgtValue = target.leafValueImpl(tgtLeafPtr);

            TaoString.Layout layout = target.stringLayouts.get(target.leafClassIds[leafClassIdx]);
            if (layout != null) {
                int len = srcValue.get(ValueLayout.JAVA_INT_UNALIGNED, layout.lenOffset());
                if (len > layout.inlineThreshold()) {
                    long srcRef = srcValue.get(ValueLayout.JAVA_LONG_UNALIGNED, layout.ptrOffset());
                    MemorySegment srcData = source.bump.resolve(srcRef, len);

                    long tgtRef = target.bump.allocate(len);
                    MemorySegment tgtData = target.bump.resolve(tgtRef, len);
                    MemorySegment.copy(srcData, 0, tgtData, 0, len);

                    MemorySegment.copy(srcValue, 0, tgtValue, 0, srcValue.byteSize());
                    tgtValue.set(ValueLayout.JAVA_LONG_UNALIGNED, layout.ptrOffset(), tgtRef);
                    return;
                }
            }
            MemorySegment.copy(srcValue, 0, tgtValue, 0, srcValue.byteSize());
        }
    }

    /**
     * Static helpers for binding key/leaf layouts to a tree.
     * Handles dictionary creation, rebinding, and string-layout registration.
     */
    private static final class Binding {

        private Binding() {}

        static KeyLayout bindDicts(TaoTree tree, KeyLayout keyLayout) {
            var fields = new KeyField[keyLayout.fieldCount()];
            boolean changed = false;
            for (int i = 0; i < fields.length; i++) {
                var f = keyLayout.field(i);
                if (f instanceof KeyField.DictU16 d && d.dict() == null) {
                    fields[i] = KeyField.dict16(d.name(), TaoDictionary.u16(tree));
                    changed = true;
                } else if (f instanceof KeyField.DictU32 d && d.dict() == null) {
                    fields[i] = KeyField.dict32(d.name(), TaoDictionary.u32(tree));
                    changed = true;
                } else {
                    fields[i] = f;
                }
            }
            return changed ? KeyLayout.of(fields) : keyLayout;
        }

        static KeyLayout rebindDicts(TaoTree tree, KeyLayout keyLayout) {
            var fields = new KeyField[keyLayout.fieldCount()];
            int dictIdx = 0;
            boolean changed = false;
            for (int i = 0; i < fields.length; i++) {
                var f = keyLayout.field(i);
                if (f instanceof KeyField.DictU16 d && d.dict() == null) {
                    if (dictIdx >= tree.dicts.size())
                        throw new IllegalArgumentException(
                            "Key layout has more dict fields than restored dictionaries ("
                            + tree.dicts.size() + ")");
                    fields[i] = KeyField.dict16(d.name(), tree.dicts.get(dictIdx++));
                    changed = true;
                } else if (f instanceof KeyField.DictU32 d && d.dict() == null) {
                    if (dictIdx >= tree.dicts.size())
                        throw new IllegalArgumentException(
                            "Key layout has more dict fields than restored dictionaries ("
                            + tree.dicts.size() + ")");
                    fields[i] = KeyField.dict32(d.name(), tree.dicts.get(dictIdx++));
                    changed = true;
                } else {
                    fields[i] = f;
                }
            }
            return changed ? KeyLayout.of(fields) : keyLayout;
        }

        static void registerStringLayouts(TaoTree tree, LeafLayout leafLayout,
                                          int leafClassIndex) {
            for (var sl : leafLayout.stringLayouts()) {
                tree.registerStringLayout(leafClassIndex, sl);
            }
        }
    }
}
