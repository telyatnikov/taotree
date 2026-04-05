package org.taotree;

import org.taotree.internal.NodePtr;

import org.taotree.internal.Node4;
import org.taotree.internal.Node16;
import org.taotree.internal.Node48;
import org.taotree.internal.Node256;
import org.taotree.internal.NodeConstants;
import org.taotree.internal.BumpAllocator;
import org.taotree.internal.ChunkStore;
import org.taotree.internal.CowEngine;
import org.taotree.internal.EpochReclaimer;
import org.taotree.internal.Preallocator;
import org.taotree.internal.PrefixNode;
import org.taotree.internal.SlabAllocator;
import org.taotree.internal.Superblock;

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
    private final ChunkStore chunkStore; // null for in-memory mode

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

    // -- COW infrastructure (v2) --
    // Volatile root pointer for lock-free reader access via VarHandle CAS.
    // Writers CAS this when the root itself changes (growth, shrink).
    // Per-subtree CAS on Node256 children bypasses this.
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

    // Atomic size counter for concurrent writers
    private final AtomicLong atomicSize = new AtomicLong(0);

    // Epoch-based reclaimer: deferred node freeing for lock-free readers
    private EpochReclaimer reclaimer; // null until COW mode is activated

    // COW engine: performs structural mutations via path-copy + CAS
    private CowEngine cowEngine; // null until COW mode is activated

    // True when COW mode is active (v2). False for legacy RW-lock mode.
    private volatile boolean cowMode = false;

    // Tracked dictionaries (for persistence)
    private final List<TaoDictionary> dicts = new ArrayList<>();

    // Bound key layout (set by layout-based factory methods; null for raw-API usage)
    private KeyLayout boundKeyLayout;

    // Bound leaf layout (set by layout-based factory methods; null for raw-API usage)
    private LeafLayout boundLeafLayout;

    // String layouts per slab class ID (for copyFrom out-of-line string handling)
    private final java.util.Map<Integer, StringLayout> stringLayouts = new java.util.HashMap<>();

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
        this.chunkStore = null; // child trees don't own the chunk store

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
     * leaf layout. Automatically registers {@link StringLayout}s for any
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
        var fields = new KeyField[keyLayout.fieldCount()];
        int dictIdx = 0;
        boolean changed = false;
        for (int i = 0; i < fields.length; i++) {
            var f = keyLayout.field(i);
            if (f instanceof KeyField.DictU16 d && d.dict() == null) {
                if (dictIdx >= tree.dicts.size()) {
                    throw new IllegalArgumentException(
                        "Key layout has more dict fields than restored dictionaries ("
                        + tree.dicts.size() + ")");
                }
                fields[i] = KeyField.dict16(d.name(), tree.dicts.get(dictIdx++));
                changed = true;
            } else if (f instanceof KeyField.DictU32 d && d.dict() == null) {
                if (dictIdx >= tree.dicts.size()) {
                    throw new IllegalArgumentException(
                        "Key layout has more dict fields than restored dictionaries ("
                        + tree.dicts.size() + ")");
                }
                fields[i] = KeyField.dict32(d.name(), tree.dicts.get(dictIdx++));
                changed = true;
            } else {
                fields[i] = f;
            }
        }
        return changed ? KeyLayout.of(fields) : keyLayout;
    }

    /** Auto-register string layouts for TaoString/JSON fields in the leaf layout. */
    private static void registerStringLayouts(TaoTree tree, LeafLayout leafLayout,
                                              int leafClassIndex) {
        for (var sl : leafLayout.stringLayouts()) {
            tree.registerStringLayout(leafClassIndex, sl);
        }
    }

    /**
     * Create dictionaries for any unbound dict fields in the key layout and return
     * a new layout with the dict references filled in.
     */
    private static KeyLayout bindDicts(TaoTree tree, KeyLayout keyLayout) {
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
    public void registerStringLayout(int leafClassIndex, StringLayout layout) {
        stringLayouts.put(leafClassIds[leafClassIndex], layout);
    }

    // -----------------------------------------------------------------------
    // Copy / compaction
    // -----------------------------------------------------------------------

    /**
     * Copy all live entries from the source tree into this tree.
     *
     * <p>Walks the source tree and re-inserts every leaf into this tree. For leaves
     * with registered {@link StringLayout}s, out-of-line string data is copied from
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
     *     try (var w = compacted.write()) {
     *         compacted.copyFrom(tree);
     *     }
     * }
     * }</pre>
     *
     * <p><b>Threading:</b> The caller must hold a write scope on this tree
     * (enforced — throws {@link IllegalStateException} if not held).
     * The source tree should not be concurrently modified (hold a read scope on
     * it, or ensure exclusive access).
     *
     * @param source the source tree to copy from
     * @throws IllegalArgumentException if key lengths, leaf class counts, or leaf value sizes don't match
     * @throws IllegalStateException if the target write lock is not held
     */
    public void copyFrom(TaoTree source) {
        if (!lock.isWriteLockedByCurrentThread()) {
            throw new IllegalStateException(
                "copyFrom() requires the target tree's write lock. "
                + "Use WriteScope.copyFrom(ReadScope) for the scope-safe API.");
        }
        copyFromImpl(source);
    }

    /**
     * Package-private implementation of copyFrom. Assumes locks are already held.
     */
    void copyFromImpl(TaoTree source) {
        if (source.keyLen != this.keyLen) {
            throw new IllegalArgumentException(
                "Key length mismatch: source=" + source.keyLen + " target=" + this.keyLen);
        }
        if (source.leafClassCount != this.leafClassCount) {
            throw new IllegalArgumentException(
                "Leaf class count mismatch: source=" + source.leafClassCount
                + " target=" + this.leafClassCount);
        }
        for (int i = 0; i < source.leafClassCount; i++) {
            if (source.leafValueSizes[i] != this.leafValueSizes[i]) {
                throw new IllegalArgumentException(
                    "Leaf value size mismatch at class " + i + ": source="
                    + source.leafValueSizes[i] + " target=" + this.leafValueSizes[i]);
            }
        }
        if (NodePtr.isEmpty(source.root)) return;
        copyNode(source, source.root);
    }

    /**
     * Recursively walk the source tree and re-insert every leaf into this tree.
     */
    private void copyNode(TaoTree source, long nodePtr) {
        if (NodePtr.isEmpty(nodePtr)) return;

        int type = NodePtr.nodeType(nodePtr);

        if (type == NodePtr.LEAF) {
            copyLeaf(source, nodePtr);
            return;
        }

        if (type == NodePtr.PREFIX) {
            MemorySegment prefSeg = source.slab.resolve(nodePtr);
            copyNode(source, PrefixNode.child(prefSeg));
            return;
        }

        // Inner node: recurse into all children
        MemorySegment seg = source.slab.resolve(nodePtr);
        switch (type) {
            case NodePtr.NODE_4 -> {
                int n = Node4.count(seg);
                for (int i = 0; i < n; i++) {
                    copyNode(source, Node4.childAt(seg, i));
                }
            }
            case NodePtr.NODE_16 -> {
                int n = Node16.count(seg);
                for (int i = 0; i < n; i++) {
                    copyNode(source, Node16.childAt(seg, i));
                }
            }
            case NodePtr.NODE_48 ->
                Node48.forEach(seg, (k, child) -> copyNode(source, child));
            case NodePtr.NODE_256 ->
                Node256.forEach(seg, (k, child) -> copyNode(source, child));
        }
    }

    /**
     * Copy a single leaf from source to this tree.
     * Extracts the key, inserts into this tree, copies the value,
     * and handles out-of-line string data if a StringLayout is registered.
     */
    private void copyLeaf(TaoTree source, long srcLeafPtr) {
        // Extract key from source leaf
        MemorySegment srcFull = source.slab.resolve(srcLeafPtr);
        MemorySegment srcKey = srcFull.asSlice(0, keyLen);

        // Determine the leaf class index in source
        int srcSlabClassId = NodePtr.slabClassId(srcLeafPtr);
        int leafClassIdx = -1;
        for (int i = 0; i < source.leafClassCount; i++) {
            if (source.leafClassIds[i] == srcSlabClassId) {
                leafClassIdx = i;
                break;
            }
        }
        if (leafClassIdx < 0 || leafClassIdx >= this.leafClassCount) {
            throw new IllegalStateException(
                "Source leaf class index " + leafClassIdx + " not compatible with target tree");
        }

        // Insert into this tree
        long tgtLeafPtr = getOrCreateImpl(srcKey, keyLen, leafClassIdx);

        // Verify the target leaf's slab class matches the requested class.
        // If the key already existed in a different leaf class, the slab class
        // will differ and raw byte copy would be unsafe.
        int tgtSlabClassId = NodePtr.slabClassId(tgtLeafPtr);
        if (tgtSlabClassId != this.leafClassIds[leafClassIdx]) {
            throw new IllegalStateException(
                "Leaf class conflict for existing key: target slab class " + tgtSlabClassId
                + " != expected " + this.leafClassIds[leafClassIdx]
                + " (leaf class index " + leafClassIdx + ")."
                + " Cannot copy into a non-empty target where the same key exists with a different leaf class.");
        }

        // Copy value portion, handling out-of-line string data if registered.
        // Order matters: allocate overflow first, then write the final value with
        // the patched pointer. This avoids a window where the target leaf contains
        // a dangling source overflow pointer if allocation fails mid-copy.
        MemorySegment srcValue = srcFull.asSlice(source.keySlotSize);
        MemorySegment tgtValue = leafValueImpl(tgtLeafPtr);

        StringLayout layout = stringLayouts.get(this.leafClassIds[leafClassIdx]);
        if (layout != null) {
            int len = srcValue.get(ValueLayout.JAVA_INT_UNALIGNED, layout.lenOffset());
            if (len > layout.inlineThreshold()) {
                // Allocate and copy overflow BEFORE writing the leaf value
                long srcRef = srcValue.get(ValueLayout.JAVA_LONG_UNALIGNED, layout.ptrOffset());
                MemorySegment srcData = source.bump.resolve(srcRef, len);

                long tgtRef = this.bump.allocate(len);
                MemorySegment tgtData = this.bump.resolve(tgtRef, len);
                MemorySegment.copy(srcData, 0, tgtData, 0, len);

                // Copy the full value, then patch with the target pointer
                MemorySegment.copy(srcValue, 0, tgtValue, 0, srcValue.byteSize());
                tgtValue.set(ValueLayout.JAVA_LONG_UNALIGNED, layout.ptrOffset(), tgtRef);
                return;
            }
        }

        // No overflow (or no layout registered): straight byte copy
        MemorySegment.copy(srcValue, 0, tgtValue, 0, srcValue.byteSize());
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
        var cs = ChunkStore.create(path, arena, chunkSize, preallocate);
        var slab = new SlabAllocator(arena, cs, slabSize);
        var bump = new BumpAllocator(arena, cs, BumpAllocator.DEFAULT_PAGE_SIZE);

        var tree = new TaoTree(arena, slab, bump, cs, keyLen, leafValueSizes);
        // Write initial superblock
        tree.writeSuperblock();
        return tree;
    }

    private static TaoTree openFileBacked(Path path) throws IOException {
        var arena = Arena.ofShared();

        // First pass: open chunk store with minimal info to read the superblock
        // We need totalPages and nextPage to remap chunks, but those are IN the superblock.
        // Approach: open with file size to determine totalPages, read superblock, then finalize.
        var channel = java.nio.channels.FileChannel.open(path,
            java.nio.file.StandardOpenOption.READ,
            java.nio.file.StandardOpenOption.WRITE);
        long fileSize = channel.size();
        channel.close();

        if (fileSize < 2L * ChunkStore.PAGE_SIZE) {
            throw new IOException("File too small to contain a superblock: " + path);
        }

        // Compute totalPages and open chunk store
        int totalPages = (int) (fileSize / ChunkStore.PAGE_SIZE);
        // We don't know nextPage yet — read it from superblock
        // Open with totalPages, nextPage=totalPages (conservative)
        var cs = ChunkStore.open(path, arena, ChunkStore.DEFAULT_CHUNK_SIZE, totalPages, totalPages);

        // Read superblock
        MemorySegment sb = cs.superblock();
        var data = Superblock.read(sb);

        // Update ChunkStore with actual nextPage (we opened with totalPages as conservative value)
        // The ChunkStore already has all chunks mapped, so we just need allocations to resume
        // at the right position. We'll create a new ChunkStore with the correct nextPage.
        cs.close();
        cs = ChunkStore.open(path, arena, data.chunkSize, data.totalPages, data.nextPage);

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

        // Restore the primary tree (index 0 in tree descriptors)
        if (data.trees.length == 0) {
            throw new IOException("No tree descriptors found in superblock");
        }
        var treeDesc = data.trees[0];
        var tree = new TaoTree(arena, slab, bump, cs, treeDesc);

        // Restore dictionaries
        for (var dictDesc : data.dicts) {
            var childTreeDesc = data.trees[dictDesc.treeIndex];
            var childTree = new TaoTree(tree, childTreeDesc);
            var dict = new TaoDictionary(tree, childTree, dictDesc.maxCode, dictDesc.nextCode);
            tree.dicts.add(dict);
        }

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
    }

    // -----------------------------------------------------------------------
    // Child tree restore constructor (shares parent's infrastructure)
    // -----------------------------------------------------------------------

    /**
     * Package-private: restore a child tree from a superblock descriptor.
     * Shares the parent's arena, slab, bump, and lock.
     * Used to reconstruct TaoDictionary child trees on reopen.
     */
    TaoTree(TaoTree parent, Superblock.TreeDescriptor desc) {
        this.arena = parent.arena;
        this.slab = parent.slab;
        this.bump = parent.bump;
        this.lock = parent.lock;
        this.ownsArena = false;
        this.chunkStore = null;

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
    }

    // -----------------------------------------------------------------------
    // COW mode activation
    // -----------------------------------------------------------------------

    /**
     * Activate ROWEX-hybrid COW mode (v2 concurrency model).
     *
     * <p>After activation:
     * <ul>
     *   <li>Readers are completely lock-free (one acquire, then plain loads)
     *   <li>Writers use COW path-copy + per-subtree CAS
     *   <li>Nodes are retired (deferred free) instead of immediately freed
     * </ul>
     *
     * <p>This is a one-way transition. Cannot be deactivated.
     */
    public void activateCowMode() {
        if (cowMode) return;
        reclaimer = new EpochReclaimer(slab);
        cowEngine = new CowEngine(slab, reclaimer,
            prefixClassId, node4ClassId, node16ClassId, node48ClassId, node256ClassId,
            keyLen, keySlotSize, leafClassIds);
        // Sync root and size to the atomic versions
        ROOT_HANDLE.setRelease(this, root);
        atomicSize.set(size);
        cowMode = true;
    }

    /** Returns true if COW mode is active. */
    public boolean isCowMode() { return cowMode; }

    /** Returns the epoch reclaimer (null if COW mode not active). */
    EpochReclaimer reclaimer() { return reclaimer; }

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

    /**
     * COW insert with retry. Uses CowEngine for structural mutation
     * and per-subtree CAS for publication. Retries on CAS failure.
     */
    long cowGetOrCreateWithRetry(MemorySegment key, int keyLen, int leafClass) {
        while (true) {
            long currentRoot = (long) ROOT_HANDLE.getAcquire(this);
            var result = cowEngine.cowGetOrCreate(currentRoot, key, keyLen, leafClass);

            if (!result.created()) {
                return result.leafPtr(); // key already existed
            }

            if (result.published()) {
                // Per-subtree CAS succeeded
                atomicSize.incrementAndGet();
                reclaimer.advanceGeneration();
                // Sync legacy fields for backward compat
                root = (long) ROOT_HANDLE.getAcquire(this);
                size = atomicSize.get();
                return result.leafPtr();
            }

            // Need root-level CAS (no Node256 ancestor or root changed)
            if (!NodePtr.isEmpty(result.newRoot())) {
                boolean success = ROOT_HANDLE.compareAndSet(this, currentRoot, result.newRoot());
                if (success) {
                    atomicSize.incrementAndGet();
                    reclaimer.advanceGeneration();
                    root = result.newRoot();
                    size = atomicSize.get();
                    return result.leafPtr();
                }
                // CAS failed — another writer changed the root
                cowEngine.retireSpeculative(result.newRoot());
            }
            // Retry from scratch
        }
    }

    /**
     * COW delete with retry.
     */
    boolean cowDeleteWithRetry(MemorySegment key, int keyLen) {
        while (true) {
            long currentRoot = (long) ROOT_HANDLE.getAcquire(this);
            var result = cowEngine.cowDelete(currentRoot, key, keyLen);

            if (!result.deleted()) {
                return false;
            }

            if (result.published()) {
                atomicSize.decrementAndGet();
                reclaimer.advanceGeneration();
                root = (long) ROOT_HANDLE.getAcquire(this);
                size = atomicSize.get();
                return true;
            }

            // Root-level CAS
            long newRoot = result.newRoot();
            boolean success = ROOT_HANDLE.compareAndSet(this, currentRoot, newRoot);
            if (success) {
                atomicSize.decrementAndGet();
                reclaimer.advanceGeneration();
                root = newRoot;
                size = atomicSize.get();
                return true;
            }
            // CAS failed — retry
        }
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
     * Flush all data to disk (file-backed mode only).
     *
     * <p>Writes the superblock (metadata) to page 0 and forces all mapped regions to disk.
     * No-op for in-memory trees.
     *
     * @throws IOException if an I/O error occurs during sync
     */
    public void sync() throws IOException {
        if (chunkStore == null) return;
        lock.writeLock().lock();
        try {
            writeSuperblock();
            chunkStore.sync();
        } finally {
            lock.writeLock().unlock();
        }
    }

    /**
     * Close this tree, releasing the underlying arena if this is a root tree.
     * For file-backed trees, syncs the superblock before closing.
     * Child trees (created internally by dictionaries) do not close the arena.
     */
    @Override
    public void close() {
        if (ownsArena) {
            if (chunkStore != null) {
                try {
                    writeSuperblock();
                    chunkStore.sync();
                    chunkStore.close();
                } catch (IOException e) {
                    throw new UncheckedIOException("Failed to sync/close chunk store", e);
                }
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

    private void writeSuperblock() {
        if (chunkStore == null) return;

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

        Superblock.write(chunkStore.superblock(), data);
    }

    // -----------------------------------------------------------------------
    // Scoped access — the primary public API
    // -----------------------------------------------------------------------

    /**
     * Acquire a read scope. In COW mode, this is lock-free (epoch-based).
     * In legacy mode, holds the read lock until closed.
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
        if (cowMode) {
            int slot = reclaimer.enterEpoch();
            return new ReadScope(slot);
        }
        acquireReadLock();
        return new ReadScope(-1);
    }

    /**
     * Acquire a write scope. In COW mode, no global lock is held —
     * individual mutations use per-subtree CAS.
     * In legacy mode, holds the write lock until closed.
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
        if (cowMode) {
            return new WriteScope(true);
        }
        acquireWriteLock();
        return new WriteScope(false);
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
        private final int epochSlot; // -1 for legacy lock mode

        private ReadScope(int epochSlot) {
            this.epochSlot = epochSlot;
        }

        /**
         * Point lookup.
         * @return a leaf reference, or {@link TaoTree#NOT_FOUND} if not found
         */
        public long lookup(MemorySegment key, int keyLen) {
            checkAccess();
            validateKeyLen(keyLen);
            return cowMode ? lookupLockFree(key, keyLen) : lookupImpl(key, keyLen);
        }

        /** Point lookup (uses the tree's key length). */
        public long lookup(MemorySegment key) {
            checkAccess();
            return cowMode ? lookupLockFree(key, TaoTree.this.keyLen)
                           : lookupImpl(key, TaoTree.this.keyLen);
        }

        public long lookup(byte[] key) {
            checkAccess();
            validateKeyLen(key.length);
            var seg = MemorySegment.ofArray(key);
            return cowMode ? lookupLockFree(seg, key.length) : lookupImpl(seg, key.length);
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
            return cowMode ? atomicSize.get() : TaoTree.this.size;
        }
        public boolean isEmpty() {
            checkAccess();
            return cowMode ? atomicSize.get() == 0 : TaoTree.this.size == 0;
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
            long ptr = cowMode ? lookupLockFree(key, TaoTree.this.keyLen)
                               : lookupImpl(key, TaoTree.this.keyLen);
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
            return walkLeaves(root, leafPtr -> {
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
            return walkPrefixed(qb.key(), prefixLen, leafPtr -> {
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
            return walkPrefixed(prefix, prefixLen, leafPtr -> {
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
                if (epochSlot >= 0) {
                    reclaimer.exitEpoch(epochSlot);
                } else {
                    releaseReadLock();
                }
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
        private final boolean cowModeScope; // true if using COW, false for legacy lock

        private WriteScope(boolean cowModeScope) {
            this.cowModeScope = cowModeScope;
        }

        // -- Read operations (also available under write lock) --

        public long lookup(MemorySegment key, int keyLen) {
            checkAccess();
            validateKeyLen(keyLen);
            return cowModeScope ? lookupLockFree(key, keyLen) : lookupImpl(key, keyLen);
        }

        /** Point lookup (uses the tree's key length). */
        public long lookup(MemorySegment key) {
            checkAccess();
            return cowModeScope ? lookupLockFree(key, TaoTree.this.keyLen)
                                : lookupImpl(key, TaoTree.this.keyLen);
        }

        public long lookup(byte[] key) {
            checkAccess();
            validateKeyLen(key.length);
            var seg = MemorySegment.ofArray(key);
            return cowModeScope ? lookupLockFree(seg, key.length) : lookupImpl(seg, key.length);
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
            return cowModeScope ? atomicSize.get() : TaoTree.this.size;
        }
        public boolean isEmpty() {
            checkAccess();
            return cowModeScope ? atomicSize.get() == 0 : TaoTree.this.size == 0;
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
            long ptr = cowModeScope
                ? cowGetOrCreateWithRetry(key, TaoTree.this.keyLen, 0)
                : getOrCreateImpl(key, TaoTree.this.keyLen, 0);
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
            return cowModeScope
                ? cowGetOrCreateWithRetry(key, keyLen, leafClass)
                : getOrCreateImpl(key, keyLen, leafClass);
        }

        /** Insert or lookup using the tree's key length and the default leaf class (0). */
        public long getOrCreate(MemorySegment key) {
            checkAccess();
            return cowModeScope
                ? cowGetOrCreateWithRetry(key, TaoTree.this.keyLen, 0)
                : getOrCreateImpl(key, TaoTree.this.keyLen, 0);
        }

        public long getOrCreate(byte[] key, int leafClass) {
            checkAccess();
            validateKeyLen(key.length);
            validateLeafClass(leafClass);
            var seg = MemorySegment.ofArray(key);
            return cowModeScope
                ? cowGetOrCreateWithRetry(seg, key.length, leafClass)
                : getOrCreateImpl(seg, key.length, leafClass);
        }

        /** Insert or lookup using the default leaf class (0). */
        public long getOrCreate(byte[] key) {
            checkAccess();
            validateKeyLen(key.length);
            var seg = MemorySegment.ofArray(key);
            return cowModeScope
                ? cowGetOrCreateWithRetry(seg, key.length, 0)
                : getOrCreateImpl(seg, key.length, 0);
        }

        /**
         * Delete a key.
         * @return true if the key existed and was removed
         */
        public boolean delete(MemorySegment key, int keyLen) {
            checkAccess();
            validateKeyLen(keyLen);
            return cowModeScope
                ? cowDeleteWithRetry(key, keyLen)
                : deleteImpl(key, keyLen);
        }

        public boolean delete(byte[] key) {
            checkAccess();
            validateKeyLen(key.length);
            var seg = MemorySegment.ofArray(key);
            return cowModeScope
                ? cowDeleteWithRetry(seg, key.length)
                : deleteImpl(seg, key.length);
        }

        /**
         * Copy all live entries from the source tree (held by a read scope) into this tree.
         *
         * <p>Both locks are enforced: this tree's write lock (via this scope) and the
         * source tree's read lock (via the source scope).
         *
         * @param sourceScope a read scope on the source tree
         * @see TaoTree#copyFrom(TaoTree)
         */
        public void copyFrom(ReadScope sourceScope) {
            checkAccess();
            sourceScope.checkAccess();
            TaoTree.this.copyFromImpl(sourceScope.tree());
        }

        @Override
        public void close() {
            if (!closed) {
                checkThread();
                closed = true;
                if (!cowModeScope) {
                    releaseWriteLock();
                }
                // In COW mode, no lock to release. Individual mutations
                // were already published via CAS.
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
            MemorySegment seg = slab.resolve(nodePtr);
            return walkLeaves(PrefixNode.child(seg), visitor);
        }

        MemorySegment seg = slab.resolve(nodePtr);
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
     * @param prefix    the prefix bytes to match
     * @param prefixLen number of prefix bytes
     * @param visitor   receives raw leaf pointers; return false to stop early
     * @return true if all matching leaves were visited, false if stopped early
     */
    private boolean walkPrefixed(MemorySegment prefix, int prefixLen,
                                 java.util.function.LongPredicate visitor) {
        if (NodePtr.isEmpty(root)) return true;
        if (prefixLen == 0) return walkLeaves(root, visitor);

        long node = root;
        int depth = 0;

        while (depth < prefixLen) {
            if (NodePtr.isEmpty(node)) return true; // no match
            int type = NodePtr.nodeType(node);

            if (type == NodePtr.LEAF) {
                // Leaf: check if its key matches the prefix
                MemorySegment leafSeg = slab.resolve(node);
                for (int i = depth; i < prefixLen; i++) {
                    if (leafSeg.get(ValueLayout.JAVA_BYTE, i) !=
                        prefix.get(ValueLayout.JAVA_BYTE, i)) {
                        return true; // prefix doesn't match this leaf
                    }
                }
                return visitor.test(node);
            }

            if (type == NodePtr.PREFIX) {
                MemorySegment seg = slab.resolve(node);
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
            MemorySegment seg = slab.resolve(node);
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
