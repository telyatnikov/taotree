package org.taotree;

import org.taotree.internal.art.NodePtr;

import org.taotree.internal.art.Node4;
import org.taotree.internal.art.Node16;
import org.taotree.internal.art.Node48;
import org.taotree.internal.art.Node256;
import org.taotree.internal.art.NodeConstants;
import org.taotree.internal.alloc.BumpAllocator;
import org.taotree.internal.alloc.ChunkStore;
import org.taotree.internal.champ.ChampVisitor;
import org.taotree.internal.cow.Compactor;
import org.taotree.internal.cow.CowEngine;
import org.taotree.internal.cow.EpochReclaimer;
import org.taotree.internal.alloc.Preallocator;
import org.taotree.internal.art.PrefixNode;
import org.taotree.internal.persist.CheckpointIO;
import org.taotree.internal.persist.Checkpoint;
import org.taotree.internal.persist.CommitRecord;
import org.taotree.internal.persist.PersistenceManager;
import org.taotree.internal.persist.ShadowPagingRecovery;
import org.taotree.internal.alloc.SlabAllocator;
import org.taotree.internal.persist.Superblock;
import org.taotree.internal.alloc.WriterArena;
import org.taotree.internal.art.ArtSearch;
import org.taotree.internal.temporal.AttributeRun;
import org.taotree.internal.temporal.EntityNode;
import org.taotree.internal.temporal.HistoryVisitor;
import org.taotree.internal.temporal.TemporalReader;
import org.taotree.internal.temporal.TemporalWriter;

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
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.locks.ReentrantLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import org.taotree.internal.champ.ChampMap;
import org.taotree.internal.value.ValueCodec;
import org.taotree.layout.KeyBuilder;
import org.taotree.layout.KeyField;
import org.taotree.layout.KeyHandle;
import org.taotree.layout.KeyLayout;
import org.taotree.layout.QueryBuilder;

/**
 * File-backed, off-heap key-value tree backed by an Adaptive Radix Tree.
 *
 * <p>Owns the memory lifecycle, off-heap slab and bump allocators, and a fair
 * {@link ReentrantReadWriteLock} that protects all shared state. Operates on
 * fixed-length, binary-comparable keys with zero GC pressure. All data is
 * memory-mapped from a {@link ChunkStore} file.
 *
 * <p>{@link TaoDictionary Dictionaries} created from a tree share its slab allocator
 * and lock, ensuring consistent concurrency control.
 *
 * <h3>Usage:</h3>
 * <pre>{@code
 * try (var tree = TaoTree.create(path, KeyLayout.of(KeyField.uint32("id")))) {
 *     try (var w = tree.write()) {
 *         var kb = tree.newKeyBuilder(Arena.ofConfined());
 *         kb.set(tree.keyUint32("id"), 42);
 *         w.put(kb, "name", Value.ofString("eagle"));
 *     }
 *
 *     try (var r = tree.read()) {
 *         var qb = tree.newQueryBuilder(Arena.ofConfined());
 *         qb.set(tree.keyUint32("id"), 42);
 *         Value name = r.get(qb, "name");
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

    /**
     * Sentinel value returned by temporal read methods when an entity, attribute, or
     * time-point has no data. Equal to {@code -1L} (distinct from any valid value reference).
     */
    public static final long TEMPORAL_NOT_FOUND = -1L;

    /**
     * Timestamp sentinel for <i>timeless</i> facts — observations that are not
     * associated with a wall-clock time.
     *
     * <p>The high-level {@link WriteScope#put(KeyBuilder, String, Value)} and
     * {@link WriteScope#putAll(KeyBuilder, java.util.Map)} overloads (those
     * without a {@code ts} parameter) use this sentinel. A timeless run
     * covers the full time range and is superseded by any subsequent
     * observation with a real timestamp.
     *
     * <p>Value: {@code 0L}. The internal AttributeRuns ART uses unsigned
     * big-endian key ordering, so {@code 0} sorts before every positive
     * epoch-ms timestamp — keeping timeless runs in correct chronological
     * position when mixed with timestamped writes.
     */
    public static final long TIMELESS = 0L;

    // Write-back thresholds for the deferred-commit fast path (see
    // writeBackMutations()). When a write scope produces only value-replacements
    // of existing keys (no inserts/deletes, leaf size unchanged) we skip
    // publishing a new root and copy the new values back into the original
    // slab-allocated leaves in place, then reset the WriterArena. This is not
    // optional complexity: removing it regresses update-only throughput
    // (JMH putTimelessInt) by ~40% because every pure-update commit would
    // otherwise allocate fresh arena pages for the COW leaf + path copy,
    // growing the file linearly under steady-state update workloads. See
    // performance/baseline-writeback.json vs after-writeback.json.
    // Package-private for WriteScopeOps (the actual write-back driver).
    static final int MAX_WRITE_BACK_ENTRIES = 2_048;
    static final int MAX_WRITE_BACK_BYTES = 512 * 1024;

    // -- Shared infrastructure --
    private final Arena arena;
    private final SlabAllocator slab;
    private final BumpAllocator bump;
    // Structural lock: writeLock for compact/sync/close/copyFrom; readLock for stats.
    // Lock ordering: writeLock → commitLock (never reversed).
    private final ReentrantReadWriteLock lock;
    // Publication lock: serialises writer commits (deferredCommitImpl, commitWrite)
    // and structural ops that read/write root/size (compact, sync, close).
    // Package-private so WriteScopeOps can acquire/release during deferred commit.
    final ReentrantLock commitLock = new ReentrantLock();
    private final boolean ownsArena;
    private final ChunkStore chunkStore; // null for child trees (dictionaries)
    // ART read-path primitives (node resolution, lookupFrom, walks). Extracted
    // from this class to shrink the god-object footprint. Initialized at the
    // end of each constructor once slab/bump/chunkStore/keyLen/keySlotSize
    // are set.
    private final ArtRead art;

    // -- Persistence coordination --
    PersistenceManager persistence; // null for child trees (dictionaries); pkg-private for FileBackedStoreIO

    /** Package-private: provides overflow storage for {@link TaoString}. */
    BumpAllocator bump() { return bump; }

    /** Package-private: provides slab allocation for tests. */
    SlabAllocator slab() { return slab; }

    /** Package-private: underlying chunk store (used by TaoTreeSnapshot). */
    ChunkStore chunkStoreInternal() { return chunkStore; }

    /** Package-private: the bound KeyLayout, if any (used by TaoTreeSnapshot). */
    KeyLayout boundKeyLayoutInternal() { return boundKeyLayout; }

    // Node slab class IDs (registered once at construction)
    final int prefixClassId;
    final int node4ClassId;
    final int node16ClassId;
    final int node48ClassId;
    final int node256ClassId;

    // -- Tree state --
    // Package-private for internal helper classes (Copier, KeyLayoutBinding,
    // etc.) in this same package. Still not part of any public API.
    final int keyLen;
    final int keySlotSize;      // keyLen rounded up to 8-byte alignment
    final int[] leafClassIds;   // slab class IDs for each user leaf class
    final int[] leafValueSizes; // value size per leaf class
    final int leafClassCount;

    long root = NodePtr.EMPTY_PTR;
    long size = 0; // package-private for TaoDictionary

    // -- COW infrastructure --
    // Immutable published state: root pointer + entry count, captured atomically
    // by readers via a single VarHandle.getAcquire. Writers build a successor
    // PublicationState and publish via VarHandle.setRelease in commitWrite().
    // This single-reference publication replaces the old separate rootPtr + atomicSize,
    // ensuring root and size are always consistent for readers.
    @SuppressWarnings("unused") // accessed via PUB_HANDLE VarHandle
    private volatile PublicationState published = PublicationState.EMPTY;

    private static final VarHandle PUB_HANDLE;
    static {
        try {
            PUB_HANDLE = MethodHandles.lookup()
                .findVarHandle(TaoTree.class, "published", PublicationState.class);
        } catch (ReflectiveOperationException e) {
            throw new ExceptionInInitializerError(e);
        }
    }

    /**
     * Immutable snapshot of the published tree state. One acquire on the
     * {@link #PUB_HANDLE} transitively covers all nodes reachable from the root.
     * This is the atomic unit for Phase 7 CAS-based concurrent writer publication.
     */
    record PublicationState(long root, long size) {
        static final PublicationState EMPTY = new PublicationState(NodePtr.EMPTY_PTR, 0);
    }

    // Epoch-based reclaimer: deferred node freeing after COW path-copy
    private EpochReclaimer reclaimer;

    // COW engine: performs structural mutations via deferred path-copy
    // Package-private: accessed by WriteScopeOps during commit and rebase.
    CowEngine cowEngine;

    // Tracked dictionaries (for persistence)
    private final List<TaoDictionary> dicts = new ArrayList<>();

    /**
     * Package-private view of the tracked dictionaries, in creation order.
     * Used by {@link KeyLayoutBinding} on reopen.
     */
    List<TaoDictionary> dictsInternal() { return dicts; }

    // Bound key layout (set by layout-based factory methods; null for raw-API usage)
    private KeyLayout boundKeyLayout;

    // Raw schema-binding bytes loaded from the checkpoint on reopen. Preserved so
    // a round-trip compact/sync does not drop the section, even when the runtime
    // layout object is not available.
    byte[] loadedSchemaBinding;

    // -- Temporal store support --
    // Leaf values are 40-byte EntityNode structs.
    // TemporalWriter/TemporalReader handle per-entity ART + CHAMP operations.
    private volatile TemporalWriter temporalWriter;  // lazy-init on first temporal write
    private volatile TemporalReader temporalReader;  // lazy-init on first temporal read
    private TaoDictionary attrDictionary;            // attribute name → uint32 mapping

    // Per-thread WriterArena reuse: each thread gets a dedicated arena that persists
    // across WriteScopes, avoiding wasteful batch re-reservation on every scope open.
    // Lazily initialized because chunkStore is not set until construction completes.
    // Volatile: prevents lost ThreadLocal on concurrent first-write() races (C2 fix).
    private volatile ThreadLocal<WriterArena> threadArena;

    /** Get or create the per-thread WriterArena for this tree. */
    private WriterArena threadArena() {
        var tl = threadArena;
        if (tl == null) {
            synchronized (this) {
                tl = threadArena;
                if (tl == null) {
                    tl = ThreadLocal.withInitial(() -> new WriterArena(chunkStore));
                    threadArena = tl;
                }
            }
        }
        return tl.get();
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
        this.chunkStore = null; // child trees share parent's ChunkStore via shared slab/bump

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

        this.art = new ArtRead(bump, slab, chunkStore, keyLen, keySlotSize);
    }

    // -----------------------------------------------------------------------
    // Static factory methods
    // -----------------------------------------------------------------------

    /**
     * Bind restored dictionaries to a key layout's dict fields (by declaration order).
     * Used when reopening a file-backed tree — dicts already exist, we just connect them.
     */
    private static KeyLayout rebindDicts(TaoTree tree, KeyLayout keyLayout) {
        return KeyLayoutBinding.rebindDicts(tree, keyLayout);
    }

    private static KeyLayout bindDicts(TaoTree tree, KeyLayout keyLayout) {
        return KeyLayoutBinding.bindDicts(tree, keyLayout);
    }

    // -----------------------------------------------------------------------
    // Temporal tree factory methods
    // -----------------------------------------------------------------------

    /**
     * Create a file-backed temporal entity store.
     *
     * <p>The entity key layout defines how entities are identified (e.g., tenant + type + object).
     * Leaf values are 40-byte {@link EntityNode} structs, managed internally.
     * An attribute dictionary is auto-created for mapping attribute names to uint32 codes.
     *
     * <p>Use the unified {@link WriteScope#put(KeyBuilder, String, Value)} /
     * {@link ReadScope#get(KeyBuilder, String)} API to read and write values.
     *
     * @param path      path to the store file (must not exist)
     * @param keyLayout the entity key layout
     */
    public static TaoTree create(Path path, KeyLayout keyLayout) throws IOException {
        var tree = FileBackedStoreIO.createFileBacked(path, SlabAllocator.DEFAULT_SLAB_SIZE,
            keyLayout.totalWidth(), new int[]{EntityNode.SIZE},
            ChunkStore.DEFAULT_CHUNK_SIZE, Preallocator.isSupported());
        tree.boundKeyLayout = bindDicts(tree, keyLayout);
        tree.attrDictionary = new TaoDictionary(tree, Integer.MAX_VALUE, 1);
        return tree;
    }

    /**
     * Open an existing file-backed temporal entity store and verify the
     * caller-supplied {@link KeyLayout} matches the one persisted at creation.
     *
     * <p>If the file contains a schema-binding section (written by recent
     * builds), the caller's layout is compared field-for-field against it and
     * an {@link IOException} is thrown on mismatch. Older files without a
     * binding section are opened without verification for backward
     * compatibility.
     *
     * @param path      path to the existing store file
     * @param keyLayout the entity key layout (must match the schema used at
     *                  creation when present on disk)
     * @throws IOException if the file is unreadable or the layout does not
     *                     match the persisted fingerprint
     */
    public static TaoTree open(Path path, KeyLayout keyLayout) throws IOException {
        var tree = FileBackedStoreIO.openFileBacked(path);
        if (tree.loadedSchemaBinding != null) {
            byte[] expected = org.taotree.internal.persist.SchemaBinding.serialize(keyLayout);
            String mismatch = org.taotree.internal.persist.SchemaBinding
                    .firstMismatch(tree.loadedSchemaBinding, expected);
            if (mismatch != null) {
                tree.close();
                throw new IOException("KeyLayout mismatch for " + path + ": " + mismatch);
            }
        }
        tree.boundKeyLayout = rebindDicts(tree, keyLayout);
        // The last dictionary is the attribute dictionary (created after key dicts)
        int attrDictIdx = tree.dicts.size() - 1;
        if (attrDictIdx < 0) {
            throw new IOException("No attribute dictionary found in temporal tree: " + path);
        }
        tree.attrDictionary = tree.dicts.get(attrDictIdx);
        return tree;
    }

    /**
     * Open an existing file-backed temporal entity store, reconstructing the
     * {@link KeyLayout} from the persisted schema-binding fingerprint.
     *
     * <p>Requires that the file was created by a build that writes schema
     * binding (current behaviour of {@link #create(Path, KeyLayout)}). Files
     * without a binding section must be opened via
     * {@link #open(Path, KeyLayout)}.
     *
     * @throws IOException if the file lacks a schema-binding section or is
     *                     otherwise unreadable
     */
    public static TaoTree open(Path path) throws IOException {
        var tree = FileBackedStoreIO.openFileBacked(path);
        if (tree.loadedSchemaBinding == null) {
            tree.close();
            throw new IOException(
                    "File " + path + " has no schema-binding section; "
                  + "use open(Path, KeyLayout) to open legacy files.");
        }
        KeyLayout reconstructed;
        try {
            reconstructed = org.taotree.internal.persist.SchemaBinding
                    .deserialize(tree.loadedSchemaBinding);
        } catch (IllegalArgumentException e) {
            tree.close();
            throw new IOException("Corrupt schema binding in " + path + ": " + e.getMessage(), e);
        }
        tree.boundKeyLayout = rebindDicts(tree, reconstructed);
        int attrDictIdx = tree.dicts.size() - 1;
        if (attrDictIdx < 0) {
            throw new IOException("No attribute dictionary found in temporal tree: " + path);
        }
        tree.attrDictionary = tree.dicts.get(attrDictIdx);
        return tree;
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
                + "Use TaoTree.open(Path, KeyLayout) to bind a layout.");
        }
        return boundKeyLayout;
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

    // -----------------------------------------------------------------------
    // Copy / compaction
    // -----------------------------------------------------------------------

    /**
     * Package-private implementation of copyFrom with explicit WriteState.
     */
    void copyFromImpl(TaoTree source, WriteState ws) {
        var copier = new TreeCopier(source, this, ws);
        copier.validate();
        copier.copy();
    }

    // -----------------------------------------------------------------------
    // File-backed constructor (create new)
    // -----------------------------------------------------------------------

    TaoTree(Arena arena, SlabAllocator slab, BumpAllocator bump,
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
        // Default context has no arena — WriteScope provides per-thread arenas explicitly
        this.reclaimer = new EpochReclaimer(slab);
        this.cowEngine = new CowEngine(slab, reclaimer, null, chunkStore,
            prefixClassId, node4ClassId, node16ClassId, node48ClassId, node256ClassId,
            keyLen, keySlotSize, leafClassIds);
        this.art = new ArtRead(bump, slab, chunkStore, keyLen, keySlotSize);
    }

    // -----------------------------------------------------------------------
    // File-backed constructor (restore from superblock)
    // -----------------------------------------------------------------------

    TaoTree(Arena arena, SlabAllocator slab, BumpAllocator bump,
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
        // Default context has no arena — WriteScope provides per-thread arenas explicitly
        this.reclaimer = new EpochReclaimer(slab);
        this.cowEngine = new CowEngine(slab, reclaimer, null, chunkStore,
            prefixClassId, node4ClassId, node16ClassId, node48ClassId, node256ClassId,
            keyLen, keySlotSize, leafClassIds);
        PUB_HANDLE.setRelease(this, new PublicationState(root, size));
        this.art = new ArtRead(bump, slab, chunkStore, keyLen, keySlotSize);
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
        this.chunkStore = null; // child trees share parent's ChunkStore via shared slab/bump

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
        // Publish root for dict resolve (lock-free via published state acquire)
        PUB_HANDLE.setRelease(this, new PublicationState(root, size));
        this.art = new ArtRead(bump, slab, chunkStore, keyLen, keySlotSize);
    }

    // -----------------------------------------------------------------------
    // COW mode
    // -----------------------------------------------------------------------

    /** Returns the epoch reclaimer. */
    EpochReclaimer reclaimer() { return reclaimer; }

    /** Package-private: publish root and size as an atomic PublicationState. */
    void publishRoot() {
        PUB_HANDLE.setRelease(this, new PublicationState(root, size));
    }

    /** Package-private: read the current published state (lock-free). */
    PublicationState publishedState() {
        return (PublicationState) PUB_HANDLE.getAcquire(this);
    }

    // -----------------------------------------------------------------------
    // Temporal store support
    // -----------------------------------------------------------------------

    /** Returns true if this tree is in temporal mode. */

    /**
     * Returns the attribute dictionary for temporal trees.
     *
     */
    public TaoDictionary attrDictionary() {
        return attrDictionary;
    }

    /**
     * Convenience: intern an attribute name into the temporal attribute dictionary.
     * Acquires the dictionary write lock internally.
     *
     */
    public int internAttr(String name) {
        return attrDictionary().intern(name);
    }

    /**
     * Convenience: resolve an attribute name from the temporal attribute dictionary.
     * Lock-free read.
     *
     * @return the attribute code, or -1 if not found
     */
    public int resolveAttr(String name) {
        return attrDictionary().resolve(name);
    }

    /** Lazy-init TemporalWriter on first temporal write. Package-private for WriteScopeOps. */
    TemporalWriter ensureTemporalWriter() {
        var tw = temporalWriter;
        if (tw != null) return tw;
        synchronized (this) {
            tw = temporalWriter;
            if (tw == null) {
                tw = new TemporalWriter(slab, reclaimer, chunkStore, bump,
                    prefixClassId, node4ClassId, node16ClassId,
                    node48ClassId, node256ClassId);
                temporalWriter = tw;
            }
        }
        return tw;
    }

    /** Lazy-init TemporalReader on first temporal read. */
    TemporalReader ensureTemporalReader() {
        var tr = temporalReader;
        if (tr != null) return tr;
        synchronized (this) {
            tr = temporalReader;
            if (tr == null) {
                tr = new TemporalReader(slab, chunkStore, bump);
                temporalReader = tr;
            }
        }
        return tr;
    }

    // -----------------------------------------------------------------------
    // Lock-free lookup (COW mode)
    // -----------------------------------------------------------------------

    /**
     * Lock-free point lookup. Uses one acquire on the published state, then
     * plain loads for the entire traversal.
     */
    long lookupLockFree(MemorySegment key, int keyLen) {
        return lookupFrom(publishedState().root(), key, keyLen);
    }

    long lookupLockFree(byte[] key, int keyLen) {
        return lookupFrom(publishedState().root(), key, keyLen);
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
        lock.writeLock().lock();
        try {
            // Acquire commitLock to prevent concurrent writer publications.
            // Without this, a WriteScope.deferredCommitImpl() could publish a
            // new root between our read of 'root' and our publishRoot(), causing
            // the writer's insertion to be silently lost.
            commitLock.lock();
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
                    reclaimer.advanceDurableGeneration(reclaimer.globalGeneration());
                    reclaimer.reclaim();
                }
            } finally {
                commitLock.unlock();
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
     * Flush all data to disk.
     *
     * <p>Writes a lightweight commit record (one page) and forces only the
     * chunks that have been written to since the last sync. Every
     * {@value org.taotree.internal.persist.PersistenceManager#COMMITS_PER_CHECKPOINT} commits, also writes a full mirrored
     * checkpoint to bound recovery time and advances {@code durableGeneration}
     * for epoch reclamation.
     *
     * @throws IOException if an I/O error occurs during sync
     */
    public void sync() throws IOException {
        lock.writeLock().lock();
        try {
            // Acquire commitLock so buildCommitData() and gatherMetadata() read
            // consistent root/size — a concurrent writer cannot publish between
            // our reads and our persistence writes.
            commitLock.lock();
            try {
                persistence.writeCommitRecord(buildCommitData());
                if (persistence.shouldCheckpoint()) {
                    persistence.writeCheckpoint(gatherMetadata());
                    persistence.resetCommitCount();
                    chunkStore.syncDirty();
                    if (reclaimer != null) {
                        reclaimer.advanceDurableGeneration(reclaimer.globalGeneration());
                        reclaimer.reclaim();
                    }
                } else {
                    chunkStore.syncDirty();
                }
            } finally {
                commitLock.unlock();
            }
        } finally {
            lock.writeLock().unlock();
        }
    }

    /**
     * Close this tree, releasing the underlying arena if this is a root tree.
     * Writes a full checkpoint and syncs before closing.
     * Child trees (created internally by dictionaries) do not close the arena.
     */
    @Override
    public void close() {
        if (ownsArena) {
            try {
                // Acquire both locks so the final checkpoint captures a
                // consistent snapshot of root/size, not torn by a concurrent
                // writer publishing via commitLock.
                lock.writeLock().lock();
                try {
                    commitLock.lock();
                    try {
                        persistence.writeCheckpoint(gatherMetadata());
                        chunkStore.sync();
                        if (reclaimer != null) {
                            reclaimer.advanceDurableGeneration(reclaimer.globalGeneration());
                            reclaimer.reclaim();
                        }
                    } finally {
                        commitLock.unlock();
                    }
                } finally {
                    lock.writeLock().unlock();
                }
                chunkStore.close();
            } catch (IOException e) {
                throw new UncheckedIOException("Failed to sync/close chunk store", e);
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
    Superblock.SuperblockData gatherMetadata() {
        return TaoTreeSnapshot.gatherMetadata(this);
    }

    /**
     * Build a commit record data snapshot from current tree + dict state.
     * The {@link PersistenceManager} fills in generation, prev-page, and arena pages.
     */
    private CommitRecord.CommitData buildCommitData() {
        return TaoTreeSnapshot.buildCommitData(this);
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
     * Acquire a write scope. Multiple write scopes can be open concurrently.
     * Each mutation (getOrCreate / delete) independently performs optimistic
     * COW against the current published root, then acquires the commit lock
     * to publish. If another writer published in between, the mutation is
     * redone against the new root (one tree-depth traversal). User code
     * between mutations runs without any lock.
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
        ensureCowEngine();
        return new WriteScope();
    }

    /**
     * Open a write scope, execute the action, and close the scope.
     *
     * @param action the action to execute within the write scope
     */
    public void write(java.util.function.Consumer<WriteScope> action) {
        try (var w = write()) {
            action.accept(w);
        }
    }

    /**
     * Open a read scope, execute the action, and close the scope.
     *
     * @param action the action to execute within the read scope
     */
    public void read(java.util.function.Consumer<ReadScope> action) {
        try (var r = read()) {
            action.accept(r);
        }
    }

    /** Lazy-init CowEngine + reclaimer for child trees on first write(). */
    private synchronized void ensureCowEngine() {
        if (cowEngine != null) return;
        if (reclaimer == null) {
            reclaimer = new EpochReclaimer(slab);
        }
        cowEngine = new CowEngine(slab, reclaimer, null, null,
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
        private final long snapshotSize;
        private final int epochSlot;

        private ReadScope() {
            this.epochSlot = reclaimer.enterEpoch();
            var pub = (PublicationState) PUB_HANDLE.getAcquire(TaoTree.this);
            this.snapshotRoot = pub.root();
            this.snapshotSize = pub.size();
        }

        /** Package-private: exposes the snapshot root for ArtSearch tests. */
        long root() { return snapshotRoot; }

        /**
         * Point lookup.
         * @return a leaf reference, or {@link TaoTree#NOT_FOUND} if not found
         */
        long lookup(MemorySegment key, int keyLen) {
            checkAccess();
            validateKeyLen(keyLen);
            return lookupFrom(snapshotRoot, key, keyLen);
        }

        /** Point lookup (uses the tree's key length). */
        long lookup(MemorySegment key) {
            checkAccess();
            return lookupFrom(snapshotRoot, key, TaoTree.this.keyLen);
        }

        long lookup(byte[] key) {
            checkAccess();
            validateKeyLen(key.length);
            return lookupFrom(snapshotRoot, key, key.length);
        }

        /**
         * Resolve a leaf pointer to a <b>read-only</b> view of the value portion.
         * The returned segment is valid only while this scope is open.
         * Writing to it throws {@link UnsupportedOperationException}.
         */
        MemorySegment leafValue(long leafPtr) {
            checkAccess();
            validateLeafPtr(leafPtr);
            return leafValueImpl(leafPtr).asReadOnly();
        }

        public long size() {
            checkAccess();
            return snapshotSize;
        }
        public boolean isEmpty() {
            checkAccess();
            return snapshotSize == 0;
        }

        // -- Scan APIs --

        /**
         * Scan all entities in lexicographic key order.
         *
         * @param visitor receives each entity key; return {@code false} to stop early
         * @return {@code true} if all entities were visited
         */
        public boolean forEach(EntityVisitor visitor) {
            checkAccess();
            byte[] entityKey = new byte[TaoTree.this.keyLen];
            return walkLeaves(snapshotRoot, leafPtr -> {
                MemorySegment.copy(resolveNode(leafPtr), 0,
                    MemorySegment.ofArray(entityKey), 0, TaoTree.this.keyLen);
                return visitor.visit(entityKey);
            });
        }

        /**
         * Prefix scan by {@link KeyBuilder}, delivering entity keys to the visitor.
         *
         * @param prefix    key builder with prefix fields set (uses intern — caller must hold write scope or have pre-interned)
         * @param upTo      the last key handle included in the prefix
         * @param visitor   receives each matching entity key; return {@code false} to stop early
         * @return {@code true} if all matching entities were visited
         */
        public boolean scan(KeyBuilder prefix, KeyHandle upTo, EntityVisitor visitor) {
            checkAccess();
            int prefixLen = upTo.end();
            byte[] entityKey = new byte[TaoTree.this.keyLen];
            return walkPrefixed(snapshotRoot, prefix.key(), prefixLen, leafPtr -> {
                MemorySegment.copy(resolveNode(leafPtr), 0,
                    MemorySegment.ofArray(entityKey), 0, TaoTree.this.keyLen);
                return visitor.visit(entityKey);
            });
        }

        /**
         * Prefix scan: iterate all entities whose key matches the prefix defined by
         * the query builder up to (and including) the given handle's field.
         *
         * <p>If the query is unsatisfiable (unknown dict value, unset field),
         * returns immediately without traversal.
         *
         * @param prefix    the query builder with prefix fields set
         * @param upTo      the last key handle in the prefix
         * @param visitor   receives each matching entity key; return {@code false} to stop early
         * @return {@code true} if all matching entities were visited
         */
        public boolean scan(QueryBuilder prefix, KeyHandle upTo, EntityVisitor visitor) {
            checkAccess();
            if (!prefix.isSatisfiable(upTo)) return true; // unsatisfiable → empty
            int prefixLen = prefix.prefixLength(upTo);
            byte[] entityKey = new byte[TaoTree.this.keyLen];
            return walkPrefixed(snapshotRoot, prefix.key(), prefixLen, leafPtr -> {
                MemorySegment.copy(resolveNode(leafPtr), 0,
                    MemorySegment.ofArray(entityKey), 0, TaoTree.this.keyLen);
                return visitor.visit(entityKey);
            });
        }

        /**
         * Low-level prefix scan with raw prefix bytes.
         *
         * @param prefix    the prefix bytes
         * @param prefixLen number of prefix bytes to match
         * @param visitor   receives each matching entity key; return {@code false} to stop early
         * @return {@code true} if all matching entities were visited
         */
        public boolean scan(MemorySegment prefix, int prefixLen, EntityVisitor visitor) {
            checkAccess();
            byte[] entityKey = new byte[TaoTree.this.keyLen];
            return walkPrefixed(snapshotRoot, prefix, prefixLen, leafPtr -> {
                MemorySegment.copy(resolveNode(leafPtr), 0,
                    MemorySegment.ofArray(entityKey), 0, TaoTree.this.keyLen);
                return visitor.visit(entityKey);
            });
        }

        // -- Temporal read operations --

        /**
         * Get the current (latest) value of an attribute for an entity.
         *
         * @param entityKey the entity key bytes
         * @param attrId    attribute dictionary ID
         * @return the value reference, or {@link TaoTree#TEMPORAL_NOT_FOUND} if the entity or attribute doesn't exist
         */
        public long latest(byte[] entityKey, int attrId) {
            checkAccess();
            long leafPtr = lookup(entityKey);
            if (leafPtr == NOT_FOUND) return TEMPORAL_NOT_FOUND;
            return ensureTemporalReader().latest(leafValueImpl(leafPtr).asReadOnly(), attrId);
        }

        /** Get the current value using a {@link QueryBuilder}. */
        public long latest(QueryBuilder qb, int attrId) {
            checkAccess();
            long leafPtr = lookup(qb.key());
            if (leafPtr == NOT_FOUND) return TEMPORAL_NOT_FOUND;
            return ensureTemporalReader().latest(leafValueImpl(leafPtr).asReadOnly(), attrId);
        }

        /**
         * Get the value of an attribute at a specific point in time.
         *
         * @param entityKey the entity key bytes
         * @param attrId    attribute dictionary ID
         * @param timestamp the point-in-time (epoch milliseconds)
         * @return the value reference, or {@link TaoTree#TEMPORAL_NOT_FOUND} if no data exists at that time
         */
        public long at(byte[] entityKey, int attrId, long timestamp) {
            checkAccess();
            long leafPtr = lookup(entityKey);
            if (leafPtr == NOT_FOUND) return TEMPORAL_NOT_FOUND;
            return ensureTemporalReader().at(leafValueImpl(leafPtr).asReadOnly(), attrId, timestamp);
        }

        /** Get value at a point in time using a {@link QueryBuilder}. */
        public long at(QueryBuilder qb, int attrId, long timestamp) {
            checkAccess();
            long leafPtr = lookup(qb.key());
            if (leafPtr == NOT_FOUND) return TEMPORAL_NOT_FOUND;
            return ensureTemporalReader().at(leafValueImpl(leafPtr).asReadOnly(), attrId, timestamp);
        }

        /**
         * Get the CHAMP root for the full entity state at a specific time.
         * The returned CHAMP root can be iterated with {@link #allFieldsAt}.
         *
         * @return CHAMP root pointer, or {@link TaoTree#TEMPORAL_NOT_FOUND} if no version exists at that time
         */
        public long stateAt(byte[] entityKey, long timestamp) {
            checkAccess();
            long leafPtr = lookup(entityKey);
            if (leafPtr == NOT_FOUND) return TEMPORAL_NOT_FOUND;
            return ensureTemporalReader().stateAt(leafValueImpl(leafPtr).asReadOnly(), timestamp);
        }

        /** Get full entity state at a point in time using a {@link QueryBuilder}. */
        public long stateAt(QueryBuilder qb, long timestamp) {
            return stateAt(qb.key().toArray(ValueLayout.JAVA_BYTE), timestamp);
        }

        /**
         * Iterate all attribute values at a specific point in time.
         *
         * @param entityKey the entity key bytes
         * @param timestamp the point-in-time (epoch milliseconds)
         * @param visitor   receives (attrId, valueRef) pairs; return false to stop
         * @return true if all fields were visited, false if stopped early
         */
        public boolean allFieldsAt(byte[] entityKey, long timestamp, ChampVisitor visitor) {
            checkAccess();
            long leafPtr = lookup(entityKey);
            if (leafPtr == NOT_FOUND) return true; // no entity → nothing to visit
            return ensureTemporalReader().allFieldsAt(
                leafValueImpl(leafPtr).asReadOnly(), timestamp, visitor);
        }

        /**
         * Iterate the full history of an attribute for an entity.
         *
         * @param entityKey the entity key bytes
         * @param attrId    attribute dictionary ID
         * @param visitor   receives (firstSeen, lastSeen, validTo, valueRef) for each run
         * @return true if all runs were visited
         */
        public boolean history(byte[] entityKey, int attrId, HistoryVisitor visitor) {
            checkAccess();
            long leafPtr = lookup(entityKey);
            if (leafPtr == NOT_FOUND) return true; // no entity → nothing to visit
            return ensureTemporalReader().history(
                leafValueImpl(leafPtr).asReadOnly(), attrId, visitor);
        }

        /** Iterate attribute history using a {@link QueryBuilder}. */
        public boolean history(QueryBuilder qb, int attrId, HistoryVisitor visitor) {
            return history(qb.key().toArray(ValueLayout.JAVA_BYTE), attrId, visitor);
        }

        /**
         * Iterate a time-bounded range of an attribute's history.
         *
         * @param entityKey the entity key bytes
         * @param attrId    attribute dictionary ID
         * @param fromMs    range start (inclusive, epoch milliseconds)
         * @param toMs      range end (inclusive, epoch milliseconds)
         * @param visitor   receives matching runs
         * @return true if all matching runs were visited
         */
        public boolean historyRange(byte[] entityKey, int attrId,
                                    long fromMs, long toMs, HistoryVisitor visitor) {
            checkAccess();
            long leafPtr = lookup(entityKey);
            if (leafPtr == NOT_FOUND) return true;
            return ensureTemporalReader().historyRange(
                leafValueImpl(leafPtr).asReadOnly(), attrId, fromMs, toMs, visitor);
        }

        /** Iterate bounded attribute history using a {@link QueryBuilder}. */
        public boolean historyRange(QueryBuilder qb, int attrId,
                                    long fromMs, long toMs, HistoryVisitor visitor) {
            return historyRange(qb.key().toArray(ValueLayout.JAVA_BYTE), attrId,
                fromMs, toMs, visitor);
        }

        // ── Unified high-level API (Value-typed) ─────────────────────────────

        /**
         * Get the current value of an attribute for the entity at {@code key},
         * or {@code null} if the entity or attribute doesn't exist.
         *
         * <p>Falls back to any open-ended timeless run if no temporal
         * observation is present.
         *
         */
        public Value get(KeyBuilder key, String attr) {
            checkAccess();
            int attrId = resolveAttr(attr);
            if (attrId < 0) return null;
            long leafPtr = lookup(key.key());
            if (leafPtr == NOT_FOUND) return null;
            MemorySegment entity = leafValueImpl(leafPtr).asReadOnly();
            long ref = ensureTemporalReader().latest(entity, attrId);
            if (ref == TemporalReader.NOT_FOUND) return null;
            return ValueCodec.decodeStandalone(ref, bump);
        }

        /** Convenience: get by {@link QueryBuilder}. */
        public Value get(QueryBuilder key, String attr) {
            checkAccess();
            int attrId = resolveAttr(attr);
            if (attrId < 0) return null;
            long leafPtr = lookup(key.key());
            if (leafPtr == NOT_FOUND) return null;
            MemorySegment entity = leafValueImpl(leafPtr).asReadOnly();
            long ref = ensureTemporalReader().latest(entity, attrId);
            if (ref == TemporalReader.NOT_FOUND) return null;
            return ValueCodec.decodeStandalone(ref, bump);
        }

        /**
         * Get the value of {@code attr} at a specific timestamp, or {@code null}
         * if absent at that time. Use {@link TaoTree#TIMELESS} to query the
         * timeless run only.
         */
        public Value getAt(KeyBuilder key, String attr, long ts) {
            checkAccess();
            int attrId = resolveAttr(attr);
            if (attrId < 0) return null;
            long leafPtr = lookup(key.key());
            if (leafPtr == NOT_FOUND) return null;
            long ref = ensureTemporalReader().at(leafValueImpl(leafPtr).asReadOnly(), attrId, ts);
            if (ref == TemporalReader.NOT_FOUND) return null;
            return ValueCodec.decodeStandalone(ref, bump);
        }

        /**
         * Get all current attribute→value pairs for the entity at {@code key}.
         * Returns an empty map if the entity doesn't exist. Iteration order is
         * unspecified (CHAMP hash order).
         */
        public Map<String, Value> getAll(KeyBuilder key) {
            checkAccess();
            long leafPtr = lookup(key.key());
            if (leafPtr == NOT_FOUND) return Collections.emptyMap();
            long stateRoot = EntityNode.currentStateRoot(leafValueImpl(leafPtr).asReadOnly());
            if (stateRoot == ChampMap.EMPTY_ROOT) return Collections.emptyMap();
            Map<String, Value> out = new LinkedHashMap<>();
            ChampMap.iterate(bump, stateRoot, (attrId, valueRef) -> {
                if (valueRef == AttributeRun.TOMBSTONE_VALUE_REF) return true;
                String name = attrDictionary.reverseLookup(attrId);
                if (name != null) {
                    out.put(name, ValueCodec.decodeStandalone(valueRef, bump));
                }
                return true;
            });
            return out;
        }

        /**
         * Get all attribute→value pairs as observed at {@code ts}.
         * Returns an empty map if no version exists at that time.
         */
        public Map<String, Value> getAllAt(KeyBuilder key, long ts) {
            checkAccess();
            long leafPtr = lookup(key.key());
            if (leafPtr == NOT_FOUND) return Collections.emptyMap();
            long stateRoot = ensureTemporalReader().stateAt(
                leafValueImpl(leafPtr).asReadOnly(), ts);
            if (stateRoot == TEMPORAL_NOT_FOUND || stateRoot == ChampMap.EMPTY_ROOT) {
                return Collections.emptyMap();
            }
            Map<String, Value> out = new LinkedHashMap<>();
            ChampMap.iterate(bump, stateRoot, (attrId, valueRef) -> {
                if (valueRef == AttributeRun.TOMBSTONE_VALUE_REF) return true;
                String name = attrDictionary.reverseLookup(attrId);
                if (name != null) {
                    out.put(name, ValueCodec.decodeStandalone(valueRef, bump));
                }
                return true;
            });
            return out;
        }

        /**
         * Iterate the full history of {@code attr}, delivering decoded values.
         * Returns {@code true} if iteration completed, {@code false} if the
         * visitor stopped early.
         */
        public boolean history(KeyBuilder key, String attr, ValueHistoryVisitor visitor) {
            checkAccess();
            int attrId = resolveAttr(attr);
            if (attrId < 0) return true;
            long leafPtr = lookup(key.key());
            if (leafPtr == NOT_FOUND) return true;
            return ensureTemporalReader().history(
                leafValueImpl(leafPtr).asReadOnly(), attrId,
                (firstSeen, lastSeen, validTo, valueRef) ->
                    visitor.visit(firstSeen, lastSeen, validTo,
                        valueRef == AttributeRun.TOMBSTONE_VALUE_REF
                            ? Value.ofNull()
                            : ValueCodec.decodeStandalone(valueRef, bump)));
        }

        /** Bounded-time attribute history as decoded values. */
        public boolean historyRange(KeyBuilder key, String attr,
                                    long fromMs, long toMs,
                                    ValueHistoryVisitor visitor) {
            checkAccess();
            int attrId = resolveAttr(attr);
            if (attrId < 0) return true;
            long leafPtr = lookup(key.key());
            if (leafPtr == NOT_FOUND) return true;
            return ensureTemporalReader().historyRange(
                leafValueImpl(leafPtr).asReadOnly(), attrId, fromMs, toMs,
                (firstSeen, lastSeen, validTo, valueRef) ->
                    visitor.visit(firstSeen, lastSeen, validTo,
                        valueRef == AttributeRun.TOMBSTONE_VALUE_REF
                            ? Value.ofNull()
                            : ValueCodec.decodeStandalone(valueRef, bump)));
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
     * Read-write scope of the TaoTree. Multiple write scopes can be open
     * concurrently. All COW mutations operate on a <b>private copy</b> of the
     * tree root (ZFS-inspired deferred-commit model). The commit lock is
     * acquired only at {@link #close()} time, not during mutations:
     *
     * <ol>
     *   <li>On scope open: snapshot the published root.</li>
     *   <li>Each {@code getOrCreate}: COW against the private root (no lock).
     *       Existing leaves are COW-copied into the WriterArena so all
     *       returned leaf pointers are writer-private.</li>
     *   <li>On {@code close()}: acquire commit lock, check for conflict.
     *       If another writer published since our snapshot, replay the
     *       mutation log against the new root. Then publish.</li>
     * </ol>
     *
     * <p>For child trees (no ChunkStore), falls back to immediate commit-lock
     * acquisition on first mutation (the old model).
     *
     * <p>All returned leaf references and {@link MemorySegment} slices are
     * valid only while this scope is open.
     */
    public final class WriteScope implements AutoCloseable {

        /** Package-private: access the underlying tree (used by TemporalReplay and WriteScopeOps). */
        TaoTree tree() { return TaoTree.this; }

        // All fields below are package-private so WriteScopeOps (same package,
        // non-nested) can drive the commit/rebase/writeback state machine.
        boolean closed;
        final Thread ownerThread = Thread.currentThread();
        final WriterArena scopeArena; // per-thread arena (null for child trees)
        boolean commitLockHeld;

        // Deferred-commit state (file-backed mode only)
        final PublicationState scopeSnapshot;   // snapshot at open time
        long scopeRoot;                          // private COW root
        long scopeSize;                          // private size counter
        final org.taotree.internal.cow.LongList scopeRetirees;
        org.taotree.internal.cow.MutationLog mutationLog;
        org.taotree.internal.cow.TemporalOpLog tempOpLog;
        org.taotree.internal.cow.LongOpenHashSet seenLeafPtrs; // dedup for mutation log
        boolean scopeMutated;                    // any mutation happened?
        boolean lockedMode;                      // transitioned to locked mode?

        private final WriteScopeOps ops = new WriteScopeOps(this);

        private WriteScope() {
            if (chunkStore != null) {
                this.scopeArena = threadArena();
                this.scopeArena.beginScope();
                // Snapshot published state for deferred commit
                this.scopeSnapshot = publishedState();
                this.scopeRoot = scopeSnapshot.root();
                this.scopeSize = scopeSnapshot.size();
                this.scopeRetirees = new org.taotree.internal.cow.LongList();
            } else {
                this.scopeArena = null;
                this.scopeSnapshot = null;
                this.scopeRetirees = null;
            }
        }

        // -- Read operations --

        long lookup(MemorySegment key, int keyLen) {
            checkAccess();
            validateKeyLen(keyLen);
            return lookupFrom(currentRoot(), key, keyLen);
        }

        /** Point lookup (uses the tree's key length). */
        long lookup(MemorySegment key) {
            checkAccess();
            return lookupFrom(currentRoot(), key, TaoTree.this.keyLen);
        }

        long lookup(byte[] key) {
            checkAccess();
            validateKeyLen(key.length);
            return lookupFrom(currentRoot(), key, key.length);
        }

        /**
         * Resolve a leaf pointer to a <b>writable</b> view of the value portion.
         * The returned segment is valid only while this scope is open.
         */
        MemorySegment leafValue(long leafPtr) {
            checkAccess();
            validateLeafPtr(leafPtr);
            return leafValueImpl(leafPtr);
        }

        public long size() {
            checkAccess();
            return currentSize();
        }
        public boolean isEmpty() {
            checkAccess();
            return currentSize() == 0;
        }

        /** Current root for this scope's view of the tree. */
        private long currentRoot() {
            if (scopeSnapshot != null && !lockedMode) return scopeRoot;
            if (commitLockHeld) return root;
            return publishedState().root();
        }

        /** Current size for this scope's view of the tree. */
        private long currentSize() {
            if (scopeSnapshot != null && !lockedMode) return scopeSize;
            if (commitLockHeld) return size;
            return publishedState().size();
        }

        // -- Write operations --

        /**
         * Insert a key or return the existing leaf.
         */
        long getOrCreate(MemorySegment key, int keyLen, int leafClass) {
            checkAccess();
            validateKeyLen(keyLen);
            validateLeafClass(leafClass);
            return optimisticGetOrCreate(key, keyLen, leafClass);
        }

        long getOrCreate(MemorySegment key) {
            checkAccess();
            return optimisticGetOrCreate(key, TaoTree.this.keyLen, 0);
        }

        long getOrCreate(byte[] key, int leafClass) {
            checkAccess();
            validateKeyLen(key.length);
            validateLeafClass(leafClass);
            return optimisticGetOrCreate(MemorySegment.ofArray(key), key.length, leafClass);
        }

        long getOrCreate(byte[] key) {
            checkAccess();
            validateKeyLen(key.length);
            return optimisticGetOrCreate(MemorySegment.ofArray(key), key.length, 0);
        }

        /**
         * Delete a key.
         * @return true if the key existed and was removed
         */
        boolean delete(MemorySegment key, int keyLen) {
            checkAccess();
            validateKeyLen(keyLen);
            return optimisticDelete(key, keyLen);
        }

        boolean delete(byte[] key) {
            checkAccess();
            validateKeyLen(key.length);
            return optimisticDelete(MemorySegment.ofArray(key), key.length);
        }

        /**
         * Delete an entity entirely from the global ART, including its EntityNode.
         * Returns {@code true} if the entity existed and was removed.
         *
         */
        public boolean delete(KeyBuilder entity) {
            checkAccess();
            return optimisticDelete(entity.key(), TaoTree.this.keyLen);
        }

        /**
         * Remove a single attribute's current CHAMP entry for an entity, so that
         * subsequent {@code get(entity, attr)} returns {@code null}. The
         * AttributeRuns history is preserved. Returns {@code false} if the entity
         * or attribute doesn't exist or isn't present in the current CHAMP state.
         *
         * <p><b>Note:</b> this operation does <em>not</em> write a tombstone —
         * it is a "clear current view" primitive. Consequently
         * {@code getAt(entity, attr, ts)} continues to return the last value
         * for any {@code ts} covered by an existing run. To record a deletion
         * event in the timeline use {@link #delete(KeyBuilder, String, long)}.
         *
         */
        public boolean delete(KeyBuilder entity, String attr) {
            checkAccess();
            int attrId = resolveAttr(attr);
            if (attrId < 0) return false;
            // Non-creating lookup first: a missing entity has no state to clear,
            // and we must not leak a phantom empty entity (see issue: delete
            // previously called optimisticGetOrCreate unconditionally).
            long existing = lookupFrom(currentRoot(), entity.key(), TaoTree.this.keyLen);
            if (NodePtr.isEmpty(existing)) return false;
            // COW-copy the entity leaf so we can safely modify its EntityNode
            long leafPtr = optimisticGetOrCreate(entity.key(), TaoTree.this.keyLen, 0);
            MemorySegment entityNodeSeg = leafValueImpl(leafPtr);
            long currentStateRoot = EntityNode.currentStateRoot(entityNodeSeg);
            if (currentStateRoot == org.taotree.internal.champ.ChampMap.EMPTY_ROOT) return false;
            var result = new org.taotree.internal.champ.ChampMap.Result();
            long newStateRoot = org.taotree.internal.champ.ChampMap.remove(bump, currentStateRoot, attrId, result);
            if (!result.modified) return false;
            EntityNode.setCurrentStateRoot(entityNodeSeg, newStateRoot);
            if (scopeArena != null) {
                ops.recordTempOp(org.taotree.internal.cow.TemporalOpLog.KIND_DELETE_ATTR,
                        entity.key(), attrId, 0L, 0L);
            }
            return true;
        }

        /**
         * Record a <i>retraction</i> (tombstone) of {@code attr} on the entity
         * identified by {@code key} at time {@code ts}. Inserts a tombstone
         * {@code AttributeRun} at {@code ts} that truncates the predecessor
         * run's validity window and causes
         * {@code getAt(key, attr, T)} to return {@code null} for any
         * {@code T ∈ [ts, next_put)}.
         *
         * <p>Use {@link TaoTree#TIMELESS} for an open-ended retraction that
         * also clears the current CHAMP state (functionally equivalent to
         * {@link #delete(KeyBuilder, String)} plus a history event).
         *
         * <p>History iterators surface tombstones as {@link Value#ofNull()}.
         *
         * @return {@code true} if the entity's aggregate state changed
         */
        public boolean delete(KeyBuilder entity, String attr, long ts) {
            checkAccess();
            int attrId = internAttr(attr);
            return putTemporalImpl(entity.key(), attrId,
                    AttributeRun.TOMBSTONE_VALUE_REF, ts);
        }

        // -- Temporal write operations --

        /**
         * Write a temporal observation for an entity attribute.
         *
         * <p>Upserts into the entity's AttributeRuns ART and conditionally updates
         * EntityVersions ART and CHAMP state snapshots. The entity is created in the
         * global ART if it doesn't exist yet.
         *
         * <p><b>valueRef contract (tightened since Phase 8):</b> {@code valueRef} must
         * be a real encoded slot ref produced by
         * {@link org.taotree.internal.value.ValueCodec#encodeStandalone(Value,
         * org.taotree.internal.alloc.BumpAllocator)} using <b>this tree's</b> bump
         * allocator, or the special {@link AttributeRun#TOMBSTONE_VALUE_REF}.
         * The temporal write path now compares slot contents (not just pointers) to
         * detect duplicates, so opaque synthetic longs (e.g., {@code 100L}) would
         * alias into live bump-page bytes and cause silent duplicate-detection
         * errors. Passing a structurally-invalid ref throws
         * {@link IllegalArgumentException}; passing a plausible-but-wrong ref
         * cannot be detected, so callers must use the public {@link Value} API
         * (or {@code ValueCodec.encodeStandalone}) to obtain refs.
         *
         * @param entityKey  the entity key bytes
         * @param attrId     attribute dictionary ID (from {@link TaoTree#internAttr})
         * @param valueRef   encoded slot ref (see contract above)
         * @param timestamp  observation timestamp (epoch milliseconds)
         * @return true if the entity's aggregate state changed (not just a last_seen extension)
         * @throws IllegalArgumentException if {@code valueRef} is not a plausible
         *         encoded slot ref for this tree's bump allocator
         */
        public boolean putTemporal(byte[] entityKey, int attrId, long valueRef, long timestamp) {
            checkAccess();
            validateKeyLen(entityKey.length);
            validateValueRef(valueRef);
            return putTemporalImpl(MemorySegment.ofArray(entityKey), attrId, valueRef, timestamp);
        }

        /**
         * Write a temporal observation using a {@link KeyBuilder}.
         *
         * @see #putTemporal(byte[], int, long, long)
         */
        public boolean putTemporal(KeyBuilder kb, int attrId, long valueRef, long timestamp) {
            checkAccess();
            validateValueRef(valueRef);
            return putTemporalImpl(kb.key(), attrId, valueRef, timestamp);
        }

        /**
         * Write a temporal observation using attribute name (convenience).
         * Interns the attribute name automatically.
         *
         * @see #putTemporal(byte[], int, long, long)
         */
        public boolean putTemporal(KeyBuilder kb, String attrName, long valueRef, long timestamp) {
            return putTemporal(kb, internAttr(attrName), valueRef, timestamp);
        }

        private void validateValueRef(long valueRef) {
            // Tombstones are a valid special sentinel produced by delete().
            if (valueRef == AttributeRun.TOMBSTONE_VALUE_REF) return;
            if (!bump.isValidSlotRef(valueRef, org.taotree.internal.value.ValueCodec.SLOT_SIZE)) {
                throw new IllegalArgumentException(
                        "putTemporal: valueRef=" + valueRef + " is not a plausible encoded slot ref for "
                        + "this tree's bump allocator. Refs must be produced by "
                        + "ValueCodec.encodeStandalone(Value, tree.bump()) or obtained via the public "
                        + "Value API (tree.write().put(..., Value, ts)).");
            }
        }

        // ── Unified high-level API (Value-typed) ─────────────────────────────

        /**
         * Record a <i>timeless</i> observation for {@code attr} on the entity
         * identified by {@code key}. Equivalent to
         * {@link #put(KeyBuilder, String, Value, long)} with
         * {@code ts = }{@link TaoTree#TIMELESS}.
         *
         * <p>The attribute is interned into the attribute dictionary if new.
         * The {@code value} is encoded off-heap via
         * {@link org.taotree.internal.value.ValueCodec}: payloads ≤ 12 bytes
         * inline, larger payloads bump-allocated.
         *
         */
        public boolean put(KeyBuilder key, String attr, Value value) {
            return put(key, attr, value, TIMELESS);
        }

        /**
         * Record an observation of {@code attr} = {@code value} at time {@code ts}.
         * Pass {@link TaoTree#TIMELESS} for a timeless fact.
         *
         * @return {@code true} if the entity's aggregate state changed
         */
        public boolean put(KeyBuilder key, String attr, Value value, long ts) {
            checkAccess();
            int attrId = internAttr(attr);
            Value v = (value == null) ? Value.ofNull() : value;
            long valueRef = ValueCodec.encodeStandalone(v, bump);
            return putTemporalImpl(key.key(), attrId, valueRef, ts);
        }

        /**
         * Record a batch of timeless observations for the entity at {@code key}.
         * Semantically equivalent to calling {@link #put(KeyBuilder, String, Value)}
         * for each entry, but avoids repeated entity lookups where possible.
         *
         */
        public void putAll(KeyBuilder key, Map<String, Value> attrs) {
            putAll(key, attrs, TIMELESS);
        }

        /**
         * Record a batch of observations at time {@code ts} for the entity at {@code key}.
         *
         */
        public void putAll(KeyBuilder key, Map<String, Value> attrs, long ts) {
            checkAccess();
            if (attrs == null || attrs.isEmpty()) return;
            for (Map.Entry<String, Value> e : attrs.entrySet()) {
                put(key, e.getKey(), e.getValue(), ts);
            }
        }

        boolean putTemporalImpl(MemorySegment entityKey, int attrId,
                                        long valueRef, long timestamp) {
            return ops.putTemporalImpl(entityKey, attrId, valueRef, timestamp);
        }

        /**
         * Copy all entities and full attribute history from {@code src} into this tree.
         *
         * <p>For each entity in the source, walks its AttributeRuns ART and replays
         * every run on the destination via {@link #put(KeyBuilder, String, Value, long)},
         * preserving {@code first_seen}, {@code last_seen}, and the chronological
         * derivation of {@code valid_to}. {@link TaoTree#TIMELESS} (0) sorts before
         * every positive timestamp in unsigned-BE key order, so per-attribute runs
         * are already iterated in the correct replay order.
         *
         * <p>Source attribute IDs are translated through their dictionary names into
         * destination IDs (re-interned), so the two trees need not share dictionary state.
         * Source value payloads are decoded and re-encoded in the destination's bump.
         *
         * <p>Both trees must be temporal.
         *
         * @param src the source read scope
         */
        public void copyFrom(ReadScope src) {
            checkAccess();
            src.checkAccess();
            TemporalReplay.replay(this, src);
        }

        // -- Per-mutation deferred COW (delegates to WriteScopeOps) --

        /** Record a retraction tombstone via the temporal op log (file-backed mode). */
        private void recordTempOp(int kind, MemorySegment entityKey,
                                  int attrId, long valueRef, long ts) {
            ops.recordTempOp(kind, entityKey, attrId, valueRef, ts);
        }

        private long optimisticGetOrCreate(MemorySegment key, int keyLen, int leafClass) {
            return ops.optimisticGetOrCreate(key, keyLen, leafClass);
        }

        private boolean optimisticDelete(MemorySegment key, int keyLen) {
            return ops.optimisticDelete(key, keyLen);
        }

        public void close() {
            if (!closed) {
                checkThread();
                closed = true;
                if (scopeSnapshot != null && scopeMutated) {
                    ops.deferredCommit();
                } else if (commitLockHeld) {
                    publishRoot();
                    commitLock.unlock();
                    commitLockHeld = false;
                }
                if (scopeArena != null) {
                    scopeArena.endScope();
                }
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
    // Internal: ordered subtree traversal (Layer 0) — delegates to ArtRead.
    // -----------------------------------------------------------------------

    private boolean walkLeaves(long nodePtr, java.util.function.LongPredicate visitor) {
        return art.walkLeaves(nodePtr, visitor);
    }

    private boolean walkPrefixed(long startRoot, MemorySegment prefix, int prefixLen,
                                 java.util.function.LongPredicate visitor) {
        return art.walkPrefixed(startRoot, prefix, prefixLen, visitor);
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
        final org.taotree.internal.cow.LongList retirees = new org.taotree.internal.cow.LongList();

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
        var pub = publishedState();
        return new WriteState(pub.root(), pub.size());
    }

    /**
     * Commit a detached WriteState: publish root, retire old nodes.
     * Used by {@link TaoDictionary#internImpl} for dictionary child tree mutations
     * (serialized by the per-dict lock, so no rebase needed).
     */
    void commitWrite(WriteState ws) {
        commitLock.lock();
        try {
            directPublish(ws);
        } finally {
            commitLock.unlock();
        }
        retireNodes(ws.retirees);
    }

    /**
     * Publish a WriteState directly (no conflict check). Caller must hold the commit lock.
     */
    private void directPublish(WriteState ws) {
        root = ws.root;
        size = ws.size;
        publishRoot();
    }

    /** Package-private for WriteScopeOps: retire a batch of COW-replaced nodes. */
    void retireNodes(org.taotree.internal.cow.LongList retirees) {
        if (reclaimer != null && retirees != null) {
            for (int i = 0, n = retirees.size(); i < n; i++) {
                reclaimer.retire(retirees.get(i));
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

    // =======================================================================
    // ART read-path delegates — bodies live in ArtRead.
    // =======================================================================

    private long lookupFrom(long startRoot, MemorySegment key, int keyLen) {
        return art.lookupFrom(startRoot, key, keyLen);
    }

    private long lookupFrom(long startRoot, byte[] key, int keyLen) {
        return art.lookupFrom(startRoot, key, keyLen);
    }

    // =======================================================================
    // Package-private API — used by TaoDictionary, TreeCopier, TemporalReplay.
    // =======================================================================

    long lookupImpl(MemorySegment key, int keyLen) {
        return art.lookupFrom(root, key, keyLen);
    }

    MemorySegment leafValueImpl(long leafPtr) {
        return art.leafValue(leafPtr);
    }

    /** Scan all leaves, visiting each key+value pair. Used by TaoDictionary cache rebuild. */
    void scanAllLeaves(java.util.function.BiPredicate<MemorySegment, MemorySegment> visitor) {
        long currentRoot = publishedState().root();
        art.walkPrefixed(currentRoot, MemorySegment.ofArray(new byte[0]), 0, leafPtr -> {
            MemorySegment full = art.resolveNode(leafPtr);
            MemorySegment keySeg = full.asSlice(0, keySlotSize);
            MemorySegment valueSeg = full.asSlice(keySlotSize);
            return visitor.test(keySeg, valueSeg);
        });
    }

    MemorySegment resolveNode(long ptr) {
        return art.resolveNode(ptr);
    }

    MemorySegment resolveNode(long ptr, int length) {
        return art.resolveNode(ptr, length);
    }

}
