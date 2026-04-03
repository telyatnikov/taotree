package org.taotree;

import org.taotree.internal.NodePtr;

import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;

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
    private int nextCode;

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

    /** Acquire a read scope. */
    public ReadScope read() {
        owner.acquireReadLock();
        return new ReadScope();
    }

    /**
     * Acquire a write scope.
     *
     * @throws IllegalStateException if the current thread holds only a read scope
     */
    public WriteScope write() {
        owner.acquireWriteLock();
        return new WriteScope();
    }

    // -----------------------------------------------------------------------
    // Convenience self-locking methods
    // -----------------------------------------------------------------------

    /**
     * Intern a string: return its code, assigning a new one if not yet known.
     *
     * <p>Acquires the tree's write lock (reentrant if already held).
     *
     * @param value the string to intern
     * @return the integer code (never 0 — zero is reserved as null sentinel)
     * @throws IllegalStateException if the dictionary is full, or if read->write upgrade
     */
    public int intern(String value) {
        owner.acquireWriteLock();
        try {
            return internImpl(value);
        } finally {
            owner.releaseWriteLock();
        }
    }

    /**
     * Resolve a string to its code without assigning. Returns -1 if not found.
     *
     * <p>Acquires the tree's read lock (reentrant if already held).
     */
    public int resolve(String value) {
        owner.acquireReadLock();
        try {
            return resolveImpl(value);
        } finally {
            owner.releaseReadLock();
        }
    }

    /** Number of entries. Acquires read lock. */
    public int size() {
        owner.acquireReadLock();
        try {
            return (int) tree.size;
        } finally {
            owner.releaseReadLock();
        }
    }

    /** The next code that will be assigned. Acquires read lock. */
    public int nextCode() {
        owner.acquireReadLock();
        try {
            return nextCode;
        } finally {
            owner.releaseReadLock();
        }
    }

    // -----------------------------------------------------------------------
    // ReadScope
    // -----------------------------------------------------------------------

    /** Read-only scope of the dictionary. Holds the tree's read lock. */
    public final class ReadScope implements AutoCloseable {
        /** Resolve a string to its code. Returns -1 if not found. */
        public int resolve(String value) { return resolveImpl(value); }

        /** Number of entries. */
        public int size() { return (int) tree.size; }

        @Override
        public void close() { owner.releaseReadLock(); }
    }

    // -----------------------------------------------------------------------
    // WriteScope
    // -----------------------------------------------------------------------

    /** Write scope of the dictionary. Holds the tree's write lock. */
    public final class WriteScope implements AutoCloseable {
        /** Intern a string: return its code, assigning a new one if not yet known. */
        public int intern(String value) { return internImpl(value); }

        /** Resolve a string to its code. Returns -1 if not found. */
        public int resolve(String value) { return resolveImpl(value); }

        /** Number of entries. */
        public int size() { return (int) tree.size; }

        @Override
        public void close() { owner.releaseWriteLock(); }
    }

    // -----------------------------------------------------------------------
    // Internal
    // -----------------------------------------------------------------------

    private int internImpl(String value) {
        byte[] padded = encodeAndPad(value);
        long leafRef = tree.getOrCreateImpl(MemorySegment.ofArray(padded), MAX_KEY_LEN, 0);
        MemorySegment leaf = tree.leafValueImpl(leafRef);
        int existing = leaf.get(ValueLayout.JAVA_INT_UNALIGNED, 0);
        if (existing != 0) return existing;

        if (nextCode > maxCode) {
            throw new IllegalStateException("TaoDictionary exhausted (max=" + maxCode + "): " + value);
        }
        int code = nextCode++;
        leaf.set(ValueLayout.JAVA_INT_UNALIGNED, 0, code);
        return code;
    }

    private int resolveImpl(String value) {
        byte[] padded = encodeAndPad(value);
        long leafRef = tree.lookupImpl(MemorySegment.ofArray(padded), MAX_KEY_LEN);
        if (NodePtr.isEmpty(leafRef)) return -1;
        return tree.leafValueImpl(leafRef).get(ValueLayout.JAVA_INT_UNALIGNED, 0);
    }

    private byte[] encodeAndPad(String value) {
        byte[] encoded = TaoKey.encodeString(value);
        if (encoded.length > MAX_KEY_LEN) {
            throw new IllegalArgumentException(
                "Encoded string too long (" + encoded.length + " > " + MAX_KEY_LEN + "): " + value);
        }
        byte[] padded = new byte[MAX_KEY_LEN];
        System.arraycopy(encoded, 0, padded, 0, encoded.length);
        return padded;
    }
}
