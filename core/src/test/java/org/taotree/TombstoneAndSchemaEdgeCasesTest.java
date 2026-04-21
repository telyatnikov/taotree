package org.taotree;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.lang.foreign.Arena;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.file.Files;
import java.nio.file.Path;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.taotree.internal.persist.SchemaBinding;
import org.taotree.layout.KeyField;
import org.taotree.layout.KeyLayout;

/**
 * Edge-case tests for {@code p8-tombstone-semantics} and
 * {@code p8-schema-binding}. Intentionally small, targeted assertions to
 * lift mutation-testing coverage on the relevant branches.
 */
class TombstoneAndSchemaEdgeCasesTest {

    @TempDir Path tmp;
    private int fc;
    private Path path() { return tmp.resolve(fc++ + ".taotree"); }
    private KeyLayout keyLayout() { return KeyLayout.of(KeyField.uint32("id")); }

    // ── Tombstone edge cases ────────────────────────────────────────────

    /**
     * Tombstone at TIMELESS ({@code ts=0}) records a retraction run at the
     * sentinel timestamp. The CHAMP current-state must be cleared.
     */
    @Test
    void tombstoneAtTimelessClearsCurrent() throws IOException {
        try (var tree = TaoTree.create(path(), keyLayout());
             var arena = Arena.ofConfined()) {
            var ID = tree.keyUint32("id");
            var kb = tree.newKeyBuilder(arena).set(ID, 1);
            try (var w = tree.write()) {
                w.put(kb, "age", Value.ofInt(30));
                assertTrue(w.delete(kb, "age", TaoTree.TIMELESS),
                        "timeless tombstone must report state change");
            }
            try (var r = tree.read()) {
                assertNull(r.get(kb, "age"),
                        "timeless tombstone must clear current state");
            }
        }
    }

    /**
     * A tombstone on an entity/attr that was never written is a no-op on
     * state but must not throw, and must not surface a value.
     */
    @Test
    void tombstoneOnUnknownAttrIsSafe() throws IOException {
        try (var tree = TaoTree.create(path(), keyLayout());
             var arena = Arena.ofConfined()) {
            var ID = tree.keyUint32("id");
            var kb = tree.newKeyBuilder(arena).set(ID, 42);
            try (var w = tree.write()) {
                w.delete(kb, "never-seen", 100L);
            }
            try (var r = tree.read()) {
                assertNull(r.get(kb, "never-seen"));
                assertNull(r.getAt(kb, "never-seen", 200L));
            }
        }
    }

    /**
     * Put <i>before</i> a tombstone in wall-clock order but written
     * <i>after</i> it must still splice correctly into history; the
     * tombstone window survives as a gap in {@code getAt}.
     */
    @Test
    void putBeforeTombstoneSplicesPastHistory() throws IOException {
        try (var tree = TaoTree.create(path(), keyLayout());
             var arena = Arena.ofConfined()) {
            var ID = tree.keyUint32("id");
            var kb = tree.newKeyBuilder(arena).set(ID, 1);
            try (var w = tree.write()) {
                w.put(kb, "age", Value.ofInt(30), 100L);
                w.delete(kb, "age", 200L);
                // Retro-active observation: value was actually 29 at ts=50
                w.put(kb, "age", Value.ofInt(29), 50L);
            }
            try (var r = tree.read()) {
                assertEquals(Value.ofInt(29), r.getAt(kb, "age", 60L));
                assertEquals(Value.ofInt(30), r.getAt(kb, "age", 150L));
                assertNull(r.getAt(kb, "age", 250L),
                        "tombstone window must still null out post-200");
            }
        }
    }

    /** Multiple independent attributes can each be tombstoned without affecting siblings. */
    @Test
    void multipleTombstonesIndependent() throws IOException {
        try (var tree = TaoTree.create(path(), keyLayout());
             var arena = Arena.ofConfined()) {
            var ID = tree.keyUint32("id");
            var kb = tree.newKeyBuilder(arena).set(ID, 1);
            try (var w = tree.write()) {
                w.put(kb, "a", Value.ofInt(1), 10L);
                w.put(kb, "b", Value.ofInt(2), 10L);
                w.put(kb, "c", Value.ofInt(3), 10L);
            }
            try (var w = tree.write()) {
                w.delete(kb, "a", 20L);
                w.delete(kb, "c", 20L);
            }
            try (var r = tree.read()) {
                assertNull(r.get(kb, "a"));
                assertEquals(Value.ofInt(2), r.get(kb, "b"));
                assertNull(r.get(kb, "c"));
                // History for 'b' must still see its single run.
                int[] runCount = {0};
                r.history(kb, "b", (fs, ls, vt, v) -> { runCount[0]++; return true; });
                assertEquals(1, runCount[0]);
            }
        }
    }

    // ── SchemaBinding edge cases ───────────────────────────────────────

    @Test
    void firstMismatchDetectsFieldCountDifference() {
        var a = KeyLayout.of(KeyField.uint32("year"));
        var b = KeyLayout.of(KeyField.uint32("year"), KeyField.dict16("taxon"));
        String mismatch = SchemaBinding.firstMismatch(
                SchemaBinding.serialize(a), SchemaBinding.serialize(b));
        assertNotNull(mismatch, "differing field counts must mismatch");
    }

    @Test
    void firstMismatchDetectsFieldReorder() {
        // Same fields, different order → different fingerprint.
        var a = KeyLayout.of(KeyField.uint32("year"), KeyField.dict16("taxon"));
        var b = KeyLayout.of(KeyField.dict16("taxon"), KeyField.uint32("year"));
        assertNotNull(SchemaBinding.firstMismatch(
                SchemaBinding.serialize(a), SchemaBinding.serialize(b)));
    }

    @Test
    void firstMismatchHandlesNullArguments() {
        byte[] bytes = SchemaBinding.serialize(KeyLayout.of(KeyField.uint32("k")));
        assertNotNull(SchemaBinding.firstMismatch(null, bytes));
        assertNotNull(SchemaBinding.firstMismatch(bytes, null));
        assertNull(SchemaBinding.firstMismatch(bytes, bytes),
                "identity comparison must short-circuit to match");
    }

    @Test
    void firstMismatchSameLengthDifferentContent() {
        // Differ at a specific byte offset only — exercises the loop in
        // firstMismatch (byte-level diff, not length diff).
        var a = KeyLayout.of(KeyField.uint32("k1"));
        var b = KeyLayout.of(KeyField.uint32("k2"));
        byte[] ba = SchemaBinding.serialize(a);
        byte[] bb = SchemaBinding.serialize(b);
        assertEquals(ba.length, bb.length);
        String msg = SchemaBinding.firstMismatch(ba, bb);
        assertNotNull(msg);
        assertTrue(msg.contains("offset"), "should cite the mismatched offset");
    }

    @Test
    void deserializeRejectsZeroFieldCount() {
        // Hand-craft a header with fieldCount=0 to hit the dedicated guard.
        ByteBuffer buf = ByteBuffer.allocate(8).order(ByteOrder.LITTLE_ENDIAN);
        buf.put(SchemaBinding.VERSION);
        buf.put((byte) 0);       // reserved
        buf.putShort((short) 0); // fieldCount = 0
        buf.putShort((short) 0); // totalWidth
        buf.putShort((short) 0); // reserved
        assertThrows(IllegalArgumentException.class,
                () -> SchemaBinding.deserialize(buf.array()));
    }

    @Test
    void deserializeRejectsNull() {
        assertThrows(IllegalArgumentException.class,
                () -> SchemaBinding.deserialize(null));
    }

    @Test
    void deserializeRejectsMismatchedTotalWidth() {
        byte[] bytes = SchemaBinding.serialize(
                KeyLayout.of(KeyField.uint32("year")));
        // Corrupt totalWidth (offset 4, LE short) to a bogus value.
        bytes[4] = (byte) 0x99;
        bytes[5] = (byte) 0x00;
        assertThrows(IllegalArgumentException.class,
                () -> SchemaBinding.deserialize(bytes));
    }

    @Test
    void deserializeRejectsNameLengthOverflow() {
        byte[] bytes = SchemaBinding.serialize(
                KeyLayout.of(KeyField.uint32("year")));
        // Overwrite nameLen at offset HEADER_SIZE + 2 (after kind + width).
        // HEADER_SIZE = 8 → nameLen at offset 10–11 (LE).
        bytes[10] = (byte) 0xFF;
        bytes[11] = (byte) 0xFF;
        assertThrows(IllegalArgumentException.class,
                () -> SchemaBinding.deserialize(bytes));
    }

    @Test
    void serializeIsStableAcrossRoundtrips() {
        var layout = KeyLayout.of(
                KeyField.uint32("year"),
                KeyField.dict16("species"));
        byte[] a = SchemaBinding.serialize(layout);
        byte[] b = SchemaBinding.serialize(SchemaBinding.deserialize(a));
        assertArrayEquals(a, b, "serialize ∘ deserialize must be the identity on bytes");
    }

    /**
     * Reopen with a truncated file surfaces {@link IOException} rather than
     * an unchecked error. Truncation is a more deterministic corruption
     * mode than overwriting random bytes (mirrored checkpoints make partial
     * overwrites recoverable by design).
     */
    @Test
    void openPathSurfacesTruncatedFileAsIOException() throws IOException {
        Path file = path();
        try (var tree = TaoTree.create(file, keyLayout());
             var arena = Arena.ofConfined()) {
            var ID = tree.keyUint32("id");
            var kb = tree.newKeyBuilder(arena).set(ID, 1);
            try (var w = tree.write()) {
                w.put(kb, "x", Value.ofInt(1));
            }
            tree.sync();
        }
        byte[] data = Files.readAllBytes(file);
        // Keep only the first page — destroys both superblock mirrors and
        // any checkpoint section that mentions the schema binding.
        Files.write(file, new byte[Math.min(data.length, 64)]);

        assertThrows(IOException.class, () -> TaoTree.open(file));
    }

    /** Reopen with a file that was never written to has no binding → friendly error. */
    @Test
    void openPathWithoutBindingOnEmptyFileThrows() throws IOException {
        Path file = path();
        Files.write(file, new byte[0]);
        assertThrows(IOException.class, () -> TaoTree.open(file));
    }

    /** Legacy open(Path, KeyLayout) must still succeed when the binding matches. */
    @Test
    void openWithLayoutSucceedsAfterSync() throws IOException {
        Path file = path();
        var layout = keyLayout();
        try (var tree = TaoTree.create(file, layout);
             var arena = Arena.ofConfined()) {
            var ID = tree.keyUint32("id");
            var kb = tree.newKeyBuilder(arena).set(ID, 7);
            try (var w = tree.write()) { w.put(kb, "k", Value.ofInt(1)); }
            tree.sync();
        }
        try (var tree = TaoTree.open(file, layout);
             var arena = Arena.ofConfined()) {
            var ID = tree.keyUint32("id");
            var kb = tree.newKeyBuilder(arena).set(ID, 7);
            try (var r = tree.read()) {
                assertEquals(Value.ofInt(1), r.get(kb, "k"));
            }
        }
    }

    @SuppressWarnings("unused")
    private static byte[] readAll(Path p) throws IOException {
        return Files.readAllBytes(p);
    }

    @SuppressWarnings("unused")
    private static void assertFalseIsOk() { assertFalse(false); }
}
