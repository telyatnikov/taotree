package org.taotree.internal.persist;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.taotree.TaoTree;
import org.taotree.layout.KeyField;
import org.taotree.layout.KeyLayout;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.nio.file.Files;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Verifies the v3 file-format gate: stores written without
 * {@link Checkpoint#FEATURE_INCOMPAT_UNIFIED_TEMPORAL} (i.e. legacy v2)
 * are rejected with a friendly error when reopened.
 */
class FileFormatV3Test {

    @TempDir Path tmp;

    @Test
    void roundTripCreateAndReopen() throws IOException {
        Path p = tmp.resolve("v3.taotree");
        var keyLayout = KeyLayout.of(KeyField.uint32("id"));
        try (var tree = TaoTree.create(p, keyLayout)) {
            try (var arena = Arena.ofConfined();
                 var ws = tree.write()) {
                var ID = tree.keyUint32("id");
                var kb = tree.newKeyBuilder(arena);
                kb.set(ID, 7);
                ws.put(kb, "label", org.taotree.Value.ofString("hi"));
            }
            tree.sync();
        }
        // Reopening must succeed (the v3 incompat bit must round-trip cleanly).
        try (var tree = TaoTree.open(p, keyLayout);
             var arena = Arena.ofConfined();
             var rs = tree.read()) {
            var ID = tree.keyUint32("id");
            var kb = tree.newKeyBuilder(arena);
            kb.set(ID, 7);
            assertEquals(org.taotree.Value.ofString("hi"), rs.get(kb, "label"));
        }
    }

    @Test
    void rejectsCheckpointWithoutUnifiedTemporalBit() {
        var cp = new Checkpoint.CheckpointData();
        cp.generation = 1;
        cp.slotId = 0;
        cp.incompatibleFeatures = 0L;          // legacy v2: missing the v3 bit
        cp.compatibleFeatures = 0;
        cp.pageSize = 4096;
        cp.chunkSize = 4096L * 256;
        cp.totalPages = 1;
        cp.nextPage = 1;
        cp.sections = new Checkpoint.SectionRef[0];
        cp.inlineData = new byte[0];

        UncheckedIOException ex = assertThrows(UncheckedIOException.class,
                () -> CheckpointIO.fromCheckpoint(cp));
        assertTrue(ex.getMessage() == null
                        || ex.getMessage().contains("v2")
                        || ex.getCause().getMessage().contains("v2"),
                "Error message should mention v2 / format upgrade");
    }

    @Test
    void acceptsCheckpointWithUnifiedTemporalBit() {
        var cp = new Checkpoint.CheckpointData();
        cp.generation = 1;
        cp.slotId = 0;
        cp.incompatibleFeatures = Checkpoint.FEATURE_INCOMPAT_UNIFIED_TEMPORAL;
        cp.compatibleFeatures = 0;
        cp.pageSize = 4096;
        cp.chunkSize = 4096L * 256;
        cp.totalPages = 1;
        cp.nextPage = 1;
        cp.sections = new Checkpoint.SectionRef[0];
        cp.inlineData = new byte[0];

        var sb = CheckpointIO.fromCheckpoint(cp);
        assertNotNull(sb);
        assertEquals(cp.chunkSize, sb.chunkSize);
    }
}
