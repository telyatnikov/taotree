package org.taotree;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.taotree.layout.KeyField;
import org.taotree.layout.KeyLayout;

import java.io.IOException;
import java.lang.foreign.Arena;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Validates that {@link TaoTree.WriteScope#copyFrom(TaoTree.ReadScope)} replays
 * the full attribute history (TIMELESS + chronological runs) instead of only
 * the latest CHAMP snapshot.
 */
class CopyFromHistoryTest {

    @TempDir Path tmp;
    private int fc;
    private Path path() { return tmp.resolve(fc++ + ".taotree"); }
    private KeyLayout keyLayout() { return KeyLayout.of(KeyField.uint32("id")); }

    @Test
    void historyIsReplayedAcrossTrees() throws IOException {
        try (var src = TaoTree.create(path(), keyLayout());
             var dst = TaoTree.create(path(), keyLayout())) {

            // Build a multi-run history on src for entity id=1, attr "score".
            // Also include a TIMELESS run for "label" to verify ordering.
            try (var arena = Arena.ofConfined()) {
                var ID = src.keyUint32("id");
                try (var ws = src.write()) {
                    var kb = src.newKeyBuilder(arena);
                    kb.set(ID, 1);

                    ws.put(kb, "label", Value.ofString("alpha"));   // TIMELESS
                    ws.put(kb, "score", Value.ofInt(10), 100L);
                    ws.put(kb, "score", Value.ofInt(10), 150L);     // extends lastSeen
                    ws.put(kb, "score", Value.ofInt(20), 200L);     // new run
                    ws.put(kb, "score", Value.ofInt(30), 300L);     // another run
                }
            }

            // Replay into dst.
            try (var ws = dst.write()) {
                try (var rs = src.read()) {
                    ws.copyFrom(rs);
                }
            }

            // Verify dst sees identical history for "score".
            try (var arena = Arena.ofConfined();
                 var rsSrc = src.read();
                 var rsDst = dst.read()) {

                var srcID = src.keyUint32("id");
                var dstID = dst.keyUint32("id");
                var srcKb = src.newKeyBuilder(arena); srcKb.set(srcID, 1);
                var dstKb = dst.newKeyBuilder(arena); dstKb.set(dstID, 1);

                List<long[]> srcRuns = new ArrayList<>();
                List<long[]> dstRuns = new ArrayList<>();
                rsSrc.history(srcKb, "score", (firstSeen, lastSeen, validTo, value) -> {
                    srcRuns.add(new long[] { firstSeen, lastSeen, validTo,
                            ((Value.Int32) value).value() });
                    return true;
                });
                rsDst.history(dstKb, "score", (firstSeen, lastSeen, validTo, value) -> {
                    dstRuns.add(new long[] { firstSeen, lastSeen, validTo,
                            ((Value.Int32) value).value() });
                    return true;
                });

                assertEquals(srcRuns.size(), dstRuns.size(), "run count must match");
                for (int i = 0; i < srcRuns.size(); i++) {
                    assertArrayEquals(srcRuns.get(i), dstRuns.get(i),
                            "run " + i + " must match (firstSeen,lastSeen,validTo,value)");
                }

                // Verify the TIMELESS "label" survived too.
                Value label = rsDst.get(dstKb, "label");
                assertEquals(Value.ofString("alpha"), label);

                // Latest "score" must be 30.
                assertEquals(Value.ofInt(30), rsDst.get(dstKb, "score"));
            }
        }
    }

    @Test
    void multipleEntitiesAndAttributesCopied() throws IOException {
        try (var src = TaoTree.create(path(), keyLayout());
             var dst = TaoTree.create(path(), keyLayout())) {

            try (var arena = Arena.ofConfined()) {
                var ID = src.keyUint32("id");
                try (var ws = src.write()) {
                    var kb = src.newKeyBuilder(arena);
                    for (int i = 1; i <= 5; i++) {
                        kb.set(ID, i);
                        ws.put(kb, "name", Value.ofString("e" + i));
                        ws.put(kb, "n", Value.ofInt(i * 100), 1000L);
                        ws.put(kb, "n", Value.ofInt(i * 200), 2000L);
                    }
                }
            }

            try (var ws = dst.write(); var rs = src.read()) {
                ws.copyFrom(rs);
            }

            try (var arena = Arena.ofConfined(); var rs = dst.read()) {
                var ID = dst.keyUint32("id");
                var kb = dst.newKeyBuilder(arena);
                for (int i = 1; i <= 5; i++) {
                    kb.set(ID, i);
                    assertEquals(Value.ofString("e" + i), rs.get(kb, "name"));
                    assertEquals(Value.ofInt(i * 200), rs.get(kb, "n"));
                    assertEquals(Value.ofInt(i * 100), rs.getAt(kb, "n", 1500L));
                }
            }
        }
    }
}
