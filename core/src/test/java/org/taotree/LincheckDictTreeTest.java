package org.taotree;

import org.jetbrains.lincheck.datastructures.IntGen;
import org.jetbrains.lincheck.datastructures.Operation;
import org.jetbrains.lincheck.datastructures.Param;
import org.jetbrains.lincheck.datastructures.StressOptions;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.lang.foreign.Arena;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;
import org.taotree.layout.KeyBuilder;
import org.taotree.layout.KeyField;
import org.taotree.layout.KeyLayout;
import org.taotree.layout.LeafField;
import org.taotree.layout.LeafLayout;

/**
 * Lincheck linearizability test for TaoTree with dictionary-encoded compound keys
 * and typed leaf values via {@link LeafLayout} / {@link LeafAccessor}.
 *
 * <p>Key layout: [category:u16 dict][id:u32] = 6 bytes.
 * <br>Leaf layout: [value:int64] = 8 bytes.
 */
@Param(name = "cat", gen = IntGen.class, conf = "1:3")
@Param(name = "id",  gen = IntGen.class, conf = "1:5")
public class LincheckDictTreeTest {

    private static final String[] CATEGORIES = {null, "Alpha", "Beta", "Gamma"};
    private static final long CHUNK_SIZE = 1024L * 1024;

    /** All trees and paths created by Lincheck invocations — drained by @AfterEach. */
    private static final List<Runnable> CLEANUPS = new CopyOnWriteArrayList<>();

    private final TaoTree tree;
    private final Path storePath;
    private final KeyLayout keyLayout;
    private final LeafLayout leafLayout;

    public LincheckDictTreeTest() {
        try {
            leafLayout = LeafLayout.of(LeafField.int64("value"));

            storePath = Files.createTempFile("lincheck-dict-", ".dat");
            storePath.toFile().deleteOnExit();
            Files.delete(storePath);
            tree = TaoTree.create(storePath, 6, leafLayout.totalWidth(), CHUNK_SIZE, false);
            // Register cleanup immediately after create so dict/layout setup failures don't leak.
            CLEANUPS.add(() -> {
                tree.close();
                try { Files.deleteIfExists(storePath); } catch (IOException ignored) {}
            });

            var dict = TaoDictionary.u16(tree);
            keyLayout = KeyLayout.of(
                KeyField.dict16("category", dict),
                KeyField.uint32("id")
            );
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    @AfterEach
    void cleanup() {
        List<Runnable> toRun = List.copyOf(CLEANUPS);
        CLEANUPS.clear();
        toRun.forEach(Runnable::run);
    }

    private long compositeValue(int cat, int id) {
        return cat * 1000L + id;
    }

    @Operation
    public long put(@Param(name = "cat") int cat, @Param(name = "id") int id) {
        long val = compositeValue(cat, id);
        try (var arena = Arena.ofConfined()) {
            var kb = new KeyBuilder(keyLayout, arena);
            kb.setDict(0, CATEGORIES[cat]);
            kb.setU32(1, id);
            try (var w = tree.write()) {
                long ptr = w.getOrCreate(kb.key(), kb.keyLen(), 0);
                w.leaf(ptr, leafLayout).setInt64("value", val);
                return val;
            }
        }
    }

    @Operation
    public long get(@Param(name = "cat") int cat, @Param(name = "id") int id) {
        try (var arena = Arena.ofConfined()) {
            var kb = new KeyBuilder(keyLayout, arena);
            kb.setDict(0, CATEGORIES[cat]);
            kb.setU32(1, id);
            try (var r = tree.read()) {
                long ptr = r.lookup(kb.key(), kb.keyLen());
                if (ptr == TaoTree.NOT_FOUND) return -1;
                return r.leaf(ptr, leafLayout).getInt64("value");
            }
        }
    }

    @Operation
    public boolean delete(@Param(name = "cat") int cat, @Param(name = "id") int id) {
        try (var arena = Arena.ofConfined()) {
            var kb = new KeyBuilder(keyLayout, arena);
            kb.setDict(0, CATEGORIES[cat]);
            kb.setU32(1, id);
            try (var w = tree.write()) {
                return w.delete(kb.key(), kb.keyLen());
            }
        }
    }

    @Operation
    public long size() {
        try (var r = tree.read()) {
            return r.size();
        }
    }

    @Test
    void stressTest() {
        new StressOptions()
            .iterations(100)
            .invocationsPerIteration(50)
            .threads(3)
            .actorsPerThread(3)
            .sequentialSpecification(SequentialSpec.class)
            .check(this.getClass());
    }

    /**
     * Sequential specification: models the same put/get/delete/size
     * behaviour using a plain HashMap keyed by (category, id).
     */
    public static class SequentialSpec {
        private final Map<Long, Long> map = new HashMap<>();

        private static long mapKey(int cat, int id) {
            return cat * 1000L + id;
        }

        public long put(int cat, int id) {
            long val = cat * 1000L + id;
            map.put(mapKey(cat, id), val);
            return val;
        }

        public long get(int cat, int id) {
            Long v = map.get(mapKey(cat, id));
            return v == null ? -1 : v;
        }

        public boolean delete(int cat, int id) {
            return map.remove(mapKey(cat, id)) != null;
        }

        public long size() {
            return map.size();
        }
    }
}
