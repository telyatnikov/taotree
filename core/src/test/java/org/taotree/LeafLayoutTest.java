package org.taotree;

import org.junit.jupiter.api.Test;

import java.lang.foreign.Arena;

import static org.junit.jupiter.api.Assertions.*;
import org.taotree.layout.KeyBuilder;
import org.taotree.layout.KeyField;
import org.taotree.layout.KeyLayout;
import org.taotree.layout.LeafField;
import org.taotree.layout.LeafLayout;

/**
 * Tests for {@link LeafField}, {@link LeafLayout}, and {@link LeafAccessor}.
 */
class LeafLayoutTest {

    // ---- LeafField widths ----

    @Test
    void fieldWidths() {
        assertEquals(1,  LeafField.int8("x").width());
        assertEquals(2,  LeafField.int16("x").width());
        assertEquals(4,  LeafField.int32("x").width());
        assertEquals(8,  LeafField.int64("x").width());
        assertEquals(4,  LeafField.float32("x").width());
        assertEquals(8,  LeafField.float64("x").width());
        assertEquals(16, LeafField.string("x").width());
        assertEquals(16, LeafField.json("x").width());
    }

    @Test
    void dictFieldWidths() {
        try (var tree = TaoTree.forDictionaries()) {
            var dict16 = TaoDictionary.u16(tree);
            var dict32 = TaoDictionary.u32(tree);
            assertEquals(2, LeafField.dict16("x", dict16).width());
            assertEquals(4, LeafField.dict32("x", dict32).width());
        }
    }

    // ---- LeafLayout ----

    @Test
    void layoutOffsetsAndWidth() {
        var layout = LeafLayout.of(
            LeafField.int32("count"),
            LeafField.int64("timestamp"),
            LeafField.float64("lat")
        );
        assertEquals(3, layout.fieldCount());
        assertEquals(0,  layout.offset(0));  // int32 @ 0
        assertEquals(4,  layout.offset(1));  // int64 @ 4
        assertEquals(12, layout.offset(2));  // float64 @ 12
        assertEquals(20, layout.totalWidth());
    }

    @Test
    void layoutWithStringAndJson() {
        var layout = LeafLayout.of(
            LeafField.int32("count"),
            LeafField.string("name"),
            LeafField.json("extras")
        );
        assertEquals(0,  layout.offset(0));  // int32 @ 0
        assertEquals(4,  layout.offset(1));  // string(16B) @ 4
        assertEquals(20, layout.offset(2));  // json(16B) @ 20
        assertEquals(36, layout.totalWidth());
    }

    @Test
    void layoutFieldIndexLookup() {
        var layout = LeafLayout.of(
            LeafField.int32("a"),
            LeafField.int64("b"),
            LeafField.float64("c")
        );
        assertEquals(0, layout.fieldIndex("a"));
        assertEquals(1, layout.fieldIndex("b"));
        assertEquals(2, layout.fieldIndex("c"));
        assertThrows(IllegalArgumentException.class, () -> layout.fieldIndex("missing"));
    }

    @Test
    void layoutRejectsNoFields() {
        assertThrows(IllegalArgumentException.class, () -> LeafLayout.of());
    }

    @Test
    void hasStringFields() {
        assertFalse(LeafLayout.of(LeafField.int32("x")).hasStringFields());
        assertTrue(LeafLayout.of(LeafField.string("x")).hasStringFields());
        assertTrue(LeafLayout.of(LeafField.int32("x"), LeafField.json("y")).hasStringFields());
    }

    // ---- LeafAccessor — numeric fields ----

    @Test
    void numericRoundTrip() {
        var layout = LeafLayout.of(
            LeafField.int8("a"),
            LeafField.int16("b"),
            LeafField.int32("c"),
            LeafField.int64("d"),
            LeafField.float32("e"),
            LeafField.float64("f")
        );

        try (var tree = TaoTree.open(4, layout.totalWidth())) {
            byte[] key = {0, 0, 0, 1};
            try (var w = tree.write()) {
                long ptr = w.getOrCreate(key);
                var leaf = w.leaf(ptr, layout);
                leaf.setInt8("a", (byte) 42)
                    .setInt16("b", (short) 1000)
                    .setInt32("c", 123456)
                    .setInt64("d", Long.MAX_VALUE)
                    .setFloat32("e", 3.14f)
                    .setFloat64("f", 2.718281828);
            }
            try (var r = tree.read()) {
                long ptr = r.lookup(key);
                var leaf = r.leaf(ptr, layout);
                assertEquals((byte) 42, leaf.getInt8("a"));
                assertEquals((short) 1000, leaf.getInt16("b"));
                assertEquals(123456, leaf.getInt32("c"));
                assertEquals(Long.MAX_VALUE, leaf.getInt64("d"));
                assertEquals(3.14f, leaf.getFloat32("e"));
                assertEquals(2.718281828, leaf.getFloat64("f"));
            }
        }
    }

    // ---- LeafAccessor — string field ----

    @Test
    void stringFieldRoundTrip() {
        var layout = LeafLayout.of(
            LeafField.int32("count"),
            LeafField.string("name")
        );

        try (var tree = TaoTree.open(4, layout.totalWidth())) {
            byte[] key = {0, 0, 0, 1};
            try (var w = tree.write()) {
                long ptr = w.getOrCreate(key);
                w.leaf(ptr, layout)
                    .setInt32("count", 7)
                    .setString("name", "Bald Eagle");
            }
            try (var r = tree.read()) {
                long ptr = r.lookup(key);
                var leaf = r.leaf(ptr, layout);
                assertEquals(7, leaf.getInt32("count"));
                assertEquals("Bald Eagle", leaf.getString("name"));
            }
        }
    }

    @Test
    void longStringOverflow() {
        var layout = LeafLayout.of(LeafField.string("desc"));

        try (var tree = TaoTree.open(4, layout.totalWidth())) {
            byte[] key = {0, 0, 0, 1};
            String longStr = "A".repeat(200); // > 12 bytes, triggers overflow
            try (var w = tree.write()) {
                long ptr = w.getOrCreate(key);
                w.leaf(ptr, layout).setString("desc", longStr);
            }
            try (var r = tree.read()) {
                long ptr = r.lookup(key);
                assertEquals(longStr, r.leaf(ptr, layout).getString("desc"));
            }
        }
    }

    // ---- LeafAccessor — JSON field ----

    @Test
    void jsonFieldRoundTrip() {
        var layout = LeafLayout.of(
            LeafField.int32("count"),
            LeafField.json("extras")
        );

        try (var tree = TaoTree.open(4, layout.totalWidth())) {
            byte[] key = {0, 0, 0, 1};
            String json = """
                {"elevation":1897,"notes":"near lake"}""";
            try (var w = tree.write()) {
                long ptr = w.getOrCreate(key);
                w.leaf(ptr, layout)
                    .setInt32("count", 1)
                    .setJson("extras", json);
            }
            try (var r = tree.read()) {
                long ptr = r.lookup(key);
                var leaf = r.leaf(ptr, layout);
                assertEquals(1, leaf.getInt32("count"));
                assertEquals(json, leaf.getJson("extras"));
            }
        }
    }

    // ---- LeafAccessor — dict fields ----

    @Test
    void dictFieldRoundTrip() {
        try (var tree = TaoTree.forDictionaries()) {
            var statusDict = TaoDictionary.u16(tree);
            var tagDict = TaoDictionary.u32(tree);

            var layout = LeafLayout.of(
                LeafField.dict16("status", statusDict),
                LeafField.dict32("tag", tagDict),
                LeafField.int32("count")
            );

            var dataTree = new TaoTree(tree, 4, new int[]{layout.totalWidth()});
            byte[] key = {0, 0, 0, 1};

            try (var w = dataTree.write()) {
                long ptr = w.getOrCreate(key);
                w.leaf(ptr, layout)
                    .setDict16("status", "active")
                    .setDict32("tag", "important")
                    .setInt32("count", 42);
            }

            try (var r = dataTree.read()) {
                long ptr = r.lookup(key);
                var leaf = r.leaf(ptr, layout);
                assertEquals(42, leaf.getInt32("count"));
                // Dict codes are > 0 for non-null values
                assertTrue(leaf.getDict16Code("status") > 0);
                assertTrue(leaf.getDict32Code("tag") > 0);
                // Same string → same code
                assertEquals(statusDict.resolve("active"), leaf.getDict16Code("status"));
                assertEquals(tagDict.resolve("important"), leaf.getDict32Code("tag"));
            }
        }
    }

    // ---- Layout-based tree factory ----

    @Test
    void treeFromLayouts() {
        var keyLayout = KeyLayout.of(
            KeyField.uint32("id")
        );
        var leafLayout = LeafLayout.of(
            LeafField.int64("value"),
            LeafField.string("name")
        );

        try (var tree = TaoTree.open(keyLayout, leafLayout)) {
            assertEquals(4, tree.keyLen());

            try (var arena = Arena.ofConfined()) {
                var kb = new KeyBuilder(keyLayout, arena);
                kb.setU32(0, 1);

                try (var w = tree.write()) {
                    long ptr = w.getOrCreate(kb.key());
                    w.leaf(ptr, leafLayout)
                        .setInt64("value", 999L)
                        .setString("name", "test entry");
                }

                try (var r = tree.read()) {
                    long ptr = r.lookup(kb.key());
                    assertNotEquals(TaoTree.NOT_FOUND, ptr);
                    var leaf = r.leaf(ptr, leafLayout);
                    assertEquals(999L, leaf.getInt64("value"));
                    assertEquals("test entry", leaf.getString("name"));
                }
            }
        }
    }

    // ---- Setter chaining ----

    @Test
    void setterChaining() {
        var layout = LeafLayout.of(
            LeafField.int32("a"),
            LeafField.int64("b")
        );

        try (var tree = TaoTree.open(4, layout.totalWidth())) {
            byte[] key = {0, 0, 0, 1};
            try (var w = tree.write()) {
                long ptr = w.getOrCreate(key);
                var leaf = w.leaf(ptr, layout);
                LeafAccessor result = leaf.setInt32("a", 1).setInt64("b", 2L);
                assertSame(leaf, result, "Chained setters should return 'this'");
            }
        }
    }

    // ---- Read-only enforcement ----

    @Test
    void readOnlyAccessorRejectsWrites() {
        var layout = LeafLayout.of(LeafField.int32("x"));

        try (var tree = TaoTree.open(4, layout.totalWidth())) {
            byte[] key = {0, 0, 0, 1};
            try (var w = tree.write()) {
                w.getOrCreate(key);
            }
            try (var r = tree.read()) {
                long ptr = r.lookup(key);
                var leaf = r.leaf(ptr, layout);
                assertThrows(IllegalArgumentException.class,
                    () -> leaf.setInt32("x", 42));
            }
        }
    }

    // ---- Mutation-killing: LeafAccessor.segment() and layout() getters ----

    @Test
    void accessorSegmentAndLayoutGetters() {
        var layout = LeafLayout.of(LeafField.int32("x"));

        try (var tree = TaoTree.open(4, layout.totalWidth())) {
            byte[] key = {0, 0, 0, 1};
            try (var w = tree.write()) {
                long ptr = w.getOrCreate(key);
                var leaf = w.leaf(ptr, layout);

                assertNotNull(leaf.segment(), "segment() must not return null");
                assertSame(layout, leaf.layout(), "layout() must return the same layout");
                assertEquals(layout.totalWidth(), leaf.segment().byteSize(),
                    "segment size must match layout width");
            }
        }
    }

    // ---- Mutation-killing: index-based getters/setters ----

    @Test
    void indexBasedAccess() {
        var layout = LeafLayout.of(
            LeafField.int32("a"),
            LeafField.float64("b")
        );

        try (var tree = TaoTree.open(4, layout.totalWidth())) {
            byte[] key = {0, 0, 0, 1};
            try (var w = tree.write()) {
                long ptr = w.getOrCreate(key);
                var leaf = w.leaf(ptr, layout);
                // Use index-based setters (index 0 = int32, index 1 = float64)
                leaf.setInt32(0, 99).setFloat64(1, 1.5);
            }
            try (var r = tree.read()) {
                long ptr = r.lookup(key);
                var leaf = r.leaf(ptr, layout);
                assertEquals(99, leaf.getInt32(0));
                assertEquals(1.5, leaf.getFloat64(1));
            }
        }
    }

    // ---- Mutation-killing: dict field type check errors ----

    @Test
    void setDict16OnNonDictFieldThrows() {
        var layout = LeafLayout.of(LeafField.int32("count"));

        try (var tree = TaoTree.open(4, layout.totalWidth())) {
            byte[] key = {0, 0, 0, 1};
            try (var w = tree.write()) {
                long ptr = w.getOrCreate(key);
                var leaf = w.leaf(ptr, layout);
                assertThrows(IllegalArgumentException.class,
                    () -> leaf.setDict16(0, "value"),
                    "setDict16 on an int32 field must throw");
            }
        }
    }

    @Test
    void setDict32OnNonDictFieldThrows() {
        var layout = LeafLayout.of(LeafField.int64("count"));

        try (var tree = TaoTree.open(4, layout.totalWidth())) {
            byte[] key = {0, 0, 0, 1};
            try (var w = tree.write()) {
                long ptr = w.getOrCreate(key);
                var leaf = w.leaf(ptr, layout);
                assertThrows(IllegalArgumentException.class,
                    () -> leaf.setDict32(0, "value"),
                    "setDict32 on an int64 field must throw");
            }
        }
    }

    // ---- Mutation-killing: LeafLayout.stringLayouts() ----

    @Test
    void stringLayoutsForMixedFields() {
        var layout = LeafLayout.of(
            LeafField.int32("count"),
            LeafField.string("name"),
            LeafField.float64("lat"),
            LeafField.json("extras")
        );
        var sls = layout.stringLayouts();
        assertEquals(2, sls.length, "Two string/json fields → two StringLayouts");

        // First string field "name" is at offset 4 (after int32)
        assertEquals(4, sls[0].lenOffset());
        assertEquals(4 + 8, sls[0].ptrOffset());
        assertEquals(TaoString.SHORT_THRESHOLD, sls[0].inlineThreshold());

        // Second json field "extras" is at offset 4+16+8=28
        assertEquals(28, sls[1].lenOffset());
        assertEquals(28 + 8, sls[1].ptrOffset());
    }

    @Test
    void stringLayoutsEmptyForNumericOnly() {
        var layout = LeafLayout.of(
            LeafField.int32("a"),
            LeafField.float64("b")
        );
        assertEquals(0, layout.stringLayouts().length);
    }

    // ---- Mutation-killing: all int types by index ----

    @Test
    void int8AndInt16ByIndex() {
        var layout = LeafLayout.of(
            LeafField.int8("a"),
            LeafField.int16("b")
        );

        try (var tree = TaoTree.open(4, layout.totalWidth())) {
            byte[] key = {0, 0, 0, 1};
            try (var w = tree.write()) {
                long ptr = w.getOrCreate(key);
                w.leaf(ptr, layout)
                    .setInt8(0, (byte) -42)
                    .setInt16(1, (short) 12345);
            }
            try (var r = tree.read()) {
                long ptr = r.lookup(key);
                var leaf = r.leaf(ptr, layout);
                assertEquals((byte) -42, leaf.getInt8(0));
                assertEquals((short) 12345, leaf.getInt16(1));
            }
        }
    }

    @Test
    void float32ByIndex() {
        var layout = LeafLayout.of(LeafField.float32("f"));

        try (var tree = TaoTree.open(4, layout.totalWidth())) {
            byte[] key = {0, 0, 0, 1};
            try (var w = tree.write()) {
                long ptr = w.getOrCreate(key);
                w.leaf(ptr, layout).setFloat32(0, 2.5f);
            }
            try (var r = tree.read()) {
                long ptr = r.lookup(key);
                assertEquals(2.5f, r.leaf(ptr, layout).getFloat32(0));
            }
        }
    }

    // ---- Mutation-killing: dict code getters ----

    @Test
    void dictCodeGettersByName() {
        try (var tree = TaoTree.forDictionaries()) {
            var d16 = TaoDictionary.u16(tree);
            var d32 = TaoDictionary.u32(tree);

            var layout = LeafLayout.of(
                LeafField.dict16("status", d16),
                LeafField.dict32("tag", d32)
            );

            var dataTree = new TaoTree(tree, 4, new int[]{layout.totalWidth()});
            byte[] key = {0, 0, 0, 1};

            try (var w = dataTree.write()) {
                long ptr = w.getOrCreate(key);
                w.leaf(ptr, layout)
                    .setDict16("status", "open")
                    .setDict32("tag", "bug");
            }

            try (var r = dataTree.read()) {
                long ptr = r.lookup(key);
                var leaf = r.leaf(ptr, layout);
                assertEquals(d16.resolve("open"), leaf.getDict16Code("status"));
                assertEquals(d32.resolve("bug"), leaf.getDict32Code("tag"));
            }
        }
    }

    // ---- Mutation-killing: dict null encodes as 0 ----

    @Test
    void dictNullEncodesZero() {
        try (var tree = TaoTree.forDictionaries()) {
            var dict = TaoDictionary.u16(tree);
            var layout = LeafLayout.of(LeafField.dict16("d", dict));

            var dataTree = new TaoTree(tree, 4, new int[]{layout.totalWidth()});
            byte[] key = {0, 0, 0, 1};

            try (var w = dataTree.write()) {
                long ptr = w.getOrCreate(key);
                w.leaf(ptr, layout).setDict16("d", null);
            }
            try (var r = dataTree.read()) {
                long ptr = r.lookup(key);
                assertEquals(0, r.leaf(ptr, layout).getDict16Code("d"));
            }
        }
    }
}
