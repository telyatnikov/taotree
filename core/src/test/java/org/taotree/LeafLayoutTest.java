package org.taotree;

import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.file.Path;
import org.junit.jupiter.api.io.TempDir;

import java.lang.foreign.Arena;

import static org.junit.jupiter.api.Assertions.*;
import org.taotree.layout.KeyBuilder;
import org.taotree.layout.KeyField;
import org.taotree.layout.KeyLayout;
import org.taotree.layout.LeafField;
import org.taotree.layout.LeafLayout;

import org.taotree.layout.LeafHandle;

/**
 * Tests for {@link LeafField}, {@link LeafLayout}, and {@link LeafAccessor}.
 */
class LeafLayoutTest {

    @TempDir Path tmp;
    private int fc;

    // ---- LeafField widths ----

    @Test
    void fieldWidths() throws IOException {
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
    void dictFieldWidths() throws IOException {
        try (var tree = TaoTree.forDictionaries(tmp.resolve(fc++ + ".tao"))) {
            var dict16 = TaoDictionary.u16(tree);
            var dict32 = TaoDictionary.u32(tree);
            assertEquals(2, LeafField.dict16("x", dict16).width());
            assertEquals(4, LeafField.dict32("x", dict32).width());
        }
    }

    // ---- LeafLayout ----

    @Test
    void layoutOffsetsAndWidth() throws IOException {
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
    void layoutWithStringAndJson() throws IOException {
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
    void layoutFieldIndexLookup() throws IOException {
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
    void layoutRejectsNoFields() throws IOException {
        assertThrows(IllegalArgumentException.class, () -> LeafLayout.of());
    }

    @Test
    void hasStringFields() throws IOException {
        assertFalse(LeafLayout.of(LeafField.int32("x")).hasStringFields());
        assertTrue(LeafLayout.of(LeafField.string("x")).hasStringFields());
        assertTrue(LeafLayout.of(LeafField.int32("x"), LeafField.json("y")).hasStringFields());
    }

    // ---- LeafAccessor — numeric fields ----

    @Test
    void numericRoundTrip() throws IOException {
        var layout = LeafLayout.of(
            LeafField.int8("a"),
            LeafField.int16("b"),
            LeafField.int32("c"),
            LeafField.int64("d"),
            LeafField.float32("e"),
            LeafField.float64("f")
        );

        try (var tree = TaoTree.create(tmp.resolve(fc++ + ".tao"), 4, layout.totalWidth())) {
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
    void stringFieldRoundTrip() throws IOException {
        var layout = LeafLayout.of(
            LeafField.int32("count"),
            LeafField.string("name")
        );

        try (var tree = TaoTree.create(tmp.resolve(fc++ + ".tao"), 4, layout.totalWidth())) {
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
    void longStringOverflow() throws IOException {
        var layout = LeafLayout.of(LeafField.string("desc"));

        try (var tree = TaoTree.create(tmp.resolve(fc++ + ".tao"), 4, layout.totalWidth())) {
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
    void jsonFieldRoundTrip() throws IOException {
        var layout = LeafLayout.of(
            LeafField.int32("count"),
            LeafField.json("extras")
        );

        try (var tree = TaoTree.create(tmp.resolve(fc++ + ".tao"), 4, layout.totalWidth())) {
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
    void dictFieldRoundTrip() throws IOException {
        try (var tree = TaoTree.forDictionaries(tmp.resolve(fc++ + ".tao"))) {
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
    void treeFromLayouts() throws IOException {
        var keyLayout = KeyLayout.of(
            KeyField.uint32("id")
        );
        var leafLayout = LeafLayout.of(
            LeafField.int64("value"),
            LeafField.string("name")
        );

        try (var tree = TaoTree.create(tmp.resolve(fc++ + ".tao"), keyLayout, leafLayout)) {
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
    void setterChaining() throws IOException {
        var layout = LeafLayout.of(
            LeafField.int32("a"),
            LeafField.int64("b")
        );

        try (var tree = TaoTree.create(tmp.resolve(fc++ + ".tao"), 4, layout.totalWidth())) {
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
    void readOnlyAccessorRejectsWrites() throws IOException {
        var layout = LeafLayout.of(LeafField.int32("x"));

        try (var tree = TaoTree.create(tmp.resolve(fc++ + ".tao"), 4, layout.totalWidth())) {
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
    void accessorSegmentAndLayoutGetters() throws IOException {
        var layout = LeafLayout.of(LeafField.int32("x"));

        try (var tree = TaoTree.create(tmp.resolve(fc++ + ".tao"), 4, layout.totalWidth())) {
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
    void indexBasedAccess() throws IOException {
        var layout = LeafLayout.of(
            LeafField.int32("a"),
            LeafField.float64("b")
        );

        try (var tree = TaoTree.create(tmp.resolve(fc++ + ".tao"), 4, layout.totalWidth())) {
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
    void setDict16OnNonDictFieldThrows() throws IOException {
        var layout = LeafLayout.of(LeafField.int32("count"));

        try (var tree = TaoTree.create(tmp.resolve(fc++ + ".tao"), 4, layout.totalWidth())) {
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
    void setDict32OnNonDictFieldThrows() throws IOException {
        var layout = LeafLayout.of(LeafField.int64("count"));

        try (var tree = TaoTree.create(tmp.resolve(fc++ + ".tao"), 4, layout.totalWidth())) {
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
    void stringLayoutsForMixedFields() throws IOException {
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
    void stringLayoutsEmptyForNumericOnly() throws IOException {
        var layout = LeafLayout.of(
            LeafField.int32("a"),
            LeafField.float64("b")
        );
        assertEquals(0, layout.stringLayouts().length);
    }

    // ---- Mutation-killing: all int types by index ----

    @Test
    void int8AndInt16ByIndex() throws IOException {
        var layout = LeafLayout.of(
            LeafField.int8("a"),
            LeafField.int16("b")
        );

        try (var tree = TaoTree.create(tmp.resolve(fc++ + ".tao"), 4, layout.totalWidth())) {
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
    void float32ByIndex() throws IOException {
        var layout = LeafLayout.of(LeafField.float32("f"));

        try (var tree = TaoTree.create(tmp.resolve(fc++ + ".tao"), 4, layout.totalWidth())) {
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
    void dictCodeGettersByName() throws IOException {
        try (var tree = TaoTree.forDictionaries(tmp.resolve(fc++ + ".tao"))) {
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
    void dictNullEncodesZero() throws IOException {
        try (var tree = TaoTree.forDictionaries(tmp.resolve(fc++ + ".tao"))) {
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

    // ====================================================================
    // Nullable fields
    // ====================================================================

    @Test
    void nullableFieldWidth() {
        // Nullable wrapper delegates width to inner field
        assertEquals(4, LeafField.int32("x").nullable().width());
        assertEquals(8, LeafField.float64("x").nullable().width());
        assertEquals(16, LeafField.string("x").nullable().width());
    }

    @Test
    void nullableFieldName() {
        assertEquals("year", LeafField.int32("year").nullable().name());
    }

    @Test
    void doubleNullableThrows() {
        var f = LeafField.int32("x").nullable();
        assertThrows(IllegalArgumentException.class, f::nullable);
    }

    @Test
    void nullBitmapSizeZeroWhenNoNullable() {
        var layout = LeafLayout.of(LeafField.int32("a"), LeafField.int64("b"));
        assertEquals(0, layout.nullBitmapSize());
        assertEquals(12, layout.totalWidth()); // 4 + 8, no bitmap
    }

    @Test
    void nullBitmapSizeOneByteForUpTo8() {
        var layout = LeafLayout.of(
            LeafField.int32("a").nullable(),
            LeafField.int32("b").nullable(),
            LeafField.int32("c")
        );
        assertEquals(1, layout.nullBitmapSize());
        // 4+4+4 = 12 fields + 1 bitmap = 13
        assertEquals(13, layout.totalWidth());
    }

    @Test
    void nullBitmapSizeTwoBytesFor9() {
        // 9 nullable fields → 2 bitmap bytes
        var layout = LeafLayout.of(
            LeafField.int8("a").nullable(),
            LeafField.int8("b").nullable(),
            LeafField.int8("c").nullable(),
            LeafField.int8("d").nullable(),
            LeafField.int8("e").nullable(),
            LeafField.int8("f").nullable(),
            LeafField.int8("g").nullable(),
            LeafField.int8("h").nullable(),
            LeafField.int8("i").nullable()
        );
        assertEquals(2, layout.nullBitmapSize());
        assertEquals(9 + 2, layout.totalWidth());
    }

    @Test
    void nullBitIndicesAssigned() {
        var layout = LeafLayout.of(
            LeafField.int32("a"),
            LeafField.int32("b").nullable(),
            LeafField.int32("c"),
            LeafField.int32("d").nullable()
        );
        assertEquals(-1, layout.nullBitIndex(0)); // a: not nullable
        assertEquals(0, layout.nullBitIndex(1));   // b: first nullable → bit 0
        assertEquals(-1, layout.nullBitIndex(2)); // c: not nullable
        assertEquals(1, layout.nullBitIndex(3));   // d: second nullable → bit 1
    }

    @Test
    void nullableRoundTrip() throws IOException {
        var layout = LeafLayout.of(
            LeafField.int32("count"),
            LeafField.int32("year").nullable(),
            LeafField.float64("elevation").nullable(),
            LeafField.string("locality").nullable()
        );
        var COUNT = layout.int32("count");
        var YEAR  = layout.int32("year");
        var ELEV  = layout.float64("elevation");
        var LOC   = layout.string("locality");

        assertTrue(YEAR.isNullable());
        assertTrue(ELEV.isNullable());
        assertTrue(LOC.isNullable());
        assertFalse(COUNT.isNullable());

        try (var tree = TaoTree.create(tmp.resolve(fc++ + ".tao"), 4, layout.totalWidth())) {
            byte[] key = {0, 0, 0, 1};
            try (var w = tree.write()) {
                long ptr = w.getOrCreate(key);
                var leaf = w.leaf(ptr, layout);
                // Set count (non-nullable) and year (nullable)
                leaf.set(COUNT, 42).set(YEAR, 2024);
                // Set elevation, then null it
                leaf.set(ELEV, 1897.5);
                assertFalse(leaf.isNull(ELEV));
                leaf.setNull(ELEV);
                assertTrue(leaf.isNull(ELEV));
                // Leave locality unset (zero-initialized → should be null if bitmap bit is set)
                leaf.setNull(LOC);
            }
            try (var r = tree.read()) {
                long ptr = r.lookup(key);
                var leaf = r.leaf(ptr, layout);
                assertEquals(42, leaf.get(COUNT));
                assertFalse(leaf.isNull(COUNT)); // non-nullable always false
                assertFalse(leaf.isNull(YEAR));
                assertEquals(2024, leaf.get(YEAR));
                assertTrue(leaf.isNull(ELEV));
                assertTrue(leaf.isNull(LOC));
            }
        }
    }

    @Test
    void setAutoClearsNullBit() throws IOException {
        var layout = LeafLayout.of(LeafField.int32("x").nullable());
        var X = layout.int32("x");

        try (var tree = TaoTree.create(tmp.resolve(fc++ + ".tao"), 4, layout.totalWidth())) {
            byte[] key = {0, 0, 0, 1};
            try (var w = tree.write()) {
                long ptr = w.getOrCreate(key);
                var leaf = w.leaf(ptr, layout);
                leaf.setNull(X);
                assertTrue(leaf.isNull(X));
                leaf.set(X, 99);
                assertFalse(leaf.isNull(X));
                assertEquals(99, leaf.get(X));
            }
        }
    }

    @Test
    void setNullOnNonNullableThrows() throws IOException {
        var layout = LeafLayout.of(LeafField.int32("x"));
        var X = layout.int32("x");
        assertFalse(X.isNullable());

        try (var tree = TaoTree.create(tmp.resolve(fc++ + ".tao"), 4, layout.totalWidth())) {
            byte[] key = {0, 0, 0, 1};
            try (var w = tree.write()) {
                long ptr = w.getOrCreate(key);
                var leaf = w.leaf(ptr, layout);
                assertThrows(IllegalStateException.class, () -> leaf.setNull(X));
            }
        }
    }

    @Test
    void isNullAlwaysFalseForNonNullable() throws IOException {
        var layout = LeafLayout.of(LeafField.int32("x"));
        var X = layout.int32("x");

        try (var tree = TaoTree.create(tmp.resolve(fc++ + ".tao"), 4, layout.totalWidth())) {
            byte[] key = {0, 0, 0, 1};
            try (var w = tree.write()) {
                w.getOrCreate(key);
            }
            try (var r = tree.read()) {
                long ptr = r.lookup(key);
                var leaf = r.leaf(ptr, layout);
                assertFalse(leaf.isNull(X));
            }
        }
    }

    @Test
    void nullableHandleFactoriesForAllTypes() throws IOException {
        try (var tree = TaoTree.forDictionaries(tmp.resolve(fc++ + ".tao"))) {
            var d16 = TaoDictionary.u16(tree);
            var d32 = TaoDictionary.u32(tree);

            var layout = LeafLayout.of(
                LeafField.int8("a").nullable(),
                LeafField.int16("b").nullable(),
                LeafField.int32("c").nullable(),
                LeafField.int64("d").nullable(),
                LeafField.float32("e").nullable(),
                LeafField.float64("f").nullable(),
                LeafField.string("g").nullable(),
                LeafField.json("h").nullable(),
                LeafField.dict16("i", d16).nullable(),
                LeafField.dict32("j", d32).nullable()
            );

            assertTrue(layout.int8("a").isNullable());
            assertTrue(layout.int16("b").isNullable());
            assertTrue(layout.int32("c").isNullable());
            assertTrue(layout.int64("d").isNullable());
            assertTrue(layout.float32("e").isNullable());
            assertTrue(layout.float64("f").isNullable());
            assertTrue(layout.string("g").isNullable());
            assertTrue(layout.json("h").isNullable());
            assertTrue(layout.dict16("i").isNullable());
            assertTrue(layout.dict32("j").isNullable());
        }
    }

    @Test
    void nullableFloat64RoundTrip() throws IOException {
        var layout = LeafLayout.of(
            LeafField.float64("lat").nullable(),
            LeafField.float64("lon").nullable()
        );
        var LAT = layout.float64("lat");
        var LON = layout.float64("lon");

        try (var tree = TaoTree.create(tmp.resolve(fc++ + ".tao"), 4, layout.totalWidth())) {
            byte[] key = {0, 0, 0, 1};
            try (var w = tree.write()) {
                long ptr = w.getOrCreate(key);
                var leaf = w.leaf(ptr, layout);
                leaf.set(LAT, 48.8566);
                leaf.setNull(LON);
            }
            try (var r = tree.read()) {
                long ptr = r.lookup(key);
                var leaf = r.leaf(ptr, layout);
                assertFalse(leaf.isNull(LAT));
                assertEquals(48.8566, leaf.get(LAT));
                assertTrue(leaf.isNull(LON));
                assertEquals(0.0, leaf.get(LON)); // zeroed bytes → 0.0
            }
        }
    }

    // ====================================================================
    // Extras (schemaless) fields
    // ====================================================================

    @Test
    void extrasFieldWidth() {
        assertEquals(TaoString.SIZE, LeafField.extras().width());
        assertEquals(TaoString.SIZE, LeafField.extras("myExtras").width());
    }

    @Test
    void extrasDefaultName() {
        assertEquals("_extras", LeafField.extras().name());
        assertEquals("myExtras", LeafField.extras("myExtras").name());
    }

    @Test
    void multipleExtrasThrows() {
        assertThrows(IllegalArgumentException.class, () ->
            LeafLayout.of(LeafField.extras("a"), LeafField.extras("b")));
    }

    @Test
    void extrasWriterReaderRoundTrip() throws IOException {
        var layout = LeafLayout.of(
            LeafField.int32("count"),
            LeafField.extras("extras")
        );
        var COUNT  = layout.int32("count");
        var EXTRAS = layout.extras("extras");

        try (var tree = TaoTree.create(tmp.resolve(fc++ + ".tao"), 4, layout.totalWidth())) {
            byte[] key = {0, 0, 0, 1};
            try (var w = tree.write()) {
                long ptr = w.getOrCreate(key);
                var leaf = w.leaf(ptr, layout);
                leaf.set(COUNT, 7);
                leaf.extrasWriter(EXTRAS)
                    .put("basisOfRecord", "HUMAN_OBSERVATION")
                    .put("coordinateUncertainty", 50.0)
                    .put("taxonKey", 12345)
                    .put("verified", true)
                    .write();
            }
            try (var r = tree.read()) {
                long ptr = r.lookup(key);
                var leaf = r.leaf(ptr, layout);
                assertEquals(7, leaf.get(COUNT));

                var er = leaf.extrasReader(EXTRAS);
                assertFalse(er.isEmpty());
                assertEquals("HUMAN_OBSERVATION", er.getString("basisOfRecord"));
                assertEquals(50.0, er.getDouble("coordinateUncertainty"));
                assertEquals(12345, er.getInt("taxonKey"));
                assertEquals(true, er.getBoolean("verified"));
                assertNull(er.getString("absent"));
                assertFalse(er.has("absent"));
                assertTrue(er.has("basisOfRecord"));
            }
        }
    }

    @Test
    void extrasRawGetSet() throws IOException {
        var layout = LeafLayout.of(LeafField.extras("e"));
        var E = layout.extras("e");

        try (var tree = TaoTree.create(tmp.resolve(fc++ + ".tao"), 4, layout.totalWidth())) {
            byte[] key = {0, 0, 0, 1};
            String json = "{\"foo\":\"bar\"}";
            try (var w = tree.write()) {
                long ptr = w.getOrCreate(key);
                w.leaf(ptr, layout).set(E, json);
            }
            try (var r = tree.read()) {
                long ptr = r.lookup(key);
                assertEquals(json, r.leaf(ptr, layout).get(E));
            }
        }
    }

    @Test
    void extrasEmptyWrite() throws IOException {
        var layout = LeafLayout.of(LeafField.extras("e"));
        var E = layout.extras("e");

        try (var tree = TaoTree.create(tmp.resolve(fc++ + ".tao"), 4, layout.totalWidth())) {
            byte[] key = {0, 0, 0, 1};
            try (var w = tree.write()) {
                long ptr = w.getOrCreate(key);
                var leaf = w.leaf(ptr, layout);
                var ew = leaf.extrasWriter(E);
                assertTrue(ew.isEmpty());
                ew.write(); // writes empty
            }
            try (var r = tree.read()) {
                long ptr = r.lookup(key);
                var er = r.leaf(ptr, layout).extrasReader(E);
                assertTrue(er.isEmpty());
                assertNull(er.getString("any"));
            }
        }
    }

    @Test
    void extrasNullValuesSkipped() throws IOException {
        var layout = LeafLayout.of(LeafField.extras("e"));
        var E = layout.extras("e");

        try (var tree = TaoTree.create(tmp.resolve(fc++ + ".tao"), 4, layout.totalWidth())) {
            byte[] key = {0, 0, 0, 1};
            try (var w = tree.write()) {
                long ptr = w.getOrCreate(key);
                w.leaf(ptr, layout).extrasWriter(E)
                    .put("present", "yes")
                    .put("absent", (String) null)
                    .write();
            }
            try (var r = tree.read()) {
                long ptr = r.lookup(key);
                var er = r.leaf(ptr, layout).extrasReader(E);
                assertEquals("yes", er.getString("present"));
                assertNull(er.getString("absent"));
            }
        }
    }

    @Test
    void extrasNanSkipped() throws IOException {
        var layout = LeafLayout.of(LeafField.extras("e"));
        var E = layout.extras("e");

        try (var tree = TaoTree.create(tmp.resolve(fc++ + ".tao"), 4, layout.totalWidth())) {
            byte[] key = {0, 0, 0, 1};
            try (var w = tree.write()) {
                long ptr = w.getOrCreate(key);
                w.leaf(ptr, layout).extrasWriter(E)
                    .put("real", 1.5)
                    .put("nan", Double.NaN)
                    .write();
            }
            try (var r = tree.read()) {
                long ptr = r.lookup(key);
                var er = r.leaf(ptr, layout).extrasReader(E);
                assertEquals(1.5, er.getDouble("real"));
                assertNull(er.getDouble("nan")); // NaN was skipped
            }
        }
    }

    @Test
    void extrasNullable() throws IOException {
        var layout = LeafLayout.of(LeafField.extras("e").nullable());
        var E = layout.extras("e");
        assertTrue(E.isNullable());

        try (var tree = TaoTree.create(tmp.resolve(fc++ + ".tao"), 4, layout.totalWidth())) {
            byte[] key = {0, 0, 0, 1};
            try (var w = tree.write()) {
                long ptr = w.getOrCreate(key);
                var leaf = w.leaf(ptr, layout);
                leaf.extrasWriter(E).put("a", "b").write();
                assertFalse(leaf.isNull(E));
                leaf.setNull(E);
                assertTrue(leaf.isNull(E));
            }
        }
    }

    @Test
    void extrasLongStringOverflow() throws IOException {
        var layout = LeafLayout.of(LeafField.extras("e"));
        var E = layout.extras("e");

        try (var tree = TaoTree.create(tmp.resolve(fc++ + ".tao"), 4, layout.totalWidth())) {
            byte[] key = {0, 0, 0, 1};
            String longVal = "X".repeat(200);
            try (var w = tree.write()) {
                long ptr = w.getOrCreate(key);
                w.leaf(ptr, layout).extrasWriter(E)
                    .put("long", longVal)
                    .write();
            }
            try (var r = tree.read()) {
                long ptr = r.lookup(key);
                var er = r.leaf(ptr, layout).extrasReader(E);
                assertEquals(longVal, er.getString("long"));
            }
        }
    }

    @Test
    void extrasStringLayoutsIncluded() {
        var layout = LeafLayout.of(
            LeafField.int32("count"),
            LeafField.extras("extras")
        );
        assertTrue(layout.hasStringFields());
        assertEquals(1, layout.stringLayouts().length);
    }

    @Test
    void extrasEscapedStrings() throws IOException {
        var layout = LeafLayout.of(LeafField.extras("e"));
        var E = layout.extras("e");

        try (var tree = TaoTree.create(tmp.resolve(fc++ + ".tao"), 4, layout.totalWidth())) {
            byte[] key = {0, 0, 0, 1};
            try (var w = tree.write()) {
                long ptr = w.getOrCreate(key);
                w.leaf(ptr, layout).extrasWriter(E)
                    .put("quote", "He said \"hello\"")
                    .put("backslash", "path\\to\\file")
                    .write();
            }
            try (var r = tree.read()) {
                long ptr = r.lookup(key);
                var er = r.leaf(ptr, layout).extrasReader(E);
                assertEquals("He said \"hello\"", er.getString("quote"));
                assertEquals("path\\to\\file", er.getString("backslash"));
            }
        }
    }
}
