package org.taotree.layout;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.taotree.TaoKey;
import org.taotree.TaoTree;
import org.taotree.Value;

import java.io.IOException;
import java.lang.foreign.Arena;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Direct coverage for {@link KeyBuilder}: every type-specific handle setter,
 * the "not-bound dict" error path, null-value encoding, and setter chaining.
 */
class KeyBuilderTest {

    @TempDir Path tmp;
    private int fc;
    private Path path() { return tmp.resolve(fc++ + ".taotree"); }

    @Test
    void allHandleSettersEncodeBigEndianAndChain() throws IOException {
        var layout = KeyLayout.of(
                KeyField.uint8("a"),
                KeyField.uint16("b"),
                KeyField.uint32("c"),
                KeyField.uint64("d"),
                KeyField.int64("e"));
        try (var tree = TaoTree.create(path(), layout);
             var arena = Arena.ofConfined()) {
            var A = tree.keyUint8("a");
            var B = tree.keyUint16("b");
            var C = tree.keyUint32("c");
            var D = tree.keyUint64("d");
            var E = tree.keyInt64("e");
            var kb = tree.newKeyBuilder(arena);

            assertSame(kb, kb.set(A, (byte) 0x77));
            assertSame(kb, kb.set(B, (short) 0x1234));
            assertSame(kb, kb.set(C, 0xDEADBEEF));
            assertSame(kb, kb.set(D, 0xCAFEBABE_DEADBEEFL));
            assertSame(kb, kb.set(E, -42L));

            var buf = kb.key();
            assertEquals((byte) 0x77, TaoKey.decodeU8(buf, A.offset()));
            assertEquals((short) 0x1234, TaoKey.decodeU16(buf, B.offset()));
            assertEquals(0xDEADBEEF, TaoKey.decodeU32(buf, C.offset()));
            assertEquals(0xCAFEBABE_DEADBEEFL, TaoKey.decodeU64(buf, D.offset()));
            assertEquals(-42L, TaoKey.decodeI64(buf, E.offset()));

            assertEquals(layout.totalWidth(), kb.keyLen());
        }
    }

    @Test
    void dictHandleSettersInternAndChain() throws IOException {
        var layout = KeyLayout.of(
                KeyField.dict16("k16"),
                KeyField.dict32("k32"));
        try (var tree = TaoTree.create(path(), layout);
             var arena = Arena.ofConfined()) {
            var K16 = tree.keyDict16("k16");
            var K32 = tree.keyDict32("k32");
            var kb = tree.newKeyBuilder(arena);

            assertSame(kb, kb.set(K16, "alpha"));
            assertSame(kb, kb.set(K32, "beta"));
            // Non-zero codes interned.
            assertNotEquals((short) 0, TaoKey.decodeU16(kb.key(), K16.offset()));
            assertNotEquals(0, TaoKey.decodeU32(kb.key(), K32.offset()));

            // Null resets to code 0.
            kb.set(K16, (String) null);
            kb.set(K32, (String) null);
            assertEquals((short) 0, TaoKey.decodeU16(kb.key(), K16.offset()));
            assertEquals(0, TaoKey.decodeU32(kb.key(), K32.offset()));
        }
    }

    @Test
    void dictHandleSettersThrowWhenDictNotBound() {
        try (var arena = Arena.ofConfined()) {
            var kb = new KeyBuilder(
                    KeyLayout.of(KeyField.dict16("a", null), KeyField.dict32("b", null)),
                    arena);
            var h16 = new KeyHandle.Dict16("a", 0, 0, null);
            var h32 = new KeyHandle.Dict32("b", 0, 2, null);
            assertThrows(IllegalStateException.class, () -> kb.set(h16, "foo"));
            assertThrows(IllegalStateException.class, () -> kb.set(h32, "foo"));
        }
    }

    @Test
    void setDictByIndexNullValueEncodesZero() throws IOException {
        var layout = KeyLayout.of(KeyField.dict16("k16"), KeyField.dict32("k32"));
        try (var tree = TaoTree.create(path(), layout);
             var arena = Arena.ofConfined()) {
            var kb = tree.newKeyBuilder(arena);
            kb.setDict(0, (String) null);
            kb.setDict(1, (String) null);
            assertEquals((short) 0, TaoKey.decodeU16(kb.key(), 0));
            assertEquals(0, TaoKey.decodeU32(kb.key(), 2));
        }
    }

    @Test
    void setDictByIndexThrowsOnNonDictField() throws IOException {
        var layout = KeyLayout.of(KeyField.uint32("id"));
        try (var tree = TaoTree.create(path(), layout);
             var arena = Arena.ofConfined()) {
            var kb = tree.newKeyBuilder(arena);
            assertThrows(IllegalArgumentException.class, () -> kb.setDict(0, "x"));
        }
    }

    @Test
    void nameBasedSettersLookUpFieldIndex() throws IOException {
        var layout = KeyLayout.of(
                KeyField.uint8("u8"), KeyField.uint16("u16"),
                KeyField.uint32("u32"), KeyField.uint64("u64"),
                KeyField.int64("i64"), KeyField.dict16("d16"));
        try (var tree = TaoTree.create(path(), layout);
             var arena = Arena.ofConfined()) {
            var kb = tree.newKeyBuilder(arena);
            kb.setU8("u8", (byte) 1)
              .setU16("u16", (short) 2)
              .setU32("u32", 3)
              .setU64("u64", 4L)
              .setI64("i64", 5L)
              .setDict("d16", "x");
            try (var w = tree.write()) {
                w.put(kb, "v", Value.ofInt(0));
            }
            try (var r = tree.read()) {
                assertEquals(Value.ofInt(0), r.get(kb, "v"));
            }
        }
    }

    @Test
    void setDictHandleNullValueEncodesZero() throws IOException {
        var layout = KeyLayout.of(KeyField.dict16("k"));
        try (var tree = TaoTree.create(path(), layout);
             var arena = Arena.ofConfined()) {
            var K = tree.keyDict16("k");
            var kb = tree.newKeyBuilder(arena);
            kb.set(K, (String) null);
            assertEquals((short) 0, TaoKey.decodeU16(kb.key(), K.offset()));
        }
    }
}
