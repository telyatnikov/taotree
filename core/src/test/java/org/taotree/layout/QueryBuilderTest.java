package org.taotree.layout;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.taotree.TaoKey;
import org.taotree.TaoTree;
import org.taotree.Value;

import java.io.IOException;
import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.*;

class QueryBuilderTest {

    @TempDir Path tmp;
    private int fc;
    private Path path() { return tmp.resolve(fc++ + ".taotree"); }

    @Test
    void setU8U16U32U64AndInt64EncodesBigEndian() throws IOException {
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
            var qb = tree.newQueryBuilder(arena);

            qb.set(A, (byte) 0xAB);
            qb.set(B, (short) 0x1234);
            qb.set(C, 0xDEADBEEF);
            qb.set(D, 0x0102030405060708L);
            qb.set(E, -1L);

            assertTrue(qb.isSatisfiable(E));
            assertEquals(layout.totalWidth(), qb.keyLen());

            MemorySegment buf = qb.key();
            assertEquals((byte) 0xAB, TaoKey.decodeU8(buf, A.offset()));
            assertEquals((short) 0x1234, TaoKey.decodeU16(buf, B.offset()));
            assertEquals(0xDEADBEEF, TaoKey.decodeU32(buf, C.offset()));
            assertEquals(0x0102030405060708L, TaoKey.decodeU64(buf, D.offset()));
            assertEquals(-1L, TaoKey.decodeI64(buf, E.offset()));
        }
    }

    @Test
    void prefixLengthReturnsOffsetPlusWidth() throws IOException {
        var layout = KeyLayout.of(
                KeyField.uint8("a"),
                KeyField.uint16("b"),
                KeyField.uint32("c"));
        try (var tree = TaoTree.create(path(), layout);
             var arena = Arena.ofConfined()) {
            var qb = tree.newQueryBuilder(arena);
            assertEquals(1, qb.prefixLength(tree.keyUint8("a")));
            assertEquals(3, qb.prefixLength(tree.keyUint16("b")));
            assertEquals(7, qb.prefixLength(tree.keyUint32("c")));
            assertEquals(7, qb.keyLen());
        }
    }

    @Test
    void setDict16UsesResolveAndMarksUnresolvedForUnknown() throws IOException {
        var layout = KeyLayout.of(KeyField.dict16("kind"), KeyField.uint32("id"));
        try (var tree = TaoTree.create(path(), layout);
             var arena = Arena.ofConfined()) {
            var KIND = tree.keyDict16("kind");
            var ID   = tree.keyUint32("id");

            try (var w = tree.write()) {
                var kb = tree.newKeyBuilder(arena);
                kb.set(KIND, "known").set(ID, 1);
                w.put(kb, "x", Value.ofInt(1));
            }

            var qb = tree.newQueryBuilder(arena);

            qb.set(KIND, "ghost");
            assertFalse(qb.isSatisfiable(KIND),
                    "unknown dict string must make the prefix unsatisfiable");

            qb.clear();
            // Known string resolves to a positive code → must be encoded into the slot.
            QueryBuilder returned = qb.set(KIND, "known");
            assertSame(qb, returned, "set() returns this for chaining");
            assertTrue(qb.isSatisfiable(KIND));
            short code = TaoKey.decodeU16(qb.key(), KIND.offset());
            assertNotEquals((short) 0, code);
            assertTrue((code & 0xFFFF) > 0);

            assertFalse(qb.isSatisfiable(ID));
        }
    }

    @Test
    void setDict32ChainsAndEncodesKnownResolveCode() throws IOException {
        var layout = KeyLayout.of(KeyField.dict32("k"));
        try (var tree = TaoTree.create(path(), layout);
             var arena = Arena.ofConfined()) {
            var K = tree.keyDict32("k");
            var kb = tree.newKeyBuilder(arena);
            try (var w = tree.write()) {
                kb.set(K, "known");
                w.put(kb, "x", Value.ofInt(1));
            }
            var qb = tree.newQueryBuilder(arena);
            QueryBuilder returned = qb.set(K, "known");
            assertSame(qb, returned);
            assertTrue(qb.isSatisfiable(K));
            int code = TaoKey.decodeU32(qb.key(), K.offset());
            assertTrue(code > 0, "resolved positive code written into slot");
        }
    }

    @Test
    void setIntegerSettersChain() throws IOException {
        var layout = KeyLayout.of(
                KeyField.uint8("a"), KeyField.uint16("b"), KeyField.uint32("c"),
                KeyField.uint64("d"), KeyField.int64("e"));
        try (var tree = TaoTree.create(path(), layout);
             var arena = Arena.ofConfined()) {
            var qb = tree.newQueryBuilder(arena);
            assertSame(qb, qb.set(tree.keyUint8("a"), (byte) 1));
            assertSame(qb, qb.set(tree.keyUint16("b"), (short) 2));
            assertSame(qb, qb.set(tree.keyUint32("c"), 3));
            assertSame(qb, qb.set(tree.keyUint64("d"), 4L));
            assertSame(qb, qb.set(tree.keyInt64("e"), 5L));
            assertSame(qb, qb.clear());
        }
    }

    @Test
    void setDict32UsesResolveAndNullEncodesZero() throws IOException {
        var layout = KeyLayout.of(KeyField.dict32("kind"));
        try (var tree = TaoTree.create(path(), layout);
             var arena = Arena.ofConfined()) {
            var KIND = tree.keyDict32("kind");
            var qb = tree.newQueryBuilder(arena);

            qb.set(KIND, (String) null);
            assertTrue(qb.isSatisfiable(KIND),
                    "null dict string is treated as resolved (encoded zero)");
            assertEquals(0, TaoKey.decodeU32(qb.key(), KIND.offset()));

            qb.clear();
            qb.set(KIND, "unknown-code");
            assertFalse(qb.isSatisfiable(KIND));
        }
    }

    @Test
    void setDict16NullEncodesZero() throws IOException {
        var layout = KeyLayout.of(KeyField.dict16("k"));
        try (var tree = TaoTree.create(path(), layout);
             var arena = Arena.ofConfined()) {
            var K = tree.keyDict16("k");
            var qb = tree.newQueryBuilder(arena);
            qb.set(K, (String) null);
            assertTrue(qb.isSatisfiable(K));
            assertEquals((short) 0, TaoKey.decodeU16(qb.key(), K.offset()));
        }
    }

    @Test
    void isSatisfiableRequiresAllEarlierFieldsSet() throws IOException {
        var layout = KeyLayout.of(
                KeyField.uint16("a"),
                KeyField.uint16("b"),
                KeyField.uint16("c"));
        try (var tree = TaoTree.create(path(), layout);
             var arena = Arena.ofConfined()) {
            var A = tree.keyUint16("a");
            var B = tree.keyUint16("b");
            var C = tree.keyUint16("c");
            var qb = tree.newQueryBuilder(arena);

            assertFalse(qb.isSatisfiable(A));
            assertFalse(qb.isSatisfiable(B));
            assertFalse(qb.isSatisfiable(C));

            qb.set(A, (short) 1);
            assertTrue(qb.isSatisfiable(A));
            assertFalse(qb.isSatisfiable(B));

            qb.set(C, (short) 3);
            assertFalse(qb.isSatisfiable(C));

            qb.set(B, (short) 2);
            assertTrue(qb.isSatisfiable(C));
        }
    }

    @Test
    void clearResetsFieldStateAndBuffer() throws IOException {
        var layout = KeyLayout.of(KeyField.uint32("id"));
        try (var tree = TaoTree.create(path(), layout);
             var arena = Arena.ofConfined()) {
            var ID = tree.keyUint32("id");
            var qb = tree.newQueryBuilder(arena);

            qb.set(ID, 0xCAFEBABE);
            assertTrue(qb.isSatisfiable(ID));

            qb.clear();
            assertFalse(qb.isSatisfiable(ID));
            for (long i = 0; i < qb.keyLen(); i++) {
                assertEquals((byte) 0,
                        qb.key().get(java.lang.foreign.ValueLayout.JAVA_BYTE, i));
            }

            qb.set(ID, 7);
            assertTrue(qb.isSatisfiable(ID));
            assertEquals(7, TaoKey.decodeU32(qb.key(), 0));
        }
    }

    // -----------------------------------------------------------------
    // clear() resets a multi-field builder, including the resolved[] flags
    // -----------------------------------------------------------------

    @Test
    void clearAlsoResetsResolvedFlags() throws IOException {
        var layout = KeyLayout.of(KeyField.uint16("a"), KeyField.uint16("b"));
        try (var tree = TaoTree.create(path(), layout);
             var arena = Arena.ofConfined()) {
            var A = tree.keyUint16("a");
            var B = tree.keyUint16("b");
            var qb = tree.newQueryBuilder(arena);
            qb.set(A, (short) 1);
            qb.set(B, (short) 2);
            assertTrue(qb.isSatisfiable(B));
            qb.clear();
            // Both the set[] and resolved[] arrays must be cleared, otherwise
            // isSatisfiable would still return true for one of the fields.
            assertFalse(qb.isSatisfiable(A));
            assertFalse(qb.isSatisfiable(B));
        }
    }

    @Test
    void setDict16WithoutBoundDictThrows() {
        try (var arena = Arena.ofConfined()) {
            var qb = new QueryBuilder(
                    KeyLayout.of(KeyField.dict16("x", null)), arena);
            var h = new KeyHandle.Dict16("x", 0, 0, null);
            var ex = assertThrows(IllegalStateException.class, () -> qb.set(h, "foo"));
            assertTrue(ex.getMessage().contains("'x'"));
        }
    }

    @Test
    void setDict32WithoutBoundDictThrows() {
        try (var arena = Arena.ofConfined()) {
            var qb = new QueryBuilder(
                    KeyLayout.of(KeyField.dict32("y", null)), arena);
            var h = new KeyHandle.Dict32("y", 0, 0, null);
            var ex = assertThrows(IllegalStateException.class, () -> qb.set(h, "foo"));
            assertTrue(ex.getMessage().contains("'y'"));
        }
    }
}
