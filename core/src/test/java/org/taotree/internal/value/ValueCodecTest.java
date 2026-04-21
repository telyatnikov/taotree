package org.taotree.internal.value;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.taotree.Value;
import org.taotree.internal.alloc.BumpAllocator;
import org.taotree.internal.alloc.ChunkStore;

import java.io.IOException;
import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.*;

class ValueCodecTest {

    @TempDir Path tmp;
    private int fc;

    private Arena arena;
    private ChunkStore cs;
    private BumpAllocator bump;
    private MemorySegment slot;

    @BeforeEach
    void setUp() throws IOException {
        arena = Arena.ofShared();
        Path file = tmp.resolve("vc-" + (fc++) + ".dat");
        if (Files.exists(file)) Files.delete(file);
        cs = ChunkStore.createCheckpointed(file, arena, ChunkStore.DEFAULT_CHUNK_SIZE, false);
        bump = new BumpAllocator(arena, cs, 4096);
        slot = arena.allocate(ValueCodec.SLOT_SIZE, 1);
    }

    @AfterEach
    void tearDown() throws IOException {
        cs.close();
        arena.close();
    }

    private Value roundTrip(Value v) {
        ValueCodec.encode(slot, v, bump);
        return ValueCodec.decode(slot, bump);
    }

    // ── Fixed-width types ─────────────────────────────────────────────────

    @Test void nullRoundtrip()    { assertEquals(Value.ofNull(),         roundTrip(Value.ofNull())); }
    @Test void boolTrue()         { assertEquals(Value.ofBool(true),     roundTrip(Value.ofBool(true))); }
    @Test void boolFalse()        { assertEquals(Value.ofBool(false),    roundTrip(Value.ofBool(false))); }
    @Test void int32Min()         { assertEquals(Value.ofInt(Integer.MIN_VALUE),  roundTrip(Value.ofInt(Integer.MIN_VALUE))); }
    @Test void int32Max()         { assertEquals(Value.ofInt(Integer.MAX_VALUE),  roundTrip(Value.ofInt(Integer.MAX_VALUE))); }
    @Test void int32Zero()        { assertEquals(Value.ofInt(0),         roundTrip(Value.ofInt(0))); }
    @Test void int64Min()         { assertEquals(Value.ofLong(Long.MIN_VALUE),    roundTrip(Value.ofLong(Long.MIN_VALUE))); }
    @Test void int64Max()         { assertEquals(Value.ofLong(Long.MAX_VALUE),    roundTrip(Value.ofLong(Long.MAX_VALUE))); }
    @Test void float32Pi()        { assertEquals(Value.ofFloat32(3.14f),  roundTrip(Value.ofFloat32(3.14f))); }
    @Test void float32Nan()       { assertEquals(Value.Type.FLOAT32, roundTrip(Value.ofFloat32(Float.NaN)).type());
                                    assertTrue(Float.isNaN(roundTrip(Value.ofFloat32(Float.NaN)).asFloat32())); }
    @Test void float64Pi()        { assertEquals(Value.ofFloat64(Math.PI), roundTrip(Value.ofFloat64(Math.PI))); }
    @Test void float64Inf()       { assertEquals(Value.ofFloat64(Double.POSITIVE_INFINITY),
                                                  roundTrip(Value.ofFloat64(Double.POSITIVE_INFINITY))); }

    // ── Variable-length: inline (≤ 12 bytes) ──────────────────────────────

    @Test
    void emptyString() {
        assertEquals(Value.ofString(""), roundTrip(Value.ofString("")));
        assertEquals(0, ValueCodec.encodedLength(slot));
        assertFalse(ValueCodec.isOverflow(slot));
    }

    @Test
    void shortStringInline() {
        Value v = Value.ofString("hello");
        assertEquals(v, roundTrip(v));
        assertEquals(5, ValueCodec.encodedLength(slot));
        assertFalse(ValueCodec.isOverflow(slot));
    }

    @Test
    void shortStringExactly12Bytes() {
        String s = "Elops smithi";
        assertEquals(12, s.getBytes(StandardCharsets.UTF_8).length);
        Value v = Value.ofString(s);
        assertEquals(v, roundTrip(v));
        assertFalse(ValueCodec.isOverflow(slot));
    }

    @Test
    void shortJsonInline() {
        Value v = Value.ofJson("{\"a\":1}");
        Value back = roundTrip(v);
        assertEquals(Value.Type.JSON, back.type());
        assertEquals("{\"a\":1}", back.asString());
    }

    @Test
    void shortBytesInline() {
        Value v = Value.ofBytes(new byte[]{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12});
        assertEquals(v, roundTrip(v));
        assertFalse(ValueCodec.isOverflow(slot));
    }

    // ── Variable-length: overflow (> 12 bytes) ─────────────────────────────

    @Test
    void longStringOverflow() {
        String s = "Haliaeetus leucocephalus";
        Value v = Value.ofString(s);
        assertEquals(v, roundTrip(v));
        assertEquals(s.length(), ValueCodec.encodedLength(slot));
        assertTrue(ValueCodec.isOverflow(slot));
    }

    @Test
    void longBytesOverflow() {
        byte[] data = new byte[1024];
        for (int i = 0; i < data.length; i++) data[i] = (byte) (i % 251);
        Value v = Value.ofBytes(data);
        assertEquals(v, roundTrip(v));
        assertTrue(ValueCodec.isOverflow(slot));
    }

    @Test
    void longJsonOverflow() {
        StringBuilder sb = new StringBuilder("{");
        for (int i = 0; i < 100; i++) {
            if (i > 0) sb.append(',');
            sb.append('"').append("k").append(i).append('"').append(':').append(i);
        }
        sb.append('}');
        Value v = Value.ofJson(sb.toString());
        Value back = roundTrip(v);
        assertEquals(Value.Type.JSON, back.type());
        assertEquals(sb.toString(), back.asString());
    }

    @Test
    void utf8Multibyte() {
        Value v = Value.ofString("日本語");
        Value back = roundTrip(v);
        assertEquals("日本語", back.asString());
    }

    // ── Standalone (encode/decode via bump-allocated slot) ─────────────────

    @Test
    void encodeStandaloneRoundtrip() {
        long ref = ValueCodec.encodeStandalone(Value.ofString("Lake Tahoe"), bump);
        Value back = ValueCodec.decodeStandalone(ref, bump);
        assertEquals(Value.ofString("Lake Tahoe"), back);
    }

    @Test
    void encodeStandaloneOverflow() {
        String big = "x".repeat(500);
        long ref = ValueCodec.encodeStandalone(Value.ofString(big), bump);
        Value back = ValueCodec.decodeStandalone(ref, bump);
        assertEquals(Value.ofString(big), back);
    }

    @Test
    void encodeStandaloneInt32() {
        long ref = ValueCodec.encodeStandalone(Value.ofInt(-7), bump);
        assertEquals(Value.ofInt(-7), ValueCodec.decodeStandalone(ref, bump));
    }

    // ── Inspection helpers ────────────────────────────────────────────────

    @Test
    void encodedTypeMatchesValue() {
        ValueCodec.encode(slot, Value.ofFloat64(1.0), bump);
        assertEquals(Value.Type.FLOAT64, ValueCodec.encodedType(slot));
        assertEquals(8, ValueCodec.encodedLength(slot));
    }

    // ── Error paths ───────────────────────────────────────────────────────

    @Test
    void payloadTooLargeThrows() {
        byte[] huge = new byte[ValueCodec.MAX_PAYLOAD + 1];
        assertThrows(IllegalArgumentException.class,
            () -> ValueCodec.encode(slot, Value.ofBytes(huge), bump));
    }

    @Test
    void overflowWithoutBumpThrows() {
        Value big = Value.ofString("x".repeat(50));
        assertThrows(IllegalStateException.class,
            () -> ValueCodec.encode(slot, big, null));
    }

    @Test
    void inlineWithoutBumpOk() {
        ValueCodec.encode(slot, Value.ofInt(42), null);
        assertEquals(Value.ofInt(42), ValueCodec.decode(slot, null));
        ValueCodec.encode(slot, Value.ofString("x"), null);
        assertEquals(Value.ofString("x"), ValueCodec.decode(slot, null));
    }

    @Test
    void encodeNullValueThrows() {
        assertThrows(NullPointerException.class,
            () -> ValueCodec.encode(slot, null, bump));
    }
}
