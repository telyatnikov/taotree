package org.taotree;

import org.junit.jupiter.api.Test;

import java.util.Arrays;

import static org.junit.jupiter.api.Assertions.*;

class ValueTest {

    @Test
    void int32Roundtrip() {
        Value v = Value.ofInt(42);
        assertEquals(Value.Type.INT32, v.type());
        assertEquals(42, v.asInt());
        assertEquals(42L, v.asLong());           // widening allowed
        assertFalse(v.isNull());
    }

    @Test
    void int64Roundtrip() {
        Value v = Value.ofLong(1L << 40);
        assertEquals(Value.Type.INT64, v.type());
        assertEquals(1L << 40, v.asLong());
        assertThrows(IllegalStateException.class, v::asInt);
    }

    @Test
    void float32Roundtrip() {
        Value v = Value.ofFloat32(3.14f);
        assertEquals(Value.Type.FLOAT32, v.type());
        assertEquals(3.14f, v.asFloat32());
        assertEquals(3.14f, (float) v.asFloat64()); // widening
    }

    @Test
    void float64Roundtrip() {
        Value v = Value.ofFloat64(Math.PI);
        assertEquals(Value.Type.FLOAT64, v.type());
        assertEquals(Math.PI, v.asFloat64());
        assertThrows(IllegalStateException.class, v::asFloat32);
    }

    @Test
    void boolRoundtrip() {
        assertSame(Value.Bool.TRUE, Value.ofBool(true));
        assertSame(Value.Bool.FALSE, Value.ofBool(false));
        assertTrue(Value.ofBool(true).asBool());
        assertFalse(Value.ofBool(false).asBool());
        assertEquals(Value.Type.BOOL, Value.ofBool(true).type());
    }

    @Test
    void stringRoundtrip() {
        Value v = Value.ofString("hello");
        assertEquals(Value.Type.STRING, v.type());
        assertEquals("hello", v.asString());
    }

    @Test
    void jsonRoundtrip() {
        Value v = Value.ofJson("{\"a\":1}");
        assertEquals(Value.Type.JSON, v.type());
        assertEquals("{\"a\":1}", v.asString());   // also via asString
    }

    @Test
    void bytesRoundtrip() {
        byte[] orig = {1, 2, 3, 4, 5};
        Value v = Value.ofBytes(orig);
        assertEquals(Value.Type.BYTES, v.type());
        assertArrayEquals(orig, v.asBytes());
        // Defensive copy on construction
        orig[0] = 99;
        assertArrayEquals(new byte[]{1, 2, 3, 4, 5}, v.asBytes());
        // Defensive copy on read
        v.asBytes()[0] = 77;
        assertArrayEquals(new byte[]{1, 2, 3, 4, 5}, v.asBytes());
    }

    @Test
    void nullRoundtrip() {
        Value v = Value.ofNull();
        assertEquals(Value.Type.NULL, v.type());
        assertTrue(v.isNull());
        assertSame(Value.Null.INSTANCE, v);
        assertSame(Value.Null.INSTANCE, Value.ofString(null));
        assertSame(Value.Null.INSTANCE, Value.ofJson(null));
        assertSame(Value.Null.INSTANCE, Value.ofBytes(null));
    }

    @Test
    void typeMismatchThrows() {
        Value v = Value.ofInt(1);
        assertThrows(IllegalStateException.class, v::asString);
        assertThrows(IllegalStateException.class, v::asBytes);
        assertThrows(IllegalStateException.class, v::asBool);
    }

    @Test
    void typeFromTagRoundtrip() {
        for (Value.Type t : Value.Type.values()) {
            assertSame(t, Value.Type.fromTag(t.tag));
        }
    }

    @Test
    void typeFromUnknownTagThrows() {
        assertThrows(IllegalArgumentException.class, () -> Value.Type.fromTag((byte) 99));
    }

    @Test
    void recordEquality() {
        assertEquals(Value.ofInt(7), Value.ofInt(7));
        assertEquals(Value.ofString("x"), Value.ofString("x"));
        assertEquals(Value.ofBytes(new byte[]{1, 2}), Value.ofBytes(new byte[]{1, 2}));
        assertNotEquals(Value.ofInt(7), Value.ofLong(7));
        assertNotEquals(Value.ofString("x"), Value.ofJson("x"));
    }

    @Test
    void boolEquality() {
        assertEquals(Value.ofBool(true), Value.ofBool(true));
        assertNotEquals(Value.ofBool(true), Value.ofBool(false));
    }

    @Test
    void bytesEqualityAndHash() {
        Value a = Value.ofBytes(new byte[]{1, 2, 3});
        Value b = Value.ofBytes(new byte[]{1, 2, 3});
        assertEquals(a, b);
        assertEquals(a.hashCode(), b.hashCode());
    }

    @Test
    void factoriesProduceCorrectVariants() {
        assertTrue(Value.ofInt(1)        instanceof Value.Int32);
        assertTrue(Value.ofLong(1L)      instanceof Value.Int64);
        assertTrue(Value.ofFloat32(1f)   instanceof Value.Float32);
        assertTrue(Value.ofFloat64(1.0)  instanceof Value.Float64);
        assertTrue(Value.ofBool(true)    instanceof Value.Bool);
        assertTrue(Value.ofString("x")   instanceof Value.Str);
        assertTrue(Value.ofJson("{}")    instanceof Value.Json);
        assertTrue(Value.ofBytes(new byte[0]) instanceof Value.Bytes);
        assertTrue(Value.ofNull()        instanceof Value.Null);
    }
}
