package org.taotree;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Phase 6b: equality, hashCode, type, and accessor coverage for all
 * {@link Value} variants. Targets PIT mutants in {@code Value.java}.
 */
class ValueEqualityTest {

    @Test
    void equalsAndHashCodeAcrossVariants() {
        assertEquals(Value.ofInt(7), Value.ofInt(7));
        assertEquals(Value.ofInt(7).hashCode(), Value.ofInt(7).hashCode());
        assertNotEquals(Value.ofInt(7), Value.ofInt(8));

        assertEquals(Value.ofLong(7L), Value.ofLong(7L));
        assertNotEquals(Value.ofLong(7L), Value.ofLong(8L));

        assertEquals(Value.ofFloat32(1.5f), Value.ofFloat32(1.5f));
        assertNotEquals(Value.ofFloat32(1.5f), Value.ofFloat32(2.5f));

        assertEquals(Value.ofFloat64(1.5), Value.ofFloat64(1.5));
        assertNotEquals(Value.ofFloat64(1.5), Value.ofFloat64(2.5));

        assertEquals(Value.ofBool(true), Value.ofBool(true));
        assertEquals(Value.ofBool(false), Value.ofBool(false));
        assertNotEquals(Value.ofBool(true), Value.ofBool(false));
        assertEquals(Value.ofBool(true).hashCode(), Value.ofBool(true).hashCode());

        assertEquals(Value.ofString("a"), Value.ofString("a"));
        assertNotEquals(Value.ofString("a"), Value.ofString("b"));

        assertEquals(Value.ofJson("{}"), Value.ofJson("{}"));
        assertNotEquals(Value.ofJson("{}"), Value.ofString("{}"),
            "JSON and STRING are distinct types");

        assertEquals(Value.ofBytes(new byte[]{1,2,3}), Value.ofBytes(new byte[]{1,2,3}));
        assertNotEquals(Value.ofBytes(new byte[]{1,2,3}), Value.ofBytes(new byte[]{1,2,4}));
        assertEquals(
            Value.ofBytes(new byte[]{1,2,3}).hashCode(),
            Value.ofBytes(new byte[]{1,2,3}).hashCode());

        assertEquals(Value.ofNull(), Value.ofNull());
    }

    @Test
    void typeMismatchAcrossVariantsIsNotEqual() {
        // int(1) and long(1L) are different Types — must not be equal.
        assertNotEquals(Value.ofInt(1), Value.ofLong(1L));
        assertNotEquals(Value.ofFloat32(1f), Value.ofFloat64(1.0));
        assertNotEquals(Value.ofString("x"), Value.ofBytes(new byte[]{'x'}));
    }

    @Test
    void typeReturnsCorrectEnum() {
        assertEquals(Value.Type.INT32,   Value.ofInt(0).type());
        assertEquals(Value.Type.INT64,   Value.ofLong(0L).type());
        assertEquals(Value.Type.FLOAT32, Value.ofFloat32(0f).type());
        assertEquals(Value.Type.FLOAT64, Value.ofFloat64(0.0).type());
        assertEquals(Value.Type.BOOL,    Value.ofBool(true).type());
        assertEquals(Value.Type.STRING,  Value.ofString("").type());
        assertEquals(Value.Type.JSON,    Value.ofJson("{}").type());
        assertEquals(Value.Type.BYTES,   Value.ofBytes(new byte[0]).type());
        assertEquals(Value.Type.NULL,    Value.ofNull().type());
    }

    @Test
    void accessorsReturnExpectedValues() {
        assertEquals(7, Value.ofInt(7).asInt());
        assertEquals(7L, Value.ofInt(7).asLong(), "int widens to long");
        assertEquals(7L, Value.ofLong(7L).asLong());
        assertEquals(1.5f, Value.ofFloat32(1.5f).asFloat32());
        assertEquals(1.5, Value.ofFloat64(1.5).asFloat64());
        assertEquals(1.5, Value.ofFloat32(1.5f).asFloat64(), "float widens to double");
        assertTrue(Value.ofBool(true).asBool());
        assertFalse(Value.ofBool(false).asBool());
        assertEquals("hi", Value.ofString("hi").asString());
        assertEquals("{}", Value.ofJson("{}").asString(), "json decodes via asString too");
        assertArrayEquals(new byte[]{1,2,3}, Value.ofBytes(new byte[]{1,2,3}).asBytes());
        assertTrue(Value.ofNull().isNull());
        assertFalse(Value.ofInt(0).isNull());
    }

    @Test
    void accessorMismatchThrows() {
        assertThrows(IllegalStateException.class, () -> Value.ofString("x").asInt());
        assertThrows(IllegalStateException.class, () -> Value.ofInt(1).asString());
        assertThrows(IllegalStateException.class, () -> Value.ofBool(true).asLong());
        assertThrows(IllegalStateException.class, () -> Value.ofBytes(new byte[0]).asFloat64());
    }

    @Test
    void factoryNullCoercion() {
        assertEquals(Value.ofNull(), Value.ofString(null));
        assertEquals(Value.ofNull(), Value.ofJson(null));
        assertEquals(Value.ofNull(), Value.ofBytes(null));
    }

    @Test
    void typeFromTagRoundTrip() {
        for (Value.Type t : Value.Type.values()) {
            assertEquals(t, Value.Type.fromTag(t.tag));
        }
    }

    @Test
    void typeFromTagUnknownThrows() {
        assertThrows(IllegalArgumentException.class, () -> Value.Type.fromTag((byte) 99));
    }

    @Test
    void bytesAccessorReturnsDefensiveCopy() {
        byte[] src = new byte[]{1, 2, 3};
        Value v = Value.ofBytes(src);
        byte[] out = v.asBytes();
        out[0] = 99;
        assertEquals(1, v.asBytes()[0], "asBytes must return a defensive copy");
        // Mutating the original after construction must also not affect v.
        src[1] = 88;
        assertEquals(2, v.asBytes()[1], "ofBytes must defensively clone");
    }
}
