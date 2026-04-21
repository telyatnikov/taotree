package org.taotree.internal.value;

import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.nio.charset.StandardCharsets;
import org.taotree.Value;
import org.taotree.internal.alloc.BumpAllocator;

/**
 * Off-heap encoder/decoder for {@link Value} into a fixed 16-byte slot.
 *
 * <p>Slot layout (16 B):
 * <pre>
 * Offset  Size  Field
 * ──────  ────  ───────────────────────────────────────
 * 0       1     tag         ({@link Value.Type#tag})
 * 1       1     reserved    (0)
 * 2       2     len         (payload length in bytes; meaning depends on tag)
 * 4       12    data        (inline payload OR overflow descriptor)
 * </pre>
 *
 * <p>For fixed-width types the payload lives directly in {@code data[0..N]}
 * and {@code len = N}. For variable-length types (STRING/JSON/BYTES) the
 * encoding mirrors {@code TaoString}:
 * <ul>
 *   <li>{@code len ≤ INLINE_THRESHOLD (12)} — payload bytes live in
 *       {@code data[0..len]}; remaining bytes are zeroed.</li>
 *   <li>{@code len > 12} — {@code data[0..4]} holds the first 4 bytes (prefix)
 *       and {@code data[4..12]} holds an 8-byte little-endian
 *       {@code BumpAllocator} reference to a {@code len}-byte overflow body.</li>
 * </ul>
 *
 * <p>The 2-byte {@code len} field caps a single Value's payload at 65 535
 * bytes. Values larger than this should be split or stored externally.
 */
public final class ValueCodec {

    private ValueCodec() {}

    public static final int SLOT_SIZE = 16;
    public static final int INLINE_THRESHOLD = 12;
    public static final int MAX_PAYLOAD = 0xFFFF;

    private static final long OFF_TAG    = 0;
    private static final long OFF_LEN    = 2;
    private static final long OFF_DATA   = 4;
    private static final long OFF_PREFIX = 4;
    private static final long OFF_OVFL   = 8;

    // ────────────────────────────────────────────────────────────────────────
    // Encode
    // ────────────────────────────────────────────────────────────────────────

    /**
     * Encode {@code value} into the given 16-byte slot. May allocate overflow
     * storage from {@code bump} for variable-length payloads &gt; 12 bytes.
     *
     * @param slot 16-byte segment (must have at least {@link #SLOT_SIZE} bytes)
     * @param value the value to encode (non-null; use {@link Value#ofNull()} for absence)
     * @param bump bump allocator for overflow storage; may be {@code null}
     *             only if no STRING/JSON/BYTES payload exceeds the inline threshold
     */
    public static void encode(MemorySegment slot, Value value, BumpAllocator bump) {
        if (value == null) throw new NullPointerException("value");
        // Always write the tag first; clear remaining bytes lazily per variant.
        Value.Type type = value.type();
        slot.set(ValueLayout.JAVA_BYTE, OFF_TAG, type.tag);
        slot.set(ValueLayout.JAVA_BYTE, 1, (byte) 0);

        switch (value) {
            case Value.Null _ -> {
                slot.set(ValueLayout.JAVA_SHORT_UNALIGNED, OFF_LEN, (short) 0);
                zero12(slot);
            }
            case Value.Bool b -> {
                slot.set(ValueLayout.JAVA_SHORT_UNALIGNED, OFF_LEN, (short) 1);
                slot.set(ValueLayout.JAVA_BYTE, OFF_DATA, (byte) (b.value() ? 1 : 0));
                for (int i = 1; i < 12; i++) slot.set(ValueLayout.JAVA_BYTE, OFF_DATA + i, (byte) 0);
            }
            case Value.Int32 i -> {
                slot.set(ValueLayout.JAVA_SHORT_UNALIGNED, OFF_LEN, (short) 4);
                slot.set(ValueLayout.JAVA_INT_UNALIGNED, OFF_DATA, i.value());
                zeroTail(slot, 4);
            }
            case Value.Int64 i -> {
                slot.set(ValueLayout.JAVA_SHORT_UNALIGNED, OFF_LEN, (short) 8);
                slot.set(ValueLayout.JAVA_LONG_UNALIGNED, OFF_DATA, i.value());
                zeroTail(slot, 8);
            }
            case Value.Float32 f -> {
                slot.set(ValueLayout.JAVA_SHORT_UNALIGNED, OFF_LEN, (short) 4);
                slot.set(ValueLayout.JAVA_FLOAT_UNALIGNED, OFF_DATA, f.value());
                zeroTail(slot, 4);
            }
            case Value.Float64 f -> {
                slot.set(ValueLayout.JAVA_SHORT_UNALIGNED, OFF_LEN, (short) 8);
                slot.set(ValueLayout.JAVA_DOUBLE_UNALIGNED, OFF_DATA, f.value());
                zeroTail(slot, 8);
            }
            case Value.Str s   -> writeBytes(slot, s.value().getBytes(StandardCharsets.UTF_8), bump);
            case Value.Json j  -> writeBytes(slot, j.value().getBytes(StandardCharsets.UTF_8), bump);
            case Value.Bytes b -> writeBytes(slot, b.value(), bump);
        }
    }

    private static void writeBytes(MemorySegment slot, byte[] data, BumpAllocator bump) {
        if (data.length > MAX_PAYLOAD) {
            throw new IllegalArgumentException(
                "Value payload too large: " + data.length + " > " + MAX_PAYLOAD);
        }
        slot.set(ValueLayout.JAVA_SHORT_UNALIGNED, OFF_LEN, (short) data.length);
        if (data.length <= INLINE_THRESHOLD) {
            MemorySegment.copy(data, 0, slot, ValueLayout.JAVA_BYTE, OFF_DATA, data.length);
            for (int i = data.length; i < 12; i++) {
                slot.set(ValueLayout.JAVA_BYTE, OFF_DATA + i, (byte) 0);
            }
        } else {
            if (bump == null) {
                throw new IllegalStateException(
                    "BumpAllocator required for Value payload > " + INLINE_THRESHOLD + " bytes");
            }
            MemorySegment.copy(data, 0, slot, ValueLayout.JAVA_BYTE, OFF_PREFIX, 4);
            long ovflRef = bump.allocate(data.length);
            MemorySegment dest = bump.resolve(ovflRef, data.length);
            MemorySegment.copy(data, 0, dest, ValueLayout.JAVA_BYTE, 0, data.length);
            slot.set(ValueLayout.JAVA_LONG_UNALIGNED, OFF_OVFL, ovflRef);
        }
    }

    private static void zero12(MemorySegment slot) {
        for (int i = 0; i < 12; i++) slot.set(ValueLayout.JAVA_BYTE, OFF_DATA + i, (byte) 0);
    }

    private static void zeroTail(MemorySegment slot, int written) {
        for (int i = written; i < 12; i++) {
            slot.set(ValueLayout.JAVA_BYTE, OFF_DATA + i, (byte) 0);
        }
    }

    // ────────────────────────────────────────────────────────────────────────
    // Decode
    // ────────────────────────────────────────────────────────────────────────

    /**
     * Decode a {@link Value} from a 16-byte slot. Resolves overflow payloads
     * via {@code bump} when the inline threshold is exceeded.
     */
    public static Value decode(MemorySegment slot, BumpAllocator bump) {
        Value.Type type = Value.Type.fromTag(slot.get(ValueLayout.JAVA_BYTE, OFF_TAG));
        int len = Short.toUnsignedInt(slot.get(ValueLayout.JAVA_SHORT_UNALIGNED, OFF_LEN));
        return switch (type) {
            case NULL    -> Value.ofNull();
            case BOOL    -> Value.ofBool(slot.get(ValueLayout.JAVA_BYTE, OFF_DATA) != 0);
            case INT32   -> Value.ofInt(slot.get(ValueLayout.JAVA_INT_UNALIGNED, OFF_DATA));
            case INT64   -> Value.ofLong(slot.get(ValueLayout.JAVA_LONG_UNALIGNED, OFF_DATA));
            case FLOAT32 -> Value.ofFloat32(slot.get(ValueLayout.JAVA_FLOAT_UNALIGNED, OFF_DATA));
            case FLOAT64 -> Value.ofFloat64(slot.get(ValueLayout.JAVA_DOUBLE_UNALIGNED, OFF_DATA));
            case STRING  -> Value.ofString(new String(readBytes(slot, len, bump), StandardCharsets.UTF_8));
            case JSON    -> Value.ofJson(new String(readBytes(slot, len, bump), StandardCharsets.UTF_8));
            case BYTES   -> Value.ofBytes(readBytes(slot, len, bump));
        };
    }

    private static byte[] readBytes(MemorySegment slot, int len, BumpAllocator bump) {
        byte[] out = new byte[len];
        if (len <= INLINE_THRESHOLD) {
            MemorySegment.copy(slot, ValueLayout.JAVA_BYTE, OFF_DATA, out, 0, len);
        } else {
            if (bump == null) {
                throw new IllegalStateException(
                    "BumpAllocator required to resolve Value overflow (len=" + len + ")");
            }
            long ovflRef = slot.get(ValueLayout.JAVA_LONG_UNALIGNED, OFF_OVFL);
            MemorySegment src = bump.resolve(ovflRef, len);
            MemorySegment.copy(src, ValueLayout.JAVA_BYTE, 0, out, 0, len);
        }
        return out;
    }

    // ────────────────────────────────────────────────────────────────────────
    // Standalone helpers — allocate the 16-byte slot from BumpAllocator
    // ────────────────────────────────────────────────────────────────────────

    /**
     * Encode {@code value} into a freshly-allocated 16-byte slot in {@code bump}.
     *
     * @return the bump-allocator reference of the slot (suitable for
     *         {@code AttrRun.value_ref})
     */
    public static long encodeStandalone(Value value, BumpAllocator bump) {
        if (bump == null) throw new NullPointerException("bump");
        long slotRef = bump.allocate(SLOT_SIZE);
        MemorySegment slot = bump.resolve(slotRef, SLOT_SIZE);
        encode(slot, value, bump);
        return slotRef;
    }

    /** Decode a value previously written via {@link #encodeStandalone}. */
    public static Value decodeStandalone(long slotRef, BumpAllocator bump) {
        if (bump == null) throw new NullPointerException("bump");
        MemorySegment slot = bump.resolve(slotRef, SLOT_SIZE);
        return decode(slot, bump);
    }

    // ────────────────────────────────────────────────────────────────────────
    // Inspection helpers
    // ────────────────────────────────────────────────────────────────────────

    /** The encoded length in bytes (matches {@code len} field). */
    public static int encodedLength(MemorySegment slot) {
        return Short.toUnsignedInt(slot.get(ValueLayout.JAVA_SHORT_UNALIGNED, OFF_LEN));
    }

    // ────────────────────────────────────────────────────────────────────────
    // Logical equality between two standalone slots (possibly allocated by
    // different encode calls) — used by the temporal write path to detect
    // same-logical-value writes that should extend a run's last_seen rather
    // than create a new AttributeRun. See docs/design-rationale.md.
    // ────────────────────────────────────────────────────────────────────────

    /**
     * Compare two encoded value slots for logical equality. Two byte-identical
     * inline slots are equal; two overflow slots are equal iff their
     * (tag, len, 4-byte prefix) headers match and their resolved payload bytes
     * match. Equality is bitwise: two distinct NaN bit-patterns are unequal.
     *
     * <p>This is the comparison that must be used when deciding "same logical
     * value" across independent {@link #encodeStandalone} calls, because each
     * call allocates a fresh slot, so raw pointer equality under-reports
     * duplicates.
     *
     * @param aRef standalone slot ref (or a sentinel like
     *             {@code TOMBSTONE_VALUE_REF=-1}; non-positive refs are treated
     *             as incomparable to real refs)
     * @param bRef standalone slot ref
     * @param bump bump allocator that owns both slots
     * @return {@code true} iff both refs decode to the same logical value
     */
    public static boolean slotEquals(long aRef, long bRef, BumpAllocator bump) {
        if (aRef == bRef) return true;
        // Sentinels (e.g., AttributeRun.TOMBSTONE_VALUE_REF = -1) are not real
        // slot refs; treat them as unequal to any other ref than themselves.
        if (aRef <= 0 || bRef <= 0) return false;

        MemorySegment a = bump.resolve(aRef, SLOT_SIZE);
        MemorySegment b = bump.resolve(bRef, SLOT_SIZE);

        byte aTag = a.get(ValueLayout.JAVA_BYTE, OFF_TAG);
        byte bTag = b.get(ValueLayout.JAVA_BYTE, OFF_TAG);
        if (aTag != bTag) return false;

        int aLen = Short.toUnsignedInt(a.get(ValueLayout.JAVA_SHORT_UNALIGNED, OFF_LEN));
        int bLen = Short.toUnsignedInt(b.get(ValueLayout.JAVA_SHORT_UNALIGNED, OFF_LEN));
        if (aLen != bLen) return false;

        if (aLen <= INLINE_THRESHOLD) {
            // Inline: compare the 12-byte data region. Encoder always zeroes
            // the tail, so this is stable across different encode calls.
            long aLo = a.get(ValueLayout.JAVA_LONG_UNALIGNED, OFF_DATA);
            long bLo = b.get(ValueLayout.JAVA_LONG_UNALIGNED, OFF_DATA);
            if (aLo != bLo) return false;
            int aHi = a.get(ValueLayout.JAVA_INT_UNALIGNED, OFF_DATA + 8);
            int bHi = b.get(ValueLayout.JAVA_INT_UNALIGNED, OFF_DATA + 8);
            return aHi == bHi;
        }

        // Overflow: compare the 4-byte prefix, then the payload.
        int aPrefix = a.get(ValueLayout.JAVA_INT_UNALIGNED, OFF_PREFIX);
        int bPrefix = b.get(ValueLayout.JAVA_INT_UNALIGNED, OFF_PREFIX);
        if (aPrefix != bPrefix) return false;

        long aOvfl = a.get(ValueLayout.JAVA_LONG_UNALIGNED, OFF_OVFL);
        long bOvfl = b.get(ValueLayout.JAVA_LONG_UNALIGNED, OFF_OVFL);
        if (aOvfl == bOvfl) return true; // same overflow blob (rare but possible)

        MemorySegment aData = bump.resolve(aOvfl, aLen);
        MemorySegment bData = bump.resolve(bOvfl, aLen);
        return aData.mismatch(bData) == -1;
    }

    /** The encoded type. */
    public static Value.Type encodedType(MemorySegment slot) {
        return Value.Type.fromTag(slot.get(ValueLayout.JAVA_BYTE, OFF_TAG));
    }

    /** True if this slot holds an overflow (out-of-line) variable-length payload. */
    public static boolean isOverflow(MemorySegment slot) {
        Value.Type t = encodedType(slot);
        if (t != Value.Type.STRING && t != Value.Type.JSON && t != Value.Type.BYTES) return false;
        return encodedLength(slot) > INLINE_THRESHOLD;
    }
}
