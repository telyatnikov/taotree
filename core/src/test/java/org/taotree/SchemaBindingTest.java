package org.taotree;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import org.junit.jupiter.api.Test;
import org.taotree.internal.persist.SchemaBinding;
import org.taotree.layout.KeyField;
import org.taotree.layout.KeyLayout;

class SchemaBindingTest {

    @Test
    void roundTripAllKinds() {
        var layout = KeyLayout.of(
                KeyField.uint8("a"),
                KeyField.uint16("bb"),
                KeyField.uint32("ccc"),
                KeyField.uint64("dddd"),
                KeyField.int64("eeeee"),
                KeyField.dict16("f6"),
                KeyField.dict32("g777")
        );
        byte[] bytes = SchemaBinding.serialize(layout);
        KeyLayout back = SchemaBinding.deserialize(bytes);

        assertEquals(layout.fieldCount(), back.fieldCount());
        assertEquals(layout.totalWidth(), back.totalWidth());
        for (int i = 0; i < layout.fieldCount(); i++) {
            assertEquals(layout.field(i).name(), back.field(i).name());
            assertEquals(layout.field(i).width(), back.field(i).width());
            assertEquals(layout.field(i).getClass(), back.field(i).getClass());
        }
        // Deterministic: same layout → same bytes.
        assertArrayEquals(bytes, SchemaBinding.serialize(back));
    }

    @Test
    void firstMismatchDetectsFieldRenames() {
        var a = KeyLayout.of(KeyField.uint32("year"), KeyField.dict16("species"));
        var b = KeyLayout.of(KeyField.uint32("year"), KeyField.dict16("taxon"));
        String mismatch = SchemaBinding.firstMismatch(
                SchemaBinding.serialize(a), SchemaBinding.serialize(b));
        assertNotEquals(null, mismatch);
    }

    @Test
    void firstMismatchDetectsKindChange() {
        var a = KeyLayout.of(KeyField.uint32("k"));
        var b = KeyLayout.of(KeyField.int64("k"));
        String mismatch = SchemaBinding.firstMismatch(
                SchemaBinding.serialize(a), SchemaBinding.serialize(b));
        assertNotEquals(null, mismatch);
    }

    @Test
    void firstMismatchReturnsNullForEquivalentLayouts() {
        var a = KeyLayout.of(KeyField.uint32("k"), KeyField.dict16("name"));
        var b = KeyLayout.of(KeyField.uint32("k"), KeyField.dict16("name"));
        assertEquals(null, SchemaBinding.firstMismatch(
                SchemaBinding.serialize(a), SchemaBinding.serialize(b)));
    }

    @Test
    void deserializeRejectsTruncatedPayload() {
        byte[] bytes = SchemaBinding.serialize(
                KeyLayout.of(KeyField.uint32("year")));
        byte[] truncated = new byte[bytes.length - 2];
        System.arraycopy(bytes, 0, truncated, 0, truncated.length);
        assertThrows(IllegalArgumentException.class, () -> SchemaBinding.deserialize(truncated));
    }

    @Test
    void deserializeRejectsUnknownVersion() {
        byte[] bytes = SchemaBinding.serialize(
                KeyLayout.of(KeyField.uint32("year")));
        bytes[0] = 99;
        assertThrows(IllegalArgumentException.class, () -> SchemaBinding.deserialize(bytes));
    }
}
