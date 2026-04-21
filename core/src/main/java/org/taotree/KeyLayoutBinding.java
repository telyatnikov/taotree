package org.taotree;

import org.taotree.layout.KeyField;
import org.taotree.layout.KeyLayout;

/**
 * Static helpers for binding a {@link KeyLayout}'s dict fields to concrete
 * {@link TaoDictionary} instances.
 *
 * <p>Two operations:
 * <ul>
 *   <li>{@link #bindDicts(TaoTree, KeyLayout)} — used on {@code create()}.
 *       For each dict field with {@code dict == null}, creates a new
 *       dictionary via {@link TaoDictionary#u16} / {@link TaoDictionary#u32}
 *       and attaches it to the field.</li>
 *   <li>{@link #rebindDicts(TaoTree, KeyLayout)} — used on {@code open()}.
 *       Walks dict fields in order and binds each to the next restored
 *       dictionary from {@link TaoTree#dictsInternal()}.</li>
 * </ul>
 *
 * <p>Extracted from {@code TaoTree.Binding} as part of Phase 8
 * {@code p8-split-taotree}; behaviour is unchanged.
 */
final class KeyLayoutBinding {

    private KeyLayoutBinding() {}

    static KeyLayout bindDicts(TaoTree tree, KeyLayout keyLayout) {
        var fields = new KeyField[keyLayout.fieldCount()];
        boolean changed = false;
        for (int i = 0; i < fields.length; i++) {
            var f = keyLayout.field(i);
            if (f instanceof KeyField.DictU16 d && d.dict() == null) {
                fields[i] = KeyField.dict16(d.name(), TaoDictionary.u16(tree));
                changed = true;
            } else if (f instanceof KeyField.DictU32 d && d.dict() == null) {
                fields[i] = KeyField.dict32(d.name(), TaoDictionary.u32(tree));
                changed = true;
            } else {
                fields[i] = f;
            }
        }
        return changed ? KeyLayout.of(fields) : keyLayout;
    }

    static KeyLayout rebindDicts(TaoTree tree, KeyLayout keyLayout) {
        var dicts = tree.dictsInternal();
        var fields = new KeyField[keyLayout.fieldCount()];
        int dictIdx = 0;
        boolean changed = false;
        for (int i = 0; i < fields.length; i++) {
            var f = keyLayout.field(i);
            if (f instanceof KeyField.DictU16 d && d.dict() == null) {
                if (dictIdx >= dicts.size())
                    throw new IllegalArgumentException(
                        "Key layout has more dict fields than restored dictionaries ("
                        + dicts.size() + ")");
                fields[i] = KeyField.dict16(d.name(), dicts.get(dictIdx++));
                changed = true;
            } else if (f instanceof KeyField.DictU32 d && d.dict() == null) {
                if (dictIdx >= dicts.size())
                    throw new IllegalArgumentException(
                        "Key layout has more dict fields than restored dictionaries ("
                        + dicts.size() + ")");
                fields[i] = KeyField.dict32(d.name(), dicts.get(dictIdx++));
                changed = true;
            } else {
                fields[i] = f;
            }
        }
        return changed ? KeyLayout.of(fields) : keyLayout;
    }
}
