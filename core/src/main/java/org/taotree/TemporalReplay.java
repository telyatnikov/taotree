package org.taotree;

import java.lang.foreign.MemorySegment;

import org.taotree.internal.art.ArtSearch;
import org.taotree.internal.art.NodePtr;
import org.taotree.internal.temporal.AttributeRun;
import org.taotree.internal.temporal.EntityNode;
import org.taotree.internal.value.ValueCodec;

/**
 * Replays the temporal history of every entity in a source {@link TaoTree.ReadScope}
 * into a destination {@link TaoTree.WriteScope}, preserving per-attribute runs.
 *
 * <p>Extracted from {@code TaoTree.WriteScope.copyFrom} to keep that method
 * short. All iteration order matches the source's ART walk (unsigned-BE on
 * {@code (attrId, firstSeen)}) which equals natural chronological order since
 * {@code TIMELESS=0} and all real timestamps are positive.
 */
final class TemporalReplay {

    private static final int ATTR_KEY_SLOT = (AttributeRun.KEY_LEN + 7) & ~7; // 16

    private TemporalReplay() {}

    static void replay(TaoTree.WriteScope dst, TaoTree.ReadScope src) {
        TaoTree dstTree = dst.tree();
        TaoTree srcTree = src.tree();

        final java.util.function.LongFunction<MemorySegment> resolver =
                srcTree.ensureTemporalReader().makeResolver();

        src.forEach(entityKey -> {
            long leafPtr = src.lookup(entityKey);
            if (leafPtr == TaoTree.NOT_FOUND) return true;
            MemorySegment entityNode = srcTree.leafValueImpl(leafPtr).asReadOnly();
            long attrRoot = EntityNode.attrArtRoot(entityNode);
            if (NodePtr.isEmpty(attrRoot)) return true;

            java.util.Map<Integer, java.util.List<long[]>> byAttr = new java.util.LinkedHashMap<>();

            byte[] searchBytes = new byte[AttributeRun.KEY_LEN];
            MemorySegment searchKey = MemorySegment.ofArray(searchBytes);
            long curLeaf = ArtSearch.successor(resolver, attrRoot,
                    searchKey, AttributeRun.KEY_LEN);
            while (!NodePtr.isEmpty(curLeaf)) {
                MemorySegment full = resolver.apply(curLeaf);
                int aId = AttributeRun.keyAttrId(full);
                long firstSeen = AttributeRun.keyFirstSeen(full);
                MemorySegment val = full.asSlice(ATTR_KEY_SLOT);
                long lastSeen = AttributeRun.lastSeen(val);
                long srcValueRef = AttributeRun.valueRef(val);

                byAttr.computeIfAbsent(aId, k -> new java.util.ArrayList<>())
                        .add(new long[] { firstSeen, lastSeen, srcValueRef });

                AttributeRun.encodeKey(searchKey, aId, firstSeen + 1);
                curLeaf = ArtSearch.successor(resolver, attrRoot,
                        searchKey, AttributeRun.KEY_LEN);
            }

            if (byAttr.isEmpty()) return true;
            MemorySegment entityKeySeg = MemorySegment.ofArray(entityKey);

            for (var e : byAttr.entrySet()) {
                int srcAttrId = e.getKey();
                String name = srcTree.attrDictionary().reverseLookup(srcAttrId);
                if (name == null) continue;
                int destAttrId = dstTree.internAttr(name);

                for (long[] run : e.getValue()) {
                    long firstSeen = run[0];
                    long lastSeen  = run[1];
                    long srcValRef = run[2];
                    long destValRef;
                    if (srcValRef == org.taotree.internal.temporal.AttributeRun.TOMBSTONE_VALUE_REF) {
                        // Tombstone runs replay as tombstones; never decode -1 as a value.
                        destValRef = org.taotree.internal.temporal.AttributeRun.TOMBSTONE_VALUE_REF;
                    } else {
                        Value v = ValueCodec.decodeStandalone(srcValRef, srcTree.bump());
                        destValRef = ValueCodec.encodeStandalone(v, dstTree.bump());
                    }
                    dst.putTemporalImpl(entityKeySeg, destAttrId, destValRef, firstSeen);
                    if (lastSeen > firstSeen) {
                        dst.putTemporalImpl(entityKeySeg, destAttrId, destValRef, lastSeen);
                    }
                }
            }
            return true;
        });
    }
}
