package org.taotree.internal.temporal;

import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.nio.ByteOrder;
import java.nio.file.Files;
import java.nio.file.Path;

import org.junit.jupiter.api.Test;
import org.taotree.internal.alloc.BumpAllocator;
import org.taotree.internal.alloc.ChunkStore;
import org.taotree.internal.alloc.SlabAllocator;
import org.taotree.internal.art.NodePtr;
import org.taotree.internal.champ.ChampMap;

import static org.junit.jupiter.api.Assertions.*;

class EntityNodeTest {

    // ── EntityNode layout tests ──────────────────────────────────────────

    @Test
    void sizeIs24Bytes() {
        assertEquals(24, EntityNode.SIZE);
    }

    @Test
    void fieldOffsetsAreContiguousAndAligned() {
        assertEquals(0, EntityNode.CURRENT_STATE_ROOT_OFFSET);
        assertEquals(8, EntityNode.ATTR_ART_ROOT_OFFSET);
        assertEquals(16, EntityNode.VERSIONS_ART_ROOT_OFFSET);
        // Last field at 16 + 8 = 24 = SIZE
    }

    @Test
    void readWriteAllFields() {
        try (var arena = Arena.ofConfined()) {
            MemorySegment node = arena.allocate(EntityNode.SIZE, 8);
            EntityNode.clear(node);

            // Initially all zeros
            assertEquals(0L, EntityNode.currentStateRoot(node));
            assertEquals(0L, EntityNode.attrArtRoot(node));
            assertEquals(0L, EntityNode.versionsArtRoot(node));

            // Set and read back
            EntityNode.setCurrentStateRoot(node, 0xAAAA_BBBB_CCCC_DDDDL);
            EntityNode.setAttrArtRoot(node, 0x1111_2222_3333_4444L);
            EntityNode.setVersionsArtRoot(node, 0x5555_6666_7777_8888L);

            assertEquals(0xAAAA_BBBB_CCCC_DDDDL, EntityNode.currentStateRoot(node));
            assertEquals(0x1111_2222_3333_4444L, EntityNode.attrArtRoot(node));
            assertEquals(0x5555_6666_7777_8888L, EntityNode.versionsArtRoot(node));
        }
    }

    @Test
    void clearZerosAllFields() {
        try (var arena = Arena.ofConfined()) {
            MemorySegment node = arena.allocate(EntityNode.SIZE, 8);
            // Fill with non-zero
            node.fill((byte) 0xFF);

            EntityNode.clear(node);

            assertEquals(0L, EntityNode.currentStateRoot(node));
            assertEquals(0L, EntityNode.attrArtRoot(node));
            assertEquals(0L, EntityNode.versionsArtRoot(node));
        }
    }

    @Test
    void copyToPreservesAllFields() {
        try (var arena = Arena.ofConfined()) {
            MemorySegment src = arena.allocate(EntityNode.SIZE, 8);
            MemorySegment dst = arena.allocate(EntityNode.SIZE, 8);

            EntityNode.setCurrentStateRoot(src, 100L);
            EntityNode.setAttrArtRoot(src, 200L);
            EntityNode.setVersionsArtRoot(src, 300L);

            EntityNode.copyTo(src, dst);

            assertEquals(100L, EntityNode.currentStateRoot(dst));
            assertEquals(200L, EntityNode.attrArtRoot(dst));
            assertEquals(300L, EntityNode.versionsArtRoot(dst));
        }
    }

    @Test
    void copyWithRootsWritesAllRoots() {
        try (var arena = Arena.ofConfined()) {
            MemorySegment src = arena.allocate(EntityNode.SIZE, 8);
            MemorySegment dst = arena.allocate(EntityNode.SIZE, 8);

            EntityNode.setCurrentStateRoot(src, 100L);
            EntityNode.setAttrArtRoot(src, 200L);
            EntityNode.setVersionsArtRoot(src, 300L);

            EntityNode.copyWithRoots(src, dst, 111L, 222L, 333L);

            // New roots in dst
            assertEquals(111L, EntityNode.currentStateRoot(dst));
            assertEquals(222L, EntityNode.attrArtRoot(dst));
            assertEquals(333L, EntityNode.versionsArtRoot(dst));
        }
    }

    @Test
    void slabAllocatedEntityNode() throws Exception {
        try (var arena = Arena.ofConfined()) {
            Path tmp = Files.createTempFile("entity-node-test-", ".dat");
            tmp.toFile().deleteOnExit();
            Files.delete(tmp);
            var cs = ChunkStore.createCheckpointed(tmp, arena, ChunkStore.DEFAULT_CHUNK_SIZE, false);
            var slab = new SlabAllocator(arena, cs, SlabAllocator.DEFAULT_SLAB_SIZE);

            int classId = slab.registerClass(EntityNode.SIZE);
            long ptr = slab.allocate(classId, NodePtr.LEAF);
            assertFalse(NodePtr.isEmpty(ptr));

            MemorySegment node = slab.resolve(ptr);
            EntityNode.clear(node);

            EntityNode.setCurrentStateRoot(node, 0xCAFEL);
            EntityNode.setAttrArtRoot(node, 0xBEEFL);

            // Re-resolve and verify persistence
            MemorySegment node2 = slab.resolve(ptr);
            assertEquals(0xCAFEL, EntityNode.currentStateRoot(node2));
            assertEquals(0xBEEFL, EntityNode.attrArtRoot(node2));

            slab.free(ptr);
        }
    }

    // ── Integration: EntityNode with CHAMP map ──────────────────────────

    @Test
    void entityNodeWithChampState() throws Exception {
        try (var arena = Arena.ofConfined()) {
            Path tmp = Files.createTempFile("entity-champ-test-", ".dat");
            tmp.toFile().deleteOnExit();
            Files.delete(tmp);
            var cs = ChunkStore.createCheckpointed(tmp, arena, ChunkStore.DEFAULT_CHUNK_SIZE, false);
            var slab = new SlabAllocator(arena, cs, SlabAllocator.DEFAULT_SLAB_SIZE);
            var bump = new BumpAllocator(arena, cs, BumpAllocator.DEFAULT_PAGE_SIZE);

            int nodeClassId = slab.registerClass(EntityNode.SIZE);

            // Allocate EntityNode
            long nodePtr = slab.allocate(nodeClassId, NodePtr.LEAF);
            MemorySegment node = slab.resolve(nodePtr);
            EntityNode.clear(node);

            // Build CHAMP state: attr 1 → valueRef 0xA, attr 2 → valueRef 0xB
            var result = new ChampMap.Result();
            result.reset();
            long champRoot = ChampMap.put(bump, ChampMap.EMPTY_ROOT, 1, 0xAL, result);
            result.reset();
            champRoot = ChampMap.put(bump, champRoot, 2, 0xBL, result);

            EntityNode.setCurrentStateRoot(node, champRoot);

            // Read back via EntityNode → ChampMap
            long stateRoot = EntityNode.currentStateRoot(slab.resolve(nodePtr));
            assertEquals(0xAL, ChampMap.get(bump, stateRoot, 1));
            assertEquals(0xBL, ChampMap.get(bump, stateRoot, 2));
            assertEquals(ChampMap.NOT_FOUND, ChampMap.get(bump, stateRoot, 3));

            slab.free(nodePtr);
        }
    }
}
