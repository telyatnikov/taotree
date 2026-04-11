package org.taotree.fray;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Comparator;

/**
 * Shared utilities for Fray concurrency tests.
 *
 * <h3>Fray test design rules</h3>
 * <ul>
 *   <li><b>2 threads max</b> — Fray explores interleavings combinatorially;
 *       each extra thread squares the search space.</li>
 *   <li><b>1–3 operations per thread</b> — keep traces short so Fray's POS
 *       scheduler covers the space in 10 iterations.</li>
 *   <li><b>Pre-populate in the main thread</b> — only the concurrent part
 *       should run inside spawned threads.</li>
 *   <li><b>Clean up in finally</b> — each test creates a fresh temp directory
 *       and deletes it to avoid exhausting the RAMDisk.</li>
 * </ul>
 */
class FrayTestBase {

    /** 1 MB chunk — large enough for slab allocation (256 pages), small enough for RAMDisk. */
    protected static final long CHUNK = 256 * 4096L;

    protected static Path newTempDir() throws IOException {
        return Files.createTempDirectory("fray-test");
    }

    protected static void deleteRecursively(Path dir) {
        if (dir == null || !Files.exists(dir)) return;
        try (var stream = Files.walk(dir)) {
            stream.sorted(Comparator.reverseOrder())
                  .forEach(f -> { try { Files.deleteIfExists(f); } catch (IOException ignore) {} });
        } catch (IOException ignore) {}
    }

    /** Encode an int as a big-endian 4-byte key. */
    protected static byte[] intKey(int value) {
        return new byte[]{
            (byte) (value >>> 24), (byte) (value >>> 16),
            (byte) (value >>> 8),  (byte) value
        };
    }

    /** Keys that share first byte 0xBB, landing in the same inner node to trigger growth. */
    protected static byte[] growthKey(int k) {
        return new byte[]{ (byte) 0xBB, (byte) k, 0x00, 0x01 };
    }
}
