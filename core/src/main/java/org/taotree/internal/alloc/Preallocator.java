package org.taotree.internal.alloc;

import java.io.IOException;
import java.lang.foreign.*;
import java.lang.invoke.MethodHandle;
import java.lang.invoke.VarHandle;
import java.nio.file.Path;

/**
 * Platform-specific physical block preallocation and durable sync.
 *
 * <p>On macOS: uses {@code fcntl(F_PREALLOCATE)} for block reservation and
 * {@code fcntl(F_FULLFSYNC)} for durable sync.
 * <p>On Linux: uses {@code fallocate()} for block reservation and
 * {@code fdatasync()} for durable sync.
 * <p>Fallback: no-op preallocation (sparse files), standard fsync.
 *
 * <p>Uses Java 22+ Foreign Function & Memory API to call native syscalls directly.
 * No reflection, no JNI, no {@code --add-opens}.
 *
 * <p><b>Thread safety:</b> all methods are stateless and thread-safe.
 */
public final class Preallocator {

    private Preallocator() {}

    private static final boolean IS_MAC;
    private static final boolean IS_LINUX;
    private static final boolean NATIVE_AVAILABLE;

    // macOS constants
    private static final int F_PREALLOCATE = 42;
    private static final int F_FULLFSYNC = 51;
    private static final int F_ALLOCATECONTIG = 0x02;
    private static final int F_ALLOCATEALL = 0x04;
    private static final int F_PEOFPOSMODE = 3;
    private static final int O_RDWR = 0x0002;

    // Linux constants
    private static final int O_RDWR_LINUX = 2;

    // fstore_t layout (macOS): { uint fst_flags, int fst_posmode, off_t fst_offset, off_t fst_length, off_t fst_bytesalloc }
    private static final StructLayout FSTORE_LAYOUT = MemoryLayout.structLayout(
        ValueLayout.JAVA_INT.withName("fst_flags"),
        ValueLayout.JAVA_INT.withName("fst_posmode"),
        ValueLayout.JAVA_LONG.withName("fst_offset"),
        ValueLayout.JAVA_LONG.withName("fst_length"),
        ValueLayout.JAVA_LONG.withName("fst_bytesalloc")
    );

    private static final VarHandle FST_FLAGS = FSTORE_LAYOUT.varHandle(MemoryLayout.PathElement.groupElement("fst_flags"));
    private static final VarHandle FST_POSMODE = FSTORE_LAYOUT.varHandle(MemoryLayout.PathElement.groupElement("fst_posmode"));
    private static final VarHandle FST_OFFSET = FSTORE_LAYOUT.varHandle(MemoryLayout.PathElement.groupElement("fst_offset"));
    private static final VarHandle FST_LENGTH = FSTORE_LAYOUT.varHandle(MemoryLayout.PathElement.groupElement("fst_length"));
    private static final VarHandle FST_BYTESALLOC = FSTORE_LAYOUT.varHandle(MemoryLayout.PathElement.groupElement("fst_bytesalloc"));

    // Native handles (resolved once)
    private static final MethodHandle OPEN;
    private static final MethodHandle CLOSE;
    private static final MethodHandle FCNTL;           // macOS: fcntl(fd, cmd, arg)
    private static final MethodHandle FTRUNCATE;
    private static final MethodHandle FALLOCATE;       // Linux only
    private static final MethodHandle FCNTL_INT;       // fcntl(fd, cmd) for F_FULLFSYNC

    static {
        String os = System.getProperty("os.name", "").toLowerCase();
        IS_MAC = os.contains("mac") || os.contains("darwin");
        IS_LINUX = os.contains("linux");

        MethodHandle openH = null, closeH = null, fcntlH = null, ftruncateH = null;
        MethodHandle fallocateH = null, fcntlIntH = null;
        boolean available = false;

        try {
            var linker = Linker.nativeLinker();
            var lookup = linker.defaultLookup();

            openH = linker.downcallHandle(
                lookup.find("open").orElseThrow(),
                FunctionDescriptor.of(ValueLayout.JAVA_INT,
                    ValueLayout.ADDRESS, ValueLayout.JAVA_INT, ValueLayout.JAVA_INT),
                Linker.Option.firstVariadicArg(2));

            closeH = linker.downcallHandle(
                lookup.find("close").orElseThrow(),
                FunctionDescriptor.of(ValueLayout.JAVA_INT, ValueLayout.JAVA_INT));

            ftruncateH = linker.downcallHandle(
                lookup.find("ftruncate").orElseThrow(),
                FunctionDescriptor.of(ValueLayout.JAVA_INT, ValueLayout.JAVA_INT, ValueLayout.JAVA_LONG));

            if (IS_MAC) {
                fcntlH = linker.downcallHandle(
                    lookup.find("fcntl").orElseThrow(),
                    FunctionDescriptor.of(ValueLayout.JAVA_INT,
                        ValueLayout.JAVA_INT, ValueLayout.JAVA_INT, ValueLayout.ADDRESS),
                    Linker.Option.firstVariadicArg(2));

                fcntlIntH = linker.downcallHandle(
                    lookup.find("fcntl").orElseThrow(),
                    FunctionDescriptor.of(ValueLayout.JAVA_INT,
                        ValueLayout.JAVA_INT, ValueLayout.JAVA_INT),
                    Linker.Option.firstVariadicArg(2));
            }

            if (IS_LINUX) {
                var fallocateSym = lookup.find("fallocate");
                if (fallocateSym.isPresent()) {
                    fallocateH = linker.downcallHandle(
                        fallocateSym.get(),
                        FunctionDescriptor.of(ValueLayout.JAVA_INT,
                            ValueLayout.JAVA_INT, ValueLayout.JAVA_INT,
                            ValueLayout.JAVA_LONG, ValueLayout.JAVA_LONG));
                }
            }

            available = true;
        } catch (Throwable t) {
            // FFM not available or native lookup failed — use fallback
        }

        OPEN = openH;
        CLOSE = closeH;
        FCNTL = fcntlH;
        FTRUNCATE = ftruncateH;
        FALLOCATE = fallocateH;
        FCNTL_INT = fcntlIntH;
        NATIVE_AVAILABLE = available;
    }

    /**
     * Preallocate physical blocks for a file region.
     *
     * <p>On macOS: uses {@code fcntl(F_PREALLOCATE)} to reserve contiguous blocks,
     * falling back to non-contiguous if contiguous fails.
     * <p>On Linux: uses {@code fallocate()}.
     * <p>Fallback: no-op (file remains sparse).
     *
     * @param path   the file path (must exist and be writable)
     * @param offset starting byte offset
     * @param length number of bytes to preallocate
     * @return true if physical blocks were reserved, false if fallback (no-op)
     * @throws IOException if the native call fails
     */
    public static boolean preallocate(Path path, long offset, long length) throws IOException {
        if (!NATIVE_AVAILABLE) return false;

        if (IS_MAC) return preallocateMac(path, offset, length);
        if (IS_LINUX) return preallocateLinux(path, offset, length);
        return false;
    }

    /**
     * Perform a durable sync (flush to physical media).
     *
     * <p>On macOS: uses {@code fcntl(F_FULLFSYNC)} which asks the drive to flush
     * its hardware write cache.
     * <p>On other platforms: no-op (caller should use {@code FileChannel.force(true)}).
     *
     * @param path the file path
     * @return true if F_FULLFSYNC was used, false if fallback
     */
    public static boolean fullSync(Path path) throws IOException {
        if (!NATIVE_AVAILABLE || !IS_MAC || FCNTL_INT == null) return false;

        try (var arena = Arena.ofConfined()) {
            int fd = nativeOpen(arena, path, IS_MAC ? O_RDWR : O_RDWR_LINUX);
            if (fd < 0) return false;
            try {
                int ret = (int) FCNTL_INT.invoke(fd, F_FULLFSYNC);
                return ret == 0;
            } catch (Throwable t) {
                throw new IOException("F_FULLFSYNC failed", t);
            } finally {
                nativeClose(fd);
            }
        }
    }

    /** Returns true if native preallocation is supported on this platform. */
    public static boolean isSupported() {
        return NATIVE_AVAILABLE && (IS_MAC || (IS_LINUX && FALLOCATE != null));
    }

    /** Returns a description of the preallocation method available. */
    public static String method() {
        if (!NATIVE_AVAILABLE) return "none (FFM unavailable)";
        if (IS_MAC) return "macOS fcntl(F_PREALLOCATE)";
        if (IS_LINUX && FALLOCATE != null) return "Linux fallocate()";
        return "none (unsupported platform)";
    }

    // ---- macOS ----

    private static boolean preallocateMac(Path path, long offset, long length) throws IOException {
        try (var arena = Arena.ofConfined()) {
            int fd = nativeOpen(arena, path, O_RDWR);
            if (fd < 0) return false;
            try {
                MemorySegment fstore = arena.allocate(FSTORE_LAYOUT);

                // Try contiguous first
                FST_FLAGS.set(fstore, 0L, F_ALLOCATECONTIG | F_ALLOCATEALL);
                FST_POSMODE.set(fstore, 0L, F_PEOFPOSMODE);
                FST_OFFSET.set(fstore, 0L, offset);
                FST_LENGTH.set(fstore, 0L, length);
                FST_BYTESALLOC.set(fstore, 0L, 0L);

                int ret = (int) FCNTL.invoke(fd, F_PREALLOCATE, fstore);
                if (ret != 0) {
                    // Fallback: non-contiguous
                    FST_FLAGS.set(fstore, 0L, F_ALLOCATEALL);
                    FST_BYTESALLOC.set(fstore, 0L, 0L);
                    ret = (int) FCNTL.invoke(fd, F_PREALLOCATE, fstore);
                }
                if (ret != 0) return false;

                // Set file size (F_PREALLOCATE doesn't change the file size)
                long newSize = offset + length;
                ret = (int) FTRUNCATE.invoke(fd, newSize);
                return ret == 0;
            } catch (IOException e) {
                throw e;
            } catch (Throwable t) {
                throw new IOException("F_PREALLOCATE failed", t);
            } finally {
                nativeClose(fd);
            }
        }
    }

    // ---- Linux ----

    private static boolean preallocateLinux(Path path, long offset, long length) throws IOException {
        if (FALLOCATE == null) return false;
        try (var arena = Arena.ofConfined()) {
            int fd = nativeOpen(arena, path, O_RDWR_LINUX);
            if (fd < 0) return false;
            try {
                int ret = (int) FALLOCATE.invoke(fd, 0, offset, length);
                return ret == 0;
            } catch (Throwable t) {
                throw new IOException("fallocate failed", t);
            } finally {
                nativeClose(fd);
            }
        }
    }

    // ---- Native helpers ----

    private static int nativeOpen(Arena arena, Path path, int flags) throws IOException {
        try {
            var pathStr = arena.allocateFrom(path.toAbsolutePath().toString());
            return (int) OPEN.invoke(pathStr, flags, 0);
        } catch (Throwable t) {
            throw new IOException("native open failed", t);
        }
    }

    private static void nativeClose(int fd) {
        try {
            CLOSE.invoke(fd);
        } catch (Throwable ignored) {}
    }
}
