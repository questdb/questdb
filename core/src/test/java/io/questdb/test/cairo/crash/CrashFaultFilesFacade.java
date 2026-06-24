package io.questdb.test.cairo.crash;

import io.questdb.std.str.LPSZ;
import io.questdb.std.str.Utf8String;
import io.questdb.test.std.TestFilesFacadeImpl;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Stream;

/**
 * Fault-injection FilesFacade that models the OS durability contract on a simulated power loss.
 *
 * <h3>Size model (original)</h3>
 * {@code durableSize[path]} is advanced by fsync/fdatasync/fsyncAndClose; {@link #crash} truncates each
 * file to its last-durable size. msync alone does not advance durable size (it flushes data pages but not
 * the inode size after a grow). Queued torn-tail ranges are zeroed after the rollback.
 *
 * <h3>Content + device-flush-batching model (added for the SYNC-mode flush-batching safety net)</h3>
 * Alongside the size model, a three-stage content model tracks, per file:
 * <ul>
 *   <li>{@code pteFlushed} — whether the file's mmap-dirty pages have been pushed to the page cache via
 *       msync since it became mappable (so {@code sync_file_range} can see them);</li>
 *   <li>{@code deviceCacheContent} — the bytes written back to the device's (volatile) cache, pending a
 *       device flush;</li>
 *   <li>{@code durableContent} — the bytes that SURVIVE a crash (promoted from the device cache by a flush).</li>
 * </ul>
 * The model intercepts msync / {@code syncFileRange} / fdatasync / fsync. The crucial batching semantic is
 * that ONE device flush (fdatasync/fsync, or an MS_SYNC msync) promotes the device-cache content of EVERY
 * tracked file to durable — exactly what lets a single batched flush make many files' content durable.
 *
 * <p>{@code readCurrent(path)} reads the real file's CURRENT bytes via a plain {@code read()}; because
 * MAP_SHARED mmap writes and {@code read()} share the kernel page cache, a {@code read()} sees the bytes a
 * test wrote into an mmap'd region (even before any msync). msync is what moves those page-cache bytes on
 * toward the device; {@code read()} merely lets the model snapshot the current page-cache state at a sync
 * point.
 *
 * <h3>Modeling limitations</h3>
 * <ul>
 *   <li>Writes are not intercepted, so {@code pteFlushed} is never CLEARED on a write after an msync. This
 *       is acceptable for the target optimization, which never writes between its msync and its
 *       {@code sync_file_range}; the self-tests exercise the un-msync'd path on FRESH files only.</li>
 *   <li>The device-cache/durable snapshots are full-file byte copies taken at sync points. Files are small
 *       in these tests, so this is cheap; if it ever became hot it could be narrowed to a dirty range.</li>
 * </ul>
 */
public class CrashFaultFilesFacade extends TestFilesFacadeImpl {
    // fds are tracked so fsync can map a fd back to its file; only fsync advances durableSize.
    private final Map<Long, String> fdToPath = new HashMap<>();
    private final Map<String, Long> durableSize = new HashMap<>();
    private final Map<String, List<long[]>> tornTails = new HashMap<>();
    private final java.util.List<String> syncOrder = new java.util.ArrayList<>();

    // --- content + device-flush-batching model ---
    // Live mmap mappings: base address -> path, so msync(addr,...) can resolve the file. mremap re-keys; munmap removes.
    private final Map<Long, long[]> mmapAddrRange = new HashMap<>(); // addr -> {endAddrExclusive}
    private final Map<Long, String> mmapAddrToPath = new HashMap<>();
    // Every file the content model is tracking (insertion-ordered so doFlush() is deterministic).
    private final Set<String> trackedFiles = new LinkedHashSet<>();
    private final Map<String, Boolean> pteFlushed = new HashMap<>();
    private final Map<String, byte[]> deviceCacheContent = new HashMap<>();
    private final Map<String, byte[]> durableContent = new HashMap<>();

    private int durabilityOps = 0;
    private int crashAtOp = -1; // -1 = disarmed

    /** Ordered list of file paths as they were fsync'd/fsyncAndClose'd (for sync-order assertions). */
    public java.util.List<String> getSyncOrder() { return syncOrder; }

    @Override
    public long openAppend(LPSZ name) {
        long fd = super.openAppend(name);
        if (fd > -1) {
            fdToPath.put(fd, toAbsPath(name));
        }
        return fd;
    }

    @Override
    public long openCleanRW(LPSZ name, long size) {
        long fd = super.openCleanRW(name, size);
        if (fd > -1) {
            fdToPath.put(fd, toAbsPath(name));
        }
        return fd;
    }

    @Override
    public long openRW(LPSZ name, int opts) {
        long fd = super.openRW(name, opts);
        if (fd > -1) {
            fdToPath.put(fd, toAbsPath(name));
        }
        return fd;
    }

    @Override
    public long openRO(LPSZ name) {
        long fd = super.openRO(name);
        if (fd > -1) {
            fdToPath.put(fd, toAbsPath(name));
        }
        return fd;
    }

    @Override
    public boolean close(long fd) {
        fdToPath.remove(fd);
        return super.close(fd);
    }

    @Override
    public long mmap(long fd, long len, long offset, int flags, int memoryTag) {
        long addr = super.mmap(fd, len, offset, flags, memoryTag);
        recordMmap(fd, addr, len);
        return addr;
    }

    @Override
    public long mmapNoCache(long fd, long len, long offset, int flags, int memoryTag) {
        long addr = super.mmapNoCache(fd, len, offset, flags, memoryTag);
        recordMmap(fd, addr, len);
        return addr;
    }

    @Override
    public long mremap(long fd, long addr, long previousSize, long newSize, long offset, int mode, int memoryTag) {
        long newAddr = super.mremap(fd, addr, previousSize, newSize, offset, mode, memoryTag);
        remapMoved(addr, newAddr, newSize, fd);
        return newAddr;
    }

    @Override
    public long mremapNoCache(long fd, long addr, long previousSize, long newSize, long offset, int mode, int memoryTag) {
        long newAddr = super.mremapNoCache(fd, addr, previousSize, newSize, offset, mode, memoryTag);
        remapMoved(addr, newAddr, newSize, fd);
        return newAddr;
    }

    @Override
    public void munmap(long address, long size, int memoryTag) {
        // Drop the addr->path mapping but KEEP the content model state keyed by path: a file's durable
        // content must outlive its mapping (e.g. unmap-then-crash), exactly like the size model.
        mmapAddrRange.remove(address);
        mmapAddrToPath.remove(address);
        super.munmap(address, size, memoryTag);
    }

    @Override
    public void fdatasync(long fd) {
        super.fdatasync(fd);
        String p = fdToPath.get(fd);
        if (p != null) {
            syncOrder.add(p);
            // Content model: a real fdatasync is writeback + device flush. Snapshot this file's current
            // bytes into its device cache, then flush -> promotes EVERY tracked file's device cache to durable.
            track(p);
            deviceCacheContent.put(p, readCurrent(p));
            doFlush();
        }
        recordDurable(fd);
        bumpDurabilityOp();
    }

    @Override
    public void fsync(long fd) {
        super.fsync(fd);
        String p = fdToPath.get(fd);
        if (p != null) {
            syncOrder.add(p);
            track(p);
            deviceCacheContent.put(p, readCurrent(p));
            doFlush();
        }
        recordDurable(fd);
        bumpDurabilityOp();
    }

    @Override
    public void fsyncAndClose(long fd) {
        final String p = fdToPath.get(fd);
        final long size = p != null ? super.length(fd) : -1L;
        final byte[] snapshot = p != null ? readCurrent(p) : null; // read BEFORE close drops the fd
        super.fsyncAndClose(fd); // performs fsync + close; close() below drops the fd
        if (p != null) {
            syncOrder.add(p);
            if (size >= 0L) {
                durableSize.put(p, size);
            }
            // fsync + close is a device flush too: snapshot+promote like fsync.
            track(p);
            deviceCacheContent.put(p, snapshot);
            doFlush();
        }
        bumpDurabilityOp();
    }

    @Override
    public void msync(long addr, long len, boolean async) {
        super.msync(addr, len, async);
        String p = pathForAddr(addr);
        if (p != null) {
            track(p);
            // msync pushes this mapping's dirty pages to the page cache -> sync_file_range can now see them.
            pteFlushed.put(p, Boolean.TRUE);
            if (!async) {
                // MS_SYNC == writeback to device cache + a device flush. Snapshot current bytes, then flush
                // (which promotes EVERY tracked file's device cache to durable -- the batching semantic).
                deviceCacheContent.put(p, readCurrent(p));
                doFlush();
            }
            // MS_ASYNC: only dirties the page cache toward writeback; nothing reaches the device cache here.
        }
        bumpDurabilityOp();
    }

    @Override
    public int syncFileRange(long fd, long offset, long nbytes, int flags) {
        int rc = super.syncFileRange(fd, offset, nbytes, flags);
        String p = fdToPath.get(fd);
        if (p != null) {
            track(p);
            if (Boolean.TRUE.equals(pteFlushed.get(p))) {
                // Writeback of (already-page-cache-dirty) pages to the device cache. NO device flush.
                deviceCacheContent.put(p, readCurrent(p));
            }
            // If NOT pteFlushed: NO-OP. sync_file_range cannot see mmap-dirty pages the kernel does not yet
            // track as dirty in the page cache (they were never msync'd / write()-n). This is the real footgun.
        }
        return rc;
    }

    /** Clear all tracked fd→path and durable-size/content state (for reuse across crash/retry cycles). */
    public void reset() {
        fdToPath.clear();
        durableSize.clear();
        tornTails.clear();
        syncOrder.clear();
        mmapAddrRange.clear();
        mmapAddrToPath.clear();
        trackedFiles.clear();
        pteFlushed.clear();
        deviceCacheContent.clear();
        durableContent.clear();
        durabilityOps = 0;
        crashAtOp = -1;
    }

    /** Record current sizes (and content) of all files under dbRoot as durable ("prior committed, log-journaled"). */
    public void markDurableBaseline(CharSequence dbRoot) {
        walk(dbRoot, p -> {
            String key = p.toAbsolutePath().toString();
            try {
                durableSize.put(key, java.nio.file.Files.size(p));
            } catch (java.io.IOException e) {
                throw new java.io.UncheckedIOException(e);
            }
            // Mirror into the content model: whatever is on disk now is durable, and is the device-cache state.
            byte[] now = readCurrent(key);
            track(key);
            deviceCacheContent.put(key, now);
            durableContent.put(key, now);
        });
    }

    /** Zero [offset, offset+len) of the given file when crash() runs (deterministic torn-write injection). */
    public void tornTail(LPSZ name, long offset, long len) {
        tornTails.computeIfAbsent(toAbsPath(name), k -> new ArrayList<>())
                .add(new long[]{offset, len});
    }

    /**
     * Roll every file under {@code dbRoot} back to its durable state, then apply any torn-tail ranges.
     * <p>
     * A file with a {@code durableContent} snapshot is restored to those exact bytes (content rollback,
     * which also fixes the durable length); this GENERALIZES the size-only rollback. A file the content
     * model never tracked falls back to truncation at its last-durable size.
     */
    public void crash(CharSequence dbRoot) {
        walk(dbRoot, p -> {
            String key = p.toAbsolutePath().toString();
            byte[] durable = durableContent.get(key);
            try (FileChannel ch = FileChannel.open(p, StandardOpenOption.WRITE)) {
                if (durable != null) {
                    // Content rollback: overwrite the prefix with durable bytes, then truncate to its length.
                    if (durable.length > 0) {
                        ch.write(ByteBuffer.wrap(durable), 0);
                    }
                    if (ch.size() > durable.length) {
                        ch.truncate(durable.length);
                    }
                } else {
                    // No content snapshot for this file: original size-only rollback.
                    Long durableSz = durableSize.get(key);
                    long target = durableSz != null ? durableSz : 0L;
                    if (ch.size() > target) {
                        ch.truncate(target);
                    }
                }
                List<long[]> ranges = tornTails.get(key);
                if (ranges != null) {
                    for (long[] r : ranges) {
                        if (r[1] < 0 || r[1] > Integer.MAX_VALUE) throw new IllegalArgumentException("tornTail len out of range: " + r[1]);
                        int n = (int) r[1];
                        ByteBuffer zeros = ByteBuffer.allocate(n);
                        ch.write(zeros, r[0]);
                    }
                }
                ch.force(true);
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        });
    }

    /** Arm a simulated crash: throw CrashSimulationError on the n-th durability op (fsync/fsyncAndClose/msync). */
    public void armCrashAt(int n) {
        this.crashAtOp = n;
    }

    /** Number of durability ops (fsync/fsyncAndClose/msync) observed so far. */
    public int durabilityOpCount() {
        return durabilityOps;
    }

    // === content-model test introspection (used by CrashModelSelfCheckTest) ===

    /** True if the content model has observed an msync on this file (so sync_file_range can see its pages). */
    public boolean isPteFlushed(CharSequence absPath) {
        return Boolean.TRUE.equals(pteFlushed.get(Paths.get(absPath.toString()).toAbsolutePath().toString()));
    }

    /** The bytes the content model currently considers durable for this file (null if untracked). */
    public byte[] durableContentOf(CharSequence absPath) {
        return durableContent.get(Paths.get(absPath.toString()).toAbsolutePath().toString());
    }

    private void bumpDurabilityOp() {
        durabilityOps++;
        if (crashAtOp > 0 && durabilityOps >= crashAtOp) {
            crashAtOp = -1; // one-shot
            throw new CrashSimulationError(durabilityOps);
        }
    }

    private void recordDurable(long fd) {
        String p = fdToPath.get(fd);
        if (p != null) {
            durableSize.put(p, super.length(fd));
        }
    }

    // --- content-model helpers ---

    /** ONE device flush promotes EVERY tracked file's device-cache content to durable (the batching semantic). */
    private void doFlush() {
        for (String f : trackedFiles) {
            byte[] dc = deviceCacheContent.get(f);
            if (dc != null) {
                durableContent.put(f, dc);
            }
        }
    }

    /** Begin tracking a file in the content model; seeds device-cache and durable snapshots from disk on first sight. */
    private void track(String path) {
        if (trackedFiles.add(path)) {
            byte[] initial = readCurrent(path);
            deviceCacheContent.put(path, initial);
            durableContent.put(path, initial);
            pteFlushed.put(path, Boolean.FALSE);
        }
    }

    private void recordMmap(long fd, long addr, long len) {
        if (addr == 0 || addr == -1L) {
            return;
        }
        String p = fdToPath.get(fd);
        if (p != null) {
            mmapAddrRange.put(addr, new long[]{addr + len});
            mmapAddrToPath.put(addr, p);
            track(p);
        }
    }

    private void remapMoved(long oldAddr, long newAddr, long newSize, long fd) {
        if (newAddr == 0 || newAddr == -1L) {
            return;
        }
        String p = mmapAddrToPath.remove(oldAddr);
        mmapAddrRange.remove(oldAddr);
        if (p == null) {
            p = fdToPath.get(fd);
        }
        if (p != null) {
            mmapAddrRange.put(newAddr, new long[]{newAddr + newSize});
            mmapAddrToPath.put(newAddr, p);
            track(p);
        }
    }

    /** Resolve the file an msync address belongs to: exact base match first, else range containment. */
    private String pathForAddr(long addr) {
        String p = mmapAddrToPath.get(addr);
        if (p != null) {
            return p;
        }
        for (Map.Entry<Long, long[]> e : mmapAddrRange.entrySet()) {
            long base = e.getKey();
            long end = e.getValue()[0];
            if (addr >= base && addr < end) {
                return mmapAddrToPath.get(base);
            }
        }
        return null;
    }

    /**
     * Read the file's CURRENT on-disk/page-cache bytes via a plain read(). MAP_SHARED mmap writes share the
     * page cache with read(), so this observes bytes written into a mapping even before any msync.
     */
    private static byte[] readCurrent(String path) {
        Path pth = Paths.get(path);
        if (!Files.exists(pth)) {
            return new byte[0];
        }
        try {
            return Files.readAllBytes(pth);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    /** Convert LPSZ (null-terminated native bytes) to a canonical absolute path String. */
    private static String toAbsPath(LPSZ name) {
        return Paths.get(Utf8String.newInstance(name).toString()).toAbsolutePath().toString();
    }

    private static void walk(CharSequence dbRoot, java.util.function.Consumer<Path> fileFn) {
        Path root = Paths.get(dbRoot.toString());
        if (!Files.exists(root)) {
            return;
        }
        try (Stream<Path> s = Files.walk(root)) {
            s.filter(Files::isRegularFile).forEach(fileFn);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }
}
