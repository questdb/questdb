package io.questdb.test.cairo.crash;

import io.questdb.std.str.LPSZ;
import io.questdb.std.str.Utf8String;
import io.questdb.test.std.TestFilesFacadeImpl;

import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Stream;

/**
 * Fault-injection FilesFacade that models the OS durability contract on a simulated power loss.
 *
 * <h3>Size model</h3>
 * {@code durableSize[path]} is advanced by fsync/fdatasync/fsyncAndClose; {@link #crash} truncates each file to
 * its last durable size. msync alone does not advance the durable size: it flushes data pages but not the inode
 * size after a grow. Queued torn-tail ranges are zeroed after the rollback.
 *
 * <h3>Content and device-flush model</h3>
 * Alongside the size model, a content model tracks, per file:
 * <ul>
 *   <li>{@code pteFlushed}: whether the file's mmap-dirty pages have been pushed to the page cache via msync
 *       since it became mappable, so {@code sync_file_range} can see them;</li>
 *   <li>{@code deviceCacheContent}: the bytes written back to the device's volatile cache, pending a device
 *       flush. A whole-file sync ({@code fsync}, {@code fdatasync}, {@code syncfs}) writes back the whole current
 *       image; a ranged one ({@code msync(MS_SYNC)}, {@code sync_file_range}) only its range, so a page dirtied
 *       through an earlier, already unmapped mapping stays behind;</li>
 *   <li>{@code durableContent}: the bytes that survive a crash, promoted from the device cache by a flush and
 *       truncated to the journaled extent (see below).</li>
 * </ul>
 *
 * <h3>Data at the device versus journaled metadata</h3>
 * A written byte is durable only when three independent things have happened:
 * <ol>
 *   <li><b>data-at-device</b>: the bytes reach the device's volatile write cache.
 *       {@code sync_file_range(WRITE | WAIT_AFTER)} and {@code msync} push them there. The {@code sync_file_range} man
 *       page: it does not flush disk write caches and "does not provide any data integrity on systems with volatile
 *       disk write caches";</li>
 *   <li><b>metadata-journaled</b>: the extent-tree and {@code i_size} update that makes those blocks belong to
 *       the file. Only a journal commit provides it ({@code fsync}, {@code fdatasync}, and {@code msync(MS_SYNC)},
 *       a range fsync). {@code sync_file_range} does not: "When writing into preallocated space, many filesystems
 *       also require calls into the block allocator, which this system call does not sync out to disk" and "None
 *       of these operations writes out the file's metadata." QuestDB writes into {@code posix_fallocate}'d
 *       (unwritten-extent) space, so a within-page append converts an unwritten extent and needs a journal
 *       commit even when {@code i_size} does not change;</li>
 *   <li><b>device-flush</b>: the volatile write cache is flushed to non-volatile media (REQ_PREFLUSH/REQ_FUA,
 *       kernel block/writeback_cache_control). {@code fsync}/{@code fdatasync} ("This includes writing through or
 *       flushing a disk cache if present") and {@code msync(MS_SYNC)} carry one; {@code sync_file_range} does
 *       not.</li>
 * </ol>
 * The model tracks three watermarks per file:
 * <ul>
 *   <li>{@code writtenDataEnd[F]}: the file-absolute high-water of bytes actually written, as opposed to
 *       {@code posix_fallocate}'d unwritten zeros. {@code write}/{@code append} advance it, as do {@code msync}
 *       (the mmap'd range, made file-absolute through the mapping's file offset, since paged
 *       {@code MemoryPMARImpl} msyncs a page-relative length at a non-zero page offset) and
 *       {@code sync_file_range} (already file-absolute). {@code syncfs} advances it to the current file length,
 *       because it also captures dirty mmap stores the facade cannot see after {@code munmap}. It keeps a
 *       per-file flush from treating a file's fallocated zero tail as durable: a column fallocated to 16 MiB
 *       with a few KB written has a {@code writtenDataEnd} of those few KB.</li>
 *   <li>{@code syncedDataEnd[F]}: the written-data end that has reached the device cache
 *       ({@code <= writtenDataEnd}). {@code sync_file_range}/{@code msync} advance it to the synced range end,
 *       capped at {@code writtenDataEnd}; a device flush ({@code fsync}/{@code fdatasync}, which writes back all
 *       dirty page-cache data) advances it to {@code writtenDataEnd}.</li>
 *   <li>{@code journaledDataEnd[F]}: the {@code syncedDataEnd} captured at the last journal commit covering F.
 *       Content beyond it sits in unwritten extents and is not durable: after a crash those blocks do not
 *       belong to the file and read back as zero.</li>
 * </ul>
 * {@link #doFlush()} sets {@code durableContent[F]} to {@code deviceCacheContent[F]} truncated to
 * {@code journaledDataEnd[F]}.
 *
 * <h3>The shared journal ({@link #modelSharedJournal}, default true)</h3>
 * On default ext4 (jbd2) and xfs the journal is one filesystem-wide log: an {@code fsync} of inode A forces a
 * journal commit that also persists every other inode's pending extent conversions (jbd2 "operates on blocks, so
 * when it commits a transaction, this transaction includes all changed blocks"). With
 * {@code modelSharedJournal=true}, a journal commit on any file advances {@code journaledDataEnd[G]} to
 * {@code syncedDataEnd[G]} for every tracked G. That is what lets the batched flush's single {@code _cv}
 * {@code fdatasync} make the other columns' within-page data durable.
 *
 * <p><b>fast_commit.</b> ext4 {@code fast_commit} (opt-in) journals at the inode level (LWN: the fast-commit
 * journal "contains changes at the file level"); in its soft-consistency mode only the fsync'd inode is
 * guaranteed, so {@code fsync(A)} does not reliably journal B's pending extent conversions. Set
 * {@code modelSharedJournal=false} to model that; new-allocation content then needs the file's own journal
 * commit (see {@code CrashModelSelfCheckTest.test7_perInodeJournalingNewAllocationLost} and
 * {@code BatchedFlushSharedJournalDependencyTest}).
 *
 * <p><b>syncfs does not depend on the journal policy.</b> {@link #syncfs(long)} models {@code syncfs(2)}: it
 * writes back all dirty data of the filesystem, performs one journal commit covering every inode's pending extent
 * conversions, then one device flush. Unlike {@code fdatasync}, it journals every tracked file even when
 * {@code modelSharedJournal} is false, so one {@code syncfs(anyColumnFd)} makes every drained column's data and
 * extent conversions durable (see {@code CrashModelSelfCheckTest.test9_syncfsJournalsEveryoneUnderPerInodeJournaling}).
 *
 * <p>{@code readCurrent(path)} reads the file's current bytes with a plain {@code read()}. MAP_SHARED mmap writes
 * and {@code read()} share the page cache, so a {@code read()} sees bytes a test stored into a mapping before any
 * msync. msync moves page-cache bytes toward the device; {@code read()} lets the model snapshot the page cache at
 * a sync point.
 *
 * <h3>The process-kill model (opt-in, {@link #armKillAt})</h3>
 * Everything above models a power loss. A process kill ({@code kill -9}, the OOM killer, a JVM crash) keeps the
 * page cache, so every byte the process wrote before it died is still there, flushed or not. The two models
 * disagree on a byte rewritten in place after its last flush: a power loss brings back the flushed value, a kill
 * keeps the new one. {@link #armKillAt} places the kill just before the n-th kill point (a {@code close},
 * {@code munmap}, {@code truncate}, {@code rename}, {@code removeQuiet} or {@code rmdir}), and
 * {@link #takeKillImage} places it at the moment of the call. Either way the bytes of every file and the
 * directory tree under the root become the kill image; the engine runs on, and the next {@link #crash} restores
 * that image instead of the last flushed content. Without an armed kill the facade models a power loss only.
 *
 * <h3>Modelling limitations</h3>
 * <ul>
 *   <li>{@code write}/{@code append} are intercepted only to advance {@code writtenDataEnd}; they do not clear
 *       {@code pteFlushed}, so a write after an msync does not re-mark the mapping un-flushed. The batched flush
 *       never writes between its msync and its {@code sync_file_range}, and the self-tests exercise the
 *       un-msync'd path on fresh files only. mmap stores (the tests' {@code Unsafe.setMemory}) are not
 *       {@code write} calls, so msync normally declares a mapping's written end. syncfs snapshots the full current
 *       image, because it writes dirty pages back even after their mapping is gone. A whole-file
 *       {@code fsync}/{@code fdatasync} also counts non-zero bytes past the declared end as written: it writes
 *       back every dirty page of its file, msync'd or not.</li>
 *   <li>The device-cache and durable snapshots are full-file byte copies taken at sync points. Files are small in
 *       these tests, so this is cheap.</li>
 *   <li>{@code syncedDataEnd} is a single end (the max synced range end), not a set of written extents. The
 *       workloads append contiguously from offset 0, so one end is exact; a sparse-write workload would need a
 *       per-extent set.</li>
 * </ul>
 */
public class CrashFaultFilesFacade extends TestFilesFacadeImpl {
    // fds are tracked so fsync can map a fd back to its file; only fsync advances durableSize.
    private final Map<Long, String> fdToPath = new HashMap<>();
    // fd -> path for descriptors closed by forceClose(), so a later use is attributable.
    private final Map<Long, String> forceClosedFds = new HashMap<>();
    // Non-cached opens (openRWNoCache/openRONoCache): tracked separately so the sweep driver can reclaim any
    // left open by an fsync-interrupted operation. No-cache DIRECTORY fds are also mapped in fdToPath: their
    // fsync persists namespace entries even though ordinary no-cache file fds remain outside the content model.
    private final Set<Long> noCacheOpenFds = new LinkedHashSet<>();
    private final Map<String, Long> durableSize = new HashMap<>();
    private final Map<String, List<long[]>> tornTails = new HashMap<>();
    private final java.util.List<String> durabilityOpLog = new java.util.ArrayList<>();
    private final java.util.List<String> syncOrder = new java.util.ArrayList<>();

    // Live mmap mappings: base address -> path, so msync(addr,...) can resolve the file. mremap re-keys; munmap
    // removes. The mapping's file offset is tracked too: paged memory (MemoryPMARImpl) maps one page at a time at a
    // non-zero file offset and msyncs a page-relative length, so the file-absolute written end of msync(addr,len) is
    // mapFileOffset(addr)+len.
    private final Map<Long, long[]> mmapAddrRange = new HashMap<>(); // addr -> {endAddrExclusive, fileOffset}
    private final Map<Long, String> mmapAddrToPath = new HashMap<>();
    // Every file the content model is tracking (insertion-ordered so doFlush() is deterministic).
    private final Set<String> trackedFiles = new LinkedHashSet<>();
    private final Map<String, Boolean> pteFlushed = new HashMap<>();
    private final Map<String, byte[]> deviceCacheContent = new HashMap<>();
    private final Map<String, byte[]> durableContent = new HashMap<>();
    // Data at the device versus journaled metadata (see the class javadoc).
    // writtenDataEnd[F]: high-water of bytes actually written (not posix_fallocate'd zeros), advanced by
    //   write()/append() and by msync (the range the app declared dirty), so a device flush of a sparsely written,
    //   largely fallocated file does not treat the zero tail as durable.
    // syncedDataEnd[F]: the written-data end that has reached the device cache (<= writtenDataEnd).
    // journaledDataEnd[F]: syncedDataEnd[F] captured at the last journal commit covering F. doFlush() truncates
    //   durableContent[F] to this end; data beyond it lives in unjournaled extents and is lost.
    private final Map<String, Long> writtenDataEnd = new HashMap<>();
    private final Map<String, Long> syncedDataEnd = new HashMap<>();
    private final Map<String, Long> journaledDataEnd = new HashMap<>();

    // Directory entries have their own durability boundary. A parent-directory fsync snapshots only that
    // directory's immediate namespace; syncfs snapshots every baseline root. File bytes are kept here only while
    // an unsynced unlink or rename hides a durable name, and a directory fsync or syncfs releases them.
    private final Set<String> namespaceRoots = new LinkedHashSet<>();
    private final Set<String> durableDirectories = new HashSet<>();
    private final Set<String> durableFileEntries = new HashSet<>();
    private final Map<String, byte[]> namespaceFileImages = new HashMap<>();
    private final Map<String, byte[]> pendingRenameTargetImages = new HashMap<>();

    /**
     * Whether a journal commit on any file persists every tracked file's pending extent conversions (true models
     * the single filesystem-wide journal of default ext4/jbd2 and xfs). Set false to model ext4 fast_commit's
     * per-inode journaling, under which fsync(A) does not journal B's metadata.
     */
    public boolean modelSharedJournal = true;

    private final Set<String> killImageDirectories = new HashSet<>();
    private final Map<String, byte[]> killImageFiles = new HashMap<>();
    private int durabilityOps = 0;
    private int crashAtOp = -1; // -1 = disarmed
    private int killAtPoint = -1; // -1 = disarmed
    private boolean killImageTaken;
    private int killPoints = 0;
    private String killRoot;
    private int syncfsCount = 0; // number of syncfs(2) calls observed (filesystem-wide flushes)
    private String dbRootPrefix; // set via setDbRoot(); scopes cached-fd reclamation to per-table files

    /**
     * Ordered list of file paths as they were fsync'd/fsyncAndClose'd (for sync-order assertions).
     */
    public java.util.List<String> getSyncOrder() {
        return syncOrder;
    }

    /**
     * Number of filesystem-wide syncfs(2) calls observed so far.
     */
    public int syncfsCount() {
        return syncfsCount;
    }

    @Override
    public long openAppend(LPSZ name) {
        long fd = super.openAppend(name);
        if (fd > -1) {
            fdToPath.put(fd, toAbsPath(name));
            forceClosedFds.remove(fd); // fd numbers recycle; a fresh open clears the marker
        }
        return fd;
    }

    @Override
    public long openCleanRW(LPSZ name, long size) {
        long fd = super.openCleanRW(name, size);
        if (fd > -1) {
            fdToPath.put(fd, toAbsPath(name));
            forceClosedFds.remove(fd); // fd numbers recycle; a fresh open clears the marker
        }
        return fd;
    }

    @Override
    public long openRW(LPSZ name, int opts) {
        long fd = super.openRW(name, opts);
        if (fd > -1) {
            fdToPath.put(fd, toAbsPath(name));
            forceClosedFds.remove(fd); // fd numbers recycle; a fresh open clears the marker
        }
        return fd;
    }

    @Override
    public long openRO(LPSZ name) {
        long fd = super.openRO(name);
        if (fd > -1) {
            fdToPath.put(fd, toAbsPath(name));
            forceClosedFds.remove(fd); // fd numbers recycle; a fresh open clears the marker
        }
        return fd;
    }

    @Override
    public long openRONoCache(LPSZ name) {
        long fd = super.openRONoCache(name);
        if (fd > -1) {
            noCacheOpenFds.add(fd);
            fdToPath.put(fd, toAbsPath(name));
            forceClosedFds.remove(fd); // fd numbers recycle; a fresh open clears the marker
        }
        return fd;
    }

    @Override
    public long openRWNoCache(LPSZ name, int opts) {
        long fd = super.openRWNoCache(name, opts);
        if (fd > -1) {
            noCacheOpenFds.add(fd);
            fdToPath.put(fd, toAbsPath(name));
            forceClosedFds.remove(fd); // fd numbers recycle; a fresh open clears the marker
        }
        return fd;
    }

    @Override
    public boolean close(long fd) {
        countKillPoint();
        fdToPath.remove(fd);
        noCacheOpenFds.remove(fd);
        return super.close(fd);
    }

    @Override
    public long write(long fd, long address, long len, long offset) {
        long n = super.write(fd, address, len, offset);
        String p = fdToPath.get(fd);
        if (p != null && n > 0) {
            // A real write() produces WRITTEN extents up to offset+n (not fallocate'd zero). Track that end
            // so a later device flush journals real data, not the fallocate-inflated file length.
            track(p);
            advanceWrittenDataEnd(p, offset + n);
        }
        return n;
    }

    @Override
    public long append(long fd, long buf, long len) {
        final long before = fdToPath.containsKey(fd) ? super.length(fd) : -1L;
        long n = super.append(fd, buf, len);
        String p = fdToPath.get(fd);
        if (p != null && n > 0 && before >= 0L) {
            track(p);
            advanceWrittenDataEnd(p, before + n); // appended real data -> written end grows by n
        }
        return n;
    }

    @Override
    public int copy(LPSZ from, LPSZ to) {
        final int rc = super.copy(from, to);
        if (rc == 0) {
            // copy() (creat(O_TRUNC) + sendfile) writes real bytes for the whole source extent into the destination.
            // The sendfile path bypasses write()/msync tracking, so set the destination's written-data end to the
            // resulting size; otherwise a later fsync of the copy would leave writtenDataEnd at 0 and crash() would
            // roll it to 0.
            final String dst = toAbsPath(to);
            shrinkTo(dst, 0); // creat() truncated a destination that already existed
            track(dst);
            // After the copy the real file length IS the written extent (creat truncated, sendfile filled).
            advanceWrittenDataEnd(dst, length(to));
        }
        return rc;
    }

    /**
     * The fd form of {@link #copy(LPSZ, LPSZ)}. An in-place content replacement ({@code TableUtils.replaceFileContent}:
     * open read-write, truncate, transfer) moves its bytes through this sendfile-family call, which bypasses
     * write()/msync tracking. Untracked, {@code writtenDataEnd} stays 0 and {@link #crash} rolls the destination
     * back to length 0.
     */
    @Override
    public long copyData(long srcFd, long destFd, long offsetSrc, long length) {
        final long n = super.copyData(srcFd, destFd, offsetSrc, length);
        trackTransferredData(destFd, n);
        return n;
    }

    @Override
    public long copyData(long srcFd, long destFd, long offsetSrc, long destOffset, long length) {
        final long n = super.copyData(srcFd, destFd, offsetSrc, destOffset, length);
        trackTransferredData(destFd, n);
        return n;
    }

    @Override
    public long mmap(long fd, long len, long offset, int flags, int memoryTag) {
        long addr = super.mmap(fd, len, offset, flags, memoryTag);
        recordMmap(fd, addr, len, offset);
        return addr;
    }

    @Override
    public long mmapNoCache(long fd, long len, long offset, int flags, int memoryTag) {
        long addr = super.mmapNoCache(fd, len, offset, flags, memoryTag);
        recordMmap(fd, addr, len, offset);
        return addr;
    }

    @Override
    public long mremap(long fd, long addr, long previousSize, long newSize, long offset, int mode, int memoryTag) {
        long newAddr = super.mremap(fd, addr, previousSize, newSize, offset, mode, memoryTag);
        remapMoved(addr, newAddr, newSize, offset, fd);
        return newAddr;
    }

    @Override
    public long mremapNoCache(long fd, long addr, long previousSize, long newSize, long offset, int mode, int memoryTag) {
        long newAddr = super.mremapNoCache(fd, addr, previousSize, newSize, offset, mode, memoryTag);
        remapMoved(addr, newAddr, newSize, offset, fd);
        return newAddr;
    }

    @Override
    public void munmap(long address, long size, int memoryTag) {
        countKillPoint();
        // Drop the addr->path mapping but keep the content model state keyed by path: a file's durable content outlives
        // its mapping (unmap, then crash), as in the size model.
        mmapAddrRange.remove(address);
        mmapAddrToPath.remove(address);
        super.munmap(address, size, memoryTag);
    }

    @Override
    public void barrierFsync(long fd) {
        // Modelled as fdatasync. That is exact off Darwin, where barrierFsync is fdatasync. On Darwin, F_BARRIERFSYNC
        // only orders writes and the data may still be in the drive cache, so this model is optimistic there and cannot
        // catch a loss real macOS hardware allows. The power-loss harness (dm-flakey) is Linux-only, so a passing macOS
        // run is no proof that the barrier is safe.
        fdatasync(fd);
    }

    @Override
    public void fsyncDurable(long fd) {
        // Off Darwin this is fsync; on Darwin F_FULLFSYNC is stronger, so modelling it as fsync never over-reports
        // durability.
        fsync(fd);
    }

    @Override
    public void fdatasync(long fd) {
        assertNotForceClosed(fd, "fdatasync");
        super.fdatasync(fd);
        String p = fdToPath.get(fd);
        if (p != null) {
            syncOrder.add(p);
            // fdatasync is a journal commit, data-at-device and a device flush. Snapshot this file's current bytes into
            // its device cache, push all written page-cache data to the device (up to writtenDataEnd, not the
            // fallocated length), commit F's metadata to the journal (all files' if modelSharedJournal), then flush,
            // which promotes the journaled extent to durable.
            track(p);
            byte[] now = readCurrent(p);
            deviceCacheContent.put(p, now);
            advanceWrittenDataEnd(p, nonZeroEnd(now));
            advanceSyncedDataEnd(p, writtenDataEnd.getOrDefault(p, 0L));
            journalCommit(p);
            doFlush();
        }
        recordDurable(fd);
        bumpDurabilityOp("fdatasync", p);
    }

    @Override
    public void syncfs(long fd) {
        super.syncfs(fd);
        syncfsCount++;
        for (String root : namespaceRoots) {
            snapshotNamespaceTree(Paths.get(root));
        }
        String p = fdToPath.get(fd);
        if (p != null) {
            syncOrder.add(p);
        }
        // syncfs(2) is a whole-filesystem operation: it writes back all dirty data, performs one journal commit
        // covering every inode's pending extent conversions (regardless of modelSharedJournal), then one device flush.
        // Snapshot every tracked file's complete current image. Unlike a per-file sync, syncfs does not cap durability
        // at writtenDataEnd: an adaptive writer can unmap a lazily written partition without msync, and real syncfs
        // still writes those dirty pages back. A fallocated zero tail in the file length is harmless (_txn owns the row
        // count) and matches a sync that also persists the inode size.
        for (String f : trackedFiles) {
            final byte[] current = readCurrent(f);
            deviceCacheContent.put(f, current);
            advanceWrittenDataEnd(f, current.length);
            advanceSyncedDataEnd(f, current.length);
            journaledDataEnd.put(f, (long) current.length);
        }
        doFlush();
        recordDurable(fd);
        bumpDurabilityOp("syncfs", fdToPath.get(fd));
    }

    @Override
    public void fsync(long fd) {
        assertNotForceClosed(fd, "fsync");
        super.fsync(fd);
        String p = fdToPath.get(fd);
        final boolean directory = p != null && Files.isDirectory(Paths.get(p));
        if (directory) {
            snapshotDirectory(Paths.get(p));
        }
        if (p != null) {
            syncOrder.add(p);
            if (!directory) {
                // fsync: journal commit, data-at-device and device flush, as fdatasync in this model.
                track(p);
                byte[] now = readCurrent(p);
                deviceCacheContent.put(p, now);
                advanceWrittenDataEnd(p, nonZeroEnd(now));
                advanceSyncedDataEnd(p, writtenDataEnd.getOrDefault(p, 0L));
                journalCommit(p);
                doFlush();
            }
        }
        if (!directory) {
            recordDurable(fd);
        }
        bumpDurabilityOp(directory ? "fsync-dir" : "fsync", p);
    }

    @Override
    public void fsyncAndClose(long fd) {
        final String p = fdToPath.get(fd);
        final boolean directory = p != null && Files.isDirectory(Paths.get(p));
        final long size = p != null && !directory ? super.length(fd) : -1L;
        final byte[] snapshot = p != null && !directory ? readCurrent(p) : null; // read BEFORE close drops the fd
        super.fsyncAndClose(fd); // performs fsync + close without routing through close()
        fdToPath.remove(fd);
        noCacheOpenFds.remove(fd);
        if (p != null) {
            syncOrder.add(p);
            if (directory) {
                snapshotDirectory(Paths.get(p));
            } else {
                if (size >= 0L) {
                    durableSize.put(p, size);
                }
                // fsync + close: journal commit, data-at-device and device flush. writtenDataEnd is keyed by path, so
                // it survives the close above.
                track(p);
                deviceCacheContent.put(p, snapshot);
                advanceWrittenDataEnd(p, nonZeroEnd(snapshot));
                advanceSyncedDataEnd(p, writtenDataEnd.getOrDefault(p, 0L));
                journalCommit(p);
                doFlush();
            }
        }
        bumpDurabilityOp(directory ? "fsyncAndClose-dir" : "fsyncAndClose", p);
    }

    @Override
    public boolean removeQuiet(LPSZ name) {
        countKillPoint();
        final String removedPath = toAbsPath(name);
        final byte[] restoreImage = durableFileEntries.contains(removedPath) ? durableImage(removedPath) : null;
        final boolean removed = super.removeQuiet(name);
        if (removed) {
            evictDurability(removedPath, false);
            if (restoreImage != null) {
                namespaceFileImages.put(removedPath, restoreImage);
            }
        }
        return removed;
    }

    @Override
    public boolean rmdir(io.questdb.std.str.Path name, boolean haltOnError) {
        countKillPoint();
        final String removedRoot = toAbsPath(name.$());
        preserveDurableSubtreeImages(removedRoot);
        final boolean removed = super.rmdir(name, haltOnError);
        if (removed) {
            // FilesFacadeImpl's recursive rmdir bypasses removeQuiet for the children, so evict the whole subtree here.
            // Otherwise long crash sweeps keep full byte[] snapshots for every dropped table directory, and syncfs
            // scans and copies them until the JVM runs out of memory.
            evictDurability(removedRoot, true);
        } else {
            // Best-effort recursive removal may delete some children before another child fails. Reconcile
            // those successfully removed paths even though the root still exists, but retain snapshots for
            // files that remain on disk.
            evictMissingDurability(removedRoot);
        }
        return removed;
    }

    @Override
    public int hardLink(LPSZ src, LPSZ hardLink) {
        final String sourcePath = toAbsPath(src);
        final String targetPath = toAbsPath(hardLink);
        final int result = super.hardLink(src, hardLink);
        if (result == io.questdb.std.Files.FILES_RENAME_OK) {
            // Both names refer to the same inode. Seed the target's durability state from the source, so crash() never
            // treats the new name as an untracked zero-length file and truncates the shared inode under the source too.
            duplicateMapValue(durableContent, sourcePath, targetPath);
            duplicateMapValue(deviceCacheContent, sourcePath, targetPath);
            duplicateMapValue(durableSize, sourcePath, targetPath);
            duplicateMapValue(writtenDataEnd, sourcePath, targetPath);
            duplicateMapValue(syncedDataEnd, sourcePath, targetPath);
            duplicateMapValue(journaledDataEnd, sourcePath, targetPath);
            duplicateMapValue(pteFlushed, sourcePath, targetPath);
            trackedFiles.add(targetPath);
        }
        return result;
    }

    @Override
    public int rename(LPSZ from, LPSZ to) {
        countKillPoint();
        final String fromPath = toAbsPath(from);
        final String toPath = toAbsPath(to);
        final byte[] restoreImage = durableFileEntries.contains(fromPath) ? durableImage(fromPath) : null;
        final byte[] overwrittenImage = durableFileEntries.contains(toPath) ? durableImage(toPath) : null;
        preserveDurableSubtreeImages(fromPath);
        // A rename that overwrites keeps the destination's previous durable image until the parent directory is
        // fsynced; a crash before that restores both original names.
        preserveDurableSubtreeImages(toPath);
        final int res = super.rename(from, to);
        if (res == io.questdb.std.Files.FILES_RENAME_OK) {
            // POSIX rename carries the inode's durable data to the new name, so re-key every tracked entry from the old
            // path (or old directory prefix) to the new one; otherwise crash() treats a synced, renamed file as never
            // synced and truncates it to 0.
            rekeyDurability(fromPath, toPath);
            // Re-key live mappings too: a mapping opened before the rename would otherwise keep advancing
            // writtenDataEnd/syncedDataEnd on the old name, and crash() would truncate the renamed file to zero.
            // WalWriter renames a column file with its column memory still open.
            for (Map.Entry<Long, String> e : mmapAddrToPath.entrySet()) {
                final String remapped = remapKey(e.getValue(), fromPath, toPath);
                if (remapped != null) {
                    e.setValue(remapped);
                }
            }
            // The same for fd-keyed accounting: fdToPath is populated at open, so an fd held across a rename would keep
            // attributing its write()/fsync()/fdatasync() to the old name.
            for (Map.Entry<Long, String> e : fdToPath.entrySet()) {
                final String remapped = remapKey(e.getValue(), fromPath, toPath);
                if (remapped != null) {
                    e.setValue(remapped);
                }
            }
            duplicateDurableNamespaceDescendants(fromPath, toPath);
            if (restoreImage != null) {
                pendingRenameTargetImages.put(toPath, restoreImage);
            }
            if (overwrittenImage != null) {
                // The destination name itself was already durable. Until its parent directory is fsynced,
                // crash rollback must restore that prior inode rather than the renamed source bytes.
                durableContent.put(toPath, overwrittenImage);
                durableSize.put(toPath, (long) overwrittenImage.length);
            }
            if (restoreImage != null) {
                namespaceFileImages.put(fromPath, restoreImage);
            }
        }
        return res;
    }

    private void preserveDurableSubtreeImages(String root) {
        final String prefix = root + File.separator;
        for (String path : new ArrayList<>(durableFileEntries)) {
            if (path.equals(root) || path.startsWith(prefix)) {
                namespaceFileImages.putIfAbsent(path, durableImage(path));
            }
        }
    }

    private static <T> void duplicateMapValue(Map<String, T> map, String sourcePath, String targetPath) {
        final T value = map.get(sourcePath);
        if (value != null) {
            map.put(targetPath, value);
        }
    }

    private void duplicateDurableNamespaceDescendants(String fromPath, String toPath) {
        final String prefix = fromPath + File.separator;
        for (String path : new ArrayList<>(durableDirectories)) {
            if (path.startsWith(prefix)) {
                durableDirectories.add(toPath + path.substring(fromPath.length()));
            }
        }
        for (String path : new ArrayList<>(durableFileEntries)) {
            if (path.startsWith(prefix)) {
                final String destination = toPath + path.substring(fromPath.length());
                durableFileEntries.add(destination);
                final byte[] image = namespaceFileImages.get(path);
                if (image != null) {
                    namespaceFileImages.put(destination, image);
                }
            }
        }
    }

    private void rekeyDurability(String fromPath, String toPath) {
        rekeyMap(durableContent, fromPath, toPath);
        rekeyMap(deviceCacheContent, fromPath, toPath);
        rekeyMap(durableSize, fromPath, toPath);
        rekeyMap(writtenDataEnd, fromPath, toPath);
        rekeyMap(syncedDataEnd, fromPath, toPath);
        rekeyMap(journaledDataEnd, fromPath, toPath);
        rekeyMap(pteFlushed, fromPath, toPath);
        rekeyMap(tornTails, fromPath, toPath);
        // trackedFiles is insertion-ordered (doFlush() determinism); rebuild preserving relative order.
        final List<String> rebuilt = new ArrayList<>(trackedFiles.size());
        boolean changed = false;
        for (String k : new ArrayList<>(trackedFiles)) {
            final String nk = remapKey(k, fromPath, toPath);
            rebuilt.add(nk != null ? nk : k);
            changed |= nk != null;
        }
        if (changed) {
            trackedFiles.clear();
            trackedFiles.addAll(rebuilt);
        }
    }

    private void evictDurability(String path, boolean includeDescendants) {
        evictMap(durableContent, path, includeDescendants);
        evictMap(deviceCacheContent, path, includeDescendants);
        evictMap(durableSize, path, includeDescendants);
        evictMap(writtenDataEnd, path, includeDescendants);
        evictMap(syncedDataEnd, path, includeDescendants);
        evictMap(journaledDataEnd, path, includeDescendants);
        evictMap(pteFlushed, path, includeDescendants);
        evictMap(tornTails, path, includeDescendants);
        trackedFiles.removeIf(key -> matchesRemovedPath(key, path, includeDescendants));
    }

    private void evictMissingDurability(String root) {
        final String prefix = root + File.separator;
        for (String key : new ArrayList<>(trackedFiles)) {
            if ((key.equals(root) || key.startsWith(prefix)) && !Files.exists(Paths.get(key))) {
                evictDurability(key, false);
            }
        }
    }

    private static <V> void evictMap(Map<String, V> map, String path, boolean includeDescendants) {
        map.keySet().removeIf(key -> matchesRemovedPath(key, path, includeDescendants));
    }

    private static boolean matchesRemovedPath(String key, String path, boolean includeDescendants) {
        return key.equals(path) || includeDescendants && key.startsWith(path + File.separator);
    }

    private static <V> void rekeyMap(Map<String, V> map, String fromPath, String toPath) {
        if (map.isEmpty()) {
            return;
        }
        for (String k : new ArrayList<>(map.keySet())) {
            final String nk = remapKey(k, fromPath, toPath);
            if (nk != null) {
                map.put(nk, map.remove(k));
            }
        }
    }

    private static String remapKey(String key, String fromPath, String toPath) {
        if (key.equals(fromPath)) {
            return toPath;
        }
        final String prefix = fromPath + File.separator;
        if (key.startsWith(prefix)) {
            return toPath + File.separator + key.substring(prefix.length());
        }
        return null;
    }

    @Override
    public void msync(long addr, long len, boolean async) {
        super.msync(addr, len, async);
        String p = pathForAddr(addr);
        if (p != null) {
            track(p);
            // msync pushes this mapping's dirty pages to the page cache, where sync_file_range can see them, and
            // declares the msync'd range written. Paged memory (MemoryPMARImpl) msyncs a page-relative len at a
            // non-zero file offset, so the file-absolute written end is mapFileOffset(addr)+len.
            final long fileEnd = mapFileOffset(addr) + len;
            pteFlushed.put(p, Boolean.TRUE);
            advanceWrittenDataEnd(p, fileEnd);
            if (!async) {
                // MS_SYNC is a range fsync: journal commit, data-at-device and device flush. Write back the current
                // bytes of the msync'd range only (dirty pages elsewhere in the file stay behind), advance the
                // at-device end to this synced end (capped at the written end), commit F's metadata to the journal (all
                // files' if modelSharedJournal), then flush, which promotes the journaled extent of every tracked file
                // to durable.
                deviceCacheContent.put(p, withRangeWrittenBack(deviceCacheContent.get(p), readCurrent(p), fileEnd - len, fileEnd));
                advanceSyncedDataEnd(p, Math.min(fileEnd, writtenDataEnd.getOrDefault(p, 0L)));
                journalCommit(p);
                doFlush();
            }
            // MS_ASYNC only dirties the page cache toward writeback. It is neither data-at-device nor a journal commit,
            // so neither syncedDataEnd nor journaledDataEnd advances.
        }
        bumpDurabilityOp(async ? "msync-async" : "msync", p);
    }

    @Override
    public int syncFileRange(long fd, long offset, long nbytes, int flags) {
        int rc = super.syncFileRange(fd, offset, nbytes, flags);
        String p = fdToPath.get(fd);
        if (p != null) {
            track(p);
            if (Boolean.TRUE.equals(pteFlushed.get(p))) {
                // Data-at-device only. sync_file_range is fd-relative, so offset+nbytes is already file-absolute (the
                // product passes pageStart+inPageLen), and the product drains only what it wrote, so advance both the
                // written and the at-device end to offset+nbytes. There is no device flush and no journal commit:
                // sync_file_range "does not write out the file's metadata" and does not sync out the block allocator,
                // so journaledDataEnd stays. The drained bytes become durable only once a later journal commit (this
                // file's own, or a shared-journal commit) journals the extent conversion.
                deviceCacheContent.put(p, withRangeWrittenBack(deviceCacheContent.get(p), readCurrent(p), offset, offset + nbytes));
                advanceWrittenDataEnd(p, offset + nbytes);
                advanceSyncedDataEnd(p, offset + nbytes);
            }
            // Not pteFlushed: no-op. sync_file_range cannot see mmap-dirty pages the kernel does not yet track as dirty
            // in the page cache (never msync'd or written with write()).
        }
        return rc;
    }

    @Override
    public boolean allocate(long fd, long size) {
        final long lengthBefore = trackedLength(fd);
        final boolean ok = super.allocate(fd, size);
        shrinkAfterAllocate(fd, lengthBefore);
        return ok;
    }

    @Override
    public boolean allocateRange(long fd, long offset, long size) {
        final long lengthBefore = trackedLength(fd);
        final boolean ok = super.allocateRange(fd, offset, size);
        shrinkAfterAllocate(fd, lengthBefore);
        return ok;
    }

    @Override
    public boolean truncate(long fd, long size) {
        countKillPoint();
        final boolean ok = super.truncate(fd, size);
        final String p = fdToPath.get(fd);
        if (ok && p != null) {
            shrinkTo(p, size);
        }
        return ok;
    }

    /**
     * Clears all tracked fd->path and durable-size/content state, for reuse across crash/retry cycles.
     */
    public void reset() {
        fdToPath.clear();
        forceClosedFds.clear();
        noCacheOpenFds.clear();
        durableSize.clear();
        tornTails.clear();
        syncOrder.clear();
        durabilityOpLog.clear();
        mmapAddrRange.clear();
        mmapAddrToPath.clear();
        trackedFiles.clear();
        pteFlushed.clear();
        deviceCacheContent.clear();
        durableContent.clear();
        writtenDataEnd.clear();
        syncedDataEnd.clear();
        journaledDataEnd.clear();
        namespaceRoots.clear();
        durableDirectories.clear();
        durableFileEntries.clear();
        namespaceFileImages.clear();
        pendingRenameTargetImages.clear();
        durabilityOps = 0;
        crashAtOp = -1;
        killImageDirectories.clear();
        killImageFiles.clear();
        killAtPoint = -1;
        killImageTaken = false;
        killPoints = 0;
        killRoot = null;
        // modelSharedJournal is a configured policy, not per-cycle state, so it is not reset here.
    }

    /**
     * Records the current sizes and content of all files under dbRoot as durable (prior committed state).
     */
    public void markDurableBaseline(CharSequence dbRoot) {
        final Path root = Paths.get(dbRoot.toString()).toAbsolutePath();
        namespaceRoots.add(root.toString());
        snapshotNamespaceTree(root);
        walk(dbRoot, p -> {
            String key = p.toAbsolutePath().toString();
            try {
                durableSize.put(key, java.nio.file.Files.size(p));
            } catch (java.io.IOException e) {
                throw new java.io.UncheckedIOException(e);
            }
            // Whatever is on disk now is durable, is the device-cache state, and is fully journaled, so both watermarks
            // sit at the full length.
            byte[] now = readCurrent(key);
            track(key);
            deviceCacheContent.put(key, now);
            durableContent.put(key, now);
            writtenDataEnd.put(key, (long) now.length);
            syncedDataEnd.put(key, (long) now.length);
            journaledDataEnd.put(key, (long) now.length);
        });
    }

    /**
     * Marks one file's current content as fully durable (written to the device and journaled), leaving every other
     * file's durability state untouched. The per-file counterpart of {@link #markDurableBaseline}.
     * <p>
     * This models the kernel writing back a file's dirty mmap pages before the crash, which can happen to any
     * mapped file at any time without an {@code msync} from QuestDB. Since {@code _txn} and {@code _cv} are lazy
     * under adaptive mode, it is the way to construct a commit pointer that survived ahead of the column data it
     * exposes.
     *
     * @param absPath absolute path of the file to promote to durable; a no-op if it does not exist
     */
    public void markFileDurable(String absPath) {
        final java.nio.file.Path p = Paths.get(absPath).toAbsolutePath();
        if (!java.nio.file.Files.exists(p)) {
            return;
        }
        final String key = p.toString();
        try {
            durableSize.put(key, java.nio.file.Files.size(p));
        } catch (java.io.IOException e) {
            throw new java.io.UncheckedIOException(e);
        }
        final byte[] now = readCurrent(key);
        track(key);
        deviceCacheContent.put(key, now);
        durableContent.put(key, now);
        writtenDataEnd.put(key, (long) now.length);
        syncedDataEnd.put(key, (long) now.length);
        journaledDataEnd.put(key, (long) now.length);
    }

    /**
     * Zeroes [offset, offset+len) of the given file when crash() runs (deterministic torn-write injection).
     */
    public void tornTail(LPSZ name, long offset, long len) {
        tornTails.computeIfAbsent(toAbsPath(name), k -> new ArrayList<>())
                .add(new long[]{offset, len});
    }

    /**
     * Rolls every file under {@code dbRoot} back to its durable state, then applies any torn-tail ranges.
     * <p>
     * A file with a {@code durableContent} snapshot is restored to those exact bytes, which also fixes the durable
     * length. A file the content model never tracked is truncated to its last durable size.
     * <p>
     * When a kill image was taken ({@link #armKillAt}, {@link #takeKillImage}), the tree under the kill's root is
     * restored to that image instead, and the image is consumed.
     */
    public void crash(CharSequence dbRoot) {
        if (killImageTaken) {
            restoreKillImage();
            return;
        }
        restoreNamespace(Paths.get(dbRoot.toString()).toAbsolutePath());
        walk(dbRoot, p -> {
            String key = p.toAbsolutePath().toString();
            byte[] durable = durableContent.get(key);
            try (FileChannel ch = FileChannel.open(p, StandardOpenOption.WRITE)) {
                if (durable != null) {
                    // Content rollback: overwrite the prefix with durable bytes, then set the durable length. A
                    // whole-file sync makes the file size durable even where the content model cannot tell written zero
                    // bytes from unwritten space (bytes stored by native code past the last non-zero one): the file
                    // keeps that size, unless it was truncated below it since, and what lies past the durable bytes
                    // reads as zero.
                    final long length = Math.max(durable.length, Math.min(durableSize.getOrDefault(key, 0L), ch.size()));
                    if (durable.length > 0) {
                        ch.write(ByteBuffer.wrap(durable), 0);
                    }
                    if (length > durable.length) {
                        ch.write(ByteBuffer.allocate((int) (length - durable.length)), durable.length);
                    }
                    if (ch.size() > length) {
                        ch.truncate(length);
                    }
                } else {
                    // No content snapshot for this file: size-only rollback.
                    Long durableSz = durableSize.get(key);
                    if (durableSz != null) {
                        if (ch.size() > durableSz) {
                            ch.truncate(durableSz);
                        }
                    } else {
                        final byte[] namespaceImage = namespaceFileImages.get(key);
                        if (namespaceImage != null) {
                            ch.write(ByteBuffer.wrap(namespaceImage), 0);
                            ch.truncate(namespaceImage.length);
                        } else {
                            ch.truncate(0L);
                        }
                    }
                }
                List<long[]> ranges = tornTails.get(key);
                if (ranges != null) {
                    for (long[] r : ranges) {
                        if (r[1] < 0 || r[1] > Integer.MAX_VALUE)
                            throw new IllegalArgumentException("tornTail len out of range: " + r[1]);
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
        pendingRenameTargetImages.clear();
    }

    /**
     * Arms a simulated crash: throws CrashSimulationError on the n-th durability op (fsync/fsyncAndClose/msync).
     */
    public void armCrashAt(int n) {
        this.crashAtOp = n;
    }

    /**
     * True while a crash armed by {@link #armCrashAt(int)} is pending, that is, the target durability op has not
     * been reached. The arm is one-shot: reaching the op disarms it. After a commit phase, {@code !isCrashArmed()}
     * shows the armed op fired, even when the engine swallowed the {@link CrashSimulationError} as best-effort
     * (a durable-epoch advance after an already committed convert) instead of propagating it or suspending the
     * table. The sweep uses it to tell an absorbed crash from op-count drift (an arm never consumed).
     */
    public boolean isCrashArmed() {
        return crashAtOp > 0;
    }

    /**
     * Arm a process kill just before the {@code n}-th kill point (counted by {@link #killPointCount()}, so
     * pass {@code killPointCount() + k} for the k-th point from now). At that point the bytes of every file
     * and the directory tree under {@code dbRoot} become the kill image, and the next {@link #crash}
     * restores it. The calling code is not interrupted: a killed process does nothing after the kill, and
     * restoring the image discards whatever the engine does after it. {@code n < 1} disarms.
     */
    public void armKillAt(int n, CharSequence dbRoot) {
        killAtPoint = n;
        killRoot = dbRoot != null ? Paths.get(dbRoot.toString()).toAbsolutePath().toString() : null;
        killImageTaken = false;
        killImageFiles.clear();
        killImageDirectories.clear();
    }

    /**
     * True once an armed kill point was reached, or {@link #takeKillImage} ran, and the image has not yet
     * been restored by {@link #crash}.
     */
    public boolean isKillImageTaken() {
        return killImageTaken;
    }

    /**
     * Number of kill points ({@code close}, {@code munmap}, {@code truncate}, {@code rename},
     * {@code removeQuiet}, {@code rmdir}) observed so far.
     */
    public int killPointCount() {
        return killPoints;
    }

    /**
     * Kill the process now: the bytes of every file and the directory tree under {@code dbRoot} become the
     * image the next {@link #crash} restores. Disarms a pending {@link #armKillAt}.
     */
    public void takeKillImage(CharSequence dbRoot) {
        killAtPoint = -1;
        killRoot = Paths.get(dbRoot.toString()).toAbsolutePath().toString();
        captureKillImage();
    }

    /**
     * Number of durability ops (fsync/fsyncAndClose/msync) observed so far.
     */
    public int durabilityOpCount() {
        return durabilityOps;
    }

    /**
     * One line per counted durability op, {@code <n> <kind> <db-root-relative path>}, in the order observed, so a
     * drifting pinned op count names the op that appeared or disappeared. Cleared by {@link #reset()}; index 0 is
     * the first op of the process, not of any one commit phase.
     */
    public java.util.List<String> durabilityOpLog() {
        return durabilityOpLog;
    }

    /**
     * Snapshot of the non-cached fds ({@code openRWNoCache}/{@code openRONoCache}) currently open. They bypass the
     * path-keyed durability tracking, so they are recorded here, not in {@code fdToPath}. The sweep driver uses this
     * to model process-death fd reclamation: a simulated crash on a live JVM leaves a non-cached fd open where a
     * real power loss would have the OS reclaim it. The driver closes the per-cycle delta.
     */
    public java.util.List<Long> noCacheOpenFdsSnapshot() {
        // Files.openRW/openRO are non-cached at the Files layer even through the ordinary facade methods, so the
        // snapshot includes those too, but only files under a table directory. Engine-root files (the tables.d name
        // registry, the table-id generator, config) sit in the db root, are held open by long-lived engine singletons
        // across the sweep, and are not released by releaseEngineHandles(). Force-closing one would leave its owner
        // holding a dead fd that its next reopen double-closes ("fd <n> is already closed!"). The same scoping
        // applies in {@link #reclaimCachedFdsUnder(String)}.
        final LinkedHashSet<Long> snapshot = new LinkedHashSet<>(noCacheOpenFds);
        if (dbRootPrefix != null) {
            for (Map.Entry<Long, String> e : fdToPath.entrySet()) {
                final String p = e.getValue();
                if (p != null
                        && p.startsWith(dbRootPrefix)
                        && p.indexOf(File.separatorChar, dbRootPrefix.length()) >= 0) {
                    snapshot.add(e.getKey());
                }
            }
        }
        return new ArrayList<>(snapshot);
    }

    /**
     * Tells the facade which directory is the engine's db root, so {@link #noCacheOpenFdsSnapshot()} can tell a
     * per-table fd (reclaimable) from an engine-root fd owned by a long-lived singleton. Until it is set, the
     * snapshot reports only the explicitly non-cached fds: under-reclaiming at worst leaves a tracked fd behind,
     * whereas over-reclaiming corrupts a live owner's state.
     */
    public void setDbRoot(CharSequence dbRoot) {
        this.dbRootPrefix = Paths.get(dbRoot.toString()).toAbsolutePath() + File.separator;
    }

    /**
     * Force-closes every open cached fd ({@code openRW}/{@code openRO}/{@code openCleanRW}, tracked in
     * {@code fdToPath}) whose path contains {@code dirMarker}, and returns the count closed. Companion to
     * {@link #noCacheOpenFdsSnapshot()}: a simulated crash can also leave a cached fd open when it unwinds an
     * operation mid-flight (a rebase clone's partition copy, an interrupted writer close), and on a live JVM that fd
     * lingers where process death would reclaim it. Engine-root files (the {@code tables.d} name registry, config,
     * id generator) never contain a table-dir marker, so this touches only fds under a dropped table directory.
     * Routes through {@link #forceClose(long)}.
     */
    public int reclaimCachedFdsUnder(String dirMarker) {
        int closed = 0;
        for (Long fd : new ArrayList<>(fdToPath.keySet())) {
            final String p = fdToPath.get(fd);
            if (p != null && p.contains(dirMarker)) {
                forceClose(fd);
                closed++;
            }
        }
        return closed;
    }

    /**
     * Reclaims a non-cached fd, modelling process-death fd closure. A non-cached fd can leave the fd cache via a
     * path this facade does not intercept ({@code detach} and others), so the underlying close may report the fd as
     * already gone, which means the OS descriptor is already reclaimed. Tracking is dropped regardless.
     */
    public void forceClose(long fd) {
        final String path = fdToPath.get(fd);
        try {
            close(fd);
        } catch (IllegalStateException | AssertionError alreadyGone) {
            noCacheOpenFds.remove(fd);
            fdToPath.remove(fd);
        }
        forceClosedFds.put(fd, path == null ? "<unknown path>" : path);
    }

    /**
     * Fails at the point of use if a force-closed fd is used again.
     * <p>
     * {@link #forceClose(long)} models the OS reclaiming fds a simulated crash stranded. If it closes an fd a live
     * object still owns, the owner's next operation runs on a dead descriptor, and otherwise the only symptom is an
     * {@code "Invalid fd=..., not found in cache"} assertion deep in {@code FdCache}, from whatever unrelated code
     * touches it next, possibly thousands of crash points later.
     */
    private void assertNotForceClosed(long fd, String op) {
        final String path = forceClosedFds.get(fd);
        if (path != null) {
            throw new AssertionError("USE AFTER FORCE-CLOSE: " + op + " on fd=" + fd + " (" + path
                    + "), which reclaimLingeringNonCacheFds/forceClose already closed. The fd was still "
                    + "owned by a live object when the reclaim ran - release engine handles BEFORE "
                    + "reclaiming, and release table writers LAST (the WAL/sequencer releases can re-open "
                    + "one). See AbstractAdaptiveCrashTest#releaseEngineHandles.");
        }
    }

    /**
     * True if the content model has observed an msync on this file (so sync_file_range can see its pages).
     */
    public boolean isPteFlushed(CharSequence absPath) {
        return Boolean.TRUE.equals(pteFlushed.get(Paths.get(absPath.toString()).toAbsolutePath().toString()));
    }

    /**
     * The bytes the content model currently considers durable for this file (null if untracked).
     */
    public byte[] durableContentOf(CharSequence absPath) {
        return durableContent.get(Paths.get(absPath.toString()).toAbsolutePath().toString());
    }

    /**
     * The path of an open file descriptor (null if not open through this facade).
     */
    public String pathOf(long fd) {
        return fdToPath.get(fd);
    }

    /**
     * Number of files currently retained by the durability content model.
     */
    public int trackedFileCount() {
        return trackedFiles.size();
    }

    /**
     * Number of retained files at or below the given path.
     */
    public int trackedFileCountUnder(CharSequence root) {
        final String path = Paths.get(root.toString()).toAbsolutePath().toString();
        final String prefix = path + File.separator;
        int count = 0;
        for (String key : trackedFiles) {
            if (key.equals(path) || key.startsWith(prefix)) {
                count++;
            }
        }
        return count;
    }

    /**
     * The written-data end (data-at-device) the model has recorded for this file (0 if untracked).
     */
    public long syncedDataEndOf(CharSequence absPath) {
        return syncedDataEnd.getOrDefault(Paths.get(absPath.toString()).toAbsolutePath().toString(), 0L);
    }

    /**
     * The journaled written-data end (metadata-journaled) the model has recorded (0 if untracked).
     */
    public long journaledDataEndOf(CharSequence absPath) {
        return journaledDataEnd.getOrDefault(Paths.get(absPath.toString()).toAbsolutePath().toString(), 0L);
    }

    /**
     * db-root-relative path for the op log, so lines stay readable and stable across temp dirs.
     */
    private String relativePath(String path) {
        if (path == null) {
            return "<unattributed>";
        }
        if (dbRootPrefix != null && path.startsWith(dbRootPrefix)) {
            return path.substring(dbRootPrefix.length());
        }
        return path;
    }

    private void captureKillImage() {
        killImageFiles.clear();
        killImageDirectories.clear();
        final Path root = Paths.get(killRoot);
        if (Files.exists(root)) {
            try (Stream<Path> stream = Files.walk(root)) {
                stream.forEach(path -> {
                    final String key = path.toAbsolutePath().toString();
                    if (Files.isDirectory(path)) {
                        killImageDirectories.add(key);
                    } else if (Files.isRegularFile(path)) {
                        killImageFiles.put(key, readCurrent(key));
                    }
                });
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        }
        killImageTaken = true;
    }

    private void countKillPoint() {
        killPoints++;
        if (killAtPoint > 0 && killPoints >= killAtPoint) {
            killAtPoint = -1;
            captureKillImage();
        }
    }

    private void restoreKillImage() {
        final Path root = Paths.get(killRoot);
        if (Files.exists(root)) {
            try (Stream<Path> stream = Files.walk(root)) {
                stream.sorted(Comparator.reverseOrder()).forEach(path -> {
                    final String key = path.toAbsolutePath().toString();
                    final boolean inImage = Files.isDirectory(path) ? killImageDirectories.contains(key) : killImageFiles.containsKey(key);
                    if (!inImage && !path.equals(root)) {
                        try {
                            Files.delete(path);
                        } catch (IOException e) {
                            throw new UncheckedIOException(e);
                        }
                    }
                });
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        }
        try {
            for (String dir : killImageDirectories) {
                Files.createDirectories(Paths.get(dir));
            }
            for (Map.Entry<String, byte[]> e : killImageFiles.entrySet()) {
                try (FileChannel ch = FileChannel.open(Paths.get(e.getKey()), StandardOpenOption.CREATE, StandardOpenOption.WRITE)) {
                    ch.write(ByteBuffer.wrap(e.getValue()), 0);
                    ch.truncate(e.getValue().length);
                    ch.force(true);
                }
            }
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        killImageFiles.clear();
        killImageDirectories.clear();
        killImageTaken = false;
        pendingRenameTargetImages.clear();
    }

    private void bumpDurabilityOp(String kind, String path) {
        durabilityOps++;
        durabilityOpLog.add(durabilityOps + " " + kind + " " + relativePath(path));
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

    /**
     * One device flush promotes every tracked file's device-cache content to durable, but each file's durable
     * content is truncated to its {@code journaledDataEnd}: only bytes whose extent metadata was journaled belong to
     * the file after a crash. Device-cache content beyond {@code journaledDataEnd} sits in unwritten extents and is
     * dropped.
     */
    private void doFlush() {
        for (String f : trackedFiles) {
            byte[] dc = deviceCacheContent.get(f);
            if (dc != null) {
                durableContent.put(f, truncateTo(dc, journaledDataEnd.getOrDefault(f, 0L)));
            }
        }
    }

    /**
     * Advances F's written-data end (real bytes written) to the max end seen; it never moves backward.
     */
    private void advanceWrittenDataEnd(String path, long end) {
        long cur = writtenDataEnd.getOrDefault(path, 0L);
        if (end > cur) {
            writtenDataEnd.put(path, end);
        }
    }

    /**
     * Advances F's at-device (synced) end to the max value seen; it never moves backward.
     */
    private void advanceSyncedDataEnd(String path, long end) {
        long cur = syncedDataEnd.getOrDefault(path, 0L);
        if (end > cur) {
            syncedDataEnd.put(path, end);
        }
    }

    /**
     * F shrank to {@code size}: its blocks past {@code size} are freed. Drop them from the device image and pull
     * every watermark back to the new end, so space F grows into again starts unwritten, as re-allocated space
     * does. Otherwise a later ranged sync (which merges its range into the previous device image) or any flush
     * would bring the freed bytes back. The durable image is left alone: until a journal commit covers the
     * shrink, a crash restores F as it was.
     */
    private void shrinkTo(String path, long size) {
        if (!trackedFiles.contains(path)) {
            return;
        }
        final byte[] device = deviceCacheContent.get(path);
        if (device != null && device.length > size) {
            deviceCacheContent.put(path, truncateTo(device, size));
        }
        clampTo(writtenDataEnd, path, size);
        clampTo(syncedDataEnd, path, size);
        clampTo(journaledDataEnd, path, size);
    }

    /**
     * allocate() never shrinks a file on Linux (posix_fallocate), but on macOS and Windows it ends in a truncate
     * to the requested size, which shrinks a longer file.
     */
    private void shrinkAfterAllocate(long fd, long lengthBefore) {
        if (lengthBefore > 0) {
            final long lengthAfter = super.length(fd);
            if (lengthAfter >= 0 && lengthAfter < lengthBefore) {
                shrinkTo(fdToPath.get(fd), lengthAfter);
            }
        }
    }

    /**
     * The current length of a file the content model tracks, or -1 for any other fd.
     */
    private long trackedLength(long fd) {
        final String p = fdToPath.get(fd);
        return p != null && trackedFiles.contains(p) ? super.length(fd) : -1L;
    }

    /**
     * A journal commit covering F: F's pending extent conversions become journaled
     * ({@code journaledDataEnd[F]=syncedDataEnd[F]}). With {@link #modelSharedJournal} (the single filesystem-wide
     * journal of default ext4/jbd2 and xfs), the same commit also journals every other tracked file's pending
     * conversions; under fast_commit (per-inode) only F advances.
     */
    private void journalCommit(String path) {
        journaledDataEnd.put(path, syncedDataEnd.getOrDefault(path, 0L));
        if (modelSharedJournal) {
            for (String g : trackedFiles) {
                journaledDataEnd.put(g, syncedDataEnd.getOrDefault(g, 0L));
            }
        }
    }

    private static void clampTo(Map<String, Long> watermark, String path, long end) {
        final Long current = watermark.get(path);
        if (current != null && current > end) {
            watermark.put(path, end);
        }
    }

    /**
     * Returns a prefix of {@code src} of length {@code min(end, src.length)} (the journaled extent).
     */
    private static byte[] truncateTo(byte[] src, long end) {
        int n = (int) Math.max(0L, Math.min(end, src.length));
        if (n == src.length) {
            return src;
        }
        byte[] out = new byte[n];
        System.arraycopy(src, 0, out, 0, n);
        return out;
    }

    /**
     * The device image after a ranged write-back ({@code msync(MS_SYNC)} of a mapping, {@code sync_file_range}):
     * the bytes of {@code [lo, hi)} come from the current page-cache image, every other byte stays as the
     * device held it, and a byte the device never held reads as zero (an unwritten extent). The image keeps
     * the current file length. A whole-file snapshot here would treat pages written through an earlier,
     * already unmapped mapping as written back, and hide a missing flush of every page released unsynced.
     */
    private static byte[] withRangeWrittenBack(byte[] device, byte[] current, long lo, long hi) {
        final byte[] out = new byte[current.length];
        if (device != null) {
            System.arraycopy(device, 0, out, 0, Math.min(device.length, current.length));
        }
        final int end = (int) Math.min(hi, current.length);
        final int start = (int) Math.max(0L, Math.min(lo, end));
        System.arraycopy(current, start, out, start, end - start);
        return out;
    }

    /**
     * End of the last non-zero byte of {@code image}. A whole-file sync writes back every dirty page of the
     * file, including stores made through a mapping that was never msync'd, which this facade cannot see. In
     * fallocate'd space such a store is the only way a non-zero byte gets past the declared written end, so
     * the sync counts it as written; zero bytes stay indistinguishable from unwritten space.
     */
    private static long nonZeroEnd(byte[] image) {
        for (int i = image.length - 1; i >= 0; i--) {
            if (image[i] != 0) {
                return i + 1;
            }
        }
        return 0;
    }

    /**
     * Begins tracking a file in the content model; seeds the device-cache and durable snapshots from disk on first
     * sight.
     * <p>
     * A file first seen mid-run holds only {@code posix_fallocate}'d (unwritten-extent) bytes, which read back as
     * zero and whose written-data extent is not journaled, so both {@code syncedDataEnd} and
     * {@code journaledDataEnd} start at 0, not at the possibly fallocated file length. Bytes a test then writes
     * through the mapping become durable only once a journal commit advances {@code journaledDataEnd} over them.
     * Prior committed state comes in through {@link #markDurableBaseline}, which seeds the watermarks to the full
     * length.
     */
    private void track(String path) {
        if (trackedFiles.add(path)) {
            byte[] initial = readCurrent(path);
            deviceCacheContent.put(path, initial); // device cache holds the fallocate'd (zero) bytes
            // Durable content honours journaledDataEnd (=0 here): nothing written-and-journaled yet, so an
            // immediate crash rolls a fresh file back to empty (its unwritten extents read as zero anyway).
            durableContent.put(path, truncateTo(initial, 0L));
            pteFlushed.put(path, Boolean.FALSE);
            writtenDataEnd.put(path, 0L);
            syncedDataEnd.put(path, 0L);
            journaledDataEnd.put(path, 0L);
        }
    }

    /**
     * Records the destination of an fd-to-fd transfer, mirroring {@link #copy(LPSZ, LPSZ)}: after the
     * transfer the real file length is the written extent, because the caller truncated the destination before
     * filling it.
     */
    private void trackTransferredData(long destFd, long transferred) {
        final String p = fdToPath.get(destFd);
        if (p != null && transferred > 0) {
            track(p);
            advanceWrittenDataEnd(p, super.length(destFd));
        }
    }

    private void recordMmap(long fd, long addr, long len, long fileOffset) {
        if (addr == 0 || addr == -1L) {
            return;
        }
        String p = fdToPath.get(fd);
        if (p != null) {
            mmapAddrRange.put(addr, new long[]{addr + len, fileOffset});
            mmapAddrToPath.put(addr, p);
            track(p);
        }
    }

    private void remapMoved(long oldAddr, long newAddr, long newSize, long fileOffset, long fd) {
        if (newAddr == 0 || newAddr == -1L) {
            return;
        }
        String p = mmapAddrToPath.remove(oldAddr);
        mmapAddrRange.remove(oldAddr);
        if (p == null) {
            p = fdToPath.get(fd);
        }
        if (p != null) {
            mmapAddrRange.put(newAddr, new long[]{newAddr + newSize, fileOffset});
            mmapAddrToPath.put(newAddr, p);
            track(p);
        }
    }

    /**
     * File offset of the mapping that {@code addr} falls in (exact base match first, else range containment).
     */
    private long mapFileOffset(long addr) {
        long[] exact = mmapAddrRange.get(addr);
        if (exact != null) {
            return exact[1];
        }
        for (Map.Entry<Long, long[]> e : mmapAddrRange.entrySet()) {
            long base = e.getKey();
            long end = e.getValue()[0];
            if (addr >= base && addr < end) {
                // The msync addr may be past the mapping base (a sub-range); the byte at addr maps to file offset
                // fileOffset + (addr - base).
                return e.getValue()[1] + (addr - base);
            }
        }
        return 0L;
    }

    /**
     * Resolve the file an msync address belongs to: exact base match first, else range containment.
     */
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
     * Reads the file's current page-cache bytes with a plain read(). MAP_SHARED mmap writes share the page cache
     * with read(), so this observes bytes written into a mapping before any msync.
     * <p>
     * A directory fd has no byte content to snapshot and reads as empty: {@code TableWriter.addColumn}
     * {@code fsyncAndClose}s the partition directory after creating column files (fsync the parent to persist the
     * new dentry), and a plain {@code read()} of a directory throws {@code IOException: Is a directory}, which would
     * surface as an {@code UncheckedIOException} and suspend the table (the engine's catch around that fsync expects
     * {@code CairoException}). {@link #walk}, used by {@link #crash} and {@link #markDurableBaseline}, keeps regular
     * files only, so a directory's entry in the content-model maps is never used for rollback.
     */
    private static byte[] readCurrent(String path) {
        Path pth = Paths.get(path);
        if (!Files.exists(pth) || Files.isDirectory(pth)) {
            return new byte[0];
        }
        try {
            return Files.readAllBytes(pth);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private byte[] durableImage(String path) {
        final byte[] durable = durableContent.get(path);
        if (durable != null) {
            return durable;
        }
        final byte[] pending = namespaceFileImages.get(path);
        return pending != null ? pending : readCurrent(path);
    }

    private void restoreNamespace(Path root) {
        if (!namespaceRoots.contains(root.toString()) || !Files.exists(root)) {
            return;
        }
        try (Stream<Path> stream = Files.walk(root)) {
            stream.sorted(Comparator.reverseOrder()).forEach(path -> {
                if (path.equals(root)) {
                    return;
                }
                if (!isNamespaceEntryDurable(root, path)) {
                    try {
                        Files.deleteIfExists(path);
                    } catch (IOException e) {
                        throw new UncheckedIOException(e);
                    }
                }
            });
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        durableDirectories.stream()
                .filter(path -> isNamespaceEntryDurable(root, Paths.get(path)))
                .sorted(Comparator.comparingInt(String::length))
                .forEach(path -> {
                    try {
                        Files.createDirectories(Paths.get(path));
                    } catch (IOException e) {
                        throw new UncheckedIOException(e);
                    }
                });
        durableFileEntries.stream()
                .filter(path -> isNamespaceEntryDurable(root, Paths.get(path)))
                .forEach(path -> {
                    final Path file = Paths.get(path);
                    if (!Files.exists(file)) {
                        try {
                            Files.createDirectories(file.getParent());
                            final byte[] image = namespaceFileImages.get(path);
                            final byte[] durable = durableContent.get(path);
                            Files.write(file, image != null ? image : durable != null ? durable : new byte[0]);
                        } catch (IOException e) {
                            throw new UncheckedIOException(e);
                        }
                    }
                });
    }

    private boolean isNamespaceEntryDurable(Path root, Path path) {
        final String key = path.toAbsolutePath().toString();
        if (!durableDirectories.contains(key) && !durableFileEntries.contains(key)) {
            return false;
        }
        // fsyncing a child directory persists its immediate entries, but not the child's own dentry in
        // its parent. An entry survives only when every directory component back to the namespace root is
        // durable; otherwise losing one ancestor necessarily loses the entire subtree.
        for (Path parent = path.toAbsolutePath().getParent(); parent != null && !parent.equals(root.toAbsolutePath()); parent = parent.getParent()) {
            if (!durableDirectories.contains(parent.toString())) {
                return false;
            }
        }
        return path.toAbsolutePath().startsWith(root.toAbsolutePath());
    }

    private void snapshotDirectory(Path directory) {
        final String prefix = directory.toAbsolutePath().toString() + File.separator;
        durableDirectories.removeIf(path -> path.startsWith(prefix) && path.indexOf(File.separator, prefix.length()) < 0);
        durableFileEntries.removeIf(path -> path.startsWith(prefix) && path.indexOf(File.separator, prefix.length()) < 0);
        namespaceFileImages.keySet().removeIf(path -> path.startsWith(prefix) && path.indexOf(File.separator, prefix.length()) < 0);
        if (!Files.isDirectory(directory)) {
            return;
        }
        try (Stream<Path> entries = Files.list(directory)) {
            entries.forEach(path -> {
                final String key = path.toAbsolutePath().toString();
                if (Files.isDirectory(path)) {
                    durableDirectories.add(key);
                } else if (Files.isRegularFile(path)) {
                    durableFileEntries.add(key);
                    final byte[] renamedImage = pendingRenameTargetImages.remove(key);
                    if (renamedImage != null) {
                        durableContent.put(key, renamedImage);
                        durableSize.put(key, (long) renamedImage.length);
                        namespaceFileImages.remove(key);
                    }
                }
            });
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private void snapshotNamespaceTree(Path root) {
        final String rootKey = root.toAbsolutePath().toString();
        final String prefix = rootKey + File.separator;
        durableDirectories.removeIf(path -> path.equals(rootKey) || path.startsWith(prefix));
        durableFileEntries.removeIf(path -> path.startsWith(prefix));
        namespaceFileImages.keySet().removeIf(path -> path.startsWith(prefix));
        pendingRenameTargetImages.keySet().removeIf(path -> path.startsWith(prefix));
        if (!Files.exists(root)) {
            return;
        }
        try (Stream<Path> stream = Files.walk(root)) {
            stream.forEach(path -> {
                final String key = path.toAbsolutePath().toString();
                if (Files.isDirectory(path)) {
                    durableDirectories.add(key);
                } else if (Files.isRegularFile(path)) {
                    durableFileEntries.add(key);
                }
            });
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    /**
     * Convert LPSZ (null-terminated native bytes) to a canonical absolute path String.
     */
    private static String toAbsPath(LPSZ name) {
        String path = Utf8String.newInstance(name).toString();
        final int nul = path.indexOf('\0');
        if (nul > -1) {
            path = path.substring(0, nul);
        }
        return Paths.get(path).toAbsolutePath().toString();
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
