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
 * <h3>Size model (original)</h3>
 * {@code durableSize[path]} is advanced by fsync/fdatasync/fsyncAndClose; {@link #crash} truncates each
 * file to its last-durable size. msync alone does not advance durable size (it flushes data pages but not
 * the inode size after a grow). Queued torn-tail ranges are zeroed after the rollback.
 *
 * <h3>Content + device-flush-batching model (added for the SYNC-mode flush-batching safety net)</h3>
 * Alongside the size model, a content model tracks, per file:
 * <ul>
 *   <li>{@code pteFlushed} — whether the file's mmap-dirty pages have been pushed to the page cache via
 *       msync since it became mappable (so {@code sync_file_range} can see them);</li>
 *   <li>{@code deviceCacheContent} — the bytes written back to the device's (volatile) cache, pending a
 *       device flush;</li>
 *   <li>{@code durableContent} — the bytes that SURVIVE a crash (promoted from the device cache by a flush,
 *       then TRUNCATED to the journaled extent — see the metadata-journaling dimension below).</li>
 * </ul>
 *
 * <h3>The metadata-journaling dimension (data-at-device vs metadata-journaled)</h3>
 * Durability of a written byte requires THREE independent things, and the original content model conflated
 * the last two by treating any device flush as making all {@code deviceCacheContent} durable. They are NOT
 * the same:
 * <ol>
 *   <li><b>data-at-device</b> — the bytes physically reaching the device's (volatile) write cache. This is
 *       what {@code sync_file_range(WRITE | WAIT_AFTER)} and {@code msync} push out
 *       ({@code sync_file_range} man page: it does NOT flush disk write caches and "does not provide any
 *       data integrity on systems with volatile disk write caches", but WAIT_AFTER does land the data IN
 *       the device cache);</li>
 *   <li><b>metadata-journaled</b> — the filesystem extent-tree / {@code i_size} update that makes those
 *       blocks actually BELONG to the file. This is provided ONLY by a JOURNAL COMMIT
 *       ({@code fsync}/{@code fdatasync}, and {@code msync(MS_SYNC)} since it is a range-fsync).
 *       {@code sync_file_range} does NOT journal it — man page: "When writing into preallocated space, many
 *       filesystems also require calls into the block allocator, which this system call does not sync out to
 *       disk" and "None of these operations writes out the file's metadata." QuestDB writes into
 *       {@code posix_fallocate}'d (unwritten-extent) space constantly, so a WITHIN-PAGE append converts an
 *       unwritten extent and needs this journal commit even when {@code i_size} does not change;</li>
 *   <li><b>device-flush</b> — the volatile write cache being flushed to non-volatile media (REQ_PREFLUSH /
 *       REQ_FUA; kernel block/writeback_cache_control). {@code fsync}/{@code fdatasync} ("This includes
 *       writing through or flushing a disk cache if present") and {@code msync(MS_SYNC)} carry one;
 *       {@code sync_file_range} does not.</li>
 * </ol>
 * The model therefore tracks, per file, three additional watermarks:
 * <ul>
 *   <li>{@code writtenDataEnd[F]} — high-water of bytes actually WRITTEN (real data, NOT
 *       {@code posix_fallocate}'d unwritten zero), as a FILE-ABSOLUTE offset. Advanced by
 *       {@code write}/{@code append} (real writes), by {@code msync} (the mmap'd range — translated to
 *       file-absolute via the mapping's file offset, since paged {@code MemoryPMARImpl} msyncs a
 *       page-relative length at a non-zero page offset), and by {@code sync_file_range} (already fd-relative,
 *       i.e. file-absolute). A whole-filesystem {@code syncfs} conservatively advances it to the current file
 *       length because it must also capture dirty mmap stores made invisible to the facade by {@code munmap}.
 *       Otherwise this is the written-data-END proxy that keeps a per-file flush from pretending a file's
 *       fallocate-inflated (zero) tail is durable: a freshly fallocate'd-to-16MiB column with only a few KB
 *       written has {@code writtenDataEnd} of those few KB, not 16 MiB.</li>
 *   <li>{@code syncedDataEnd[F]} — the written-data end that has reached the device cache
 *       ({@code <= writtenDataEnd}). Advanced to the synced RANGE end (capped at {@code writtenDataEnd}) by
 *       {@code sync_file_range}/{@code msync}, and to {@code writtenDataEnd} by a device flush
 *       ({@code fsync}/{@code fdatasync}, which writes back ALL dirty page-cache data).</li>
 *   <li>{@code journaledDataEnd[F]} — the {@code syncedDataEnd} captured at the last JOURNAL COMMIT covering
 *       F. Content at the device beyond this end sits in non-journaled (still-unwritten) extents and is NOT
 *       durable: on crash the extent conversion never happened, so those blocks do not belong to the file
 *       and read back as zero / rolled-back.</li>
 * </ul>
 * {@link #doFlush()} sets {@code durableContent[F]} = {@code deviceCacheContent[F]} TRUNCATED to
 * {@code journaledDataEnd[F]} — durable means data-at-device AND its extent metadata journaled.
 *
 * <h3>The shared-journal assumption ({@link #modelSharedJournal}, default true)</h3>
 * On default ext4 (jbd2) and xfs the journal is a single filesystem-wide log: an {@code fsync} of inode A
 * forces a journal commit that also persists EVERY other inode's pending extent conversions (jbd2 "operates
 * on blocks, so when it commits a transaction, this transaction includes all changed blocks"). With
 * {@code modelSharedJournal=true} (the default, faithful to default ext4/xfs), a journal commit on ANY file
 * advances {@code journaledDataEnd[G]=syncedDataEnd[G]} for EVERY tracked G. This is exactly what lets the
 * batched optimization's single {@code _cv} {@code fdatasync} make the OTHER columns' within-page data
 * durable.
 *
 * <p><b>fast_commit caveat.</b> ext4 {@code fast_commit} (opt-in, newer) journals at the FILE/inode level
 * rather than committing the whole filesystem-wide transaction (LWN: the fast-commit journal "contains
 * changes at the file level"); under its soft-consistency mode only the fsync'd inode is guaranteed. So an
 * {@code fsync(A)} does NOT reliably journal B's pending extent conversions. Set
 * {@code modelSharedJournal=false} to model that per-inode world; under it, new-allocation content needs the
 * file's OWN journal commit (see {@code CrashModelSelfCheckTest} ST7 and
 * {@code BatchedFlushSharedJournalDependencyTest}).
 *
 * <p><b>syncfs is journal-policy-INDEPENDENT.</b> {@link #syncfs(long)} models {@code syncfs(2)}: it writes
 * back ALL dirty data of the whole filesystem and performs ONE journal commit covering EVERY inode's pending
 * extent conversions, then a single device flush. Unlike {@code fdatasync}, this is NOT gated on
 * {@code modelSharedJournal} — syncfs ALWAYS journals every tracked file, even under per-inode
 * ({@code fast_commit}) journaling. This is the salvage property the batched SYNC commit relies on: one
 * {@code syncfs(anyColumnFd)} makes every just-drained column's data AND extent conversions durable in a
 * single flush (see {@code CrashModelSelfCheckTest} ST9).
 *
 * <p>{@code readCurrent(path)} reads the real file's CURRENT bytes via a plain {@code read()}; because
 * MAP_SHARED mmap writes and {@code read()} share the kernel page cache, a {@code read()} sees the bytes a
 * test wrote into an mmap'd region (even before any msync). msync is what moves those page-cache bytes on
 * toward the device; {@code read()} merely lets the model snapshot the current page-cache state at a sync
 * point.
 *
 * <h3>Modeling limitations</h3>
 * <ul>
 *   <li>{@code write}/{@code append} are intercepted ONLY to advance {@code writtenDataEnd} (real bytes
 *       written); they do NOT clear {@code pteFlushed}, so a write after an msync does not re-mark the
 *       mapping un-flushed. This is acceptable for the target optimization, which never writes between its
 *       msync and its {@code sync_file_range}; the self-tests exercise the un-msync'd path on FRESH files
 *       only. mmap STORES (the tests' {@code Unsafe.setMemory}) are not visible as {@code write} calls, so a
 *       mapping's written end is normally declared by msync; syncfs is the exception and snapshots the full
 *       current image because it writes dirty pages back even after their mapping has been removed.</li>
 *   <li>The device-cache/durable snapshots are full-file byte copies taken at sync points. Files are small
 *       in these tests, so this is cheap; if it ever became hot it could be narrowed to a dirty range.</li>
 *   <li>{@code syncedDataEnd} is a written-data-END proxy (the max synced range end), not a precise set of
 *       written extents. The target workload appends contiguously from offset 0, so a single end is exact
 *       for it; a sparse-write workload would need a per-extent set.</li>
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

    // --- content + device-flush-batching model ---
    // Live mmap mappings: base address -> path, so msync(addr,...) can resolve the file. mremap re-keys; munmap removes.
    // The mapping's FILE OFFSET is tracked too: paged memory (MemoryPMARImpl) maps one page at a time at a
    // non-zero file offset and msyncs a PAGE-RELATIVE length, so the file-absolute written end of an
    // msync(addr,len) is mapFileOffset(addr)+len -- without the file offset the model would mistake a
    // page-relative length for a file-absolute one and badly under-count writtenDataEnd.
    private final Map<Long, long[]> mmapAddrRange = new HashMap<>(); // addr -> {endAddrExclusive, fileOffset}
    private final Map<Long, String> mmapAddrToPath = new HashMap<>();
    // Every file the content model is tracking (insertion-ordered so doFlush() is deterministic).
    private final Set<String> trackedFiles = new LinkedHashSet<>();
    private final Map<String, Boolean> pteFlushed = new HashMap<>();
    private final Map<String, byte[]> deviceCacheContent = new HashMap<>();
    private final Map<String, byte[]> durableContent = new HashMap<>();
    // --- metadata-journaling dimension (data-at-device vs metadata-journaled) ---
    // writtenDataEnd[F]: high-water of bytes actually WRITTEN (real data, not posix_fallocate'd zero). It is
    //   the written-data-END proxy: advanced by write()/append() (real writes) and by msync (the mmap'd
    //   range the app declared dirty). This is what distinguishes written content from fallocate-inflated
    //   file length, so a device flush of a sparsely-written-but-largely-fallocate'd file does NOT pretend
    //   the whole (zero) tail is durable.
    // syncedDataEnd[F]: the written-data end that has reached the device cache (<= writtenDataEnd).
    // journaledDataEnd[F]: syncedDataEnd[F] captured at the last journal commit covering F. doFlush()
    //   truncates durableContent[F] to this end -> data beyond it lives in non-journaled extents and is lost.
    private final Map<String, Long> writtenDataEnd = new HashMap<>();
    private final Map<String, Long> syncedDataEnd = new HashMap<>();
    private final Map<String, Long> journaledDataEnd = new HashMap<>();

    // Directory entries have an independent durability boundary. A parent-directory fsync snapshots only
    // that directory's immediate namespace; syncfs snapshots every baseline root. Content durability remains
    // governed by the existing per-file model above. File bytes are retained here only while an unsynced
    // unlink/rename has hidden a durable name; ordinary namespace snapshots therefore add no duplicate
    // full-file images, and a directory fsync/syncfs releases the pending images.
    private final Set<String> namespaceRoots = new LinkedHashSet<>();
    private final Set<String> durableDirectories = new HashSet<>();
    private final Set<String> durableFileEntries = new HashSet<>();
    private final Map<String, byte[]> namespaceFileImages = new HashMap<>();
    private final Map<String, byte[]> pendingRenameTargetImages = new HashMap<>();

    /**
     * Whether a journal commit on ANY file persists EVERY tracked file's pending extent conversions
     * (default true = faithful to default ext4/jbd2 + xfs's single filesystem-wide journal). Set false to
     * model ext4 fast_commit's per-inode journaling, under which fsync(A) does NOT journal B's metadata.
     */
    public boolean modelSharedJournal = true;

    private int durabilityOps = 0;
    private int crashAtOp = -1; // -1 = disarmed
    private int syncfsCount = 0; // number of syncfs(2) calls observed (filesystem-wide flushes)
    private String dbRootPrefix; // set via setDbRoot(); scopes cached-fd reclamation to per-table files

    /**
     * Ordered list of file paths as they were fsync'd/fsyncAndClose'd (for sync-order assertions).
     */
    public java.util.List<String> getSyncOrder() {
        return syncOrder;
    }

    /**
     * Number of filesystem-wide syncfs(2) calls observed so far (for the I1 fs-wide-epoch-flush proof).
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
            // copy() (creat(O_TRUNC) + sendfile) writes REAL bytes for the whole source extent into the
            // destination (replacing it). Track the destination's written-data end as the resulting file
            // size, so a subsequent fsync of the copy journals its full content durable (otherwise the
            // sendfile path bypasses write()/msync tracking, writtenDataEnd stays 0, and crash() would
            // roll the copied file back to length 0). Mirrors how write()/append() advance writtenDataEnd.
            final String dst = toAbsPath(to);
            track(dst);
            // After the copy the real file length IS the written extent (creat truncated, sendfile filled).
            advanceWrittenDataEnd(dst, length(to));
        }
        return rc;
    }

    /**
     * Same blind spot as {@link #copy(LPSZ, LPSZ)}, reached by fd instead of by path. An in-place content
     * replacement ({@code TableUtils.replaceFileContent}: open read-write, truncate, transfer) moves its
     * bytes through this call, which is the same sendfile family and so bypasses write()/msync tracking
     * just as much. Untracked, {@code writtenDataEnd} stays 0 and {@link #crash} rolls the destination —
     * a {@code _txn.epoch}/{@code _cv.epoch} payload, or a live file restored over — back to length 0.
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
        // Drop the addr->path mapping but KEEP the content model state keyed by path: a file's durable
        // content must outlive its mapping (e.g. unmap-then-crash), exactly like the size model.
        mmapAddrRange.remove(address);
        mmapAddrToPath.remove(address);
        super.munmap(address, size, memoryTag);
    }

    @Override
    public void barrierFsync(long fd) {
        // Model it as fdatasync. Exact off Darwin, where barrierFsync IS fdatasync. On Darwin
        // (F_BARRIERFSYNC) the real primitive only ORDERS -- the data may still be in the drive cache --
        // so this model is OPTIMISTIC there and would not catch a loss that real macOS hardware allows.
        // Acceptable because the power-loss harness (dm-flakey) is Linux-only anyway; noted so nobody reads
        // a green macOS run as proof of the barrier's safety.
        fdatasync(fd);
    }

    @Override
    public void fsyncDurable(long fd) {
        // Off Darwin this IS fsync; on Darwin F_FULLFSYNC is strictly stronger, so modelling it as fsync
        // never under-reports durability.
        fsync(fd);
    }

    @Override
    public void fdatasync(long fd) {
        assertNotForceClosed(fd, "fdatasync");
        super.fdatasync(fd);
        String p = fdToPath.get(fd);
        if (p != null) {
            syncOrder.add(p);
            // fdatasync is a JOURNAL COMMIT + data-at-device + DEVICE FLUSH. Snapshot this file's current
            // bytes into its device cache, push ALL written page-cache data to the device (at-device end ->
            // writtenDataEnd; NOT the fallocate-inflated file length), commit F's metadata to the journal
            // (and all files' if modelSharedJournal), then flush -> promotes the journaled extent to durable.
            track(p);
            byte[] now = readCurrent(p);
            deviceCacheContent.put(p, now);
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
        // syncfs(2) is a WHOLE-FILESYSTEM operation: it writes back ALL dirty data of the filesystem AND
        // performs ONE journal commit that journals EVERY inode's pending extent conversions (independent of
        // modelSharedJournal -- syncfs ALWAYS journals everyone, that is the salvage property), then issues a
        // single device flush. Snapshot every tracked file's COMPLETE current image. Unlike per-file sync
        // operations, syncfs must not cap durability at writtenDataEnd: mmap stores are otherwise invisible
        // after an ADAPTIVE writer unmaps a lazily-written partition without calling msync, even though real
        // syncfs still writes those dirty page-cache pages back. The current file length may include a
        // fallocate'd zero tail; retaining that tail is both harmless (_txn owns the logical row count) and
        // faithful to a whole-filesystem sync that also persists the inode size and unwritten extents.
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
                // fsync: JOURNAL COMMIT + data-at-device + DEVICE FLUSH (like fdatasync for this model).
                track(p);
                byte[] now = readCurrent(p);
                deviceCacheContent.put(p, now);
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
                // fsync + close: JOURNAL COMMIT + data-at-device + DEVICE FLUSH. Snapshot+advance+commit+flush.
                // writtenDataEnd is keyed by path so it survives the close above.
                track(p);
                deviceCacheContent.put(p, snapshot);
                advanceSyncedDataEnd(p, writtenDataEnd.getOrDefault(p, 0L));
                journalCommit(p);
                doFlush();
            }
        }
        bumpDurabilityOp(directory ? "fsyncAndClose-dir" : "fsyncAndClose", p);
    }

    @Override
    public boolean removeQuiet(LPSZ name) {
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
        final String removedRoot = toAbsPath(name.$());
        preserveDurableSubtreeImages(removedRoot);
        final boolean removed = super.rmdir(name, haltOnError);
        if (removed) {
            // FilesFacadeImpl's recursive rmdir bypasses removeQuiet for the children, so evict the entire
            // subtree here. Without this, long crash sweeps retain full byte[] snapshots for every dropped
            // physical table token (cf_wal~1, cf_wal~2, ...), making syncfs scan/copy stale tables until OOM.
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
            // Both names refer to the same inode. Seed the target's path-keyed durability state from the
            // source so crash() never treats the new alias as an untracked zero-length file and truncates
            // the shared inode (which would corrupt the still-live source name too).
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
        final String fromPath = toAbsPath(from);
        final String toPath = toAbsPath(to);
        final byte[] restoreImage = durableFileEntries.contains(fromPath) ? durableImage(fromPath) : null;
        final byte[] overwrittenImage = durableFileEntries.contains(toPath) ? durableImage(toPath) : null;
        preserveDurableSubtreeImages(fromPath);
        // rename-overwrite must also retain the destination's previous durable inode/image until the parent
        // directory is fsynced; an unsynced crash restores both original names.
        preserveDurableSubtreeImages(toPath);
        final int res = super.rename(from, to);
        if (res == io.questdb.std.Files.FILES_RENAME_OK) {
            // POSIX rename carries the inode's durable data to the new name, so the path-keyed durability
            // model must follow the file(s): re-key every tracked entry from the old path -- or old-dir
            // prefix, for a directory rename -- to the new one. Without this a synced-then-renamed file is
            // wrongly seen as never-synced and crash() truncates it to 0.
            rekeyDurability(fromPath, toPath);
            // Re-key LIVE MAPPINGS too. mmapAddrToPath is populated at mmap time, so without this a
            // mapping opened before the rename keeps resolving to the OLD name: every later msync through
            // it advances writtenDataEnd/syncedDataEnd on a path that no longer exists, while the renamed
            // file accumulates none. crash() then truncates a file whose bytes ARE on disk back to zero,
            // and WAL apply reports "WAL segment column too short for committed row range [... actual=0]".
            // Renaming a column file with its column memory still open is exactly what WalWriter does.
            for (Map.Entry<Long, String> e : mmapAddrToPath.entrySet()) {
                final String remapped = remapKey(e.getValue(), fromPath, toPath);
                if (remapped != null) {
                    e.setValue(remapped);
                }
            }
            // Same staleness for fd-keyed accounting: fdToPath is populated at OPEN, so an fd held across
            // a rename keeps attributing its write()/fsync()/fdatasync() to the old name. WalWriter renames
            // a WAL column file with its column memory (and fd) still open, so without this every barrier
            // the renamed column pays lands on a path that no longer exists.
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
            // msync pushes this mapping's dirty pages to the page cache -> sync_file_range can now see them.
            // The msync'd range declares those bytes WRITTEN. Paged memory (MemoryPMARImpl) maps one page at
            // a non-zero file offset and msyncs a PAGE-RELATIVE len, so the FILE-ABSOLUTE written end is
            // mapFileOffset(addr)+len -- advance the written-data end to that (regardless of async).
            final long fileEnd = mapFileOffset(addr) + len;
            pteFlushed.put(p, Boolean.TRUE);
            advanceWrittenDataEnd(p, fileEnd);
            if (!async) {
                // MS_SYNC is a range-fsync: JOURNAL COMMIT + data-at-device + DEVICE FLUSH. Snapshot current
                // bytes, advance the at-device end to this file-absolute synced end (capped at the written
                // end), commit F's metadata to the journal (and all files' if modelSharedJournal), then flush
                // -> promotes the journaled extent of EVERY tracked file to durable.
                deviceCacheContent.put(p, readCurrent(p));
                advanceSyncedDataEnd(p, Math.min(fileEnd, writtenDataEnd.getOrDefault(p, 0L)));
                journalCommit(p);
                doFlush();
            }
            // MS_ASYNC: only dirties the page cache toward writeback. NOT data-at-device, NOT a journal
            // commit -> neither syncedDataEnd nor journaledDataEnd advances here.
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
                // DATA-AT-DEVICE only. sync_file_range is fd-relative, so offset+nbytes is ALREADY
                // file-absolute (the product passes pageStart+inPageLen). The drained range is written data
                // (the product only drains what it wrote), so advance BOTH the written and the at-device end
                // to offset+nbytes. NO device flush and -- crucially -- NOT a journal commit: sync_file_range
                // "does not write out the file's metadata" and "does not sync out [the block allocator] to
                // disk", so journaledDataEnd does NOT advance. The within-page bytes it drains are durable
                // ONLY once a later journal commit (this file's own, or a shared-journal commit) journals the
                // extent conversion.
                deviceCacheContent.put(p, readCurrent(p));
                advanceWrittenDataEnd(p, offset + nbytes);
                advanceSyncedDataEnd(p, offset + nbytes);
            }
            // If NOT pteFlushed: NO-OP. sync_file_range cannot see mmap-dirty pages the kernel does not yet
            // track as dirty in the page cache (they were never msync'd / write()-n). This is the real footgun.
        }
        return rc;
    }

    /**
     * Clear all tracked fd→path and durable-size/content state (for reuse across crash/retry cycles).
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
        // NB: modelSharedJournal is a configured policy, NOT per-cycle state -> intentionally NOT reset here.
    }

    /**
     * Record current sizes (and content) of all files under dbRoot as durable ("prior committed, log-journaled").
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
            // Mirror into the content model: whatever is on disk now is durable, is the device-cache state,
            // and is FULLY journaled (prior committed, log-journaled state) -> both watermarks at full length.
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
     * Mark ONE file's current content as fully durable (device-written AND journaled), leaving every other
     * file's durability state untouched. The per-file counterpart of {@link #markDurableBaseline}.
     * <p>
     * This models the KERNEL independently writing back a file's dirty mmap pages before the crash --
     * something that can happen to any mapped file at any time, with no {@code msync} from QuestDB. It is
     * the only way to construct the "commit pointer survived AHEAD of the column data it exposes" state now
     * that {@code _txn}/{@code _cv} are correctly lazy under ADAPTIVE: their per-apply flush used to create
     * that skew on every commit, which made it reachable by accident rather than by intent.
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
     * Zero [offset, offset+len) of the given file when crash() runs (deterministic torn-write injection).
     */
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
        restoreNamespace(Paths.get(dbRoot.toString()).toAbsolutePath());
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
     * Arm a simulated crash: throw CrashSimulationError on the n-th durability op (fsync/fsyncAndClose/msync).
     */
    public void armCrashAt(int n) {
        this.crashAtOp = n;
    }

    /**
     * True while a crash armed by {@link #armCrashAt(int)} is still pending — i.e. the target durability op
     * has NOT yet been reached. The arm is one-shot: reaching it resets the arm (back to disarmed), so after
     * a commit phase {@code !isCrashArmed()} proves the armed op actually fired, EVEN when the resulting
     * {@link CrashSimulationError} was best-effort-swallowed by the engine (a durable-epoch advance after an
     * already-committed convert) rather than propagated or turned into a table suspend. The sweep uses this
     * to distinguish "the crash fired but was safely absorbed" from a genuine op-count drift (arm never
     * consumed).
     */
    public boolean isCrashArmed() {
        return crashAtOp > 0;
    }

    /**
     * Number of durability ops (fsync/fsyncAndClose/msync) observed so far.
     */
    public int durabilityOpCount() {
        return durabilityOps;
    }

    /**
     * One line per counted durability op — {@code <n> <kind> <db-root-relative path>} — in the order they
     * were observed. A pinned op count that drifts is otherwise a bare number with no story: this says WHICH
     * op appeared or vanished, so the reviewer can decide whether the change moved a barrier or removed one.
     * Cleared by {@link #reset()}; index 0 is the first op of the process, not of any one commit phase.
     */
    public java.util.List<String> durabilityOpLog() {
        return durabilityOpLog;
    }

    /**
     * Snapshot of the NON-cached fds ({@code openRWNoCache}/{@code openRONoCache}) currently open. These
     * bypass the path-keyed durability tracking (so they are recorded here, NOT in {@code fdToPath}); the
     * sweep driver uses this to model process-death fd reclamation — a simulated crash on a live JVM cannot
     * actually kill the process, so a NON-cached fd left open by an fsync-interrupted operation lingers
     * where a real power loss would have the OS reclaim it. The driver closes the per-cycle delta.
     */
    public java.util.List<Long> noCacheOpenFdsSnapshot() {
        // Files.openRW/openRO wrappers are currently non-cached at the Files layer even when they arrive
        // through the ordinary facade methods, so process-death simulation must be able to reclaim those
        // too — but ONLY the ones a per-table durability op could have leaked. SCOPE the union to files
        // living UNDER a table directory: engine-root files (the tables.d name registry, the table-id
        // generator, config) sit directly in the db root and are owned by long-lived engine singletons
        // that legitimately hold them open across the whole sweep and are NOT released by
        // releaseEngineHandles(). Force-closing one of those is not process-death modelling — it leaves
        // the live owner holding a dead fd, and the owner's next reopen double-closes it
        // ("fd <n> is already closed!"). This mirrors the scoping invariant that
        // {@link #reclaimCachedFdsUnder(String)} already documents.
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
     * Tell the facade which directory is the engine's db root, so {@link #noCacheOpenFdsSnapshot()} can
     * tell a per-table fd (reclaimable) from an engine-root fd owned by a long-lived singleton (not).
     * Until this is set the snapshot reports only the explicitly non-cached fds — the conservative
     * direction: under-reclaiming can at worst leave a tracked fd behind, whereas over-reclaiming
     * corrupts a live owner's state.
     */
    public void setDbRoot(CharSequence dbRoot) {
        this.dbRootPrefix = Paths.get(dbRoot.toString()).toAbsolutePath() + File.separator;
    }

    /**
     * Force-close every open CACHED fd ({@code openRW}/{@code openRO}/{@code openCleanRW}, tracked in
     * {@code fdToPath}) whose path contains {@code dirMarker}, returning the count closed. Companion to
     * {@link #noCacheOpenFdsSnapshot()}: a simulated crash can also leave a cached fd open when it unwinds an
     * operation (e.g. a rebase clone's partition copy or an interrupted writer close) mid-flight, and on a
     * live JVM that fd lingers where process death would reclaim it. Scoping to a table-dir marker is the
     * safety net a blind fd-delta lacks: engine-root files (the {@code tables.d} name registry, config, id
     * generator) live at the db root and never contain a table-dir marker, so this cannot touch a live
     * registry/config fd — only fds under a (by call time, dropped) table dir. Routes through
     * {@link #forceClose(long)} (proper cached close, robust to an already-gone fd).
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
     * Reclaim a non-cached fd (used by the sweep driver to model process-death fd closure). Robust to a
     * stale bookkeeping entry: a non-cached fd can leave the fd cache via a path this facade does not
     * intercept ({@code detach}, etc.), so the underlying close may report the fd as already gone — that is
     * fine here (it means the OS descriptor is already reclaimed). Tracking is dropped regardless.
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
     * Fail LOUDLY and locally if a force-closed fd is used again.
     * <p>
     * {@link #forceClose(long)} exists to model the OS reclaiming fds a simulated crash stranded. If it
     * ever closes an fd a LIVE object still owns, the owner's next operation runs on a dead descriptor —
     * and the only symptom is an {@code "Invalid fd=..., not found in cache"} assertion raised deep inside
     * {@code FdCache}, from whatever unrelated code touches it next, potentially thousands of crash points
     * later. That is what made the writer-release ordering bug in
     * {@code AbstractAdaptiveCrashTest#releaseEngineHandles} so expensive to attribute. Naming the reclaim
     * at the point of misuse turns that into a one-line diagnosis.
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

    // === content-model test introspection (used by CrashModelSelfCheckTest) ===

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

    // --- content-model helpers ---

    /**
     * ONE device flush promotes EVERY tracked file's device-cache content to durable (the batching
     * semantic), but each file's durable content is TRUNCATED to its {@code journaledDataEnd}: only bytes
     * whose extent metadata was journaled actually belong to the file after a crash. Device-cache content
     * beyond {@code journaledDataEnd} sits in non-journaled (still-unwritten) extents and is dropped.
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
     * Advance F's WRITTEN-data end (real bytes written) to the max end seen; never moves backward.
     */
    private void advanceWrittenDataEnd(String path, long end) {
        long cur = writtenDataEnd.getOrDefault(path, 0L);
        if (end > cur) {
            writtenDataEnd.put(path, end);
        }
    }

    /**
     * Advance F's at-device (synced) end to the max value seen; never moves backward.
     */
    private void advanceSyncedDataEnd(String path, long end) {
        long cur = syncedDataEnd.getOrDefault(path, 0L);
        if (end > cur) {
            syncedDataEnd.put(path, end);
        }
    }

    /**
     * A JOURNAL COMMIT covering F. F's own pending extent conversions become journaled
     * ({@code journaledDataEnd[F]=syncedDataEnd[F]}). If {@link #modelSharedJournal} (default ext4 jbd2 /
     * xfs single filesystem-wide journal), the same commit ALSO journals every OTHER tracked file's pending
     * conversions; under fast_commit (per-inode) it does not, so only F advances.
     */
    private void journalCommit(String path) {
        journaledDataEnd.put(path, syncedDataEnd.getOrDefault(path, 0L));
        if (modelSharedJournal) {
            for (String g : trackedFiles) {
                journaledDataEnd.put(g, syncedDataEnd.getOrDefault(g, 0L));
            }
        }
    }

    /**
     * Return a prefix of {@code src} of length {@code min(end, src.length)} (the journaled extent).
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
     * Begin tracking a file in the content model; seeds device-cache and durable snapshots from disk on
     * first sight.
     * <p>
     * CRUCIAL for the metadata-journaling dimension: a file first seen MID-RUN holds only
     * {@code posix_fallocate}'d (unwritten-extent) bytes, which read back as ZERO and whose written-data
     * extent has NOT been journaled. So both {@code syncedDataEnd} and {@code journaledDataEnd} seed to 0 —
     * NOT the (possibly fallocate-inflated) file length. The bytes a test then writes through the mapping
     * become durable only once a journal commit advances {@code journaledDataEnd} over them. Prior,
     * already-committed state is instead introduced via {@link #markDurableBaseline} (which seeds the
     * watermarks to the full length), exactly as the size model distinguishes baseline from fresh growth.
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
     * transfer the real file length IS the written extent, because the caller truncated the destination
     * before filling it.
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
                // The msync addr may be > the mapping base (a sub-range); the written byte at addr maps to
                // file offset fileOffset + (addr - base).
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
     * Read the file's CURRENT on-disk/page-cache bytes via a plain read(). MAP_SHARED mmap writes share the
     * page cache with read(), so this observes bytes written into a mapping even before any msync.
     * <p>
     * A DIRECTORY fd is treated as having no snapshottable byte content (empty), not an error: {@code
     * TableWriter.addColumn} legitimately {@code fsyncAndClose}'s the partition directory itself after
     * creating new column files (the standard POSIX "fsync the parent to persist the new dentry" idiom,
     * guarded only on Windows) — a plain {@code read()} of a directory throws {@code IOException: Is a
     * directory}, which would otherwise surface as an uncaught {@code UncheckedIOException} and wrongly
     * distress/suspend the table (the engine's own catch around that fsync only expects {@code
     * CairoException}). This is harmless: {@link #walk} (used by {@link #crash} and {@link
     * #markDurableBaseline}) already filters to {@code Files::isRegularFile}, so a directory's entry in the
     * content-model maps is never consulted for rollback.
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
