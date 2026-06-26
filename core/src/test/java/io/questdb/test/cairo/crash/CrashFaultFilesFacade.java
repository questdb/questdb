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
 *       i.e. file-absolute). This is the written-data-END proxy that keeps a flush from pretending a file's
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
 *       mapping's written end is instead declared by the msync that flushes those pages.</li>
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
    private final Map<String, Long> durableSize = new HashMap<>();
    private final Map<String, List<long[]>> tornTails = new HashMap<>();
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

    /**
     * Whether a journal commit on ANY file persists EVERY tracked file's pending extent conversions
     * (default true = faithful to default ext4/jbd2 + xfs's single filesystem-wide journal). Set false to
     * model ext4 fast_commit's per-inode journaling, under which fsync(A) does NOT journal B's metadata.
     */
    public boolean modelSharedJournal = true;

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
    public void fdatasync(long fd) {
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
        bumpDurabilityOp();
    }

    @Override
    public void syncfs(long fd) {
        super.syncfs(fd);
        String p = fdToPath.get(fd);
        if (p != null) {
            syncOrder.add(p);
        }
        // syncfs(2) is a WHOLE-FILESYSTEM operation: it writes back ALL dirty data of the filesystem AND
        // performs ONE journal commit that journals EVERY inode's pending extent conversions (independent of
        // modelSharedJournal -- syncfs ALWAYS journals everyone, that is the salvage property), then issues a
        // single device flush. So for EVERY tracked file F: snapshot its current bytes into the device cache
        // (syncfs writes back all dirty page-cache data even WITHOUT a prior sync_file_range), advance its
        // at-device end to its written-data end, journal F's extent metadata, then one doFlush() promotes
        // every file's journaled extent to durable. Net: all tracked files become fully durable (data +
        // metadata) regardless of which fd was passed or whether the journal is shared.
        for (String f : trackedFiles) {
            deviceCacheContent.put(f, readCurrent(f));
            advanceSyncedDataEnd(f, writtenDataEnd.getOrDefault(f, 0L));
            journaledDataEnd.put(f, syncedDataEnd.getOrDefault(f, 0L));
        }
        doFlush();
        recordDurable(fd);
        bumpDurabilityOp();
    }

    @Override
    public void fsync(long fd) {
        super.fsync(fd);
        String p = fdToPath.get(fd);
        if (p != null) {
            syncOrder.add(p);
            // fsync: JOURNAL COMMIT + data-at-device + DEVICE FLUSH (like fdatasync for this model).
            track(p);
            byte[] now = readCurrent(p);
            deviceCacheContent.put(p, now);
            advanceSyncedDataEnd(p, writtenDataEnd.getOrDefault(p, 0L));
            journalCommit(p);
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
            // fsync + close: JOURNAL COMMIT + data-at-device + DEVICE FLUSH. Snapshot+advance+commit+flush.
            // writtenDataEnd is keyed by path so it survives the close above.
            track(p);
            deviceCacheContent.put(p, snapshot);
            advanceSyncedDataEnd(p, writtenDataEnd.getOrDefault(p, 0L));
            journalCommit(p);
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
        bumpDurabilityOp();
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
        syncedDataEnd.clear();
        journaledDataEnd.clear();
        durabilityOps = 0;
        crashAtOp = -1;
        // NB: modelSharedJournal is a configured policy, NOT per-cycle state -> intentionally NOT reset here.
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

    /** The written-data end (data-at-device) the model has recorded for this file (0 if untracked). */
    public long syncedDataEndOf(CharSequence absPath) {
        return syncedDataEnd.getOrDefault(Paths.get(absPath.toString()).toAbsolutePath().toString(), 0L);
    }

    /** The journaled written-data end (metadata-journaled) the model has recorded (0 if untracked). */
    public long journaledDataEndOf(CharSequence absPath) {
        return journaledDataEnd.getOrDefault(Paths.get(absPath.toString()).toAbsolutePath().toString(), 0L);
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

    /** Advance F's WRITTEN-data end (real bytes written) to the max end seen; never moves backward. */
    private void advanceWrittenDataEnd(String path, long end) {
        long cur = writtenDataEnd.getOrDefault(path, 0L);
        if (end > cur) {
            writtenDataEnd.put(path, end);
        }
    }

    /** Advance F's at-device (synced) end to the max value seen; never moves backward. */
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

    /** Return a prefix of {@code src} of length {@code min(end, src.length)} (the journaled extent). */
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

    /** File offset of the mapping that {@code addr} falls in (exact base match first, else range containment). */
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
