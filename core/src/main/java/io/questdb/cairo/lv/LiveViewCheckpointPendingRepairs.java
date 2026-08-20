/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2026 QuestDB
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 *
 ******************************************************************************/

package io.questdb.cairo.lv;

import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.CairoException;
import io.questdb.cairo.CommitMode;
import io.questdb.cairo.vm.Vm;
import io.questdb.cairo.vm.api.MemoryMARW;
import io.questdb.std.Files;
import io.questdb.std.FilesFacade;
import io.questdb.std.LongList;
import io.questdb.std.MemoryTag;
import io.questdb.std.Numbers;
import io.questdb.std.Transient;
import io.questdb.std.Zip;
import io.questdb.std.str.Path;
import org.jetbrains.annotations.NotNull;

/**
 * The durable set of closed anchor segments carrying a correction the view has
 * consumed but not yet repaired: {@code _checkpoints/_pending}.
 * <p>
 * A repair's cost is paid per replacement range, and a deep correction's range is the
 * correction's own depth. Stage 1(i) scoped that range to the segments the rows land in;
 * this is what takes those segments off the refresh's critical path altogether. When the
 * drain classifies an out-of-order change set, the corrections landing in a <b>closed</b>
 * anchor segment are recorded here and the turn advances its watermark over them; a
 * periodic backfill pass drains the set, repairing each segment once however many
 * corrections it accumulated.
 * <p>
 * Recording seqTxn ranges would not survive that deferral - {@code lvConsumedSeqTxn}
 * advances as soon as the turn completes, so the base WAL entry may be reclaimed before
 * the pass runs. What the pass actually needs is the <b>segment</b>: a whole-segment
 * replay reads the applied base table over {@code [segmentStart, segmentEndExclusive)},
 * which the base holds durably whatever the WAL retention did. So an entry carries the
 * segment's own boundary plus the extremes the corrections touched inside it, which is
 * exactly what {@link LiveViewCheckpointRepairPlan#ofSegment} plans from.
 * <p>
 * <b>Ordering against the watermark.</b> The set is made durable <em>before</em> the turn
 * advances over the change it describes. A crash in between re-drains the same base range,
 * re-classifies it and re-records the same segments, which {@link #add} merges rather than
 * duplicates - so the window is idempotent in everything but {@link #getRowCount}, which
 * bounds what a segment owes rather than counting it.
 * <p>
 * <b>Idempotence of the repair itself</b> is what makes the drain side of the pass safe
 * too: a whole-segment recompute from the base produces the same rows however many times
 * it runs, so the entry is cleared <em>after</em> its replacement applies and a crash in
 * between costs one repeated segment rather than a lost correction.
 * <p>
 * The file is a header, a run of fixed-size entries ordered oldest segment first, and a
 * trailing CRC over both. It is staged through a {@code .tmp} sibling and renamed into
 * place, so a crash mid-write leaves the previous set intact and an orphan {@code .tmp}
 * a reader never looks at. The in-memory half is one view's scratch, held on
 * {@link LiveViewInstance} and mutated only under its refresh latch.
 */
public final class LiveViewCheckpointPendingRepairs {

    public static final int DEFINITION_TXN_OFFSET = 16;
    public static final int ENTRY_COUNT_OFFSET = 12;
    /**
     * Bytes one entry occupies: {@code segmentStart}, {@code segmentEndExclusive},
     * {@code minTs}, {@code maxTs}, {@code rowCount}, all LONG.
     */
    public static final int ENTRY_SIZE = 5 * Long.BYTES; // 40
    public static final int FORMAT_VERSION = 1;
    public static final int FORMAT_VERSION_OFFSET = 8;
    /**
     * Bytes ahead of the first entry: magic LONG, formatVersion INT, entryCount INT,
     * definitionTxn LONG, reserved LONG.
     */
    public static final int HEADER_SIZE = 32;
    /**
     * Magic marking the pending-repair set: ASCII {@code "LVPEND"} with a trailing
     * version nibble.
     */
    public static final long MAGIC = 0x4C56_5045_4E44_0001L;
    public static final int MAGIC_OFFSET = 0;
    /**
     * How many distinct closed segments one view may carry unrepaired. A view that
     * reaches the cap keeps recording nothing further and repairs the overflowing
     * correction inline instead, which is what every correction did before deferral
     * existed - so the cap degrades the saving rather than the view.
     */
    public static final int MAX_SEGMENTS = 1024;
    public static final String PENDING_FILE_NAME = "_pending";
    /**
     * No set on disk. Indistinguishable from an empty one, and treated as one: a view
     * that never deferred writes no file, and a pass that drains the last entry removes it.
     */
    public static final int READ_ABSENT = 1;
    /**
     * A set is on disk and cannot be trusted - wrong size, bad magic, bad format
     * version, failed CRC, or a definition txn naming a different view. The corrections
     * it named are already consumed from the base WAL's point of view, so the caller
     * cannot simply drop it: it has to recompute the view from the applied base, which
     * holds every one of those rows.
     */
    public static final int READ_CORRUPT = 2;
    /**
     * The set read back and validated.
     */
    public static final int READ_OK = 0;
    static final int RESERVED_OFFSET = 24;
    // segmentStart, segmentEndExclusive, minTs, maxTs, rowCount per entry, ordered by
    // segmentStart ascending - the order a pass has to drain them in, because a later
    // segment's cumulative row positions depend on how many rows the earlier ones added.
    private static final int STRIDE = 5;
    private final LongList segments = new LongList();

    /**
     * Points {@code dst} at {@code <checkpointsDir>/_pending}.
     */
    public static Path pendingPath(@NotNull Path dst, @Transient @NotNull Path checkpointsDir) {
        return dst.of(checkpointsDir).concat(PENDING_FILE_NAME);
    }

    /**
     * Reads the durable set into {@code dst}, replacing whatever it held.
     *
     * @return {@link #READ_OK}, {@link #READ_ABSENT} or {@link #READ_CORRUPT}
     */
    public static int read(
            @NotNull CairoConfiguration configuration,
            @Transient @NotNull Path checkpointsDir,
            long definitionTxn,
            @NotNull LiveViewCheckpointPendingRepairs dst
    ) {
        dst.clear();
        final FilesFacade ff = configuration.getFilesFacade();
        try (Path path = new Path()) {
            pendingPath(path, checkpointsDir);
            if (!ff.exists(path.$())) {
                return READ_ABSENT;
            }
            final long length = ff.length(path.$());
            if (length < HEADER_SIZE + Integer.BYTES
                    || length > HEADER_SIZE + (long) MAX_SEGMENTS * ENTRY_SIZE + Integer.BYTES) {
                // Bounded before the mapping, not after: a corrupt length field is exactly
                // what a set that does not validate is likely to carry, and mapping it
                // first would let the file decide how much address space to take.
                return READ_CORRUPT;
            }
            final MemoryMARW mem = Vm.getCMARWInstance();
            try {
                mem.of(ff, path.$(), length, -1, MemoryTag.MMAP_DEFAULT, CairoConfiguration.O_NONE, -1);
                if (mem.getLong(MAGIC_OFFSET) != MAGIC || mem.getInt(FORMAT_VERSION_OFFSET) != FORMAT_VERSION) {
                    return READ_CORRUPT;
                }
                final int entryCount = mem.getInt(ENTRY_COUNT_OFFSET);
                if (entryCount < 0 || entryCount > MAX_SEGMENTS) {
                    return READ_CORRUPT;
                }
                final int crcOffset = HEADER_SIZE + entryCount * ENTRY_SIZE;
                if (length != crcOffset + Integer.BYTES) {
                    return READ_CORRUPT;
                }
                if (Zip.crc32(0, mem.addressOf(0), crcOffset) != mem.getInt(crcOffset)) {
                    return READ_CORRUPT;
                }
                if (mem.getLong(DEFINITION_TXN_OFFSET) != definitionTxn) {
                    // A set written for a different incarnation of this table directory.
                    // It names segments of a view that no longer exists, and the current
                    // one cannot prove its own output is repaired, so it reads as corrupt
                    // rather than as absent.
                    return READ_CORRUPT;
                }
                long previousStart = Long.MIN_VALUE;
                for (int i = 0; i < entryCount; i++) {
                    final long base = HEADER_SIZE + (long) i * ENTRY_SIZE;
                    final long segmentStart = mem.getLong(base);
                    final long segmentEndExclusive = mem.getLong(base + Long.BYTES);
                    final long minTs = mem.getLong(base + 2L * Long.BYTES);
                    final long maxTs = mem.getLong(base + 3L * Long.BYTES);
                    final long rowCount = mem.getLong(base + 4L * Long.BYTES);
                    if (i > 0 && segmentStart <= previousStart) {
                        // The drain order is the whole contract of the file. A set that
                        // does not hold it describes a repair sequence whose row positions
                        // would not add up.
                        dst.clear();
                        return READ_CORRUPT;
                    }
                    if (segmentEndExclusive <= segmentStart
                            || minTs < segmentStart || maxTs >= segmentEndExclusive || maxTs < minTs
                            || rowCount < 0) {
                        dst.clear();
                        return READ_CORRUPT;
                    }
                    previousStart = segmentStart;
                    dst.segments.add(segmentStart);
                    dst.segments.add(segmentEndExclusive);
                    dst.segments.add(minTs);
                    dst.segments.add(maxTs);
                    dst.segments.add(rowCount);
                }
                return READ_OK;
            } finally {
                mem.close(false);
            }
        }
    }

    /**
     * Removes the set and any orphan {@code .tmp}. Best effort: a failure to unlink
     * leaves a set whose entries a pass recomputes and clears again, which costs one
     * repeated whole-segment replay and no correctness.
     */
    public static void remove(@NotNull FilesFacade ff, @Transient @NotNull Path checkpointsDir) {
        try (Path path = new Path()) {
            pendingPath(path, checkpointsDir);
            ff.removeQuiet(path.$());
            path.put(LiveViewCheckpointLayout.TMP_SUFFIX);
            ff.removeQuiet(path.$());
        }
    }

    /**
     * Durably writes {@code src}, staged through {@code _pending.tmp} and renamed into
     * place. An empty set removes the file instead of writing an empty one, so a view
     * that owes nothing leaves nothing behind for a reader to validate.
     * <p>
     * Must be ordered before the watermark advances over the change the set describes,
     * and after the replacement that clears an entry has applied.
     */
    public static void write(
            @NotNull CairoConfiguration configuration,
            @Transient @NotNull Path checkpointsDir,
            long definitionTxn,
            @NotNull LiveViewCheckpointPendingRepairs src
    ) {
        final FilesFacade ff = configuration.getFilesFacade();
        final int entryCount = src.size();
        if (entryCount == 0) {
            remove(ff, checkpointsDir);
            return;
        }
        final int commitMode = configuration.getCommitMode();
        final int crcOffset = HEADER_SIZE + entryCount * ENTRY_SIZE;
        final long size = crcOffset + Integer.BYTES;
        try (Path tmpPath = new Path(); Path finalPath = new Path()) {
            // A view can defer before it ever seals, in which case nothing has created
            // the checkpoint directory yet. Every other file under it is written by a
            // seal, which makes it first.
            tmpPath.of(checkpointsDir).slash();
            if (ff.mkdirs(tmpPath, configuration.getMkDirMode()) != 0) {
                throw CairoException.critical(ff.errno())
                        .put("could not create live view checkpoint directory [path=").put(tmpPath).put(']');
            }
            pendingPath(finalPath, checkpointsDir);
            pendingPath(tmpPath, checkpointsDir);
            tmpPath.put(LiveViewCheckpointLayout.TMP_SUFFIX);
            final MemoryMARW mem = Vm.getCMARWInstance();
            try {
                mem.of(ff, tmpPath.$(), size, -1, MemoryTag.MMAP_DEFAULT, CairoConfiguration.O_NONE, -1);
                mem.putLong(MAGIC_OFFSET, MAGIC);
                mem.putInt(FORMAT_VERSION_OFFSET, FORMAT_VERSION);
                mem.putInt(ENTRY_COUNT_OFFSET, entryCount);
                mem.putLong(DEFINITION_TXN_OFFSET, definitionTxn);
                mem.putLong(RESERVED_OFFSET, 0);
                for (int i = 0; i < entryCount; i++) {
                    final long base = HEADER_SIZE + (long) i * ENTRY_SIZE;
                    final int entry = i * STRIDE;
                    mem.putLong(base, src.segments.getQuick(entry));
                    mem.putLong(base + Long.BYTES, src.segments.getQuick(entry + 1));
                    mem.putLong(base + 2L * Long.BYTES, src.segments.getQuick(entry + 2));
                    mem.putLong(base + 3L * Long.BYTES, src.segments.getQuick(entry + 3));
                    mem.putLong(base + 4L * Long.BYTES, src.segments.getQuick(entry + 4));
                }
                mem.putInt(crcOffset, Zip.crc32(0, mem.addressOf(0), crcOffset));
                if (commitMode != CommitMode.NOSYNC) {
                    mem.sync(commitMode == CommitMode.ASYNC);
                }
            } finally {
                // Close before rename: Windows rejects a rename over an open file, and
                // POSIX would leave a stale mapping to the old inode.
                mem.close(false);
            }
            // Every write past the first rewrites the fixed name, so the destination
            // can already exist.
            if (LiveViewCheckpointLayout.publishOverwrite(ff, tmpPath.$(), finalPath.$()) != Files.FILES_RENAME_OK) {
                ff.removeQuiet(tmpPath.$());
                throw CairoException.critical(ff.errno())
                        .put("could not publish live view checkpoint pending repair set");
            }
        }
    }

    /**
     * Folds one closed segment's correction into the set. An entry the set already holds
     * widens to cover the new extremes and adds the rows; a segment it does not opens a
     * fresh entry in drain order.
     *
     * @return false when the set is full, after which the caller must repair the
     * correction inline rather than defer it
     */
    public boolean add(long segmentStart, long segmentEndExclusive, long minTs, long maxTs, long rowCount) {
        final int index = indexOf(segmentStart);
        if (index >= 0) {
            final int entry = index * STRIDE;
            segments.setQuick(entry + 2, Math.min(segments.getQuick(entry + 2), minTs));
            segments.setQuick(entry + 3, Math.max(segments.getQuick(entry + 3), maxTs));
            segments.setQuick(entry + 4, segments.getQuick(entry + 4) + rowCount);
            return true;
        }
        if (size() >= MAX_SEGMENTS) {
            return false;
        }
        // Inserted in reverse so each add() lands ahead of the ones before it, leaving
        // the five fields in order at the entry's own base offset.
        final int base = (-index - 1) * STRIDE;
        segments.add(base, rowCount);
        segments.add(base, maxTs);
        segments.add(base, minTs);
        segments.add(base, segmentEndExclusive);
        segments.add(base, segmentStart);
        return true;
    }

    public void clear() {
        segments.clear();
    }

    /**
     * @return whether the set already names the closed segment starting at
     * {@code segmentStart}, which {@link #add} would merge into rather than open
     */
    public boolean contains(long segmentStart) {
        return indexOf(segmentStart) >= 0;
    }

    /**
     * Replaces this set's entries with {@code src}'s. The caller stages a mutation in a
     * scratch copy, writes the scratch, and promotes it here only once the write is
     * durable - so a write that throws leaves the mirror and the file agreeing.
     */
    public void copyFrom(@NotNull LiveViewCheckpointPendingRepairs src) {
        segments.clear();
        segments.addAll(src.segments);
    }

    /**
     * @return the highest in-view timestamp the corrections touched inside entry
     * {@code index}
     */
    public long getMaxTs(int index) {
        return segments.getQuick(index * STRIDE + 3);
    }

    /**
     * @return the lowest in-view timestamp the corrections touched inside entry
     * {@code index}. This is the correction floor the segment's repair plans from.
     */
    public long getMinTs(int index) {
        return segments.getQuick(index * STRIDE + 2);
    }

    /**
     * @return how many qualifying base rows the recorded corrections carried into entry
     * {@code index}. An upper bound on what the segment still owes rather than a count of
     * it, and diagnostic for that reason. Two things loosen it: a crash between the
     * durable write and the watermark advance re-records the same range, which the merge
     * cannot make idempotent; and a pass replays its segment off the <em>applied</em>
     * base, so it routinely emits corrections the drain has not classified yet and the
     * set then records rows that are already in the output.
     */
    public long getRowCount(int index) {
        return segments.getQuick(index * STRIDE + 4);
    }

    public long getSegmentEndExclusive(int index) {
        return segments.getQuick(index * STRIDE + 1);
    }

    /**
     * @return the inclusive start of entry {@code index}. Entries come back oldest
     * first, which is the order a pass must drain them in.
     */
    public long getSegmentStart(int index) {
        return segments.getQuick(index * STRIDE);
    }

    /**
     * @return the total qualifying rows across every entry, or 0 when the set is empty
     */
    public long getTotalRowCount() {
        long total = 0;
        for (int i = 0, n = size(); i < n; i++) {
            total += getRowCount(i);
        }
        return total;
    }

    public boolean isEmpty() {
        return segments.size() == 0;
    }

    /**
     * @return the inclusive start of the oldest pending segment, or
     * {@link Numbers#LONG_NULL} when the set is empty
     */
    public long oldestSegmentStart() {
        return isEmpty() ? Numbers.LONG_NULL : getSegmentStart(0);
    }

    /**
     * Drops entry {@code index}, which a pass calls once the segment's replacement has
     * applied.
     */
    public void removeAt(int index) {
        segments.removeIndexBlock(index * STRIDE, STRIDE);
    }

    public int size() {
        return segments.size() / STRIDE;
    }

    /**
     * Binary search over the segment starts. Returns the entry index when found, and
     * {@code -(insertionPoint) - 1} when not, in the {@code Arrays.binarySearch} shape.
     */
    private int indexOf(long segmentStart) {
        int low = 0;
        int high = size() - 1;
        while (low <= high) {
            final int mid = (low + high) >>> 1;
            final long midStart = segments.getQuick(mid * STRIDE);
            if (midStart < segmentStart) {
                low = mid + 1;
            } else if (midStart > segmentStart) {
                high = mid - 1;
            } else {
                return mid;
            }
        }
        return -(low + 1);
    }
}
