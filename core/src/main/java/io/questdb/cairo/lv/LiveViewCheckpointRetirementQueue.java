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
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.std.Files;
import io.questdb.std.FilesFacade;
import io.questdb.std.LongList;
import io.questdb.std.MemoryTag;
import io.questdb.std.Transient;
import io.questdb.std.Zip;
import io.questdb.std.str.Path;
import org.jetbrains.annotations.NotNull;

/**
 * Durable bounded work set for checkpoint segment reclamation. Entries are
 * {@code (segmentId, fileLength, retireGeneration, kind)} longs sorted by id.
 * The complete image is written to a checksummed temporary sibling and renamed
 * before the superblock publication which makes new zero-reference transitions
 * authoritative. A failed publication can therefore leave false-positive work,
 * but the purge always rechecks the pinned catalogue before unlinking, so it can
 * never turn one into a false deletion.
 *
 * <p>The queue deliberately retains an already-absent file until a later normal
 * publication removes its catalogue entry. That makes the hand-off crash safe:
 * every sweep re-proposes the dead entry, and only observing it absent from the
 * selected catalogue prunes it from this file.</p>
 */
final class LiveViewCheckpointRetirementQueue {

    static final int ENTRY_STRIDE = 4;
    private static final int COUNT_OFFSET = 12;
    private static final int FORMAT_VERSION = 1;
    private static final int FORMAT_VERSION_OFFSET = 8;
    private static final int GENERATION_OFFSET = 16;
    private static final int HEADER_SIZE = 32;
    private static final int LIVE_DATA_SEGMENT_COUNT_OFFSET = 24;
    private static final Log LOG = LogFactory.getLog(LiveViewCheckpointRetirementQueue.class);
    private static final long MAGIC = 0x4C56_5254_5155_0001L; // LVRTQU v1
    private static final int MAX_ENTRY_COUNT = 1 << 24;

    private LiveViewCheckpointRetirementQueue() {
    }

    static void mergeAndWrite(
            @NotNull CairoConfiguration configuration,
            @Transient @NotNull Path checkpointsDir,
            @NotNull LongList additions,
            @NotNull LongList seed,
            long generation,
            long liveDataSegmentCount
    ) {
        final LongList entries = new LongList();
        final State state = new State();
        if (!read(configuration, checkpointsDir, entries, state) || state.generation + 1 != generation) {
            entries.clear();
            entries.add(seed);
        }
        for (int i = 0, n = additions.size(); i < n; i += ENTRY_STRIDE) {
            put(entries, additions, i);
        }
        write(configuration, checkpointsDir, entries, generation, liveDataSegmentCount);
    }

    /** Returns true only for a present, structurally valid, checksummed image. */
    static boolean read(
            @NotNull CairoConfiguration configuration,
            @Transient @NotNull Path checkpointsDir,
            @NotNull LongList out,
            @NotNull State state
    ) {
        out.clear();
        state.clear();
        final FilesFacade ff = configuration.getFilesFacade();
        try (Path path = new Path()) {
            LiveViewCheckpointLayout.retirementQueuePath(path, checkpointsDir);
            if (!ff.exists(path.$())) {
                return false;
            }
            final long size = ff.length(path.$());
            if (size < HEADER_SIZE + Integer.BYTES || size > Integer.MAX_VALUE) {
                logInvalid(path, "size");
                return false;
            }
            final MemoryMARW mem = Vm.getCMARWInstance();
            try {
                mem.of(ff, path.$(), size, -1, MemoryTag.MMAP_DEFAULT, CairoConfiguration.O_NONE, -1);
                if (mem.getLong(0) != MAGIC || mem.getInt(FORMAT_VERSION_OFFSET) != FORMAT_VERSION) {
                    logInvalid(path, "magic or format");
                    return false;
                }
                final int count = mem.getInt(COUNT_OFFSET);
                final long generation = mem.getLong(GENERATION_OFFSET);
                final long liveDataSegmentCount = mem.getLong(LIVE_DATA_SEGMENT_COUNT_OFFSET);
                final long expectedSize = HEADER_SIZE + (long) count * ENTRY_STRIDE * Long.BYTES + Integer.BYTES;
                if (count < 0 || count > MAX_ENTRY_COUNT || generation < 0
                        || liveDataSegmentCount < 0 || expectedSize != size) {
                    logInvalid(path, "entry count");
                    return false;
                }
                final int crcOffset = (int) size - Integer.BYTES;
                if (Zip.crc32(0, mem.addressOf(0), crcOffset) != mem.getInt(crcOffset)) {
                    logInvalid(path, "checksum");
                    return false;
                }
                long offset = HEADER_SIZE;
                long previousSegmentId = -1;
                for (int i = 0; i < count; i++) {
                    final long segmentId = mem.getLong(offset);
                    final long fileLength = mem.getLong(offset + Long.BYTES);
                    final long retireGeneration = mem.getLong(offset + 2L * Long.BYTES);
                    final long kind = mem.getLong(offset + 3L * Long.BYTES);
                    if (segmentId <= previousSegmentId
                            || fileLength <= 0
                            || retireGeneration < 0
                            || !isValidKind(kind)) {
                        logInvalid(path, "entry payload");
                        out.clear();
                        return false;
                    }
                    out.add(segmentId, fileLength, retireGeneration, kind);
                    previousSegmentId = segmentId;
                    offset += ENTRY_STRIDE * Long.BYTES;
                }
                state.generation = generation;
                state.liveDataSegmentCount = liveDataSegmentCount;
                return true;
            } finally {
                mem.close(false);
            }
        }
    }

    static void write(
            @NotNull CairoConfiguration configuration,
            @Transient @NotNull Path checkpointsDir,
            @NotNull LongList entries,
            long generation,
            long liveDataSegmentCount
    ) {
        if (entries.size() % ENTRY_STRIDE != 0 || entries.size() / ENTRY_STRIDE > MAX_ENTRY_COUNT
                || generation < 0 || liveDataSegmentCount < 0) {
            throw CairoException.critical(0).put("live view checkpoint retirement queue size invalid");
        }
        long previousSegmentId = -1;
        for (int i = 0, n = entries.size(); i < n; i += ENTRY_STRIDE) {
            final long segmentId = entries.getQuick(i);
            if (segmentId <= previousSegmentId
                    || entries.getQuick(i + 1) <= 0
                    || entries.getQuick(i + 2) < 0
                    || !isValidKind(entries.getQuick(i + 3))) {
                throw CairoException.critical(0).put("live view checkpoint retirement queue entry invalid");
            }
            previousSegmentId = segmentId;
        }
        final FilesFacade ff = configuration.getFilesFacade();
        final int commitMode = configuration.getCommitMode();
        final long size = HEADER_SIZE + (long) entries.size() * Long.BYTES + Integer.BYTES;
        try (Path finalPath = new Path(); Path tmpPath = new Path()) {
            LiveViewCheckpointLayout.retirementQueuePath(finalPath, checkpointsDir);
            LiveViewCheckpointLayout.retirementQueuePath(tmpPath, checkpointsDir).put(LiveViewCheckpointLayout.TMP_SUFFIX);
            final MemoryMARW mem = Vm.getCMARWInstance();
            try {
                mem.of(ff, tmpPath.$(), size, -1, MemoryTag.MMAP_DEFAULT, CairoConfiguration.O_NONE, -1);
                mem.jumpTo(0);
                mem.putLong(MAGIC);
                mem.putInt(FORMAT_VERSION);
                mem.putInt(entries.size() / ENTRY_STRIDE);
                mem.putLong(generation);
                mem.putLong(liveDataSegmentCount);
                for (int i = 0, n = entries.size(); i < n; i++) {
                    mem.putLong(entries.getQuick(i));
                }
                final int crc = Zip.crc32(0, mem.addressOf(0), (int) size - Integer.BYTES);
                mem.putInt(crc);
                if (commitMode != CommitMode.NOSYNC) {
                    mem.sync(commitMode == CommitMode.ASYNC);
                }
            } finally {
                mem.close(true, Vm.TRUNCATE_TO_POINTER);
            }
            if (LiveViewCheckpointLayout.publishOverwrite(ff, tmpPath.$(), finalPath.$()) != Files.FILES_RENAME_OK) {
                ff.removeQuiet(tmpPath.$());
                throw CairoException.critical(ff.errno())
                        .put("could not publish live view checkpoint retirement queue");
            }
        }
    }

    private static void logInvalid(Path path, CharSequence reason) {
        LOG.error().$("invalid live view checkpoint retirement queue [path=")
                .$(path).$(", reason=").$(reason).I$();
    }

    private static boolean isValidKind(long kind) {
        return kind == LiveViewCheckpointSegmentDirectory.SEGMENT_KIND_DATA
                || kind == LiveViewCheckpointSegmentDirectory.SEGMENT_KIND_META
                || kind == LiveViewCheckpointSegmentDirectory.SEGMENT_KIND_BOUNDARY;
    }

    private static void put(LongList entries, LongList source, int sourceOffset) {
        final long segmentId = source.getQuick(sourceOffset);
        int lo = 0;
        int hi = entries.size() / ENTRY_STRIDE;
        while (lo < hi) {
            final int mid = (lo + hi) >>> 1;
            if (entries.getQuick(mid * ENTRY_STRIDE) < segmentId) {
                lo = mid + 1;
            } else {
                hi = mid;
            }
        }
        final int offset = lo * ENTRY_STRIDE;
        if (lo < entries.size() / ENTRY_STRIDE && entries.getQuick(offset) == segmentId) {
            for (int i = 0; i < ENTRY_STRIDE; i++) {
                entries.setQuick(offset + i, source.getQuick(sourceOffset + i));
            }
            return;
        }
        entries.insertFromSource(offset, source, sourceOffset, sourceOffset + ENTRY_STRIDE);
    }

    static final class State {
        long generation = -1;
        long liveDataSegmentCount;

        void clear() {
            generation = -1;
            liveDataSegmentCount = 0;
        }
    }
}
