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

package io.questdb.cairo.wal.sortedruns;

import io.questdb.cairo.CairoException;
import io.questdb.cairo.vm.Vm;
import io.questdb.cairo.vm.api.MemoryCMR;
import io.questdb.std.FilesFacade;
import io.questdb.std.LongList;
import io.questdb.std.MemoryTag;
import io.questdb.std.Misc;
import io.questdb.std.QuietCloseable;
import io.questdb.std.Unsafe;
import io.questdb.std.str.LPSZ;

/**
 * Per-pa handle for the {@code _sortedruns} sidecar.
 * Memory-maps the file read-only, parses the header, and builds an
 * in-memory directory of CommitStat records keyed by their offset and
 * partition row range. Exposes:
 * <ul>
 *   <li>{@link #findRunsOverlapping(long, long, int[])} for interval pruning,</li>
 *   <li>{@link #materialiseSortedSlice(long, long, long, int, long[], int)} for
 *       building a ts-sorted physRowId scratch over a logical row range,</li>
 *   <li>{@link #materialiseSortedSliceReverse(long, long, long, int, long[], int)}
 *       for the reverse-iteration counterpart.</li>
 * </ul>
 * <p>
 * Run cursor state for K-way merge is held internally and reused across
 * calls; the class is not thread-safe.
 */
public final class SortedRunsReader implements QuietCloseable {
    private final MemoryCMR commitsMem = Vm.getCMRInstance();
    private final LongList recordOffsets = new LongList();
    // Parallel arrays for fast lookup of decoded record fields.
    private final LongList recordPhysRowStarts = new LongList();
    private final LongList recordRowCounts = new LongList();
    private final LongList recordMinTs = new LongList();
    private final LongList recordMaxTs = new LongList();
    // K-way merge scratch: per-run current row offset within the run.
    private final LongList runCursors = new LongList();
    private long globalMaxTs = Long.MIN_VALUE;
    private long globalMinTs = Long.MAX_VALUE;
    private long logicalRowCount;
    private int recordCount;

    @Override
    public void close() {
        Misc.free(commitsMem);
        recordOffsets.clear();
        recordPhysRowStarts.clear();
        recordRowCounts.clear();
        recordMinTs.clear();
        recordMaxTs.clear();
        runCursors.clear();
        recordCount = 0;
        logicalRowCount = 0;
        globalMinTs = Long.MAX_VALUE;
        globalMaxTs = Long.MIN_VALUE;
    }

    /**
     * Returns the [loIndex, hiIndex] inclusive range of CommitStat records
     * whose ts coverage [minTs, maxTs] overlaps [intervalLo, intervalHi].
     * Writes the range into {@code outLoHi}. Returns true if any overlap
     * was found; false leaves {@code outLoHi} undefined.
     */
    public boolean findRunsOverlapping(long intervalLo, long intervalHi, int[] outLoHi) {
        int lo = -1;
        int hi = -1;
        for (int i = 0; i < recordCount; i++) {
            final long rMin = recordMinTs.getQuick(i);
            final long rMax = recordMaxTs.getQuick(i);
            if (rMax < intervalLo || rMin > intervalHi) {
                continue;
            }
            if (lo == -1) {
                lo = i;
            }
            hi = i;
        }
        if (lo == -1) {
            return false;
        }
        outLoHi[0] = lo;
        outLoHi[1] = hi;
        return true;
    }

    public long getGlobalMaxTs() {
        return globalMaxTs;
    }

    public long getGlobalMinTs() {
        return globalMinTs;
    }

    public long getLogicalRowCount() {
        return logicalRowCount;
    }

    /**
     * Walks all runs in K-way merge order and writes physRowId of each row in
     * timestamp-ascending order to off-heap memory at {@code destAddr}. Skips
     * the first {@code logicalLo} merge outputs, writes
     * {@code logicalHi - logicalLo} eight-byte entries.
     *
     * @param logicalLo            inclusive lower bound on the logical sorted
     *                             position to materialise.
     * @param logicalHi            exclusive upper bound.
     * @param tsColumnBaseAddress  base address of the partition's timestamp
     *                             column.
     * @param tsElementSize        size in bytes of each timestamp slot
     *                             (usually 8).
     * @param destAddr             off-heap destination, must have at least
     *                             {@code (logicalHi - logicalLo) * 8} bytes
     *                             available.
     */
    public void materialiseSortedSlice(
            long logicalLo,
            long logicalHi,
            long tsColumnBaseAddress,
            int tsElementSize,
            long destAddr
    ) {
        if (logicalHi <= logicalLo) {
            return;
        }
        runCursors.setPos(recordCount);
        for (int i = 0; i < recordCount; i++) {
            runCursors.setQuick(i, 0L);
        }
        long logicalIndex = 0;
        long outOffset = 0L;
        final long wantBytes = (logicalHi - logicalLo) * Long.BYTES;
        while (logicalIndex < logicalHi) {
            // Find the run with the smallest current head ts that still has rows.
            int bestRun = -1;
            long bestTs = Long.MAX_VALUE;
            for (int i = 0; i < recordCount; i++) {
                final long cursor = runCursors.getQuick(i);
                if (cursor >= recordRowCounts.getQuick(i)) {
                    continue;
                }
                final long physRowId = recordPhysRowStarts.getQuick(i) + cursor;
                final long ts = Unsafe.getUnsafe().getLong(tsColumnBaseAddress + physRowId * (long) tsElementSize);
                if (ts < bestTs) {
                    bestTs = ts;
                    bestRun = i;
                }
            }
            if (bestRun < 0) {
                break;
            }
            final long cursor = runCursors.getQuick(bestRun);
            final long physRowId = recordPhysRowStarts.getQuick(bestRun) + cursor;
            if (logicalIndex >= logicalLo) {
                Unsafe.getUnsafe().putLong(destAddr + outOffset, physRowId);
                outOffset += Long.BYTES;
                if (outOffset >= wantBytes) {
                    return;
                }
            }
            runCursors.setQuick(bestRun, cursor + 1);
            logicalIndex++;
        }
    }

    public void materialiseSortedSliceReverse(
            long logicalLo,
            long logicalHi,
            long tsColumnBaseAddress,
            int tsElementSize,
            long destAddr
    ) {
        if (logicalHi <= logicalLo) {
            return;
        }
        // Initialise cursors at run tails (one past the last row), so the first
        // step decrements to the last row.
        runCursors.setPos(recordCount);
        for (int i = 0; i < recordCount; i++) {
            runCursors.setQuick(i, recordRowCounts.getQuick(i));
        }
        // We iterate in reverse logical position: the merge produces rows in
        // descending ts. logicalHi-1 is the first row to emit, logicalLo is the
        // last. The merge index counts down from totalRows-1.
        long logicalIndex = logicalRowCount - 1;
        long outOffset = 0L;
        final long wantBytes = (logicalHi - logicalLo) * Long.BYTES;
        while (logicalIndex >= 0) {
            int bestRun = -1;
            long bestTs = Long.MIN_VALUE;
            for (int i = 0; i < recordCount; i++) {
                final long cursor = runCursors.getQuick(i);
                if (cursor <= 0) {
                    continue;
                }
                final long physRowId = recordPhysRowStarts.getQuick(i) + (cursor - 1);
                final long ts = Unsafe.getUnsafe().getLong(tsColumnBaseAddress + physRowId * (long) tsElementSize);
                if (ts > bestTs) {
                    bestTs = ts;
                    bestRun = i;
                }
            }
            if (bestRun < 0) {
                break;
            }
            final long cursor = runCursors.getQuick(bestRun);
            final long physRowId = recordPhysRowStarts.getQuick(bestRun) + (cursor - 1);
            if (logicalIndex < logicalHi && logicalIndex >= logicalLo) {
                Unsafe.getUnsafe().putLong(destAddr + outOffset, physRowId);
                outOffset += Long.BYTES;
                if (outOffset >= wantBytes) {
                    return;
                }
            }
            runCursors.setQuick(bestRun, cursor - 1);
            logicalIndex--;
        }
    }

    public SortedRunsReader of(FilesFacade ff, LPSZ commitsFile, long validSize) {
        if (validSize < SortedRunsFormat.HEADER_SIZE_BYTES) {
            throw CairoException.critical(0)
                    .put("_ts.commits valid size too small [size=").put(validSize)
                    .put(", required>=").put(SortedRunsFormat.HEADER_SIZE_BYTES).put(']');
        }
        commitsMem.of(ff, commitsFile, 0, validSize, MemoryTag.MMAP_DEFAULT);
        final long base = commitsMem.addressOf(0);
        final int magic = Unsafe.getUnsafe().getInt(base + SortedRunsFormat.HEADER_OFFSET_MAGIC);
        if (magic != SortedRunsFormat.MAGIC) {
            throw CairoException.critical(0)
                    .put("_ts.commits has bad magic [expected=0x").put(Integer.toHexString(SortedRunsFormat.MAGIC))
                    .put(", actual=0x").put(Integer.toHexString(magic)).put(']');
        }
        final int version = Unsafe.getUnsafe().getShort(base + SortedRunsFormat.HEADER_OFFSET_VERSION) & 0xFFFF;
        if (version != SortedRunsFormat.VERSION_1) {
            throw CairoException.critical(0)
                    .put("_ts.commits unsupported version [expected=").put(SortedRunsFormat.VERSION_1)
                    .put(", actual=").put(version).put(']');
        }
        recordOffsets.clear();
        recordPhysRowStarts.clear();
        recordRowCounts.clear();
        recordMinTs.clear();
        recordMaxTs.clear();
        recordCount = 0;
        logicalRowCount = 0;
        globalMinTs = Long.MAX_VALUE;
        globalMaxTs = Long.MIN_VALUE;
        long offset = SortedRunsFormat.HEADER_SIZE_BYTES;
        while (offset + SortedRunsFormat.RECORD_HEADER_SIZE_BYTES <= validSize) {
            final long physRowStart = Unsafe.getUnsafe().getLong(base + offset + SortedRunsFormat.RECORD_OFFSET_PHYS_ROW_START);
            final int rowCount = Unsafe.getUnsafe().getInt(base + offset + SortedRunsFormat.RECORD_OFFSET_ROW_COUNT);
            final long minTs = Unsafe.getUnsafe().getLong(base + offset + SortedRunsFormat.RECORD_OFFSET_MIN_TS);
            final long maxTs = Unsafe.getUnsafe().getLong(base + offset + SortedRunsFormat.RECORD_OFFSET_MAX_TS);
            final int extLength = Unsafe.getUnsafe().getInt(base + offset + SortedRunsFormat.RECORD_OFFSET_EXT_LENGTH);
            recordOffsets.add(offset);
            recordPhysRowStarts.add(physRowStart);
            recordRowCounts.add(rowCount);
            recordMinTs.add(minTs);
            recordMaxTs.add(maxTs);
            logicalRowCount += rowCount;
            if (minTs < globalMinTs) {
                globalMinTs = minTs;
            }
            if (maxTs > globalMaxTs) {
                globalMaxTs = maxTs;
            }
            recordCount++;
            offset += SortedRunsFormat.RECORD_HEADER_SIZE_BYTES + extLength;
        }
        return this;
    }

    public void recordAt(int i, SortedRunsRecord out) {
        if (i < 0 || i >= recordCount) {
            throw CairoException.nonCritical().put("record index out of range [i=").put(i).put(", count=").put(recordCount).put(']');
        }
        out.bind(commitsMem.addressOf(0), recordOffsets.getQuick(i));
    }

    public int recordCount() {
        return recordCount;
    }
}
