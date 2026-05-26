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

import io.questdb.cairo.TableReader;
import io.questdb.cairo.TimestampFinder;
import io.questdb.cairo.vm.api.MemoryR;
import io.questdb.std.Mutable;
import io.questdb.std.Unsafe;

/**
 * {@link TimestampFinder} implementation for partitions in the
 * indexed-sorted-runs format. The partition's timestamp column is in
 * insertion order on disk; the sort permutation lives in a per-partition
 * mmapped {@code _sortedruns.idx} file. Binary-search and random-access
 * lookups dereference the mapping via {@link Unsafe} - no allocations, no
 * K-way merge at query time.
 */
public final class SortedRunsTimestampFinder implements TimestampFinder, Mutable {
    private MemoryR column;
    private long indexAddr;
    private long maxTimestampApprox;
    private long minTimestampApprox;
    private int partitionIndex;
    private TableReader reader;
    private long rowCount;
    private SortedRunsReader runsReader;
    private int timestampColumnOffset;

    @Override
    public void clear() {
        column = null;
        reader = null;
        runsReader = null;
        indexAddr = 0;
        rowCount = 0;
    }

    @Override
    public long findTimestamp(long value, long rowLo, long rowHi) {
        // Binary search the mmapped permutation: find the largest logical
        // position p in [rowLo, rowHi] where ts(idx[p]) <= value.
        long lo = rowLo;
        long hi = rowHi;
        long resultIdx = rowLo - 1;
        while (lo <= hi) {
            final long mid = (lo + hi) >>> 1;
            final long physRowId = Unsafe.getLong(indexAddr + (mid << 3));
            final long ts = column.getLong(physRowId * 8L);
            if (ts <= value) {
                resultIdx = mid;
                lo = mid + 1;
            } else {
                hi = mid - 1;
            }
        }
        return resultIdx;
    }

    @Override
    public long maxTimestampApproxFromMetadata() {
        return maxTimestampApprox;
    }

    @Override
    public long maxTimestampExact() {
        return runsReader.getGlobalMaxTs();
    }

    @Override
    public long minTimestampApproxFromMetadata() {
        return minTimestampApprox;
    }

    @Override
    public long minTimestampExact() {
        return runsReader.getGlobalMinTs();
    }

    public SortedRunsTimestampFinder of(TableReader reader, int partitionIndex, int timestampIndex) {
        this.reader = reader;
        this.partitionIndex = partitionIndex;
        this.timestampColumnOffset = TableReader.getPrimaryColumnIndex(
                reader.getColumnBase(partitionIndex), timestampIndex);
        this.runsReader = reader.getAndInitSortedRunsReader(partitionIndex);
        this.rowCount = runsReader.getLogicalRowCount();
        this.minTimestampApprox = reader.getPartitionMinTimestampFromMetadata(partitionIndex);
        this.maxTimestampApprox = reader.getPartitionMaxTimestampFromMetadata(partitionIndex);
        return this;
    }

    @Override
    public void prepare() {
        this.column = reader.getColumn(timestampColumnOffset);
        this.indexAddr = reader.getAndInitSortedRunsIndex(partitionIndex).getAddress();
    }

    @Override
    public long timestampAt(long rowIndex) {
        final long physRowId = Unsafe.getUnsafe().getLong(indexAddr + (rowIndex << 3));
        return column.getLong(physRowId * 8L);
    }
}
