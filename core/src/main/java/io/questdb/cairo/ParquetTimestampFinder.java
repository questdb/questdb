/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2024 QuestDB
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

package io.questdb.cairo;

import io.questdb.griffin.engine.table.parquet.PartitionDecoder;
import io.questdb.griffin.engine.table.parquet.RowGroupBuffers;
import io.questdb.griffin.engine.table.parquet.RowGroupStatBuffers;
import io.questdb.std.DirectIntList;
import io.questdb.std.MemoryTag;
import io.questdb.std.Misc;
import io.questdb.std.Mutable;
import io.questdb.std.QuietCloseable;
import io.questdb.std.Unsafe;
import io.questdb.std.Vect;

import static io.questdb.std.Vect.BIN_SEARCH_SCAN_DOWN;

public class ParquetTimestampFinder implements TimestampFinder, Mutable, QuietCloseable {
    private final PartitionDecoder partitionDecoder; // the decoder is managed externally
    private final RowGroupBuffers rowGroupBuffers = new RowGroupBuffers(MemoryTag.NATIVE_PARQUET_PARTITION_DECODER);
    private final RowGroupStatBuffers statBuffers = new RowGroupStatBuffers(MemoryTag.NATIVE_PARQUET_PARTITION_DECODER);
    private final DirectIntList timestampIdAndType = new DirectIntList(2, MemoryTag.NATIVE_DEFAULT);
    private long maxTimestampApprox;
    private long minTimestampApprox;
    private int partitionIndex = -1;
    private TableReader reader;
    private TableToken tableToken;
    private int timestampIndex;

    public ParquetTimestampFinder(PartitionDecoder partitionDecoder) {
        this.partitionDecoder = partitionDecoder;
    }

    @Override
    public void clear() {
        partitionIndex = -1;
        tableToken = null;
    }

    @Override
    public void close() {
        Misc.free(rowGroupBuffers);
        Misc.free(statBuffers);
        Misc.free(timestampIdAndType);
        clear();
    }

    @Override
    public long findTimestamp(long value, long rowLo, long rowHi) {
        final PartitionDecoder.Metadata metadata = partitionDecoder.metadata();
        final int rowGroupCount = metadata.rowGroupCount();

        // First, find row group containing the timestamp.
        final long encodedIndex = partitionDecoder.findRowGroupByTimestamp(value, rowLo, rowHi, timestampIdAndType.get(0));
        final int rowGroupIndex = PartitionDecoder.decodeRowGroupIndex(encodedIndex);
        final boolean noNeedToDecode = PartitionDecoder.decodeNoNeedToDecodeFlag(encodedIndex);
        if (rowGroupIndex == -1 && noNeedToDecode) {
            // timestamp is to the left of the first row group
            return rowLo - 1;
        }
        if (rowGroupIndex == rowGroupCount - 1 && noNeedToDecode) {
            // timestamp is at the end of the last row group
            // or between the last group and the previous one
            return rowHi;
        }

        // Next, decode it and do binary search.
        long offset = 0;
        for (int i = 0; i < rowGroupCount; i++) {
            if (i == rowGroupIndex) {
                if (noNeedToDecode) {
                    // right boundary of row group rowGroupIndex
                    // or between row groups rowGroupIndex and rowGroupIndex+1
                    return Math.max(rowLo, offset + metadata.rowGroupSize(i)) - 1;
                }
                break;
            }
            offset += metadata.rowGroupSize(i);
        }

        // Looks like we have to decode the row group.
        final long rowGroupRowLo = Math.max(rowLo - offset, 0);
        final long rowGroupRowHi = Math.min(rowHi - offset, metadata.rowGroupSize(rowGroupIndex) - 1);
        assert rowGroupRowLo <= rowGroupRowHi;
        partitionDecoder.decodeRowGroup(
                rowGroupBuffers,
                timestampIdAndType,
                rowGroupIndex,
                (int) rowGroupRowLo,
                (int) (rowGroupRowHi + 1) // exclusive
        );

        // At last, we can do binary search.
        long idx = Vect.binarySearch64Bit(
                rowGroupBuffers.getChunkDataPtr(0),
                value,
                0,
                rowGroupRowHi - rowGroupRowLo,
                BIN_SEARCH_SCAN_DOWN
        );
        if (idx < 0) {
            idx = -idx - 2;
        }
        return idx + rowGroupRowLo + offset;
    }

    @Override
    public long maxTimestampApproxFromMetadata() {
        return maxTimestampApprox;
    }

    @Override
    public long maxTimestampExact() {
        // Read the min value from the stats to avoid decoding.
        final int rowGroupCount = partitionDecoder.metadata().rowGroupCount();
        partitionDecoder.readRowGroupStats(statBuffers, timestampIdAndType, rowGroupCount - 1);
        return statBuffers.getMaxValueLong(0);
    }

    @Override
    public long minTimestampApproxFromMetadata() {
        return minTimestampApprox;
    }

    @Override
    public long minTimestampExact() {
        // Read the min value from the stats to avoid decoding.
        partitionDecoder.readRowGroupStats(statBuffers, timestampIdAndType, 0);
        return statBuffers.getMinValueLong(0);
    }

    public ParquetTimestampFinder of(TableReader reader, int partitionIndex, int timestampIndex) {
        this.partitionIndex = partitionIndex;
        this.reader = reader;
        this.timestampIndex = timestampIndex;
        this.minTimestampApprox = reader.getPartitionMinTimestampFromMetadata(partitionIndex);
        this.maxTimestampApprox = reader.getPartitionMaxTimestampFromMetadata(partitionIndex);
        tableToken = reader.getTableToken();
        return this;
    }

    @Override
    public void prepare() {
        partitionDecoder.of(
                reader.getParquetAddr(partitionIndex),
                reader.getParquetFileSize(partitionIndex),
                MemoryTag.NATIVE_PARQUET_PARTITION_DECODER
        );
        rowGroupBuffers.reopen();
        statBuffers.reopen();

        int parquetTimestampIndex = findTimestampIndex(partitionDecoder, timestampIndex);
        if (parquetTimestampIndex == -1) {
            throw CairoException.critical(0).put("missing timestamp column in parquet partition [table=").put(tableToken)
                    .put(", partitionIndex=").put(partitionIndex)
                    .put(", timestampIndex=").put(timestampIndex)
                    .put(']');
        }
        timestampIdAndType.reopen();
        timestampIdAndType.clear();
        timestampIdAndType.add(parquetTimestampIndex);
        timestampIdAndType.add(reader.getMetadata().getColumnType(timestampIndex));
    }

    @Override
    public long timestampAt(long rowIndex) {
        // Here we find the row group to which the given row belongs and decode a single row into a buffer.
        final PartitionDecoder.Metadata metadata = partitionDecoder.metadata();
        long rowCount = 0;
        for (int rowGroupIndex = 0, n = metadata.rowGroupCount(); rowGroupIndex < n; rowGroupIndex++) {
            long size = metadata.rowGroupSize(rowGroupIndex);
            if (rowIndex >= rowCount && rowIndex < rowCount + size) {
                int rowLo = (int) (rowIndex - rowCount);
                partitionDecoder.decodeRowGroup(rowGroupBuffers, timestampIdAndType, rowGroupIndex, rowLo, rowLo + 1);
                return Unsafe.getUnsafe().getLong(rowGroupBuffers.getChunkDataPtr(0));
            }
            rowCount += size;
        }
        throw CairoException.critical(0).put("index out of bounds when reading timestamp value in parquet partition [table=").put(tableToken)
                .put(", rowIndex=").put(rowIndex)
                .put(", partitionIndex=").put(partitionIndex)
                .put(']');
    }

    private static int findTimestampIndex(PartitionDecoder partitionDecoder, int timestampIndex) {
        final PartitionDecoder.Metadata metadata = partitionDecoder.metadata();
        for (int i = 0, n = metadata.columnCount(); i < n; i++) {
            if (metadata.columnId(i) == timestampIndex) {
                return i;
            }
        }
        return -1;
    }
}
