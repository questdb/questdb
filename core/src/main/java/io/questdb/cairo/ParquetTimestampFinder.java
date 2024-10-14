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

public class ParquetTimestampFinder implements TimestampFinder, Mutable, QuietCloseable {
    private final PartitionDecoder partitionDecoder = new PartitionDecoder();
    private final RowGroupBuffers rowGroupBuffers = new RowGroupBuffers(MemoryTag.NATIVE_PARQUET_PARTITION_DECODER);
    private final RowGroupStatBuffers statBuffers = new RowGroupStatBuffers(MemoryTag.NATIVE_PARQUET_PARTITION_DECODER);
    private final DirectIntList timestampIdAndType = new DirectIntList(2, MemoryTag.NATIVE_DEFAULT);
    private int partitionIndex = -1;
    private TableToken tableToken;

    @Override
    public void clear() {
        partitionIndex = -1;
        tableToken = null;
    }

    @Override
    public void close() {
        Misc.free(partitionDecoder);
        Misc.free(rowGroupBuffers);
        Misc.free(statBuffers);
        Misc.free(timestampIdAndType);
        clear();
    }

    @Override
    public long findTimestamp(long value, long rowLo, long rowHi, int scanDir) {
        // TODO(puzpuzpuz): implement me
        return 0;
    }

    @Override
    public long maxTimestamp() {
        // Read the min value from the stats to avoid decoding.
        final int rowGroupCount = partitionDecoder.metadata().rowGroupCount();
        partitionDecoder.readRowGroupStats(statBuffers, timestampIdAndType, rowGroupCount - 1);
        return statBuffers.getMaxValueLong(0);
    }

    @Override
    public long minTimestamp() {
        // Read the min value from the stats to avoid decoding.
        partitionDecoder.readRowGroupStats(statBuffers, timestampIdAndType, 0);
        return statBuffers.getMinValueLong(0);
    }

    public ParquetTimestampFinder of(TableReader reader, int partitionIndex, int timestampIndex) {
        this.partitionIndex = partitionIndex;
        tableToken = reader.getTableToken();
        partitionDecoder.of(
                reader.getParquetFd(partitionIndex),
                reader.getParquetReadSize(partitionIndex),
                MemoryTag.NATIVE_PARQUET_PARTITION_DECODER);

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
        timestampIdAndType.add(ColumnType.TIMESTAMP);

        return this;
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
