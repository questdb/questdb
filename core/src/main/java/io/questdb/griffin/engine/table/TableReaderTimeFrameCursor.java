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

package io.questdb.griffin.engine.table;

import io.questdb.cairo.*;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.*;
import io.questdb.cairo.vm.api.MemoryR;
import io.questdb.std.IntList;
import io.questdb.std.Misc;
import io.questdb.std.Mutable;
import io.questdb.std.Rows;

/**
 * The only supported partition order is forward, i.e. navigation
 * should start with a {@link #next()} call.
 */
public class TableReaderTimeFrameCursor implements TimeFrameRecordCursor {
    private final IntList columnIndexes;
    private final TableReaderSelectedColumnRecord recordA;
    private final TableReaderSelectedColumnRecord recordB;
    private final TableReaderTimeFrame timeFrame = new TableReaderTimeFrame();
    private DataFrameCursor dataFrameCursor;
    private PartitionBy.PartitionCeilMethod partitionCeilMethod;
    private int partitionHi;
    private TableReader reader;
    private int timestampIndex;

    public TableReaderTimeFrameCursor(IntList columnIndexes) {
        this.columnIndexes = columnIndexes;
        recordA = new TableReaderSelectedColumnRecord(columnIndexes);
        recordB = new TableReaderSelectedColumnRecord(columnIndexes);
    }

    @Override
    public void close() {
        dataFrameCursor = Misc.free(dataFrameCursor);
    }

    @Override
    public Record getRecord() {
        return recordA;
    }

    @Override
    public Record getRecordB() {
        return recordB;
    }

    @Override
    public SymbolTable getSymbolTable(int columnIndex) {
        return reader.getSymbolTable(columnIndexes.getQuick(columnIndex));
    }

    @Override
    public TimeFrame getTimeFrame() {
        return timeFrame;
    }

    @Override
    public SymbolTable newSymbolTable(int columnIndex) {
        return reader.newSymbolTable(columnIndexes.getQuick(columnIndex));
    }

    @Override
    public boolean next() {
        int partitionIndex = timeFrame.partitionIndex;
        if (++partitionIndex < partitionHi) {
            long timestampLo = reader.getPartitionTimestampByIndex(partitionIndex);
            long maxTimestampHi = partitionIndex < partitionHi - 2 ? reader.getPartitionTimestampByIndex(partitionIndex + 1) : Long.MAX_VALUE;
            timeFrame.of(partitionIndex, timestampLo, estimatePartitionHi(timestampLo, maxTimestampHi));
            return true;
        }
        // Update frame index in case of subsequent prev() call.
        timeFrame.of(partitionHi, Long.MIN_VALUE, Long.MIN_VALUE);
        return false;
    }

    public TimeFrameRecordCursor of(DataFrameCursor dataFrameCursor) {
        this.dataFrameCursor = dataFrameCursor;
        reader = dataFrameCursor.getTableReader();
        recordA.of(reader);
        recordB.of(reader);
        partitionHi = reader.getPartitionCount();
        partitionCeilMethod = PartitionBy.getPartitionCeilMethod(reader.getPartitionedBy());
        timestampIndex = reader.getMetadata().getTimestampIndex();
        toTop();
        return this;
    }

    @Override
    public long open() throws DataUnavailableException {
        int partitionIndex = timeFrame.partitionIndex;
        if (partitionIndex < 0 || partitionIndex > partitionHi - 1) {
            throw CairoException.nonCritical().put("open call on uninitialized time frame");
        }
        long size = reader.openPartition(timeFrame.partitionIndex);
        if (size > 0) {
            timeFrame.rowLo = 0;
            timeFrame.rowHi = size;
            final MemoryR column = reader.getColumn(TableReader.getPrimaryColumnIndex(reader.getColumnBase(timeFrame.partitionIndex), timestampIndex));
            timeFrame.timestampLo = column.getLong(0);
            timeFrame.timestampHi = column.getLong((size - 1) * 8) + 1;
            return size;
        }
        timeFrame.rowLo = 0;
        timeFrame.rowHi = 0;
        timeFrame.timestampLo = timeFrame.estimateTimestampLo;
        timeFrame.timestampHi = timeFrame.estimateTimestampLo;
        return 0;
    }

    @Override
    public boolean prev() {
        int partitionIndex = timeFrame.partitionIndex;
        if (--partitionIndex >= 0) {
            long timestampLo = reader.getPartitionTimestampByIndex(partitionIndex);
            long maxTimestampHi = partitionIndex < partitionHi - 2 ? reader.getPartitionTimestampByIndex(partitionIndex + 1) : Long.MAX_VALUE;
            timeFrame.of(partitionIndex, timestampLo, estimatePartitionHi(timestampLo, maxTimestampHi));
            return true;
        }
        // Update frame index in case of subsequent next() call.
        timeFrame.of(-1, Long.MIN_VALUE, Long.MIN_VALUE);
        return false;
    }

    @Override
    public void recordAt(Record record, long atRowId) {
        ((TableReaderSelectedColumnRecord) record).jumpTo(Rows.toPartitionIndex(atRowId), Rows.toLocalRowID(atRowId));
    }

    @Override
    public void toTop() {
        timeFrame.clear();
    }

    // maxTimestampHi is used to handle split partitions correctly as ceil method
    // will return the same value for all split partitions
    private long estimatePartitionHi(long partitionTimestamp, long maxTimestampHi) {
        // partitionCeilMethod is null in case of partition by NONE
        long partitionHi = partitionCeilMethod != null ? partitionCeilMethod.ceil(partitionTimestamp) : Long.MAX_VALUE;
        return Math.min(partitionHi, maxTimestampHi);
    }

    private static class TableReaderTimeFrame implements TimeFrame, Mutable {
        private long estimateTimestampHi;
        private long estimateTimestampLo;
        private int partitionIndex;
        private long rowHi;
        private long rowLo;
        private long timestampHi;
        private long timestampLo;

        @Override
        public void clear() {
            partitionIndex = -1;
            estimateTimestampLo = Long.MIN_VALUE;
            estimateTimestampHi = Long.MIN_VALUE;
            timestampLo = Long.MIN_VALUE;
            timestampHi = Long.MIN_VALUE;
            rowLo = -1;
            rowHi = -1;
        }

        @Override
        public int getIndex() {
            return partitionIndex;
        }

        @Override
        public long getRowHi() {
            return rowHi;
        }

        @Override
        public long getRowLo() {
            return rowLo;
        }

        @Override
        public long getTimestampEstimateHi() {
            return estimateTimestampHi;
        }

        @Override
        public long getTimestampEstimateLo() {
            return estimateTimestampLo;
        }

        @Override
        public long getTimestampHi() {
            return timestampHi;
        }

        @Override
        public long getTimestampLo() {
            return timestampLo;
        }

        @Override
        public boolean isOpen() {
            return rowLo != -1 && rowHi != -1;
        }

        public void of(int partitionIndex, long estimateTimestampLo, long estimateTimestampHi) {
            this.partitionIndex = partitionIndex;
            this.estimateTimestampLo = estimateTimestampLo;
            this.estimateTimestampHi = estimateTimestampHi;
            timestampLo = Long.MIN_VALUE;
            timestampHi = Long.MIN_VALUE;
            rowLo = -1;
            rowHi = -1;
        }
    }
}
