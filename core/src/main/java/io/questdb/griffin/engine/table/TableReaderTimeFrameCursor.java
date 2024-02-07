/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2023 QuestDB
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
import io.questdb.std.Misc;
import io.questdb.std.Mutable;
import io.questdb.std.Rows;

/**
 * The only supported partition order is forward, i.e. navigation
 * should start with a {@link #next()} call.
 */
public class TableReaderTimeFrameCursor implements TimeFrameRecordCursor {
    private final TableReaderRecord recordA = new TableReaderRecord();
    private final TableReaderRecord recordB = new TableReaderRecord();
    private final TableReaderTimeFrame timeFrame = new TableReaderTimeFrame();
    private DataFrameCursor dataFrameCursor;
    private PartitionBy.PartitionCeilMethod partitionCeilMethod;
    private int partitionHi;
    private TableReader reader;

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
        return dataFrameCursor.getSymbolTable(columnIndex);
    }

    @Override
    public TimeFrame getTimeFrame() {
        return timeFrame;
    }

    @Override
    public SymbolTable newSymbolTable(int columnIndex) {
        return dataFrameCursor.newSymbolTable(columnIndex);
    }

    @Override
    public boolean next() {
        int partitionIndex = timeFrame.partitionIndex;
        if (++partitionIndex < partitionHi) {
            long timestampLo = reader.getPartitionTimestampByIndex(partitionIndex);
            timeFrame.of(partitionIndex, timestampLo, estimatePartitionHi(timestampLo));
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
        toTop();
        return this;
    }

    @Override
    public void open() throws DataUnavailableException {
        int partitionIndex = timeFrame.partitionIndex;
        if (partitionIndex < 0 || partitionIndex > partitionHi - 1) {
            throw CairoException.nonCritical().put("open call on uninitialized time frame");
        }
        long size = reader.openPartition(timeFrame.partitionIndex);
        timeFrame.rowLo = 0;
        timeFrame.rowHi = size;
    }

    @Override
    public boolean prev() {
        int partitionIndex = timeFrame.partitionIndex;
        if (--partitionIndex >= 0) {
            long timestampLo = reader.getPartitionTimestampByIndex(partitionIndex);
            timeFrame.of(partitionIndex, timestampLo, estimatePartitionHi(timestampLo));
            return true;
        }
        // Update frame index in case of subsequent next() call.
        timeFrame.of(-1, Long.MIN_VALUE, Long.MIN_VALUE);
        return false;
    }

    @Override
    public void recordAt(Record record, long atRowId) {
        ((TableReaderRecord) record).jumpTo(Rows.toPartitionIndex(atRowId), Rows.toLocalRowID(atRowId));
    }

    @Override
    public void toTop() {
        timeFrame.clear();
    }

    private long estimatePartitionHi(long partitionTimestamp) {
        // partitionCeilMethod is null in case of partition by NONE
        return partitionCeilMethod != null ? partitionCeilMethod.ceil(partitionTimestamp) : Long.MAX_VALUE;
    }

    private static class TableReaderTimeFrame implements TimeFrame, Mutable {
        private int partitionIndex;
        private long rowHi;
        private long rowLo;
        private long timestampHi;
        private long timestampLo;

        @Override
        public void clear() {
            partitionIndex = -1;
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

        public void of(int partitionIndex, long timestampLo, long timestampHi) {
            this.partitionIndex = partitionIndex;
            this.timestampLo = timestampLo;
            this.timestampHi = timestampHi;
            rowLo = -1;
            rowHi = -1;
        }
    }
}
