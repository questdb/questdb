/*+*****************************************************************************
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

package io.questdb.griffin.engine.table;

import io.questdb.cairo.SymbolMapReader;
import io.questdb.cairo.TableReader;
import io.questdb.cairo.idx.IndexReader;
import io.questdb.cairo.sql.PartitionFrame;
import io.questdb.cairo.sql.PartitionFrameCursor;
import io.questdb.cairo.sql.PartitionFrameCursorFactory;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.cairo.sql.RecordMetadata;
import io.questdb.cairo.sql.SqlExecutionCircuitBreaker;
import io.questdb.cairo.sql.SymbolTable;
import io.questdb.griffin.PlanSink;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.std.DirectBitSet;
import io.questdb.std.IntList;
import io.questdb.std.MemoryTag;
import io.questdb.std.Misc;
import io.questdb.std.Numbers;
import org.jetbrains.annotations.NotNull;

public class PostingIndexDistinctRecordCursorFactory implements RecordCursorFactory {
    private final IntList columnIndexes;

    private final DistinctCursor cursor;
    private final PartitionFrameCursorFactory dfcFactory;
    private final RecordMetadata metadata;

    public PostingIndexDistinctRecordCursorFactory(
            @NotNull RecordMetadata metadata,
            @NotNull PartitionFrameCursorFactory dfcFactory,
            int readerColumnIndex,
            int queryColumnPosition,
            @NotNull IntList columnIndexes
    ) {
        this.metadata = metadata;
        this.dfcFactory = dfcFactory;
        this.columnIndexes = columnIndexes;
        this.cursor = new DistinctCursor(readerColumnIndex, queryColumnPosition);
    }

    @Override
    public void close() {
        Misc.free(dfcFactory);
        Misc.free(cursor);
    }

    @Override
    public RecordCursor getCursor(SqlExecutionContext executionContext) throws SqlException {
        PartitionFrameCursor frameCursor = dfcFactory.getCursor(
                executionContext,
                columnIndexes,
                PartitionFrameCursorFactory.ORDER_ASC
        );
        try {
            cursor.of(frameCursor, executionContext.getCircuitBreaker());
        } catch (Throwable th) {
            Misc.free(frameCursor);
            throw th;
        }
        return cursor;
    }

    @Override
    public RecordMetadata getMetadata() {
        return metadata;
    }

    @Override
    public boolean recordCursorSupportsRandomAccess() {
        return false;
    }

    @Override
    public void toPlan(PlanSink sink) {
        sink.type("PostingIndex");
        sink.meta("op").val("distinct");
        sink.meta("on").val(metadata.getColumnName(cursor.queryColumnPosition));
        sink.child(dfcFactory);
    }

    private static class DistinctCursor implements RecordCursor {
        private final DirectBitSet foundKeys = new DirectBitSet(DirectBitSet.BITS_PER_WORD, MemoryTag.NATIVE_BIT_SET, true);
        private final int queryColumnPosition;
        private final int readerColumnIndex;
        private final DistinctRecord record = new DistinctRecord();
        private SqlExecutionCircuitBreaker circuitBreaker;
        private int foundCount;
        private PartitionFrameCursor frameCursor;
        private boolean isExhausted;
        private boolean isNullReturned;
        private boolean isScanned;
        private int nextKeyToReturn;
        private int symbolCount;
        private TableReader tableReader;
        private int yieldedCount;

        DistinctCursor(int readerColumnIndex, int queryColumnPosition) {
            this.readerColumnIndex = readerColumnIndex;
            this.queryColumnPosition = queryColumnPosition;
        }

        @Override
        public void calculateSize(SqlExecutionCircuitBreaker circuitBreaker, RecordCursor.Counter counter) {
            if (!isScanned) {
                scanPartitions(circuitBreaker);
                isScanned = true;
            }
            counter.add(foundCount - yieldedCount);
            yieldedCount = foundCount;
            nextKeyToReturn = symbolCount;
            isNullReturned = true;
            isExhausted = true;
        }

        @Override
        public void close() {
            frameCursor = Misc.free(frameCursor);
            Misc.free(foundKeys);
        }

        @Override
        public Record getRecord() {
            return record;
        }

        @Override
        public Record getRecordB() {
            throw new UnsupportedOperationException();
        }

        @Override
        public SymbolTable getSymbolTable(int columnIndex) {
            return frameCursor.getSymbolTable(readerColumnIndex);
        }

        @Override
        public boolean hasNext() {
            if (isExhausted) {
                return false;
            }
            if (!isScanned) {
                scanPartitions(circuitBreaker);
                isScanned = true;
            }
            int bit = foundKeys.nextSetBit(nextKeyToReturn + 1);
            if (bit > 0 && bit <= symbolCount) {
                record.symbolKey = bit - 1;
                nextKeyToReturn = bit;
                yieldedCount++;
                return true;
            }
            nextKeyToReturn = symbolCount;
            if (!isNullReturned && foundKeys.get(0)) {
                isNullReturned = true;
                record.symbolKey = SymbolTable.VALUE_IS_NULL;
                yieldedCount++;
                return true;
            }
            isExhausted = true;
            return false;
        }

        @Override
        public SymbolTable newSymbolTable(int columnIndex) {
            return frameCursor.newSymbolTable(readerColumnIndex);
        }

        @Override
        public long preComputedStateSize() {
            return 0;
        }

        @Override
        public void recordAt(Record record, long atRowId) {
            throw new UnsupportedOperationException();
        }

        @Override
        public long size() {
            return isScanned ? foundCount : -1;
        }

        @Override
        public void toTop() {
            nextKeyToReturn = 0;
            isScanned = false;
            foundCount = 0;
            yieldedCount = 0;
            isNullReturned = false;
            isExhausted = false;
            foundKeys.clear();
            if (frameCursor != null) {
                frameCursor.toTop();
            }
        }

        private void scanPartitions(SqlExecutionCircuitBreaker cb) {
            int totalExpected = symbolCount + 1;
            while (foundCount < totalExpected) {
                PartitionFrame frame = frameCursor.next();
                if (frame == null) {
                    return;
                }
                cb.statefulThrowExceptionIfTripped();
                int partitionIndex = frame.getPartitionIndex();
                IndexReader indexReader = tableReader.getIndexReader(
                        partitionIndex,
                        readerColumnIndex,
                        IndexReader.DIR_FORWARD
                );
                long rowLo = frame.getRowLo();
                long rowHi = frame.getRowHi();
                boolean fullPartition = rowLo == 0 && rowHi == tableReader.getPartitionRowCount(partitionIndex);
                foundCount += fullPartition
                        ? indexReader.collectDistinctKeys(foundKeys)
                        : indexReader.collectDistinctKeysInRange(foundKeys, rowLo, rowHi - 1);
            }
        }

        void of(PartitionFrameCursor frameCursor, SqlExecutionCircuitBreaker circuitBreaker) {
            this.frameCursor = frameCursor;
            this.circuitBreaker = circuitBreaker;
            this.tableReader = frameCursor.getTableReader();
            SymbolMapReader smr = tableReader.getSymbolMapReader(readerColumnIndex);
            this.symbolCount = smr.getSymbolCount();
            this.record.symbolTable = smr;
            this.nextKeyToReturn = 0;
            this.isScanned = false;
            this.foundCount = 0;
            this.yieldedCount = 0;
            this.isNullReturned = false;
            this.isExhausted = false;
            foundKeys.reserve(symbolCount + 1);
            foundKeys.clear();
        }
    }

    private static class DistinctRecord implements Record {
        private int symbolKey;
        private SymbolTable symbolTable;

        @Override
        public int getInt(int col) {
            return symbolKey;
        }

        @Override
        public long getRowId() {
            return Numbers.LONG_NULL;
        }

        @Override
        public CharSequence getSymA(int col) {
            return symbolTable.valueOf(symbolKey);
        }

        @Override
        public CharSequence getSymB(int col) {
            return symbolTable.valueBOf(symbolKey);
        }
    }
}
