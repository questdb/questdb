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

package io.questdb.griffin.engine.table;

import io.questdb.cairo.SymbolMapReader;
import io.questdb.cairo.TableReader;
import io.questdb.cairo.TableUtils;
import io.questdb.cairo.idx.BitmapIndexReader;
import io.questdb.cairo.sql.PartitionFrame;
import io.questdb.cairo.sql.PartitionFrameCursor;
import io.questdb.cairo.sql.PartitionFrameCursorFactory;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.cairo.sql.RecordMetadata;
import io.questdb.cairo.sql.RowCursor;
import io.questdb.cairo.sql.SymbolTable;
import io.questdb.griffin.PlanSink;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.std.BitSet;
import io.questdb.std.IntList;
import io.questdb.std.Misc;
import org.jetbrains.annotations.NotNull;

/**
 * Returns DISTINCT symbol values by iterating posting index keys and checking
 * for existence in any partition. O(K × P) where K = symbol key count and
 * P = partition count, vs O(N) for a full table scan (N = total rows).
 * <p>
 * The symbol table retains keys even after all rows for a symbol are dropped
 * (via DROP PARTITION or soft deletes), so DISTINCT cannot trust the symbol
 * table alone. The posting index provides the authoritative answer: a key
 * with zero entries across all partitions has no rows.
 */
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
            @NotNull IntList columnIndexes,
            @NotNull IntList columnSizeShifts
    ) {
        this.metadata = metadata;
        this.dfcFactory = dfcFactory;
        this.columnIndexes = columnIndexes;

        this.cursor = new DistinctCursor(readerColumnIndex, queryColumnPosition);
    }

    @Override
    public void close() {
        Misc.free(dfcFactory);
    }

    @Override
    public RecordCursor getCursor(SqlExecutionContext executionContext) throws SqlException {
        PartitionFrameCursor frameCursor = dfcFactory.getCursor(
                executionContext,
                columnIndexes,
                PartitionFrameCursorFactory.ORDER_ASC
        );
        try {
            cursor.of(frameCursor);
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
        private final BitSet foundKeys = new BitSet();
        private final int queryColumnPosition;
        private final int readerColumnIndex;
        private final DistinctRecord record = new DistinctRecord();
        private int foundCount;
        private PartitionFrameCursor frameCursor;
        private boolean isNullReturned;
        private boolean isScanned;
        private int nextKeyToReturn;
        private int symbolCount;
        private TableReader tableReader;

        DistinctCursor(int readerColumnIndex, int queryColumnPosition) {
            this.readerColumnIndex = readerColumnIndex;
            this.queryColumnPosition = queryColumnPosition;
        }

        @Override
        public void close() {
            frameCursor = Misc.free(frameCursor);
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
            // Map query column index to reader column index — the DISTINCT result
            // has a single column but the underlying table may have the symbol at a
            // different position.
            return frameCursor.getSymbolTable(readerColumnIndex);
        }

        @Override
        public boolean hasNext() {
            if (!isScanned) {
                scanPartitions();
                isScanned = true;
            }
            while (nextKeyToReturn < symbolCount) {
                int key = nextKeyToReturn++;
                // foundKeys uses index key space: 0 = NULL, 1..N = symbol keys.
                // Symbol key k maps to index key k+1 (toIndexKey adds 1).
                if (foundKeys.get(key + 1)) {
                    record.symbolKey = key;
                    return true;
                }
            }
            // After all non-null keys, emit NULL if found (index key 0)
            if (foundKeys.get(0) && !isNullReturned) {
                isNullReturned = true;
                record.symbolKey = SymbolTable.VALUE_IS_NULL;
                return true;
            }
            return false;
        }

        private void scanPartitions() {
            // totalExpected = symbolCount non-null keys + 1 potential NULL key
            int totalExpected = symbolCount + 1;
            while (foundCount < totalExpected) {
                PartitionFrame frame = frameCursor.next();
                if (frame == null) {
                    return;
                }
                BitmapIndexReader indexReader = tableReader.getBitmapIndexReader(
                        frame.getPartitionIndex(),
                        readerColumnIndex,
                        BitmapIndexReader.DIR_FORWARD
                );
                // Bulk stride-scan: walks all dense/sparse gens in one sequential pass
                // and marks present keys in the BitSet. Returns newly found count,
                // or -1 if not supported (bitmap index fallback).
                int newlyFound = indexReader.collectDistinctKeys(foundKeys);
                if (newlyFound >= 0) {
                    foundCount += newlyFound;
                } else {
                    // Bitmap index fallback: per-key cursor check
                    for (int key = 0; key < symbolCount; key++) {
                        int indexKey = TableUtils.toIndexKey(key);
                        if (!foundKeys.get(indexKey)) {
                            RowCursor c = indexReader.getCursor(true, indexKey, frame.getRowLo(), frame.getRowHi() - 1);
                            if (c.hasNext() && !foundKeys.getAndSet(indexKey)) {
                                foundCount++;
                            }
                        }
                    }
                    if (!foundKeys.get(0)) {
                        RowCursor c = indexReader.getCursor(true, 0, frame.getRowLo(), frame.getRowHi() - 1);
                        if (c.hasNext() && !foundKeys.getAndSet(0)) {
                            foundCount++;
                        }
                    }
                }
            }
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
            return -1;
        }

        @Override
        public void toTop() {
            nextKeyToReturn = 0;
            isScanned = false;
            foundCount = 0;
            isNullReturned = false;
            foundKeys.clear();
            if (frameCursor != null) {
                frameCursor.toTop();
            }
        }

        void of(PartitionFrameCursor frameCursor) {
            this.frameCursor = frameCursor;
            this.tableReader = frameCursor.getTableReader();
            SymbolMapReader smr = tableReader.getSymbolMapReader(readerColumnIndex);
            this.symbolCount = smr.getSymbolCount();
            this.record.symbolTable = smr;
            this.nextKeyToReturn = 0;
            this.isScanned = false;
            this.foundCount = 0;
            this.isNullReturned = false;
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
            return symbolKey;
        }

        @Override
        public CharSequence getSymA(int col) {
            return symbolTable != null ? symbolTable.valueOf(symbolKey) : null;
        }

        @Override
        public CharSequence getSymB(int col) {
            return symbolTable != null ? symbolTable.valueBOf(symbolKey) : null;
        }
    }
}
