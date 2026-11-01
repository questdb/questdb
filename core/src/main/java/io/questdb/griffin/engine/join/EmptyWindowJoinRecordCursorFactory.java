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


package io.questdb.griffin.engine.join;

import io.questdb.cairo.AbstractRecordCursorFactory;
import io.questdb.cairo.EmptySymbolMapReader;
import io.questdb.cairo.TableToken;
import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.ParquetDecodeHint;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.cairo.sql.RecordMetadata;
import io.questdb.cairo.sql.SqlExecutionCircuitBreaker;
import io.questdb.cairo.sql.SymbolTable;
import io.questdb.cairo.sql.VirtualRecord;
import io.questdb.griffin.PlanSink;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.engine.functions.GroupByFunction;
import io.questdb.griffin.engine.groupby.SimpleMapValue;
import io.questdb.std.DirectLongLongSortedList;
import io.questdb.std.Misc;
import io.questdb.std.ObjList;

/**
 * Null-extends its base for a window join step whose filter is constant false and which aggregates a count: every
 * row of the base carries the values the step's aggregates take over an empty window, which the group-by functions
 * read from a map value they set empty. A count reads zero there rather than NULL, so the factory serves neither page
 * nor time frames, unlike the NULL padding of {@link io.questdb.griffin.engine.table.ExtraNullColumnCursorFactory}.
 */
public final class EmptyWindowJoinRecordCursorFactory extends AbstractRecordCursorFactory {
    private final RecordCursorFactory base;
    private final int columnSplit;
    private final EmptyWindowJoinRecordCursor cursor;
    private final SimpleMapValue emptyValue;
    private final ObjList<GroupByFunction> groupByFunctions;

    /**
     * Adopts the base and the group-by functions, whose value types take {@code valueCount} map value slots, and frees
     * them on failure.
     */
    public EmptyWindowJoinRecordCursorFactory(
            RecordMetadata metadata,
            int columnSplit,
            RecordCursorFactory base,
            ObjList<GroupByFunction> groupByFunctions,
            int valueCount
    ) {
        super(metadata);
        this.base = base;
        this.columnSplit = columnSplit;
        this.groupByFunctions = groupByFunctions;
        try {
            this.emptyValue = new SimpleMapValue(valueCount);
            for (int i = 0, n = groupByFunctions.size(); i < n; i++) {
                groupByFunctions.getQuick(i).setEmpty(emptyValue);
            }
            this.cursor = new EmptyWindowJoinRecordCursor(base.recordCursorSupportsRandomAccess());
        } catch (Throwable th) {
            Misc.free(this, th);
            throw th;
        }
    }

    @Override
    public boolean followedOrderByAdvice() {
        return base.followedOrderByAdvice();
    }

    @Override
    public RecordCursorFactory getBaseFactory() {
        return base;
    }

    @Override
    public RecordCursor getCursor(SqlExecutionContext executionContext) throws SqlException {
        final RecordCursor baseCursor = base.getCursor(executionContext);
        try {
            cursor.of(baseCursor, executionContext);
            return cursor;
        } catch (Throwable th) {
            cursor.close();
            throw th;
        }
    }

    @Override
    public Function getFilter() {
        return base.getFilter();
    }

    @Override
    public int getScanDirection() {
        return base.getScanDirection();
    }

    @Override
    public boolean implementsLimit() {
        return base.implementsLimit();
    }

    @Override
    public boolean isProjection() {
        return true;
    }

    @Override
    public boolean recordCursorSupportsLongTopK(int columnIndex) {
        return columnIndex < columnSplit && base.recordCursorSupportsLongTopK(columnIndex);
    }

    @Override
    public boolean recordCursorSupportsRandomAccess() {
        return base.recordCursorSupportsRandomAccess();
    }

    @Override
    public boolean supportsUpdateRowId(TableToken tableToken) {
        return base.supportsUpdateRowId(tableToken);
    }

    @Override
    public void toPlan(PlanSink sink) {
        sink.type("Empty Window Join");
        sink.child(base);
    }

    @Override
    public boolean usesCompiledFilter() {
        return base.usesCompiledFilter();
    }

    @Override
    public boolean usesIndex() {
        return base.usesIndex();
    }

    @Override
    protected void _close() {
        Misc.free(base);
        Misc.freeObjList(groupByFunctions);
        Misc.free(emptyValue);
    }

    /**
     * Reads the base's columns below the split and the aggregates' empty values above it. The slave of the step reads
     * no rows, so the symbol tables the aggregates resolve their slave columns through are empty.
     */
    private class EmptyWindowJoinRecordCursor implements RecordCursor {
        private final VirtualRecord aggregates;
        private final JoinRecord recordA;
        private final JoinRecord recordB;
        private RecordCursor baseCursor;

        private EmptyWindowJoinRecordCursor(boolean isRandomAccess) {
            this.aggregates = new VirtualRecord(groupByFunctions);
            aggregates.of(emptyValue);
            this.recordA = new JoinRecord(columnSplit);
            this.recordB = isRandomAccess ? new JoinRecord(columnSplit) : null;
        }

        @Override
        public void calculateSize(SqlExecutionCircuitBreaker circuitBreaker, Counter counter) {
            baseCursor.calculateSize(circuitBreaker, counter);
        }

        @Override
        public void close() {
            if (baseCursor != null) {
                Function.cursorClosed(groupByFunctions);
                baseCursor = Misc.free(baseCursor);
            }
        }

        @Override
        public Record getRecord() {
            return recordA;
        }

        @Override
        public Record getRecordB() {
            if (recordB != null) {
                return recordB;
            }
            throw new UnsupportedOperationException();
        }

        @Override
        public SymbolTable getSymbolTable(int columnIndex) {
            return columnIndex < columnSplit ? baseCursor.getSymbolTable(columnIndex) : EmptySymbolMapReader.INSTANCE;
        }

        @Override
        public boolean hasNext() {
            return baseCursor.hasNext();
        }

        @Override
        public void longTopK(DirectLongLongSortedList list, int columnIndex) {
            if (columnIndex < columnSplit) {
                baseCursor.longTopK(list, columnIndex);
            }
        }

        @Override
        public SymbolTable newSymbolTable(int columnIndex) {
            return columnIndex < columnSplit ? baseCursor.newSymbolTable(columnIndex) : EmptySymbolMapReader.INSTANCE;
        }

        @Override
        public long preComputedStateSize() {
            return baseCursor.preComputedStateSize();
        }

        @Override
        public void recordAt(Record record, long atRowId) {
            baseCursor.recordAt(((JoinRecord) record).getMaster(), atRowId);
        }

        @Override
        public void setParquetDecodeHint(ParquetDecodeHint hint) {
            baseCursor.setParquetDecodeHint(hint);
        }

        @Override
        public long size() {
            return baseCursor.size();
        }

        @Override
        public void skipRows(Counter rowCount, long maxRowsAfterSkip) {
            baseCursor.skipRows(rowCount, maxRowsAfterSkip);
        }

        @Override
        public void toTop() {
            baseCursor.toTop();
        }

        private void of(RecordCursor baseCursor, SqlExecutionContext executionContext) throws SqlException {
            this.baseCursor = baseCursor;
            recordA.of(baseCursor.getRecord(), aggregates);
            if (recordB != null) {
                recordB.of(baseCursor.getRecordB(), aggregates);
            }
            Function.init(groupByFunctions, this, executionContext, null);
        }
    }
}
