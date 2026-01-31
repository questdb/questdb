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

import io.questdb.cairo.AbstractRecordCursorFactory;
import io.questdb.cairo.idx.BitmapIndexReader;
import io.questdb.cairo.EmptySymbolMapReader;
import io.questdb.cairo.TableToken;
import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.PageFrame;
import io.questdb.cairo.sql.PageFrameCursor;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.cairo.sql.RecordMetadata;
import io.questdb.cairo.sql.StaticSymbolTable;
import io.questdb.cairo.sql.SymbolTable;
import io.questdb.cairo.sql.TimeFrame;
import io.questdb.cairo.sql.TimeFrameCursor;
import io.questdb.cairo.vm.api.MemoryCARW;
import io.questdb.griffin.PlanSink;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.engine.table.parquet.PartitionDecoder;
import io.questdb.jit.CompiledFilter;
import io.questdb.std.IntList;
import io.questdb.std.Misc;
import io.questdb.std.ObjList;
import org.jetbrains.annotations.Nullable;

public final class ExtraNullColumnCursorFactory extends AbstractRecordCursorFactory {

    private final RecordCursorFactory base;
    private final int columnSplit;
    private final ExtraNullColumnRecordCursor cursor;
    private ExtraNullColumnPageFrameCursor pageFrameCursor;
    private ExtraNullColumnTimeFrameCursor timeFrameCursor;

    public ExtraNullColumnCursorFactory(RecordMetadata metadata, int columnSplit, RecordCursorFactory base) {
        super(metadata);
        this.base = base;
        this.columnSplit = columnSplit;
        this.cursor = new ExtraNullColumnRecordCursor(columnSplit, base.recordCursorSupportsRandomAccess());
    }

    @Override
    public boolean followedOrderByAdvice() {
        return base.followedOrderByAdvice();
    }

    @Override
    public RecordCursorFactory getBaseFactory() {
        return base;
    }

    @Nullable
    public ObjList<Function> getBindVarFunctions() {
        return base.getBindVarFunctions();
    }

    @Nullable
    public MemoryCARW getBindVarMemory() {
        return base.getBindVarMemory();
    }

    @Override
    public CompiledFilter getCompiledFilter() {
        return base.getCompiledFilter();
    }

    @Override
    public RecordCursor getCursor(SqlExecutionContext executionContext) throws SqlException {
        final RecordCursor baseCursor = base.getCursor(executionContext);
        try {
            cursor.of(baseCursor);
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
    public PageFrameCursor getPageFrameCursor(SqlExecutionContext executionContext, int order) throws SqlException {
        PageFrameCursor baseCursor = base.getPageFrameCursor(executionContext, order);
        if (pageFrameCursor == null) {
            pageFrameCursor = new ExtraNullColumnPageFrameCursor(columnSplit, getMetadata().getColumnCount());
        }
        return pageFrameCursor.of(baseCursor);
    }

    @Override
    public int getScanDirection() {
        return base.getScanDirection();
    }

    @Override
    public TimeFrameCursor getTimeFrameCursor(SqlExecutionContext executionContext) throws SqlException {
        TimeFrameCursor baseCursor = base.getTimeFrameCursor(executionContext);
        if (timeFrameCursor == null) {
            timeFrameCursor = new ExtraNullColumnTimeFrameCursor(columnSplit, base.recordCursorSupportsRandomAccess());
        }
        return timeFrameCursor.of(baseCursor);
    }

    @Override
    public void halfClose() {
        base.halfClose();
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
        return base.recordCursorSupportsLongTopK(columnIndex);
    }

    @Override
    public boolean recordCursorSupportsRandomAccess() {
        return base.recordCursorSupportsRandomAccess();
    }

    @Override
    public boolean supportsPageFrameCursor() {
        return base.supportsPageFrameCursor();
    }

    @Override
    public boolean supportsTimeFrameCursor() {
        return base.supportsTimeFrameCursor();
    }

    @Override
    public boolean supportsUpdateRowId(TableToken tableToken) {
        return base.supportsUpdateRowId(tableToken);
    }

    @Override
    public void toPlan(PlanSink sink) {
        sink.type("ExtraNullColumnRecord");
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
        base.close();
    }

    private static class ExtraNullColumnPageFrame implements PageFrame {
        private final int columnCount;
        private final int columnSplit;
        private PageFrame baseFrame;

        private ExtraNullColumnPageFrame(int columnSplit, int columnCount) {
            this.columnSplit = columnSplit;
            this.columnCount = columnCount;
        }

        @Override
        public long getAuxPageAddress(int columnIndex) {
            return columnIndex < columnSplit ? baseFrame.getAuxPageAddress(columnIndex) : 0;
        }

        @Override
        public long getAuxPageSize(int columnIndex) {
            return columnIndex < columnSplit ? baseFrame.getAuxPageSize(columnIndex) : 0;
        }

        @Override
        public BitmapIndexReader getBitmapIndexReader(int columnIndex, int direction) {
            return columnIndex < columnSplit ? baseFrame.getBitmapIndexReader(columnIndex, direction) : null;
        }

        @Override
        public int getColumnCount() {
            return columnCount;
        }

        @Override
        public byte getFormat() {
            return baseFrame.getFormat();
        }

        @Override
        public long getPageAddress(int columnIndex) {
            return columnIndex < columnSplit ? baseFrame.getPageAddress(columnIndex) : 0;
        }

        @Override
        public long getPageSize(int columnIndex) {
            return columnIndex < columnSplit ? baseFrame.getPageSize(columnIndex) : 0;
        }

        @Override
        public PartitionDecoder getParquetPartitionDecoder() {
            return baseFrame.getParquetPartitionDecoder();
        }

        @Override
        public int getParquetRowGroup() {
            return baseFrame.getParquetRowGroup();
        }

        @Override
        public int getParquetRowGroupHi() {
            return baseFrame.getParquetRowGroupHi();
        }

        @Override
        public int getParquetRowGroupLo() {
            return baseFrame.getParquetRowGroupLo();
        }

        @Override
        public long getPartitionHi() {
            return baseFrame.getPartitionHi();
        }

        @Override
        public int getPartitionIndex() {
            return baseFrame.getPartitionIndex();
        }

        @Override
        public long getPartitionLo() {
            return baseFrame.getPartitionLo();
        }

        public ExtraNullColumnPageFrame of(PageFrame basePageFrame) {
            this.baseFrame = basePageFrame;
            return this;
        }
    }

    private static class ExtraNullColumnPageFrameCursor implements PageFrameCursor {
        private final int columnSplit;
        private final ExtraNullColumnPageFrame pageFrame;
        private PageFrameCursor baseCursor;

        private ExtraNullColumnPageFrameCursor(int columnSplit, int columnCount) {
            this.pageFrame = new ExtraNullColumnPageFrame(columnSplit, columnCount);
            this.columnSplit = columnSplit;
        }

        @Override
        public void calculateSize(RecordCursor.Counter counter) {
            baseCursor.calculateSize(counter);
        }

        @Override
        public void close() {
            baseCursor.close();
        }

        @Override
        public IntList getColumnIndexes() {
            return baseCursor.getColumnIndexes();
        }

        @Override
        public long getRemainingRowsInInterval() {
            return baseCursor.getRemainingRowsInInterval();
        }

        @Override
        public StaticSymbolTable getSymbolTable(int columnIndex) {
            return columnIndex < columnSplit ? baseCursor.getSymbolTable(columnIndex) : EmptySymbolMapReader.INSTANCE;
        }

        @Override
        public boolean isExternal() {
            return baseCursor.isExternal();
        }

        @Override
        public SymbolTable newSymbolTable(int columnIndex) {
            return columnIndex < columnSplit ? baseCursor.newSymbolTable(columnIndex) : EmptySymbolMapReader.INSTANCE;
        }

        @Override
        public @Nullable PageFrame next(long skipTarget) {
            PageFrame baseFrame = baseCursor.next(skipTarget);
            return baseFrame != null ? pageFrame.of(baseFrame) : null;
        }

        public ExtraNullColumnPageFrameCursor of(PageFrameCursor baseCursor) {
            this.baseCursor = baseCursor;
            return this;
        }

        @Override
        public long size() {
            return baseCursor.size();
        }

        @Override
        public boolean supportsSizeCalculation() {
            return baseCursor.supportsSizeCalculation();
        }

        @Override
        public void toTop() {
            baseCursor.toTop();
        }
    }

    public static final class ExtraNullColumnTimeFrameCursor implements TimeFrameCursor {
        private final int columnSplit;
        private final ExtraNullColumnRecord recordA;
        private final ExtraNullColumnRecord recordB;
        private TimeFrameCursor baseCursor;

        public ExtraNullColumnTimeFrameCursor(int columnSplit, boolean supportsRandomAccess) {
            this.recordA = new ExtraNullColumnRecord(columnSplit);
            if (supportsRandomAccess) {
                this.recordB = new ExtraNullColumnRecord(columnSplit);
            } else {
                this.recordB = null;
            }
            this.columnSplit = columnSplit;
        }

        @Override
        public void close() {
            baseCursor = Misc.free(baseCursor);
        }

        @Override
        public BitmapIndexReader getIndexReaderForCurrentFrame(int columnIndex, int direction) {
            return columnIndex < columnSplit ? baseCursor.getIndexReaderForCurrentFrame(columnIndex, direction) : null;
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
        public StaticSymbolTable getSymbolTable(int columnIndex) {
            return columnIndex < columnSplit ? baseCursor.getSymbolTable(columnIndex) : EmptySymbolMapReader.INSTANCE;
        }

        @Override
        public TimeFrame getTimeFrame() {
            return baseCursor.getTimeFrame();
        }

        @Override
        public int getTimestampIndex() {
            return 0;
        }

        @Override
        public void jumpTo(int frameIndex) {
            baseCursor.jumpTo(frameIndex);
        }

        @Override
        public SymbolTable newSymbolTable(int columnIndex) {
            return columnIndex < columnSplit ? baseCursor.newSymbolTable(columnIndex) : EmptySymbolMapReader.INSTANCE;
        }

        @Override
        public boolean next() {
            return baseCursor.next();
        }

        public ExtraNullColumnTimeFrameCursor of(TimeFrameCursor baseCursor) {
            this.baseCursor = baseCursor;
            recordA.of(baseCursor.getRecord());
            if (recordB != null) {
                recordB.of(baseCursor.getRecordB());
            }
            return this;
        }

        @Override
        public long open() {
            return baseCursor.open();
        }

        @Override
        public boolean prev() {
            return baseCursor.prev();
        }

        @Override
        public void recordAt(Record record, long rowId) {
            record = ((ExtraNullColumnRecord) record).getBaseRecord();
            baseCursor.recordAt(record, rowId);
        }

        @Override
        public void recordAtRowIndex(Record record, long rowIndex) {
            record = ((ExtraNullColumnRecord) record).getBaseRecord();
            baseCursor.recordAtRowIndex(record, rowIndex);
        }

        @Override
        public void toTop() {
            baseCursor.toTop();
        }
    }
}
