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

import io.questdb.cairo.AbstractRecordCursorFactory;
import io.questdb.cairo.EmptySymbolMapReader;
import io.questdb.cairo.ReaderScanProfile;
import io.questdb.cairo.TableReader;
import io.questdb.cairo.TableToken;
import io.questdb.cairo.idx.IndexReader;
import io.questdb.cairo.sql.ColumnMapping;
import io.questdb.cairo.sql.DataSource;
import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.PageFrame;
import io.questdb.cairo.sql.PageFrameCursor;
import io.questdb.cairo.sql.ParquetDecodeHint;
import io.questdb.cairo.sql.PartitionFrameCursor;
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
import io.questdb.griffin.engine.table.parquet.ParquetDecoder;
import io.questdb.jit.CompiledFilter;
import io.questdb.std.Misc;
import io.questdb.std.ObjList;
import org.jetbrains.annotations.Nullable;

public final class ExtraNullColumnCursorFactory extends AbstractRecordCursorFactory {
    private final RecordCursorFactory base;
    private final int columnSplit;
    private final ExtraNullColumnRecordCursor cursor;
    private ExtraNullColumnPageFrameCursor pageFrameCursor;
    private ExtraNullColumnTablePageFrameCursor tablePageFrameCursor;
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
        final PageFrameCursor baseCursor = base.getPageFrameCursor(executionContext, order);
        try {
            // Claim only what the base provides: a table base keeps the TablePageFrameCursor
            // surface (window-join parents downcast a slave's page-frame cursor to it), while a
            // non-table base such as read_parquet() - whose page-frame cursor is a plain
            // PageFrameCursor - gets a plain null-padding wrapper instead of a getTableReader()/
            // hasIntervalFilter()/toPartition() contract it cannot honor.
            if (baseCursor instanceof TablePageFrameCursor tableBaseCursor) {
                if (tablePageFrameCursor == null) {
                    tablePageFrameCursor = new ExtraNullColumnTablePageFrameCursor(columnSplit, getMetadata().getColumnCount());
                }
                return tablePageFrameCursor.wrap(tableBaseCursor);
            }
            if (pageFrameCursor == null) {
                pageFrameCursor = new ExtraNullColumnPageFrameCursor(columnSplit, getMetadata().getColumnCount());
            }
            return pageFrameCursor.wrap(baseCursor);
        } catch (Throwable th) {
            Misc.free(baseCursor);
            throw th;
        }
    }

    @Override
    public int getScanDirection() {
        return base.getScanDirection();
    }

    @Override
    public TimeFrameCursor getTimeFrameCursor(SqlExecutionContext executionContext) throws SqlException {
        TimeFrameCursor baseCursor = base.getTimeFrameCursor(executionContext);
        if (timeFrameCursor == null) {
            timeFrameCursor = new ExtraNullColumnTimeFrameCursor(columnSplit, base.recordCursorSupportsRandomAccess(), getMetadata().getTimestampIndex());
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
    public ConcurrentTimeFrameCursor newTimeFrameCursor() {
        ConcurrentTimeFrameCursor baseCursor = base.newTimeFrameCursor();
        if (baseCursor == null) {
            return null;
        }
        return new ExtraNullColumnConcurrentTimeFrameCursor(baseCursor, columnSplit, getMetadata().getTimestampIndex());
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

    static final class ExtraNullColumnConcurrentTimeFrameCursor implements ConcurrentTimeFrameCursor {
        private final int columnSplit;
        private final ConcurrentTimeFrameCursor delegate;
        private final ExtraNullColumnRecord extraNullColumnRecord;
        private final int selectedTimestampIndex;

        ExtraNullColumnConcurrentTimeFrameCursor(
                ConcurrentTimeFrameCursor delegate,
                int columnSplit,
                int selectedTimestampIndex
        ) {
            this.delegate = delegate;
            this.columnSplit = columnSplit;
            this.selectedTimestampIndex = selectedTimestampIndex;
            this.extraNullColumnRecord = new ExtraNullColumnRecord(columnSplit);
        }

        @Override
        public void close() {
            delegate.close();
        }

        @Override
        public Record getRecord() {
            return extraNullColumnRecord;
        }

        @Override
        public StaticSymbolTable getSymbolTable(int columnIndex) {
            return columnIndex < columnSplit ? delegate.getSymbolTable(columnIndex) : EmptySymbolMapReader.INSTANCE;
        }

        @Override
        public TimeFrame getTimeFrame() {
            return delegate.getTimeFrame();
        }

        @Override
        public int getTimestampIndex() {
            return selectedTimestampIndex;
        }

        @Override
        public void jumpTo(int frameIndex) {
            delegate.jumpTo(frameIndex);
        }

        @Override
        public SymbolTable newSymbolTable(int columnIndex) {
            return columnIndex < columnSplit ? delegate.newSymbolTable(columnIndex) : EmptySymbolMapReader.INSTANCE;
        }

        @Override
        public boolean next() {
            return delegate.next();
        }

        @Override
        public ConcurrentTimeFrameCursor of(
                ConcurrentTimeFrameState sharedState,
                TablePageFrameCursor frameCursor,
                int timestampIndex
        ) {
            delegate.of(sharedState, frameCursor, selectedTimestampIndex);
            extraNullColumnRecord.of(delegate.getRecord());
            return this;
        }

        @Override
        public long open() {
            return delegate.open();
        }

        @Override
        public boolean prev() {
            return delegate.prev();
        }

        @Override
        public void recordAt(Record record, long rowId) {
            delegate.recordAt(((ExtraNullColumnRecord) record).getBaseRecord(), rowId);
        }

        @Override
        public void recordAt(Record record, int frameIndex, long rowIndex) {
            delegate.recordAt(((ExtraNullColumnRecord) record).getBaseRecord(), frameIndex, rowIndex);
        }

        @Override
        public void recordAtRowIndex(Record record, long rowIndex) {
            delegate.recordAtRowIndex(((ExtraNullColumnRecord) record).getBaseRecord(), rowIndex);
        }

        @Override
        public void seekEstimate(long timestamp) {
            delegate.seekEstimate(timestamp);
        }

        @Override
        public void setParquetDecodeHint(ParquetDecodeHint hint) {
            delegate.setParquetDecodeHint(hint);
        }

        @Override
        public void toTop() {
            delegate.toTop();
        }
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
        public int getColumnCount() {
            return columnCount;
        }

        @Override
        public byte getColumnSource(int columnIndex) {
            // Below the split columns delegate 1:1 to the base (so a covered base
            // column still reports COVERED and drives the worker covered-decode
            // arm); the synthetic null-padding columns above the split are DIRECT.
            return columnIndex < columnSplit ? baseFrame.getColumnSource(columnIndex) : DataSource.DIRECT;
        }

        @Override
        public int getCoveredIncludeIndex(int columnIndex) {
            // Per-column: below the split delegate to the base; synthetic null
            // columns above the split have no sidecar include index.
            return columnIndex < columnSplit ? baseFrame.getCoveredIncludeIndex(columnIndex) : -1;
        }

        @Override
        public int[] getCoveredIncludeIndices() {
            // Per-frame set of sidecar columns to decode -- pass through to base.
            return baseFrame.getCoveredIncludeIndices();
        }

        @Override
        public int getCoveredKey() {
            // Per-frame resolved WHERE symbol key -- pass through to base.
            return baseFrame.getCoveredKey();
        }

        @Override
        public long getCoveredRowHi() {
            // Per-frame base row range -- pass through to base.
            return baseFrame.getCoveredRowHi();
        }

        @Override
        public long getCoveredRowLo() {
            // Per-frame base row range -- pass through to base.
            return baseFrame.getCoveredRowLo();
        }

        @Override
        public byte getFormat() {
            return baseFrame.getFormat();
        }

        @Override
        public IndexReader getIndexReader(int columnIndex, int direction) {
            return columnIndex < columnSplit ? baseFrame.getIndexReader(columnIndex, direction) : null;
        }

        @Override
        public long getIndexRowHi() {
            return baseFrame.getIndexRowHi();
        }

        @Override
        public long getIndexRowLo() {
            return baseFrame.getIndexRowLo();
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
        public ParquetDecoder getParquetDecoder() {
            return baseFrame.getParquetDecoder();
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

    /**
     * Pads the base page-frame cursor with synthetic null columns above the split. Claims only
     * the plain {@link PageFrameCursor} surface, so it can sit over any base (e.g.
     * read_parquet()). When the base cursor is a {@link TablePageFrameCursor}, the factory hands
     * out {@link ExtraNullColumnTablePageFrameCursor} instead so parents that downcast to the
     * table surface keep working.
     */
    private static class ExtraNullColumnPageFrameCursor implements PageFrameCursor {
        private final int columnCount;
        private final ColumnMapping columnMapping = new ColumnMapping();
        private final int columnSplit;
        private final ExtraNullColumnPageFrame pageFrame;
        private PageFrameCursor baseCursor;

        private ExtraNullColumnPageFrameCursor(int columnSplit, int columnCount) {
            this.pageFrame = new ExtraNullColumnPageFrame(columnSplit, columnCount);
            this.columnCount = columnCount;
            this.columnSplit = columnSplit;
        }

        @Override
        public void calculateSize(RecordCursor.Counter counter) {
            baseCursor.calculateSize(counter);
        }

        @Override
        public void close() {
            baseCursor = Misc.free(baseCursor);
        }

        @Override
        public ColumnMapping getColumnMapping() {
            return columnMapping;
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
        public boolean hasActivePushdownFilter() {
            return baseCursor.hasActivePushdownFilter();
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

        @Override
        public void releaseOpenPartitions() {
            baseCursor.releaseOpenPartitions();
        }

        @Override
        public void setScanProfile(ReaderScanProfile profile) {
            baseCursor.setScanProfile(profile);
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

        private ExtraNullColumnPageFrameCursor wrap(PageFrameCursor baseCursor) {
            this.baseCursor = baseCursor;
            // The mapping must stay parallel with this cursor's metadata: every consumer
            // (PageFrameAddressCache, PageFrameMemoryPool.resolveParquetColumn,
            // SelectedPageFrameCursor.wrap) indexes it by query column. The base only
            // maps its own columnSplit columns, so the synthetic null columns above the
            // split need entries of their own. They belong to no reader, writer or
            // parquet column, so all three indexes are -1: columnIdToParquetIdx keys are
            // never negative, which leaves the column undecoded and its page address 0
            // -- NULL, matching what the record and page frame report for them.
            columnMapping.copyFrom(baseCursor.getColumnMapping());
            assert columnMapping.getColumnCount() == columnSplit
                    : "base column mapping must cover exactly the split columns";
            for (int i = columnMapping.getColumnCount(); i < columnCount; i++) {
                columnMapping.addColumn(-1, -1, -1);
            }
            return this;
        }
    }

    /**
     * The null-padding wrapper over a table base: extends the plain wrapper with the
     * {@link TablePageFrameCursor} surface, delegating the table-specific methods to the typed
     * base cursor. The factory hands this wrapper out only when the base cursor is a
     * {@link TablePageFrameCursor}, so no cast can fail.
     */
    private static final class ExtraNullColumnTablePageFrameCursor extends ExtraNullColumnPageFrameCursor implements TablePageFrameCursor {
        private TablePageFrameCursor tableBaseCursor;

        private ExtraNullColumnTablePageFrameCursor(int columnSplit, int columnCount) {
            super(columnSplit, columnCount);
        }

        @Override
        public void close() {
            tableBaseCursor = null;
            super.close();
        }

        @Override
        public TableReader getTableReader() {
            return tableBaseCursor.getTableReader();
        }

        @Override
        public boolean hasIntervalFilter() {
            return tableBaseCursor.hasIntervalFilter();
        }

        // This wrapper is initialized via wrap(TablePageFrameCursor), not via of(PartitionFrameCursor, ...).
        // The base factory's getPageFrameCursor() handles partition-level initialization internally,
        // then we wrap the already-initialized result.
        @Override
        public TablePageFrameCursor of(SqlExecutionContext executionContext, PartitionFrameCursor partitionFrameCursor) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void toPartition(int partitionIndex) {
            tableBaseCursor.toPartition(partitionIndex);
        }

        private ExtraNullColumnTablePageFrameCursor wrap(TablePageFrameCursor baseCursor) {
            this.tableBaseCursor = baseCursor;
            super.wrap(baseCursor);
            return this;
        }
    }

    public static final class ExtraNullColumnTimeFrameCursor implements TimeFrameCursor {
        private final int columnSplit;
        private final ExtraNullColumnRecord recordA;
        private final ExtraNullColumnRecord recordB;
        private final int selectedTimestampIndex;
        private TimeFrameCursor baseCursor;

        public ExtraNullColumnTimeFrameCursor(int columnSplit, boolean supportsRandomAccess, int selectedTimestampIndex) {
            this.selectedTimestampIndex = selectedTimestampIndex;
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
        public IndexReader getIndexReaderForCurrentFrame(int columnIndex, int direction) {
            return columnIndex < columnSplit ? baseCursor.getIndexReaderForCurrentFrame(columnIndex, direction) : null;
        }

        @Override
        public long getIndexRowLoForCurrentFrame() {
            return baseCursor.getIndexRowLoForCurrentFrame();
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
            return selectedTimestampIndex;
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
        public void recordAt(Record record, int frameIndex, long rowIndex) {
            record = ((ExtraNullColumnRecord) record).getBaseRecord();
            baseCursor.recordAt(record, frameIndex, rowIndex);
        }

        @Override
        public void recordAtRowIndex(Record record, long rowIndex) {
            record = ((ExtraNullColumnRecord) record).getBaseRecord();
            baseCursor.recordAtRowIndex(record, rowIndex);
        }

        @Override
        public void seekEstimate(long timestamp) {
            baseCursor.seekEstimate(timestamp);
        }

        @Override
        public void setParquetDecodeHint(ParquetDecodeHint hint) {
            baseCursor.setParquetDecodeHint(hint);
        }

        @Override
        public void toTop() {
            baseCursor.toTop();
        }
    }
}
