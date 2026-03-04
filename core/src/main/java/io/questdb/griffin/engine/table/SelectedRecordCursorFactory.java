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
import io.questdb.cairo.BitmapIndexReader;
import io.questdb.cairo.TableReader;
import io.questdb.cairo.TableToken;
import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.PageFrame;
import io.questdb.cairo.sql.PageFrameAddressCache;
import io.questdb.cairo.sql.PageFrameCursor;
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
import io.questdb.griffin.engine.table.parquet.PartitionDecoder;
import io.questdb.jit.CompiledFilter;
import io.questdb.std.DirectIntList;
import io.questdb.std.IntList;
import io.questdb.std.LongList;
import io.questdb.std.Misc;
import io.questdb.std.ObjList;
import org.jetbrains.annotations.Nullable;

public final class SelectedRecordCursorFactory extends AbstractRecordCursorFactory {
    private final RecordCursorFactory base;
    private final IntList columnCrossIndex;
    private final boolean crossedIndex;
    private final SelectedRecordCursor cursor;
    private SelectedPageFrameCursor pageFrameCursor;
    private SelectedTimeFrameCursor timeFrameCursor;

    public SelectedRecordCursorFactory(RecordMetadata metadata, IntList columnCrossIndex, RecordCursorFactory base) {
        super(metadata);
        this.base = base;
        this.columnCrossIndex = columnCrossIndex;
        this.cursor = new SelectedRecordCursor(columnCrossIndex, base.recordCursorSupportsRandomAccess());
        this.crossedIndex = isCrossedIndex(columnCrossIndex);
    }

    public static boolean isCrossedIndex(IntList columnCrossIndex) {
        for (int i = 0, n = columnCrossIndex.size(); i < n; i++) {
            if (columnCrossIndex.get(i) != i) {
                return true;
            }
        }
        return false;
    }

    @Override
    public boolean followedOrderByAdvice() {
        return base.followedOrderByAdvice();
    }

    @Override
    public RecordCursorFactory getBaseFactory() {
        return base;
    }

    // to be used in combination with compiled filter
    @Nullable
    public ObjList<Function> getBindVarFunctions() {
        return base.getBindVarFunctions();
    }

    // to be used in combination with compiled filter
    @Nullable
    public MemoryCARW getBindVarMemory() {
        return base.getBindVarMemory();
    }

    @Override
    public IntList getColumnCrossIndex() {
        return columnCrossIndex;
    }

    @Override
    public CompiledFilter getCompiledFilter() {
        return base.getCompiledFilter();
    }

    @Override
    public RecordCursor getCursor(SqlExecutionContext executionContext) throws SqlException {
        final RecordCursor baseCursor = base.getCursor(executionContext);
        if (!crossedIndex) {
            return baseCursor;
        }
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
        if (baseCursor == null || !crossedIndex) {
            return baseCursor;
        }
        if (pageFrameCursor == null) {
            pageFrameCursor = new SelectedPageFrameCursor(columnCrossIndex);
        }
        return pageFrameCursor.wrap((TablePageFrameCursor) baseCursor);
    }

    @Override
    public int getScanDirection() {
        return base.getScanDirection();
    }

    @Override
    public TimeFrameCursor getTimeFrameCursor(SqlExecutionContext executionContext) throws SqlException {
        TimeFrameCursor baseCursor = base.getTimeFrameCursor(executionContext);
        if (baseCursor == null || !crossedIndex) {
            return baseCursor;
        }
        if (timeFrameCursor == null) {
            timeFrameCursor = new SelectedTimeFrameCursor(columnCrossIndex, base.recordCursorSupportsRandomAccess(), getMetadata().getTimestampIndex());
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
        if (baseCursor == null || !crossedIndex) {
            return baseCursor;
        }
        return new SelectedConcurrentTimeFrameCursor(baseCursor, getMetadata().getTimestampIndex());
    }

    @Override
    public boolean recordCursorSupportsLongTopK(int columnIndex) {
        return base.recordCursorSupportsLongTopK(columnCrossIndex.getQuick(columnIndex));
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
        sink.type("SelectedRecord");
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

    // This wrapper handles column remapping for ConcurrentTimeFrameCursor when a
    // SelectedRecordCursorFactory wraps another factory. Column remapping for symbol
    // tables and page frame data is handled by SelectedPageFrameCursor (which wraps
    // the delegate's frameCursor). This wrapper only needs to:
    // 1. Override getTimestampIndex() to return the selected timestamp index.
    // 2. Pass the selected timestamp index to the delegate via of() so that
    //    ConcurrentTimeFrameCursorImpl.open() reads timestamps from the correct
    //    position in the logically-remapped address cache.
    // Record data access works correctly without wrapping because the address cache
    // is populated with SelectedPageFrame data (logically-indexed), and the join code
    // accesses it using logical column indices from the selected metadata.
    static final class SelectedConcurrentTimeFrameCursor implements ConcurrentTimeFrameCursor {
        private final ConcurrentTimeFrameCursor delegate;
        private final int selectedTimestampIndex;

        SelectedConcurrentTimeFrameCursor(
                ConcurrentTimeFrameCursor delegate,
                int selectedTimestampIndex
        ) {
            this.delegate = delegate;
            this.selectedTimestampIndex = selectedTimestampIndex;
        }

        @Override
        public void close() {
            delegate.close();
        }

        @Override
        public Record getRecord() {
            return delegate.getRecord();
        }

        @Override
        public StaticSymbolTable getSymbolTable(int columnIndex) {
            return delegate.getSymbolTable(columnIndex);
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
            return delegate.newSymbolTable(columnIndex);
        }

        @Override
        public boolean next() {
            return delegate.next();
        }

        @Override
        public ConcurrentTimeFrameCursor of(
                TablePageFrameCursor frameCursor,
                PageFrameAddressCache frameAddressCache,
                DirectIntList framePartitionIndexes,
                LongList frameRowCounts,
                LongList partitionTimestamps,
                LongList partitionCeilings,
                int frameCount,
                int timestampIndex
        ) {
            delegate.of(frameCursor, frameAddressCache, framePartitionIndexes, frameRowCounts, partitionTimestamps, partitionCeilings, frameCount, selectedTimestampIndex);
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
            delegate.recordAt(record, rowId);
        }

        @Override
        public void recordAt(Record record, int frameIndex, long rowIndex) {
            delegate.recordAt(record, frameIndex, rowIndex);
        }

        @Override
        public void recordAtRowIndex(Record record, long rowIndex) {
            delegate.recordAtRowIndex(record, rowIndex);
        }

        @Override
        public void seekEstimate(long timestamp) {
            delegate.seekEstimate(timestamp);
        }

        @Override
        public void toTop() {
            delegate.toTop();
        }
    }

    private static class SelectedPageFrame implements PageFrame {
        private final IntList columnCrossIndex;
        private PageFrame baseFrame;

        private SelectedPageFrame(IntList columnCrossIndex) {
            this.columnCrossIndex = columnCrossIndex;
        }

        @Override
        public long getAuxPageAddress(int columnIndex) {
            return baseFrame.getAuxPageAddress(columnCrossIndex.getQuick(columnIndex));
        }

        @Override
        public long getAuxPageSize(int columnIndex) {
            return baseFrame.getAuxPageSize(columnCrossIndex.getQuick(columnIndex));
        }

        @Override
        public BitmapIndexReader getBitmapIndexReader(int columnIndex, int direction) {
            return baseFrame.getBitmapIndexReader(columnCrossIndex.getQuick(columnIndex), direction);
        }

        @Override
        public int getColumnCount() {
            return columnCrossIndex.size();
        }

        @Override
        public byte getFormat() {
            return baseFrame.getFormat();
        }

        @Override
        public long getPageAddress(int columnIndex) {
            return baseFrame.getPageAddress(columnCrossIndex.getQuick(columnIndex));
        }

        @Override
        public long getPageSize(int columnIndex) {
            return baseFrame.getPageSize(columnCrossIndex.getQuick(columnIndex));
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

        public SelectedPageFrame of(PageFrame basePageFrame) {
            this.baseFrame = basePageFrame;
            return this;
        }
    }

    private static class SelectedPageFrameCursor implements TablePageFrameCursor {
        private final IntList columnCrossIndex;
        private final SelectedPageFrame pageFrame;
        private TablePageFrameCursor baseCursor;

        private SelectedPageFrameCursor(IntList columnCrossIndex) {
            this.columnCrossIndex = columnCrossIndex;
            this.pageFrame = new SelectedPageFrame(columnCrossIndex);
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
            return baseCursor.getSymbolTable(columnCrossIndex.getQuick(columnIndex));
        }

        @Override
        public TableReader getTableReader() {
            return baseCursor.getTableReader();
        }

        @Override
        public boolean isExternal() {
            return baseCursor.isExternal();
        }

        @Override
        public SymbolTable newSymbolTable(int columnIndex) {
            return baseCursor.newSymbolTable(columnCrossIndex.getQuick(columnIndex));
        }

        @Override
        public @Nullable PageFrame next(long skipTarget) {
            PageFrame baseFrame = baseCursor.next(skipTarget);
            return baseFrame != null ? pageFrame.of(baseFrame) : null;
        }

        // This wrapper is initialized via wrap(TablePageFrameCursor), not via of(PartitionFrameCursor, ...).
        // The base factory's getPageFrameCursor() handles partition-level initialization internally,
        // then we wrap the already-initialized result.
        @Override
        public TablePageFrameCursor of(PartitionFrameCursor partitionFrameCursor, int pageFrameMinRows, int pageFrameMaxRows) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void setStreamingMode(boolean enabled) {
            baseCursor.setStreamingMode(enabled);
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

        public SelectedPageFrameCursor wrap(TablePageFrameCursor baseCursor) {
            this.baseCursor = baseCursor;
            return this;
        }
    }

    public static final class SelectedTimeFrameCursor implements TimeFrameCursor {
        private final IntList columnCrossIndex;
        private final SelectedRecord recordA;
        private final SelectedRecord recordB;
        private final int selectedTimestampIndex;
        private TimeFrameCursor baseCursor;

        public SelectedTimeFrameCursor(IntList columnCrossIndex, boolean supportsRandomAccess, int selectedTimestampIndex) {
            this.columnCrossIndex = columnCrossIndex;
            this.selectedTimestampIndex = selectedTimestampIndex;
            this.recordA = new SelectedRecord(columnCrossIndex);
            if (supportsRandomAccess) {
                this.recordB = new SelectedRecord(columnCrossIndex);
            } else {
                this.recordB = null;
            }
        }

        @Override
        public void close() {
            baseCursor = Misc.free(baseCursor);
        }

        @Override
        public BitmapIndexReader getIndexReaderForCurrentFrame(int columnIndex, int direction) {
            return baseCursor.getIndexReaderForCurrentFrame(columnCrossIndex.getQuick(columnIndex), direction);
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
            return baseCursor.getSymbolTable(columnCrossIndex.getQuick(columnIndex));
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
            return baseCursor.newSymbolTable(columnCrossIndex.getQuick(columnIndex));
        }

        @Override
        public boolean next() {
            return baseCursor.next();
        }

        public SelectedTimeFrameCursor of(TimeFrameCursor baseCursor) {
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
            record = ((SelectedRecord) record).getBaseRecord();
            baseCursor.recordAt(record, rowId);
        }

        @Override
        public void recordAt(Record record, int frameIndex, long rowIndex) {
            record = ((SelectedRecord) record).getBaseRecord();
            baseCursor.recordAt(record, frameIndex, rowIndex);
        }

        @Override
        public void recordAtRowIndex(Record record, long rowIndex) {
            record = ((SelectedRecord) record).getBaseRecord();
            baseCursor.recordAtRowIndex(record, rowIndex);
        }

        @Override
        public void seekEstimate(long timestamp) {
            baseCursor.seekEstimate(timestamp);
        }

        @Override
        public void toTop() {
            baseCursor.toTop();
        }
    }
}
