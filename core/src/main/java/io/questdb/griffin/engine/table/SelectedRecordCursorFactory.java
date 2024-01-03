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

import io.questdb.cairo.AbstractRecordCursorFactory;
import io.questdb.cairo.BitmapIndexReader;
import io.questdb.cairo.TableToken;
import io.questdb.cairo.sql.*;
import io.questdb.griffin.PlanSink;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.std.IntList;
import org.jetbrains.annotations.Nullable;

public class SelectedRecordCursorFactory extends AbstractRecordCursorFactory {

    private final RecordCursorFactory base;
    private final IntList columnCrossIndex;
    private final SelectedRecordCursor cursor;
    private SelectedPageFrameCursor pageFrameCursor;

    public SelectedRecordCursorFactory(RecordMetadata metadata, IntList columnCrossIndex, RecordCursorFactory base) {
        super(metadata);
        this.base = base;
        this.columnCrossIndex = columnCrossIndex;
        this.cursor = new SelectedRecordCursor(columnCrossIndex, base.recordCursorSupportsRandomAccess());
    }

    @Override
    public boolean followedLimitAdvice() {
        return base.followedLimitAdvice();
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
        cursor.of(base.getCursor(executionContext));
        return cursor;
    }

    @Override
    public PageFrameCursor getPageFrameCursor(SqlExecutionContext executionContext, int order) throws SqlException {
        PageFrameCursor baseCursor = base.getPageFrameCursor(executionContext, order);
        if (baseCursor == null) {
            return null;
        }
        if (pageFrameCursor == null) {
            pageFrameCursor = new SelectedPageFrameCursor(columnCrossIndex);
        }
        return pageFrameCursor.of(baseCursor);
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
    public boolean recordCursorSupportsRandomAccess() {
        return base.recordCursorSupportsRandomAccess();
    }

    @Override
    public boolean supportPageFrameCursor() {
        return base.supportPageFrameCursor();
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

    private static class SelectedPageFrame implements PageFrame {
        private final IntList columnCrossIndex;
        private PageFrame baseFrame;

        private SelectedPageFrame(IntList columnCrossIndex) {
            this.columnCrossIndex = columnCrossIndex;
        }

        @Override
        public BitmapIndexReader getBitmapIndexReader(int columnIndex, int dirForward) {
            return baseFrame.getBitmapIndexReader(columnCrossIndex.getQuick(columnIndex), dirForward);
        }

        @Override
        public int getColumnShiftBits(int columnIndex) {
            return baseFrame.getColumnShiftBits(columnCrossIndex.getQuick(columnIndex));
        }

        @Override
        public long getIndexPageAddress(int columnIndex) {
            return baseFrame.getIndexPageAddress(columnCrossIndex.getQuick(columnIndex));
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

    private static class SelectedPageFrameCursor implements PageFrameCursor {
        private final IntList columnCrossIndex;
        private final SelectedPageFrame pageFrame;
        private PageFrameCursor baseCursor;

        private SelectedPageFrameCursor(IntList columnCrossIndex) {
            this.columnCrossIndex = columnCrossIndex;
            this.pageFrame = new SelectedPageFrame(columnCrossIndex);
        }

        @Override
        public void close() {
            baseCursor.close();
        }

        @Override
        public SymbolTable getSymbolTable(int columnIndex) {
            return baseCursor.getSymbolTable(columnCrossIndex.getQuick(columnIndex));
        }

        @Override
        public long getUpdateRowId(long rowIndex) {
            return baseCursor.getUpdateRowId(rowIndex);
        }

        @Override
        public SymbolTable newSymbolTable(int columnIndex) {
            return baseCursor.newSymbolTable(columnCrossIndex.getQuick(columnIndex));
        }

        @Override
        public @Nullable PageFrame next() {
            PageFrame baseFrame = baseCursor.next();
            return baseFrame != null ? pageFrame.of(baseFrame) : null;
        }

        public SelectedPageFrameCursor of(PageFrameCursor baseCursor) {
            this.baseCursor = baseCursor;
            return this;
        }

        @Override
        public long size() {
            return baseCursor.size();
        }

        @Override
        public void toTop() {
            baseCursor.toTop();
        }
    }
}
