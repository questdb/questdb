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

import io.questdb.cairo.TableUtils;
import io.questdb.cairo.idx.BitmapIndexFwdReader;
import io.questdb.cairo.idx.IndexReader;
import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.PageFrame;
import io.questdb.cairo.sql.PageFrameMemory;
import io.questdb.cairo.sql.RowCursor;
import io.questdb.cairo.sql.SymbolTable;
import io.questdb.griffin.PlanSink;

public class SymbolIndexRowCursorFactory implements SymbolFunctionRowCursorFactory {
    private final int columnIndex;
    private final int indexDirection;
    private final ShiftedRowCursor shiftedCursor = new ShiftedRowCursor();
    private final Function symbolFunction;
    private int symbolKey;

    public SymbolIndexRowCursorFactory(
            int columnIndex,
            int symbolKey,
            int indexDirection,
            Function symbolFunction
    ) {
        this.columnIndex = columnIndex;
        this.symbolKey = TableUtils.toIndexKey(symbolKey);
        this.indexDirection = indexDirection;
        this.symbolFunction = symbolFunction;
    }

    @Override
    public RowCursor getCursor(PageFrame pageFrame, PageFrameMemory pageFrameMemory) {
        return pageFrame
                .getIndexReader(columnIndex, indexDirection)
                .getCursor(symbolKey, pageFrame.getIndexRowLo(), pageFrame.getIndexRowHi() - 1);
    }

    @Override
    public RowCursor getCursor(PageFrame pageFrame, PageFrameMemory pageFrameMemory, long firstRow) {
        final IndexReader reader = pageFrame.getIndexReader(columnIndex, indexDirection);
        final long indexShift = pageFrame.getIndexRowLo() - pageFrame.getPartitionLo();
        if (indexDirection == IndexReader.DIR_BACKWARD) {
            return reader.getCursor(symbolKey, pageFrame.getIndexRowLo(), firstRow + indexShift);
        }
        return shiftedCursor.of(reader.getCursor(symbolKey, firstRow + indexShift, pageFrame.getIndexRowHi() - 1), firstRow - pageFrame.getPartitionLo());
    }

    @Override
    public Function getFunction() {
        return symbolFunction;
    }

    @Override
    public long getRow(PageFrame pageFrame, long rowLo, long rowHi, long rowIndex) {
        final BitmapIndexFwdReader reader = (BitmapIndexFwdReader) pageFrame.getIndexReader(columnIndex, IndexReader.DIR_FORWARD);
        final long indexShift = pageFrame.getIndexRowLo() - pageFrame.getPartitionLo();
        return reader.getRow(symbolKey, rowLo + indexShift, rowHi + indexShift, indexDirection, rowIndex) - indexShift;
    }

    @Override
    public long getRowCount(PageFrame pageFrame, long rowLo, long rowHi, long maxCount) {
        if (!(pageFrame.getIndexReader(columnIndex, IndexReader.DIR_FORWARD) instanceof BitmapIndexFwdReader reader)) {
            return -1;
        }
        final long indexShift = pageFrame.getIndexRowLo() - pageFrame.getPartitionLo();
        return reader.countRows(symbolKey, rowLo + indexShift, rowHi + indexShift, maxCount);
    }

    public int getSymbolKey() {
        return symbolKey == 0 ? SymbolTable.VALUE_IS_NULL : symbolKey - 1;
    }

    @Override
    public boolean isEntity() {
        return false;
    }

    @Override
    public boolean isUsingIndex() {
        return true;
    }

    @Override
    public void of(int symbolKey) {
        this.symbolKey = TableUtils.toIndexKey(symbolKey);
    }

    @Override
    public void toPlan(PlanSink sink) {
        sink.type("Index ").type(IndexReader.nameOf(indexDirection)).type(" scan").meta("on").putBaseColumnName(columnIndex);
        sink.attr("filter").putBaseColumnName(columnIndex).val('=').val(symbolKey);
    }
}
