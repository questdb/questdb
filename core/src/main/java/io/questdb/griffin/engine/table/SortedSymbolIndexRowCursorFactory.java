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

import io.questdb.cairo.EmptyRowCursor;
import io.questdb.cairo.TableUtils;
import io.questdb.cairo.idx.BitmapIndexFwdReader;
import io.questdb.cairo.idx.IndexReader;
import io.questdb.cairo.sql.PageFrame;
import io.questdb.cairo.sql.PageFrameCursor;
import io.questdb.cairo.sql.PageFrameMemory;
import io.questdb.cairo.sql.RowCursor;
import io.questdb.cairo.sql.StaticSymbolTable;
import io.questdb.cairo.sql.SymbolTable;
import io.questdb.griffin.PlanSink;
import io.questdb.std.CarrierLocal;
import io.questdb.std.Chars;
import io.questdb.std.IntList;
import io.questdb.std.Misc;
import io.questdb.std.ObjList;

import java.util.Comparator;

public class SortedSymbolIndexRowCursorFactory implements KeyOrderedRowCursorFactory {
    private final static CarrierLocal<SortHelper> TL_SORT_HELPER = new CarrierLocal<>(SortHelper::new);
    private final int columnIndex;
    private final boolean columnOrderDirectionAsc;
    private final ListBasedSymbolIndexRowCursor cursor = new ListBasedSymbolIndexRowCursor();
    private final int indexDirection;
    private final IntList symbolKeys = new IntList();
    private int symbolKeyLimit;

    public SortedSymbolIndexRowCursorFactory(
            int columnIndex,
            boolean columnOrderDirectionAsc,
            int indexDirection
    ) {
        this.columnIndex = columnIndex;
        this.indexDirection = indexDirection;
        this.columnOrderDirectionAsc = columnOrderDirectionAsc;
    }

    @Override
    public int getColumnIndex() {
        return columnIndex;
    }

    @Override
    public RowCursor getCursor(PageFrame pageFrame, PageFrameMemory pageFrameMemory) {
        return getCursor(pageFrame, pageFrameMemory, 0, symbolKeyLimit);
    }

    @Override
    public RowCursor getCursor(PageFrame pageFrame, PageFrameMemory pageFrameMemory, int keyLo, int keyHi) {
        return getCursor(pageFrame, pageFrameMemory, keyLo, keyHi, indexDirection == IndexReader.DIR_BACKWARD ? pageFrame.getPartitionHi() - 1 : pageFrame.getPartitionLo());
    }

    @Override
    public RowCursor getCursor(PageFrame pageFrame, PageFrameMemory pageFrameMemory, int keyLo, int keyHi, long firstRow) {
        cursor.of(pageFrame, keyLo, keyHi, firstRow);
        return cursor;
    }

    @Override
    public int getIndexDirection() {
        return indexDirection;
    }

    @Override
    public int getKeyCount() {
        return symbolKeyLimit;
    }

    @Override
    public long getRow(PageFrame pageFrame, int keyIndex, long rowLo, long rowHi, long rowIndex) {
        final BitmapIndexFwdReader reader = (BitmapIndexFwdReader) pageFrame.getIndexReader(columnIndex, IndexReader.DIR_FORWARD);
        final long indexShift = pageFrame.getIndexRowLo() - pageFrame.getPartitionLo();
        return reader.getRow(symbolKeys.getQuick(keyIndex), rowLo + indexShift, rowHi + indexShift, indexDirection, rowIndex) - indexShift;
    }

    @Override
    public long getRowCount(PageFrame pageFrame, int keyLo, int keyHi, long rowLo, long rowHi, long maxCount) {
        if (!(pageFrame.getIndexReader(columnIndex, IndexReader.DIR_FORWARD) instanceof BitmapIndexFwdReader reader)) {
            return -1;
        }
        final long indexShift = pageFrame.getIndexRowLo() - pageFrame.getPartitionLo();
        long count = 0;
        for (int i = keyLo; i < keyHi && count <= maxCount; i++) {
            count += reader.countRows(symbolKeys.getQuick(i), rowLo + indexShift, rowHi + indexShift, maxCount - count);
        }
        return count;
    }

    @Override
    public int getSymbolKey(int keyIndex) {
        final int key = symbolKeys.getQuick(keyIndex);
        return key == 0 ? SymbolTable.VALUE_IS_NULL : key - 1;
    }

    @Override
    public boolean isEntity() {
        return false;
    }

    @Override
    public boolean isReadingEveryKey() {
        return true;
    }

    @Override
    public void prepareCursor(PageFrameCursor pageFrameCursor) {
        symbolKeys.clear();

        final StaticSymbolTable staticSymbolTable = pageFrameCursor.getSymbolTable(columnIndex);
        int count = staticSymbolTable.getSymbolCount();

        final SortHelper sortHelper = TL_SORT_HELPER.get();
        final ObjList<SymbolTableEntry> entries = sortHelper.getEntries();
        symbolKeyLimit = count + 1;

        sortHelper.fillEntries(symbolKeyLimit);

        for (int i = 0; i < count; i++) {
            final SymbolTableEntry e = entries.getQuick(i);
            e.key = TableUtils.toIndexKey(i);
            e.value = Chars.toString(staticSymbolTable.valueOf(i));
        }

        // add NULL
        final SymbolTableEntry e = entries.getQuick(count);
        e.key = TableUtils.toIndexKey(SymbolTable.VALUE_IS_NULL);
        e.value = null;

        if (columnOrderDirectionAsc) {
            sortHelper.sort(symbolKeyLimit, sortHelper.ascComparator);
        } else {
            sortHelper.sort(symbolKeyLimit, sortHelper.dscComparator);
        }
        // populate our list
        for (int i = 0; i < symbolKeyLimit; i++) {
            symbolKeys.add(entries.getQuick(i).key);
        }
    }

    @Override
    public void toPlan(PlanSink sink) {
        sink.type("Index ").type(IndexReader.nameOf(indexDirection)).type(" scan").meta("on").putColumnName(columnIndex);
        sink.attr("symbolOrder").val(columnOrderDirectionAsc ? "asc" : "desc");
    }

    private static class SortHelper {
        private final Comparator<SymbolTableEntry> ascComparator = this::compareAsc;
        private final Comparator<SymbolTableEntry> dscComparator = this::compareDesc;
        private final ObjList<SymbolTableEntry> entries = new ObjList<>();

        public void fillEntries(int max) {
            int size = entries.size();
            if (max > entries.size()) {
                while (size++ < max) {
                    entries.add(new SymbolTableEntry());
                }
            }
        }

        public ObjList<SymbolTableEntry> getEntries() {
            return entries;
        }

        public void sort(int max, Comparator<SymbolTableEntry> comparator) {
            entries.sort(0, max, comparator);
        }

        private int compareAsc(SymbolTableEntry e1, SymbolTableEntry e2) {
            return (e1.value == null && e2.value == null) ? 0
                    : e1.value == null ? -1
                      : e2.value == null ? 1
                        : e1.value.compareTo(e2.value);
        }

        private int compareDesc(SymbolTableEntry e1, SymbolTableEntry e2) {
            return (e1.value == null && e2.value == null) ? 0
                    : e1.value == null ? 1
                      : e2.value == null ? -1
                        : e2.value.compareTo(e1.value);
        }
    }

    // this is a thread-local contraption used for sorting symbol values. We ought to think of something better
    private static class SymbolTableEntry {
        private int key;
        private String value;
    }

    private class ListBasedSymbolIndexRowCursor implements RowCursor {
        private RowCursor current;
        private long firstRow;
        private int index;
        private int indexHi;
        private int indexLo;
        private PageFrame pageFrame;
        private long shift;

        @Override
        public void close() {
            current = Misc.free(current);
        }

        @Override
        public boolean hasNext() {
            return current.hasNext() || fetchNext();
        }

        @Override
        public long next() {
            return current.next() + shift;
        }

        private boolean fetchNext() {
            while (index < indexHi) {
                Misc.free(current);
                final long partitionLo = pageFrame.getPartitionLo();
                long rowLo = partitionLo;
                long rowHi = pageFrame.getPartitionHi() - 1;
                if (index == indexLo) {
                    if (indexDirection == IndexReader.DIR_BACKWARD) {
                        rowHi = firstRow;
                    } else {
                        rowLo = firstRow;
                    }
                }
                shift = rowLo - partitionLo;
                final long indexShift = pageFrame.getIndexRowLo() - partitionLo;
                current = pageFrame
                        .getIndexReader(columnIndex, indexDirection)
                        .getCursor(symbolKeys.getQuick(index++), rowLo + indexShift, rowHi + indexShift);

                if (current.hasNext()) {
                    return true;
                }
            }
            return false;
        }

        private void of(PageFrame pageFrame, int indexLo, int indexHi, long firstRow) {
            this.pageFrame = pageFrame;
            this.index = indexLo;
            this.indexLo = indexLo;
            this.indexHi = indexHi;
            this.firstRow = firstRow;
            Misc.free(current);
            this.current = EmptyRowCursor.INSTANCE;
        }
    }
}
