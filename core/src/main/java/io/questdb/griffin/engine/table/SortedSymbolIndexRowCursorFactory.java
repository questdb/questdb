/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2022 QuestDB
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
import io.questdb.cairo.TableReader;
import io.questdb.cairo.TableUtils;
import io.questdb.cairo.sql.*;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.std.Chars;
import io.questdb.std.IntList;
import io.questdb.std.ObjList;

import java.util.Comparator;

public class SortedSymbolIndexRowCursorFactory implements RowCursorFactory {
    private final static ThreadLocal<SortHelper> TL_SORT_HELPER = ThreadLocal.withInitial(SortHelper::new);
    private final int columnIndex;
    private final int indexDirection;
    private final IntList symbolKeys = new IntList();
    private final ListBasedSymbolIndexRowCursor cursor = new ListBasedSymbolIndexRowCursor();
    private final boolean columnOrderDirectionAsc;
    private int symbolKeyLimit;

    public SortedSymbolIndexRowCursorFactory(int columnIndex, boolean columnOrderDirectionAsc, int indexDirection) {
        this.columnIndex = columnIndex;
        this.indexDirection = indexDirection;
        this.columnOrderDirectionAsc = columnOrderDirectionAsc;
    }

    @Override
    public void prepareCursor(TableReader tableReader, SqlExecutionContext sqlExecutionContext) {
        symbolKeys.clear();

        final StaticSymbolTable staticSymbolTable = tableReader.getSymbolMapReader(columnIndex);
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
    public RowCursor getCursor(DataFrame dataFrame) {
        cursor.of(dataFrame);
        return cursor;
    }

    @Override
    public boolean isEntity() {
        return false;
    }

    // this is a thread-local contraption used for sorting symbol values. We ought to think of something better
    private static class SymbolTableEntry {
        private String value;
        private int key;
    }

    private static class SortHelper {
        private final ObjList<SymbolTableEntry> entries = new ObjList<>();
        private final Comparator<SymbolTableEntry> ascComparator = this::compareAsc;
        private final Comparator<SymbolTableEntry> dscComparator = this::compareDesc;

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

        private int compareAsc(SymbolTableEntry e1, SymbolTableEntry e2) {
            return (e1.value == null && e2.value == null) ? 0 : e1.value == null ? -1 : e1.value.compareTo(e2.value);
        }

        private int compareDesc(SymbolTableEntry e1, SymbolTableEntry e2) {
            return (e1.value == null && e2.value == null) ? 0 : e1.value == null ? 1 : e2.value.compareTo(e1.value);
        }

        public void sort(int max, Comparator<SymbolTableEntry> comparator) {
            entries.sort(0, max, comparator);
        }
    }

    private class ListBasedSymbolIndexRowCursor implements RowCursor {
        private int index;
        private RowCursor current;
        private DataFrame dataFrame;

        private void of(DataFrame dataFrame) {
            this.dataFrame = dataFrame;
            this.index = 0;
            this.current = EmptyRowCursor.INSTANCE;
        }

        @Override
        public boolean hasNext() {
            return current.hasNext() || fetchNext();
        }

        private boolean fetchNext() {
            while (index < symbolKeyLimit) {
                current = dataFrame
                        .getBitmapIndexReader(columnIndex, indexDirection)
                        .getCursor(
                                true,
                                symbolKeys.getQuick(index++),
                                dataFrame.getRowLo(),
                                dataFrame.getRowHi() - 1
                        );

                if (current.hasNext()) {
                    return true;
                }
            }
            return false;
        }

        @Override
        public long next() {
            return current.next();
        }
    }
}
