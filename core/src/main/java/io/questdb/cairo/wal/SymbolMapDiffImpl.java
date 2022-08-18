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

package io.questdb.cairo.wal;

public class SymbolMapDiffImpl implements SymbolMapDiff {
    public static final int END_OF_SYMBOL_DIFFS = -1;
    public static final int END_OF_SYMBOL_ENTRIES = -1;

    private final WalEventCursor cursor;
    private final Entry entry = new Entry();

    private int columnIndex = -1;
    private int cleanSymbolCount;
    private int size;

    SymbolMapDiffImpl(WalEventCursor cursor) {
        this.cursor = cursor;
    }

    void of(int columnIndex, int cleanSymbolCount, int size) {
        this.columnIndex = columnIndex;
        this.cleanSymbolCount = cleanSymbolCount;
        this.size = size;
        entry.clear();
    }

    @Override
    public int getColumnIndex() {
        return columnIndex;
    }

    @Override
    public int getCleanSymbolCount() {
        return cleanSymbolCount;
    }

    @Override
    public int getSize() {
        return size;
    }

    @Override
    public SymbolMapDiffEntry nextEntry() {
        return cursor.readNextSymbolMapDiffEntry(entry);
    }

    public static class Entry implements SymbolMapDiffEntry {
        private int key;
        private CharSequence symbol;

        void of(int key, CharSequence symbol) {
            this.key = key;
            this.symbol = symbol;
        }

        @Override
        public int getKey() {
            return key;
        }

        @Override
        public CharSequence getSymbol() {
            return symbol;
        }

        void clear() {
            of(-1, null);
        }
    }
}
