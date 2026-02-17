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

package io.questdb.cairo.wal;

import io.questdb.cairo.vm.Vm;
import io.questdb.cairo.vm.api.MemoryARW;
import io.questdb.cairo.vm.api.MemoryCR;
import io.questdb.cairo.vm.api.NullMemory;

import static io.questdb.cairo.vm.Vm.STRING_LENGTH_BYTES;

public class SymbolMapDiffImpl implements SymbolMapDiff {
    public static final int END_OF_SYMBOL_DIFFS = -1;
    public static final int END_OF_SYMBOL_ENTRIES = -1;

    private final WalEventCursor cursor;
    private final Entry entry = new Entry();
    private int cleanSymbolCount;
    private int columnIndex = -1;
    private boolean nullFlag;
    private int recordCount;

    SymbolMapDiffImpl(WalEventCursor cursor) {
        this.cursor = cursor;
    }

    @Override
    public void drain() {
        cursor.drain();
    }

    @Override
    public int getCleanSymbolCount() {
        return cleanSymbolCount;
    }

    @Override
    public int getColumnIndex() {
        return columnIndex;
    }

    @Override
    public int getRecordCount() {
        return recordCount;
    }

    @Override
    public boolean hasNullValue() {
        return nullFlag;
    }

    @Override
    public SymbolMapDiffEntry nextEntry() {
        return cursor.readNextSymbolMapDiffEntry(entry);
    }

    void of(int columnIndex, int cleanSymbolCount, int size, boolean nullFlag) {
        this.columnIndex = columnIndex;
        this.cleanSymbolCount = cleanSymbolCount;
        this.recordCount = size;
        this.nullFlag = nullFlag;
        entry.clear();
    }

    public static class Entry implements SymbolMapDiffEntry {
        private int key;
        private MemoryCR memoryR;
        private long symbolOffset;

        @Override
        public void appendSymbolTo(MemoryARW symbolMem) {
            int len = memoryR.getInt(symbolOffset);
            symbolMem.putBlockOfBytes(memoryR.addressOf(symbolOffset), len < 0 ? STRING_LENGTH_BYTES : Vm.getStorageLength(len));
        }

        @Override
        public int getKey() {
            return key;
        }

        @Override
        public CharSequence getSymbol() {
            return memoryR.getStrA(symbolOffset);
        }

        void clear() {
            of(-1, 0, NullMemory.INSTANCE);
        }

        void of(int key, long symbolOffset, MemoryCR symbolMem) {
            this.key = key;
            this.symbolOffset = symbolOffset;
            this.memoryR = symbolMem;
        }
    }
}
