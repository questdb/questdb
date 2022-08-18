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

import io.questdb.cairo.vm.Vm;
import io.questdb.cairo.vm.api.MemoryMARW;
import io.questdb.std.*;
import io.questdb.std.str.Path;

import java.io.Closeable;

import static io.questdb.cairo.TableUtils.*;
import static io.questdb.cairo.wal.WalUtils.WAL_FORMAT_VERSION;

class WalWriterEvents implements Closeable {
    private final FilesFacade ff;
    private final MemoryMARW eventMem = Vm.getMARWInstance();
    private ObjList<CharSequenceIntHashMap> txnSymbolMaps;
    private IntList initialSymbolCounts;
    private long txn = 0;

    WalWriterEvents(FilesFacade ff) {
        this.ff = ff;
    }

    void of(ObjList<CharSequenceIntHashMap> txnSymbolMaps, IntList initialSymbolCounts) {
        this.txnSymbolMaps = txnSymbolMaps;
        this.initialSymbolCounts = initialSymbolCounts;
    }

    @Override
    public void close() {
        Misc.free(eventMem);
    }

    void openEventFile(Path path, int pathLen) {
        openSmallFile(ff, path, pathLen, eventMem, EVENT_FILE_NAME, MemoryTag.MMAP_TABLE_WAL_WRITER);
        init();
    }

    private void writeSymbolMapDiffs() {
        int columns = txnSymbolMaps.size();
        for (int i = 0; i < columns; i++) {
            final CharSequenceIntHashMap symbolMap = txnSymbolMaps.getQuick(i);
            int initialCount = initialSymbolCounts.get(i);

            if (initialCount > 0 || (initialCount > -1L && symbolMap.size() > 0)) {
                eventMem.putInt(i);
                eventMem.putInt(initialCount);

                int size = symbolMap.size();
                eventMem.putInt(size);

                for (int j = 0; j < size; j++) {
                    CharSequence symbol = symbolMap.keys().getQuick(j);
                    int value = symbolMap.get(symbol);

                    eventMem.putInt(value);
                    eventMem.putStr(symbol);
                }

                eventMem.putInt(SymbolMapDiffImpl.END_OF_SYMBOL_ENTRIES);
                initialSymbolCounts.setQuick(i, initialCount + size);
                symbolMap.clear();
            }
        }
        eventMem.putInt(SymbolMapDiffImpl.END_OF_SYMBOL_DIFFS);
    }

    long data(long startRowID, long endRowID, long minTimestamp, long maxTimestamp, boolean outOfOrder) {
        long startOffset = eventMem.getAppendOffset() - Integer.BYTES;
        eventMem.putLong(txn);
        eventMem.putByte(WalTxnType.DATA);
        eventMem.putLong(startRowID);
        eventMem.putLong(endRowID);
        eventMem.putLong(minTimestamp);
        eventMem.putLong(maxTimestamp);
        eventMem.putBool(outOfOrder);
        writeSymbolMapDiffs();
        eventMem.putInt(startOffset, (int) (eventMem.getAppendOffset() - startOffset));
        eventMem.putInt(-1);
        return txn++;
    }

    long addColumn(int columnIndex, CharSequence columnName, int columnType) {
        long startOffset = eventMem.getAppendOffset() - Integer.BYTES;
        eventMem.putLong(txn);
        eventMem.putByte(WalTxnType.ADD_COLUMN);
        eventMem.putInt(columnIndex);
        eventMem.putStr(columnName);
        eventMem.putInt(columnType);
        eventMem.putInt(startOffset, (int) (eventMem.getAppendOffset() - startOffset));
        eventMem.putInt(-1);
        return txn++;
    }

    long removeColumn(int columnIndex) {
        long startOffset = eventMem.getAppendOffset() - Integer.BYTES;
        eventMem.putLong(txn);
        eventMem.putByte(WalTxnType.REMOVE_COLUMN);
        eventMem.putInt(columnIndex);
        eventMem.putInt(startOffset, (int) (eventMem.getAppendOffset() - startOffset));
        eventMem.putInt(-1);
        return txn++;
    }

    private void init() {
        eventMem.putInt(WAL_FORMAT_VERSION);
        eventMem.putInt(-1);
    }
}
