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

package io.questdb.cairo;

import io.questdb.cairo.vm.Vm;
import io.questdb.cairo.vm.api.MemoryMAR;
import io.questdb.std.*;
import io.questdb.std.str.Path;

import java.io.Closeable;

import static io.questdb.cairo.TableUtils.*;

class WalWriterEvents implements Closeable {
    private final MemoryMAR eventMem = Vm.getMARInstance();

    private IntList startSymbolCounts;
    private ObjList<MapWriter> symbolMapWriters;

    WalWriterEvents() {
    }

    void of(IntList startSymbolCounts, ObjList<MapWriter> symbolMapWriters) {
        this.startSymbolCounts = startSymbolCounts;
        this.symbolMapWriters = symbolMapWriters;
    }

    @Override
    public void close() {
        Misc.free(eventMem);
    }

    void openEventFile(FilesFacade ff, Path path, int pathLen) {
        openSmallFile(ff, path, pathLen, eventMem, EVENT_FILE_NAME, MemoryTag.MMAP_TABLE_WAL_WRITER);
        init();
    }

    private void writeSymbolMapDiffs() {
        for (int i = 0; i < startSymbolCounts.size(); i++) {
            final int startSymbolCount = startSymbolCounts.get(i);
            if (startSymbolCount > -1) {
                final MapWriter symbolMapWriter = symbolMapWriters.get(i);
                final int symbolCount = symbolMapWriter.getSymbolCount();
                if (symbolCount > startSymbolCount) {
                    eventMem.putInt(i);
                    for (int j = startSymbolCount; j < symbolCount; j++) {
                        eventMem.putInt(j);
                        eventMem.putStr(symbolMapWriter.valueOf(j));
                    }
                    eventMem.putInt(SymbolMapDiff.END_OF_SYMBOL_ENTRIES);
                    startSymbolCounts.setQuick(i, symbolCount);
                }
            }
        }
        eventMem.putInt(SymbolMapDiff.END_OF_SYMBOL_DIFFS);
    }

    void data(long txn, long startRowID, long endRowID, long minTimestamp, long maxTimestamp, boolean outOfOrder) {
        eventMem.putLong(txn);
        eventMem.putByte(WalTxnType.DATA);
        eventMem.putLong(startRowID);
        eventMem.putLong(endRowID);
        eventMem.putLong(minTimestamp);
        eventMem.putLong(maxTimestamp);
        eventMem.putBool(outOfOrder);
        writeSymbolMapDiffs();
    }

    void addColumn(long txn, int columnIndex, CharSequence columnName, int columnType) {
        eventMem.putLong(txn);
        eventMem.putByte(WalTxnType.ADD_COLUMN);
        eventMem.putInt(columnIndex);
        eventMem.putStr(columnName);
        eventMem.putInt(columnType);
    }

    void removeColumn(long txn, int columnIndex) {
        eventMem.putLong(txn);
        eventMem.putByte(WalTxnType.REMOVE_COLUMN);
        eventMem.putInt(columnIndex);
    }

    private void init() {
        eventMem.putInt(WalWriter.WAL_FORMAT_VERSION);
    }

    void end() {
        eventMem.putLong(WalEventCursor.END_OF_EVENTS);
    }
}
