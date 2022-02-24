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

package io.questdb.cutlass.line.tcp;

import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.SymbolMapReaderImpl;
import io.questdb.cairo.TableUtils;
import io.questdb.cairo.sql.SymbolLookup;
import io.questdb.cairo.sql.SymbolTable;
import io.questdb.cairo.vm.Vm;
import io.questdb.cairo.vm.api.MemoryMR;
import io.questdb.std.Chars;
import io.questdb.std.FilesFacade;
import io.questdb.std.MemoryTag;
import io.questdb.std.ObjIntHashMap;
import io.questdb.std.datetime.microtime.MicrosecondClock;
import io.questdb.std.str.Path;

import java.io.Closeable;

class SymbolCache implements Closeable, SymbolLookup {
    private final ObjIntHashMap<CharSequence> symbolValueToKeyMap = new ObjIntHashMap<>(
            256,
            0.5,
            SymbolTable.VALUE_NOT_FOUND
    );
    private final MemoryMR txMem = Vm.getMRInstance();
    private final SymbolMapReaderImpl symbolMapReader = new SymbolMapReaderImpl();
    private final MicrosecondClock clock;
    private final long waitUsBeforeReload;
    private long transientSymCountOffset;
    private long lastSymbolReaderReloadTimestamp;

    SymbolCache(LineTcpReceiverConfiguration configuration) {
        this.clock = configuration.getMicrosecondClock();
        this.waitUsBeforeReload = configuration.getSymbolCacheWaitUsBeforeReload();
    }

    @Override
    public void close() {
        symbolMapReader.close();
        symbolValueToKeyMap.clear();
        txMem.close();
    }

    @Override
    public int keyOf(CharSequence symbolValue) {
        final int index = symbolValueToKeyMap.keyIndex(symbolValue);
        if (index < 0) {
            return symbolValueToKeyMap.valueAt(index);
        }

        final int symbolValueCount = txMem.getInt(transientSymCountOffset);
        final long ticks;

        if (
                symbolValueCount > symbolMapReader.getSymbolCount()
                        && (ticks = clock.getTicks()) - lastSymbolReaderReloadTimestamp > waitUsBeforeReload
        ) {
            symbolMapReader.updateSymbolCount(symbolValueCount);
            lastSymbolReaderReloadTimestamp = ticks;
        }

        final int symbolKey = symbolMapReader.keyOf(symbolValue);

        if (SymbolTable.VALUE_NOT_FOUND != symbolKey) {
            symbolValueToKeyMap.putAt(index, Chars.toString(symbolValue), symbolKey);
        }

        return symbolKey;
    }

    int getCacheValueCount() {
        return symbolValueToKeyMap.size();
    }

    void of(CairoConfiguration configuration, Path path, CharSequence columnName, int symbolIndexInTxFile) {
        FilesFacade ff = configuration.getFilesFacade();
        transientSymCountOffset = TableUtils.getSymbolWriterTransientIndexOffset(symbolIndexInTxFile);
        final int plen = path.length();
        txMem.of(
                ff,
                path.concat(TableUtils.TXN_FILE_NAME).$(),
                transientSymCountOffset,
                // we will be reading INT value at `transientSymCountOffset`
                // must ensure there is mapped memory
                transientSymCountOffset + 4,
                MemoryTag.MMAP_INDEX_READER
        );
        int symCount = txMem.getInt(transientSymCountOffset);
        path.trimTo(plen);
        symbolMapReader.of(configuration, path, columnName, symCount);
        symbolValueToKeyMap.clear(symCount);
    }
}
