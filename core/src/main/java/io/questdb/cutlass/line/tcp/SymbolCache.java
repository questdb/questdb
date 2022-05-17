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
import io.questdb.cairo.TxReader;
import io.questdb.cairo.sql.SymbolLookup;
import io.questdb.cairo.sql.SymbolTable;
import io.questdb.std.Chars;
import io.questdb.std.ObjIntHashMap;
import io.questdb.std.Unsafe;
import io.questdb.std.datetime.microtime.MicrosecondClock;
import io.questdb.std.str.Path;

import java.io.Closeable;

class SymbolCache implements Closeable, SymbolLookup {
    private final ObjIntHashMap<CharSequence> symbolValueToKeyMap = new ObjIntHashMap<>(
            256,
            0.5,
            SymbolTable.VALUE_NOT_FOUND
    );
    private TxReader txReader;
    private final SymbolMapReaderImpl symbolMapReader = new SymbolMapReaderImpl();
    private final MicrosecondClock clock;
    private final long waitUsBeforeReload;
    private long lastSymbolReaderReloadTimestamp;
    private int symbolIndexInTxFile;

    SymbolCache(LineTcpReceiverConfiguration configuration) {
        this.clock = configuration.getMicrosecondClock();
        this.waitUsBeforeReload = configuration.getSymbolCacheWaitUsBeforeReload();
    }

    @Override
    public void close() {
        symbolMapReader.close();
        symbolValueToKeyMap.clear();
    }

    @Override
    public int keyOf(CharSequence value) {
        final int index = symbolValueToKeyMap.keyIndex(value);
        if (index < 0) {
            return symbolValueToKeyMap.valueAt(index);
        }

        final long ticks = clock.getTicks();
        int symbolValueCount;

        if (
                ticks - lastSymbolReaderReloadTimestamp > waitUsBeforeReload &&
                        (symbolValueCount = safeReadUncommittedSymbolCount(symbolIndexInTxFile, true)) > symbolMapReader.getSymbolCount()
        ) {
            symbolMapReader.updateSymbolCount(symbolValueCount);
            lastSymbolReaderReloadTimestamp = ticks;
        }

        final int symbolKey = symbolMapReader.keyOf(value);

        if (SymbolTable.VALUE_NOT_FOUND != symbolKey) {
            symbolValueToKeyMap.putAt(index, Chars.toString(value), symbolKey);
        }

        return symbolKey;
    }

    int getCacheValueCount() {
        return symbolValueToKeyMap.size();
    }

    void of(CairoConfiguration configuration,
            Path path,
            CharSequence columnName,
            int symbolIndexInTxFile,
            TxReader txReader,
            long columnNameTxn
    ) {
        this.symbolIndexInTxFile = symbolIndexInTxFile;
        final int plen = path.length();
        this.txReader = txReader;
        int symCount = safeReadUncommittedSymbolCount(symbolIndexInTxFile, false);
        path.trimTo(plen);
        symbolMapReader.of(configuration, path, columnName, columnNameTxn, symCount);
        symbolValueToKeyMap.clear(symCount);
    }

    private int safeReadUncommittedSymbolCount(int symbolIndexInTxFile, boolean initialStateOk) {
        // TODO: avoid reading dirty distinct counts from _txn file, add new file instead
        boolean offsetReloadOk = initialStateOk;
        while (true) {
            if (offsetReloadOk) {
                int count = txReader.unsafeReadSymbolTransientCount(symbolIndexInTxFile);
                Unsafe.getUnsafe().loadFence();

                if (txReader.unsafeReadVersion() == txReader.getVersion()) {
                    return count;
                }
            }
            offsetReloadOk = txReader.unsafeLoadBaseOffset();
        }
    }
}
