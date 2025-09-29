/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2024 QuestDB
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
import io.questdb.cairo.TableWriterAPI;
import io.questdb.cairo.TxReader;
import io.questdb.cairo.sql.SymbolTable;
import io.questdb.std.Unsafe;
import io.questdb.std.Utf8StringIntHashMap;
import io.questdb.std.datetime.Clock;
import io.questdb.std.str.*;

import java.io.Closeable;

public class SymbolCache implements DirectUtf8SymbolLookup, Closeable {
    private final Clock clock;
    private final SymbolMapReaderImpl symbolMapReader = new SymbolMapReaderImpl();
    private final Utf8StringIntHashMap symbolValueToKeyMap = new Utf8StringIntHashMap(
            256,
            0.5,
            SymbolTable.VALUE_NOT_FOUND
    );
    private final StringSink tempSink = new StringSink();
    private final long waitIntervalBeforeReload;
    private int columnIndex;
    private long lastSymbolReaderReloadTimestamp;
    private int symbolIndexInTxFile;
    private TxReader txReader;
    private TableWriterAPI writerAPI;

    public SymbolCache(LineTcpReceiverConfiguration configuration) {
        this(configuration.getMicrosecondClock(), configuration.getSymbolCacheWaitBeforeReload());
    }

    public SymbolCache(Clock microsecondClock, long waitIntervalBeforeReload) {
        this.clock = microsecondClock;
        this.waitIntervalBeforeReload = waitIntervalBeforeReload;
    }

    @Override
    public void close() {
        txReader = null;
        writerAPI = null;
        symbolMapReader.close();
        symbolValueToKeyMap.reset();
    }

    public int getCacheCapacity() {
        return symbolValueToKeyMap.capacity();
    }

    public int getCacheValueCount() {
        return symbolValueToKeyMap.size();
    }

    @Override
    public int keyOf(DirectUtf8Sequence value) {
        final int index = symbolValueToKeyMap.keyIndex(value);
        if (index < 0) {
            return symbolValueToKeyMap.valueAt(index);
        }

        final long ticks = clock.getTicks();
        int symbolValueCount;

        if (
                ticks - lastSymbolReaderReloadTimestamp > waitIntervalBeforeReload &&
                        (symbolValueCount = readSymbolCount(symbolIndexInTxFile, true)) > symbolMapReader.getSymbolCount()
        ) {
            symbolMapReader.updateSymbolCount(symbolValueCount);
            lastSymbolReaderReloadTimestamp = ticks;
        }

        Utf8s.utf8ToUtf16Unchecked(value, tempSink);
        final int symbolKey = symbolMapReader.keyOf(tempSink);

        if (symbolKey != SymbolTable.VALUE_NOT_FOUND) {
            symbolValueToKeyMap.putAt(index, Utf8String.newInstance(value), symbolKey);
        }

        return symbolKey;
    }

    public void of(
            CairoConfiguration configuration,
            TableWriterAPI writerAPI,
            int columnIndex,
            Path path,
            CharSequence columnName,
            int symbolIndexInTxFile,
            TxReader txReader,
            long columnNameTxn
    ) {
        this.writerAPI = writerAPI;
        this.columnIndex = columnIndex;
        this.symbolIndexInTxFile = symbolIndexInTxFile;
        final int plen = path.size();
        this.txReader = txReader;
        int symCount = readSymbolCount(symbolIndexInTxFile, false);
        path.trimTo(plen);
        symbolMapReader.of(configuration, path, columnName, columnNameTxn, symCount);
        symbolValueToKeyMap.clear();
    }

    private int readSymbolCount(int symbolIndexInTxFile, boolean initialStateOk) {
        int watermark = writerAPI.getSymbolCountWatermark(columnIndex);
        if (watermark != -1) {
            return watermark;
        }
        return safeReadUncommittedSymbolCount(symbolIndexInTxFile, initialStateOk);
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
