/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2023 QuestDB
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
import io.questdb.std.ByteCharSequenceIntHashMap;
import io.questdb.std.Unsafe;
import io.questdb.std.datetime.microtime.MicrosecondClock;
import io.questdb.std.str.ByteCharSequence;
import io.questdb.std.str.DirectByteCharSequence;
import io.questdb.std.str.Path;
import io.questdb.std.str.StringSink;

import java.io.Closeable;

import static io.questdb.std.Chars.utf8ToUtf16Unchecked;

/**
 * Important note:
 * This cache is optimized for ASCII and UTF8 DirectByteCharSequence lookups. Lookups of UTF16
 * strings (j.l.String) with non-ASCII chars will not work correctly, so make sure to re-encode
 * the string in UTF8.
 */
public class SymbolCache implements Closeable, DirectByteSymbolLookup {
    private final MicrosecondClock clock;
    private final SymbolMapReaderImpl symbolMapReader = new SymbolMapReaderImpl();
    private final ByteCharSequenceIntHashMap symbolValueToKeyMap = new ByteCharSequenceIntHashMap(
            256,
            0.5,
            SymbolTable.VALUE_NOT_FOUND
    );
    private final StringSink tempSink = new StringSink();
    private final long waitUsBeforeReload;
    private int columnIndex;
    private long lastSymbolReaderReloadTimestamp;
    private int symbolIndexInTxFile;
    private TxReader txReader;
    private TableWriterAPI writerAPI;

    public SymbolCache(LineTcpReceiverConfiguration configuration) {
        this.clock = configuration.getMicrosecondClock();
        this.waitUsBeforeReload = configuration.getSymbolCacheWaitUsBeforeReload();
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
    public int keyOf(DirectByteCharSequence value) {
        final int index = symbolValueToKeyMap.keyIndex(value);
        if (index < 0) {
            return symbolValueToKeyMap.valueAt(index);
        }

        final long ticks = clock.getTicks();
        int symbolValueCount;

        if (
                ticks - lastSymbolReaderReloadTimestamp > waitUsBeforeReload &&
                        (symbolValueCount = readSymbolCount(symbolIndexInTxFile, true)) > symbolMapReader.getSymbolCount()
        ) {
            symbolMapReader.updateSymbolCount(symbolValueCount);
            lastSymbolReaderReloadTimestamp = ticks;
        }

        utf8ToUtf16Unchecked(value, tempSink);
        final int symbolKey = symbolMapReader.keyOf(tempSink);

        if (symbolKey != SymbolTable.VALUE_NOT_FOUND) {
            symbolValueToKeyMap.putAt(index, ByteCharSequence.newInstance(value), symbolKey);
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
        final int plen = path.length();
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
