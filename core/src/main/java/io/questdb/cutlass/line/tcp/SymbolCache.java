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

package io.questdb.cutlass.line.tcp;

import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.ColumnVersionReader;
import io.questdb.cairo.SymbolMapReaderImpl;
import io.questdb.cairo.TableWriterAPI;
import io.questdb.cairo.TxReader;
import io.questdb.cairo.sql.SymbolTable;
import io.questdb.std.Os;
import io.questdb.std.Unsafe;
import io.questdb.std.Utf8StringIntHashMap;
import io.questdb.std.datetime.Clock;
import io.questdb.std.str.DirectUtf8Sequence;
import io.questdb.std.str.StringSink;
import io.questdb.std.str.Utf8Sequence;
import io.questdb.std.str.Utf8String;
import io.questdb.std.str.Utf8StringSink;
import io.questdb.std.str.Utf8s;

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
    private String columnName;
    private ColumnVersionReader columnVersionReader;
    private CairoConfiguration configuration;
    private int denseSymbolIndex;
    private long lastSymbolReaderReloadTimestamp;
    private Utf8StringSink pathToTableDir;
    private long symbolTableNameTxn;
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
        columnVersionReader = null;
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
                        (symbolValueCount = readSymbolCount(denseSymbolIndex, true)) > symbolMapReader.getSymbolCount()
        ) {
            // when symbol capacity auto-scales, we also need to reload symbol map reader, as in
            // make it open different set of files altogether
            long nextSymbolTableNameTxn = columnVersionReader.getSymbolTableNameTxn(columnIndex);
            if (nextSymbolTableNameTxn != symbolTableNameTxn) {
                // reload
                symbolMapReader.of(
                        configuration,
                        pathToTableDir,
                        columnName,
                        nextSymbolTableNameTxn,
                        symbolValueCount
                );
                this.symbolTableNameTxn = nextSymbolTableNameTxn;
            } else {
                symbolMapReader.updateSymbolCount(symbolValueCount);
            }
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
            String columnName,
            int columnIndex,
            int denseSymbolIndex,
            Utf8Sequence pathToTableDir,
            TableWriterAPI writerAPI,
            TxReader txReader,
            ColumnVersionReader columnVersionReader
    ) {
        this.configuration = configuration;
        this.writerAPI = writerAPI;
        this.columnIndex = columnIndex;
        this.denseSymbolIndex = denseSymbolIndex;
        this.txReader = txReader;
        this.columnVersionReader = columnVersionReader;
        int symCount = readSymbolCount(denseSymbolIndex, false);
        // hold on to the column name txn so that we know when to reload
        this.symbolTableNameTxn = columnVersionReader.getSymbolTableNameTxn(columnIndex);
        this.columnName = columnName;
        this.pathToTableDir = new Utf8StringSink();
        this.pathToTableDir.put(pathToTableDir);

        symbolMapReader.of(
                configuration,
                this.pathToTableDir,
                columnName,
                symbolTableNameTxn,
                symCount
        );
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
        boolean offsetReloadOk = initialStateOk;
        boolean cvReadOk = initialStateOk;
        while (true) {
            if (offsetReloadOk) {
                int count = txReader.unsafeReadSymbolTransientCount(symbolIndexInTxFile);
                long txColumnVersion = txReader.unsafeReadColumnVersion();
                Unsafe.getUnsafe().loadFence();

                if (txReader.unsafeReadVersion() == txReader.getVersion()) {
                    // Check if _cv file has to be reloaded
                    if (!cvReadOk || columnVersionReader.getVersion() != txColumnVersion) {
                        cvReadOk = columnVersionReader.readSafe();
                        cvReadOk &= columnVersionReader.getVersion() == txColumnVersion;
                    }
                    if (cvReadOk) {
                        return count;
                    }
                }
            }
            offsetReloadOk = txReader.unsafeLoadBaseOffset();
            Os.pause();
        }
    }
}
