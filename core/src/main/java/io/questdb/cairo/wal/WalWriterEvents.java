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

import io.questdb.cairo.ColumnType;
import io.questdb.cairo.sql.BindVariableService;
import io.questdb.cairo.sql.Function;
import io.questdb.cairo.vm.Vm;
import io.questdb.cairo.vm.api.MemoryMARW;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.std.*;
import io.questdb.std.str.Path;
import io.questdb.std.str.StringSink;

import java.io.Closeable;

import static io.questdb.cairo.TableUtils.EVENT_FILE_NAME;
import static io.questdb.cairo.TableUtils.openSmallFile;
import static io.questdb.cairo.wal.WalUtils.*;

class WalWriterEvents implements Closeable {
    private final MemoryMARW eventMem = Vm.getMARWInstance();
    private final FilesFacade ff;
    private final StringSink sink = new StringSink();
    private AtomicIntList initialSymbolCounts;
    private long startOffset = 0;
    private BoolList symbolMapNullFlags;
    private int txn = 0;
    private ObjList<CharSequenceIntHashMap> txnSymbolMaps;

    WalWriterEvents(FilesFacade ff) {
        this.ff = ff;
    }

    @Override
    public void close() {
        eventMem.close(true, Vm.TRUNCATE_TO_POINTER);
    }

    private void init() {
        eventMem.putLong(WALE_HEADER_SIZE + Integer.BYTES);
        eventMem.putInt(WAL_FORMAT_VERSION);
        eventMem.putInt(-1);
        txn = 0;
    }

    private void writeFunction(Function function) {
        final int type = function.getType();
        eventMem.putInt(type);

        switch (ColumnType.tagOf(type)) {
            case ColumnType.BOOLEAN:
                eventMem.putBool(function.getBool(null));
                break;
            case ColumnType.BYTE:
                eventMem.putByte(function.getByte(null));
                break;
            case ColumnType.GEOBYTE:
                eventMem.putByte(function.getGeoByte(null));
                break;
            case ColumnType.SHORT:
                eventMem.putShort(function.getShort(null));
                break;
            case ColumnType.GEOSHORT:
                eventMem.putShort(function.getGeoShort(null));
                break;
            case ColumnType.CHAR:
                eventMem.putChar(function.getChar(null));
                break;
            case ColumnType.INT:
                eventMem.putInt(function.getInt(null));
                break;
            case ColumnType.GEOINT:
                eventMem.putInt(function.getGeoInt(null));
                break;
            case ColumnType.FLOAT:
                eventMem.putFloat(function.getFloat(null));
                break;
            case ColumnType.LONG:
                eventMem.putLong(function.getLong(null));
                break;
            case ColumnType.GEOLONG:
                eventMem.putLong(function.getGeoLong(null));
                break;
            case ColumnType.DATE:
                eventMem.putLong(function.getDate(null));
                break;
            case ColumnType.TIMESTAMP:
                eventMem.putLong(function.getTimestamp(null));
                break;
            case ColumnType.DOUBLE:
                eventMem.putDouble(function.getDouble(null));
                break;
            case ColumnType.STRING:
                eventMem.putStr(function.getStr(null));
                break;
            case ColumnType.BINARY:
                eventMem.putBin(function.getBin(null));
                break;
            default:
                throw new UnsupportedOperationException("unsupported column type: " + ColumnType.nameOf(type));
        }
    }

    private void writeIndexedVariables(BindVariableService bindVariableService) {
        final int count = bindVariableService != null ? bindVariableService.getIndexedVariableCount() : 0;
        eventMem.putInt(count);

        for (int i = 0; i < count; i++) {
            writeFunction(bindVariableService.getFunction(i));
        }
    }

    private void writeNamedVariables(BindVariableService bindVariableService) {
        final int count = bindVariableService != null ? bindVariableService.getNamedVariables().size() : 0;
        eventMem.putInt(count);

        if (count > 0) {
            final ObjList<CharSequence> namedVariables = bindVariableService.getNamedVariables();
            for (int i = 0; i < count; i++) {
                final CharSequence name = namedVariables.get(i);
                eventMem.putStr(name);
                sink.clear();
                sink.put(':').put(name);
                writeFunction(bindVariableService.getFunction(sink));
            }
        }
    }

    private void writeSymbolMapDiffs() {
        final int columns = txnSymbolMaps.size();
        for (int i = 0; i < columns; i++) {
            final CharSequenceIntHashMap symbolMap = txnSymbolMaps.getQuick(i);
            if (symbolMap != null) {
                final int initialCount = initialSymbolCounts.get(i);
                if (initialCount > 0 || (initialCount == 0 && symbolMap.size() > 0)) {
                    eventMem.putInt(i);
                    eventMem.putBool(symbolMapNullFlags.get(i));
                    eventMem.putInt(initialCount);

                    final int size = symbolMap.size();
                    eventMem.putInt(size);

                    for (int j = 0; j < size; j++) {
                        final CharSequence symbol = symbolMap.keys().getQuick(j);
                        final int value = symbolMap.get(symbol);

                        eventMem.putInt(value);
                        eventMem.putStr(symbol);
                    }
                    eventMem.putInt(SymbolMapDiffImpl.END_OF_SYMBOL_ENTRIES);
                }
            }
        }
        eventMem.putInt(SymbolMapDiffImpl.END_OF_SYMBOL_DIFFS);
    }

    int data(long startRowID, long endRowID, long minTimestamp, long maxTimestamp, boolean outOfOrder) {
        startOffset = eventMem.getAppendOffset() - Integer.BYTES;
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
        eventMem.putLong(WALE_SIZE_OFFSET, eventMem.getAppendOffset());
        return txn++;
    }

    void of(ObjList<CharSequenceIntHashMap> txnSymbolMaps, AtomicIntList initialSymbolCounts, BoolList symbolMapNullFlags) {
        this.txnSymbolMaps = txnSymbolMaps;
        this.initialSymbolCounts = initialSymbolCounts;
        this.symbolMapNullFlags = symbolMapNullFlags;
    }

    void openEventFile(Path path, int pathLen) {
        if (eventMem.getFd() > -1) {
            close();
        }
        openSmallFile(ff, path, pathLen, eventMem, EVENT_FILE_NAME, MemoryTag.MMAP_TABLE_WAL_WRITER);
        init();
    }

    void rollback() {
        eventMem.jumpTo(startOffset);
        eventMem.putInt(-1);
        eventMem.putLong(WALE_SIZE_OFFSET, eventMem.getAppendOffset());
    }

    int sql(int cmdType, CharSequence sql, SqlExecutionContext sqlExecutionContext) {
        startOffset = eventMem.getAppendOffset() - Integer.BYTES;
        eventMem.putLong(txn);
        eventMem.putByte(WalTxnType.SQL);
        eventMem.putInt(cmdType); // byte would be enough probably
        eventMem.putStr(sql);
        final Rnd rnd = sqlExecutionContext.getRandom();
        eventMem.putLong(rnd.getSeed0());
        eventMem.putLong(rnd.getSeed1());
        final BindVariableService bindVariableService = sqlExecutionContext.getBindVariableService();
        writeIndexedVariables(bindVariableService);
        writeNamedVariables(bindVariableService);
        eventMem.putInt(startOffset, (int) (eventMem.getAppendOffset() - startOffset));
        eventMem.putInt(-1);
        eventMem.putLong(WALE_SIZE_OFFSET, eventMem.getAppendOffset());
        return txn++;
    }

    int truncate() {
        startOffset = eventMem.getAppendOffset() - Integer.BYTES;
        eventMem.putLong(txn);
        eventMem.putByte(WalTxnType.TRUNCATE);
        eventMem.putInt(startOffset, (int) (eventMem.getAppendOffset() - startOffset));
        eventMem.putInt(-1);
        return txn++;
    }
}
