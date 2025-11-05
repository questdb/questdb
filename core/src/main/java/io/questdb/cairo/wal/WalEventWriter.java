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

package io.questdb.cairo.wal;

import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.CairoException;
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.CommitMode;
import io.questdb.cairo.VarcharTypeDriver;
import io.questdb.cairo.sql.BindVariableService;
import io.questdb.cairo.sql.Function;
import io.questdb.cairo.vm.Vm;
import io.questdb.cairo.vm.api.MemoryMARW;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.std.AtomicIntList;
import io.questdb.std.BoolList;
import io.questdb.std.CharSequenceIntHashMap;
import io.questdb.std.Decimal128;
import io.questdb.std.Decimal256;
import io.questdb.std.Files;
import io.questdb.std.FilesFacade;
import io.questdb.std.LongList;
import io.questdb.std.MemoryTag;
import io.questdb.std.Numbers;
import io.questdb.std.ObjList;
import io.questdb.std.Rnd;
import io.questdb.std.str.Path;
import io.questdb.std.str.StringSink;
import org.jetbrains.annotations.Nullable;
import org.jetbrains.annotations.TestOnly;

import java.io.Closeable;

import static io.questdb.cairo.wal.WalUtils.*;

class WalEventWriter implements Closeable {
    private final CairoConfiguration configuration;
    private final Decimal128 decimal128 = new Decimal128();
    private final Decimal256 decimal256 = new Decimal256();
    private final MemoryMARW eventIndexMem = Vm.getCMARWInstance();
    private final MemoryMARW eventMem = Vm.getCMARWInstance();
    private final FilesFacade ff;
    private final StringSink sink = new StringSink();
    private AtomicIntList initialSymbolCounts;
    // used to test older mat view format
    private boolean legacyMatViewFormat;
    private long startOffset = 0;
    private BoolList symbolMapNullFlags;
    private int txn = 0;
    private ObjList<CharSequenceIntHashMap> txnSymbolMaps;

    WalEventWriter(CairoConfiguration configuration) {
        this.configuration = configuration;
        this.ff = configuration.getFilesFacade();
    }

    @Override
    public void close() {
        close(true, Vm.TRUNCATE_TO_POINTER);
    }

    public void close(boolean truncate, byte truncateMode) {
        eventMem.close(truncate, truncateMode);
        eventIndexMem.close(truncate, truncateMode);
    }

    @TestOnly
    public void setLegacyMatViewFormat(boolean legacyMatViewFormat) {
        this.legacyMatViewFormat = legacyMatViewFormat;
    }

    /**
     * Size in bytes consumed by the events file, including any symbols.
     */
    public long size() {
        return eventMem.getAppendOffset();
    }

    private void appendBindVariableValuesByIndex(BindVariableService bindVariableService) {
        final int count = bindVariableService != null ? bindVariableService.getIndexedVariableCount() : 0;
        eventMem.putInt(count);
        for (int i = 0; i < count; i++) {
            appendFunctionValue(bindVariableService.getFunction(i));
        }
    }

    private void appendBindVariableValuesByName(BindVariableService bindVariableService) {
        final int count = bindVariableService != null ? bindVariableService.getNamedVariables().size() : 0;
        eventMem.putInt(count);

        if (count > 0) {
            final ObjList<CharSequence> namedVariables = bindVariableService.getNamedVariables();
            for (int i = 0; i < count; i++) {
                final CharSequence name = namedVariables.get(i);
                eventMem.putStr(name);
                sink.clear();
                sink.put(':').put(name);
                appendFunctionValue(bindVariableService.getFunction(sink));
            }
        }
    }

    private void appendFunctionValue(Function function) {
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
            case ColumnType.IPv4:
                eventMem.putInt(function.getIPv4(null));
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
                eventMem.putStr(function.getStrA(null));
                break;
            case ColumnType.VARCHAR:
                VarcharTypeDriver.appendPlainValue(eventMem, function.getVarcharA(null));
                break;
            case ColumnType.BINARY:
                eventMem.putBin(function.getBin(null));
                break;
            case ColumnType.UUID:
                eventMem.putLong128(function.getLong128Lo(null), function.getLong128Hi(null));
                break;
            case ColumnType.ARRAY:
                eventMem.putArray(function.getArray(null));
                break;
            case ColumnType.DECIMAL8:
                eventMem.putByte(function.getDecimal8(null));
                break;
            case ColumnType.DECIMAL16:
                eventMem.putShort(function.getDecimal16(null));
                break;
            case ColumnType.DECIMAL32:
                eventMem.putInt(function.getDecimal32(null));
                break;
            case ColumnType.DECIMAL64:
                eventMem.putLong(function.getDecimal64(null));
                break;
            case ColumnType.DECIMAL128: {
                function.getDecimal128(null, decimal128);
                eventMem.putDecimal128(decimal128.getHigh(), decimal128.getLow());
                break;
            }
            case ColumnType.DECIMAL256: {
                function.getDecimal256(null, decimal256);
                eventMem.putDecimal256(
                        decimal256.getHh(),
                        decimal256.getHl(),
                        decimal256.getLh(),
                        decimal256.getLl()
                );
                break;
            }
            default:
                throw new UnsupportedOperationException("unsupported column type: " + ColumnType.nameOf(type));
        }
    }

    private void appendIndex(long value) {
        eventIndexMem.putLong(value);
    }

    private void init() {
        eventMem.putInt(0);
        eventMem.putInt(WALE_FORMAT_VERSION);
        eventMem.putInt(-1);

        appendIndex(WALE_HEADER_SIZE);
        txn = 0;
    }

    private void writeSymbolMapDiffs() {
        final int columns = txnSymbolMaps.size();
        for (int columnIndex = 0; columnIndex < columns; columnIndex++) {
            final CharSequenceIntHashMap symbolMap = txnSymbolMaps.getQuick(columnIndex);
            if (symbolMap != null) {
                final int initialCount = initialSymbolCounts.get(columnIndex);
                if (initialCount > 0 || (initialCount == 0 && symbolMap.size() > 0)) {
                    eventMem.putInt(columnIndex);
                    eventMem.putBool(symbolMapNullFlags.get(columnIndex));
                    eventMem.putInt(initialCount);

                    final int size = symbolMap.size();
                    long appendAddress = eventMem.getAppendOffset();
                    eventMem.putInt(size);

                    int symbolCount = 0;
                    for (int j = 0; j < size; j++) {
                        final CharSequence symbol = symbolMap.keys().getQuick(j);
                        assert symbol != null;
                        final int value = symbolMap.get(symbol);
                        // Ignore symbols cached from symbolMapReader
                        if (value >= initialCount) {
                            eventMem.putInt(value);
                            eventMem.putStr(symbol);
                            symbolCount++;
                        }
                    }
                    // Update the size with the exact symbolCount
                    // An empty SymbolMapDiff can be created because symbolCount can be 0
                    // in case all cached symbols come from symbolMapReader.
                    // Alternatively, two-pass approach can be used.
                    eventMem.putInt(appendAddress, symbolCount);
                    eventMem.putInt(SymbolMapDiffImpl.END_OF_SYMBOL_ENTRIES);
                }
            }
        }
        eventMem.putInt(SymbolMapDiffImpl.END_OF_SYMBOL_DIFFS);
    }

    /**
     * Append data to the WAL. This method is used for both regular and materialized view data.
     * The method takes various parameters to specify the data range, timestamps, and other options.
     *
     * @param startRowID           the starting row ID of the data in the segment.
     * @param endRowID             the ending row ID of the data  in the segment.
     * @param minTimestamp         the minimum timestamp of the data, inclusive
     * @param maxTimestamp         the maximum timestamp of the data, inclusive
     * @param outOfOrder           indicates if the data is out of order
     * @param lastRefreshBaseTxn   seqTxn of base transaction ID when refresh is performed
     * @param lastRefreshTimestamp wall clock mat view refresh timestamp
     * @param lastPeriodHi         the high timestamp for the last mat view period refresh
     * @param replaceRangeLowTs    the low timestamp for the range to be replaced, inclusive
     * @param replaceRangeHiTs     the high timestamp for the range to be replaced, exclusive
     * @param dedupMode            deduplication mode, can be DEFAULT, NO_DEDUP, UPSERT_NEW or REPLACE_RANGE.
     */
    int appendData(
            byte txnType,
            long startRowID,
            long endRowID,
            long minTimestamp,
            long maxTimestamp,
            boolean outOfOrder,
            long lastRefreshBaseTxn,
            long lastRefreshTimestamp,
            long lastPeriodHi,
            long replaceRangeLowTs,
            long replaceRangeHiTs,
            byte dedupMode
    ) {
        assert txnType == WalTxnType.MAT_VIEW_DATA || txnType == WalTxnType.DATA : "unexpected txn type: " + txnType;
        startOffset = eventMem.getAppendOffset() - Integer.BYTES;
        eventMem.putLong(txn);
        eventMem.putByte(txnType);
        eventMem.putLong(startRowID);
        eventMem.putLong(endRowID);
        eventMem.putLong(minTimestamp);
        eventMem.putLong(maxTimestamp);
        eventMem.putBool(outOfOrder);
        if (txnType == WalTxnType.MAT_VIEW_DATA) {
            assert lastRefreshBaseTxn != Numbers.LONG_NULL;
            eventMem.putLong(lastRefreshBaseTxn);
            eventMem.putLong(lastRefreshTimestamp);
        }

        if (dedupMode == WalUtils.WAL_DEDUP_MODE_REPLACE_RANGE) {
            if (replaceRangeLowTs >= replaceRangeHiTs) {
                throw CairoException.nonCritical().put("Replace range low timestamp must be less than replace range high timestamp.");
            }
            if (replaceRangeLowTs > minTimestamp) {
                throw CairoException.nonCritical().put("Replace range low timestamp must be less than or equal to the minimum timestamp.");
            }
            if (replaceRangeHiTs <= maxTimestamp) {
                throw CairoException.nonCritical().put("Replace range high timestamp must be greater than the maximum timestamp.");
            }
        }

        writeSymbolMapDiffs();

        // To test backwards compatibility and ensure that we still can read WALs
        // written by the old QuestDB version, make the dedup mode
        // and replace range value optional. It will then not be written for the most transactions,
        // and it will test the reading WAL-E code to read the old format.
        if (dedupMode != WAL_DEDUP_MODE_DEFAULT) {
            // Always write extra 8 bytes, even for normal tables, to simplify reading the extra footer.
            if (txnType == WalTxnType.MAT_VIEW_DATA) {
                eventMem.putLong(lastPeriodHi);
            } else {
                eventMem.putLong(Numbers.LONG_NULL);
            }
            eventMem.putLong(replaceRangeLowTs);
            eventMem.putLong(replaceRangeHiTs);
            eventMem.putByte(dedupMode);
        }
        eventMem.putInt(startOffset, (int) (eventMem.getAppendOffset() - startOffset));
        eventMem.putInt(-1);

        appendIndex(eventMem.getAppendOffset() - Integer.BYTES);
        eventMem.putInt(WALE_MAX_TXN_OFFSET_32, txn);
        if (txnType == WalTxnType.MAT_VIEW_DATA) {
            eventMem.putInt(WAL_FORMAT_OFFSET_32, WALE_MAT_VIEW_FORMAT_VERSION);
        }
        return txn++;
    }

    int appendMatViewInvalidate(
            long lastRefreshBaseTxn,
            long lastRefreshTimestamp,
            boolean invalid,
            @Nullable CharSequence invalidationReason,
            long lastPeriodHi,
            @Nullable LongList refreshIntervals,
            long refreshIntervalsBaseTxn
    ) {
        startOffset = eventMem.getAppendOffset() - Integer.BYTES;
        eventMem.putLong(txn);
        eventMem.putByte(WalTxnType.MAT_VIEW_INVALIDATE);
        eventMem.putLong(lastRefreshBaseTxn);
        eventMem.putLong(lastRefreshTimestamp);
        eventMem.putBool(invalid);
        eventMem.putStr(invalidationReason);
        if (!legacyMatViewFormat) {
            eventMem.putLong(lastPeriodHi);
            eventMem.putLong(refreshIntervalsBaseTxn);
            if (refreshIntervals != null) {
                eventMem.putInt(refreshIntervals.size());
                for (int i = 0, n = refreshIntervals.size(); i < n; i++) {
                    eventMem.putLong(refreshIntervals.getQuick(i));
                }
            } else {
                eventMem.putInt(-1);
            }
        }
        eventMem.putInt(startOffset, (int) (eventMem.getAppendOffset() - startOffset));
        eventMem.putInt(-1);

        appendIndex(eventMem.getAppendOffset() - Integer.BYTES);
        eventMem.putInt(WALE_MAX_TXN_OFFSET_32, txn);
        eventMem.putInt(WAL_FORMAT_OFFSET_32, WALE_MAT_VIEW_FORMAT_VERSION);
        return txn++;
    }

    int appendSql(int cmdType, CharSequence sqlText, SqlExecutionContext sqlExecutionContext) {
        startOffset = eventMem.getAppendOffset() - Integer.BYTES;
        eventMem.putLong(txn);
        eventMem.putByte(WalTxnType.SQL);
        eventMem.putInt(cmdType); // byte would be enough probably
        eventMem.putStr(sqlText);
        final Rnd rnd = sqlExecutionContext.getRandom();
        eventMem.putLong(rnd.getSeed0());
        eventMem.putLong(rnd.getSeed1());
        final BindVariableService bindVariableService = sqlExecutionContext.getBindVariableService();
        appendBindVariableValuesByIndex(bindVariableService);
        appendBindVariableValuesByName(bindVariableService);
        eventMem.putInt(startOffset, (int) (eventMem.getAppendOffset() - startOffset));
        eventMem.putInt(-1);

        appendIndex(eventMem.getAppendOffset() - Integer.BYTES);
        eventMem.putInt(WALE_MAX_TXN_OFFSET_32, txn);
        return txn++;
    }

    void of(ObjList<CharSequenceIntHashMap> txnSymbolMaps, AtomicIntList initialSymbolCounts, BoolList symbolMapNullFlags) {
        this.txnSymbolMaps = txnSymbolMaps;
        this.initialSymbolCounts = initialSymbolCounts;
        this.symbolMapNullFlags = symbolMapNullFlags;
    }

    void openEventFile(Path path, int pathLen, boolean truncate, boolean systemTable) {
        if (eventMem.getFd() > -1) {
            close(truncate, Vm.TRUNCATE_TO_POINTER);
        }
        final long appendPageSize = systemTable
                ? configuration.getSystemWalEventAppendPageSize()
                : configuration.getWalEventAppendPageSize();
        eventMem.of(
                ff,
                path.trimTo(pathLen).concat(EVENT_FILE_NAME).$(),
                appendPageSize,
                -1,
                MemoryTag.NATIVE_TABLE_WAL_WRITER,
                CairoConfiguration.O_NONE,
                Files.POSIX_MADV_RANDOM
        );
        eventIndexMem.of(
                ff,
                path.trimTo(pathLen).concat(EVENT_INDEX_FILE_NAME).$(),
                Math.max(ff.getPageSize(), appendPageSize / 4),
                -1,
                MemoryTag.NATIVE_TABLE_WAL_WRITER,
                CairoConfiguration.O_NONE,
                Files.POSIX_MADV_SEQUENTIAL
        );
        init();
    }

    void rollback() {
        eventMem.putInt(startOffset, -1);
        eventMem.putInt(WALE_MAX_TXN_OFFSET_32, --txn - 1);
        // Do not truncate files, these files may be read by WAL Apply job at the moment.
        // This is very rare case, WALE will not be written anymore after this call.
        // Not truncating the files saves from reading complexity.
    }

    void sync() {
        int commitMode = configuration.getCommitMode();
        if (commitMode != CommitMode.NOSYNC) {
            eventMem.sync(commitMode == CommitMode.ASYNC);
            eventIndexMem.sync(commitMode == CommitMode.ASYNC);
        }
    }

    int truncate() {
        startOffset = eventMem.getAppendOffset() - Integer.BYTES;
        eventMem.putLong(txn);
        eventMem.putByte(WalTxnType.TRUNCATE);
        eventMem.putInt(startOffset, (int) (eventMem.getAppendOffset() - startOffset));
        eventMem.putInt(-1);

        appendIndex(eventMem.getAppendOffset() - Integer.BYTES);
        eventMem.putInt(WALE_MAX_TXN_OFFSET_32, txn);
        return txn++;
    }
}
