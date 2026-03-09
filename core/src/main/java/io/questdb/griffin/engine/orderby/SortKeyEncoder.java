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

package io.questdb.griffin.engine.orderby;

import io.questdb.cairo.ColumnType;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordMetadata;
import io.questdb.cairo.sql.StaticSymbolTable;
import io.questdb.cairo.sql.SymbolTable;
import io.questdb.griffin.engine.functions.SymbolFunction;
import io.questdb.std.Decimal256;
import io.questdb.std.DirectIntList;
import io.questdb.std.IntList;
import io.questdb.std.MemoryTag;
import io.questdb.std.Misc;
import io.questdb.std.ObjList;
import io.questdb.std.QuietCloseable;
import io.questdb.std.Unsafe;

/**
 * Analyzes sort column types at compile time, produces a fixed-width
 * byte-comparable key encoder, and serializes sort columns into native memory.
 * After encoding, the key can be compared using unsigned long comparison
 * to determine sort order.
 */
public class SortKeyEncoder implements QuietCloseable {
    private final int[] columnByteWidths;
    private final int[] columnIndices;
    private final int[] columnTypes;
    private final Decimal256 decimal256Sink;
    private final boolean[] isDesc;
    private final boolean[] isSymbol;
    private final int[] offsets;
    private SortKeyType keyType;
    private ObjList<DirectIntList> rankMaps;

    public SortKeyEncoder(RecordMetadata metadata, IntList sortColumnFilter) {
        int n = sortColumnFilter.size();
        this.columnIndices = new int[n];
        this.columnTypes = new int[n];
        this.isDesc = new boolean[n];
        this.isSymbol = new boolean[n];
        this.offsets = new int[n];
        this.columnByteWidths = new int[n];
        boolean hasDecimal256 = false;

        for (int i = 0; i < n; i++) {
            int encoded = sortColumnFilter.getQuick(i);
            isDesc[i] = encoded < 0;
            columnIndices[i] = (encoded > 0 ? encoded : -encoded) - 1;
            columnTypes[i] = ColumnType.tagOf(metadata.getColumnType(columnIndices[i]));
            isSymbol[i] = ColumnType.isSymbol(columnTypes[i]);
            hasDecimal256 |= columnTypes[i] == ColumnType.DECIMAL256;
            // Pre-compute byte width for non-symbol columns (symbol width depends on runtime symbol count)
            if (!isSymbol[i]) {
                columnByteWidths[i] = columnByteWidth(columnTypes[i], metadata, columnIndices[i]);
            }
        }

        this.decimal256Sink = hasDecimal256 ? new Decimal256() : null;
    }

    /**
     * Checks whether all sort columns can be encoded as a fixed-width
     * byte-comparable key (at most 32 bytes).
     */
    public static boolean isSupported(RecordMetadata metadata, IntList sortColumnFilter) {
        int totalBytes = 0;
        for (int i = 0, n = sortColumnFilter.size(); i < n; i++) {
            int encoded = sortColumnFilter.getQuick(i);
            int columnIndex = (encoded > 0 ? encoded : -encoded) - 1;
            int columnType = ColumnType.tagOf(metadata.getColumnType(columnIndex));
            int width = columnByteWidth(columnType, metadata, columnIndex);
            if (width < 0) {
                return false;
            }
            totalBytes += width;
        }
        return totalBytes <= 32;
    }

    @Override
    public void close() {
        Misc.freeObjList(rankMaps);
    }

    public SortKeyType configure(RecordCursor baseCursor) {
        Misc.freeObjList(rankMaps);
        rankMaps = new ObjList<>(columnIndices.length);
        int totalBytes = 0;

        for (int i = 0; i < columnIndices.length; i++) {
            offsets[i] = totalBytes;

            if (isSymbol[i]) {
                StaticSymbolTable symbolTable = getStaticSymbolTable(baseCursor.getSymbolTable(columnIndices[i]));
                int symbolCount = symbolTable.getSymbolCount();
                rankMaps.add(buildRankMap(symbolTable, symbolCount));

                if (symbolCount <= 0xFF) {
                    columnByteWidths[i] = 1;
                } else if (symbolCount <= 0xFFFF) {
                    columnByteWidths[i] = 2;
                } else {
                    columnByteWidths[i] = 4;
                }
            } else {
                rankMaps.add(null);
            }
            totalBytes += columnByteWidths[i];
        }

        keyType = SortKeyType.fromKeyLength(totalBytes);
        return keyType;
    }

    public void encode(Record record, long destAddr) {
        for (int i = 0; i < columnIndices.length; i++) {
            int colIdx = columnIndices[i];
            long addr = destAddr + offsets[i];
            int width = columnByteWidths[i];

            if (isSymbol[i]) {
                int key = record.getInt(colIdx);
                DirectIntList rankMap = rankMaps.getQuick(i);
                int rank = rankMap.get(key + 1);
                SortKeyEncoding.encodeUnsignedRank(addr, rank, width);
            } else {
                switch (columnTypes[i]) {
                    case ColumnType.BOOLEAN -> SortKeyEncoding.encodeBoolean(addr, record.getBool(colIdx));
                    case ColumnType.BYTE, ColumnType.GEOBYTE ->
                            SortKeyEncoding.encodeByte(addr, record.getByte(colIdx));
                    case ColumnType.DECIMAL8 -> SortKeyEncoding.encodeByte(addr, record.getDecimal8(colIdx));
                    case ColumnType.SHORT, ColumnType.GEOSHORT ->
                            SortKeyEncoding.encodeShort(addr, record.getShort(colIdx));
                    case ColumnType.DECIMAL16 -> SortKeyEncoding.encodeShort(addr, record.getDecimal16(colIdx));
                    case ColumnType.CHAR -> SortKeyEncoding.encodeChar(addr, record.getChar(colIdx));
                    case ColumnType.INT, ColumnType.GEOINT -> SortKeyEncoding.encodeInt(addr, record.getInt(colIdx));
                    case ColumnType.DECIMAL32 -> SortKeyEncoding.encodeInt(addr, record.getDecimal32(colIdx));
                    case ColumnType.LONG -> SortKeyEncoding.encodeLong(addr, record.getLong(colIdx));
                    case ColumnType.GEOLONG -> SortKeyEncoding.encodeLong(addr, record.getGeoLong(colIdx));
                    case ColumnType.DECIMAL64 -> SortKeyEncoding.encodeLong(addr, record.getDecimal64(colIdx));
                    case ColumnType.DATE -> SortKeyEncoding.encodeLong(addr, record.getDate(colIdx));
                    case ColumnType.TIMESTAMP -> SortKeyEncoding.encodeLong(addr, record.getTimestamp(colIdx));
                    case ColumnType.FLOAT -> SortKeyEncoding.encodeFloat(addr, record.getFloat(colIdx));
                    case ColumnType.DOUBLE -> SortKeyEncoding.encodeDouble(addr, record.getDouble(colIdx));
                    case ColumnType.IPv4 -> SortKeyEncoding.encodeInt(addr, record.getIPv4(colIdx));
                    case ColumnType.DECIMAL128 -> {
                        SortKeyEncoding.encodeLong(addr, record.getLong128Hi(colIdx));
                        SortKeyEncoding.encodeUnsignedLong(addr + 8, record.getLong128Lo(colIdx));
                    }
                    case ColumnType.DECIMAL256 -> {
                        record.getDecimal256(colIdx, decimal256Sink);
                        SortKeyEncoding.encodeLong(addr, decimal256Sink.getHh());
                        SortKeyEncoding.encodeUnsignedLong(addr + 8, decimal256Sink.getHl());
                        SortKeyEncoding.encodeUnsignedLong(addr + 16, decimal256Sink.getLh());
                        SortKeyEncoding.encodeUnsignedLong(addr + 24, decimal256Sink.getLl());
                    }
                }
            }

            if (isDesc[i]) {
                SortKeyEncoding.flipBytes(addr, width);
            }
        }
        // zero-pad the remaining key bytes up to keyType.keyLength()
        int actualKeyBytes = offsets[columnIndices.length - 1] + columnByteWidths[columnIndices.length - 1];
        int padBytes = keyType.keyLength() - actualKeyBytes;
        if (padBytes > 0) {
            Unsafe.getUnsafe().setMemory(destAddr + actualKeyBytes, padBytes, (byte) 0);
        }

        // Reverse each 8-byte word from big-endian to native byte order.
        // The encoding writes big-endian bytes (for memcmp ordering), but
        // the native sort compares uint64_t values. On little-endian machines
        // we must byte-swap each word so uint64_t comparison matches memcmp.
        int keyLen = keyType.keyLength();
        for (int w = 0; w < keyLen; w += 8) {
            long val = Unsafe.getUnsafe().getLong(destAddr + w);
            Unsafe.getUnsafe().putLong(destAddr + w, Long.reverseBytes(val));
        }
    }

    private static DirectIntList buildRankMap(StaticSymbolTable symbolTable, int symbolCount) {
        int totalEntries = symbolCount + 1; // +1 for NULL
        DirectIntList rankMap = new DirectIntList(totalEntries, MemoryTag.NATIVE_DEFAULT);
        rankMap.setCapacity(totalEntries);
        rankMap.setPos(totalEntries);

        int[] sortedKeys = new int[symbolCount];
        for (int k = 0; k < symbolCount; k++) {
            sortedKeys[k] = k;
        }
        // insertion sort by symbol value (symbol count is typically small)
        for (int i = 1; i < symbolCount; i++) {
            int keyI = sortedKeys[i];
            CharSequence valI = symbolTable.valueOf(keyI);
            int j = i - 1;
            while (j >= 0) {
                CharSequence valJ = symbolTable.valueOf(sortedKeys[j]);
                if (compare(valJ, valI) <= 0) {
                    break;
                }
                sortedKeys[j + 1] = sortedKeys[j];
                j--;
            }
            sortedKeys[j + 1] = keyI;
        }

        // NULL gets rank 0 (NULLS FIRST), sorted symbols get ranks 1..symbolCount
        rankMap.set(0, 0);
        for (int rank = 0; rank < symbolCount; rank++) {
            int key = sortedKeys[rank];
            rankMap.set(key + 1, rank + 1);
        }

        return rankMap;
    }

    private static int compare(CharSequence a, CharSequence b) {
        if (a == null && b == null) {
            return 0;
        }
        if (a == null) {
            return -1;
        }
        if (b == null) {
            return 1;
        }
        int len = Math.min(a.length(), b.length());
        for (int i = 0; i < len; i++) {
            int cmp = Character.compare(a.charAt(i), b.charAt(i));
            if (cmp != 0) {
                return cmp;
            }
        }
        return Integer.compare(a.length(), b.length());
    }

    private static StaticSymbolTable getStaticSymbolTable(SymbolTable symbolTable) {
        if (symbolTable instanceof StaticSymbolTable) {
            return (StaticSymbolTable) symbolTable;
        }
        if (symbolTable instanceof SymbolFunction) {
            return ((SymbolFunction) symbolTable).getStaticSymbolTable();
        }
        throw new AssertionError("Failed to get static symbol table from " + symbolTable);
    }

    static int columnByteWidth(int columnType, RecordMetadata metadata, int columnIndex) {
        return switch (columnType) {
            case ColumnType.BOOLEAN, ColumnType.BYTE, ColumnType.GEOBYTE, ColumnType.DECIMAL8 -> 1;
            case ColumnType.SHORT, ColumnType.GEOSHORT, ColumnType.CHAR, ColumnType.DECIMAL16 -> 2;
            case ColumnType.INT, ColumnType.GEOINT, ColumnType.IPv4, ColumnType.FLOAT, ColumnType.DECIMAL32 -> 4;
            case ColumnType.LONG, ColumnType.DATE, ColumnType.TIMESTAMP, ColumnType.DOUBLE,
                 ColumnType.GEOLONG, ColumnType.DECIMAL64 -> 8;
            case ColumnType.DECIMAL128 -> 16;
            case ColumnType.DECIMAL256 -> 32;
            case ColumnType.SYMBOL -> {
                if (metadata.isSymbolTableStatic(columnIndex)) {
                    yield 4;
                }
                yield -1;
            }
            default -> -1;
        };
    }
}
