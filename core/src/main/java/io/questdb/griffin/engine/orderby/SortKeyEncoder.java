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

import io.questdb.cairo.CairoException;
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordMetadata;
import io.questdb.cairo.sql.StaticSymbolTable;
import io.questdb.cairo.sql.SymbolTable;
import io.questdb.griffin.engine.functions.SymbolFunction;
import io.questdb.std.Chars;
import io.questdb.std.Decimal128;
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
    private final Decimal128 decimal128Sink;
    private final Decimal256 decimal256Sink;
    private final boolean[] isDesc;
    private final boolean isSingleColumnFixed8;
    private final boolean[] isSymbol;
    private final int[] offsets;
    private final ObjList<DirectIntList> rankMaps;
    private final DirectIntList tempList;
    private SortKeyType keyType;
    private long padMask;

    public SortKeyEncoder(RecordMetadata metadata, IntList sortColumnFilter) {
        int n = sortColumnFilter.size();
        this.columnIndices = new int[n];
        this.columnTypes = new int[n];
        this.isDesc = new boolean[n];
        this.isSymbol = new boolean[n];
        this.offsets = new int[n];
        this.columnByteWidths = new int[n];
        boolean hasDecimal128 = false;
        boolean hasDecimal256 = false;
        boolean hasSymbol = false;
        this.rankMaps = new ObjList<>(n);

        for (int i = 0; i < n; i++) {
            int encoded = sortColumnFilter.getQuick(i);
            isDesc[i] = encoded < 0;
            columnIndices[i] = (encoded > 0 ? encoded : -encoded) - 1;
            columnTypes[i] = ColumnType.tagOf(metadata.getColumnType(columnIndices[i]));
            isSymbol[i] = ColumnType.isSymbol(columnTypes[i]);
            hasDecimal128 |= columnTypes[i] == ColumnType.DECIMAL128;
            hasDecimal256 |= columnTypes[i] == ColumnType.DECIMAL256;
            if (!isSymbol[i]) {
                columnByteWidths[i] = columnByteWidth(columnTypes[i], metadata, columnIndices[i]);
                rankMaps.add(null);
            } else {
                hasSymbol = true;
                rankMaps.add(new DirectIntList(1024, MemoryTag.NATIVE_DEFAULT, true));
            }
        }
        this.isSingleColumnFixed8 = n == 1 && columnByteWidths[0] <= 8;
        this.tempList = hasSymbol ? new DirectIntList(1024, MemoryTag.NATIVE_DEFAULT, true) : null;
        this.decimal128Sink = hasDecimal128 ? new Decimal128() : null;
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
        Misc.freeObjListAndKeepObjects(rankMaps);
        Misc.free(tempList);
    }

    public void encode(Record record, long destAddr, long rowId) {
        if (isSingleColumnFixed8) {
            encodeFixed8(record, destAddr, rowId);
            return;
        }
        encodeGeneric(record, destAddr);
        Unsafe.getUnsafe().putLong(destAddr + keyType.rowIdOffset(), rowId);
    }

    public SortKeyType init(RecordCursor baseCursor) {
        int totalBytes = 0;

        try {
            int maxSymbolCount = 0;
            for (int i = 0; i < columnIndices.length; i++) {
                if (isSymbol[i]) {
                    maxSymbolCount = Math.max(maxSymbolCount, getStaticSymbolTable(baseCursor.getSymbolTable(columnIndices[i])).getSymbolCount());
                }
            }
            if (maxSymbolCount > 0) {
                tempList.setCapacity(maxSymbolCount);
            }

            for (int i = 0; i < columnIndices.length; i++) {
                offsets[i] = totalBytes;
                if (isSymbol[i]) {
                    StaticSymbolTable symbolTable = getStaticSymbolTable(baseCursor.getSymbolTable(columnIndices[i]));
                    int symbolCount = symbolTable.getSymbolCount();
                    buildRankMap(rankMaps.getQuick(i), tempList, symbolTable, symbolCount);

                    if (symbolCount <= 0xFF) {
                        columnByteWidths[i] = 1;
                    } else if (symbolCount <= 0xFFFF) {
                        columnByteWidths[i] = 2;
                    } else {
                        columnByteWidths[i] = 4;
                    }
                }
                totalBytes += columnByteWidths[i];
            }
        } finally {
            Misc.free(tempList);
        }

        keyType = SortKeyType.fromKeyLength(totalBytes);
        int padBytes = keyType.keyLength() - totalBytes;
        padMask = padBytes > 0 ? (1L << ((8 - padBytes) * 8)) - 1 : -1L;
        return keyType;
    }

    private static void buildRankMap(DirectIntList rankMap, DirectIntList tempList, StaticSymbolTable symbolTable, int symbolCount) {
        if (symbolCount == 0) {
            return;
        }
        tempList.setPos(symbolCount);
        for (int k = 0; k < symbolCount; k++) {
            tempList.set(k, k);
        }

        quickSortRankMap(tempList, symbolTable, 0, symbolCount - 1);
        rankMap.setCapacity(symbolCount);
        rankMap.setPos(symbolCount);
        for (int rank = 0; rank < symbolCount; rank++) {
            rankMap.set(tempList.get(rank), rank + 1);
        }
    }

    private static int columnByteWidth(int columnType, RecordMetadata metadata, int columnIndex) {
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

    private static void encodeBoolean(long addr, boolean value, boolean desc) {
        byte b = value ? (byte) 1 : (byte) 0;
        Unsafe.getUnsafe().putByte(addr, desc ? (byte) ~b : b);
    }

    private static void encodeByte(long addr, byte value, boolean desc) {
        Unsafe.getUnsafe().putByte(addr, (byte) (value ^ (desc ? 0x7F : 0x80)));
    }

    private static void encodeChar(long addr, char value, boolean desc) {
        short s = desc ? (short) ~value : (short) value;
        Unsafe.getUnsafe().putShort(addr, Short.reverseBytes(s));
    }

    private static void encodeDouble(long addr, double value, boolean desc) {
        long bits = Double.doubleToRawLongBits(value);
        if (desc) {
            bits = bits >= 0 ? bits ^ Long.MAX_VALUE : bits;
        } else {
            bits = bits >= 0 ? bits ^ Long.MIN_VALUE : ~bits;
        }
        Unsafe.getUnsafe().putLong(addr, Long.reverseBytes(bits));
    }

    private static void encodeFloat(long addr, float value, boolean desc) {
        int bits = Float.floatToRawIntBits(value);
        if (desc) {
            bits = bits >= 0 ? bits ^ Integer.MAX_VALUE : bits;
        } else {
            bits = bits >= 0 ? bits ^ Integer.MIN_VALUE : ~bits;
        }
        Unsafe.getUnsafe().putInt(addr, Integer.reverseBytes(bits));
    }

    private static void encodeInt(long addr, int value, boolean desc) {
        Unsafe.getUnsafe().putInt(addr, Integer.reverseBytes(value ^ (desc ? 0x7FFFFFFF : 0x80000000)));
    }

    private static void encodeLong(long addr, long value, boolean desc) {
        Unsafe.getUnsafe().putLong(addr, Long.reverseBytes(value ^ (desc ? Long.MAX_VALUE : Long.MIN_VALUE)));
    }

    private static void encodeShort(long addr, short value, boolean desc) {
        Unsafe.getUnsafe().putShort(addr, Short.reverseBytes((short) (value ^ (desc ? 0x7FFF : 0x8000))));
    }

    private static void encodeUnsignedInt(long addr, int value, boolean desc) {
        Unsafe.getUnsafe().putInt(addr, Integer.reverseBytes(desc ? ~value : value));
    }

    private static void encodeUnsignedLong(long addr, long value, boolean desc) {
        Unsafe.getUnsafe().putLong(addr, Long.reverseBytes(desc ? ~value : value));
    }

    private static void encodeUnsignedRank(long addr, int rank, int byteWidth, boolean desc) {
        switch (byteWidth) {
            case 1 -> Unsafe.getUnsafe().putByte(addr, (byte) (desc ? ~rank : rank));
            case 2 -> Unsafe.getUnsafe().putShort(addr, Short.reverseBytes((short) (desc ? ~rank : rank)));
            default -> Unsafe.getUnsafe().putInt(addr, Integer.reverseBytes(desc ? ~rank : rank));
        }
    }

    private static StaticSymbolTable getStaticSymbolTable(SymbolTable symbolTable) {
        if (symbolTable instanceof StaticSymbolTable sst) {
            return sst;
        }
        if (symbolTable instanceof SymbolFunction sf) {
            return sf.getStaticSymbolTable();
        }
        throw new AssertionError("Failed to get static symbol table from " + symbolTable);
    }

    private static void insertionSortRankMap(DirectIntList rankMap, StaticSymbolTable symbolTable, int lo, int hi) {
        for (int i = lo + 1; i <= hi; i++) {
            int key = rankMap.get(i);
            CharSequence val = symbolTable.valueBOf(key);
            int j = i - 1;
            while (j >= lo && Chars.compare(symbolTable.valueOf(rankMap.get(j)), val) > 0) {
                rankMap.set(j + 1, rankMap.get(j));
                j--;
            }
            rankMap.set(j + 1, key);
        }
    }

    private static void quickSortRankMap(DirectIntList rankMap, StaticSymbolTable symbolTable, int lo, int hi) {
        while (lo < hi) {
            if (hi - lo < 24) {
                insertionSortRankMap(rankMap, symbolTable, lo, hi);
                return;
            }

            int mid = lo + (hi - lo) / 2;
            int pivotKey = rankMap.get(mid);
            rankMap.set(mid, rankMap.get(hi));
            rankMap.set(hi, pivotKey);
            CharSequence pivotVal = symbolTable.valueBOf(pivotKey);

            int store = lo;
            for (int i = lo; i < hi; i++) {
                if (Chars.compare(symbolTable.valueOf(rankMap.get(i)), pivotVal) < 0) {
                    int tmp = rankMap.get(store);
                    rankMap.set(store, rankMap.get(i));
                    rankMap.set(i, tmp);
                    store++;
                }
            }
            rankMap.set(hi, rankMap.get(store));
            rankMap.set(store, pivotKey);

            if (store - lo < hi - store) {
                quickSortRankMap(rankMap, symbolTable, lo, store - 1);
                lo = store + 1;
            } else {
                quickSortRankMap(rankMap, symbolTable, store + 1, hi);
                hi = store - 1;
            }
        }
    }

    private void encodeFixed8(Record record, long destAddr, long rowId) {
        int colIdx = columnIndices[0];
        int colType = columnTypes[0];
        boolean desc = isDesc[0];
        int shift = (8 - columnByteWidths[0]) * 8;
        long key;
        if (isSymbol[0]) {
            int symKey = record.getInt(colIdx);
            int rank = (symKey < 0) ? 0 : rankMaps.getQuick(0).get(symKey);
            key = Integer.toUnsignedLong(desc ? ~rank : rank) << shift;
            Unsafe.getUnsafe().putLong(destAddr, key);
            Unsafe.getUnsafe().putLong(destAddr + 8, rowId);
            return;
        }
        key = switch (colType) {
            case ColumnType.BOOLEAN -> {
                byte b = record.getBool(colIdx) ? (byte) 1 : (byte) 0;
                yield (desc ? ~b : b) & 0xFFL;
            }
            case ColumnType.BYTE -> (record.getByte(colIdx) ^ (desc ? 0x7F : 0x80)) & 0xFFL;
            case ColumnType.GEOBYTE -> (record.getGeoByte(colIdx) ^ (desc ? 0x7F : 0x80)) & 0xFFL;
            case ColumnType.DECIMAL8 -> (record.getDecimal8(colIdx) ^ (desc ? 0x7F : 0x80)) & 0xFFL;
            case ColumnType.SHORT -> (record.getShort(colIdx) ^ (desc ? 0x7FFF : 0x8000)) & 0xFFFFL;
            case ColumnType.GEOSHORT -> (record.getGeoShort(colIdx) ^ (desc ? 0x7FFF : 0x8000)) & 0xFFFFL;
            case ColumnType.DECIMAL16 -> (record.getDecimal16(colIdx) ^ (desc ? 0x7FFF : 0x8000)) & 0xFFFFL;
            case ColumnType.CHAR -> (desc ? ~record.getChar(colIdx) : record.getChar(colIdx)) & 0xFFFFL;
            case ColumnType.INT -> Integer.toUnsignedLong(record.getInt(colIdx) ^ (desc ? 0x7FFFFFFF : 0x80000000));
            case ColumnType.GEOINT ->
                    Integer.toUnsignedLong(record.getGeoInt(colIdx) ^ (desc ? 0x7FFFFFFF : 0x80000000));
            case ColumnType.DECIMAL32 ->
                    Integer.toUnsignedLong(record.getDecimal32(colIdx) ^ (desc ? 0x7FFFFFFF : 0x80000000));
            case ColumnType.IPv4 -> Integer.toUnsignedLong(desc ? ~record.getIPv4(colIdx) : record.getIPv4(colIdx));
            case ColumnType.FLOAT -> {
                int bits = Float.floatToRawIntBits(record.getFloat(colIdx));
                if (desc) {
                    bits = bits >= 0 ? bits ^ Integer.MAX_VALUE : bits;
                } else {
                    bits = bits >= 0 ? bits ^ Integer.MIN_VALUE : ~bits;
                }
                yield Integer.toUnsignedLong(bits);
            }
            case ColumnType.LONG -> record.getLong(colIdx) ^ (desc ? Long.MAX_VALUE : Long.MIN_VALUE);
            case ColumnType.GEOLONG -> record.getGeoLong(colIdx) ^ (desc ? Long.MAX_VALUE : Long.MIN_VALUE);
            case ColumnType.TIMESTAMP -> record.getTimestamp(colIdx) ^ (desc ? Long.MAX_VALUE : Long.MIN_VALUE);
            case ColumnType.DATE -> record.getDate(colIdx) ^ (desc ? Long.MAX_VALUE : Long.MIN_VALUE);
            case ColumnType.DECIMAL64 -> record.getDecimal64(colIdx) ^ (desc ? Long.MAX_VALUE : Long.MIN_VALUE);
            case ColumnType.DOUBLE -> {
                long bits = Double.doubleToRawLongBits(record.getDouble(colIdx));
                if (desc) {
                    yield bits >= 0 ? bits ^ Long.MAX_VALUE : bits;
                } else {
                    yield bits >= 0 ? bits ^ Long.MIN_VALUE : ~bits;
                }
            }
            default -> throw new AssertionError("unexpected FIXED_8 type: " + ColumnType.nameOf(colType));
        } << shift;
        Unsafe.getUnsafe().putLong(destAddr, key);
        Unsafe.getUnsafe().putLong(destAddr + 8, rowId);
    }

    private void encodeGeneric(Record record, long destAddr) {
        for (int i = 0; i < columnIndices.length; i++) {
            int colIdx = columnIndices[i];
            long addr = destAddr + offsets[i];
            boolean desc = isDesc[i];

            if (isSymbol[i]) {
                int key = record.getInt(colIdx);
                DirectIntList rankMap = rankMaps.getQuick(i);
                int rank = (key < 0) ? 0 : rankMap.get(key);
                encodeUnsignedRank(addr, rank, columnByteWidths[i], desc);
            } else {
                switch (columnTypes[i]) {
                    case ColumnType.BOOLEAN -> encodeBoolean(addr, record.getBool(colIdx), desc);
                    case ColumnType.BYTE -> encodeByte(addr, record.getByte(colIdx), desc);
                    case ColumnType.GEOBYTE -> encodeByte(addr, record.getGeoByte(colIdx), desc);
                    case ColumnType.DECIMAL8 -> encodeByte(addr, record.getDecimal8(colIdx), desc);
                    case ColumnType.SHORT -> encodeShort(addr, record.getShort(colIdx), desc);
                    case ColumnType.GEOSHORT -> encodeShort(addr, record.getGeoShort(colIdx), desc);
                    case ColumnType.DECIMAL16 -> encodeShort(addr, record.getDecimal16(colIdx), desc);
                    case ColumnType.CHAR -> encodeChar(addr, record.getChar(colIdx), desc);
                    case ColumnType.INT -> encodeInt(addr, record.getInt(colIdx), desc);
                    case ColumnType.GEOINT -> encodeInt(addr, record.getGeoInt(colIdx), desc);
                    case ColumnType.DECIMAL32 -> encodeInt(addr, record.getDecimal32(colIdx), desc);
                    case ColumnType.IPv4 -> encodeUnsignedInt(addr, record.getIPv4(colIdx), desc);
                    case ColumnType.LONG -> encodeLong(addr, record.getLong(colIdx), desc);
                    case ColumnType.GEOLONG -> encodeLong(addr, record.getGeoLong(colIdx), desc);
                    case ColumnType.DECIMAL64 -> encodeLong(addr, record.getDecimal64(colIdx), desc);
                    case ColumnType.DATE -> encodeLong(addr, record.getDate(colIdx), desc);
                    case ColumnType.TIMESTAMP -> encodeLong(addr, record.getTimestamp(colIdx), desc);
                    case ColumnType.FLOAT -> encodeFloat(addr, record.getFloat(colIdx), desc);
                    case ColumnType.DOUBLE -> encodeDouble(addr, record.getDouble(colIdx), desc);
                    case ColumnType.DECIMAL128 -> {
                        record.getDecimal128(colIdx, decimal128Sink);
                        encodeLong(addr, decimal128Sink.getHigh(), desc);
                        encodeUnsignedLong(addr + 8, decimal128Sink.getLow(), desc);
                    }
                    case ColumnType.DECIMAL256 -> {
                        record.getDecimal256(colIdx, decimal256Sink);
                        encodeLong(addr, decimal256Sink.getHh(), desc);
                        encodeUnsignedLong(addr + 8, decimal256Sink.getHl(), desc);
                        encodeUnsignedLong(addr + 16, decimal256Sink.getLh(), desc);
                        encodeUnsignedLong(addr + 24, decimal256Sink.getLl(), desc);
                    }
                    default -> throw CairoException.nonCritical().put("unexpected type in encodeGeneric: ").put(ColumnType.nameOf(columnTypes[i]));
                }
            }
        }

        // Reverse each 8-byte word from big-endian to native byte order.
        // The encoding writes big-endian bytes (for memcmp ordering), but
        // the native sort compares uint64_t values.
        int keyLen = keyType.keyLength();
        int lastWord = keyLen - 8;
        for (int w = 0; w < lastWord; w += 8) {
            long val = Unsafe.getUnsafe().getLong(destAddr + w);
            Unsafe.getUnsafe().putLong(destAddr + w, Long.reverseBytes(val));
        }
        long val = Unsafe.getUnsafe().getLong(destAddr + lastWord);
        Unsafe.getUnsafe().putLong(destAddr + lastWord, Long.reverseBytes(val & padMask));
    }
}
