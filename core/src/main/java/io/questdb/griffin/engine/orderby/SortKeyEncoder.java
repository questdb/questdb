/*+*****************************************************************************
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
import io.questdb.cairo.sql.SymbolTableSource;
import io.questdb.cairo.vm.api.MemoryCARW;
import io.questdb.griffin.engine.RecordComparator;
import io.questdb.griffin.engine.functions.SymbolFunction;
import io.questdb.std.Chars;
import io.questdb.std.Decimal128;
import io.questdb.std.Decimal256;
import io.questdb.std.DirectIntList;
import io.questdb.std.IntList;
import io.questdb.std.Long256;
import io.questdb.std.Long256Impl;
import io.questdb.std.MemoryTag;
import io.questdb.std.Misc;
import io.questdb.std.Numbers;
import io.questdb.std.ObjList;
import io.questdb.std.QuietCloseable;
import io.questdb.std.Unsafe;
import io.questdb.std.Uuid;
import io.questdb.std.str.Utf8Sequence;

/**
 * Analyzes sort column types at compile time, produces a byte-comparable
 * key encoder, and serializes sort columns into native memory. Keys of up
 * to 32 fixed-width bytes are encoded inline into the entry; wider or
 * variable-length keys are written to a key heap, with the entry holding a
 * 16-byte prefix, the key length, the heap offset and the rowId. After
 * encoding, keys compare with unsigned long/byte comparison.
 */
public class SortKeyEncoder implements QuietCloseable {
    public static final int KEY_PREFIX_BYTES = 16;
    private final int[] columnByteWidths;
    private final int[] columnIndices;
    private final int[] columnTypes;
    private final Decimal128 decimal128Sink;
    private final Decimal256 decimal256Sink;
    private final boolean[] isDesc;
    private final boolean isSingleColumnFixed8;
    private final boolean[] isStaticSymbol;
    private final int[] offsets;
    private final int[] rankMapSizes;
    private final ObjList<DirectIntList> rankMaps;
    private MemoryCARW keyHeap;
    private SortKeyType keyType;
    private long padMask;

    public SortKeyEncoder(RecordMetadata metadata, IntList sortColumnFilter) {
        int n = sortColumnFilter.size();
        this.columnIndices = new int[n];
        this.columnTypes = new int[n];
        this.isDesc = new boolean[n];
        this.isStaticSymbol = new boolean[n];
        this.offsets = new int[n];
        this.columnByteWidths = new int[n];
        this.rankMapSizes = new int[n];
        boolean hasDecimal128 = false;
        boolean hasDecimal256 = false;
        this.rankMaps = new ObjList<>(n);

        for (int i = 0; i < n; i++) {
            int encoded = sortColumnFilter.getQuick(i);
            isDesc[i] = encoded < 0;
            columnIndices[i] = (encoded > 0 ? encoded : -encoded) - 1;
            columnTypes[i] = ColumnType.tagOf(metadata.getColumnType(columnIndices[i]));
            hasDecimal128 |= columnTypes[i] == ColumnType.DECIMAL128;
            hasDecimal256 |= columnTypes[i] == ColumnType.DECIMAL256;
            if (ColumnType.isSymbol(columnTypes[i]) && metadata.isSymbolTableStatic(columnIndices[i])) {
                isStaticSymbol[i] = true;
                columnByteWidths[i] = 4;
                rankMaps.add(new DirectIntList(1024, MemoryTag.NATIVE_DEFAULT, true));
            } else {
                columnByteWidths[i] = fixedColumnByteWidth(columnTypes[i]);
                rankMaps.add(null);
            }
        }
        this.isSingleColumnFixed8 = n == 1 && columnByteWidths[0] >= 0 && columnByteWidths[0] <= 8;
        this.decimal128Sink = hasDecimal128 ? new Decimal128() : null;
        this.decimal256Sink = hasDecimal256 ? new Decimal256() : null;
    }

    public static void buildRankMap(SymbolTable symbolTable, DirectIntList rankMap) {
        buildRankMap(getStaticSymbolTable(symbolTable), rankMap);
    }

    public static void buildRankMap(StaticSymbolTable sst, DirectIntList rankMap) {
        int symbolCount = sst.getSymbolCount();
        if (symbolCount == 0) {
            rankMap.clear();
            return;
        }
        rankMap.setCapacity(symbolCount);
        rankMap.setPos(symbolCount);
        for (int k = 0; k < symbolCount; k++) {
            rankMap.set(k, k);
        }

        quickSortRankMap(rankMap, sst, 0, symbolCount - 1);

        for (int i = 0; i < symbolCount; i++) {
            int j = rankMap.get(i);
            if (j < 0) {
                continue;
            }
            int prev = i;
            while (j != i) {
                int next = rankMap.get(j);
                rankMap.set(j, -(prev + 1));
                prev = j;
                j = next;
            }
            rankMap.set(i, -(prev + 1));
        }
        for (int i = 0; i < symbolCount; i++) {
            rankMap.set(i, -rankMap.get(i));
        }
    }

    public static void buildRankMaps(SymbolTableSource symbolTableSource, ObjList<DirectIntList> rankMaps, RecordComparator comparator) {
        if (rankMaps == null) {
            return;
        }
        for (int i = 0, n = rankMaps.size(); i < n; i++) {
            if (rankMaps.getQuick(i) != null) {
                buildRankMap(symbolTableSource.getSymbolTable(i), rankMaps.getQuick(i));
            }
        }
        comparator.setRankMaps(rankMaps);
    }

    public static ObjList<DirectIntList> createRankMaps(RecordMetadata metadata, IntList sortColumnFilter) {
        ObjList<DirectIntList> rankMaps = null;
        try {
            for (int i = 0, n = sortColumnFilter.size(); i < n; i++) {
                int encoded = sortColumnFilter.getQuick(i);
                int colIdx = (encoded > 0 ? encoded : -encoded) - 1;
                if (ColumnType.isSymbol(ColumnType.tagOf(metadata.getColumnType(colIdx)))
                        && metadata.isSymbolTableStatic(colIdx)) {
                    if (rankMaps == null) {
                        rankMaps = new ObjList<>();
                    }
                    rankMaps.extendAndSet(colIdx, new DirectIntList(8, MemoryTag.NATIVE_DEFAULT, true));
                }
            }
        } catch (Throwable e) {
            Misc.freeObjList(rankMaps);
            throw e;
        }

        return rankMaps;
    }

    /**
     * Checks whether all sort columns can be encoded into a byte-comparable
     * key. Every column type that ORDER BY accepts is encodable.
     */
    public static boolean isSupported(RecordMetadata metadata, IntList sortColumnFilter) {
        for (int i = 0, n = sortColumnFilter.size(); i < n; i++) {
            int encoded = sortColumnFilter.getQuick(i);
            int columnIndex = (encoded > 0 ? encoded : -encoded) - 1;
            if (!isEncodable(ColumnType.tagOf(metadata.getColumnType(columnIndex)))) {
                return false;
            }
        }
        return true;
    }

    @SuppressWarnings("unused") // called from generated bytecode (RecordComparatorCompiler)
    public static int rank(Object rankMap, int key) {
        return key < 0 ? 0 : ((DirectIntList) rankMap).get(key);
    }

    @Override
    public void close() {
        Misc.freeObjListAndKeepObjects(rankMaps);
    }

    public void encode(Record record, long destAddr, long rowId) {
        if (isSingleColumnFixed8) {
            encodeFixed8(record, destAddr, rowId);
            return;
        }
        if (keyType.isVariable()) {
            encodeVariable(record, destAddr, rowId);
            return;
        }
        encodeGeneric(record, destAddr);
        Unsafe.putLong(destAddr + keyType.rowIdOffset(), rowId);
    }

    public SortKeyType init(RecordCursor baseCursor) {
        int totalBytes = 0;
        boolean hasVarLength = false;

        for (int i = 0; i < columnIndices.length; i++) {
            if (isStaticSymbol[i]) {
                StaticSymbolTable sst = getStaticSymbolTable(baseCursor.getSymbolTable(columnIndices[i]));
                int symbolCount = sst.getSymbolCount();
                buildRankMap(sst, rankMaps.getQuick(i));
                rankMapSizes[i] = symbolCount;

                if (symbolCount <= 0xFF) {
                    columnByteWidths[i] = 1;
                } else if (symbolCount <= 0xFFFF) {
                    columnByteWidths[i] = 2;
                } else {
                    columnByteWidths[i] = 4;
                }
            }
            if (columnByteWidths[i] < 0) {
                hasVarLength = true;
            } else {
                offsets[i] = totalBytes;
                totalBytes += columnByteWidths[i];
            }
        }

        keyType = hasVarLength ? SortKeyType.VARIABLE : SortKeyType.fromKeyLength(totalBytes);
        if (!keyType.isVariable()) {
            int padBytes = keyType.keyLength() - totalBytes;
            padMask = padBytes > 0 ? (1L << ((8 - padBytes) * 8)) - 1 : -1L;
        }
        return keyType;
    }

    public void setKeyHeap(MemoryCARW keyHeap) {
        this.keyHeap = keyHeap;
    }

    private static void encodeBoolean(long addr, boolean value, boolean desc) {
        byte b = value ? (byte) 1 : (byte) 0;
        Unsafe.putByte(addr, desc ? (byte) ~b : b);
    }

    private static void encodeByte(long addr, byte value, boolean desc) {
        Unsafe.putByte(addr, (byte) (value ^ (desc ? 0x7F : 0x80)));
    }

    private static void encodeChar(long addr, char value, boolean desc) {
        short s = desc ? (short) ~value : (short) value;
        Unsafe.putShort(addr, Short.reverseBytes(s));
    }

    private static void encodeDouble(long addr, double value, boolean desc) {
        long bits = Double.doubleToRawLongBits(value);
        if (desc) {
            bits = bits >= 0 ? bits ^ Long.MAX_VALUE : bits;
        } else {
            bits = bits >= 0 ? bits ^ Long.MIN_VALUE : ~bits;
        }
        Unsafe.putLong(addr, Long.reverseBytes(bits));
    }

    private static void encodeFloat(long addr, float value, boolean desc) {
        int bits = Float.floatToRawIntBits(value);
        if (desc) {
            bits = bits >= 0 ? bits ^ Integer.MAX_VALUE : bits;
        } else {
            bits = bits >= 0 ? bits ^ Integer.MIN_VALUE : ~bits;
        }
        Unsafe.putInt(addr, Integer.reverseBytes(bits));
    }

    private static void encodeInt(long addr, int value, boolean desc) {
        Unsafe.putInt(addr, Integer.reverseBytes(value ^ (desc ? 0x7FFFFFFF : 0x80000000)));
    }

    private static void encodeLong(long addr, long value, boolean desc) {
        Unsafe.putLong(addr, Long.reverseBytes(value ^ (desc ? Long.MAX_VALUE : Long.MIN_VALUE)));
    }

    private static void encodeLong256(long addr, Long256 value, boolean desc) {
        long l0 = value.getLong0();
        long l1 = value.getLong1();
        long l2 = value.getLong2();
        long l3 = value.getLong3();
        if (Long256Impl.isNull(l0, l1, l2, l3)) {
            l0 = 0;
            l1 = 0;
            l2 = 0;
            l3 = 0;
        } else if (isBelowLong256Null(l0, l1, l2, l3)) {
            if (++l0 == 0 && ++l1 == 0 && ++l2 == 0) {
                l3++;
            }
        }
        encodeUnsignedLong(addr, l3, desc);
        encodeUnsignedLong(addr + 8, l2, desc);
        encodeUnsignedLong(addr + 16, l1, desc);
        encodeUnsignedLong(addr + 24, l0, desc);
    }

    private static void encodeShort(long addr, short value, boolean desc) {
        Unsafe.putShort(addr, Short.reverseBytes((short) (value ^ (desc ? 0x7FFF : 0x8000))));
    }

    private static void encodeUnsignedInt(long addr, int value, boolean desc) {
        Unsafe.putInt(addr, Integer.reverseBytes(desc ? ~value : value));
    }

    private static void encodeUnsignedLong(long addr, long value, boolean desc) {
        Unsafe.putLong(addr, Long.reverseBytes(desc ? ~value : value));
    }

    private static void encodeUnsignedRank(long addr, int rank, int byteWidth, boolean desc) {
        switch (byteWidth) {
            case 1 -> Unsafe.putByte(addr, (byte) (desc ? ~rank : rank));
            case 2 -> Unsafe.putShort(addr, Short.reverseBytes((short) (desc ? ~rank : rank)));
            default -> Unsafe.putInt(addr, Integer.reverseBytes(desc ? ~rank : rank));
        }
    }

    private static void encodeUuid(long addr, long hi, long lo, boolean desc) {
        if (Uuid.isNull(lo, hi)) {
            hi = 0;
            lo = 0;
        } else if (Long.compareUnsigned(hi, Numbers.LONG_NULL) < 0
                || (hi == Numbers.LONG_NULL && Long.compareUnsigned(lo, Numbers.LONG_NULL) < 0)) {
            if (++lo == 0) {
                hi++;
            }
        }
        encodeUnsignedLong(addr, hi, desc);
        encodeUnsignedLong(addr + 8, lo, desc);
    }

    private static int fixedColumnByteWidth(int columnType) {
        return switch (ColumnType.tagOf(columnType)) {
            case ColumnType.BOOLEAN, ColumnType.BYTE, ColumnType.GEOBYTE, ColumnType.DECIMAL8 -> 1;
            case ColumnType.SHORT, ColumnType.GEOSHORT, ColumnType.CHAR, ColumnType.DECIMAL16 -> 2;
            case ColumnType.INT, ColumnType.GEOINT, ColumnType.IPv4, ColumnType.FLOAT, ColumnType.DECIMAL32 -> 4;
            case ColumnType.LONG, ColumnType.DATE, ColumnType.TIMESTAMP, ColumnType.DOUBLE, ColumnType.GEOLONG,
                 ColumnType.DECIMAL64 -> 8;
            case ColumnType.DECIMAL128, ColumnType.LONG128, ColumnType.UUID -> 16;
            case ColumnType.DECIMAL256, ColumnType.LONG256 -> 32;
            default -> -1;
        };
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

    private static boolean hasByteFF(long word) {
        final long inv = ~word;
        return ((inv - 0x0101010101010101L) & word & 0x8080808080808080L) != 0;
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

    private static CairoException invalidVarcharUtf8() {
        return CairoException.nonCritical().put("cannot ORDER BY a VARCHAR with invalid UTF-8 (contains a 0xFF byte)");
    }

    private static boolean isBelowLong256Null(long l0, long l1, long l2, long l3) {
        int cmp = Long.compareUnsigned(l3, Numbers.LONG_NULL);
        if (cmp != 0) {
            return cmp < 0;
        }
        cmp = Long.compareUnsigned(l2, Numbers.LONG_NULL);
        if (cmp != 0) {
            return cmp < 0;
        }
        cmp = Long.compareUnsigned(l1, Numbers.LONG_NULL);
        if (cmp != 0) {
            return cmp < 0;
        }
        return Long.compareUnsigned(l0, Numbers.LONG_NULL) < 0;
    }

    private static boolean isEncodable(int columnType) {
        return switch (columnType) {
            case ColumnType.STRING, ColumnType.VARCHAR, ColumnType.SYMBOL -> true;
            default -> fixedColumnByteWidth(columnType) >= 0;
        };
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

    private void appendUtf16(CharSequence value, boolean desc) {
        final byte mask = desc ? (byte) 0xFF : 0;
        if (value == null) {
            Unsafe.putByte(keyHeap.appendAddressFor(1), mask);
            return;
        }
        final int n = value.length();
        final long offset = keyHeap.getAppendOffset();
        long p = keyHeap.appendAddressFor(4L * n + 2);
        final long base = p;
        Unsafe.putByte(p++, (byte) (1 ^ mask));
        for (int i = 0; i < n; i++) {
            final char c = value.charAt(i);
            final int hi = c >>> 8;
            final int lo = c & 0xFF;
            if (hi <= 1) {
                Unsafe.putByte(p++, (byte) (1 ^ mask));
            }
            Unsafe.putByte(p++, (byte) (hi ^ mask));
            if (lo <= 1) {
                Unsafe.putByte(p++, (byte) (1 ^ mask));
            }
            Unsafe.putByte(p++, (byte) (lo ^ mask));
        }
        Unsafe.putByte(p++, mask);
        keyHeap.jumpTo(offset + (p - base));
    }

    private void appendVarchar(Utf8Sequence value, boolean desc) {
        final byte mask = desc ? (byte) 0xFF : 0;
        if (value == null) {
            Unsafe.putByte(keyHeap.appendAddressFor(1), mask);
            return;
        }
        final int size = value.size();
        long p = keyHeap.appendAddressFor(size + 2L);
        Unsafe.putByte(p++, (byte) (1 ^ mask));
        final long wordMask = desc ? -1L : 0;
        int i = 0;
        for (; i + 8 <= size; i += 8, p += 8) {
            final long word = value.longAt(i);
            // The +1 shift reserves 0x00 as the terminator; a 0xFF byte would
            // wrap to 0x00 and break the order. Valid UTF-8 has no 0xFF.
            if (hasByteFF(word)) {
                throw invalidVarcharUtf8();
            }
            Unsafe.putLong(p, (word + 0x0101010101010101L) ^ wordMask);
        }
        for (; i < size; i++, p++) {
            final byte b = value.byteAt(i);
            if (b == (byte) 0xFF) {
                throw invalidVarcharUtf8();
            }
            Unsafe.putByte(p, (byte) ((b + 1) ^ mask));
        }
        Unsafe.putByte(p, mask);
    }

    private void encodeFixed8(Record record, long destAddr, long rowId) {
        int colIdx = columnIndices[0];
        int colType = columnTypes[0];
        boolean desc = isDesc[0];
        int shift = (8 - columnByteWidths[0]) * 8;
        long key;
        if (isStaticSymbol[0]) {
            int symKey = record.getInt(colIdx);
            int rank = (symKey < 0 || symKey >= rankMapSizes[0]) ? 0 : rankMaps.getQuick(0).get(symKey);
            key = Integer.toUnsignedLong(desc ? ~rank : rank) << shift;
            Unsafe.putLong(destAddr, key);
            Unsafe.putLong(destAddr + 8, rowId);
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
        Unsafe.putLong(destAddr, key);
        Unsafe.putLong(destAddr + 8, rowId);
    }

    private void encodeFixedColumn(Record record, int i, long addr) {
        int colIdx = columnIndices[i];
        boolean desc = isDesc[i];

        if (isStaticSymbol[i]) {
            int key = record.getInt(colIdx);
            int rank = (key < 0 || key >= rankMapSizes[i]) ? 0 : rankMaps.getQuick(i).get(key);
            encodeUnsignedRank(addr, rank, columnByteWidths[i], desc);
            return;
        }
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
            case ColumnType.LONG128 -> {
                encodeLong(addr, record.getLong128Hi(colIdx), desc);
                encodeUnsignedLong(addr + 8, record.getLong128Lo(colIdx), desc);
            }
            case ColumnType.UUID -> encodeUuid(addr, record.getLong128Hi(colIdx), record.getLong128Lo(colIdx), desc);
            case ColumnType.LONG256 -> encodeLong256(addr, record.getLong256A(colIdx), desc);
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
            default ->
                    throw CairoException.nonCritical().put("unexpected type in encodeFixedColumn: ").put(ColumnType.nameOf(columnTypes[i]));
        }
    }

    private void encodeGeneric(Record record, long destAddr) {
        for (int i = 0; i < columnIndices.length; i++) {
            encodeFixedColumn(record, i, destAddr + offsets[i]);
        }

        // Reverse each 8-byte word from big-endian to native byte order.
        // The encoding writes big-endian bytes (for memcmp ordering), but
        // the native sort compares uint64_t values.
        int keyLen = keyType.keyLength();
        int lastWord = keyLen - 8;
        for (int w = 0; w < lastWord; w += 8) {
            long val = Unsafe.getLong(destAddr + w);
            Unsafe.putLong(destAddr + w, Long.reverseBytes(val));
        }
        long val = Unsafe.getLong(destAddr + lastWord);
        Unsafe.putLong(destAddr + lastWord, Long.reverseBytes(val & padMask));
    }

    private void encodeVariable(Record record, long destAddr, long rowId) {
        final MemoryCARW heap = keyHeap;
        final long start = heap.getAppendOffset();
        for (int i = 0; i < columnIndices.length; i++) {
            if (columnByteWidths[i] >= 0) {
                encodeFixedColumn(record, i, heap.appendAddressFor(columnByteWidths[i]));
            } else {
                switch (columnTypes[i]) {
                    case ColumnType.VARCHAR -> appendVarchar(record.getVarcharA(columnIndices[i]), isDesc[i]);
                    case ColumnType.STRING -> appendUtf16(record.getStrA(columnIndices[i]), isDesc[i]);
                    case ColumnType.SYMBOL -> appendUtf16(record.getSymA(columnIndices[i]), isDesc[i]);
                    default ->
                            throw CairoException.nonCritical().put("unexpected type in encodeVariable: ").put(ColumnType.nameOf(columnTypes[i]));
                }
            }
        }
        final long len = heap.getAppendOffset() - start;
        if (len < KEY_PREFIX_BYTES) {
            long padAddr = heap.appendAddressFor(KEY_PREFIX_BYTES);
            Unsafe.putLong(padAddr, 0);
            Unsafe.putLong(padAddr + 8, 0);
        }
        final long keyAddr = heap.addressOf(start);
        Unsafe.putLong(destAddr, Long.reverseBytes(Unsafe.getLong(keyAddr)));
        Unsafe.putLong(destAddr + 8, Long.reverseBytes(Unsafe.getLong(keyAddr + 8)));
        Unsafe.putLong(destAddr + 16, len);
        Unsafe.putLong(destAddr + 24, start);
        Unsafe.putLong(destAddr + 32, rowId);
        heap.jumpTo(len <= KEY_PREFIX_BYTES ? start : start + len);
    }
}
