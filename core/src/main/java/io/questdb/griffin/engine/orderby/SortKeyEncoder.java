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

import io.questdb.PropertyKey;
import io.questdb.cairo.CairoException;
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.sql.PageFrameMemory;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.RecordMetadata;
import io.questdb.cairo.sql.StaticSymbolTable;
import io.questdb.cairo.sql.SymbolTable;
import io.questdb.cairo.sql.SymbolTableSource;
import io.questdb.griffin.engine.LimitOverflowException;
import io.questdb.griffin.engine.RecordComparator;
import io.questdb.griffin.engine.functions.SymbolFunction;
import io.questdb.std.Chars;
import io.questdb.std.Decimal128;
import io.questdb.std.Decimal256;
import io.questdb.std.DirectIntList;
import io.questdb.std.DirectLongList;
import io.questdb.std.IntHashSet;
import io.questdb.std.IntList;
import io.questdb.std.MemoryTag;
import io.questdb.std.Misc;
import io.questdb.std.ObjList;
import io.questdb.std.QuietCloseable;
import io.questdb.std.Rows;
import io.questdb.std.Unsafe;

/**
 * Analyzes sort column types at compile time, produces a fixed-width
 * byte-comparable key encoder, and serializes sort columns into native memory.
 * After encoding, the key can be compared using unsigned long comparison
 * to determine sort order.
 */
public class SortKeyEncoder implements QuietCloseable {
    // Vergesort's index space caps a sort at (2^32 - 2) entries of 8 bytes.
    public static final long MAX_ENTRY_HEAP_BYTES = (Integer.toUnsignedLong(-1) - 1) << 3;
    private final int[] columnByteWidths;
    private final int[] columnIndices;
    private final int[] columnTypes;
    private final Decimal128 decimal128Sink;
    private final Decimal256 decimal256Sink;
    private final boolean hasBorrowedRankMaps;
    private final boolean[] isDesc;
    private final boolean isSingleColumnFixed8;
    private final boolean[] isSymbol;
    private final int[] offsets;
    private final int[] rankMapSizes;
    private final ObjList<DirectIntList> rankMaps;
    private SortKeyType keyType;
    private long padMask;

    public SortKeyEncoder(RecordMetadata metadata, IntList sortColumnFilter) {
        this(metadata, sortColumnFilter, null);
    }

    /**
     * A non-null {@code rankMapOwner} makes this encoder share the owner's rank
     * maps for its whole lifetime: building a rank map sorts the whole symbol
     * dictionary, so per-worker encoders share the owner's read-only maps instead
     * of building identical ones. A sharing encoder is initialized with
     * {@link #initFrom(SortKeyEncoder)}, and its {@link #close()} leaves the maps
     * to the owner.
     */
    public SortKeyEncoder(RecordMetadata metadata, IntList sortColumnFilter, SortKeyEncoder rankMapOwner) {
        int n = sortColumnFilter.size();
        this.columnIndices = new int[n];
        this.columnTypes = new int[n];
        this.isDesc = new boolean[n];
        this.isSymbol = new boolean[n];
        this.offsets = new int[n];
        this.columnByteWidths = new int[n];
        this.rankMapSizes = new int[n];
        boolean hasDecimal128 = false;
        boolean hasDecimal256 = false;
        this.hasBorrowedRankMaps = rankMapOwner != null;
        this.rankMaps = hasBorrowedRankMaps ? rankMapOwner.rankMaps : new ObjList<>(n);

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
            }
            if (!hasBorrowedRankMaps) {
                rankMaps.add(isSymbol[i] ? new DirectIntList(1024, MemoryTag.NATIVE_DEFAULT, true) : null);
            }
        }
        this.isSingleColumnFixed8 = n == 1 && columnByteWidths[0] <= 8;
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

    public static IntHashSet extractSortKeyColumnIndexes(IntList sortColumnFilter) {
        final IntHashSet indexes = new IntHashSet(sortColumnFilter.size());
        for (int i = 0, n = sortColumnFilter.size(); i < n; i++) {
            final int encoded = sortColumnFilter.getQuick(i);
            indexes.add((encoded > 0 ? encoded : -encoded) - 1);
        }
        return indexes;
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

    /**
     * Returns how many encoded entries fit the two sort budgets. The settings size
     * the tree chain's separate key and value heaps; the encoded layout splits each
     * entry the same way - key bytes against the key budget, the rowId word against
     * the light value budget - so each setting keeps binding on its own even when
     * the other is left at its effectively unbounded default.
     */
    public static long maxEntries(long keyCapBytes, long valueCapBytes, SortKeyType keyType) {
        final long keyCap = Math.min(keyCapBytes, MAX_ENTRY_HEAP_BYTES);
        final long valueCap = Math.min(valueCapBytes, MAX_ENTRY_HEAP_BYTES);
        return Math.min(
                Math.min(keyCap / keyType.keyLength(), valueCap / Long.BYTES),
                MAX_ENTRY_HEAP_BYTES / keyType.entrySize()
        );
    }

    @SuppressWarnings("unused") // called from generated bytecode (RecordComparatorCompiler)
    public static int rank(Object rankMap, int key) {
        return key < 0 ? 0 : ((DirectIntList) rankMap).get(key);
    }

    public static void throwSortHeapOverflow(long maxEntryMemBytes) {
        throw LimitOverflowException.instance()
                .put("limit of ").put(maxEntryMemBytes)
                .put(" memory exceeded in EncodedSort (raise ")
                .put(PropertyKey.CAIRO_SQL_SORT_KEY_MAX_BYTES.getPropertyPath())
                .put(" or ")
                .put(PropertyKey.CAIRO_SQL_SORT_LIGHT_VALUE_MAX_BYTES.getPropertyPath())
                .put(')');
    }

    @Override
    public void close() {
        if (!hasBorrowedRankMaps) {
            Misc.freeObjListAndKeepObjects(rankMaps);
        }
    }

    public void encode(Record record, long destAddr, long rowId) {
        if (isSingleColumnFixed8) {
            encodeFixed8(record, destAddr, rowId);
            return;
        }
        encodeGeneric(record, destAddr);
        Unsafe.putLong(destAddr + keyType.rowIdOffset(), rowId);
    }

    /**
     * Encodes every frame row into the buffer in one pass, with the column
     * address, type dispatch and direction transform hoisted out of the per-row
     * loop. Returns false when the batch does not apply - multi-column key, or
     * the column is absent from the frame - and the caller must fall back to
     * per-row {@link #encode(Record, long, long)}.
     */
    public boolean encodeFixed8Frame(PageFrameMemory frameMemory, int frameIndex, long frameRowCount, EncodedTopKBuffer topK) {
        return encodeFixed8Batch(frameMemory, frameIndex, null, frameRowCount, topK);
    }

    /**
     * Filtered variant of {@link #encodeFixed8Frame(PageFrameMemory, int, long, EncodedTopKBuffer)}:
     * encodes only the frame-local row indexes in {@code rows}.
     */
    public boolean encodeFixed8Frame(PageFrameMemory frameMemory, int frameIndex, DirectLongList rows, EncodedTopKBuffer topK) {
        return encodeFixed8Batch(frameMemory, frameIndex, rows, rows.size(), topK);
    }

    public SortKeyType init(SymbolTableSource symbolTableSource) {
        // A borrowing encoder shares the owner's maps; rebuilding here would
        // mutate them behind the owner's back.
        assert !hasBorrowedRankMaps;
        int totalBytes = 0;

        for (int i = 0; i < columnIndices.length; i++) {
            offsets[i] = totalBytes;
            if (isSymbol[i]) {
                StaticSymbolTable sst = getStaticSymbolTable(symbolTableSource.getSymbolTable(columnIndices[i]));
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
            totalBytes += columnByteWidths[i];
        }

        keyType = SortKeyType.fromKeyLength(totalBytes);
        int padBytes = keyType.keyLength() - totalBytes;
        padMask = padBytes > 0 ? (1L << ((8 - padBytes) * 8)) - 1 : -1L;
        return keyType;
    }

    /**
     * Adopts the layout of an already-initialized encoder this one was constructed
     * to share rank maps with; the maps themselves need no copying as both encoders
     * reference the same list.
     */
    public SortKeyType initFrom(SortKeyEncoder owner) {
        assert rankMaps == owner.rankMaps;
        for (int i = 0; i < columnIndices.length; i++) {
            offsets[i] = owner.offsets[i];
            columnByteWidths[i] = owner.columnByteWidths[i];
            rankMapSizes[i] = owner.rankMapSizes[i];
        }
        keyType = owner.keyType;
        padMask = owner.padMask;
        return keyType;
    }

    private static void appendEntry(EncodedTopKBuffer topK, long key, long rowId) {
        if (topK.fastRejectsKey(key)) {
            return;
        }
        final long addr = topK.beginAppend();
        Unsafe.putLong(addr, key);
        Unsafe.putLong(addr + 8, rowId);
        topK.endAppend();
    }

    private static void batchDouble(EncodedTopKBuffer topK, long colAddr, long rowIdBase, DirectLongList rows, long rowCount, boolean desc) {
        if (rows == null) {
            for (long r = 0; r < rowCount; r++) {
                appendEntry(topK, doubleKey(Unsafe.getDouble(colAddr + (r << 3)), desc), rowIdBase + r);
            }
        } else {
            for (long p = 0; p < rowCount; p++) {
                final long r = rows.get(p);
                appendEntry(topK, doubleKey(Unsafe.getDouble(colAddr + (r << 3)), desc), rowIdBase + r);
            }
        }
    }

    private static void batchFloat(EncodedTopKBuffer topK, long colAddr, long rowIdBase, DirectLongList rows, long rowCount, boolean desc) {
        if (rows == null) {
            for (long r = 0; r < rowCount; r++) {
                appendEntry(topK, floatKey(Unsafe.getFloat(colAddr + (r << 2)), desc), rowIdBase + r);
            }
        } else {
            for (long p = 0; p < rowCount; p++) {
                final long r = rows.get(p);
                appendEntry(topK, floatKey(Unsafe.getFloat(colAddr + (r << 2)), desc), rowIdBase + r);
            }
        }
    }

    private static void batchIntegral1(EncodedTopKBuffer topK, long colAddr, long rowIdBase, DirectLongList rows, long rowCount, long xorMask) {
        if (rows == null) {
            for (long r = 0; r < rowCount; r++) {
                appendEntry(topK, ((Unsafe.getByte(colAddr + r) & 0xFFL) ^ xorMask) << 56, rowIdBase + r);
            }
        } else {
            for (long p = 0; p < rowCount; p++) {
                final long r = rows.get(p);
                appendEntry(topK, ((Unsafe.getByte(colAddr + r) & 0xFFL) ^ xorMask) << 56, rowIdBase + r);
            }
        }
    }

    private static void batchIntegral2(EncodedTopKBuffer topK, long colAddr, long rowIdBase, DirectLongList rows, long rowCount, long xorMask) {
        if (rows == null) {
            for (long r = 0; r < rowCount; r++) {
                appendEntry(topK, ((Unsafe.getShort(colAddr + (r << 1)) & 0xFFFFL) ^ xorMask) << 48, rowIdBase + r);
            }
        } else {
            for (long p = 0; p < rowCount; p++) {
                final long r = rows.get(p);
                appendEntry(topK, ((Unsafe.getShort(colAddr + (r << 1)) & 0xFFFFL) ^ xorMask) << 48, rowIdBase + r);
            }
        }
    }

    private static void batchIntegral4(EncodedTopKBuffer topK, long colAddr, long rowIdBase, DirectLongList rows, long rowCount, long xorMask) {
        if (rows == null) {
            for (long r = 0; r < rowCount; r++) {
                appendEntry(topK, ((Unsafe.getInt(colAddr + (r << 2)) & 0xFFFFFFFFL) ^ xorMask) << 32, rowIdBase + r);
            }
        } else {
            for (long p = 0; p < rowCount; p++) {
                final long r = rows.get(p);
                appendEntry(topK, ((Unsafe.getInt(colAddr + (r << 2)) & 0xFFFFFFFFL) ^ xorMask) << 32, rowIdBase + r);
            }
        }
    }

    private static void batchIntegral8(EncodedTopKBuffer topK, long colAddr, long rowIdBase, DirectLongList rows, long rowCount, long xorMask) {
        if (rows == null) {
            for (long r = 0; r < rowCount; r++) {
                appendEntry(topK, Unsafe.getLong(colAddr + (r << 3)) ^ xorMask, rowIdBase + r);
            }
        } else {
            for (long p = 0; p < rowCount; p++) {
                final long r = rows.get(p);
                appendEntry(topK, Unsafe.getLong(colAddr + (r << 3)) ^ xorMask, rowIdBase + r);
            }
        }
    }

    private static int columnByteWidth(int columnType, RecordMetadata metadata, int columnIndex) {
        return switch (columnType) {
            case ColumnType.BOOLEAN, ColumnType.BYTE, ColumnType.GEOBYTE, ColumnType.DECIMAL8 -> 1;
            case ColumnType.SHORT, ColumnType.GEOSHORT, ColumnType.CHAR, ColumnType.DECIMAL16 -> 2;
            case ColumnType.INT, ColumnType.GEOINT, ColumnType.IPv4, ColumnType.FLOAT, ColumnType.DECIMAL32 -> 4;
            case ColumnType.LONG, ColumnType.DATE, ColumnType.TIMESTAMP, ColumnType.DOUBLE, ColumnType.GEOLONG,
                 ColumnType.DECIMAL64 -> 8;
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

    private static long doubleKey(double value, boolean desc) {
        final long bits = Double.doubleToLongBits(value);
        if (desc) {
            return bits >= 0 ? bits ^ Long.MAX_VALUE : bits;
        }
        return bits >= 0 ? bits ^ Long.MIN_VALUE : ~bits;
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
        long bits = Double.doubleToLongBits(value);
        if (desc) {
            bits = bits >= 0 ? bits ^ Long.MAX_VALUE : bits;
        } else {
            bits = bits >= 0 ? bits ^ Long.MIN_VALUE : ~bits;
        }
        Unsafe.putLong(addr, Long.reverseBytes(bits));
    }

    private static void encodeFloat(long addr, float value, boolean desc) {
        int bits = Float.floatToIntBits(value);
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

    private static long floatKey(float value, boolean desc) {
        int bits = Float.floatToIntBits(value);
        if (desc) {
            bits = bits >= 0 ? bits ^ Integer.MAX_VALUE : bits;
        } else {
            bits = bits >= 0 ? bits ^ Integer.MIN_VALUE : ~bits;
        }
        return Integer.toUnsignedLong(bits) << 32;
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

    private void batchSymbol(EncodedTopKBuffer topK, long colAddr, long rowIdBase, DirectLongList rows, long rowCount, boolean desc) {
        final DirectIntList rankMap = rankMaps.getQuick(0);
        final int rankMapSize = rankMapSizes[0];
        final int shift = (8 - columnByteWidths[0]) * 8;
        if (rows == null) {
            for (long r = 0; r < rowCount; r++) {
                final int symKey = Unsafe.getInt(colAddr + (r << 2));
                final int rank = (symKey < 0 || symKey >= rankMapSize) ? 0 : rankMap.get(symKey);
                appendEntry(topK, Integer.toUnsignedLong(desc ? ~rank : rank) << shift, rowIdBase + r);
            }
        } else {
            for (long p = 0; p < rowCount; p++) {
                final long r = rows.get(p);
                final int symKey = Unsafe.getInt(colAddr + (r << 2));
                final int rank = (symKey < 0 || symKey >= rankMapSize) ? 0 : rankMap.get(symKey);
                appendEntry(topK, Integer.toUnsignedLong(desc ? ~rank : rank) << shift, rowIdBase + r);
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
                int bits = Float.floatToIntBits(record.getFloat(colIdx));
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
                long bits = Double.doubleToLongBits(record.getDouble(colIdx));
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

    private boolean encodeFixed8Batch(PageFrameMemory frameMemory, int frameIndex, DirectLongList rows, long rowCount, EncodedTopKBuffer topK) {
        if (!isSingleColumnFixed8) {
            return false;
        }
        final long colAddr = frameMemory.getPageAddress(columnIndices[0]);
        if (colAddr == 0) {
            // Column top: every frame row is NULL for this column.
            return false;
        }
        final long rowIdBase = Rows.toRowID(frameIndex, 0);
        final boolean desc = isDesc[0];
        switch (columnTypes[0]) {
            case ColumnType.SYMBOL -> batchSymbol(topK, colAddr, rowIdBase, rows, rowCount, desc);
            case ColumnType.FLOAT -> batchFloat(topK, colAddr, rowIdBase, rows, rowCount, desc);
            case ColumnType.DOUBLE -> batchDouble(topK, colAddr, rowIdBase, rows, rowCount, desc);
            case ColumnType.BOOLEAN -> batchIntegral1(topK, colAddr, rowIdBase, rows, rowCount, desc ? 0xFFL : 0L);
            case ColumnType.BYTE, ColumnType.GEOBYTE, ColumnType.DECIMAL8 ->
                    batchIntegral1(topK, colAddr, rowIdBase, rows, rowCount, desc ? 0x7FL : 0x80L);
            case ColumnType.SHORT, ColumnType.GEOSHORT, ColumnType.DECIMAL16 ->
                    batchIntegral2(topK, colAddr, rowIdBase, rows, rowCount, desc ? 0x7FFFL : 0x8000L);
            case ColumnType.CHAR -> batchIntegral2(topK, colAddr, rowIdBase, rows, rowCount, desc ? 0xFFFFL : 0L);
            case ColumnType.INT, ColumnType.GEOINT, ColumnType.DECIMAL32 ->
                    batchIntegral4(topK, colAddr, rowIdBase, rows, rowCount, desc ? 0x7FFFFFFFL : 0x80000000L);
            case ColumnType.IPv4 -> batchIntegral4(topK, colAddr, rowIdBase, rows, rowCount, desc ? 0xFFFFFFFFL : 0L);
            case ColumnType.LONG, ColumnType.GEOLONG, ColumnType.TIMESTAMP, ColumnType.DATE, ColumnType.DECIMAL64 ->
                    batchIntegral8(topK, colAddr, rowIdBase, rows, rowCount, desc ? Long.MAX_VALUE : Long.MIN_VALUE);
            default -> throw new AssertionError("unexpected FIXED_8 type: " + ColumnType.nameOf(columnTypes[0]));
        }
        return true;
    }

    private void encodeGeneric(Record record, long destAddr) {
        for (int i = 0; i < columnIndices.length; i++) {
            int colIdx = columnIndices[i];
            long addr = destAddr + offsets[i];
            boolean desc = isDesc[i];

            if (isSymbol[i]) {
                int key = record.getInt(colIdx);
                int rank = (key < 0 || key >= rankMapSizes[i]) ? 0 : rankMaps.getQuick(i).get(key);
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
                    default ->
                            throw CairoException.nonCritical().put("unexpected type in encodeGeneric: ").put(ColumnType.nameOf(columnTypes[i]));
                }
            }
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
}
