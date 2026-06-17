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
import io.questdb.cairo.TableUtils;
import io.questdb.cairo.VarcharTypeDriver;
import io.questdb.cairo.sql.PageFrameMemory;
import io.questdb.cairo.sql.PageFrameMemoryRecord;
import io.questdb.cairo.sql.PartitionFormat;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.RecordMetadata;
import io.questdb.cairo.sql.StaticSymbolTable;
import io.questdb.cairo.sql.SymbolTable;
import io.questdb.cairo.sql.SymbolTableSource;
import io.questdb.cairo.vm.Vm;
import io.questdb.cairo.vm.api.MemoryCARW;
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
import io.questdb.std.Long256;
import io.questdb.std.Long256Impl;
import io.questdb.std.MemoryTag;
import io.questdb.std.Misc;
import io.questdb.std.Numbers;
import io.questdb.std.ObjList;
import io.questdb.std.QuietCloseable;
import io.questdb.std.Rows;
import io.questdb.std.Unsafe;
import io.questdb.std.Uuid;
import io.questdb.std.str.DirectString;
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
    public static final long MAX_ENTRY_HEAP_BYTES = (Integer.toUnsignedLong(-1) - 1) << 3;
    private final int[] columnByteWidths;
    private final int[] columnIndices;
    private final int[] columnTypes;
    private final Decimal128 decimal128Sink;
    private final Decimal256 decimal256Sink;
    private final boolean hasBorrowedRankMaps;
    private final boolean[] isDesc;
    private final boolean[] isStaticSymbol;
    // Encode fast-path selector; keyType is the entry layout (the two are not redundant).
    private final KeyShape keyShape;
    private final int[] offsets;
    private final int[] rankMapSizes;
    private final ObjList<DirectIntList> rankMaps;
    private final DirectString stringView;
    private MemoryCARW keyHeap;
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
        this.isStaticSymbol = new boolean[n];
        this.offsets = new int[n];
        this.columnByteWidths = new int[n];
        this.rankMapSizes = new int[n];
        boolean hasDecimal128 = false;
        boolean hasDecimal256 = false;
        this.hasBorrowedRankMaps = rankMapOwner != null;
        this.rankMaps = hasBorrowedRankMaps ? rankMapOwner.rankMaps : new ObjList<>(n);

        try {
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
                    if (!hasBorrowedRankMaps) {
                        rankMaps.add(new DirectIntList(1024, MemoryTag.NATIVE_DEFAULT, true));
                    }
                } else {
                    columnByteWidths[i] = fixedColumnByteWidth(columnTypes[i]);
                    if (!hasBorrowedRankMaps) {
                        rankMaps.add(null);
                    }
                }
            }
        } catch (Throwable th) {
            if (!hasBorrowedRankMaps) {
                Misc.freeObjList(rankMaps);
            }
            throw th;
        }
        // A static SYMBOL has columnByteWidths[0] == 4 and lands in FIXED8; only a
        // non-static SYMBOL reaches KeyShape.SYMBOL.
        if (n != 1) {
            this.keyShape = KeyShape.GENERIC;
        } else if (columnByteWidths[0] >= 0 && columnByteWidths[0] <= 8) {
            this.keyShape = KeyShape.FIXED8;
        } else {
            this.keyShape = switch (columnTypes[0]) {
                case ColumnType.VARCHAR -> KeyShape.VARCHAR;
                case ColumnType.STRING -> KeyShape.STRING;
                case ColumnType.SYMBOL -> KeyShape.SYMBOL;
                case ColumnType.UUID, ColumnType.LONG128, ColumnType.LONG256 -> KeyShape.FIXED_WIDE;
                default -> KeyShape.GENERIC;
            };
        }
        this.decimal128Sink = hasDecimal128 ? new Decimal128() : null;
        this.decimal256Sink = hasDecimal256 ? new Decimal256() : null;
        this.stringView = keyShape == KeyShape.STRING ? new DirectString() : null;
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
        if (keyShape == KeyShape.FIXED8) {
            encodeFixed8(record, destAddr, rowId);
            return;
        }
        if (keyType.isVariable()) {
            encodeVariable(record, destAddr, rowId);
            return;
        }
        // FIXED_WIDE and multi-column fixed keys both go through encodeGeneric, which
        // reverses each word to native order to match the entry comparison.
        encodeGeneric(record, destAddr);
        Unsafe.putLong(destAddr + keyType.rowIdOffset(), rowId);
    }

    /**
     * Encodes a whole page frame into the top-K buffer in one pass, dispatching once on
     * the key shape to a monomorphic batch loop that hoists the column address, type
     * dispatch and direction transform out of the per-row work. A non-null {@code rows}
     * restricts the pass to those frame-local row indexes (filtered scan); a null
     * {@code rows} walks all {@code frameRowCount} rows. A shape with no batch loop, and
     * a batch that declines a column-top frame, fall back to per-row {@link #encodeTopK}.
     */
    public void encodeFrame(PageFrameMemory frameMemory, int frameIndex, DirectLongList rows, long frameRowCount, EncodedTopKBuffer topK, PageFrameMemoryRecord record) {
        final long count = rows != null ? rows.size() : frameRowCount;
        switch (keyShape) {
            case FIXED8:
                if (encodeFixed8Batch(frameMemory, frameIndex, rows, count, topK)) {
                    return;
                }
                break;
            case FIXED_WIDE:
                if (encodeFixedWideBatch(frameMemory, frameIndex, rows, count, topK)) {
                    return;
                }
                break;
            case VARCHAR:
                if (encodeVarcharBatch(frameMemory, frameIndex, rows, count, topK, record)) {
                    return;
                }
                break;
            case STRING:
                if (encodeStringBatch(frameMemory, frameIndex, rows, count, topK, record)) {
                    return;
                }
                break;
            default:
                break;
        }
        if (rows == null) {
            for (long r = 0; r < count; r++) {
                record.setRowIndex(r);
                encodeTopK(record, record.getRowId(), topK);
            }
        } else {
            for (long p = 0; p < count; p++) {
                record.setRowIndex(rows.get(p));
                encodeTopK(record, record.getRowId(), topK);
            }
        }
    }

    /**
     * Encodes one row into the top-K buffer, dropping it up front when the buffer's
     * threshold already excludes its leading prefix word. For a variable-length key
     * this skips the full key encode and key-heap write for the rows that lose,
     * which is the bulk of a scan once the top-K is warm. Fixed keys encode inline
     * and cheaply, so they fall straight through to the full encode.
     */
    public void encodeTopK(Record record, long rowId, EncodedTopKBuffer topK) {
        if (keyType.isVariable() && variableRowRejected(record, rowId, topK)) {
            return;
        }
        encode(record, topK.beginAppend(), rowId);
        topK.endAppend();
    }

    public SortKeyType init(SymbolTableSource symbolTableSource) {
        // A borrowing encoder shares the owner's maps; rebuilding here would
        // mutate them behind the owner's back.
        assert !hasBorrowedRankMaps;
        int totalBytes = 0;
        boolean hasVarLength = false;

        for (int i = 0; i < columnIndices.length; i++) {
            if (isStaticSymbol[i]) {
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

    public void setKeyHeap(MemoryCARW keyHeap) {
        this.keyHeap = keyHeap;
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

    // LONG128/UUID page layout is lo at +0, hi at +8. Reject on the natural leading word
    // before encoding so a losing row pays no key write; endAppend re-checks the full key.
    private static void appendLong128Row(EncodedTopKBuffer topK, long colAddr, long rowIdBase, int rowIdOffset, long r, boolean desc) {
        final long base = colAddr + (r << 4);
        final long k1 = signedKey(Unsafe.getLong(base + 8), desc);
        if (topK.fastRejectsKey(k1)) {
            return;
        }
        final long addr = topK.beginAppend();
        Unsafe.putLong(addr, k1);
        Unsafe.putLong(addr + 8, unsignedKey(Unsafe.getLong(base), desc));
        Unsafe.putLong(addr + rowIdOffset, rowIdBase + r);
        topK.endAppend();
    }

    private static void appendLong256Row(EncodedTopKBuffer topK, long colAddr, long rowIdBase, int rowIdOffset, long r, boolean desc) {
        final long base = colAddr + (r << 5);
        long l0 = Unsafe.getLong(base);
        long l1 = Unsafe.getLong(base + 8);
        long l2 = Unsafe.getLong(base + 16);
        long l3 = Unsafe.getLong(base + 24);
        if (Long256Impl.isNull(l0, l1, l2, l3)) {
            l0 = l1 = l2 = l3 = 0;
        } else if (isBelowLong256Null(l0, l1, l2, l3) && ++l0 == 0 && ++l1 == 0 && ++l2 == 0) {
            l3++;
        }
        final long k1 = unsignedKey(l3, desc);
        if (topK.fastRejectsKey(k1)) {
            return;
        }
        final long addr = topK.beginAppend();
        Unsafe.putLong(addr, k1);
        Unsafe.putLong(addr + 8, unsignedKey(l2, desc));
        Unsafe.putLong(addr + 16, unsignedKey(l1, desc));
        Unsafe.putLong(addr + 24, unsignedKey(l0, desc));
        Unsafe.putLong(addr + rowIdOffset, rowIdBase + r);
        topK.endAppend();
    }

    private static void appendUuidRow(EncodedTopKBuffer topK, long colAddr, long rowIdBase, int rowIdOffset, long r, boolean desc) {
        final long base = colAddr + (r << 4);
        long hi = Unsafe.getLong(base + 8);
        long lo = Unsafe.getLong(base);
        if (Uuid.isNull(lo, hi)) {
            hi = 0;
            lo = 0;
        } else if (isUuidBelowNull(hi, lo) && ++lo == 0) {
            hi++;
        }
        final long k1 = unsignedKey(hi, desc);
        if (topK.fastRejectsKey(k1)) {
            return;
        }
        final long addr = topK.beginAppend();
        Unsafe.putLong(addr, k1);
        Unsafe.putLong(addr + 8, unsignedKey(lo, desc));
        Unsafe.putLong(addr + rowIdOffset, rowIdBase + r);
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

    private static void batchLong128(EncodedTopKBuffer topK, long colAddr, long rowIdBase, int rowIdOffset, DirectLongList rows, long rowCount, boolean desc) {
        if (rows == null) {
            for (long r = 0; r < rowCount; r++) {
                appendLong128Row(topK, colAddr, rowIdBase, rowIdOffset, r, desc);
            }
        } else {
            for (long p = 0; p < rowCount; p++) {
                appendLong128Row(topK, colAddr, rowIdBase, rowIdOffset, rows.get(p), desc);
            }
        }
    }

    private static void batchLong256(EncodedTopKBuffer topK, long colAddr, long rowIdBase, int rowIdOffset, DirectLongList rows, long rowCount, boolean desc) {
        if (rows == null) {
            for (long r = 0; r < rowCount; r++) {
                appendLong256Row(topK, colAddr, rowIdBase, rowIdOffset, r, desc);
            }
        } else {
            for (long p = 0; p < rowCount; p++) {
                appendLong256Row(topK, colAddr, rowIdBase, rowIdOffset, rows.get(p), desc);
            }
        }
    }

    private static void batchUuid(EncodedTopKBuffer topK, long colAddr, long rowIdBase, int rowIdOffset, DirectLongList rows, long rowCount, boolean desc) {
        if (rows == null) {
            for (long r = 0; r < rowCount; r++) {
                appendUuidRow(topK, colAddr, rowIdBase, rowIdOffset, r, desc);
            }
        } else {
            for (long p = 0; p < rowCount; p++) {
                appendUuidRow(topK, colAddr, rowIdBase, rowIdOffset, rows.get(p), desc);
            }
        }
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
        Unsafe.putLong(addr, Long.reverseBytes(signedKey(value, desc)));
    }

    private static void encodeLong256(long addr, Long256 value, boolean desc) {
        encodeLong256(addr, value.getLong0(), value.getLong1(), value.getLong2(), value.getLong3(), desc);
    }

    // word 0 = most significant l3 ... word 3 = l0.
    private static void encodeLong256(long addr, long l0, long l1, long l2, long l3, boolean desc) {
        if (Long256Impl.isNull(l0, l1, l2, l3)) {
            l0 = l1 = l2 = l3 = 0;
        } else if (isBelowLong256Null(l0, l1, l2, l3) && ++l0 == 0 && ++l1 == 0 && ++l2 == 0) {
            l3++;
        }
        Unsafe.putLong(addr, Long.reverseBytes(unsignedKey(l3, desc)));
        Unsafe.putLong(addr + 8, Long.reverseBytes(unsignedKey(l2, desc)));
        Unsafe.putLong(addr + 16, Long.reverseBytes(unsignedKey(l1, desc)));
        Unsafe.putLong(addr + 24, Long.reverseBytes(unsignedKey(l0, desc)));
    }

    private static void encodeShort(long addr, short value, boolean desc) {
        Unsafe.putShort(addr, Short.reverseBytes((short) (value ^ (desc ? 0x7FFF : 0x8000))));
    }

    private static void encodeUnsignedInt(long addr, int value, boolean desc) {
        Unsafe.putInt(addr, Integer.reverseBytes(desc ? ~value : value));
    }

    private static void encodeUnsignedLong(long addr, long value, boolean desc) {
        Unsafe.putLong(addr, Long.reverseBytes(unsignedKey(value, desc)));
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
        } else if (isUuidBelowNull(hi, lo) && ++lo == 0) {
            hi++;
        }
        Unsafe.putLong(addr, Long.reverseBytes(unsignedKey(hi, desc)));
        Unsafe.putLong(addr + 8, Long.reverseBytes(unsignedKey(lo, desc)));
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

    private static boolean isUuidBelowNull(long hi, long lo) {
        return Long.compareUnsigned(hi, Numbers.LONG_NULL) < 0
                || (hi == Numbers.LONG_NULL && Long.compareUnsigned(lo, Numbers.LONG_NULL) < 0);
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

    // Natural (compareUnsigned-comparable) key words; encode* stores the reverseBytes'd form.
    private static long signedKey(long value, boolean desc) {
        return value ^ (desc ? Long.MAX_VALUE : Long.MIN_VALUE);
    }

    private static long unsignedKey(long value, boolean desc) {
        return desc ? ~value : value;
    }

    // The length is only compared as len <= 8 (whole key fits the leading word) vs len > 8, plus
    // a tie-break for equal short keys, so the single scan stops once the word fills and len > 8.
    private static boolean utf16Rejects(CharSequence value, boolean desc, long rowId, EncodedTopKBuffer topK) {
        final long byteMask = desc ? 0xFFL : 0L;
        if (value == null) {
            return topK.fastRejectsVarEntry(byteMask << 56, 1, rowId);
        }
        long word = (1L ^ byteMask) << 56; // marker
        int shift = 48;
        long len = 1; // marker
        for (int i = 0, n = value.length(); i < n; i++) {
            final char c = value.charAt(i);
            final long hi = c >>> 8;
            final long lo = c & 0xFF;
            if (hi <= 1) {
                if (shift >= 0) {
                    word |= (1L ^ byteMask) << shift; // escape
                    shift -= 8;
                }
                len++;
            }
            if (shift >= 0) {
                word |= ((hi ^ byteMask) & 0xFFL) << shift;
                shift -= 8;
            }
            len++;
            if (lo <= 1) {
                if (shift >= 0) {
                    word |= (1L ^ byteMask) << shift; // escape
                    shift -= 8;
                }
                len++;
            }
            if (shift >= 0) {
                word |= ((lo ^ byteMask) & 0xFFL) << shift;
                shift -= 8;
            }
            len++;
            if (len > Long.BYTES) {
                return topK.fastRejectsVarEntry(word, len, rowId);
            }
        }
        if (shift >= 0) {
            word |= byteMask << shift; // terminator; trailing bytes stay zero-padded
        }
        len++; // terminator
        return topK.fastRejectsVarEntry(word, len, rowId);
    }

    /**
     * The leading 8-byte comparison word of a single VARCHAR key, computed straight
     * from the value - bit-identical to the {@code k1} word that
     * {@link #appendVarchar(Utf8Sequence, boolean, long)} writes (marker, then up to
     * seven {@code +1}-normalized value bytes, then terminator and zero padding for a
     * shorter value), but with no key-heap write or rollback. Rejects a 0xFF byte in
     * the leading word, matching the full encode's invalid-UTF-8 guard; bytes past
     * the leading word are checked by appendVarchar on the rows that survive.
     */
    private static long varcharLeadingWord(Utf8Sequence value, boolean desc) {
        final long byteMask = desc ? 0xFFL : 0L;
        if (value == null) {
            return byteMask << 56;
        }
        final int size = value.size();
        if (size >= Long.BYTES) {
            final long word = value.longAt(0);
            if (hasByteFF(word)) {
                throw invalidVarcharUtf8();
            }
            final long normalized = (word + 0x0101010101010101L) ^ (desc ? -1L : 0L);
            return ((1L ^ byteMask) << 56) | (Long.reverseBytes(normalized) >>> 8);
        }
        long word = (1L ^ byteMask) << 56;
        int shift = 48;
        for (int i = 0; i < size; i++, shift -= 8) {
            final byte b = value.byteAt(i);
            if (b == (byte) 0xFF) {
                throw invalidVarcharUtf8();
            }
            word |= (((b + 1) & 0xFFL) ^ byteMask) << shift;
        }
        if (size < 7) {
            word |= byteMask << shift; // terminator; trailing bytes stay zero-padded
        }
        return word;
    }

    /**
     * Leading word of a VARCHAR key read straight from {@code size} value bytes at a
     * native address - the no-view-materialization counterpart of
     * {@link #varcharLeadingWord(Utf8Sequence, boolean)}, used by the batched page-frame
     * path. The address must back at least {@code min(size, 8)} bytes (the aux inline
     * bytes, the data vector, or the Parquet slice all satisfy this).
     */
    private static long varcharLeadingWordFromBytes(long addr, int size, boolean desc) {
        final long byteMask = desc ? 0xFFL : 0L;
        if (size >= Long.BYTES) {
            final long word = Unsafe.getLong(addr);
            if (hasByteFF(word)) {
                throw invalidVarcharUtf8();
            }
            final long normalized = (word + 0x0101010101010101L) ^ (desc ? -1L : 0L);
            return ((1L ^ byteMask) << 56) | (Long.reverseBytes(normalized) >>> 8);
        }
        long word = (1L ^ byteMask) << 56;
        int shift = 48;
        for (int i = 0; i < size; i++, shift -= 8) {
            final byte b = Unsafe.getByte(addr + i);
            if (b == (byte) 0xFF) {
                throw invalidVarcharUtf8();
            }
            word |= (((b + 1) & 0xFFL) ^ byteMask) << shift;
        }
        if (size < 7) {
            word |= byteMask << shift; // terminator; trailing bytes stay zero-padded
        }
        return word;
    }

    /**
     * Leading word of a split VARCHAR key built from the six-byte inline prefix stored in the
     * aux entry, with the seventh value byte forced to zero. This is the data-vector-free
     * counterpart of {@link #varcharLeadingWordFromBytes(long, int, boolean)} for the batched
     * reject path: it is bit-identical to that word masked with {@code 0xFF..FF00}, so
     * {@link EncodedTopKBuffer#fastRejectsVarEntryPrefix6(long, long, long)} can compare it
     * against the (full) threshold word masked the same way. Only the six prefix bytes are
     * checked for 0xFF; bytes past the prefix are validated by appendVarchar on survivors.
     */
    private static long varcharLeadingWordFromPrefix(long auxEntry, boolean desc) {
        final long byteMask = desc ? 0xFFL : 0L;
        final long prefix = VarcharTypeDriver.getInlinedPrefixWord(auxEntry);
        if (hasByteFF(prefix)) {
            throw invalidVarcharUtf8();
        }
        final long normalized = (prefix + 0x0000010101010101L) ^ (desc ? VarcharTypeDriver.VARCHAR_INLINED_PREFIX_MASK : 0L);
        return ((1L ^ byteMask) << 56) | (Long.reverseBytes(normalized) >>> 8);
    }

    /**
     * Appends the variable-length key's normalized bytes to the key heap, one
     * sort column after another, and returns the byte length written. A
     * {@code budget} below {@link Long#MAX_VALUE} stops once that many bytes are
     * on the heap, so {@link #variableRowRejected(Record, long, EncodedTopKBuffer)}
     * can materialize just the leading word without encoding the full key. The full encode passes
     * {@code Long.MAX_VALUE} and writes every byte. The leading bytes the bounded
     * call produces are a byte-for-byte prefix of the full encode's, so a key
     * fast-rejected on its leading word never has to be encoded in full.
     */
    private long appendKeyBytes(Record record, long budget) {
        final MemoryCARW heap = keyHeap;
        final long start = heap.getAppendOffset();
        for (int i = 0; i < columnIndices.length; i++) {
            if (heap.getAppendOffset() - start >= budget) {
                break;
            }
            if (columnByteWidths[i] >= 0) {
                encodeFixedColumn(record, i, heap.appendAddressFor(columnByteWidths[i]));
            } else {
                final long remaining = budget - (heap.getAppendOffset() - start);
                switch (columnTypes[i]) {
                    case ColumnType.VARCHAR ->
                            appendVarchar(record.getVarcharA(columnIndices[i]), isDesc[i], remaining);
                    case ColumnType.STRING -> appendUtf16(record.getStrA(columnIndices[i]), isDesc[i], remaining);
                    case ColumnType.SYMBOL -> appendUtf16(record.getSymA(columnIndices[i]), isDesc[i], remaining);
                    default ->
                            throw CairoException.nonCritical().put("unexpected type in encodeVariable: ").put(ColumnType.nameOf(columnTypes[i]));
                }
            }
        }
        return heap.getAppendOffset() - start;
    }

    private void appendUtf16(CharSequence value, boolean desc, long maxBytes) {
        final byte mask = desc ? (byte) 0xFF : 0;
        if (value == null) {
            Unsafe.putByte(keyHeap.appendAddressFor(1), mask);
            return;
        }
        final int n = value.length();
        final long offset = keyHeap.getAppendOffset();
        final long full = 4L * n + 2;
        // Reserve the whole key, or, when bounded, the budget plus one character's
        // worth of slack for the step that can overrun maxBytes mid-character.
        final long base = keyHeap.appendAddressFor(maxBytes >= full ? full : maxBytes + 4L);
        long p = base;
        Unsafe.putByte(p++, (byte) (1 ^ mask));
        int i = 0;
        for (; i < n && (p - base) < maxBytes; i++) {
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
        if (i >= n) {
            Unsafe.putByte(p++, mask);
        }
        keyHeap.jumpTo(offset + (p - base));
    }

    private void appendVarchar(Utf8Sequence value, boolean desc, long maxBytes) {
        final byte mask = desc ? (byte) 0xFF : 0;
        if (value == null) {
            Unsafe.putByte(keyHeap.appendAddressFor(1), mask);
            return;
        }
        final int size = value.size();
        // Reserve the whole key, or, when bounded, the budget plus a word of
        // slack for the 8-byte SWAR step that can overrun maxBytes.
        final long offset = keyHeap.getAppendOffset();
        final long base = keyHeap.appendAddressFor(maxBytes >= size + 2L ? size + 2L : maxBytes + Long.BYTES);
        long p = base;
        Unsafe.putByte(p++, (byte) (1 ^ mask));
        final long wordMask = desc ? -1L : 0;
        int i = 0;
        for (; i + 8 <= size && (p - base) < maxBytes; i += 8, p += 8) {
            final long word = value.longAt(i);
            // The +1 shift reserves 0x00 as the terminator; a 0xFF byte would
            // wrap to 0x00 and break the order. Valid UTF-8 has no 0xFF.
            if (hasByteFF(word)) {
                throw invalidVarcharUtf8();
            }
            Unsafe.putLong(p, (word + 0x0101010101010101L) ^ wordMask);
        }
        for (; i < size && (p - base) < maxBytes; i++, p++) {
            final byte b = value.byteAt(i);
            if (b == (byte) 0xFF) {
                throw invalidVarcharUtf8();
            }
            Unsafe.putByte(p, (byte) ((b + 1) ^ mask));
        }
        if (i >= size) {
            Unsafe.putByte(p++, mask);
        }
        keyHeap.jumpTo(offset + (p - base));
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
        if (frameMemory.getSourceColumnType(columnIndices[0]) != -1) {
            // Lazily type-cast parquet column (ALTER COLUMN TYPE): the page holds the
            // source type's raw bytes (e.g. a VARCHAR_SLICE for a STRING->LONG cast),
            // not this column's current fixed type. Fall back to the per-row path,
            // which materializes the converted value through the record accessors.
            return false;
        }
        final long colAddr = frameMemory.getPageAddress(columnIndices[0]);
        if (colAddr == 0) {
            return false;
        }
        final long rowIdBase = Rows.toRowID(frameIndex, 0);
        final boolean desc = isDesc[0];
        switch (columnTypes[0]) {
            case ColumnType.SYMBOL -> batchSymbol(topK, colAddr, rowIdBase, rows, rowCount, desc);
            case ColumnType.FLOAT -> batchFloat(topK, colAddr, rowIdBase, rows, rowCount, desc);
            case ColumnType.DOUBLE -> batchDouble(topK, colAddr, rowIdBase, rows, rowCount, desc);
            // BOOLEAN is stored as exactly 0 or 1 (putBool), so xor-ing the raw byte reproduces the
            // per-row getBool() normalization in encodeFixed8.
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

    /**
     * Batched single wide-fixed top-K path (UUID/LONG128/LONG256) over a page frame: reads
     * each row's value straight from the column page, normalizes it with the shared encode
     * helpers, and threshold-rejects on the leading 16 bytes. Returns false - so the caller
     * falls back to per-row {@link #encodeTopK} - for a column-top frame.
     */
    private boolean encodeFixedWideBatch(PageFrameMemory frameMemory, int frameIndex, DirectLongList rows, long rowCount, EncodedTopKBuffer topK) {
        if (frameMemory.getSourceColumnType(columnIndices[0]) != -1) {
            // Lazily type-cast parquet column (ALTER COLUMN TYPE): the page holds the
            // source type's raw bytes (e.g. a VARCHAR_SLICE for a STRING->UUID cast),
            // not this column's current wide-fixed type. Fall back to the per-row path.
            return false;
        }
        final long colAddr = frameMemory.getPageAddress(columnIndices[0]);
        if (colAddr == 0) {
            // Column top: every frame row is NULL for this column.
            return false;
        }
        final long rowIdBase = Rows.toRowID(frameIndex, 0);
        final int rowIdOffset = columnByteWidths[0];
        final boolean desc = isDesc[0];
        switch (columnTypes[0]) {
            case ColumnType.UUID -> batchUuid(topK, colAddr, rowIdBase, rowIdOffset, rows, rowCount, desc);
            case ColumnType.LONG128 -> batchLong128(topK, colAddr, rowIdBase, rowIdOffset, rows, rowCount, desc);
            case ColumnType.LONG256 -> batchLong256(topK, colAddr, rowIdBase, rowIdOffset, rows, rowCount, desc);
            default -> throw new AssertionError("unexpected FIXED_WIDE type: " + ColumnType.nameOf(columnTypes[0]));
        }
        return true;
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

    // A natively-stored STRING is materialized into native layout even in Parquet frames, so the
    // value is read straight from the page. A lazily type-cast STRING (e.g. an INT->STRING cast on
    // a Parquet partition) is not: the page holds the source type's bytes, so the per-row path must
    // materialize the converted value through the record accessors.
    private boolean encodeStringBatch(PageFrameMemory frameMemory, int frameIndex, DirectLongList rows, long rowCount, EncodedTopKBuffer topK, PageFrameMemoryRecord record) {
        if (frameMemory.getSourceColumnType(columnIndices[0]) != -1) {
            return false;
        }
        final long dataAddr = frameMemory.getPageAddress(columnIndices[0]);
        if (dataAddr == 0) {
            // Column top: every frame row is NULL for this column.
            return false;
        }
        final long auxAddr = frameMemory.getAuxPageAddress(columnIndices[0]);
        final long rowIdBase = Rows.toRowID(frameIndex, 0);
        if (rows == null) {
            for (long r = 0; r < rowCount; r++) {
                encodeStringRow(topK, record, dataAddr, auxAddr, rowIdBase, r);
            }
        } else {
            for (long p = 0; p < rowCount; p++) {
                encodeStringRow(topK, record, dataAddr, auxAddr, rowIdBase, rows.get(p));
            }
        }
        return true;
    }

    private void encodeStringRow(EncodedTopKBuffer topK, PageFrameMemoryRecord record, long dataAddr, long auxAddr, long rowIdBase, long r) {
        final long valueAddr = dataAddr + Unsafe.getLong(auxAddr + (r << 3));
        final int len = Unsafe.getInt(valueAddr);
        final CharSequence value = len == TableUtils.NULL_LEN ? null : stringView.of(valueAddr + Vm.STRING_LENGTH_BYTES, len);
        final boolean desc = isDesc[0];
        final long rowId = rowIdBase + r;
        if (utf16Rejects(value, desc, rowId, topK)) {
            return;
        }
        record.setRowIndex(r);
        encode(record, topK.beginAppend(), rowId);
        topK.endAppend();
    }

    private boolean encodeVarcharBatch(PageFrameMemory frameMemory, int frameIndex, DirectLongList rows, long rowCount, EncodedTopKBuffer topK, PageFrameMemoryRecord record) {
        final long auxAddr = frameMemory.getAuxPageAddress(columnIndices[0]);
        if (auxAddr == 0) {
            return false;
        }
        final boolean isParquet = frameMemory.getFrameFormat() == PartitionFormat.PARQUET;
        final long dataAddr = isParquet ? 0 : frameMemory.getPageAddress(columnIndices[0]);
        final long rowIdBase = Rows.toRowID(frameIndex, 0);
        if (rows == null) {
            for (long r = 0; r < rowCount; r++) {
                encodeVarcharRow(topK, record, auxAddr, dataAddr, isParquet, rowIdBase, r);
            }
        } else {
            for (long p = 0; p < rowCount; p++) {
                encodeVarcharRow(topK, record, auxAddr, dataAddr, isParquet, rowIdBase, rows.get(p));
            }
        }
        return true;
    }

    private void encodeVarcharRow(EncodedTopKBuffer topK, PageFrameMemoryRecord record, long auxAddr, long dataAddr, boolean isParquet, long rowIdBase, long r) {
        final boolean desc = isDesc[0];
        final long auxEntry = auxAddr + r * VarcharTypeDriver.VARCHAR_AUX_WIDTH_BYTES;
        final int header = Unsafe.getInt(auxEntry);
        final long rowId = rowIdBase + r;
        final boolean isRejected;
        if (VarcharTypeDriver.hasNullFlag(header)) {
            final long k1 = desc ? (0xFFL << 56) : 0L; // matches varcharLeadingWord(null)
            isRejected = topK.fastRejectsVarEntry(k1, 1, rowId);
        } else if (isParquet) {
            final int size = VarcharTypeDriver.getSliceSize(header);
            final long k1 = varcharLeadingWordFromBytes(VarcharTypeDriver.getSliceByteAddress(auxEntry), size, desc);
            isRejected = topK.fastRejectsVarEntry(k1, size + 2L, rowId);
        } else if (VarcharTypeDriver.hasInlinedFlag(header)) {
            final int size = VarcharTypeDriver.getInlinedOrSplitSize(header);
            final long k1 = varcharLeadingWordFromBytes(VarcharTypeDriver.getValueByteAddress(header, auxEntry, dataAddr), size, desc);
            isRejected = topK.fastRejectsVarEntry(k1, size + 2L, rowId);
        } else {
            final int size = VarcharTypeDriver.getInlinedOrSplitSize(header);
            final long k1 = varcharLeadingWordFromPrefix(auxEntry, desc);
            isRejected = topK.fastRejectsVarEntryPrefix6(k1, size + 2L, rowId);
        }
        if (isRejected) {
            return;
        }
        record.setRowIndex(r);
        encode(record, topK.beginAppend(), rowId);
        topK.endAppend();
    }

    private void encodeVariable(Record record, long destAddr, long rowId) {
        final MemoryCARW heap = keyHeap;
        final long start = heap.getAppendOffset();
        final long len = appendKeyBytes(record, Long.MAX_VALUE);
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

    private boolean variableRowRejected(Record record, long rowId, EncodedTopKBuffer topK) {
        // Single-column hot path: the leading word and length come straight from the
        // value with a few ALU ops - no key-heap write and no rollback.
        switch (keyShape) {
            case VARCHAR: {
                final Utf8Sequence value = record.getVarcharA(columnIndices[0]);
                final long len = value == null ? 1 : value.size() + 2L;
                return topK.fastRejectsVarEntry(varcharLeadingWord(value, isDesc[0]), len, rowId);
            }
            // STRING and non-static SYMBOL (resolved through the symbol table) share the UTF-16 word.
            case STRING: {
                final CharSequence value = record.getStrA(columnIndices[0]);
                return utf16Rejects(value, isDesc[0], rowId, topK);
            }
            case SYMBOL: {
                final CharSequence value = record.getSymA(columnIndices[0]);
                return utf16Rejects(value, isDesc[0], rowId, topK);
            }
            default:
                break;
        }
        // General fallback: materialize the leading word in the key heap (reusing the
        // real encode path so it cannot drift), then roll the scratch back. The budget
        // is one byte past the leading word, so a key that fits it reports its exact
        // length while a longer key is simply flagged as spilling past it.
        final MemoryCARW heap = keyHeap;
        final long mark = heap.getAppendOffset();
        final long len = appendKeyBytes(record, Long.BYTES + 1);
        final long padBytes = Long.BYTES - len;
        if (padBytes > 0) {
            heap.appendAddressFor(padBytes); // ensure 8 bytes are mapped for the read below
        }
        long k1 = Long.reverseBytes(Unsafe.getLong(heap.addressOf(mark)));
        if (padBytes > 0) {
            k1 &= -1L << (8 * (int) padBytes);
        }
        heap.jumpTo(mark);
        return topK.fastRejectsVarEntry(k1, len, rowId);
    }

    private enum KeyShape {
        FIXED8,
        FIXED_WIDE, // single UUID/LONG128/LONG256
        GENERIC, // multi-column, or single wide-fixed without a batch path (DECIMAL128/256)
        STRING,
        SYMBOL,
        VARCHAR
    }
}
