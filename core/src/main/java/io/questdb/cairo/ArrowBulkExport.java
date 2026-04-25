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

package io.questdb.cairo;

import io.questdb.cairo.arr.ArrayView;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.cairo.sql.RecordMetadata;
import io.questdb.cairo.sql.StaticSymbolTable;
import io.questdb.cairo.sql.SymbolTable;
import io.questdb.cairo.vm.MemoryCARWImpl;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.std.BinarySequence;
import io.questdb.std.Decimal128;
import io.questdb.std.Decimal256;
import io.questdb.std.Interval;
import io.questdb.std.Long256;
import io.questdb.std.MemoryTag;
import io.questdb.std.Numbers;
import io.questdb.std.QuietCloseable;
import io.questdb.std.Unsafe;
import io.questdb.std.str.Utf8Sequence;

/**
 * Bulk reader from a {@link RecordCursorFactory} into Arrow-shaped native
 * buffers. Mirrors {@link ArrowBulkIngest} in the reverse direction.
 *
 * <p>Per batch the instance fills one {@link MemoryCARWImpl} per column
 * (data + validity bitmap) in the layout pyarrow expects. Python wraps the
 * native pointers via {@code pa.foreign_buffer(ptr, size, base)} with a
 * strong reference to this exporter as {@code base} so the JVM cannot
 * reclaim the buffer while Arrow is reading it.
 *
 * <p>Null representation:
 * <ul>
 *     <li>Arrow-standard validity bitmap — 1 bit per row, LSB-first,
 *         1 = valid, 0 = null.</li>
 *     <li>The Java-side per-type writer dispatches on the QuestDB null
 *         sentinel (INT_NULL for INT/IPv4, LONG_NULL for
 *         LONG/DATE/TIMESTAMP, NaN for FLOAT/DOUBLE, matching pair of
 *         LONG_NULL for UUID, signed-min per DECIMAL width).
 *         BOOLEAN/BYTE/SHORT/CHAR have no nulls in QuestDB's storage
 *         model — their bits are always valid.</li>
 * </ul>
 *
 * <p>Batch memory is allocated fresh per {@link #nextBatch()} and retained
 * in an append-only chain so Arrow buffers returned from earlier batches
 * keep working. {@link #releaseBatch()} closes the oldest still-open batch;
 * {@link #close()} closes all.
 *
 * <p>Phase 1 covers fixed-width types only. Variable-width scalars,
 * SYMBOL, ARRAY follow in Phases 2-4; calls for those types throw
 * {@link UnsupportedOperationException}.
 */
public final class ArrowBulkExport implements QuietCloseable {

    // Arrow layout: sign-extended 16-byte little-endian decimal128 for all
    // of QuestDB's narrower DECIMALs (8/16/32/64 bits). Decimal256 is 32
    // bytes little-endian.
    private static final int DEC128_BYTES = 16;
    private static final int DEC256_BYTES = 32;
    private static final int INTERVAL_BYTES = 16;       // two longs (lo, hi)
    private static final int LONG256_BYTES = 32;        // four longs little-endian
    private static final long DEFAULT_PAGE_SIZE = 1024 * 1024L; // 1 MiB
    private static final int MAX_PAGES = Integer.MAX_VALUE;
    private static final int MEM_TAG = MemoryTag.NATIVE_DEFAULT;

    private final RecordCursorFactory factory;
    private final SqlExecutionContext ctx;
    private final int batchSize;
    private final int columnCount;
    private final int[] columnTags;
    private final int[] columnTypes;   // full QuestDB column type (tag + metadata)
    private final int[] columnSizes;   // bytes per element in the Arrow data buffer
    // Divisor that converts ``varBytesWritten[col]`` (bytes in the data
    // buffer) into the value to write into the offsets buffer. 1 for
    // utf8/binary shapes (offsets-in-bytes); element size for ARRAY
    // (offsets-in-elements per Arrow ListArray spec). 0 = column has
    // no offsets buffer.
    private final int[] arrowOffsetDivisor;

    private RecordCursor cursor;
    private Record record;
    // Per-column buffers for the current in-flight batch. When nextBatch()
    // is called again, the previous batch's buffers move onto the
    // openBatches chain (only if the caller hasn't already released them).
    private MemoryCARWImpl[] dataBufs;     // fixed-width data OR var-width aux bytes
    private MemoryCARWImpl[] offsetBufs;   // int32 offsets for var-width (null for fixed)
    private MemoryCARWImpl[] validityBufs; // per-column validity bitmap
    private int[] nullCounts;
    private int[] varBytesWritten;         // running byte count for var-width cols
    private int currentBatchRows;
    // First unreleased batch in the chain (oldest). May be null.
    private BatchLink openBatchesHead;
    private BatchLink openBatchesTail;
    // Lazy sinks for wide decimals; reused across rows to avoid per-cell
    // allocation.
    private final Decimal128 dec128Sink = new Decimal128();
    private final Decimal256 dec256Sink = new Decimal256();
    // Reusable scratch for small ASCII / UTF-8 outputs (CHAR, IPv4,
    // UUID). 64 bytes covers UUID's 36 chars and the largest IPv4
    // string ("255.255.255.255") with room to spare; CHAR uses up to 4.
    private final byte[] scratch = new byte[64];
    private boolean closed;

    private ArrowBulkExport(
            CairoEngine engine,
            SqlExecutionContext ctx,
            RecordCursorFactory factory,
            int batchSize
    ) {
        this.factory = factory;
        this.ctx = ctx;
        this.batchSize = batchSize;
        RecordMetadata metadata = factory.getMetadata();
        this.columnCount = metadata.getColumnCount();
        this.columnTags = new int[columnCount];
        this.columnTypes = new int[columnCount];
        this.columnSizes = new int[columnCount];
        this.arrowOffsetDivisor = new int[columnCount];
        for (int i = 0; i < columnCount; i++) {
            int type = metadata.getColumnType(i);
            int tag = ColumnType.tagOf(type);
            this.columnTypes[i] = type;
            this.columnTags[i] = tag;
            this.columnSizes[i] = arrowElementSize(tag);
            if (tag == ColumnType.ARRAY) {
                int dims = ColumnType.decodeArrayDimensionality(type);
                short elemTag = ColumnType.decodeArrayElementType(type);
                if (dims != 1) {
                    throw new UnsupportedOperationException(
                            "ArrowBulkExport: ARRAY dimensionality " + dims
                                    + " not supported by the bulk path; "
                                    + "multi-dim ARRAY falls back to the "
                                    + "page-frame / copy paths");
                }
                if (elemTag != ColumnType.LONG && elemTag != ColumnType.DOUBLE) {
                    throw new UnsupportedOperationException(
                            "ArrowBulkExport: ARRAY element type "
                                    + ColumnType.nameOf(elemTag)
                                    + " not yet implemented");
                }
                // Both LONG and DOUBLE are 8 bytes per element; Arrow
                // ListArray offsets count elements, so divide by elem size.
                this.arrowOffsetDivisor[i] = 8;
            } else if (needsOffsetsBuffer(tag)) {
                // utf8/binary shapes write offsets in bytes.
                this.arrowOffsetDivisor[i] = 1;
            }
        }
        this.nullCounts = new int[columnCount];
        this.varBytesWritten = new int[columnCount];
    }

    public static ArrowBulkExport of(
            CairoEngine engine,
            SqlExecutionContext ctx,
            RecordCursorFactory factory,
            int batchSize
    ) {
        if (batchSize < 1) {
            throw new IllegalArgumentException("batchSize must be >= 1");
        }
        return new ArrowBulkExport(engine, ctx, factory, batchSize);
    }

    // -------------------------------------------------------------------
    // Public batch API (alphabetical)
    // -------------------------------------------------------------------

    @Override
    public void close() {
        if (closed) {
            return;
        }
        closed = true;
        BatchLink link = openBatchesHead;
        while (link != null) {
            closeColumnBuffers(link.dataBufs, link.offsetBufs, link.validityBufs);
            link = link.next;
        }
        openBatchesHead = null;
        openBatchesTail = null;
        if (dataBufs != null) {
            closeColumnBuffers(dataBufs, offsetBufs, validityBufs);
            dataBufs = null;
            offsetBufs = null;
            validityBufs = null;
        }
        if (cursor != null) {
            cursor.close();
            cursor = null;
        }
    }

    public long getDataPtr(int col) {
        requireBatch();
        return dataBufs[col].getAddress();
    }

    public long getDataSize(int col) {
        requireBatch();
        if (needsOffsetsBuffer(columnTags[col])) {
            return varBytesWritten[col];
        }
        return (long) currentBatchRows * columnSizes[col];
    }

    public int getNullCount(int col) {
        requireBatch();
        return nullCounts[col];
    }

    /**
     * @return int32 offsets buffer pointer for a var-width column, or
     *         0 for a fixed-width column. Size is (rows + 1) * 4 bytes.
     */
    public long getOffsetsPtr(int col) {
        requireBatch();
        MemoryCARWImpl b = offsetBufs[col];
        return b == null ? 0L : b.getAddress();
    }

    public long getOffsetsSize(int col) {
        requireBatch();
        if (offsetBufs[col] == null) return 0L;
        return (long) (currentBatchRows + 1) * 4;
    }

    /**
     * @return the number of entries in the SYMBOL dictionary for column
     *         {@code col}, or -1 if the column isn't SYMBOL.
     */
    public int getSymbolCount(int col) {
        if (columnTags[col] != ColumnType.SYMBOL || cursor == null) {
            return -1;
        }
        SymbolTable table = cursor.getSymbolTable(col);
        if (table instanceof StaticSymbolTable) {
            return ((StaticSymbolTable) table).getSymbolCount();
        }
        // Non-static tables (growing mid-cursor) — count via iteration;
        // uncommon in practice, but handle gracefully.
        int n = 0;
        while (table.valueOf(n) != null) {
            n++;
        }
        return n;
    }

    /**
     * @return the dictionary value at index {@code idx} for SYMBOL column
     *         {@code col}. The returned String is safe to materialise on
     *         the Python side; no native pointer semantics.
     */
    public String getSymbolValue(int col, int idx) {
        if (columnTags[col] != ColumnType.SYMBOL || cursor == null) {
            return null;
        }
        CharSequence v = cursor.getSymbolTable(col).valueOf(idx);
        return v == null ? null : v.toString();
    }

    /**
     * @return the entire SYMBOL dictionary for column {@code col} as a
     *         {@code String[]}, or an empty array when the column isn't
     *         SYMBOL or the cursor hasn't been opened. Lets callers fetch
     *         every dictionary entry with one JNI round-trip per column
     *         instead of one round-trip per dictionary slot.
     */
    public String[] getSymbolValues(int col) {
        int n = getSymbolCount(col);
        if (n <= 0) {
            return new String[0];
        }
        SymbolTable table = cursor.getSymbolTable(col);
        String[] values = new String[n];
        for (int i = 0; i < n; i++) {
            CharSequence v = table.valueOf(i);
            values[i] = v == null ? null : v.toString();
        }
        return values;
    }

    /**
     * @return validity bitmap native pointer, or {@code 0} if the column
     *         has no nulls in this batch (caller should pass {@code null}
     *         as the validity buffer to pyarrow).
     */
    public long getValidityPtr(int col) {
        requireBatch();
        if (nullCounts[col] == 0) {
            return 0L;
        }
        return validityBufs[col].getAddress();
    }

    public long getValiditySize(int col) {
        requireBatch();
        if (nullCounts[col] == 0) {
            return 0L;
        }
        return (currentBatchRows + 7) / 8;
    }

    /**
     * Fills the per-column buffers with up to {@link #batchSize} rows.
     * Returns the row count; {@code 0} means end of stream.
     */
    public long nextBatch() throws SqlException {
        if (closed) {
            throw new IllegalStateException("ArrowBulkExport already closed");
        }
        if (cursor == null) {
            cursor = factory.getCursor(ctx);
            record = cursor.getRecord();
        }
        // If there's a live batch from a previous call that the caller
        // never released, park it on the chain so its buffers stay valid.
        if (dataBufs != null && currentBatchRows > 0) {
            parkCurrentBatch();
        }
        allocateBatchBuffers();
        int rowIdx = 0;
        while (rowIdx < batchSize && cursor.hasNext()) {
            writeRow(rowIdx);
            rowIdx++;
        }
        currentBatchRows = rowIdx;
        return rowIdx;
    }

    /**
     * Release the oldest still-open batch's buffers. Callers that have
     * fully consumed a batch (copied the data into their own memory or
     * handed it off to an Arrow writer) should call this so we don't
     * accumulate buffers across a long streaming session.
     */
    public void releaseBatch() {
        BatchLink head = openBatchesHead;
        if (head == null) {
            if (dataBufs != null) {
                closeColumnBuffers(dataBufs, offsetBufs, validityBufs);
                dataBufs = null;
                offsetBufs = null;
                validityBufs = null;
                currentBatchRows = 0;
            }
            return;
        }
        openBatchesHead = head.next;
        if (openBatchesHead == null) {
            openBatchesTail = null;
        }
        closeColumnBuffers(head.dataBufs, head.offsetBufs, head.validityBufs);
    }

    // -------------------------------------------------------------------
    // Private static helpers (alphabetical)
    // -------------------------------------------------------------------

    private static int arrowElementSize(int tag) {
        // Fixed-width element size in the Arrow data buffer. Returns -1
        // for variable-width and unsupported types.
        if (tag == ColumnType.BOOLEAN) return 1; // byte-per-row; Python casts to bool_()
        if (tag == ColumnType.BYTE) return 1;
        if (tag == ColumnType.GEOBYTE) return 1;
        if (tag == ColumnType.SHORT) return 2;
        if (tag == ColumnType.GEOSHORT) return 2;
        if (tag == ColumnType.INT) return 4;
        if (tag == ColumnType.GEOINT) return 4;
        if (tag == ColumnType.LONG) return 8;
        if (tag == ColumnType.GEOLONG) return 8;
        if (tag == ColumnType.DATE) return 8;
        if (tag == ColumnType.TIMESTAMP) return 8;
        if (tag == ColumnType.FLOAT) return 4;
        if (tag == ColumnType.DOUBLE) return 8;
        if (tag == ColumnType.SYMBOL) return 4; // int32 dict indices
        if (tag == ColumnType.UUID) return 16; // emitted as fixed_size_binary(16)
        if (tag == ColumnType.INTERVAL) return INTERVAL_BYTES;
        if (tag == ColumnType.LONG256) return LONG256_BYTES;
        if (tag == ColumnType.DECIMAL8 || tag == ColumnType.DECIMAL16
                || tag == ColumnType.DECIMAL32 || tag == ColumnType.DECIMAL64
                || tag == ColumnType.DECIMAL128) return DEC128_BYTES;
        if (tag == ColumnType.DECIMAL256) return DEC256_BYTES;
        return -1;
    }

    private static void closeColumnBuffers(
            MemoryCARWImpl[] data,
            MemoryCARWImpl[] offsets,
            MemoryCARWImpl[] validity
    ) {
        if (data != null) {
            for (MemoryCARWImpl b : data) {
                if (b != null) b.close();
            }
        }
        if (offsets != null) {
            for (MemoryCARWImpl b : offsets) {
                if (b != null) b.close();
            }
        }
        if (validity != null) {
            for (MemoryCARWImpl b : validity) {
                if (b != null) b.close();
            }
        }
    }

    /**
     * True when the Arrow layout for {@code tag} requires an int32
     * offsets buffer alongside the data buffer. UUID emits as native
     * fixed_size_binary(16), so it is NOT in this set. IPv4 stays here
     * (emitted as utf8 dotted-quad) because PGWire ships IPv4 over
     * VARCHAR with no dedicated OID — keeping utf8 keeps Local and
     * Remote backends consistent at the Arrow boundary. CHAR stays
     * here because every value transcodes to 1-3 UTF-8 bytes. ARRAY
     * (1-D only) writes element-count offsets per Arrow ListArray
     * spec; multi-dim arrays are rejected at constructor time.
     * LONG256 emits as fixed_size_binary(32), not via offsets.
     */
    private static boolean needsOffsetsBuffer(int tag) {
        return tag == ColumnType.STRING
                || tag == ColumnType.VARCHAR
                || tag == ColumnType.BINARY
                || tag == ColumnType.CHAR
                || tag == ColumnType.IPv4
                || tag == ColumnType.ARRAY;
    }

    private static void writeDecimalSignExtended128(MemoryCARWImpl data, int rowIdx, long lo, long hi) {
        long base = (long) rowIdx * 16;
        data.putLong(base, lo);
        data.putLong(base + 8, hi);
    }

    private static long writeReplacementChar(long dst) {
        Unsafe.getUnsafe().putByte(dst, (byte) 0xEF);
        Unsafe.getUnsafe().putByte(dst + 1, (byte) 0xBF);
        Unsafe.getUnsafe().putByte(dst + 2, (byte) 0xBD);
        return dst + 3;
    }

    // -------------------------------------------------------------------
    // Private instance helpers (alphabetical)
    // -------------------------------------------------------------------

    private void allocateBatchBuffers() {
        dataBufs = new MemoryCARWImpl[columnCount];
        offsetBufs = new MemoryCARWImpl[columnCount];
        validityBufs = new MemoryCARWImpl[columnCount];
        for (int c = 0; c < columnCount; c++) {
            int size = columnSizes[c];
            int tag = columnTags[c];
            if (size < 0 && !needsOffsetsBuffer(tag)) {
                throw new UnsupportedOperationException(
                        "ArrowBulkExport: column " + c + " has type "
                                + ColumnType.nameOf(columnTypes[c])
                                + " which is not supported yet");
            }
            MemoryCARWImpl data = new MemoryCARWImpl(DEFAULT_PAGE_SIZE, MAX_PAGES, MEM_TAG);
            if (size > 0) {
                // Fixed-width: preallocate batchSize * size.
                data.extend((long) batchSize * size);
            } else {
                // Var-width: start with one page; grow as bytes are appended.
                data.extend(DEFAULT_PAGE_SIZE);
                MemoryCARWImpl offsets = new MemoryCARWImpl(DEFAULT_PAGE_SIZE, MAX_PAGES, MEM_TAG);
                offsets.extend((long) (batchSize + 1) * 4);
                // offsets[0] = 0; subsequent entries written per row.
                offsets.putInt(0, 0);
                offsetBufs[c] = offsets;
                varBytesWritten[c] = 0;
            }
            dataBufs[c] = data;

            long validityBytes = (batchSize + 7) / 8;
            MemoryCARWImpl validity = new MemoryCARWImpl(DEFAULT_PAGE_SIZE, MAX_PAGES, MEM_TAG);
            validity.extend(validityBytes);
            long addr = validity.getAddress();
            for (long i = 0; i < validityBytes; i++) {
                Unsafe.getUnsafe().putByte(addr + i, (byte) 0xFF);
            }
            validityBufs[c] = validity;
            nullCounts[c] = 0;
        }
    }

    private void clearValidityBit(int col, int rowIdx) {
        MemoryCARWImpl v = validityBufs[col];
        long byteOffset = rowIdx >>> 3;
        int bit = rowIdx & 7;
        long addr = v.getAddress() + byteOffset;
        byte b = Unsafe.getUnsafe().getByte(addr);
        Unsafe.getUnsafe().putByte(addr, (byte) (b & ~(1 << bit)));
        nullCounts[col]++;
    }

    /** Writes 1-3 UTF-8 bytes for {@code c} to the scratch buffer; returns byte count. */
    private int encodeCharToScratch(char c) {
        if (c < 0x80) {
            scratch[0] = (byte) c;
            return 1;
        }
        if (c < 0x800) {
            scratch[0] = (byte) (0xC0 | (c >>> 6));
            scratch[1] = (byte) (0x80 | (c & 0x3F));
            return 2;
        }
        scratch[0] = (byte) (0xE0 | (c >>> 12));
        scratch[1] = (byte) (0x80 | ((c >>> 6) & 0x3F));
        scratch[2] = (byte) (0x80 | (c & 0x3F));
        return 3;
    }

    /** Formats {@code v} as dotted-quad ASCII into {@link #scratch}; returns byte count. */
    private int formatIpv4ToScratch(int v) {
        int p = 0;
        p = writeIpv4Octet((v >>> 24) & 0xFF, p);
        scratch[p++] = '.';
        p = writeIpv4Octet((v >>> 16) & 0xFF, p);
        scratch[p++] = '.';
        p = writeIpv4Octet((v >>> 8) & 0xFF, p);
        scratch[p++] = '.';
        p = writeIpv4Octet(v & 0xFF, p);
        return p;
    }

    private void parkCurrentBatch() {
        BatchLink link = new BatchLink();
        link.dataBufs = dataBufs;
        link.offsetBufs = offsetBufs;
        link.validityBufs = validityBufs;
        link.rowCount = currentBatchRows;
        if (openBatchesTail == null) {
            openBatchesHead = link;
        } else {
            openBatchesTail.next = link;
        }
        openBatchesTail = link;
        dataBufs = null;
        offsetBufs = null;
        validityBufs = null;
        currentBatchRows = 0;
    }

    private void requireBatch() {
        if (dataBufs == null) {
            throw new IllegalStateException(
                    "ArrowBulkExport: no current batch — call nextBatch() first");
        }
    }

    private void writeArrayCell(int col, int rowIdx) {
        // 1-D ARRAY only — multi-dim is rejected at construction time.
        // Both LONG[] and DOUBLE[] are 8 bytes per element, matching
        // arrowOffsetDivisor[col] = 8.
        ArrayView arr = record.getArray(col, columnTypes[col]);
        if (arr == null || arr.isNull()) {
            writeVarBytesNull(col, rowIdx);
            return;
        }
        // appendDataToMem advances the MemoryA cursor and auto-extends.
        // Empty arrays (cardinality == 0) short-circuit inside, leaving
        // the cursor unchanged — the row's list span is then zero, which
        // is exactly what Arrow ListArray expects for an empty (non-null)
        // entry.
        MemoryCARWImpl data = dataBufs[col];
        arr.appendDataToMem(data);
        varBytesWritten[col] = (int) data.getAppendOffset();
        writeArrowOffset(col, rowIdx);
    }

    private void writeArrowOffset(int col, int rowIdx) {
        // varBytesWritten[col] is bytes in the data buffer; Arrow offsets
        // count bytes for utf8/binary (divisor = 1) and elements for
        // ARRAY (divisor = elem size).
        int value = varBytesWritten[col] / arrowOffsetDivisor[col];
        offsetBufs[col].putInt((long) (rowIdx + 1) * 4, value);
    }

    private void writeBinaryDirect(int col, int rowIdx, BinarySequence bin) {
        int len = (int) bin.length();
        MemoryCARWImpl data = dataBufs[col];
        int current = varBytesWritten[col];
        int newEnd = current + len;
        data.extend(newEnd);
        bin.copyTo(data.getAddress() + current, 0, len);
        varBytesWritten[col] = newEnd;
        writeArrowOffset(col, rowIdx);
    }

    private void writeCell(int col, int rowIdx) {
        int tag = columnTags[col];
        if (needsOffsetsBuffer(tag)) {
            writeVarWidthCell(col, rowIdx, tag);
            return;
        }
        MemoryCARWImpl data = dataBufs[col];
        if (tag == ColumnType.BOOLEAN) {
            data.putByte(rowIdx, (byte) (record.getBool(col) ? 1 : 0));
            return;
        }
        if (tag == ColumnType.BYTE) {
            data.putByte(rowIdx, record.getByte(col));
            return;
        }
        if (tag == ColumnType.SHORT) {
            data.putShort((long) rowIdx * 2, record.getShort(col));
            return;
        }
        if (tag == ColumnType.INT) {
            int v = record.getInt(col);
            data.putInt((long) rowIdx * 4, v);
            if (v == Numbers.INT_NULL) clearValidityBit(col, rowIdx);
            return;
        }
        if (tag == ColumnType.LONG) {
            long v = record.getLong(col);
            data.putLong((long) rowIdx * 8, v);
            if (v == Numbers.LONG_NULL) clearValidityBit(col, rowIdx);
            return;
        }
        if (tag == ColumnType.DATE) {
            long v = record.getDate(col);
            data.putLong((long) rowIdx * 8, v);
            if (v == Numbers.LONG_NULL) clearValidityBit(col, rowIdx);
            return;
        }
        if (tag == ColumnType.TIMESTAMP) {
            long v = record.getTimestamp(col);
            data.putLong((long) rowIdx * 8, v);
            if (v == Numbers.LONG_NULL) clearValidityBit(col, rowIdx);
            return;
        }
        if (tag == ColumnType.FLOAT) {
            float v = record.getFloat(col);
            data.putFloat((long) rowIdx * 4, v);
            if (Float.isNaN(v)) clearValidityBit(col, rowIdx);
            return;
        }
        if (tag == ColumnType.DOUBLE) {
            double v = record.getDouble(col);
            data.putDouble((long) rowIdx * 8, v);
            if (Double.isNaN(v)) clearValidityBit(col, rowIdx);
            return;
        }
        if (tag == ColumnType.SYMBOL) {
            // QuestDB SYMBOL key is an int32; negative = null.
            int key = record.getInt(col);
            data.putInt((long) rowIdx * 4, key);
            if (key < 0) clearValidityBit(col, rowIdx);
            return;
        }
        if (tag == ColumnType.UUID) {
            // Emit as Arrow fixed_size_binary(16) in canonical big-endian
            // byte order — matches Python ``uuid.UUID.bytes`` and the
            // Arrow UUID extension type (RFC 4122). Layout: hi as 8
            // big-endian bytes followed by lo as 8 big-endian bytes.
            long lo = record.getLong128Lo(col);
            long hi = record.getLong128Hi(col);
            long base = (long) rowIdx * 16;
            data.putLong(base, Long.reverseBytes(hi));
            data.putLong(base + 8, Long.reverseBytes(lo));
            if (lo == Numbers.LONG_NULL && hi == Numbers.LONG_NULL) {
                clearValidityBit(col, rowIdx);
            }
            return;
        }
        if (tag == ColumnType.GEOBYTE) {
            // Geohashes encode at varying bit widths; null sentinel is
            // -1 (GeoHashes.BYTE_NULL) regardless of declared precision.
            byte v = record.getGeoByte(col);
            data.putByte(rowIdx, v);
            if (v == -1) clearValidityBit(col, rowIdx);
            return;
        }
        if (tag == ColumnType.GEOSHORT) {
            short v = record.getGeoShort(col);
            data.putShort((long) rowIdx * 2, v);
            if (v == -1) clearValidityBit(col, rowIdx);
            return;
        }
        if (tag == ColumnType.GEOINT) {
            int v = record.getGeoInt(col);
            data.putInt((long) rowIdx * 4, v);
            if (v == -1) clearValidityBit(col, rowIdx);
            return;
        }
        if (tag == ColumnType.GEOLONG) {
            long v = record.getGeoLong(col);
            data.putLong((long) rowIdx * 8, v);
            if (v == -1L) clearValidityBit(col, rowIdx);
            return;
        }
        if (tag == ColumnType.INTERVAL) {
            // QuestDB INTERVAL is a (lo, hi) timestamp pair. Emit as
            // fixed_size_binary(16): lo at offset 0, hi at offset 8,
            // both little-endian. Null when either bound is LONG_NULL.
            Interval iv = record.getInterval(col);
            long lo = iv == null ? Numbers.LONG_NULL : iv.getLo();
            long hi = iv == null ? Numbers.LONG_NULL : iv.getHi();
            long base = (long) rowIdx * INTERVAL_BYTES;
            data.putLong(base, lo);
            data.putLong(base + 8, hi);
            if (iv == null || lo == Numbers.LONG_NULL || hi == Numbers.LONG_NULL) {
                clearValidityBit(col, rowIdx);
            }
            return;
        }
        if (tag == ColumnType.LONG256) {
            // Emit as Arrow fixed_size_binary(32). Layout: l0..l3 each 8
            // bytes little-endian — matches QuestDB's natural on-disk
            // format and ``int.from_bytes(buf, "little", signed=False)``
            // on the Python side. NULL_LONG256 has all four longs set
            // to LONG_NULL (Long.MIN_VALUE).
            Long256 v = record.getLong256A(col);
            long l0 = v.getLong0();
            long l1 = v.getLong1();
            long l2 = v.getLong2();
            long l3 = v.getLong3();
            long base = (long) rowIdx * LONG256_BYTES;
            data.putLong(base, l0);
            data.putLong(base + 8, l1);
            data.putLong(base + 16, l2);
            data.putLong(base + 24, l3);
            if (l0 == Numbers.LONG_NULL && l1 == Numbers.LONG_NULL
                    && l2 == Numbers.LONG_NULL && l3 == Numbers.LONG_NULL) {
                clearValidityBit(col, rowIdx);
            }
            return;
        }
        if (tag == ColumnType.DECIMAL8) {
            byte v = record.getDecimal8(col);
            writeDecimalSignExtended128(data, rowIdx, v == Byte.MIN_VALUE ? 0 : (long) v,
                    v == Byte.MIN_VALUE ? 0 : ((v < 0) ? -1L : 0L));
            if (v == Byte.MIN_VALUE) clearValidityBit(col, rowIdx);
            return;
        }
        if (tag == ColumnType.DECIMAL16) {
            short v = record.getDecimal16(col);
            writeDecimalSignExtended128(data, rowIdx, v == Short.MIN_VALUE ? 0 : (long) v,
                    v == Short.MIN_VALUE ? 0 : ((v < 0) ? -1L : 0L));
            if (v == Short.MIN_VALUE) clearValidityBit(col, rowIdx);
            return;
        }
        if (tag == ColumnType.DECIMAL32) {
            int v = record.getDecimal32(col);
            writeDecimalSignExtended128(data, rowIdx, v == Numbers.INT_NULL ? 0 : (long) v,
                    v == Numbers.INT_NULL ? 0 : ((v < 0) ? -1L : 0L));
            if (v == Numbers.INT_NULL) clearValidityBit(col, rowIdx);
            return;
        }
        if (tag == ColumnType.DECIMAL64) {
            long v = record.getDecimal64(col);
            writeDecimalSignExtended128(data, rowIdx, v == Numbers.LONG_NULL ? 0 : v,
                    v == Numbers.LONG_NULL ? 0 : ((v < 0) ? -1L : 0L));
            if (v == Numbers.LONG_NULL) clearValidityBit(col, rowIdx);
            return;
        }
        if (tag == ColumnType.DECIMAL128) {
            record.getDecimal128(col, dec128Sink);
            long lo = dec128Sink.getLow();
            long hi = dec128Sink.getHigh();
            writeDecimalSignExtended128(data, rowIdx, lo, hi);
            // QuestDB's Decimal128 null: hi == LONG_NULL, lo == 0.
            if (hi == Numbers.LONG_NULL && lo == 0L) clearValidityBit(col, rowIdx);
            return;
        }
        if (tag == ColumnType.DECIMAL256) {
            record.getDecimal256(col, dec256Sink);
            long ll = dec256Sink.getLl();
            long lh = dec256Sink.getLh();
            long hl = dec256Sink.getHl();
            long hh = dec256Sink.getHh();
            long base = (long) rowIdx * 32;
            // Arrow decimal256 is 32 bytes little-endian.
            data.putLong(base, ll);
            data.putLong(base + 8, lh);
            data.putLong(base + 16, hl);
            data.putLong(base + 24, hh);
            if (hh == Numbers.LONG_NULL && hl == 0L && lh == 0L && ll == 0L) {
                clearValidityBit(col, rowIdx);
            }
            return;
        }
        throw new UnsupportedOperationException(
                "ArrowBulkExport: column " + col + " tag "
                        + ColumnType.nameOf(columnTypes[col])
                        + " not yet implemented");
    }

    /** Writes one IPv4 octet (0-255) as ASCII into {@link #scratch} at {@code off}; returns new pos. */
    private int writeIpv4Octet(int octet, int off) {
        if (octet >= 100) {
            scratch[off++] = (byte) ('0' + octet / 100);
            scratch[off++] = (byte) ('0' + (octet / 10) % 10);
            scratch[off++] = (byte) ('0' + octet % 10);
        } else if (octet >= 10) {
            scratch[off++] = (byte) ('0' + octet / 10);
            scratch[off++] = (byte) ('0' + octet % 10);
        } else {
            scratch[off++] = (byte) ('0' + octet);
        }
        return off;
    }

    private void writeRow(int rowIdx) {
        for (int c = 0; c < columnCount; c++) {
            writeCell(c, rowIdx);
        }
    }

    /** Copies the first {@code len} bytes of {@link #scratch} into the column's data buffer. */
    private void writeScratch(int col, int rowIdx, int len) {
        MemoryCARWImpl data = dataBufs[col];
        int current = varBytesWritten[col];
        int newEnd = current + len;
        data.extend(newEnd);
        long dst = data.getAddress() + current;
        for (int i = 0; i < len; i++) {
            Unsafe.getUnsafe().putByte(dst + i, scratch[i]);
        }
        varBytesWritten[col] = newEnd;
        writeArrowOffset(col, rowIdx);
    }

    private void writeStringDirect(int col, int rowIdx, CharSequence s) {
        MemoryCARWImpl data = dataBufs[col];
        int current = varBytesWritten[col];
        int len = s.length();
        // Worst case: 3 UTF-8 bytes per BMP char. Surrogate pairs encode
        // 2 input chars into 4 output bytes (2 per char), so 3 * len is
        // a safe upper bound for any input. Over-allocation is harmless
        // — varBytesWritten tracks the actually-used prefix.
        data.extend(current + len * 3);
        long base = data.getAddress();
        long dst = base + current;
        for (int i = 0; i < len; i++) {
            char c = s.charAt(i);
            if (c < 0x80) {
                Unsafe.getUnsafe().putByte(dst++, (byte) c);
            } else if (c < 0x800) {
                Unsafe.getUnsafe().putByte(dst++, (byte) (0xC0 | (c >>> 6)));
                Unsafe.getUnsafe().putByte(dst++, (byte) (0x80 | (c & 0x3F)));
            } else if (Character.isHighSurrogate(c)) {
                if (i + 1 < len && Character.isLowSurrogate(s.charAt(i + 1))) {
                    int cp = Character.toCodePoint(c, s.charAt(++i));
                    Unsafe.getUnsafe().putByte(dst++, (byte) (0xF0 | (cp >>> 18)));
                    Unsafe.getUnsafe().putByte(dst++, (byte) (0x80 | ((cp >>> 12) & 0x3F)));
                    Unsafe.getUnsafe().putByte(dst++, (byte) (0x80 | ((cp >>> 6) & 0x3F)));
                    Unsafe.getUnsafe().putByte(dst++, (byte) (0x80 | (cp & 0x3F)));
                } else {
                    // Unpaired high surrogate → U+FFFD replacement, matching
                    // String.getBytes(UTF_8) (CharsetEncoder.REPLACE).
                    dst = writeReplacementChar(dst);
                }
            } else if (Character.isLowSurrogate(c)) {
                // Unpaired low surrogate → U+FFFD replacement.
                dst = writeReplacementChar(dst);
            } else {
                Unsafe.getUnsafe().putByte(dst++, (byte) (0xE0 | (c >>> 12)));
                Unsafe.getUnsafe().putByte(dst++, (byte) (0x80 | ((c >>> 6) & 0x3F)));
                Unsafe.getUnsafe().putByte(dst++, (byte) (0x80 | (c & 0x3F)));
            }
        }
        int written = (int) (dst - base - current);
        int newEnd = current + written;
        varBytesWritten[col] = newEnd;
        writeArrowOffset(col, rowIdx);
    }

    private void writeUtf8Direct(int col, int rowIdx, Utf8Sequence utf8) {
        int size = utf8.size();
        MemoryCARWImpl data = dataBufs[col];
        int current = varBytesWritten[col];
        int newEnd = current + size;
        data.extend(newEnd);
        utf8.writeTo(data.getAddress() + current, 0, size);
        varBytesWritten[col] = newEnd;
        writeArrowOffset(col, rowIdx);
    }

    private void writeVarBytesNull(int col, int rowIdx) {
        // Null row: empty slice — next offset == previous offset.
        writeArrowOffset(col, rowIdx);
        clearValidityBit(col, rowIdx);
    }

    private void writeVarWidthCell(int col, int rowIdx, int tag) {
        if (tag == ColumnType.VARCHAR) {
            Utf8Sequence utf8 = record.getVarcharA(col);
            if (utf8 == null) {
                writeVarBytesNull(col, rowIdx);
            } else {
                // VARCHAR is already UTF-8. Direct write via
                // Utf8Sequence.writeTo(addr, lo, hi) skips the per-cell
                // String + byte[] allocation the previous version paid.
                writeUtf8Direct(col, rowIdx, utf8);
            }
            return;
        }
        if (tag == ColumnType.STRING) {
            CharSequence s = record.getStrA(col);
            if (s == null) {
                writeVarBytesNull(col, rowIdx);
            } else {
                // STRING is UTF-16; transcode straight into the data
                // buffer instead of routing through String + byte[].
                writeStringDirect(col, rowIdx, s);
            }
            return;
        }
        if (tag == ColumnType.BINARY) {
            BinarySequence bin = record.getBin(col);
            if (bin == null) {
                writeVarBytesNull(col, rowIdx);
            } else {
                writeBinaryDirect(col, rowIdx, bin);
            }
            return;
        }
        if (tag == ColumnType.CHAR) {
            char c = record.getChar(col);
            if (c == 0) {
                writeVarBytesNull(col, rowIdx);
            } else {
                int len = encodeCharToScratch(c);
                writeScratch(col, rowIdx, len);
            }
            return;
        }
        if (tag == ColumnType.IPv4) {
            int v = record.getIPv4(col);
            if (v == 0) {
                writeVarBytesNull(col, rowIdx);
            } else {
                int len = formatIpv4ToScratch(v);
                writeScratch(col, rowIdx, len);
            }
            return;
        }
        if (tag == ColumnType.ARRAY) {
            writeArrayCell(col, rowIdx);
            return;
        }
        throw new UnsupportedOperationException(
                "ArrowBulkExport: var-width tag " + ColumnType.nameOf(columnTypes[col])
                        + " not yet implemented");
    }

    // Per-batch buffer link node. Intrusive singly-linked list avoids an
    // ObjList import for what is always a short chain.
    private static final class BatchLink {
        MemoryCARWImpl[] dataBufs;
        MemoryCARWImpl[] offsetBufs;
        MemoryCARWImpl[] validityBufs;
        int rowCount;
        BatchLink next;
    }
}
