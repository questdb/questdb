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
import io.questdb.std.MemoryTag;
import io.questdb.std.Numbers;
import io.questdb.std.QuietCloseable;
import io.questdb.std.Unsafe;
import io.questdb.std.str.Utf8Sequence;

import java.nio.charset.StandardCharsets;

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
        for (int i = 0; i < columnCount; i++) {
            int type = metadata.getColumnType(i);
            int tag = ColumnType.tagOf(type);
            this.columnTypes[i] = type;
            this.columnTags[i] = tag;
            this.columnSizes[i] = arrowElementSize(tag);
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
    // Public batch API
    // -------------------------------------------------------------------

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

    public long getDataPtr(int col) {
        requireBatch();
        return dataBufs[col].getAddress();
    }

    public long getDataSize(int col) {
        requireBatch();
        if (isVarWidth(columnTags[col])) {
            return varBytesWritten[col];
        }
        return (long) currentBatchRows * columnSizes[col];
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

    // -------------------------------------------------------------------
    // Internal — batch management
    // -------------------------------------------------------------------

    private void allocateBatchBuffers() {
        dataBufs = new MemoryCARWImpl[columnCount];
        offsetBufs = new MemoryCARWImpl[columnCount];
        validityBufs = new MemoryCARWImpl[columnCount];
        for (int c = 0; c < columnCount; c++) {
            int size = columnSizes[c];
            int tag = columnTags[c];
            if (size < 0 && !isVarWidth(tag)) {
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

    private static boolean isVarWidth(int tag) {
        return tag == ColumnType.STRING
                || tag == ColumnType.VARCHAR
                || tag == ColumnType.BINARY
                || tag == ColumnType.CHAR
                || tag == ColumnType.IPv4
                || tag == ColumnType.UUID;
        // LONG256 lands in a follow-up phase — needs a CharSink thread
        // through Numbers.appendLong256 to avoid per-row StringBuilder.
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

    private static int arrowElementSize(int tag) {
        // Fixed-width element size in the Arrow data buffer. Returns -1
        // for variable-width and unsupported types.
        if (tag == ColumnType.BOOLEAN) return 1; // byte-per-row; Python casts to bool_()
        if (tag == ColumnType.BYTE) return 1;
        if (tag == ColumnType.SHORT) return 2;
        if (tag == ColumnType.INT) return 4;
        if (tag == ColumnType.LONG) return 8;
        if (tag == ColumnType.DATE) return 8;
        if (tag == ColumnType.TIMESTAMP) return 8;
        if (tag == ColumnType.FLOAT) return 4;
        if (tag == ColumnType.DOUBLE) return 8;
        if (tag == ColumnType.SYMBOL) return 4; // int32 dict indices
        if (tag == ColumnType.DECIMAL8 || tag == ColumnType.DECIMAL16
                || tag == ColumnType.DECIMAL32 || tag == ColumnType.DECIMAL64
                || tag == ColumnType.DECIMAL128) return DEC128_BYTES;
        if (tag == ColumnType.DECIMAL256) return DEC256_BYTES;
        return -1;
    }

    // -------------------------------------------------------------------
    // Internal — per-type writers
    // -------------------------------------------------------------------

    private void writeRow(int rowIdx) {
        for (int c = 0; c < columnCount; c++) {
            writeCell(c, rowIdx);
        }
    }

    private void writeCell(int col, int rowIdx) {
        int tag = columnTags[col];
        if (isVarWidth(tag)) {
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

    private static void writeDecimalSignExtended128(MemoryCARWImpl data, int rowIdx, long lo, long hi) {
        long base = (long) rowIdx * 16;
        data.putLong(base, lo);
        data.putLong(base + 8, hi);
    }

    // -------------------------------------------------------------------
    // Var-width cells
    // -------------------------------------------------------------------

    private void writeVarWidthCell(int col, int rowIdx, int tag) {
        if (tag == ColumnType.VARCHAR) {
            Utf8Sequence utf8 = record.getVarcharA(col);
            if (utf8 == null) {
                writeVarBytesNull(col, rowIdx);
            } else {
                // VARCHAR is already UTF-8 on the storage side. Materialise
                // to byte[] to avoid per-row Utf8Sequence indirection;
                // cheap relative to the surrounding cursor iteration.
                byte[] bytes = utf8.toString().getBytes(StandardCharsets.UTF_8);
                writeVarBytes(col, rowIdx, bytes);
            }
            return;
        }
        if (tag == ColumnType.STRING) {
            CharSequence s = record.getStrA(col);
            if (s == null) {
                writeVarBytesNull(col, rowIdx);
            } else {
                writeVarBytes(col, rowIdx, s.toString().getBytes(StandardCharsets.UTF_8));
            }
            return;
        }
        if (tag == ColumnType.BINARY) {
            BinarySequence bin = record.getBin(col);
            if (bin == null) {
                writeVarBytesNull(col, rowIdx);
            } else {
                int len = (int) bin.length();
                byte[] bytes = new byte[len];
                for (int i = 0; i < len; i++) {
                    bytes[i] = bin.byteAt(i);
                }
                writeVarBytes(col, rowIdx, bytes);
            }
            return;
        }
        if (tag == ColumnType.CHAR) {
            char c = record.getChar(col);
            if (c == 0) {
                writeVarBytesNull(col, rowIdx);
            } else {
                writeVarBytes(col, rowIdx, String.valueOf(c).getBytes(StandardCharsets.UTF_8));
            }
            return;
        }
        if (tag == ColumnType.IPv4) {
            int v = record.getIPv4(col);
            if (v == 0) {
                writeVarBytesNull(col, rowIdx);
            } else {
                String s = ((v >>> 24) & 0xFF) + "." + ((v >>> 16) & 0xFF) + "."
                        + ((v >>> 8) & 0xFF) + "." + (v & 0xFF);
                writeVarBytes(col, rowIdx, s.getBytes(StandardCharsets.US_ASCII));
            }
            return;
        }
        if (tag == ColumnType.UUID) {
            long lo = record.getLong128Lo(col);
            long hi = record.getLong128Hi(col);
            if (lo == Numbers.LONG_NULL && hi == Numbers.LONG_NULL) {
                writeVarBytesNull(col, rowIdx);
            } else {
                // Canonical lowercase hex with dashes: XXXXXXXX-XXXX-XXXX-XXXX-XXXXXXXXXXXX.
                StringBuilder sb = new StringBuilder(36);
                appendHex8(sb, (hi >>> 32) & 0xFFFFFFFFL);
                sb.append('-');
                appendHex4(sb, (hi >>> 16) & 0xFFFFL);
                sb.append('-');
                appendHex4(sb, hi & 0xFFFFL);
                sb.append('-');
                appendHex4(sb, (lo >>> 48) & 0xFFFFL);
                sb.append('-');
                appendHex12(sb, lo & 0xFFFFFFFFFFFFL);
                writeVarBytes(col, rowIdx, sb.toString().getBytes(StandardCharsets.US_ASCII));
            }
            return;
        }
        throw new UnsupportedOperationException(
                "ArrowBulkExport: var-width tag " + ColumnType.nameOf(columnTypes[col])
                        + " not yet implemented");
    }

    private void writeVarBytes(int col, int rowIdx, byte[] bytes) {
        MemoryCARWImpl data = dataBufs[col];
        int current = varBytesWritten[col];
        int newEnd = current + bytes.length;
        // Grow the data buffer if needed. MemoryCARWImpl.extend(size)
        // extends the underlying allocation so pageAddress + size stays
        // valid; appendAddressFor+putByte loop would be slower.
        data.extend(newEnd);
        long dst = data.getAddress() + current;
        for (int i = 0; i < bytes.length; i++) {
            Unsafe.getUnsafe().putByte(dst + i, bytes[i]);
        }
        varBytesWritten[col] = newEnd;
        offsetBufs[col].putInt((long) (rowIdx + 1) * 4, newEnd);
    }

    private void writeVarBytesNull(int col, int rowIdx) {
        // Null row: empty slice — next offset == previous offset.
        offsetBufs[col].putInt((long) (rowIdx + 1) * 4, varBytesWritten[col]);
        clearValidityBit(col, rowIdx);
    }

    private static final char[] HEX = "0123456789abcdef".toCharArray();

    private static void appendHex8(StringBuilder sb, long v) {
        for (int i = 28; i >= 0; i -= 4) sb.append(HEX[(int) ((v >> i) & 0xF)]);
    }

    private static void appendHex4(StringBuilder sb, long v) {
        for (int i = 12; i >= 0; i -= 4) sb.append(HEX[(int) ((v >> i) & 0xF)]);
    }

    private static void appendHex12(StringBuilder sb, long v) {
        for (int i = 44; i >= 0; i -= 4) sb.append(HEX[(int) ((v >> i) & 0xF)]);
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
