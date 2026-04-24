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
import io.questdb.cairo.vm.MemoryCARWImpl;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.std.Decimal128;
import io.questdb.std.Decimal256;
import io.questdb.std.MemoryTag;
import io.questdb.std.Numbers;
import io.questdb.std.QuietCloseable;
import io.questdb.std.Unsafe;

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
    private MemoryCARWImpl[] dataBufs;     // per-column data
    private MemoryCARWImpl[] validityBufs; // per-column validity bitmap
    private int[] nullCounts;
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

    // Variable-width column accessors — stubbed until Phase 2.
    public long getAuxPtr(int col) {
        throw new UnsupportedOperationException("Phase 1: no aux buffers");
    }

    public long getAuxSize(int col) {
        throw new UnsupportedOperationException("Phase 1: no aux buffers");
    }

    public long getOffsetsPtr(int col) {
        throw new UnsupportedOperationException("Phase 1: no offsets");
    }

    public long getDictIdRef(int col) {
        throw new UnsupportedOperationException("Phase 1: no SYMBOL dict");
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
            // Release the current batch if no chain exists.
            if (dataBufs != null) {
                closeColumnBuffers(dataBufs, validityBufs);
                dataBufs = null;
                validityBufs = null;
                currentBatchRows = 0;
            }
            return;
        }
        openBatchesHead = head.next;
        if (openBatchesHead == null) {
            openBatchesTail = null;
        }
        closeColumnBuffers(head.dataBufs, head.validityBufs);
    }

    @Override
    public void close() {
        if (closed) {
            return;
        }
        closed = true;
        // Close every open batch in the chain plus the current batch.
        BatchLink link = openBatchesHead;
        while (link != null) {
            closeColumnBuffers(link.dataBufs, link.validityBufs);
            link = link.next;
        }
        openBatchesHead = null;
        openBatchesTail = null;
        if (dataBufs != null) {
            closeColumnBuffers(dataBufs, validityBufs);
            dataBufs = null;
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
        validityBufs = new MemoryCARWImpl[columnCount];
        for (int c = 0; c < columnCount; c++) {
            int size = columnSizes[c];
            if (size < 0) {
                // Variable-width / unsupported in Phase 1.
                throw new UnsupportedOperationException(
                        "ArrowBulkExport: column " + c + " has type "
                                + ColumnType.nameOf(columnTypes[c])
                                + " which is not supported in Phase 1 "
                                + "(fixed-width types only)");
            }
            long dataBytes = (long) batchSize * size;
            MemoryCARWImpl data = new MemoryCARWImpl(DEFAULT_PAGE_SIZE, MAX_PAGES, MEM_TAG);
            data.extend(dataBytes);
            dataBufs[c] = data;

            long validityBytes = (batchSize + 7) / 8;
            MemoryCARWImpl validity = new MemoryCARWImpl(DEFAULT_PAGE_SIZE, MAX_PAGES, MEM_TAG);
            validity.extend(validityBytes);
            // Initialise to all-valid (0xFF) so per-row null writes only
            // need to clear bits.
            long addr = validity.getAddress();
            for (long i = 0; i < validityBytes; i++) {
                Unsafe.getUnsafe().putByte(addr + i, (byte) 0xFF);
            }
            validityBufs[c] = validity;
            nullCounts[c] = 0;
        }
    }

    private void parkCurrentBatch() {
        BatchLink link = new BatchLink();
        link.dataBufs = dataBufs;
        link.validityBufs = validityBufs;
        link.rowCount = currentBatchRows;
        if (openBatchesTail == null) {
            openBatchesHead = link;
        } else {
            openBatchesTail.next = link;
        }
        openBatchesTail = link;
        dataBufs = null;
        validityBufs = null;
        currentBatchRows = 0;
    }

    private void requireBatch() {
        if (dataBufs == null) {
            throw new IllegalStateException(
                    "ArrowBulkExport: no current batch — call nextBatch() first");
        }
    }

    private static void closeColumnBuffers(MemoryCARWImpl[] data, MemoryCARWImpl[] validity) {
        if (data != null) {
            for (MemoryCARWImpl b : data) {
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
        // The element size the Arrow data buffer uses for each type. Must
        // match the pyarrow DataType in _arrow_bulk.py's wrappers.
        if (tag == ColumnType.BOOLEAN) return 1; // emitted as 1-byte per row, bit-packed on the Python side
        if (tag == ColumnType.BYTE) return 1;
        if (tag == ColumnType.SHORT) return 2;
        if (tag == ColumnType.CHAR) return 2;
        if (tag == ColumnType.INT) return 4;
        if (tag == ColumnType.LONG) return 8;
        if (tag == ColumnType.DATE) return 8;
        if (tag == ColumnType.TIMESTAMP) return 8;
        if (tag == ColumnType.FLOAT) return 4;
        if (tag == ColumnType.DOUBLE) return 8;
        if (tag == ColumnType.IPv4) return 4;
        if (tag == ColumnType.UUID) return 16;
        if (tag == ColumnType.DECIMAL8 || tag == ColumnType.DECIMAL16
                || tag == ColumnType.DECIMAL32 || tag == ColumnType.DECIMAL64
                || tag == ColumnType.DECIMAL128) return DEC128_BYTES;
        if (tag == ColumnType.DECIMAL256) return DEC256_BYTES;
        return -1; // variable-width / unsupported
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
        if (tag == ColumnType.CHAR) {
            data.putChar((long) rowIdx * 2, record.getChar(col));
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
        if (tag == ColumnType.IPv4) {
            int v = record.getIPv4(col);
            data.putInt((long) rowIdx * 4, v);
            // 0 is QuestDB's IPv4 null sentinel.
            if (v == 0) clearValidityBit(col, rowIdx);
            return;
        }
        if (tag == ColumnType.UUID) {
            long lo = record.getLong128Lo(col);
            long hi = record.getLong128Hi(col);
            long base = (long) rowIdx * 16;
            data.putLong(base, lo);
            data.putLong(base + 8, hi);
            if (lo == Numbers.LONG_NULL && hi == Numbers.LONG_NULL) {
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

    private static void writeDecimalSignExtended128(MemoryCARWImpl data, int rowIdx, long lo, long hi) {
        long base = (long) rowIdx * 16;
        data.putLong(base, lo);
        data.putLong(base + 8, hi);
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
        MemoryCARWImpl[] validityBufs;
        int rowCount;
        BatchLink next;
    }
}
