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

import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.std.QuietCloseable;

/**
 * Bulk reader from a QuestDB {@link RecordCursorFactory} to Arrow-shaped
 * native buffers. Called by the errand Python layer (mirrors
 * {@link ArrowBulkIngest} in the reverse direction) to avoid per-cell JPype
 * overhead when materialising query results to {@code pyarrow.Table}.
 *
 * <p>Per batch, the instance fills native memory buffers with column data
 * in the layout pyarrow expects for each column type:
 *
 * <ul>
 *     <li>fixed-width types: one contiguous data buffer + an optional
 *         validity bitmap;</li>
 *     <li>variable-width types (STRING, VARCHAR, BINARY, LONG256): validity
 *         + int32 offsets + an aux buffer holding the raw bytes;</li>
 *     <li>SYMBOL: int32 dictionary indices; the dictionary itself is
 *         materialised once per {@link SymbolMapReader} identity and
 *         reused across batches via the caller's cache;</li>
 *     <li>ARRAY: int32 offsets + a flat data buffer of element values.</li>
 * </ul>
 *
 * <p>Python wraps the per-column {@code (dataPtr, validityPtr, auxPtr,
 * offsetsPtr)} tuples via {@code pa.foreign_buffer(ptr, size, base)} where
 * {@code base} is a strong reference to this {@link ArrowBulkExport} so the
 * JVM cannot reclaim the buffer while Arrow is reading it.
 *
 * <p>Phase 0 — skeleton only, all methods throw
 * {@link UnsupportedOperationException}.
 */
public final class ArrowBulkExport implements QuietCloseable {

    private final CairoEngine engine;
    private final SqlExecutionContext ctx;
    private final RecordCursorFactory factory;
    private final int batchSize;
    private boolean closed;

    private ArrowBulkExport(
            CairoEngine engine,
            SqlExecutionContext ctx,
            RecordCursorFactory factory,
            int batchSize
    ) {
        this.engine = engine;
        this.ctx = ctx;
        this.factory = factory;
        this.batchSize = batchSize;
    }

    /**
     * Construct a bulk exporter bound to {@code factory}. The caller remains
     * responsible for closing {@code factory} after {@link #close()} has
     * released this exporter's buffers.
     */
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

    /**
     * Pull up to {@link #batchSize} rows from the cursor into the column
     * buffers. Returns the number of rows read; {@code 0} signals EOS and
     * the caller should stop.
     *
     * <p>Subsequent calls to {@link #getDataPtr(int)} and friends refer to
     * the batch just read.
     */
    public long nextBatch() {
        throw new UnsupportedOperationException(
                "ArrowBulkExport.nextBatch() not implemented (Phase 0 skeleton)"
        );
    }

    /** Native pointer to the current batch's data buffer for {@code col}. */
    public long getDataPtr(int col) {
        throw new UnsupportedOperationException();
    }

    /** Byte size of the current batch's data buffer for {@code col}. */
    public long getDataSize(int col) {
        throw new UnsupportedOperationException();
    }

    /**
     * Native pointer to the current batch's validity bitmap for {@code col},
     * or {@code 0} if the column has no nulls in this batch.
     */
    public long getValidityPtr(int col) {
        throw new UnsupportedOperationException();
    }

    /**
     * Native pointer to the aux (variable-length bytes) buffer for {@code
     * col}, or {@code 0} for fixed-width columns.
     */
    public long getAuxPtr(int col) {
        throw new UnsupportedOperationException();
    }

    public long getAuxSize(int col) {
        throw new UnsupportedOperationException();
    }

    /**
     * Native pointer to the int32 offsets buffer for variable-width columns,
     * or {@code 0} for fixed-width columns.
     */
    public long getOffsetsPtr(int col) {
        throw new UnsupportedOperationException();
    }

    /**
     * Identity reference for a SYMBOL column's dictionary in this batch.
     * Python keeps a cache keyed on this identity so a multi-partition
     * query doesn't rebuild the same dict per batch.
     */
    public long getDictIdRef(int col) {
        throw new UnsupportedOperationException();
    }

    /**
     * Mark the current batch's buffers as reusable. Called once the
     * caller's wrapping {@code pa.Buffer} objects have been consumed or
     * explicitly copied.
     */
    public void releaseBatch() {
        throw new UnsupportedOperationException();
    }

    @Override
    public void close() {
        if (closed) {
            return;
        }
        closed = true;
        // Per-type buffers get freed here once the Phase 1+ materialisers land.
    }
}
