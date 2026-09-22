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

package io.questdb.griffin.engine.table;

import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.CairoException;
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.arr.ArrayView;
import io.questdb.cairo.sql.PageFrame;
import io.questdb.cairo.sql.PageFrameAddressCache;
import io.questdb.cairo.sql.PageFrameCursor;
import io.questdb.cairo.sql.PageFrameMemoryPool;
import io.questdb.cairo.sql.PageFrameMemoryRecord;
import io.questdb.cairo.sql.ParquetDecodeHint;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.cairo.sql.RecordMetadata;
import io.questdb.cairo.sql.SymbolTable;
import io.questdb.cairo.sql.SymbolTableSource;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.engine.join.HashJoinPayloadSource;
import io.questdb.std.BinarySequence;
import io.questdb.std.Decimal128;
import io.questdb.std.Decimal256;
import io.questdb.std.IntList;
import io.questdb.std.Long256;
import io.questdb.std.LongList;
import io.questdb.std.MemoryTracker;
import io.questdb.std.Misc;
import io.questdb.std.QuietCloseable;
import io.questdb.std.Rows;
import io.questdb.std.str.CharSink;
import io.questdb.std.str.Utf8Sequence;
import org.jetbrains.annotations.Nullable;

import static io.questdb.cairo.sql.PartitionFrameCursorFactory.ORDER_ASC;

/**
 * The page frames of a fused hash join's build input for one execution, and the payload source
 * that probes read through them. The owner opens the build scan's page frame cursor, walks its
 * frames into an address cache and builds from them; a build row's id is its frame index and
 * its row within that frame, {@link Rows#toRowID(int, long)}. The cursor stays open until
 * {@link #clear()}, because probes read payload columns and resolve SYMBOL payloads through it.
 * <p>
 * Every probe reads through a {@link Reader} of its own: a page frame memory pool and record
 * over the shared, read-only address cache, positioned at one build row at a time. Native frames
 * read straight from the cache's page addresses. A Parquet frame decodes into the reader's pool,
 * so matches that alternate between Parquet row groups decode them again; the fused plan accepts
 * that cost.
 */
public final class HashJoinBuildFrames implements HashJoinPayloadSource, QuietCloseable {
    private final PageFrameAddressCache addressCache = new PageFrameAddressCache();
    private final CairoConfiguration configuration;
    private final LongList frameRowCounts = new LongList();
    // Build scan column of each payload column, in payload order.
    private final int[] payloadColumns;
    private final boolean[] payloadSymbols;
    private PageFrameCursor frameCursor;
    @Nullable
    private MemoryTracker memoryTracker;
    private long rowCount;

    public HashJoinBuildFrames(CairoConfiguration configuration, IntList payloadColumns, RecordMetadata buildMetadata) {
        this.configuration = configuration;
        final int n = payloadColumns.size();
        this.payloadColumns = new int[n];
        this.payloadSymbols = new boolean[n];
        for (int i = 0; i < n; i++) {
            final int column = payloadColumns.getQuick(i);
            this.payloadColumns[i] = column;
            this.payloadSymbols[i] = ColumnType.isSymbol(buildMetadata.getColumnType(column));
        }
    }

    /** Closes the execution's frame cursor. Only call once every probe and reader of the execution is closed. */
    public void clear() {
        Throwable failure = Misc.freeBestEffort(null, frameCursor);
        frameCursor = null;
        failure = Misc.freeBestEffort(failure, addressCache);
        frameRowCounts.clear();
        rowCount = 0;
        memoryTracker = null;
        CairoException.rethrowCleanupFailure(failure);
    }

    @Override
    public void close() {
        clear();
    }

    public PageFrameAddressCache getAddressCache() {
        return addressCache;
    }

    public int getFrameCount() {
        return frameRowCounts.size();
    }

    public long getFrameRowCount(int frameIndex) {
        return frameRowCounts.getQuick(frameIndex);
    }

    /** Rows of every frame, before any row filter. */
    public long getRowCount() {
        return rowCount;
    }

    /** The open frame cursor, which serves the build input's symbol tables. */
    public SymbolTableSource getSymbolTableSource() {
        return frameCursor;
    }

    @Override
    public HashJoinPayloadSource.Reader newReader() {
        return new Reader();
    }

    /**
     * Opens the build scan's page frame cursor for an execution and walks its frames. On failure
     * the caller's cleanup path calls {@link #clear()}.
     */
    public void of(RecordCursorFactory buildFactory, SqlExecutionContext executionContext) throws SqlException {
        assert frameCursor == null;
        memoryTracker = executionContext.getMemoryTracker();
        frameCursor = buildFactory.getPageFrameCursor(executionContext, ORDER_ASC);
        addressCache.of(buildFactory.getMetadata(), frameCursor.getColumnMapping(), frameCursor.isExternal());
        PageFrame frame;
        int frameIndex = 0;
        while ((frame = frameCursor.next()) != null) {
            final long frameRows = frame.getPartitionHi() - frame.getPartitionLo();
            frameRowCounts.add(frameRows);
            addressCache.add(frameIndex++, frame);
            rowCount += frameRows;
        }
    }

    private static final class BuildRecord extends PageFrameMemoryRecord {
        BuildRecord() {
            super(RECORD_A_LETTER);
        }

        @Override
        public SymbolTable getSymbolTable(int columnIndex) {
            return super.getSymbolTable(columnIndex);
        }
    }

    /**
     * One probe's view of the build rows: payload column {@code i} reads build scan column
     * {@code payloadColumns[i]} of the row that {@link #position(long)} selected.
     */
    private final class Reader implements HashJoinPayloadSource.Reader {
        private final PageFrameMemoryPool pool;
        private final BuildRecord record = new BuildRecord();
        private int frameIndex = -1;

        private Reader() {
            // Matches land on build rows in no particular order, so the pool caches for scattered access.
            pool = new PageFrameMemoryPool(configuration);
        }

        @Override
        public void close() {
            frameIndex = -1;
            record.of(null);
            // A closed pool releases its buffers and takes the next execution's address cache in reopen().
            Misc.free(pool);
        }

        @Override
        public ArrayView getArray(int col, int columnType) {
            return record.getArray(payloadColumns[col], columnType);
        }

        @Override
        public BinarySequence getBin(int col) {
            return record.getBin(payloadColumns[col]);
        }

        @Override
        public long getBinLen(int col) {
            return record.getBinLen(payloadColumns[col]);
        }

        @Override
        public boolean getBool(int col) {
            return record.getBool(payloadColumns[col]);
        }

        @Override
        public byte getByte(int col) {
            return record.getByte(payloadColumns[col]);
        }

        @Override
        public char getChar(int col) {
            return record.getChar(payloadColumns[col]);
        }

        @Override
        public long getDate(int col) {
            return record.getDate(payloadColumns[col]);
        }

        @Override
        public void getDecimal128(int col, Decimal128 sink) {
            record.getDecimal128(payloadColumns[col], sink);
        }

        @Override
        public short getDecimal16(int col) {
            return record.getDecimal16(payloadColumns[col]);
        }

        @Override
        public void getDecimal256(int col, Decimal256 sink) {
            record.getDecimal256(payloadColumns[col], sink);
        }

        @Override
        public int getDecimal32(int col) {
            return record.getDecimal32(payloadColumns[col]);
        }

        @Override
        public long getDecimal64(int col) {
            return record.getDecimal64(payloadColumns[col]);
        }

        @Override
        public byte getDecimal8(int col) {
            return record.getDecimal8(payloadColumns[col]);
        }

        @Override
        public double getDouble(int col) {
            return record.getDouble(payloadColumns[col]);
        }

        @Override
        public float getFloat(int col) {
            return record.getFloat(payloadColumns[col]);
        }

        @Override
        public byte getGeoByte(int col) {
            return record.getGeoByte(payloadColumns[col]);
        }

        @Override
        public int getGeoInt(int col) {
            return record.getGeoInt(payloadColumns[col]);
        }

        @Override
        public long getGeoLong(int col) {
            return record.getGeoLong(payloadColumns[col]);
        }

        @Override
        public short getGeoShort(int col) {
            return record.getGeoShort(payloadColumns[col]);
        }

        @Override
        public int getIPv4(int col) {
            return record.getIPv4(payloadColumns[col]);
        }

        @Override
        public int getInt(int col) {
            return record.getInt(payloadColumns[col]);
        }

        @Override
        public long getLong(int col) {
            return record.getLong(payloadColumns[col]);
        }

        @Override
        public long getLong128Hi(int col) {
            return record.getLong128Hi(payloadColumns[col]);
        }

        @Override
        public long getLong128Lo(int col) {
            return record.getLong128Lo(payloadColumns[col]);
        }

        @Override
        public void getLong256(int col, CharSink<?> sink) {
            record.getLong256(payloadColumns[col], sink);
        }

        @Override
        public Long256 getLong256A(int col) {
            return record.getLong256A(payloadColumns[col]);
        }

        @Override
        public Long256 getLong256B(int col) {
            return record.getLong256B(payloadColumns[col]);
        }

        @Override
        public short getShort(int col) {
            return record.getShort(payloadColumns[col]);
        }

        @Override
        public CharSequence getStrA(int col) {
            return record.getStrA(payloadColumns[col]);
        }

        @Override
        public CharSequence getStrB(int col) {
            return record.getStrB(payloadColumns[col]);
        }

        @Override
        public int getStrLen(int col) {
            return record.getStrLen(payloadColumns[col]);
        }

        @Override
        public CharSequence getSymA(int col) {
            return record.getSymA(payloadColumns[col]);
        }

        @Override
        public CharSequence getSymB(int col) {
            return record.getSymB(payloadColumns[col]);
        }

        @Override
        public SymbolTable getSymbolTable(int col) {
            return record.getSymbolTable(payloadColumns[col]);
        }

        @Override
        public long getTimestamp(int col) {
            return record.getTimestamp(payloadColumns[col]);
        }

        @Override
        public Utf8Sequence getVarcharA(int col) {
            return record.getVarcharA(payloadColumns[col]);
        }

        @Override
        public Utf8Sequence getVarcharB(int col) {
            return record.getVarcharB(payloadColumns[col]);
        }

        @Override
        public int getVarcharSize(int col) {
            return record.getVarcharSize(payloadColumns[col]);
        }

        @Override
        public SymbolTable newSymbolTable(int col) {
            return frameCursor.newSymbolTable(payloadColumns[col]);
        }

        @Override
        public void position(long rowId) {
            final int frameIndex = Rows.toPartitionIndex(rowId);
            if (frameIndex != this.frameIndex) {
                pool.navigateTo(frameIndex, record);
                this.frameIndex = frameIndex;
            }
            record.setRowIndex(Rows.toLocalRowID(rowId));
        }

        @Override
        public void reopen() {
            frameIndex = -1;
            pool.setMemoryTracker(memoryTracker);
            pool.of(addressCache, ParquetDecodeHint.SCATTERED);
            record.of(frameCursor);
            // Symbol tables come from the build input, so this slot takes its own here, on the
            // owner, rather than on first use from a worker.
            for (int i = 0; i < payloadColumns.length; i++) {
                if (payloadSymbols[i]) {
                    record.getSymbolTable(payloadColumns[i]);
                }
            }
        }
    }
}
