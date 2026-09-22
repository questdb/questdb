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
import io.questdb.cairo.GeoHashes;
import io.questdb.cairo.vm.NullMemoryCMR;
import io.questdb.cairo.arr.ArrayView;
import io.questdb.cairo.sql.PageFrame;
import io.questdb.cairo.sql.PageFrameAddressCache;
import io.questdb.cairo.sql.PageFrameCursor;
import io.questdb.cairo.sql.PageFrameMemoryPool;
import io.questdb.cairo.sql.PageFrameMemoryRecord;
import io.questdb.cairo.sql.ParquetDecodeHint;
import io.questdb.cairo.sql.PartitionFormat;
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
import io.questdb.std.DirectLongList;
import io.questdb.std.IntList;
import io.questdb.std.Long256;
import io.questdb.std.LongList;
import io.questdb.std.MemoryTracker;
import io.questdb.std.Misc;
import io.questdb.std.QuietCloseable;
import io.questdb.std.Rows;
import io.questdb.std.Unsafe;
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
 * Every probe reads through a {@link Reader} of its own, positioned at one build row at a time.
 * Matches land on build rows in no particular order, so consecutive matches often sit in
 * different frames, and a reader cannot afford to rebind a record per match. The owner therefore
 * lays out the page address of every payload column of every native frame in one flat table, once
 * per execution, and every reader's fixed-size and SYMBOL getters read through it, returning the
 * nulls a page frame record returns for a column the frame lacks. A Parquet frame, and a getter
 * without such a path, reads through the reader's own page frame memory pool and record over the
 * shared, read-only address cache. A Parquet frame decodes into that pool, so matches that
 * alternate between Parquet row groups decode them again; the fused plan accepts that cost.
 */
public final class HashJoinBuildFrames implements HashJoinPayloadSource, QuietCloseable {
    private final PageFrameAddressCache addressCache = new PageFrameAddressCache();
    private final CairoConfiguration configuration;
    private final LongList frameRowCounts = new LongList();
    // Page address of payload column c of native frame f at f * payloadColumns.length + c; zero for a
    // column the frame lacks, and for every column of a Parquet frame, which the readers decode.
    private final LongList payloadAddresses = new LongList();
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
        payloadAddresses.clear();
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
        final DirectLongList pageAddresses = addressCache.getPageAddresses();
        for (int f = 0; f < frameIndex; f++) {
            final boolean isNative = addressCache.getFrameFormat(f) == PartitionFormat.NATIVE;
            final int columnOffset = addressCache.toColumnOffset(f);
            for (int column : payloadColumns) {
                payloadAddresses.add(isNative ? pageAddresses.get(columnOffset + column) : 0);
            }
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
        // Where the current frame's payload addresses start in the shared table.
        private int addressBase;
        private int frameIndex = -1;
        // True while the current frame is native, so that getters read the shared address table.
        private boolean isDirect;
        // The frame the record is bound to; the record binds only for a read the table cannot serve.
        private int recordFrameIndex = -1;
        private long row;

        private Reader() {
            // Matches land on build rows in no particular order, so the pool caches for scattered access.
            pool = new PageFrameMemoryPool(configuration);
        }

        @Override
        public void close() {
            frameIndex = -1;
            recordFrameIndex = -1;
            isDirect = false;
            record.of(null);
            // A closed pool releases its buffers and takes the next execution's address cache in reopen().
            Misc.free(pool);
        }

        @Override
        public ArrayView getArray(int col, int columnType) {
            return record().getArray(payloadColumns[col], columnType);
        }

        @Override
        public BinarySequence getBin(int col) {
            return record().getBin(payloadColumns[col]);
        }

        @Override
        public long getBinLen(int col) {
            return record().getBinLen(payloadColumns[col]);
        }

        @Override
        public boolean getBool(int col) {
            if (isDirect) {
                final long address = payloadAddresses.getQuick(addressBase + col);
                return address != 0 ? Unsafe.getByte(address + row) == 1 : NullMemoryCMR.INSTANCE.getBool(0);
            }
            return record().getBool(payloadColumns[col]);
        }

        @Override
        public byte getByte(int col) {
            if (isDirect) {
                final long address = payloadAddresses.getQuick(addressBase + col);
                return address != 0 ? Unsafe.getByte(address + row) : NullMemoryCMR.INSTANCE.getByte(0);
            }
            return record().getByte(payloadColumns[col]);
        }

        @Override
        public char getChar(int col) {
            if (isDirect) {
                final long address = payloadAddresses.getQuick(addressBase + col);
                return address != 0 ? Unsafe.getChar(address + (row << 1)) : NullMemoryCMR.INSTANCE.getChar(0);
            }
            return record().getChar(payloadColumns[col]);
        }

        @Override
        public long getDate(int col) {
            if (isDirect) {
                final long address = payloadAddresses.getQuick(addressBase + col);
                return address != 0 ? Unsafe.getLong(address + (row << 3)) : NullMemoryCMR.INSTANCE.getLong(0);
            }
            return record().getDate(payloadColumns[col]);
        }

        @Override
        public void getDecimal128(int col, Decimal128 sink) {
            record().getDecimal128(payloadColumns[col], sink);
        }

        @Override
        public short getDecimal16(int col) {
            if (isDirect) {
                final long address = payloadAddresses.getQuick(addressBase + col);
                return address != 0 ? Unsafe.getShort(address + (row << 1)) : NullMemoryCMR.INSTANCE.getDecimal16(0);
            }
            return record().getDecimal16(payloadColumns[col]);
        }

        @Override
        public void getDecimal256(int col, Decimal256 sink) {
            record().getDecimal256(payloadColumns[col], sink);
        }

        @Override
        public int getDecimal32(int col) {
            if (isDirect) {
                final long address = payloadAddresses.getQuick(addressBase + col);
                return address != 0 ? Unsafe.getInt(address + (row << 2)) : NullMemoryCMR.INSTANCE.getDecimal32(0);
            }
            return record().getDecimal32(payloadColumns[col]);
        }

        @Override
        public long getDecimal64(int col) {
            if (isDirect) {
                final long address = payloadAddresses.getQuick(addressBase + col);
                return address != 0 ? Unsafe.getLong(address + (row << 3)) : NullMemoryCMR.INSTANCE.getDecimal64(0);
            }
            return record().getDecimal64(payloadColumns[col]);
        }

        @Override
        public byte getDecimal8(int col) {
            if (isDirect) {
                final long address = payloadAddresses.getQuick(addressBase + col);
                return address != 0 ? Unsafe.getByte(address + row) : NullMemoryCMR.INSTANCE.getDecimal8(0);
            }
            return record().getDecimal8(payloadColumns[col]);
        }

        @Override
        public double getDouble(int col) {
            if (isDirect) {
                final long address = payloadAddresses.getQuick(addressBase + col);
                return address != 0 ? Unsafe.getDouble(address + (row << 3)) : NullMemoryCMR.INSTANCE.getDouble(0);
            }
            return record().getDouble(payloadColumns[col]);
        }

        @Override
        public float getFloat(int col) {
            if (isDirect) {
                final long address = payloadAddresses.getQuick(addressBase + col);
                return address != 0 ? Unsafe.getFloat(address + (row << 2)) : NullMemoryCMR.INSTANCE.getFloat(0);
            }
            return record().getFloat(payloadColumns[col]);
        }

        @Override
        public byte getGeoByte(int col) {
            if (isDirect) {
                final long address = payloadAddresses.getQuick(addressBase + col);
                return address != 0 ? Unsafe.getByte(address + row) : GeoHashes.BYTE_NULL;
            }
            return record().getGeoByte(payloadColumns[col]);
        }

        @Override
        public int getGeoInt(int col) {
            if (isDirect) {
                final long address = payloadAddresses.getQuick(addressBase + col);
                return address != 0 ? Unsafe.getInt(address + (row << 2)) : GeoHashes.INT_NULL;
            }
            return record().getGeoInt(payloadColumns[col]);
        }

        @Override
        public long getGeoLong(int col) {
            if (isDirect) {
                final long address = payloadAddresses.getQuick(addressBase + col);
                return address != 0 ? Unsafe.getLong(address + (row << 3)) : GeoHashes.NULL;
            }
            return record().getGeoLong(payloadColumns[col]);
        }

        @Override
        public short getGeoShort(int col) {
            if (isDirect) {
                final long address = payloadAddresses.getQuick(addressBase + col);
                return address != 0 ? Unsafe.getShort(address + (row << 1)) : GeoHashes.SHORT_NULL;
            }
            return record().getGeoShort(payloadColumns[col]);
        }

        @Override
        public int getIPv4(int col) {
            if (isDirect) {
                final long address = payloadAddresses.getQuick(addressBase + col);
                return address != 0 ? Unsafe.getInt(address + (row << 2)) : NullMemoryCMR.INSTANCE.getIPv4(0);
            }
            return record().getIPv4(payloadColumns[col]);
        }

        @Override
        public int getInt(int col) {
            if (isDirect) {
                final long address = payloadAddresses.getQuick(addressBase + col);
                return address != 0 ? Unsafe.getInt(address + (row << 2)) : NullMemoryCMR.INSTANCE.getInt(0);
            }
            return record().getInt(payloadColumns[col]);
        }

        @Override
        public long getLong(int col) {
            if (isDirect) {
                final long address = payloadAddresses.getQuick(addressBase + col);
                return address != 0 ? Unsafe.getLong(address + (row << 3)) : NullMemoryCMR.INSTANCE.getLong(0);
            }
            return record().getLong(payloadColumns[col]);
        }

        @Override
        public long getLong128Hi(int col) {
            if (isDirect) {
                final long address = payloadAddresses.getQuick(addressBase + col);
                return address != 0 ? Unsafe.getLong(address + (row << 4) + Long.BYTES) : NullMemoryCMR.INSTANCE.getLong128Hi();
            }
            return record().getLong128Hi(payloadColumns[col]);
        }

        @Override
        public long getLong128Lo(int col) {
            if (isDirect) {
                final long address = payloadAddresses.getQuick(addressBase + col);
                return address != 0 ? Unsafe.getLong(address + (row << 4)) : NullMemoryCMR.INSTANCE.getLong128Lo();
            }
            return record().getLong128Lo(payloadColumns[col]);
        }

        @Override
        public void getLong256(int col, CharSink<?> sink) {
            record().getLong256(payloadColumns[col], sink);
        }

        @Override
        public Long256 getLong256A(int col) {
            return record().getLong256A(payloadColumns[col]);
        }

        @Override
        public Long256 getLong256B(int col) {
            return record().getLong256B(payloadColumns[col]);
        }

        @Override
        public short getShort(int col) {
            if (isDirect) {
                final long address = payloadAddresses.getQuick(addressBase + col);
                return address != 0 ? Unsafe.getShort(address + (row << 1)) : NullMemoryCMR.INSTANCE.getShort(0);
            }
            return record().getShort(payloadColumns[col]);
        }

        @Override
        public CharSequence getStrA(int col) {
            return record().getStrA(payloadColumns[col]);
        }

        @Override
        public CharSequence getStrB(int col) {
            return record().getStrB(payloadColumns[col]);
        }

        @Override
        public int getStrLen(int col) {
            return record().getStrLen(payloadColumns[col]);
        }

        @Override
        public CharSequence getSymA(int col) {
            if (isDirect) {
                final long address = payloadAddresses.getQuick(addressBase + col);
                return address != 0 ? getSymbolTable(col).valueOf(Unsafe.getInt(address + (row << 2))) : null;
            }
            return record().getSymA(payloadColumns[col]);
        }

        @Override
        public CharSequence getSymB(int col) {
            if (isDirect) {
                final long address = payloadAddresses.getQuick(addressBase + col);
                return address != 0 ? getSymbolTable(col).valueBOf(Unsafe.getInt(address + (row << 2))) : null;
            }
            return record().getSymB(payloadColumns[col]);
        }

        @Override
        public SymbolTable getSymbolTable(int col) {
            return record.getSymbolTable(payloadColumns[col]);
        }

        @Override
        public long getTimestamp(int col) {
            if (isDirect) {
                final long address = payloadAddresses.getQuick(addressBase + col);
                return address != 0 ? Unsafe.getLong(address + (row << 3)) : NullMemoryCMR.INSTANCE.getLong(0);
            }
            return record().getTimestamp(payloadColumns[col]);
        }

        @Override
        public Utf8Sequence getVarcharA(int col) {
            return record().getVarcharA(payloadColumns[col]);
        }

        @Override
        public Utf8Sequence getVarcharB(int col) {
            return record().getVarcharB(payloadColumns[col]);
        }

        @Override
        public int getVarcharSize(int col) {
            return record().getVarcharSize(payloadColumns[col]);
        }

        @Override
        public SymbolTable newSymbolTable(int col) {
            return frameCursor.newSymbolTable(payloadColumns[col]);
        }

        @Override
        public void position(long rowId) {
            final int frameIndex = Rows.toPartitionIndex(rowId);
            if (frameIndex != this.frameIndex) {
                this.frameIndex = frameIndex;
                addressBase = frameIndex * payloadColumns.length;
                isDirect = addressCache.getFrameFormat(frameIndex) == PartitionFormat.NATIVE;
            }
            row = Rows.toLocalRowID(rowId);
        }

        @Override
        public void reopen() {
            frameIndex = -1;
            recordFrameIndex = -1;
            isDirect = false;
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

        // The record, bound to the current frame and row, for a read the address table cannot serve.
        private PageFrameMemoryRecord record() {
            if (recordFrameIndex != frameIndex) {
                pool.navigateTo(frameIndex, record);
                recordFrameIndex = frameIndex;
            }
            record.setRowIndex(row);
            return record;
        }
    }
}
