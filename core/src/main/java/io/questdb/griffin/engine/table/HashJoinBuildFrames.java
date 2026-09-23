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
import io.questdb.cairo.sql.SqlExecutionCircuitBreaker;
import io.questdb.cairo.sql.SymbolTable;
import io.questdb.cairo.sql.SymbolTableSource;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.engine.join.FrozenHashJoinBuild;
import io.questdb.griffin.engine.join.HashJoinPayloadSource;
import io.questdb.std.BinarySequence;
import io.questdb.std.Decimal128;
import io.questdb.std.Decimal256;
import io.questdb.std.DirectLongList;
import io.questdb.std.IntList;
import io.questdb.std.Long256;
import io.questdb.std.Long256Impl;
import io.questdb.std.LongList;
import io.questdb.std.MemoryTag;
import io.questdb.std.MemoryTracker;
import io.questdb.std.Misc;
import io.questdb.std.Numbers;
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
 * <p>
 * Reading a column where it lives costs a cache line per payload column per match, which a probe
 * that matches every build row many times pays over and over. For such a build the owner calls
 * {@link #copyPayload(FrozenHashJoinBuild, SqlExecutionCircuitBreaker)} before the probes run: one
 * forward pass over the build rows copies their payload columns, row after row in build order,
 * into a block of fixed-size rows, and every reader then reads the copied row that the match's
 * ordinal names. The pass reads each frame once, so it decodes each Parquet row group once as
 * well. Only fixed-size payload columns copy; a build with any other payload column keeps reading
 * where the columns live.
 */
public final class HashJoinBuildFrames implements HashJoinPayloadSource, QuietCloseable {
    // The copy pass checks the breaker once per this many rows; a power of two.
    private static final int COPY_ROWS_PER_CHECK = 64 * 1024;
    private final PageFrameAddressCache addressCache = new PageFrameAddressCache();
    private final CairoConfiguration configuration;
    // Byte offset of each payload column within a copied row; empty when the payload cannot be copied.
    private final int[] copyOffsets;
    // Bytes of one copied row; zero when the payload cannot be copied.
    private final int copyRowSize;
    private final LongList frameRowCounts = new LongList();
    // Page address of payload column c of native frame f at f * payloadColumns.length + c; zero for a
    // column the frame lacks, and for every column of a Parquet frame, which the readers decode.
    private final LongList payloadAddresses = new LongList();
    // Build scan column of each payload column, in payload order.
    private final int[] payloadColumns;
    private final boolean[] payloadSymbols;
    // Column type tag of each payload column, in payload order.
    private final int[] payloadTypes;
    // The copied payload of this execution, or zero while readers read the columns where they live.
    // The owner writes it before the probes are dispatched, which publishes it to their workers.
    private long copyAddress;
    private long copyCapacity;
    // The owner's reader for the copy pass, kept across executions.
    private Reader copyReader;
    private PageFrameCursor frameCursor;
    @Nullable
    private MemoryTracker memoryTracker;
    private long rowCount;

    public HashJoinBuildFrames(CairoConfiguration configuration, IntList payloadColumns, RecordMetadata buildMetadata) {
        this.configuration = configuration;
        final int n = payloadColumns.size();
        this.payloadColumns = new int[n];
        this.payloadSymbols = new boolean[n];
        this.payloadTypes = new int[n];
        final int[] offsets = new int[n];
        boolean isCopyable = n > 0;
        int offset = 0;
        int rowAlignment = 1;
        for (int i = 0; i < n; i++) {
            final int column = payloadColumns.getQuick(i);
            final int type = ColumnType.tagOf(buildMetadata.getColumnType(column));
            this.payloadColumns[i] = column;
            this.payloadSymbols[i] = type == ColumnType.SYMBOL;
            this.payloadTypes[i] = type;
            final int size = getCopySize(type);
            if (size < 0) {
                isCopyable = false;
            } else {
                // A wider value is a run of longs, so eight bytes is its natural alignment.
                final int alignment = Math.min(size, Long.BYTES);
                offset = (offset + alignment - 1) & -alignment;
                offsets[i] = offset;
                offset += size;
                rowAlignment = Math.max(rowAlignment, alignment);
            }
        }
        this.copyOffsets = isCopyable ? offsets : new int[0];
        this.copyRowSize = isCopyable ? (offset + rowAlignment - 1) & -rowAlignment : 0;
    }

    /**
     * Bytes of one copied row with these payload column types, or zero when a type cannot be copied,
     * so that the owner can bound a copy before it allocates one.
     */
    public int getCopyRowSize() {
        return copyRowSize;
    }

    /** Closes the execution's frame cursor. Only call once every probe and reader of the execution is closed. */
    public void clear() {
        Throwable failure = Misc.freeBestEffort(null, copyReader);
        if (copyAddress != 0) {
            copyAddress = Unsafe.free(copyAddress, copyCapacity, MemoryTag.NATIVE_JOIN_MAP, memoryTracker);
            copyCapacity = 0;
        }
        failure = Misc.freeBestEffort(failure, frameCursor);
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
        copyReader = null;
    }

    /**
     * Copies the payload columns of every row of the frozen build, in build order, and switches
     * every reader of this execution to the copy: a reader that positions after this returns reads
     * the copied row that the match's ordinal names. Call it on the owner, after the build froze and
     * before any probe of the execution runs, and only for a build whose payload can be copied, see
     * {@link #getCopyRowSize()}. The copy is charged to the execution's memory tracker and released
     * by {@link #clear()}, which the caller's failure path also runs.
     */
    public void copyPayload(FrozenHashJoinBuild build, SqlExecutionCircuitBreaker circuitBreaker) {
        assert copyRowSize > 0 && copyAddress == 0 && frameCursor != null;
        final long rowCount = build.getRowCount();
        if (rowCount < 1) {
            return;
        }
        if (rowCount > (Long.MAX_VALUE - copyRowSize) / copyRowSize) {
            throw CairoException.nonCritical().put("hash join payload copy overflow");
        }
        final long size = rowCount * copyRowSize;
        circuitBreaker.statefulThrowExceptionIfTrippedNoThrottle();
        final long address = Unsafe.malloc(size, MemoryTag.NATIVE_JOIN_MAP, memoryTracker);
        try {
            if (copyReader == null) {
                copyReader = new Reader();
            }
            // The rows are in build input order, so the pass reads the frames forward and a Parquet
            // row group decodes once.
            copyReader.reopen(ParquetDecodeHint.MONOTONIC);
            long row = address;
            for (long ordinal = 0; ordinal < rowCount; ordinal++, row += copyRowSize) {
                if ((ordinal & (COPY_ROWS_PER_CHECK - 1)) == 0) {
                    circuitBreaker.statefulThrowExceptionIfTrippedTimeThrottled();
                }
                copyReader.positionRow(build.getRowId(ordinal));
                copyRow(copyReader, row);
            }
        } catch (Throwable th) {
            Unsafe.free(address, size, MemoryTag.NATIVE_JOIN_MAP, memoryTracker);
            // The decoded frames and the symbol views go with the reader; clear() closes it anyway.
            Misc.free(copyReader);
            throw th;
        }
        // A decoded Parquet frame of the pass has no later use.
        copyReader.close();
        copyAddress = address;
        copyCapacity = size;
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

    /** Bytes that a copied value of this column type takes, or -1 for a type the copy cannot hold. */
    private static int getCopySize(int type) {
        return switch (type) {
            case ColumnType.BOOLEAN, ColumnType.BYTE, ColumnType.GEOBYTE, ColumnType.DECIMAL8 -> 1;
            case ColumnType.SHORT, ColumnType.CHAR, ColumnType.GEOSHORT, ColumnType.DECIMAL16 -> 2;
            case ColumnType.INT, ColumnType.FLOAT, ColumnType.SYMBOL, ColumnType.IPv4,
                 ColumnType.GEOINT, ColumnType.DECIMAL32 -> 4;
            case ColumnType.LONG, ColumnType.DATE, ColumnType.TIMESTAMP, ColumnType.DOUBLE,
                 ColumnType.GEOLONG, ColumnType.DECIMAL64 -> 8;
            case ColumnType.UUID, ColumnType.DECIMAL128 -> 16;
            case ColumnType.LONG256, ColumnType.DECIMAL256 -> 32;
            default -> -1;
        };
    }

    // Copies the payload columns of the row the reader is positioned at, which reads them where they live.
    private void copyRow(Reader reader, long row) {
        for (int col = 0, n = payloadTypes.length; col < n; col++) {
            final long dest = row + copyOffsets[col];
            switch (payloadTypes[col]) {
                case ColumnType.BOOLEAN -> Unsafe.putByte(dest, (byte) (reader.getBool(col) ? 1 : 0));
                case ColumnType.BYTE -> Unsafe.putByte(dest, reader.getByte(col));
                case ColumnType.SHORT -> Unsafe.putShort(dest, reader.getShort(col));
                case ColumnType.CHAR -> Unsafe.putChar(dest, reader.getChar(col));
                // A SYMBOL keeps the build input's key, which the reader resolves through its own table.
                case ColumnType.INT, ColumnType.SYMBOL -> Unsafe.putInt(dest, reader.getInt(col));
                case ColumnType.LONG -> Unsafe.putLong(dest, reader.getLong(col));
                case ColumnType.DATE -> Unsafe.putLong(dest, reader.getDate(col));
                case ColumnType.TIMESTAMP -> Unsafe.putLong(dest, reader.getTimestamp(col));
                case ColumnType.FLOAT -> Unsafe.putFloat(dest, reader.getFloat(col));
                case ColumnType.DOUBLE -> Unsafe.putDouble(dest, reader.getDouble(col));
                case ColumnType.IPv4 -> Unsafe.putInt(dest, reader.getIPv4(col));
                case ColumnType.GEOBYTE -> Unsafe.putByte(dest, reader.getGeoByte(col));
                case ColumnType.GEOSHORT -> Unsafe.putShort(dest, reader.getGeoShort(col));
                case ColumnType.GEOINT -> Unsafe.putInt(dest, reader.getGeoInt(col));
                case ColumnType.GEOLONG -> Unsafe.putLong(dest, reader.getGeoLong(col));
                // Lo first, then hi: the layout every fixed-size record reads a LONG128 from.
                case ColumnType.UUID -> {
                    Unsafe.putLong(dest, reader.getLong128Lo(col));
                    Unsafe.putLong(dest + Long.BYTES, reader.getLong128Hi(col));
                }
                case ColumnType.LONG256 -> {
                    final Long256 value = reader.getLong256A(col);
                    Unsafe.putLong(dest, value.getLong0());
                    Unsafe.putLong(dest + Long.BYTES, value.getLong1());
                    Unsafe.putLong(dest + Long.BYTES * 2, value.getLong2());
                    Unsafe.putLong(dest + Long.BYTES * 3, value.getLong3());
                }
                case ColumnType.DECIMAL8 -> Unsafe.putByte(dest, reader.getDecimal8(col));
                case ColumnType.DECIMAL16 -> Unsafe.putShort(dest, reader.getDecimal16(col));
                case ColumnType.DECIMAL32 -> Unsafe.putInt(dest, reader.getDecimal32(col));
                case ColumnType.DECIMAL64 -> Unsafe.putLong(dest, reader.getDecimal64(col));
                // The scale rides in the payload column's type, so the copy keeps raw words only.
                case ColumnType.DECIMAL128 -> {
                    reader.getDecimal128(col, reader.decimal128);
                    Unsafe.putLong(dest, reader.decimal128.getHigh());
                    Unsafe.putLong(dest + Long.BYTES, reader.decimal128.getLow());
                }
                case ColumnType.DECIMAL256 -> {
                    reader.getDecimal256(col, reader.decimal256);
                    Decimal256.put(reader.decimal256, dest);
                }
                default -> throw new AssertionError("uncopyable hash join payload type: " + ColumnType.nameOf(payloadTypes[col]));
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
        // Scratch sinks of the copy pass, which reads the two wide decimals through a sink alone;
        // null for a payload without them.
        private final Decimal128 decimal128;
        private final Decimal256 decimal256;
        // One flyweight per LONG256 column over the copy, and a second set for getLong256B(), whose
        // contract is that it survives a getLong256A() on the same column; null for other columns.
        private final Long256Impl[] long256A;
        private final Long256Impl[] long256B;
        private final PageFrameMemoryPool pool;
        private final BuildRecord record = new BuildRecord();
        // Where the current frame's payload addresses start in the shared table.
        private int addressBase;
        // The copied row of the current match, or zero while the reader reads the columns where they live.
        private long copyRow;
        private int frameIndex = -1;
        // True while the current frame is native, so that getters read the shared address table.
        private boolean isDirect;
        // The frame the record is bound to; the record binds only for a read the table cannot serve.
        private int recordFrameIndex = -1;
        private long row;

        private Reader() {
            pool = new PageFrameMemoryPool(configuration);
            final int n = payloadTypes.length;
            long256A = new Long256Impl[n];
            long256B = new Long256Impl[n];
            boolean hasDecimal128 = false;
            boolean hasDecimal256 = false;
            for (int i = 0; i < n; i++) {
                switch (payloadTypes[i]) {
                    case ColumnType.LONG256 -> {
                        long256A[i] = new Long256Impl();
                        long256B[i] = new Long256Impl();
                    }
                    case ColumnType.DECIMAL128 -> hasDecimal128 = true;
                    case ColumnType.DECIMAL256 -> hasDecimal256 = true;
                    default -> {
                    }
                }
            }
            decimal128 = hasDecimal128 ? new Decimal128() : null;
            decimal256 = hasDecimal256 ? new Decimal256() : null;
        }

        @Override
        public void close() {
            copyRow = 0;
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
            if (copyRow != 0) {
                return Unsafe.getByte(copied(col)) == 1;
            }
            if (isDirect) {
                final long address = payloadAddresses.getQuick(addressBase + col);
                return address != 0 ? Unsafe.getByte(address + row) == 1 : NullMemoryCMR.INSTANCE.getBool(0);
            }
            return record().getBool(payloadColumns[col]);
        }

        @Override
        public byte getByte(int col) {
            if (copyRow != 0) {
                return Unsafe.getByte(copied(col));
            }
            if (isDirect) {
                final long address = payloadAddresses.getQuick(addressBase + col);
                return address != 0 ? Unsafe.getByte(address + row) : NullMemoryCMR.INSTANCE.getByte(0);
            }
            return record().getByte(payloadColumns[col]);
        }

        @Override
        public char getChar(int col) {
            if (copyRow != 0) {
                return Unsafe.getChar(copied(col));
            }
            if (isDirect) {
                final long address = payloadAddresses.getQuick(addressBase + col);
                return address != 0 ? Unsafe.getChar(address + (row << 1)) : NullMemoryCMR.INSTANCE.getChar(0);
            }
            return record().getChar(payloadColumns[col]);
        }

        @Override
        public long getDate(int col) {
            if (copyRow != 0) {
                return Unsafe.getLong(copied(col));
            }
            if (isDirect) {
                final long address = payloadAddresses.getQuick(addressBase + col);
                return address != 0 ? Unsafe.getLong(address + (row << 3)) : NullMemoryCMR.INSTANCE.getLong(0);
            }
            return record().getDate(payloadColumns[col]);
        }

        @Override
        public void getDecimal128(int col, Decimal128 sink) {
            if (copyRow != 0) {
                final long address = copied(col);
                sink.ofRaw(Unsafe.getLong(address), Unsafe.getLong(address + Long.BYTES));
                return;
            }
            record().getDecimal128(payloadColumns[col], sink);
        }

        @Override
        public short getDecimal16(int col) {
            if (copyRow != 0) {
                return Unsafe.getShort(copied(col));
            }
            if (isDirect) {
                final long address = payloadAddresses.getQuick(addressBase + col);
                return address != 0 ? Unsafe.getShort(address + (row << 1)) : NullMemoryCMR.INSTANCE.getDecimal16(0);
            }
            return record().getDecimal16(payloadColumns[col]);
        }

        @Override
        public void getDecimal256(int col, Decimal256 sink) {
            if (copyRow != 0) {
                sink.ofRawAddress(copied(col));
                return;
            }
            record().getDecimal256(payloadColumns[col], sink);
        }

        @Override
        public int getDecimal32(int col) {
            if (copyRow != 0) {
                return Unsafe.getInt(copied(col));
            }
            if (isDirect) {
                final long address = payloadAddresses.getQuick(addressBase + col);
                return address != 0 ? Unsafe.getInt(address + (row << 2)) : NullMemoryCMR.INSTANCE.getDecimal32(0);
            }
            return record().getDecimal32(payloadColumns[col]);
        }

        @Override
        public long getDecimal64(int col) {
            if (copyRow != 0) {
                return Unsafe.getLong(copied(col));
            }
            if (isDirect) {
                final long address = payloadAddresses.getQuick(addressBase + col);
                return address != 0 ? Unsafe.getLong(address + (row << 3)) : NullMemoryCMR.INSTANCE.getDecimal64(0);
            }
            return record().getDecimal64(payloadColumns[col]);
        }

        @Override
        public byte getDecimal8(int col) {
            if (copyRow != 0) {
                return Unsafe.getByte(copied(col));
            }
            if (isDirect) {
                final long address = payloadAddresses.getQuick(addressBase + col);
                return address != 0 ? Unsafe.getByte(address + row) : NullMemoryCMR.INSTANCE.getDecimal8(0);
            }
            return record().getDecimal8(payloadColumns[col]);
        }

        @Override
        public double getDouble(int col) {
            if (copyRow != 0) {
                return Unsafe.getDouble(copied(col));
            }
            if (isDirect) {
                final long address = payloadAddresses.getQuick(addressBase + col);
                return address != 0 ? Unsafe.getDouble(address + (row << 3)) : NullMemoryCMR.INSTANCE.getDouble(0);
            }
            return record().getDouble(payloadColumns[col]);
        }

        @Override
        public float getFloat(int col) {
            if (copyRow != 0) {
                return Unsafe.getFloat(copied(col));
            }
            if (isDirect) {
                final long address = payloadAddresses.getQuick(addressBase + col);
                return address != 0 ? Unsafe.getFloat(address + (row << 2)) : NullMemoryCMR.INSTANCE.getFloat(0);
            }
            return record().getFloat(payloadColumns[col]);
        }

        @Override
        public byte getGeoByte(int col) {
            if (copyRow != 0) {
                return Unsafe.getByte(copied(col));
            }
            if (isDirect) {
                final long address = payloadAddresses.getQuick(addressBase + col);
                return address != 0 ? Unsafe.getByte(address + row) : GeoHashes.BYTE_NULL;
            }
            return record().getGeoByte(payloadColumns[col]);
        }

        @Override
        public int getGeoInt(int col) {
            if (copyRow != 0) {
                return Unsafe.getInt(copied(col));
            }
            if (isDirect) {
                final long address = payloadAddresses.getQuick(addressBase + col);
                return address != 0 ? Unsafe.getInt(address + (row << 2)) : GeoHashes.INT_NULL;
            }
            return record().getGeoInt(payloadColumns[col]);
        }

        @Override
        public long getGeoLong(int col) {
            if (copyRow != 0) {
                return Unsafe.getLong(copied(col));
            }
            if (isDirect) {
                final long address = payloadAddresses.getQuick(addressBase + col);
                return address != 0 ? Unsafe.getLong(address + (row << 3)) : GeoHashes.NULL;
            }
            return record().getGeoLong(payloadColumns[col]);
        }

        @Override
        public short getGeoShort(int col) {
            if (copyRow != 0) {
                return Unsafe.getShort(copied(col));
            }
            if (isDirect) {
                final long address = payloadAddresses.getQuick(addressBase + col);
                return address != 0 ? Unsafe.getShort(address + (row << 1)) : GeoHashes.SHORT_NULL;
            }
            return record().getGeoShort(payloadColumns[col]);
        }

        @Override
        public int getIPv4(int col) {
            if (copyRow != 0) {
                return Unsafe.getInt(copied(col));
            }
            if (isDirect) {
                final long address = payloadAddresses.getQuick(addressBase + col);
                return address != 0 ? Unsafe.getInt(address + (row << 2)) : NullMemoryCMR.INSTANCE.getIPv4(0);
            }
            return record().getIPv4(payloadColumns[col]);
        }

        @Override
        public int getInt(int col) {
            if (copyRow != 0) {
                return Unsafe.getInt(copied(col));
            }
            if (isDirect) {
                final long address = payloadAddresses.getQuick(addressBase + col);
                return address != 0 ? Unsafe.getInt(address + (row << 2)) : NullMemoryCMR.INSTANCE.getInt(0);
            }
            return record().getInt(payloadColumns[col]);
        }

        @Override
        public long getLong(int col) {
            if (copyRow != 0) {
                return Unsafe.getLong(copied(col));
            }
            if (isDirect) {
                final long address = payloadAddresses.getQuick(addressBase + col);
                return address != 0 ? Unsafe.getLong(address + (row << 3)) : NullMemoryCMR.INSTANCE.getLong(0);
            }
            return record().getLong(payloadColumns[col]);
        }

        @Override
        public long getLong128Hi(int col) {
            if (copyRow != 0) {
                return Unsafe.getLong(copied(col) + Long.BYTES);
            }
            if (isDirect) {
                final long address = payloadAddresses.getQuick(addressBase + col);
                return address != 0 ? Unsafe.getLong(address + (row << 4) + Long.BYTES) : NullMemoryCMR.INSTANCE.getLong128Hi();
            }
            return record().getLong128Hi(payloadColumns[col]);
        }

        @Override
        public long getLong128Lo(int col) {
            if (copyRow != 0) {
                return Unsafe.getLong(copied(col));
            }
            if (isDirect) {
                final long address = payloadAddresses.getQuick(addressBase + col);
                return address != 0 ? Unsafe.getLong(address + (row << 4)) : NullMemoryCMR.INSTANCE.getLong128Lo();
            }
            return record().getLong128Lo(payloadColumns[col]);
        }

        @Override
        public void getLong256(int col, CharSink<?> sink) {
            if (copyRow != 0) {
                Numbers.appendLong256FromUnsafe(copied(col), sink);
                return;
            }
            record().getLong256(payloadColumns[col], sink);
        }

        @Override
        public Long256 getLong256A(int col) {
            if (copyRow != 0) {
                return copiedLong256(long256A, col);
            }
            return record().getLong256A(payloadColumns[col]);
        }

        @Override
        public Long256 getLong256B(int col) {
            if (copyRow != 0) {
                return copiedLong256(long256B, col);
            }
            return record().getLong256B(payloadColumns[col]);
        }

        @Override
        public short getShort(int col) {
            if (copyRow != 0) {
                return Unsafe.getShort(copied(col));
            }
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
            if (copyRow != 0) {
                return getSymbolTable(col).valueOf(Unsafe.getInt(copied(col)));
            }
            if (isDirect) {
                final long address = payloadAddresses.getQuick(addressBase + col);
                return address != 0 ? getSymbolTable(col).valueOf(Unsafe.getInt(address + (row << 2))) : null;
            }
            return record().getSymA(payloadColumns[col]);
        }

        @Override
        public CharSequence getSymB(int col) {
            if (copyRow != 0) {
                return getSymbolTable(col).valueBOf(Unsafe.getInt(copied(col)));
            }
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
            if (copyRow != 0) {
                return Unsafe.getLong(copied(col));
            }
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
        public void position(long rowIdAddress, long ordinal) {
            final long copy = copyAddress;
            if (copy != 0) {
                copyRow = copy + ordinal * copyRowSize;
                return;
            }
            positionRow(Unsafe.getLong(rowIdAddress));
        }

        @Override
        public void reopen() {
            // Matches land on build rows in no particular order, so the pool caches for scattered access.
            reopen(ParquetDecodeHint.SCATTERED);
        }

        // Positions the reader at the build row with this id, where its columns live.
        private void positionRow(long rowId) {
            final int frameIndex = Rows.toPartitionIndex(rowId);
            if (frameIndex != this.frameIndex) {
                this.frameIndex = frameIndex;
                addressBase = frameIndex * payloadColumns.length;
                isDirect = addressCache.getFrameFormat(frameIndex) == PartitionFormat.NATIVE;
            }
            row = Rows.toLocalRowID(rowId);
        }

        private void reopen(ParquetDecodeHint hint) {
            copyRow = 0;
            frameIndex = -1;
            recordFrameIndex = -1;
            isDirect = false;
            pool.setMemoryTracker(memoryTracker);
            pool.of(addressCache, hint);
            record.of(frameCursor);
            // Symbol tables come from the build input, so this slot takes its own here, on the
            // owner, rather than on first use from a worker.
            for (int i = 0; i < payloadColumns.length; i++) {
                if (payloadSymbols[i]) {
                    record.getSymbolTable(payloadColumns[i]);
                }
            }
        }

        // The address of payload column col in the current copied row.
        private long copied(int col) {
            return copyRow + copyOffsets[col];
        }

        private Long256 copiedLong256(Long256Impl[] flyweights, int col) {
            final Long256Impl value = flyweights[col];
            final long address = copied(col);
            value.setAll(
                    Unsafe.getLong(address),
                    Unsafe.getLong(address + Long.BYTES),
                    Unsafe.getLong(address + Long.BYTES * 2),
                    Unsafe.getLong(address + Long.BYTES * 3)
            );
            return value;
        }

        // The record, bound to the current frame and row, for a read the address table cannot serve.
        // A reader over the copy never gets here: the copy holds every payload column.
        private PageFrameMemoryRecord record() {
            assert copyRow == 0;
            if (recordFrameIndex != frameIndex) {
                pool.navigateTo(frameIndex, record);
                recordFrameIndex = frameIndex;
            }
            record.setRowIndex(row);
            return record;
        }
    }
}
