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

import io.questdb.cairo.ArrayColumnTypes;
import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.CairoException;
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.ColumnTypes;
import io.questdb.cairo.GeoHashes;
import io.questdb.cairo.VarcharTypeDriver;
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
import io.questdb.std.ObjList;
import io.questdb.std.QuietCloseable;
import io.questdb.std.Rows;
import io.questdb.std.Transient;
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
 * shared, read-only address cache. A Parquet frame decodes into that pool, and every reader of the
 * execution decodes and caches the row groups that its own matches land on, so the readers share
 * the configured Parquet decode cache budget, see
 * {@link #HashJoinBuildFrames(CairoConfiguration, IntList, RecordMetadata, IntList, ColumnTypes, int)},
 * and a reader whose matches alternate between more row groups than its share holds decodes them
 * again. To spare the readers that cost, the owner copies a payload that fits the copy whenever
 * the build's Parquet frames decode past a reader's share, see
 * {@link #isReaderCacheExceeded(long, long)}, for a probe whose matches can land on as many of
 * those frames as the copy decodes, and otherwise when the probe repays the copy; see
 * {@code AsyncHashJoinGroupByAtom.maybeCopyPayload()}. A payload that the copy cannot hold keeps
 * that cost.
 * <p>
 * Reading a column where it lives costs a cache line per payload column per match, which a probe
 * that matches every build row many times pays over and over. For such a build the owner calls
 * {@link #copyPayload(FrozenHashJoinBuild, SqlExecutionCircuitBreaker)} before the probes run: one
 * forward pass over the build rows copies their payload columns, row after row in build order,
 * into a block of fixed-size rows, and every reader then reads the copied row that the match's
 * ordinal names. The pass reads each frame once, so it decodes each Parquet row group once as
 * well. A parallel build keeps each hash partition's rows in one region of the heap, or each
 * frame's rows grouped by hash bucket, so its rows are not in build order; its copy runs one task
 * per frame instead, each copying the rows its frame kept through a copy reader of its thread, see
 * {@link #beginCopy}. Only fixed-size payload
 * columns copy; a build with any other payload column keeps reading where the columns live.
 */
public final class HashJoinBuildFrames implements HashJoinPayloadSource, QuietCloseable {
    // The copy pass checks the breaker once per this many rows; a power of two.
    private static final int COPY_ROWS_PER_CHECK = 64 * 1024;
    // The bytes a row that a var-size column which no key sink stages counts at, by a deliberately
    // high estimate, since the build knows nothing of its values; see isReaderCacheExceeded().
    private static final int UNSTAGED_VAR_SIZE_DECODED_BYTES = 256;
    // The bytes a row that a staged var-size key column counts beyond its aux entry and its staged
    // value; see isReaderCacheExceeded().
    private static final int VAR_SIZE_DECODED_SLACK = 1;
    private final PageFrameAddressCache addressCache = new PageFrameAddressCache();
    private final CairoConfiguration configuration;
    // Byte offset of each payload column within a copied row; empty when the payload cannot be copied.
    private final int[] copyOffsets;
    // Bytes of one copied row; zero when the payload cannot be copied.
    private final int copyRowSize;
    // One reader per thread of a copy, kept across executions; see beginCopy().
    private final ObjList<Reader> copyReaders = new ObjList<>();
    // Bytes a row that a probe reader decodes a Parquet frame to, before the values of the var-size
    // key columns: every fixed-size column of the build scan, the aux entry and slack of every
    // var-size key column, and the estimate of every other var-size column; see isReaderCacheExceeded().
    private final int decodedRowSize;
    private final LongList frameRowCounts = new LongList();
    // Page address of payload column c of native frame f at f * payloadColumns.length + c; zero for a
    // column the frame lacks, and for every column of a Parquet frame, which the readers decode.
    private final LongList payloadAddresses = new LongList();
    // Build scan column of each payload column, in payload order.
    private final int[] payloadColumns;
    private final boolean[] payloadSymbols;
    // Column type tag of each payload column, in payload order.
    private final int[] payloadTypes;
    // The most probe readers that one execution reads through, which share the Parquet decode cache budget.
    private final int readerCount;
    // The most bytes that a reader decodes the values of the var-size key columns to per byte that
    // the key sink staged for them: 2 when a STRING column stages as UTF-8, 1 for any other var-size
    // key column, and 0 for a build scan without one; see isReaderCacheExceeded().
    private final int stagedKeyDecodeFactor;
    // The copied payload of this execution, or zero while readers read the columns where they live.
    // The owner writes it before the probes are dispatched, which publishes it to their workers.
    private long copyAddress;
    private long copyCapacity;
    private PageFrameCursor frameCursor;
    @Nullable
    private MemoryTracker memoryTracker;
    // The copy that copyRows() fills until publishCopy() hands it to the readers; zero otherwise.
    private long pendingCopyAddress;
    private long pendingCopySize;
    private long rowCount;

    /**
     * Frames of a build that stages no key, read through one probe reader per execution.
     */
    public HashJoinBuildFrames(CairoConfiguration configuration, IntList payloadColumns, RecordMetadata buildMetadata) {
        this(configuration, payloadColumns, buildMetadata, new IntList(0), new ArrayColumnTypes(), 1);
    }

    /**
     * Frames whose probe readers, at most {@code readerCount} of them per execution, share the
     * configured Parquet decode cache budget. A reader caches the Parquet row groups it decodes for
     * the whole execution, and the matches of every reader land on any row group, so a full budget
     * per reader would let an execution hold the decoded build once per reader. Each reader takes an
     * even share of the budget instead, but no less than a quarter of it, the budget of a page frame
     * cursor that walks its frames in order: the readers of an execution hold at most
     * max(budget, readerCount * budget / 4) between them, and a reader whose matches land on more
     * row groups than its share holds decodes them again, see
     * {@link #isReaderCacheExceeded(long, long)}. The copy readers walk the build's frames in order,
     * and keep the configured budget.
     * <p>
     * The build's key sink stages key {@code k} from build scan column {@code keyColumns[k]} as
     * {@code keyTypes[k]}; a build that stages no key, such as the INT layout's, passes no key types.
     * They tell which var-size columns of the build scan the build can measure from the keys it staged.
     */
    public HashJoinBuildFrames(
            CairoConfiguration configuration,
            IntList payloadColumns,
            RecordMetadata buildMetadata,
            @Transient IntList keyColumns,
            @Transient ColumnTypes keyTypes,
            int readerCount
    ) {
        assert readerCount > 0;
        this.configuration = configuration;
        this.readerCount = readerCount;
        int decodedRowSize = 0;
        int stagedKeyDecodeFactor = 0;
        for (int column = 0, columnCount = buildMetadata.getColumnCount(); column < columnCount; column++) {
            final int type = buildMetadata.getColumnType(column);
            if (!ColumnType.isVarSize(type)) {
                decodedRowSize += Math.max(ColumnType.sizeOf(type), 0);
                continue;
            }
            final int tag = ColumnType.tagOf(type);
            final int keyType = getStagedKeyType(column, keyColumns, keyTypes);
            if (tag == ColumnType.VARCHAR && keyType == ColumnType.VARCHAR) {
                // A reader decodes a VARCHAR to a slice entry that points into the decompressed page.
                decodedRowSize += VarcharTypeDriver.VARCHAR_AUX_WIDTH_BYTES + VAR_SIZE_DECODED_SLACK;
                stagedKeyDecodeFactor = Math.max(stagedKeyDecodeFactor, 1);
            } else if (tag == ColumnType.STRING && (keyType == ColumnType.STRING || keyType == ColumnType.VARCHAR)) {
                // A reader decodes a STRING to an offset entry and a copy of its UTF-16 chars, which
                // take up to twice the bytes of the UTF-8 that a VARCHAR key stages.
                decodedRowSize += Long.BYTES + VAR_SIZE_DECODED_SLACK;
                stagedKeyDecodeFactor = Math.max(stagedKeyDecodeFactor, keyType == ColumnType.VARCHAR ? 2 : 1);
            } else {
                decodedRowSize += UNSTAGED_VAR_SIZE_DECODED_BYTES;
            }
        }
        this.decodedRowSize = decodedRowSize;
        this.stagedKeyDecodeFactor = stagedKeyDecodeFactor;
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

    /**
     * Allocates the copy of a frozen build's payload columns and binds as many copy readers, one
     * per thread that will copy, for {@link #copyRows} to fill the copy on those threads. Readers
     * of the execution keep reading the columns where they live until {@link #publishCopy()}. Call
     * it on the owner, after the build froze and before any probe of the execution runs, and only
     * for a build with rows whose payload can be copied, see {@link #getCopyRowSize()}. The copy is
     * charged to the execution's memory tracker; {@link #clear()} releases it, which the caller's
     * failure path runs.
     */
    public void beginCopy(FrozenHashJoinBuild build, int copyReaderCount, SqlExecutionCircuitBreaker circuitBreaker) {
        assert copyRowSize > 0 && copyAddress == 0 && pendingCopyAddress == 0 && frameCursor != null && copyReaderCount > 0;
        final long rowCount = build.getRowCount();
        assert rowCount > 0;
        if (rowCount > (Long.MAX_VALUE - copyRowSize) / copyRowSize) {
            throw CairoException.nonCritical().put("hash join payload copy overflow");
        }
        final long size = rowCount * copyRowSize;
        circuitBreaker.statefulThrowExceptionIfTrippedNoThrottle();
        while (copyReaders.size() < copyReaderCount) {
            // A copy reader walks its frames in order, and the MONOTONIC hint caps its cache at four
            // row groups and a quarter of its budget, so it keeps the configured budget rather than
            // a probe reader's share.
            copyReaders.add(new Reader(configuration.getSqlParquetCacheMemorySize()));
        }
        pendingCopyAddress = Unsafe.malloc(size, MemoryTag.NATIVE_JOIN_MAP, memoryTracker);
        pendingCopySize = size;
        for (int i = 0; i < copyReaderCount; i++) {
            // A copying thread reads its rows in build order, so a Parquet row group decodes once.
            copyReaders.getQuick(i).reopen(ParquetDecodeHint.MONOTONIC);
        }
    }

    /**
     * Closes the execution's frame cursor. Only call once every probe and reader of the execution is closed.
     */
    public void clear() {
        Throwable failure = Misc.freeObjListAndKeepObjectsBestEffort(null, copyReaders);
        if (copyAddress != 0) {
            copyAddress = Unsafe.free(copyAddress, copyCapacity, MemoryTag.NATIVE_JOIN_MAP, memoryTracker);
            copyCapacity = 0;
        }
        if (pendingCopyAddress != 0) {
            pendingCopyAddress = Unsafe.free(pendingCopyAddress, pendingCopySize, MemoryTag.NATIVE_JOIN_MAP, memoryTracker);
            pendingCopySize = 0;
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
        copyReaders.clear();
    }

    /**
     * Copies the payload columns of every row of the frozen build, in build order, and switches
     * every reader of this execution to the copy: a reader that positions after this returns reads
     * the copied row that the match's ordinal names. The owner copies alone here; a parallel copy
     * runs {@link #beginCopy}, {@link #copyRows} and {@link #publishCopy()} instead. The conditions
     * of {@link #beginCopy} apply. On failure the copy is released at once, and the build can be
     * copied again.
     */
    public void copyPayload(FrozenHashJoinBuild build, SqlExecutionCircuitBreaker circuitBreaker) {
        final long rowCount = build.getRowCount();
        if (rowCount < 1) {
            return;
        }
        try {
            beginCopy(build, 1, circuitBreaker);
            copyRows(0, build, 0, rowCount, circuitBreaker);
        } catch (Throwable th) {
            if (pendingCopyAddress != 0) {
                pendingCopyAddress = Unsafe.free(pendingCopyAddress, pendingCopySize, MemoryTag.NATIVE_JOIN_MAP, memoryTracker);
                pendingCopySize = 0;
            }
            // The decoded frames go with the reader; clear() closes it anyway.
            Misc.freeObjListAndKeepObjects(copyReaders);
            throw th;
        }
        publishCopy();
    }

    /**
     * Copies the payload columns of the build rows with ordinals from {@code lo} to {@code hi},
     * exclusive, through the given copy reader, which one thread uses at a time. Any thread may
     * run it between {@link #beginCopy} and {@link #publishCopy()}; the caller's failure path
     * releases the copy through {@link #clear()} once every copying thread has stopped.
     */
    public void copyRows(int reader, FrozenHashJoinBuild build, long lo, long hi, SqlExecutionCircuitBreaker circuitBreaker) {
        assert pendingCopyAddress != 0 && lo >= 0 && hi <= build.getRowCount();
        final Reader copyReader = copyReaders.getQuick(reader);
        long row = pendingCopyAddress + lo * copyRowSize;
        for (long ordinal = lo; ordinal < hi; ordinal++, row += copyRowSize) {
            if ((ordinal & (COPY_ROWS_PER_CHECK - 1)) == 0) {
                circuitBreaker.statefulThrowExceptionIfTrippedTimeThrottled();
            }
            copyReader.positionRow(build.getRowId(ordinal));
            copyRow(copyReader, row);
        }
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

    /**
     * Bytes of the Parquet decode cache that each probe reader of an execution caches decoded row
     * groups in: an even share of the configured budget among the readers, but no less than a
     * quarter of it; see
     * {@link #HashJoinBuildFrames(CairoConfiguration, IntList, RecordMetadata, IntList, ColumnTypes, int)}.
     */
    public long getReaderCacheSize() {
        final long cacheSize = configuration.getSqlParquetCacheMemorySize();
        return Math.max(cacheSize / readerCount, cacheSize / 4);
    }

    /**
     * Rows of every frame, before any row filter.
     */
    public long getRowCount() {
        return rowCount;
    }

    /**
     * The open frame cursor, which serves the build input's symbol tables.
     */
    public SymbolTableSource getSymbolTableSource() {
        return frameCursor;
    }

    /**
     * True when a frame that {@link #loadFrames()} walked for the open execution is a Parquet
     * frame. Each probe reader decodes the Parquet row groups that its matches land on into its own
     * share of the decode cache, while a payload copy decodes each row group once.
     */
    public boolean hasParquetFrames() {
        return addressCache.hasParquetFrames();
    }

    /**
     * True when the build scan has a VARCHAR or STRING column that the build's key sink stages, whose
     * decoded size {@link #isReaderCacheExceeded(long, long)} takes from the keys the build staged.
     */
    public boolean hasStagedVarSizeKeys() {
        return stagedKeyDecodeFactor > 0;
    }

    /**
     * True when Parquet frames of this many rows in all decode to more than a probe reader's share
     * of the decode cache, {@link #getReaderCacheSize()}. A reader decodes a frame whole, every
     * column of the build scan, and keeps the frames it decoded until they fill its share, after
     * which it evicts the least recently used one, so a reader whose matches keep moving between
     * more frames than its share holds decodes them again and again.
     * <p>
     * The share's budget counts the data, aux and retained page bytes that a frame decodes to. A
     * fixed-size column decodes to its size a row. A var-size key column decodes as wide as its
     * values are, which the keys that the build staged tell: {@code stagedKeySize} bytes a key on
     * average, over all the key's columns, see {@code MapHashJoinBuild.getStagedKeySize()}.
     * <ul>
     * <li>A VARCHAR decodes to a 16-byte slice entry a row, which points into the decompressed page
     * that the reader retains: a data page that holds the value's bytes and, at most, a 4-byte length
     * for each, or a dictionary page that holds each distinct value once. The sink stages a 4-byte
     * header and the value's bytes, so a value counts what the sink staged.</li>
     * <li>A STRING decodes to an 8-byte offset a row, plus one a frame, and to a copy of the value:
     * a 4-byte length and two bytes a char, just as the sink stages it. A VARCHAR probe key makes the
     * sink stage the STRING as UTF-8 instead, a 4-byte header and at least a byte a char, so a value
     * then counts twice what the sink staged.</li>
     * </ul>
     * Each var-size key column counts {@value #VAR_SIZE_DECODED_SLACK} byte a row more for what the
     * page holds beyond the values, such as the definition levels of a nullable column, at most a
     * bit a row, and a STRING's extra offset a frame. The staged size errs high where it counts the
     * other columns of a composite key again, which decode at their fixed size, a SYMBOL's at 4
     * bytes rather than its staged text, and where it counts the padding that aligns a key's entry
     * in its key table. It averages over the distinct keys that the build kept, of native and
     * Parquet frames alike, though, so it errs low for a build whose longer keys repeat more often
     * than its shorter ones, or whose filter drops longer keys than it keeps. A var-size column that
     * the sink does not stage counts {@value #UNSTAGED_VAR_SIZE_DECODED_BYTES} bytes a row; the
     * planner admits none today, since it admits var-size build columns as join keys only.
     */
    public boolean isReaderCacheExceeded(long parquetFrameRows, long stagedKeySize) {
        final long rowSize = Math.max(decodedRowSize + stagedKeyDecodeFactor * stagedKeySize, 1);
        return parquetFrameRows > getReaderCacheSize() / rowSize;
    }

    /**
     * Walks the frames of the cursor that {@link #open} opened into the address cache, once per
     * execution. The walk opens every partition the build scan reads, so the owner runs it when
     * the build starts rather than when the cursor opens. On failure the caller's cleanup path
     * calls {@link #clear()}.
     */
    public void loadFrames() {
        assert frameCursor != null && frameRowCounts.size() == 0 && rowCount == 0;
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

    @Override
    public HashJoinPayloadSource.Reader newReader() {
        return new Reader(getReaderCacheSize());
    }

    /**
     * Switches every reader of this execution to the copy that {@link #copyRows} filled, and
     * releases what the copy readers decoded. Call it on the owner once every copying thread has
     * stopped, before the probes run.
     */
    public void publishCopy() {
        assert pendingCopyAddress != 0 && copyAddress == 0;
        // A decoded Parquet frame of the copy has no later use.
        Misc.freeObjListAndKeepObjects(copyReaders);
        copyAddress = pendingCopyAddress;
        copyCapacity = pendingCopySize;
        pendingCopyAddress = 0;
        pendingCopySize = 0;
    }

    /**
     * Releases what a copy reader decoded, once its thread has copied a frame's rows.
     */
    public void releaseCopyBuffers(int reader) {
        copyReaders.getQuick(reader).pool.releaseParquetBuffers();
    }

    /**
     * Opens the build scan's page frame cursor for an execution and walks its frames:
     * {@link #open} and {@link #loadFrames()} in one call. On failure the caller's cleanup path
     * calls {@link #clear()}.
     */
    public void of(RecordCursorFactory buildFactory, SqlExecutionContext executionContext) throws SqlException {
        open(buildFactory, executionContext);
        loadFrames();
    }

    /**
     * Opens the build scan's page frame cursor for an execution and reads no frame of it. The
     * cursor serves the build input's symbol tables from here on; {@link #loadFrames()} walks its
     * frames. On failure the caller's cleanup path calls {@link #clear()}.
     */
    public void open(RecordCursorFactory buildFactory, SqlExecutionContext executionContext) throws SqlException {
        assert frameCursor == null;
        memoryTracker = executionContext.getMemoryTracker();
        frameCursor = buildFactory.getPageFrameCursor(executionContext, ORDER_ASC);
        addressCache.of(buildFactory.getMetadata(), frameCursor.getColumnMapping(), frameCursor.isExternal());
    }

    /**
     * Bytes that a copied value of this column type takes, or -1 for a type the copy cannot hold.
     */
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

    // The type tag that the key sink stages this build scan column as, or -1 when it stages no key from it.
    private static int getStagedKeyType(int column, IntList keyColumns, ColumnTypes keyTypes) {
        for (int key = 0, n = Math.min(keyColumns.size(), keyTypes.getColumnCount()); key < n; key++) {
            if (keyColumns.getQuick(key) == column) {
                return ColumnType.tagOf(keyTypes.getColumnType(key));
            }
        }
        return -1;
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
                default ->
                        throw new AssertionError("uncopyable hash join payload type: " + ColumnType.nameOf(payloadTypes[col]));
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

        private Reader(long cacheSize) {
            pool = new PageFrameMemoryPool(configuration, cacheSize);
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
            // Symbol tables come from the build input, so this slot takes its own here, on the
            // owner, rather than on first use from a worker.
            for (int i = 0; i < payloadColumns.length; i++) {
                if (payloadSymbols[i]) {
                    record.getSymbolTable(payloadColumns[i]);
                }
            }
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

        // Rebinds the reader to the current execution without taking symbol tables: the copy pass
        // copies a SYMBOL's key and never resolves it.
        private void reopen(ParquetDecodeHint hint) {
            copyRow = 0;
            frameIndex = -1;
            recordFrameIndex = -1;
            isDirect = false;
            pool.setMemoryTracker(memoryTracker);
            pool.of(addressCache, hint);
            record.of(frameCursor);
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
