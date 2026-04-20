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

import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.std.MemoryTag;
import io.questdb.std.Numbers;
import io.questdb.std.Unsafe;
import io.questdb.std.Vect;
import io.questdb.std.str.Utf8Sequence;
import io.questdb.std.str.Utf8SplitString;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;

/**
 * M5.2: {@link SortedRowSink} that emits fixed-size Arrow batches into a
 * blocking queue for a Python consumer to drain. Supports fixed-width types
 * (BYTE, SHORT, INT, LONG, TIMESTAMP, DATE, FLOAT, DOUBLE) and variable-width
 * types VARCHAR (UTF-8) and BINARY. STRING (UTF-16), SYMBOL, ARRAY, UUID,
 * LONG256, IPv4, GEO*, CHAR, BOOLEAN are rejected in {@link #onStart} until
 * later milestones.
 * <p>
 * Per-column layout:
 * <ul>
 *   <li>Fixed-width: data = {@code batchSize * elemSize} bytes; validity =
 *       {@code ceil(batchSize / 8)} bytes; offsets absent (pointer = 0).</li>
 *   <li>Variable-width: data = growable byte buffer (starts at
 *       {@code 64 * batchSize} bytes, realloc'd on overflow); offsets =
 *       int32[batchSize + 1], offsets[0] == 0; validity as above.</li>
 * </ul>
 * Null handling: fixed columns use QuestDB null sentinels (INT/LONG/TIMESTAMP/
 * DATE: MIN_VALUE; FLOAT/DOUBLE: NaN). BYTE / SHORT surface parquet NULLs as
 * zeros (matches TableWriterSink). Variable columns use the type's own null
 * encoding (VARCHAR aux entry null-flag, BINARY {@code NULL_LEN} marker).
 * Arrow validity bit 0 => null, 1 => valid.
 * <p>
 * Threading and lifecycle are identical to earlier milestones: producer fills
 * a batch in {@link #acceptRow}, consumer pulls via {@link #takeBatch};
 * {@link #close} signals abandonment and drains the queue. Producer is the
 * sole owner of the active batch's buffers — {@code close} does not free
 * them, eliminating the UAF race that was the M5.0 known limit.
 */
public class ArrowStreamSink implements SortedRowSink {

    public static final int DEFAULT_BATCH_SIZE = 65_536;
    // Null-sentinel dispatch codes.
    static final int NK_F32 = 5;
    static final int NK_F64 = 6;
    static final int NK_I32 = 3;
    static final int NK_I64 = 4;
    static final int NK_NONE = 0;
    private static final int DEFAULT_QUEUE_CAPACITY = 2;
    private static final Log LOG = LogFactory.getLog(ArrowStreamSink.class);
    private static final int MEMORY_TAG = MemoryTag.NATIVE_IMPORT;
    private static final int VAR_DATA_INITIAL_PER_ROW = 64;
    private final int batchSize;
    private final BlockingQueue<ArrowBatchHandle> readyQueue;
    private final int validityBytesPerBatch;
    private final Utf8SplitString varcharView = new Utf8SplitString();
    private int[] colTypes;
    private int columnCount;
    private String[] columnNames;
    private long[] curDataPtrs;
    private long[] curDataSizes;
    private long[] curDataUsed; // bytes filled for var cols; unused for fixed
    private long[] curOffsetsPtrs;
    private long[] curOffsetsSizes;
    private int curRows;
    private long[] curValidityPtrs;
    private long[] curValiditySizes;
    private int[] elemSize;
    private volatile boolean isAbandoned;
    private volatile boolean isEnded;
    private boolean[] isVarCol;
    private int[] nullKind;

    public ArrowStreamSink() {
        this(DEFAULT_BATCH_SIZE, DEFAULT_QUEUE_CAPACITY);
    }

    public ArrowStreamSink(int batchSize) {
        this(batchSize, DEFAULT_QUEUE_CAPACITY);
    }

    public ArrowStreamSink(int batchSize, int queueCapacity) {
        if (batchSize <= 0) {
            throw CairoException.nonCritical()
                    .put("ArrowStreamSink batchSize must be positive [got=")
                    .put(batchSize).put(']');
        }
        if (queueCapacity <= 0) {
            throw CairoException.nonCritical()
                    .put("ArrowStreamSink queueCapacity must be positive [got=")
                    .put(queueCapacity).put(']');
        }
        this.batchSize = batchSize;
        this.validityBytesPerBatch = (batchSize + 7) >>> 3;
        this.readyQueue = new ArrayBlockingQueue<>(queueCapacity);
    }

    @Override
    public void acceptRow(ColumnBlockSource src, int row, long ts) {
        if (isAbandoned) {
            freeCurrentBuffers();
            throw CairoException.nonCritical().put("ArrowStreamSink consumer abandoned the stream");
        }
        if (curDataPtrs == null) {
            allocNewBatchBuffers();
        }
        try {
            final int rowInBatch = curRows;
            final long byteOffset = rowInBatch >>> 3;
            final int bitPos = rowInBatch & 7;
            final byte setBit = (byte) (1 << bitPos);
            for (int c = 0; c < columnCount; c++) {
                if (isVarCol[c]) {
                    appendVarValue(src, c, row, rowInBatch, byteOffset, setBit);
                } else {
                    appendFixedValue(src, c, row, rowInBatch, byteOffset, setBit);
                }
            }
            curRows++;
            if (curRows >= batchSize) {
                sealAndEnqueueCurrent();
            }
        } catch (Throwable t) {
            // Any producer-side exception (OOM in var-data realloc, offset
            // overflow, etc.) abandons the active batch. Free its buffers
            // here so they don't leak — close() deliberately does not touch
            // curDataPtrs to avoid racing with a still-live producer, so the
            // producer must clean up after itself before the exception
            // escapes. sealAndEnqueueCurrent transfers ownership to a handle
            // before it can throw, so if we reach the catch with curDataPtrs
            // null there is nothing to free (freeCurrentBuffers handles that).
            freeCurrentBuffers();
            throw t;
        }
    }

    @Override
    public void close() {
        isAbandoned = true;
        while (true) {
            final ArrowBatchHandle h = readyQueue.poll();
            if (h == null) {
                break;
            }
            h.release();
        }
        readyQueue.offer(ArrowBatchHandle.endSentinel());
    }

    public int getBatchSize() {
        return batchSize;
    }

    public int getColumnCount() {
        return columnCount;
    }

    public String getColumnName(int col) {
        return columnNames != null ? columnNames[col] : null;
    }

    public int getColumnType(int col) {
        return colTypes != null ? colTypes[col] : 0;
    }

    @Override
    public void onFinish() {
        try {
            if (curRows > 0) {
                sealAndEnqueueCurrent();
            }
            enqueueRespectingAbandonment(ArrowBatchHandle.endSentinel());
        } finally {
            isEnded = true;
        }
        LOG.info().$("arrow stream sink done").I$();
    }

    @Override
    public void onStart(SortedStreamMetadata meta, int tsColumnIndex, long totalRows) {
        this.columnCount = meta.getColumnCount();
        this.colTypes = new int[columnCount];
        this.elemSize = new int[columnCount];
        this.nullKind = new int[columnCount];
        this.isVarCol = new boolean[columnCount];
        this.columnNames = new String[columnCount];
        for (int c = 0; c < columnCount; c++) {
            final int type = meta.getColumnType(c);
            colTypes[c] = type;
            columnNames[c] = meta.getColumnName(c).toString();
            classifyColumn(c, type);
        }
        LOG.info()
                .$("arrow stream sink start [columns=").$(columnCount)
                .$(", batchSize=").$(batchSize)
                .$(", totalRows=").$(totalRows)
                .I$();
    }

    public ArrowBatchHandle takeBatch() throws InterruptedException {
        if (isEnded && readyQueue.isEmpty()) {
            return ArrowBatchHandle.endSentinel();
        }
        return readyQueue.take();
    }

    private static boolean isFixedNullAt(long ptr, int kind) {
        return switch (kind) {
            case NK_I32 -> Unsafe.getUnsafe().getInt(ptr) == Numbers.INT_NULL;
            case NK_I64 -> Unsafe.getUnsafe().getLong(ptr) == Numbers.LONG_NULL;
            case NK_F32 -> {
                final int raw = Unsafe.getUnsafe().getInt(ptr);
                yield Float.isNaN(Float.intBitsToFloat(raw));
            }
            case NK_F64 -> {
                final long raw = Unsafe.getUnsafe().getLong(ptr);
                yield Double.isNaN(Double.longBitsToDouble(raw));
            }
            default -> false;
        };
    }

    private void allocNewBatchBuffers() {
        curDataPtrs = new long[columnCount];
        curDataSizes = new long[columnCount];
        curDataUsed = new long[columnCount];
        curValidityPtrs = new long[columnCount];
        curValiditySizes = new long[columnCount];
        curOffsetsPtrs = new long[columnCount];
        curOffsetsSizes = new long[columnCount];
        for (int c = 0; c < columnCount; c++) {
            curValidityPtrs[c] = Unsafe.calloc(validityBytesPerBatch, MEMORY_TAG);
            curValiditySizes[c] = validityBytesPerBatch;
            if (nullKind[c] == NK_NONE && !isVarCol[c]) {
                Vect.memset(curValidityPtrs[c], validityBytesPerBatch, 0xFF);
            }
            if (isVarCol[c]) {
                final long initDataSize = (long) VAR_DATA_INITIAL_PER_ROW * batchSize;
                curDataPtrs[c] = Unsafe.malloc(initDataSize, MEMORY_TAG);
                curDataSizes[c] = initDataSize;
                curDataUsed[c] = 0;
                final long offsetsSize = (long) (batchSize + 1) * 4L;
                curOffsetsPtrs[c] = Unsafe.calloc(offsetsSize, MEMORY_TAG);
                curOffsetsSizes[c] = offsetsSize;
                // offsets[0] is already 0 from calloc; subsequent offsets are
                // written in acceptRow as rows come in.
            } else {
                final long dSize = (long) elemSize[c] * batchSize;
                curDataPtrs[c] = Unsafe.malloc(dSize, MEMORY_TAG);
                curDataSizes[c] = dSize;
            }
        }
        curRows = 0;
    }

    private void appendFixedValue(
            ColumnBlockSource src, int c, int row, int rowInBatch,
            long validByteOffset, byte setBit
    ) {
        final int size = elemSize[c];
        final long srcPtr = src.getChunkDataPtr(c) + (long) row * size;
        final long dstPtr = curDataPtrs[c] + (long) rowInBatch * size;
        copyFixed(srcPtr, dstPtr, size);
        if (nullKind[c] != NK_NONE && !isFixedNullAt(srcPtr, nullKind[c])) {
            final long validByte = curValidityPtrs[c] + validByteOffset;
            Unsafe.getUnsafe().putByte(
                    validByte,
                    (byte) (Unsafe.getUnsafe().getByte(validByte) | setBit)
            );
        }
        // NK_NONE: validity was memset to 0xFF; no per-row update needed.
        // Null case: validity bit stays 0 (calloc initialised).
    }

    private void appendVarValue(
            ColumnBlockSource src, int c, int row, int rowInBatch,
            long validByteOffset, byte setBit
    ) {
        final int type = colTypes[c];
        final int tag = ColumnType.tagOf(type);
        final long srcAuxPtr = src.getChunkAuxPtr(c);
        final long srcDataPtr = src.getChunkDataPtr(c);
        final long srcAuxSize = src.getChunkAuxSize(c);
        final long srcDataSize = src.getChunkDataSize(c);
        final long priorUsed = curDataUsed[c];
        long valueLen = 0;
        boolean isNull = false;

        if (tag == ColumnType.VARCHAR) {
            final Utf8Sequence v = VarcharTypeDriver.getSplitValue(
                    srcAuxPtr, srcAuxPtr + srcAuxSize,
                    srcDataPtr, srcDataPtr + srcDataSize,
                    row, varcharView
            );
            if (v == null) {
                isNull = true;
            } else {
                valueLen = v.size();
                if (valueLen > 0) {
                    ensureVarDataCapacity(c, priorUsed + valueLen);
                    v.writeTo(curDataPtrs[c] + priorUsed, 0, (int) valueLen);
                }
            }
        } else {
            // BINARY: aux = 8-byte offset; data = 8-byte length header + bytes.
            final long auxEntry = srcAuxPtr + (long) row * 8L;
            final long valOffset = Unsafe.getUnsafe().getLong(auxEntry);
            final long valAddr = srcDataPtr + valOffset;
            final long len = Unsafe.getUnsafe().getLong(valAddr);
            if (len == TableUtils.NULL_LEN) {
                isNull = true;
            } else {
                valueLen = len;
                if (valueLen > 0) {
                    ensureVarDataCapacity(c, priorUsed + valueLen);
                    Vect.memcpy(curDataPtrs[c] + priorUsed, valAddr + 8, valueLen);
                }
            }
        }

        // Advance tracked "used" bytes only for non-null values. Arrow
        // convention: null rows have offsets[i+1] == offsets[i] and validity
        // bit 0.
        final long nextUsed = priorUsed + (isNull ? 0 : valueLen);
        curDataUsed[c] = nextUsed;

        // Write offsets[rowInBatch + 1] = nextUsed as int32.
        // offsets[0] was zero'd at alloc; each row writes the next slot so
        // earlier slots are never revisited.
        if (nextUsed > Integer.MAX_VALUE) {
            throw CairoException.nonCritical()
                    .put("ArrowStreamSink var-column offset exceeds int32 [col=")
                    .put(columnNames[c]).put(", bytes=").put(nextUsed).put(']');
        }
        Unsafe.getUnsafe().putInt(
                curOffsetsPtrs[c] + (long) (rowInBatch + 1) * 4L,
                (int) nextUsed
        );

        if (!isNull) {
            final long validByte = curValidityPtrs[c] + validByteOffset;
            Unsafe.getUnsafe().putByte(
                    validByte,
                    (byte) (Unsafe.getUnsafe().getByte(validByte) | setBit)
            );
        }
    }

    private void classifyColumn(int col, int type) {
        final int tag = ColumnType.tagOf(type);
        switch (tag) {
            case ColumnType.BYTE -> {
                elemSize[col] = 1;
                nullKind[col] = NK_NONE;
            }
            case ColumnType.SHORT -> {
                elemSize[col] = 2;
                nullKind[col] = NK_NONE;
            }
            case ColumnType.INT -> {
                elemSize[col] = 4;
                nullKind[col] = NK_I32;
            }
            case ColumnType.LONG, ColumnType.TIMESTAMP, ColumnType.DATE -> {
                elemSize[col] = 8;
                nullKind[col] = NK_I64;
            }
            case ColumnType.FLOAT -> {
                elemSize[col] = 4;
                nullKind[col] = NK_F32;
            }
            case ColumnType.DOUBLE -> {
                elemSize[col] = 8;
                nullKind[col] = NK_F64;
            }
            case ColumnType.VARCHAR, ColumnType.BINARY -> {
                isVarCol[col] = true;
                elemSize[col] = -1;
                nullKind[col] = NK_NONE; // validity handled in appendVarValue
            }
            // STRING (UTF-16) intentionally not supported here: parquet has no
            // UTF-16 encoding, so parquet text surfaces as VARCHAR (UTF-8)
            // through QuestDB's PartitionDecoder — the only input to this
            // sink's upstream (phaseB). A STRING column could only reach the
            // sink via a non-parquet data path that does not exist today.
            // Reject explicitly to catch a future regression rather than
            // silently copy raw UTF-16 bytes as if they were UTF-8.
            default -> throw CairoException.nonCritical()
                    .put("ArrowStreamSink does not yet support column type [col=")
                    .put(columnNames[col])
                    .put(", type=").put(ColumnType.nameOf(type))
                    .put(']');
        }
    }

    private void copyFixed(long src, long dst, int size) {
        switch (size) {
            case 1 -> Unsafe.getUnsafe().putByte(dst, Unsafe.getUnsafe().getByte(src));
            case 2 -> Unsafe.getUnsafe().putShort(dst, Unsafe.getUnsafe().getShort(src));
            case 4 -> Unsafe.getUnsafe().putInt(dst, Unsafe.getUnsafe().getInt(src));
            case 8 -> Unsafe.getUnsafe().putLong(dst, Unsafe.getUnsafe().getLong(src));
            default -> Vect.memcpy(dst, src, size);
        }
    }

    private void enqueueRespectingAbandonment(ArrowBatchHandle handle) {
        while (!isAbandoned) {
            try {
                if (readyQueue.offer(handle, 100L, TimeUnit.MILLISECONDS)) {
                    return;
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                handle.release();
                throw CairoException.nonCritical().put("ArrowStreamSink producer interrupted");
            }
        }
        handle.release();
        throw CairoException.nonCritical().put("ArrowStreamSink consumer abandoned the stream");
    }

    private void ensureVarDataCapacity(int col, long needed) {
        long cap = curDataSizes[col];
        if (needed <= cap) {
            return;
        }
        long newCap = Math.max(cap * 2, needed);
        curDataPtrs[col] = Unsafe.realloc(curDataPtrs[col], cap, newCap, MEMORY_TAG);
        curDataSizes[col] = newCap;
    }

    private void freeCurrentBuffers() {
        if (curDataPtrs == null) {
            return;
        }
        for (int c = 0; c < columnCount; c++) {
            if (curDataPtrs[c] != 0) {
                Unsafe.free(curDataPtrs[c], curDataSizes[c], MEMORY_TAG);
                curDataPtrs[c] = 0;
            }
            if (curValidityPtrs[c] != 0) {
                Unsafe.free(curValidityPtrs[c], curValiditySizes[c], MEMORY_TAG);
                curValidityPtrs[c] = 0;
            }
            if (curOffsetsPtrs[c] != 0) {
                Unsafe.free(curOffsetsPtrs[c], curOffsetsSizes[c], MEMORY_TAG);
                curOffsetsPtrs[c] = 0;
            }
        }
        curDataPtrs = null;
        curDataSizes = null;
        curDataUsed = null;
        curValidityPtrs = null;
        curValiditySizes = null;
        curOffsetsPtrs = null;
        curOffsetsSizes = null;
        curRows = 0;
    }

    private void sealAndEnqueueCurrent() {
        final long[] dataPtrs = curDataPtrs;
        final long[] dataSizes = curDataSizes;
        final long[] validityPtrs = curValidityPtrs;
        final long[] validitySizes = curValiditySizes;
        final long[] offsetsPtrs = curOffsetsPtrs;
        final long[] offsetsSizes = curOffsetsSizes;
        final int rows = curRows;
        curDataPtrs = null;
        curDataSizes = null;
        curDataUsed = null;
        curValidityPtrs = null;
        curValiditySizes = null;
        curOffsetsPtrs = null;
        curOffsetsSizes = null;
        curRows = 0;

        final ArrowBatchHandle handle = new ArrowBatchHandle(
                columnCount, dataPtrs, dataSizes,
                validityPtrs, validitySizes,
                offsetsPtrs, offsetsSizes,
                rows, false
        );
        enqueueRespectingAbandonment(handle);
    }
}
