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

/**
 * Pre-sized {@link SortedRowSink} that materialises the full sorted result as
 * a single in-memory Arrow table. Complementary to {@link ArrowStreamSink}:
 * use this when the caller knows the full result fits in RAM and wants one
 * contiguous pa.Table rather than an iterator of RecordBatches.
 * <p>
 * In {@link #onStart} the sink uses the merger-supplied {@code totalRows} to
 * allocate one data + validity (+ offsets, for var columns) buffer per
 * column. Before allocating it enforces an optional {@code memoryBudgetBytes}
 * so callers can reject runs that would blow their RAM ceiling rather than
 * discover it via OOM. The estimate assumes 64 bytes/row for var columns — a
 * heuristic shared with the rest of the pipeline.
 * <p>
 * Type coverage mirrors {@link ArrowStreamSink}: BYTE, SHORT, INT, LONG,
 * TIMESTAMP, DATE, FLOAT, DOUBLE, VARCHAR, BINARY. STRING, SYMBOL, UUID,
 * LONG256, GEO, IPv4, BOOLEAN, ARRAY are rejected in {@link #onStart} --
 * extending the set here should happen in lockstep with the streaming sink.
 * <p>
 * Threading: the sink is single-threaded. Unlike {@link ArrowStreamSink},
 * there is no queue/consumer thread -- phaseB runs, the sink accumulates, and
 * after {@link #onFinish} the caller reads the buffers directly off this
 * object via the {@code getXxxPtr/Size} accessors. This is simpler and faster
 * when the result fits in memory; it does not pipeline producer/consumer.
 * <p>
 * Lifecycle: {@link #close} frees everything. Calling the accessor methods
 * after close is undefined.
 */
public class ArrowTableSink implements SortedRowSink {

    public static final long NO_MEMORY_BUDGET = 0L;
    private static final long HEURISTIC_VAR_BYTES_PER_ROW = 64L;
    private static final Log LOG = LogFactory.getLog(ArrowTableSink.class);
    private static final int MEMORY_TAG = MemoryTag.NATIVE_IMPORT;
    // Same null-kind dispatch codes as ArrowStreamSink to keep the two
    // implementations aligned; if they diverge, re-extract into a shared
    // helper rather than letting each sink drift independently.
    private static final int NK_F32 = 5;
    private static final int NK_F64 = 6;
    private static final int NK_I32 = 3;
    private static final int NK_I64 = 4;
    private static final int NK_NONE = 0;
    private final long memoryBudgetBytes;
    private final Utf8SplitString varcharView = new Utf8SplitString();
    private int[] colTypes;
    private int columnCount;
    private String[] columnNames;
    private long[] dataPtrs;
    private long[] dataSizes;
    private long[] dataUsed;
    private int[] elemSize;
    private boolean[] isVarCol;
    private int[] nullKind;
    private long[] offsetsPtrs;
    private long[] offsetsSizes;
    private long reservedRows;
    private long rowCount;
    private long[] validityPtrs;
    private long[] validitySizes;

    public ArrowTableSink() {
        this(NO_MEMORY_BUDGET);
    }

    public ArrowTableSink(long memoryBudgetBytes) {
        if (memoryBudgetBytes < 0) {
            throw CairoException.nonCritical()
                    .put("ArrowTableSink memoryBudgetBytes must be non-negative [got=")
                    .put(memoryBudgetBytes).put(']');
        }
        this.memoryBudgetBytes = memoryBudgetBytes;
    }

    @Override
    public void acceptRow(ColumnBlockSource src, int row, long ts) {
        if (dataPtrs == null) {
            throw CairoException.nonCritical().put("ArrowTableSink was closed before acceptRow");
        }
        if (rowCount >= reservedRows) {
            throw CairoException.nonCritical()
                    .put("ArrowTableSink received more rows than onStart announced [reserved=")
                    .put(reservedRows).put(", got=").put(rowCount + 1).put(']');
        }
        final int rowIdx = (int) rowCount;
        final long byteOffset = rowIdx >>> 3;
        final int bitPos = rowIdx & 7;
        final byte setBit = (byte) (1 << bitPos);
        for (int c = 0; c < columnCount; c++) {
            if (isVarCol[c]) {
                appendVarValue(src, c, row, rowIdx, byteOffset, setBit);
            } else {
                appendFixedValue(src, c, row, rowIdx, byteOffset, setBit);
            }
        }
        rowCount++;
    }

    @Override
    public void close() {
        if (dataPtrs == null) {
            return;
        }
        for (int c = 0; c < columnCount; c++) {
            if (dataPtrs[c] != 0) {
                Unsafe.free(dataPtrs[c], dataSizes[c], MEMORY_TAG);
                dataPtrs[c] = 0;
            }
            if (validityPtrs[c] != 0) {
                Unsafe.free(validityPtrs[c], validitySizes[c], MEMORY_TAG);
                validityPtrs[c] = 0;
            }
            if (offsetsPtrs[c] != 0) {
                Unsafe.free(offsetsPtrs[c], offsetsSizes[c], MEMORY_TAG);
                offsetsPtrs[c] = 0;
            }
        }
        dataPtrs = null;
        dataSizes = null;
        dataUsed = null;
        validityPtrs = null;
        validitySizes = null;
        offsetsPtrs = null;
        offsetsSizes = null;
        columnCount = 0;
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

    public long getDataPtr(int col) {
        checkOpen();
        return dataPtrs[col];
    }

    public long getDataSize(int col) {
        checkOpen();
        return dataSizes[col];
    }

    public long getOffsetsPtr(int col) {
        checkOpen();
        return offsetsPtrs[col];
    }

    public long getOffsetsSize(int col) {
        checkOpen();
        return offsetsSizes[col];
    }

    public long getRowCount() {
        return rowCount;
    }

    public long getValidityPtr(int col) {
        checkOpen();
        return validityPtrs[col];
    }

    public long getValiditySize(int col) {
        checkOpen();
        return validitySizes[col];
    }

    @Override
    public void onFinish() {
        if (rowCount != reservedRows) {
            // phaseB must always deliver the announced totalRows. If it
            // didn't the caller has a bug upstream — surface it as a clear
            // sink error rather than silently leaving the last few rows of
            // the validity bitmap uninitialised.
            throw CairoException.nonCritical()
                    .put("ArrowTableSink finished short [reserved=")
                    .put(reservedRows).put(", got=").put(rowCount).put(']');
        }
        LOG.info().$("arrow table sink done [rows=").$(rowCount).$("]").I$();
    }

    @Override
    public void onStart(SortedStreamMetadata meta, int tsColumnIndex, long totalRows) {
        if (totalRows < 0) {
            throw CairoException.nonCritical()
                    .put("ArrowTableSink got negative totalRows [").put(totalRows).put(']');
        }
        if (totalRows > Integer.MAX_VALUE) {
            // Arrow arrays are indexed by int32 length; beyond that we would
            // need Arrow's LargeString / chunked arrays. Out of scope here —
            // callers with >2B rows should stream with ArrowStreamSink.
            throw CairoException.nonCritical()
                    .put("ArrowTableSink supports up to ").put(Integer.MAX_VALUE)
                    .put(" rows; got ").put(totalRows);
        }

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

        final long validityBytes = (totalRows + 7) >>> 3;
        final long estimate = estimateBytes(totalRows, validityBytes);
        if (memoryBudgetBytes != NO_MEMORY_BUDGET && estimate > memoryBudgetBytes) {
            throw CairoException.nonCritical()
                    .put("ArrowTableSink memory budget exceeded [estimated=")
                    .put(estimate)
                    .put(", budget=").put(memoryBudgetBytes)
                    .put(", rows=").put(totalRows)
                    .put(", cols=").put(columnCount).put(']');
        }

        reservedRows = totalRows;
        rowCount = 0;
        allocateBuffers(totalRows, validityBytes);

        LOG.info()
                .$("arrow table sink start [cols=").$(columnCount)
                .$(", rows=").$(totalRows)
                .$(", estimatedBytes=").$(estimate)
                .I$();
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

    private void allocateBuffers(long totalRows, long validityBytes) {
        dataPtrs = new long[columnCount];
        dataSizes = new long[columnCount];
        dataUsed = new long[columnCount];
        validityPtrs = new long[columnCount];
        validitySizes = new long[columnCount];
        offsetsPtrs = new long[columnCount];
        offsetsSizes = new long[columnCount];

        for (int c = 0; c < columnCount; c++) {
            validityPtrs[c] = Unsafe.calloc(validityBytes, MEMORY_TAG);
            validitySizes[c] = validityBytes;
            if (nullKind[c] == NK_NONE && !isVarCol[c]) {
                Vect.memset(validityPtrs[c], validityBytes, 0xFF);
            }
            if (isVarCol[c]) {
                final long initDataSize = HEURISTIC_VAR_BYTES_PER_ROW * totalRows;
                dataPtrs[c] = Unsafe.malloc(Math.max(initDataSize, 16L), MEMORY_TAG);
                dataSizes[c] = Math.max(initDataSize, 16L);
                final long offsetsBytes = (totalRows + 1L) * 4L;
                offsetsPtrs[c] = Unsafe.calloc(offsetsBytes, MEMORY_TAG);
                offsetsSizes[c] = offsetsBytes;
            } else {
                final long bytes = (long) elemSize[c] * totalRows;
                dataPtrs[c] = Unsafe.malloc(Math.max(bytes, 16L), MEMORY_TAG);
                dataSizes[c] = Math.max(bytes, 16L);
            }
        }
    }

    private void appendFixedValue(
            ColumnBlockSource src, int c, int row, int rowIdx,
            long validByteOffset, byte setBit
    ) {
        final int size = elemSize[c];
        final long srcPtr = src.getChunkDataPtr(c) + (long) row * size;
        final long dstPtr = dataPtrs[c] + (long) rowIdx * size;
        copyFixed(srcPtr, dstPtr, size);
        if (nullKind[c] != NK_NONE && !isFixedNullAt(srcPtr, nullKind[c])) {
            final long validByte = validityPtrs[c] + validByteOffset;
            Unsafe.getUnsafe().putByte(
                    validByte,
                    (byte) (Unsafe.getUnsafe().getByte(validByte) | setBit)
            );
        }
    }

    private void appendVarValue(
            ColumnBlockSource src, int c, int row, int rowIdx,
            long validByteOffset, byte setBit
    ) {
        final int type = colTypes[c];
        final int tag = ColumnType.tagOf(type);
        final long srcAuxPtr = src.getChunkAuxPtr(c);
        final long srcDataPtr = src.getChunkDataPtr(c);
        final long srcAuxSize = src.getChunkAuxSize(c);
        final long srcDataSize = src.getChunkDataSize(c);
        final long priorUsed = dataUsed[c];
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
                    v.writeTo(dataPtrs[c] + priorUsed, 0, (int) valueLen);
                }
            }
        } else {
            // BINARY
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
                    Vect.memcpy(dataPtrs[c] + priorUsed, valAddr + 8, valueLen);
                }
            }
        }

        final long nextUsed = priorUsed + (isNull ? 0 : valueLen);
        dataUsed[c] = nextUsed;
        if (nextUsed > Integer.MAX_VALUE) {
            throw CairoException.nonCritical()
                    .put("ArrowTableSink var column exceeds int32 offset [col=")
                    .put(columnNames[c]).put(", bytes=").put(nextUsed).put(']');
        }
        Unsafe.getUnsafe().putInt(
                offsetsPtrs[c] + (long) (rowIdx + 1) * 4L,
                (int) nextUsed
        );
        if (!isNull) {
            final long validByte = validityPtrs[c] + validByteOffset;
            Unsafe.getUnsafe().putByte(
                    validByte,
                    (byte) (Unsafe.getUnsafe().getByte(validByte) | setBit)
            );
        }
    }

    private void checkOpen() {
        if (dataPtrs == null) {
            throw CairoException.nonCritical()
                    .put("ArrowTableSink accessors called after close()");
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
                nullKind[col] = NK_NONE;
            }
            default -> throw CairoException.nonCritical()
                    .put("ArrowTableSink does not yet support column type [col=")
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

    private void ensureVarDataCapacity(int col, long needed) {
        long cap = dataSizes[col];
        if (needed <= cap) {
            return;
        }
        long newCap = Math.max(cap * 2, needed);
        dataPtrs[col] = Unsafe.realloc(dataPtrs[col], cap, newCap, MEMORY_TAG);
        dataSizes[col] = newCap;
    }

    private long estimateBytes(long totalRows, long validityBytes) {
        long total = 0;
        for (int c = 0; c < columnCount; c++) {
            total += validityBytes;
            if (isVarCol[c]) {
                total += HEURISTIC_VAR_BYTES_PER_ROW * totalRows; // data heuristic
                total += (totalRows + 1L) * 4L;                    // offsets
            } else {
                total += (long) elemSize[c] * totalRows;
            }
        }
        return total;
    }
}
