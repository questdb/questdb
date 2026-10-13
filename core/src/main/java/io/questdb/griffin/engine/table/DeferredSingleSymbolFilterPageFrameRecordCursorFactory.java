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
import io.questdb.cairo.TableReader;
import io.questdb.cairo.TableUtils;
import io.questdb.cairo.idx.AbstractPostingIndexReader;
import io.questdb.cairo.idx.IndexReader;
import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.PageFrameCursor;
import io.questdb.cairo.sql.PartitionFormat;
import io.questdb.cairo.sql.PartitionFrame;
import io.questdb.cairo.sql.PartitionFrameCursor;
import io.questdb.cairo.sql.PartitionFrameCursorFactory;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.cairo.sql.RecordMetadata;
import io.questdb.cairo.sql.RowCursorFactory;
import io.questdb.cairo.sql.SingleSymbolFilter;
import io.questdb.cairo.sql.StaticSymbolTable;
import io.questdb.cairo.sql.SymbolTable;
import io.questdb.griffin.PlanSink;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.std.IntList;
import io.questdb.std.Misc;
import io.questdb.std.Numbers;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.TestOnly;

/**
 * A single-symbol scan over a symbol index: the index's row cursor, frame by frame, on the query's
 * thread. With a parallel plan attached ({@link #setParallelScan}, a POSTING index, the frame
 * dispatch switch on), each open first counts the symbol's rows from the posting index, and when
 * they are many, or spread over many partitions, opens the parallel plan instead: an Async Filter
 * whose workers read each frame's rows of the symbol from the frame's posting index, see
 * {@link IndexKeyPageFrameCursor}. Both return the same rows in the same order. The plan the
 * factory shows, and the shape other plans see (SAMPLE BY's conversion to page frames), are the
 * serial scan's.
 */
public class DeferredSingleSymbolFilterPageFrameRecordCursorFactory extends PageFrameRecordCursorFactory {
    // the parallel plan opens from this many rows of the symbol, or ...
    static final long PARALLEL_MIN_ROWS = 1 << 20;
    // ... from this many rows spread over at least PARALLEL_MIN_PARTITIONS partitions
    static final long PARALLEL_MIN_SPREAD_ROWS = 16_384;
    static final int PARALLEL_MIN_PARTITIONS = 64;
    // partitions whose counts an estimate reads at most, by stride past that
    private static final int ESTIMATE_MAX_PARTITIONS = 256;
    // tests: the thresholds in force, see setParallelThresholdsForTesting()
    private static long parallelMinRows = PARALLEL_MIN_ROWS;
    private static long parallelMinSpreadRows = PARALLEL_MIN_SPREAD_ROWS;
    private static long parallelMinPartitions = PARALLEL_MIN_PARTITIONS;
    private final int symbolColumnIndex;
    private final SingleSymbolFilter symbolFilter;
    private boolean convertedToFrame;
    // the parallel plan, or null; see setParallelScan()
    private RecordCursorFactory parallelScan;
    private long parallelOpens;
    // the rows an open reads at most (a constant LIMIT), or Long.MAX_VALUE; see setParallelScan()
    private long parallelRowsCap = Long.MAX_VALUE;
    // tests: the last open's estimate of the symbol's rows
    private long lastEstimateRows = -1;
    private long serialOpens;
    private Function symbolFunc;
    private int symbolKey;

    public DeferredSingleSymbolFilterPageFrameRecordCursorFactory(
            @NotNull CairoConfiguration configuration,
            int symbolColumnIndex,
            Function symbolFunc,
            RowCursorFactory rowCursorFactory,
            RecordMetadata metadata,
            PartitionFrameCursorFactory partitionFrameCursorFactory,
            boolean followsOrderByAdvice,
            @NotNull IntList columnIndexes,
            @NotNull IntList columnSizes,
            boolean supportsRandomAccess
    ) {
        super(
                configuration,
                metadata,
                partitionFrameCursorFactory,
                rowCursorFactory,
                followsOrderByAdvice,
                null,
                false,
                columnIndexes,
                columnSizes,
                supportsRandomAccess,
                false
        );
        this.symbolFunc = symbolFunc;
        symbolKey = SymbolTable.VALUE_NOT_FOUND;
        this.symbolColumnIndex = symbolColumnIndex;

        symbolFilter = new SingleSymbolFilter() {
            @Override
            public int getColumnIndex() {
                return DeferredSingleSymbolFilterPageFrameRecordCursorFactory.this.symbolColumnIndex;
            }

            @Override
            public int getSymbolFilterKey() {
                return symbolKey;
            }
        };
    }

    public SingleSymbolFilter convertToSampleByIndexPageFrameCursorFactory() {
        convertedToFrame = true;
        return symbolFilter;
    }

    @Override
    protected void _close() {
        final Function symbolFunc = this.symbolFunc;
        this.symbolFunc = null;
        // the parallel plan borrows the key function and the partition frames: it goes first
        Throwable failure = Misc.freeBestEffort(null, parallelScan);
        parallelScan = null;
        try {
            super._close();
        } catch (Throwable th) {
            failure = failure == null ? th : failure;
        }
        failure = Misc.freeBestEffort(failure, symbolFunc);
        CairoException.rethrowCleanupFailure(failure);
    }

    @Override
    public RecordCursor getCursor(SqlExecutionContext executionContext) throws SqlException {
        if (parallelScan != null && !convertedToFrame && isParallelWorthIt(executionContext)) {
            parallelOpens++;
            return parallelScan.getCursor(executionContext);
        }
        serialOpens++;
        return super.getCursor(executionContext);
    }

    /**
     * Opens of the parallel plan and of the serial scan, since the factory was built.
     */
    @TestOnly
    public long[] getOpenCounts() {
        return new long[]{serialOpens, parallelOpens};
    }

    /**
     * Tests: the rows, or the rows over partitions, from which the parallel plan opens.
     */
    @TestOnly
    public static void setParallelThresholdsForTesting(long minRows, long minSpreadRows, long minPartitions) {
        parallelMinRows = minRows;
        parallelMinSpreadRows = minSpreadRows;
        parallelMinPartitions = minPartitions;
    }

    @TestOnly
    public static void resetParallelThresholdsForTesting() {
        setParallelThresholdsForTesting(PARALLEL_MIN_ROWS, PARALLEL_MIN_SPREAD_ROWS, PARALLEL_MIN_PARTITIONS);
    }

    @TestOnly
    public long getLastEstimateRows() {
        return lastEstimateRows;
    }

    @TestOnly
    public RecordCursorFactory getParallelScan() {
        return parallelScan;
    }

    /**
     * Takes the parallel plan, which shares this factory's partition frames and key function;
     * the factory frees it. {@code rowsCap} is the rows an open reads at most, a constant LIMIT, or
     * Long.MAX_VALUE: an open whose cap is below the parallel plan's thresholds keeps the serial
     * scan without an estimate.
     */
    public void setParallelScan(@NotNull RecordCursorFactory parallelScan, long rowsCap) {
        this.parallelScan = parallelScan;
        this.parallelRowsCap = rowsCap;
    }

    @Override
    public String getBaseColumnName(int columnIndex) {
        return getMetadata().getColumnName(columnIndex);
    }

    @Override
    public PageFrameCursor getPageFrameCursor(SqlExecutionContext executionContext, int order) throws SqlException {
        assert convertedToFrame;
        PartitionFrameCursor partitionFrameCursor = partitionFrameCursorFactory.getCursor(executionContext, columnIndexes, order);
        initFwdPageFrameCursor(partitionFrameCursor, executionContext);
        try {
            if (symbolKey == SymbolTable.VALUE_NOT_FOUND) {
                final CharSequence symbol = symbolFunc.getStrA(null);
                final StaticSymbolTable symbolMapReader = fwdPageFrameCursor.getSymbolTable(symbolColumnIndex);
                symbolKey = symbolMapReader.keyOf(symbol);
                if (symbolKey != SymbolTable.VALUE_NOT_FOUND) {
                    symbolKey = TableUtils.toIndexKey(symbolKey);
                }
            }
            return fwdPageFrameCursor;
        } catch (Throwable th) {
            Misc.free(fwdPageFrameCursor);
            throw th;
        }
    }

    @Override
    public void revertFromSampleByIndexPageFrameCursorFactory() {
        convertedToFrame = false;
    }

    @Override
    public boolean supportsPageFrameCursor() {
        return convertedToFrame;
    }

    // Whether this open's selection is large enough for the parallel plan: from the posting index's
    // counts of the symbol, a partition at a time, sampled by stride past ESTIMATE_MAX_PARTITIONS.
    private boolean isParallelWorthIt(SqlExecutionContext executionContext) throws SqlException {
        final long minRows = Math.min(parallelMinRows, parallelMinSpreadRows);
        if (parallelRowsCap < minRows) {
            // a LIMIT reads fewer rows than any parallel open needs
            lastEstimateRows = parallelRowsCap;
            return false;
        }
        try (PartitionFrameCursor frameCursor = partitionFrameCursorFactory.getCursor(
                executionContext,
                columnIndexes,
                PartitionFrameCursorFactory.ORDER_ASC
        )) {
            final TableReader reader = frameCursor.getTableReader();
            final int readerColumnIndex = columnIndexes.getQuick(symbolColumnIndex);
            symbolFunc.init(frameCursor, executionContext);
            final int key = reader.getSymbolMapReader(readerColumnIndex).keyOf(symbolFunc.getStrA(null));
            if (key == SymbolTable.VALUE_NOT_FOUND) {
                lastEstimateRows = 0;
                return false;
            }
            final int indexKey = TableUtils.toIndexKey(key);
            final long bound = frameCursor.getFrameCountUpperBound();
            final long partitions = bound > 0 ? bound : reader.getPartitionCount();
            final long tableRows = reader.size();
            if (tableRows < minRows || (tableRows < parallelMinRows && partitions < parallelMinPartitions)) {
                // the whole table holds too few rows, or too few over too few partitions: no count
                // of the symbol can reach the thresholds, so none is read
                lastEstimateRows = tableRows;
                return false;
            }
            final long stride = Math.max(1, (partitions + ESTIMATE_MAX_PARTITIONS - 1) / ESTIMATE_MAX_PARTITIONS);
            long sampledRows = 0;
            long sampledPartitions = 0;
            long partitionsWithRows = 0;
            long frameIndex = 0;
            while (true) {
                final boolean probe = frameIndex % stride == 0;
                final PartitionFrame frame = frameCursor.next(probe ? 0 : Long.MAX_VALUE);
                if (frame == null) {
                    break;
                }
                frameIndex++;
                if (!probe || frame.getRowHi() <= frame.getRowLo() || frame.getPartitionFormat() != PartitionFormat.NATIVE) {
                    continue;
                }
                final IndexReader indexReader = reader.getIndexReader(frame.getPartitionIndex(), readerColumnIndex, IndexReader.DIR_FORWARD);
                if (!(indexReader instanceof AbstractPostingIndexReader posting)) {
                    continue;
                }
                final long hiInclusive = frame.getRowHi() - 1;
                final long entryMax = posting.getEntryMaxValue();
                long count = posting.estimateMatchesClamped(indexKey, frame.getRowLo(), hiInclusive, entryMax >= 0 ? Math.min(hiInclusive, entryMax) : hiInclusive);
                if (count == AbstractPostingIndexReader.ESTIMATE_REJECT || count == Numbers.LONG_NULL) {
                    count = frame.getRowHi() - frame.getRowLo();
                }
                sampledPartitions++;
                sampledRows += count;
                if (count > 0) {
                    partitionsWithRows++;
                }
            }
            if (sampledPartitions == 0) {
                return false;
            }
            final long rows = Math.min(sampledRows * stride, parallelRowsCap);
            lastEstimateRows = rows;
            return rows >= parallelMinRows
                    || (rows >= parallelMinSpreadRows && partitionsWithRows * stride >= parallelMinPartitions);
        }
    }

    @Override
    public void toPlan(PlanSink sink) {
        sink.type("DeferredSingleSymbolFilterPageFrame");
        super.toPlanInner(sink);
    }

    @Override
    protected RecordCursor initRecordCursor(
            PageFrameCursor pageFrameCursor,
            SqlExecutionContext executionContext
    ) throws SqlException {
        assert !convertedToFrame;
        return super.initRecordCursor(pageFrameCursor, executionContext);
    }
}
