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

package io.questdb.griffin.engine.table;

import io.questdb.cairo.AbstractRecordCursorFactory;
import io.questdb.cairo.CairoException;
import io.questdb.cairo.TableReader;
import io.questdb.cairo.TableToken;
import io.questdb.cairo.TableUtils;
import io.questdb.cairo.idx.AbstractPostingIndexReader;
import io.questdb.cairo.idx.BitmapIndexFwdReader;
import io.questdb.cairo.idx.IndexReader;
import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.PageFrameCursor;
import io.questdb.cairo.sql.PartitionFrame;
import io.questdb.cairo.sql.PartitionFrameCursor;
import io.questdb.cairo.sql.PartitionFrameCursorFactory;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.cairo.sql.RecordMetadata;
import io.questdb.cairo.sql.RowCursor;
import io.questdb.cairo.sql.StaticSymbolTable;
import io.questdb.cairo.sql.SymbolTable;
import io.questdb.cairo.sql.SymbolTableSource;
import io.questdb.griffin.PlanSink;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.engine.functions.BooleanFunction;
import io.questdb.griffin.engine.functions.regex.SymbolKeySetProvider;
import io.questdb.std.IntList;
import io.questdb.std.Misc;
import io.questdb.std.Numbers;
import io.questdb.std.ObjList;
import io.questdb.std.str.CharSink;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.jetbrains.annotations.TestOnly;

import java.util.concurrent.atomic.AtomicLong;

/**
 * Runtime owner for symbol-pattern access paths. It refreshes the provider once per
 * cursor open, estimates matching-row selectivity with bounded metadata probes, and
 * then opens exactly one of its covering, bitmap-index, or scan delegates.
 * <p>
 * The factory has two ownership modes, chosen by the code generator:
 * <ul>
 *   <li><b>Wrapped mode</b> - a covering delegate exists. The delegates emit unfiltered
 *       rows and the code generator wraps this factory in an async or serial filter. Both
 *       routes this factory can open supply page frames, so it advertises page-frame
 *       support and the wrapper is the parallel {@code AsyncFilteredRecordCursorFactory}.</li>
 *   <li><b>Self-filtering mode</b> - no covering delegate exists, so the bitmap-index route
 *       is reachable and it cannot supply page frames. A single wrapper would therefore have
 *       to be serial for every open, including the ones that fall back to a full scan. In
 *       this mode the code generator hands in a scan delegate that is already an
 *       {@code AsyncFilteredRecordCursorFactory}, and this factory applies the same filter
 *       instance to the index route itself and becomes the top-level operator. The per-open
 *       estimate then selects between a serial index plan and a parallel scan plan instead of
 *       forcing the whole shape to be serial. Both routes share one filter instance, so a
 *       re-bound bind variable cannot make them disagree.</li>
 * </ul>
 * <p>
 * The conservative policy opens an index delegate only after a bounded estimate proves that the
 * matching index entries cover at most a fixed share of the selected rows. The share is route
 * dependent and fixed at construction from {@code coveringDelegate != null}: 1/50 (2%) for the
 * covering route ({@code MAX_COVERING_ROUTE_ROW_SHARE_DIVISOR}), 1/20 (5%) for the bitmap-index
 * route ({@code MAX_INDEX_ROUTE_ROW_SHARE_DIVISOR}). See the two constants for the measurements
 * behind each number.
 * <p>
 * The estimate counts, not traverses, wherever the index reader can answer from metadata: a POSTING
 * reader answers with {@code countMatchesClamped} and a bitmap reader - the shape a plain
 * {@code SYMBOL INDEX} builds, and therefore the default - with
 * {@link BitmapIndexFwdReader#countMatchesInRange}, which reads the key entry and seeks two blocks.
 * Two reader shapes cannot answer: a POSTING reader on a mixed/unsealed generation, and the
 * {@code IndexFwdNullReader} a partition that predates the column supplies. There the estimate falls
 * back to walking row cursors, charged against the same row budget so it still stops early. A
 * cursor that cannot state its row count up front (every interval-filtered query) supplies the
 * denominator frame by frame instead of being rejected outright. Probe budget exhaustion on either
 * axis selects the scan delegate.
 * <p>
 * The configured threshold {@code max(1, configuredThreshold)} caps planning work on the
 * two INDEPENDENT axes the estimate spends it on: at most that many partition frames, and
 * at most that many key probes within each frame. A single counter spent across both makes
 * the two multiply, and then partition count alone exhausts the budget - on a 50-partition
 * table the default 100 was gone the moment a pattern matched a third symbol, which
 * measured as a 40x regression (0.51 ms on the index route against 20.2 ms on the parallel
 * scan, 20M rows, four shared workers).
 * <p>
 * Splitting one counter into two makes the estimate's total work the PRODUCT of the two caps,
 * not their sum: at most {@code threshold} frames times {@code threshold} key probes, so the
 * default 100 bounds an open at 10 000 metadata count calls instead of the 100 a single
 * cumulative counter allowed. That corner needs no more than 100 partitions and a pattern
 * matching exactly 100 keys, so it is reachable, and it is UNMEASURED - the measured corners
 * are 50 frames x 20 keys and 999 frames x 1 key. An operator raising
 * {@code cairo.sql.symbol.pattern.index.threshold} is therefore raising the planning-work
 * bound quadratically, not linearly.
 * <p>
 * The frame cap doubles as the guard that keeps the index route out of the partition counts
 * where it loses. It is serial and pays a per-partition index-reader open, while the fallback
 * scales with the shared query workers, so its advantage falls away as partitions multiply.
 * Measured on a 20M-row table with one matched key and four workers, the index route ran
 * 0.39 ms against the scan's 9.4 ms up to 100 partitions, 10.1 ms against 12.7 ms at 200,
 * then LOST at 50.5 ms against 23.2 ms at 500 and 136.6 ms against 43.7 ms at 1000 - a
 * crossover between 200 and 500 partitions. The default of 100 keeps a margin below it, the
 * same discipline the row-share divisors above use. Raising
 * {@code cairo.sql.symbol.pattern.index.threshold} therefore buys more than extra probes: it
 * re-admits the index route on tables with more partitions than that, where the measurements
 * say it is the slower plan, and it raises planning cost with it (about 0.02 ms per frame for
 * the first hundred frames and about 0.07 ms per frame beyond, on the same table).
 * <p>
 * What the two caps buy is a planning cost independent of the TOTAL ROW COUNT - not a
 * data-independent one. The per-frame figures above are for a single matched key on a native
 * partition; the real per-open bound is {@code frameCap x (frame open + keyCap x key probe)},
 * and opening an interval frame runs up to two O(log rowsInPartition) timestamp binary searches
 * (a row-group metadata read for a PARQUET partition), so the constant grows sub-linearly with
 * partition width. The property the caps guarantee is that it does not grow with the table.
 */
public class AdaptiveSymbolPatternRecordCursorFactory extends AbstractRecordCursorFactory {
    // @TestOnly observability for the selectivity estimator's traversal fallback. The estimate runs
    // on the caller's thread before the first row, so an index reader that cannot answer the count
    // from metadata makes planning cost scale with the data instead of with the plan. Row counts
    // alone cannot tell the two apart -- only a count of index entries the estimator itself read
    // can. The frame counter answers the same kind of question one level up: a route rejected in
    // O(1) off the partition count and one rejected after walking to the frame cap pick the same
    // delegate, so only a count of frames the estimator pulled can tell them apart.
    // A plain static boolean guards both: the JIT folds the always-false production branch away,
    // and the tests that flip it drive their queries on the calling thread.
    @TestOnly
    public static boolean isEstimatorCounterEnabled = false;
    @TestOnly
    public static final AtomicLong testCoveringInvocations = new AtomicLong();
    @TestOnly
    public static final AtomicLong testEstimatorFramesWalked = new AtomicLong();
    @TestOnly
    public static final AtomicLong testEstimatorIndexEntryReads = new AtomicLong();
    @TestOnly
    public static final AtomicLong testScanInvocations = new AtomicLong();
    // The share of the selected rows the COVERING route may match before the estimate prefers the
    // parallel scan. The covering route needs its own number: it reads a narrow projection out of the
    // posting index, but it produces those page frames on the opening thread, so only the filter above
    // it scales with the shared query workers while the fallback scan scales end to end. Benchmarks on
    // the same 2M-row table (POSTING index with INCLUDE, fully covered projection, four matched keys,
    // real worker pool) put its crossover at ~4% of rows with 4 workers and ~2.5-3% with 8 -- covering
    // is 1.57x faster at 2% and 1.20x faster at 3% on 4 workers, then 1.25x slower at 5% and 1.90x
    // slower at 8%; on 8 workers it is 1.23x faster at 2% but already 1.10x slower at 3% and 2.0x
    // slower at 5%. The crossover therefore falls as workers are added, so 1/50 keeps a margin below
    // it on a well-provisioned server while preserving every measured win.
    private static final int MAX_COVERING_ROUTE_ROW_SHARE_DIVISOR = 50;
    // The share of the selected rows the INDEX route may match before the estimate prefers the
    // parallel scan. Benchmarks on a 2M-row table put the crossover between the two routes at 8-10%
    // of rows for a bare filter -- at 4% the index route is still ~1.8x faster, at 10% it is ~1.3x
    // slower and at 20% ~1.6x slower -- so 1/20 admits the range where the index actually wins and
    // keeps a margin below the crossover.
    private static final int MAX_INDEX_ROUTE_ROW_SHARE_DIVISOR = 20;
    private final IntList columnIndexes;
    private final RecordCursorFactory coveringDelegate;
    private final PartitionFrameCursorFactory dfcFactory;
    private final IntList effectiveKeys;
    private final RecordCursorFactory indexDelegate;
    // Non-null only in self-filtering mode. Owns nothing: the filter belongs to the scan delegate and
    // the wrapped cursor belongs to the index delegate, so _close() must not free it.
    private final FilteredRecordCursor indexRouteFilterCursor;
    private final int indexReaderColumnIndex;
    private final boolean isNegated;
    // Applied on two independent axes - frames walked, and key probes within one frame. See the
    // class javadoc for the measurements behind the frame cap.
    private final int maxEstimateProbes;
    // Which of the two admission thresholds applies. The route the estimate admits is fixed at
    // construction, not per open: a covering delegate always wins the selective branch, and without
    // one the selective branch is always the index delegate.
    private final int maxRowShareDivisor;
    private final PreparedSymbolPatternFilter patternFilter;
    private final RecordCursorFactory scanDelegate;
    private final SymbolTableSourceMapper symbolTableSourceMapper;

    public AdaptiveSymbolPatternRecordCursorFactory(
            @NotNull RecordMetadata metadata,
            @NotNull PartitionFrameCursorFactory dfcFactory,
            @NotNull IntList columnIndexes,
            @NotNull IntList effectiveKeys,
            int indexReaderColumnIndex,
            boolean isNegated,
            int threshold,
            @NotNull PreparedSymbolPatternFilter patternFilter,
            boolean isSelfFiltering,
            @NotNull RecordCursorFactory indexDelegate,
            @Nullable RecordCursorFactory coveringDelegate,
            @NotNull RecordCursorFactory scanDelegate
    ) {
        super(metadata);
        this.dfcFactory = dfcFactory;
        this.columnIndexes = columnIndexes;
        this.effectiveKeys = effectiveKeys;
        this.indexReaderColumnIndex = indexReaderColumnIndex;
        this.isNegated = isNegated;
        this.maxEstimateProbes = Math.max(1, threshold);
        this.patternFilter = patternFilter;
        this.indexDelegate = indexDelegate;
        this.coveringDelegate = coveringDelegate;
        this.maxRowShareDivisor = coveringDelegate != null
                ? MAX_COVERING_ROUTE_ROW_SHARE_DIVISOR
                : MAX_INDEX_ROUTE_ROW_SHARE_DIVISOR;
        this.scanDelegate = scanDelegate;
        this.indexRouteFilterCursor = isSelfFiltering ? new FilteredRecordCursor(patternFilter) : null;
        this.symbolTableSourceMapper = new SymbolTableSourceMapper(columnIndexes);
    }

    @Override
    public PageFrameCursor getPageFrameCursor(SqlExecutionContext executionContext, int order) throws SqlException {
        // Wrapped mode only; supportsPageFrameCursor() reports false for the other one, where the scan
        // delegate is an async filter with no frames to give.
        assert indexRouteFilterCursor == null;
        final boolean isSelective = prepareAndEstimate(executionContext);
        if (isSelective && coveringDelegate != null) {
            testCoveringInvocations.incrementAndGet();
            return coveringDelegate.getPageFrameCursor(executionContext, order);
        }
        testScanInvocations.incrementAndGet();
        if (!isSelective) {
            SymbolPatternIndexRecordCursorFactory.testFallbackInvocations.incrementAndGet();
        }
        return scanDelegate.getPageFrameCursor(executionContext, order);
    }

    @Override
    public RecordCursor getCursor(SqlExecutionContext executionContext) throws SqlException {
        final boolean isSelective = prepareAndEstimate(executionContext);
        if (isSelective) {
            if (coveringDelegate != null) {
                testCoveringInvocations.incrementAndGet();
                return coveringDelegate.getCursor(executionContext);
            }
            final RecordCursor indexCursor = indexDelegate.getCursor(executionContext);
            if (indexRouteFilterCursor == null) {
                return indexCursor;
            }
            // The index route only guarantees the pattern conjunct; the residual still has to run, and
            // in self-filtering mode no wrapper above this factory will run it.
            try {
                indexRouteFilterCursor.of(indexCursor, executionContext);
                return indexRouteFilterCursor;
            } catch (Throwable th) {
                Misc.free(indexCursor);
                throw th;
            }
        }
        testScanInvocations.incrementAndGet();
        SymbolPatternIndexRecordCursorFactory.testFallbackInvocations.incrementAndGet();
        return scanDelegate.getCursor(executionContext);
    }

    @Override
    public int getScanDirection() {
        // Compile-time callers read this before the runtime picks a delegate, so it must hold for every
        // delegate this factory could open: the covering merge, the bitmap index route, and the fallback
        // scan. The covering merge and the fallback scan emit row ids ascending; the index route does so
        // only with its heap row cursor, and reports SCAN_DIRECTION_OTHER when it drains key by key.
        if (indexDelegate.getScanDirection() != SCAN_DIRECTION_FORWARD
                || scanDelegate.getScanDirection() != SCAN_DIRECTION_FORWARD
                || (coveringDelegate != null && coveringDelegate.getScanDirection() != SCAN_DIRECTION_FORWARD)) {
            return SCAN_DIRECTION_OTHER;
        }
        return SCAN_DIRECTION_FORWARD;
    }

    @Override
    public TableToken getTableToken() {
        return dfcFactory.getTableToken();
    }

    @Override
    public boolean recordCursorSupportsRandomAccess() {
        // Same reasoning as getScanDirection(): the answer must hold for every delegate the runtime
        // could open. The covering merge cursor has no random access, so a covered pattern reports
        // false; the bitmap index and fallback scan cursors both position by row id.
        return indexDelegate.recordCursorSupportsRandomAccess()
                && scanDelegate.recordCursorSupportsRandomAccess()
                && (coveringDelegate == null || coveringDelegate.recordCursorSupportsRandomAccess());
    }

    @TestOnly
    public static void resetTestCounters() {
        testCoveringInvocations.set(0);
        testEstimatorFramesWalked.set(0);
        testEstimatorIndexEntryReads.set(0);
        testScanInvocations.set(0);
    }

    @Override
    public boolean supportsPageFrameCursor() {
        // True exactly in wrapped mode. getPageFrameCursor() opens only the covering delegate or the
        // scan delegate, and both supply page frames, so the answer holds for every route it can take.
        // In self-filtering mode the answer must stay false for a second, stronger reason: the scan
        // delegate is then an async-filtered factory, so exposing frames upward would either hand the
        // caller a factory that has no frames or, in an earlier link of the chain, unfiltered rows.
        return coveringDelegate != null;
    }

    @Override
    public void toPlan(PlanSink sink) {
        sink.type("AdaptiveSymbolPattern");
        // Route-dependent: the covering and index routes admit different row shares, and which one
        // applies is fixed at construction. See MAX_COVERING/INDEX_ROUTE_ROW_SHARE_DIVISOR.
        // The noun has to hold for whichever reader answers the estimate: postings on a POSTING reader,
        // index entries on the bitmap reader a plain SYMBOL INDEX builds, walked row cursors when neither
        // can count from metadata. All three count rows, so the plan says "matching rows".
        sink.meta("policy").val(coveringDelegate != null
                ? "matching rows <= 2%, bounded probes"
                : "matching rows <= 5%, bounded probes");
        sink.meta("route").val("one child per open");
        if (indexRouteFilterCursor != null) {
            // Self-filtering mode: name the filter this factory applies to the index child. The scan
            // child is an async filter and prints its own.
            sink.attr("indexRouteFilter").val(patternFilter);
        }
        sink.child(indexDelegate);
        if (coveringDelegate != null) {
            sink.child(coveringDelegate);
        }
        sink.child(scanDelegate);
    }

    @Override
    public void toSink(@NotNull CharSink<?> sink) {
        sink.putAscii("{\"name\":\"AdaptiveSymbolPatternRecordCursorFactory\"}");
    }

    @Override
    public boolean usesIndex() {
        // The factory can select an index at runtime, but callers must not assume every open does.
        // getScanDirection() above answers the mirror image of the same question: a compile-time caller
        // cannot know which delegate an open picks, so that method reports what holds for all of them.
        return false;
    }

    @Override
    protected void _close() {
        // Best-effort: in self-filtering mode the index and scan delegates own compiled filter
        // functions, so a throw from the first close must not strand the rest.
        Throwable failure = Misc.freeBestEffort(null, coveringDelegate);
        failure = Misc.freeBestEffort(failure, indexDelegate);
        failure = Misc.freeBestEffort(failure, scanDelegate);
        failure = Misc.freeBestEffort(failure, dfcFactory);
        CairoException.rethrowCleanupFailure(failure);
    }

    private boolean prepareAndEstimate(SqlExecutionContext executionContext) throws SqlException {
        try (PartitionFrameCursor frameCursor = dfcFactory.getCursor(
                executionContext,
                columnIndexes,
                PartitionFrameCursorFactory.ORDER_ASC
        )) {
            symbolTableSourceMapper.of(frameCursor);
            patternFilter.prepare(symbolTableSourceMapper, executionContext);
            buildEffectiveKeys(symbolTableSourceMapper);
            if (effectiveKeys.size() == 0) {
                return true;
            }

            // A cursor confined to designated-timestamp intervals answers size() with -1 rather than
            // counting rows it has not walked yet (AbstractIntervalPartitionFrameCursor.size()), and
            // that is the shape every time-filtered query takes. Reading -1 as "reject" kept the index
            // route dark on exactly the queries whose row set is already narrowed to where it wins, so
            // the denominator comes from the frames instead - they carry the selected row count and the
            // loop below walks them anyway. calculateSize() would answer it too, but it re-walks every
            // partition in the interval, a second pass this loop does not need.
            final long totalRows = frameCursor.size();
            final boolean isTotalRowsKnown = totalRows >= 0;
            // Recomputed per frame while the total is unknown. The running denominator only ever
            // under-estimates the final one, so the admission test below can reject a run of matches
            // that later frames would have diluted, but it can never admit a route the final ratio
            // would have rejected.
            long maxIndexRows = isTotalRowsKnown ? Math.max(1, totalRows / maxRowShareDivisor) : 1;
            final TableReader tableReader = frameCursor.getTableReader();
            // The frame cap in O(1), for the cursors whose partition count bounds their frame count.
            // size() >= 0 is exactly that signal: a cursor that can state its row count off reader
            // metadata is walking whole partitions, and it yields one frame per NON-EMPTY partition
            // (FullFwdPartitionFrameCursor.next()), so the reader's partition count bounds it. Walking
            // to find that out reaches the same verdict -- the loop below rejects at frame
            // maxEstimateProbes + 1 -- so this skips the walk without moving the route. The one
            // divergence is a table with enough empty partitions to bring the frame count back under
            // the cap, and that can only arise on a table whose partition count is already past the
            // cap, where the fallback is the answer the cap exists to give.
            // An interval cursor is deliberately excluded: its frames are the partitions IN RANGE, and
            // the total partition count does not bound those usefully -- a one-day filter on a
            // three-year table walks a single frame, which is the shape the index route wins on.
            if (isTotalRowsKnown && tableReader.getPartitionCount() > maxEstimateProbes) {
                return false;
            }
            long matchedRows = 0;
            long selectedRows = 0;
            int frames = 0;
            PartitionFrame frame;
            while ((frame = frameCursor.next()) != null) {
                if (isEstimatorCounterEnabled) {
                    testEstimatorFramesWalked.incrementAndGet();
                }
                if (++frames > maxEstimateProbes) {
                    return false;
                }
                final IndexReader reader = tableReader.getIndexReader(
                        frame.getPartitionIndex(),
                        indexReaderColumnIndex,
                        IndexReader.DIR_FORWARD
                );
                final long rowLo = frame.getRowLo();
                final long rowHiExclusive = frame.getRowHi();
                final long callerHiInclusive = rowHiExclusive - 1;
                if (!isTotalRowsKnown) {
                    selectedRows += rowHiExclusive - rowLo;
                    maxIndexRows = Math.max(1, selectedRows / maxRowShareDivisor);
                }
                // Reset per frame: the key cap bounds what one frame may cost, and the frame cap above
                // bounds how many frames may cost that. Spending one counter across both multiplies the
                // two axes together, which is what let partition count alone exhaust the budget.
                int keyProbes = 0;
                for (int i = 0, n = effectiveKeys.size(); i < n; i++) {
                    if (++keyProbes > maxEstimateProbes) {
                        return false;
                    }
                    final int indexKey = TableUtils.toIndexKey(effectiveKeys.getQuick(i));
                    long count = Numbers.LONG_NULL;
                    if (reader instanceof AbstractPostingIndexReader posting) {
                        final long entryMax = posting.getEntryMaxValue();
                        final long clampedMax = entryMax >= 0
                                ? Math.min(callerHiInclusive, entryMax)
                                : callerHiInclusive;
                        count = posting.countMatchesClamped(indexKey, rowLo, callerHiInclusive, clampedMax);
                    } else if (reader instanceof BitmapIndexFwdReader bitmap) {
                        // SYMBOL INDEX without an explicit type builds a bitmap index, so this is the
                        // default shape. countMatchesInRange() answers the same question from the key
                        // entry and two block seeks; without it every open of a broad pattern walked
                        // index entries up to the whole maxIndexRows budget before rejecting the route.
                        count = bitmap.countMatchesInRange(indexKey, rowLo, callerHiInclusive);
                    }
                    if (count == Numbers.LONG_NULL) {
                        // A mixed/unsealed generation cannot supply an exact metadata count. Traverse only
                        // until the remaining budget is exceeded; this preserves a bounded estimate.
                        count = 0;
                        try (RowCursor rowCursor = reader.getCursor(indexKey, rowLo, callerHiInclusive)) {
                            while (rowCursor.hasNext() && matchedRows + count <= maxIndexRows) {
                                rowCursor.next();
                                count++;
                            }
                        }
                        if (isEstimatorCounterEnabled) {
                            testEstimatorIndexEntryReads.addAndGet(count);
                        }
                    }
                    matchedRows += count;
                    if (matchedRows > maxIndexRows) {
                        return false;
                    }
                }
            }
            return true;
        }
    }

    private void buildEffectiveKeys(SymbolTableSource symbolTableSource) {
        final IntList matched = patternFilter.getMatchedSymbolKeys();
        effectiveKeys.clear();
        if (!isNegated) {
            effectiveKeys.addAll(matched);
            return;
        }

        final StaticSymbolTable symbolTable = (StaticSymbolTable) symbolTableSource.getSymbolTable(
                patternFilter.getSymbolColumnIndex()
        );
        int matchedIndex = 0;
        final int matchedSize = matched.size();
        for (int key = 0, symbolCount = symbolTable.getSymbolCount(); key < symbolCount; key++) {
            if (matchedIndex < matchedSize && matched.getQuick(matchedIndex) == key) {
                matchedIndex++;
            } else {
                effectiveKeys.add(key);
            }
        }
        if (symbolTable.containsNullValue()) {
            effectiveKeys.add(SymbolTable.VALUE_IS_NULL);
        }
    }

    public static final class NonOwningPartitionFrameCursorFactory implements PartitionFrameCursorFactory {
        private final PartitionFrameCursorFactory delegate;

        public NonOwningPartitionFrameCursorFactory(PartitionFrameCursorFactory delegate) {
            this.delegate = delegate;
        }

        @Override
        public void close() {
        }

        @Override
        public PartitionFrameCursor getCursor(SqlExecutionContext executionContext, IntList columnIndexes, int order) throws SqlException {
            return delegate.getCursor(executionContext, columnIndexes, order);
        }

        @Override
        public RecordMetadata getMetadata() {
            return delegate.getMetadata();
        }

        @Override
        public int getOrder() {
            return delegate.getOrder();
        }

        @Override
        public ObjList<PushdownFilterExtractor.PushdownFilterCondition> getPushdownFilterConditions() {
            return delegate.getPushdownFilterConditions();
        }

        @Override
        public TableToken getTableToken() {
            return delegate.getTableToken();
        }

        @Override
        public boolean hasParquetFormatPartitions(SqlExecutionContext executionContext) {
            return delegate.hasParquetFormatPartitions(executionContext);
        }

        @Override
        public void setPushdownFilterCondition(ObjList<PushdownFilterExtractor.PushdownFilterCondition> conditions) {
            delegate.setPushdownFilterCondition(conditions);
        }

        @Override
        public boolean supportsTableRowId(TableToken tableToken) {
            return delegate.supportsTableRowId(tableToken);
        }

        @Override
        public void toPlan(PlanSink sink) {
            delegate.toPlan(sink);
        }
    }

    public static final class PreparedSymbolPatternFilter extends BooleanFunction {
        private final boolean isNegated;
        private final SymbolKeySetProvider provider;
        private final Function providerFunction;
        private final Function residualFilter;
        private final int symbolColumnIndex;
        // Set by prepare(). getBool() asserts it, because isThreadSafe() rests on it - see that override.
        private boolean hasPreparedKeySet;

        public PreparedSymbolPatternFilter(
                @NotNull Function providerFunction,
                @Nullable Function residualFilter,
                boolean isNegated,
                int symbolColumnIndex
        ) {
            this.providerFunction = providerFunction;
            this.provider = (SymbolKeySetProvider) providerFunction;
            this.residualFilter = residualFilter;
            this.isNegated = isNegated;
            this.symbolColumnIndex = symbolColumnIndex;
        }

        @Override
        public void close() {
            Misc.free(providerFunction);
            Misc.free(residualFilter);
        }

        @Override
        public void cursorClosed() {
            providerFunction.cursorClosed();
            if (residualFilter != null) {
                residualFilter.cursorClosed();
            }
        }

        @Override
        public boolean getBool(Record rec) {
            // Enforces the precondition isThreadSafe() reports true on. Workers of an async filter built
            // with perWorkerFilters == null share this one instance, so the provider must already be past
            // its lazy-init branch by the time any of them gets here.
            assert hasPreparedKeySet : "prepare() must run before getBool(); see isThreadSafe()";
            final boolean isMatch = providerFunction.getBool(rec);
            return (isNegated != isMatch) && (residualFilter == null || residualFilter.getBool(rec));
        }

        public IntList getMatchedSymbolKeys() {
            return provider.getMatchedSymbolKeys();
        }

        public int getSymbolColumnIndex() {
            return symbolColumnIndex;
        }

        @Override
        public void init(SymbolTableSource symbolTableSource, SqlExecutionContext executionContext) throws SqlException {
            if (residualFilter != null) {
                residualFilter.init(symbolTableSource, executionContext);
            }
        }

        @Override
        public boolean isThreadSafe() {
            // Deliberately ignores providerFunction.isThreadSafe(), which is false for every LIKE/ILIKE and
            // regex provider. The answer here is true on one precondition: prepare() has already run on the
            // query thread. Once it has, provider getBool() only reads the prepared key list and the
            // record's integer symbol key. Before it has, MatchStaticSymbolTableConstPatternFunction and
            // MatchStaticSymbolTableRuntimeConstPatternFunction still take a lazy-init branch in getBool()
            // that rebuilds their key list through a shared Matcher, which several workers sharing this one
            // instance would corrupt. prepareAndEstimate() calls prepare() before every open of every route,
            // and getBool() asserts hasPreparedKeySet, so a future caller that skips prepare() fails loudly
            // instead of racing.
            return residualFilter == null || residualFilter.isThreadSafe();
        }

        public void prepare(SymbolTableSource symbolTableSource, SqlExecutionContext executionContext) throws SqlException {
            // init() rebuilds the provider's key set and retires its lazy-init branch, which is what makes
            // the shared instance re-entrant. Record that so getBool() can assert it.
            providerFunction.init(symbolTableSource, executionContext);
            hasPreparedKeySet = true;
        }

        @Override
        public void toPlan(PlanSink sink) {
            if (isNegated) {
                sink.val("not(");
            }
            sink.val(providerFunction);
            if (isNegated) {
                sink.val(')');
            }
            if (residualFilter != null) {
                sink.val(" and ").val(residualFilter);
            }
        }

        @Override
        public void toTop() {
            providerFunction.toTop();
            if (residualFilter != null) {
                residualFilter.toTop();
            }
        }
    }

    private static final class SymbolTableSourceMapper implements SymbolTableSource {
        private final IntList columnIndexes;
        private PartitionFrameCursor frameCursor;

        private SymbolTableSourceMapper(IntList columnIndexes) {
            this.columnIndexes = columnIndexes;
        }

        @Override
        public SymbolTable getSymbolTable(int columnIndex) {
            return frameCursor.getSymbolTable(columnIndexes.getQuick(columnIndex));
        }

        @Override
        public SymbolTable newSymbolTable(int columnIndex) {
            return frameCursor.newSymbolTable(columnIndexes.getQuick(columnIndex));
        }

        private void of(PartitionFrameCursor frameCursor) {
            this.frameCursor = frameCursor;
        }
    }
}
