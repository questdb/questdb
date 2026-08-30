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
import io.questdb.cairo.idx.IndexFwdNullReader;
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
import io.questdb.cairo.vm.api.MemoryCARW;
import io.questdb.griffin.PlanSink;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.engine.functions.BooleanFunction;
import io.questdb.griffin.engine.functions.regex.SymbolKeySetProvider;
import io.questdb.griffin.model.ExpressionNode;
import io.questdb.jit.CompiledFilter;
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
 * Self-filtering mode also advertises FILTER STEALING, because being the top-level operator with
 * no page frames is exactly what a parallel parent cannot work with. A parent that needs page
 * frames - parallel GROUP BY, SAMPLE BY, async top-K - admits a child through either
 * {@code supportsPageFrameCursor()} or {@code supportsFilterStealing()}, and self-filtering mode
 * answered false to both, so the parent silently fell back to its serial operator EVEN WHEN the
 * per-open estimate would have picked the parallel scan. Measured with four workers over 10M rows
 * and a pattern matching 10% of them, that cost 28.982 ms against 8.644 ms for a keyed GROUP BY and
 * 34.802 ms against 8.962 ms for a SAMPLE BY. See {@link #supportsFilterStealing()} and
 * {@link #halfClose()} for what a steal costs in return.
 * <p>
 * The conservative policy opens an index delegate only after a bounded estimate proves that the
 * matching index entries cover at most a fixed share of the selected rows. The share is route
 * dependent and fixed at construction from {@code coveringDelegate != null}: 1/50 (2%) for the
 * covering route ({@code MAX_COVERING_ROUTE_ROW_SHARE_DIVISOR}), 1/20 (5%) for the bitmap-index
 * route ({@code MAX_INDEX_ROUTE_ROW_SHARE_DIVISOR}). See the two constants for the measurements
 * behind each number.
 * <p>
 * The estimate counts, not traverses, wherever the index reader can answer from metadata. A POSTING
 * reader provides exact dense/sparse FLAT and DELTA range counts and fixed-stride ranked-EF counts.
 * Existing unranked EF stays readable, but a range that cannot be proven un-clipped from its legacy
 * prefix rejects adaptive admission directly rather than traversing its high vector. A bitmap reader -
 * the shape a plain {@code SYMBOL INDEX} builds, and therefore the default - uses
 * {@link BitmapIndexFwdReader#countMatchesInRange}, which reads the key entry and seeks two blocks.
 * {@code IndexFwdNullReader} computes its contiguous implicit-NULL range directly. Any future reader
 * shape that cannot answer still falls back to walking row cursors, but a single traversal-probe
 * budget spans the whole estimate and selects the scan delegate before another entry could exceed
 * it. The bitmap count is metadata rather than a walk, yet it is not free: its two block seeks hop a
 * linked chain, so the same threshold caps the hops one count may spend, and a range sitting deeper
 * into the chain than that selects the scan delegate instead of being counted exactly. A cursor that
 * cannot state its row count up front (every interval-filtered query) supplies the denominator frame
 * by frame instead of being rejected outright. Unknown-reader probe exhaustion
 * selects the scan delegate; a safe metadata upper bound may admit the index only when that upper
 * bound itself fits the selectivity limit.
 * <p>
 * Every route reads the SAME table-reader transaction the estimate ran on. The estimate opens one
 * partition-frame cursor, builds {@code effectiveKeys} and the prepared filter's key set from that
 * reader's symbol dictionary, and then hands the cursor itself to the delegate it selects, through
 * the shared {@link NonOwningPartitionFrameCursorFactory} all three delegates open against. Without
 * that hand-off the delegate acquired a SECOND reader, which the pool positions at the latest
 * transaction: a commit landing between the two acquisitions then produced a result belonging to
 * neither snapshot - rows appeared under symbol keys the estimate had already seen, while every row
 * under a symbol the same commit introduced was silently dropped, because it has no key in the
 * list the estimate built. The hand-off also removes one reader acquisition per open.
 * <p>
 * The configured threshold {@code max(1, configuredThreshold)} caps planning work on the
 * two INDEPENDENT metadata axes the estimate spends it on: at most that many partition frames,
 * and at most that many key probes within each frame. It also caps the value blocks ONE bitmap
 * count may hop across, which keeps a probe over a hot key proportional to the frame the query
 * selects rather than to the posting list the key holds across the whole partition: a narrow
 * interval in the middle of a ten-million-row chain used to cost every one of its 39,063 blocks on
 * each cursor open, measured at 11.3 ms against 7.5 ms for the same query on the merge base. It
 * separately caps the TOTAL index entries that traversal fallbacks may read across all keys and
 * frames in one estimate. A single counter
 * spent across the metadata axes makes the two multiply, and then partition count alone exhausts
 * the budget - on a 50-partition table the default 100 was gone the moment a pattern matched a
 * third symbol, which measured as a 40x regression (0.51 ms on the index route against 20.2 ms
 * on the parallel scan, 20M rows, four shared workers).
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
 * The caps keep index-entry traversal independent of the TOTAL ROW COUNT, but they do not make
 * planning data-independent. The per-frame figures above are for a single matched key on a native
 * partition; the real per-open bound is
 * {@code frameCap x (frame open + keyCap x metadata probe) + traversalCap}, and opening an interval
 * frame runs up to two O(log rowsInPartition) timestamp binary searches (a row-group metadata read
 * for a PARQUET partition), so that part grows sub-linearly with partition width.
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
    // The one partition-frame factory every delegate opens against, so the cursor the estimate ran on
    // can be handed to the delegate instead of the delegate acquiring a second reader.
    private final NonOwningPartitionFrameCursorFactory sharedFrameFactory;
    private final SymbolTableSourceMapper symbolTableSourceMapper;
    // Set by halfClose(). Marks that a parent took the scan delegate's base and the prepared filter,
    // so _close() must free neither them nor anything halfClose() already freed.
    private boolean isFilterStolen;

    public AdaptiveSymbolPatternRecordCursorFactory(
            @NotNull RecordMetadata metadata,
            @NotNull PartitionFrameCursorFactory dfcFactory,
            @NotNull NonOwningPartitionFrameCursorFactory sharedFrameFactory,
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
        this.sharedFrameFactory = sharedFrameFactory;
        sharedFrameFactory.of(this);
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
        // The delegate this open selects requests the very same order, so the estimate opens its
        // cursor with it and the hand-off below matches.
        final boolean isSelective = prepareAndEstimate(executionContext, order);
        try {
            if (isSelective && coveringDelegate != null) {
                if (SymbolPatternIndexRecordCursorFactory.isRouteCounterEnabled) {
                    testCoveringInvocations.incrementAndGet();
                }
                return coveringDelegate.getPageFrameCursor(executionContext, order);
            }
            if (SymbolPatternIndexRecordCursorFactory.isRouteCounterEnabled) {
                testScanInvocations.incrementAndGet();
                if (!isSelective) {
                    SymbolPatternIndexRecordCursorFactory.testFallbackInvocations.incrementAndGet();
                }
            }
            return scanDelegate.getPageFrameCursor(executionContext, order);
        } finally {
            // A no-op once the delegate has taken the pinned cursor. It releases the reader when the
            // delegate never asked for a cursor, when it asked for the opposite scan direction, or
            // when the open threw before reaching the delegate.
            sharedFrameFactory.releasePinnedCursor();
        }
    }

    @Override
    public RecordCursor getCursor(SqlExecutionContext executionContext) throws SqlException {
        // Ascending: the covering delegate opens ORDER_ASC, and both page-frame delegates open
        // ORDER_ANY, which resolves to the partition factory's own base order.
        final boolean isSelective = prepareAndEstimate(executionContext, PartitionFrameCursorFactory.ORDER_ASC);
        try {
            if (isSelective) {
                if (coveringDelegate != null) {
                    if (SymbolPatternIndexRecordCursorFactory.isRouteCounterEnabled) {
                        testCoveringInvocations.incrementAndGet();
                    }
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
            if (SymbolPatternIndexRecordCursorFactory.isRouteCounterEnabled) {
                testScanInvocations.incrementAndGet();
                SymbolPatternIndexRecordCursorFactory.testFallbackInvocations.incrementAndGet();
            }
            return scanDelegate.getCursor(executionContext);
        } finally {
            // See getPageFrameCursor().
            sharedFrameFactory.releasePinnedCursor();
        }
    }

    /**
     * The scan delegate's own base - a bare page-frame factory - in self-filtering mode, and null in
     * wrapped mode, which does not offer filter stealing. See {@link #supportsFilterStealing()}.
     */
    @Override
    public RecordCursorFactory getBaseFactory() {
        return indexRouteFilterCursor != null ? scanDelegate.getBaseFactory() : null;
    }

    // The next three answer null today, whichever mode this factory is in: in wrapped mode the guard
    // returns null, and in self-filtering mode the scan delegate is always the
    // AsyncFilteredRecordCursorFactory built by tryGenerateSymbolPatternIndex, which overrides none of
    // them and so falls through to the interface defaults. They stay because a stealing parent reads
    // this whole group together with getFilter() - see SqlCodeGenerator's parallel-aggregate steal -
    // so it is a contract unit that must keep delegating if the scan delegate ever stops being async.

    @Override
    public @Nullable ObjList<Function> getBindVarFunctions() {
        return indexRouteFilterCursor != null ? scanDelegate.getBindVarFunctions() : null;
    }

    @Override
    public @Nullable MemoryCARW getBindVarMemory() {
        return indexRouteFilterCursor != null ? scanDelegate.getBindVarMemory() : null;
    }

    @Override
    public @Nullable CompiledFilter getCompiledFilter() {
        return indexRouteFilterCursor != null ? scanDelegate.getCompiledFilter() : null;
    }

    @Override
    public @Nullable Function getFilter() {
        return indexRouteFilterCursor != null ? scanDelegate.getFilter() : null;
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
    public @Nullable ExpressionNode getStealFilterExpr() {
        return indexRouteFilterCursor != null ? scanDelegate.getStealFilterExpr() : null;
    }

    @Override
    public TableToken getTableToken() {
        return dfcFactory.getTableToken();
    }

    /**
     * Dismantles this factory after a parent has stolen the filter. The parent keeps exactly two
     * things: {@link #getBaseFactory()}, the scan delegate's bare page-frame factory, and
     * {@link #getFilter()}, the shared prepared filter. Everything else this factory owns is freed
     * here, because the code generator drops a stolen-from factory without ever calling
     * {@code close()} on it - {@code halfClose()} IS the terminal cleanup.
     * <p>
     * Ownership of {@code dfcFactory} moves to {@link #sharedFrameFactory}, which the surviving base
     * still holds and still closes. The base keeps opening through that wrapper, so it keeps calling
     * back into {@link #prepareKeysFor}: that method rebuilds the stolen filter's matched-key set
     * against the executing reader, since {@code PreparedSymbolPatternFilter.init()} deliberately
     * leaves the provider alone, but skips the effective-key list whose only consumers halfClose()
     * frees. The half-closed factory therefore stays reachable as the wrapper's owner; only its
     * delegates are gone.
     */
    @Override
    public void halfClose() {
        assert indexRouteFilterCursor != null : "only self-filtering mode advertises filter stealing";
        isFilterStolen = true;
        // The index route is unreachable from here on: the parent aggregates over page frames the
        // bitmap index cannot supply. That is what a steal costs -- a pattern selective enough for
        // the per-open estimate to admit the index now runs the parallel scan instead.
        Throwable failure = Misc.freeBestEffort(null, sharedFrameFactory.takePinnedCursor());
        // Order matters: closing the index delegate closes the shared wrapper too, so the ownership
        // transfer below has to come after it, or the wrapper would free dfcFactory here and the
        // surviving base would then hold a closed one.
        failure = Misc.freeBestEffort(failure, indexDelegate);
        try {
            // Frees the async machinery but neither the base nor the filter, which is precisely the
            // pair the parent took.
            scanDelegate.halfClose();
        } catch (Throwable th) {
            if (failure == null) {
                failure = th;
            } else if (failure != th) {
                failure.addSuppressed(th);
            }
        }
        sharedFrameFactory.assumeDelegateOwnership();
        CairoException.rethrowCleanupFailure(failure);
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
    public boolean supportsFilterStealing() {
        // Self-filtering mode only, where this factory is the top-level operator and supplies no page
        // frames. Wrapped mode needs no answer here: it already supplies frames, so its parent admits
        // it directly and steals from the async filter above it instead.
        //
        // The claim a true makes is that this factory is a filter over getBaseFactory(). That holds
        // for the scan route verbatim, and it is the route the estimate picks for every pattern the
        // index route would lose on. It does NOT hold for the index route, which the steal discards
        // outright -- see halfClose(). The alternative is what shipped before: no parent can
        // parallelise over a symbol-pattern filter at all, on any pattern.
        return indexRouteFilterCursor != null && scanDelegate.supportsFilterStealing();
    }

    @Override
    public boolean supportsPageFrameCursor() {
        // True exactly in wrapped mode. getPageFrameCursor() opens only the covering delegate or the
        // scan delegate, and both supply page frames, so the answer holds for every route it can take.
        // In self-filtering mode the answer must stay false for a second, stronger reason: the scan
        // delegate is then an async-filtered factory, so exposing frames upward would either hand the
        // caller a factory that has no frames or, in an earlier link of the chain, unfiltered rows.
        // A parallel parent reaches the scan plan through supportsFilterStealing() instead.
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
        if (isFilterStolen) {
            // halfClose() already freed everything this factory still owned and handed the scan
            // delegate's base and the shared filter to the parent that stole them. Freeing again here
            // would free the parent's own children.
            return;
        }
        // Best-effort: in self-filtering mode the index and scan delegates own compiled filter
        // functions, so a throw from the first close must not strand the rest. The pin release joins
        // the chain rather than preceding it, so a cleanup failure there cannot strand the delegates.
        // It is defensive only: both getCursor() and getPageFrameCursor() release the pin in a finally
        // block, so no traced path reaches close() with one held.
        Throwable failure = Misc.freeBestEffort(null, sharedFrameFactory.takePinnedCursor());
        failure = Misc.freeBestEffort(failure, coveringDelegate);
        failure = Misc.freeBestEffort(failure, indexDelegate);
        failure = Misc.freeBestEffort(failure, scanDelegate);
        failure = Misc.freeBestEffort(failure, dfcFactory);
        CairoException.rethrowCleanupFailure(failure);
    }

    /**
     * Opens one partition-frame cursor, builds the key set from its reader, costs the routes against
     * it, and then PINS it for the delegate this open selects. The delegate takes that cursor - and
     * therefore that reader transaction - out of {@link #sharedFrameFactory} instead of acquiring a
     * second reader the pool would position at the latest transaction. The caller releases the pin in
     * a finally block, which is a no-op once a delegate has taken it.
     */
    private boolean prepareAndEstimate(SqlExecutionContext executionContext, int order) throws SqlException {
        final PartitionFrameCursor frameCursor = dfcFactory.getCursor(executionContext, columnIndexes, order);
        final boolean isSelective;
        try {
            isSelective = prepareKeysFor(frameCursor, executionContext, true) && estimate(frameCursor);
        } catch (Throwable th) {
            Misc.free(frameCursor);
            throw th;
        }
        sharedFrameFactory.pinCursor(frameCursor, columnIndexes, order);
        return isSelective;
    }

    /**
     * Rebuilds the prepared filter's matched-key set and, while an index consumer remains,
     * {@code effectiveKeys} from the symbol dictionary of {@code frameCursor}'s reader. Both key
     * lists are only valid against the transaction they were read from: symbol keys are append-only,
     * so a list built on an older transaction cannot point at the wrong symbol, but it has no key at
     * all for a symbol a later commit introduced, and every row carrying that symbol would then be
     * dropped.
     *
     * @return false when an estimate can reject a negated complement from its cardinality without
     * materializing it
     */
    private boolean prepareKeysFor(
            PartitionFrameCursor frameCursor,
            SqlExecutionContext executionContext,
            boolean isProbeCapApplied
    ) throws SqlException {
        symbolTableSourceMapper.of(frameCursor);
        patternFilter.prepare(symbolTableSourceMapper, executionContext);
        if (isFilterStolen) {
            return false;
        }
        return buildEffectiveKeys(symbolTableSourceMapper, isProbeCapApplied);
    }

    private boolean estimate(PartitionFrameCursor frameCursor) {
        if (effectiveKeys.size() == 0) {
            return true;
        }
        // Apply the per-frame key cap even when an interval selects no frames. Otherwise the empty
        // loop admits the index delegate, which builds and retains one child factory per key.
        if (effectiveKeys.size() > maxEstimateProbes) {
            return false;
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
        int traversedIndexEntries = 0;
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
                    // SymbolPatternIndexRecordCursorFactory is a bitmap-row-cursor delegate. POSTING
                    // readers are executable only through the covering delegate; admitting one in
                    // self-filtering mode would hand absolute posting row ids to bitmap frame logic.
                    if (coveringDelegate == null) {
                        return false;
                    }
                    final long entryMax = posting.getEntryMaxValue();
                    final long clampedMax = entryMax >= 0
                            ? Math.min(callerHiInclusive, entryMax)
                            : callerHiInclusive;
                    count = posting.estimateMatchesClamped(indexKey, rowLo, callerHiInclusive, clampedMax);
                } else if (reader instanceof BitmapIndexFwdReader bitmap) {
                    // SYMBOL INDEX without an explicit type builds a bitmap index, so this is the
                    // default shape. countMatchesInRange() answers the same question from the key
                    // entry and two block seeks; without it every open of a broad pattern walked
                    // index entries up to the whole maxIndexRows budget before rejecting the route.
                    // Those seeks hop a linked chain, so a frame narrowed to the middle of a hot
                    // key's posting list makes them cross the whole chain -- cost that grows with the
                    // partition while the frame stays put. The probe budget caps the hops, and a key
                    // that runs it out rejects the route below rather than paying the crossing.
                    count = bitmap.countMatchesInRange(indexKey, rowLo, callerHiInclusive, maxEstimateProbes);
                } else if (reader instanceof IndexFwdNullReader nullReader) {
                    count = nullReader.estimateMatches(indexKey, rowLo, callerHiInclusive);
                }
                if (count == AbstractPostingIndexReader.ESTIMATE_REJECT) {
                    // Two readers ask for the same treatment. A genuinely clipped legacy EF blob has
                    // no bounded rank metadata, and a bitmap range that sits deeper into the posting
                    // chain than the probe budget reaches has no bounded exact count either. Reject
                    // both without disguising them as a generic unknown that would spend the fallback
                    // cursor budget -- and, for the bitmap, position a cursor over the same chain.
                    return false;
                }
                if (count == Numbers.LONG_NULL) {
                    // A mixed/unsealed generation cannot supply an exact metadata count. One traversal
                    // budget spans the whole estimate, so neither row share nor table size can make the
                    // fallback read more than maxEstimateProbes index entries before selecting the scan.
                    count = 0;
                    try (RowCursor rowCursor = reader.getCursor(indexKey, rowLo, callerHiInclusive)) {
                        while (rowCursor.hasNext()) {
                            if (traversedIndexEntries >= maxEstimateProbes) {
                                return false;
                            }
                            rowCursor.next();
                            count++;
                            traversedIndexEntries++;
                            if (isEstimatorCounterEnabled) {
                                testEstimatorIndexEntryReads.incrementAndGet();
                            }
                            if (matchedRows + count > maxIndexRows) {
                                return false;
                            }
                        }
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

    private boolean buildEffectiveKeys(SymbolTableSource symbolTableSource, boolean isProbeCapApplied) {
        final IntList matched = patternFilter.getMatchedSymbolKeys();
        effectiveKeys.clear();
        if (!isNegated) {
            effectiveKeys.addAll(matched);
            return true;
        }

        final StaticSymbolTable symbolTable = (StaticSymbolTable) symbolTableSource.getSymbolTable(
                patternFilter.getSymbolColumnIndex()
        );
        final boolean hasNull = symbolTable.containsNullValue();
        final int matchedSize = matched.size();
        final long effectiveKeyCount = (long) symbolTable.getSymbolCount() - matchedSize + (hasNull ? 1 : 0);
        if (isProbeCapApplied && effectiveKeyCount > maxEstimateProbes) {
            return false;
        }

        int matchedIndex = 0;
        for (int key = 0, symbolCount = symbolTable.getSymbolCount(); key < symbolCount; key++) {
            if (matchedIndex < matchedSize && matched.getQuick(matchedIndex) == key) {
                matchedIndex++;
            } else {
                effectiveKeys.add(key);
            }
        }
        if (hasNull) {
            effectiveKeys.add(SymbolTable.VALUE_IS_NULL);
        }
        return true;
    }

    /**
     * The single partition-frame factory every delegate of one
     * {@link AdaptiveSymbolPatternRecordCursorFactory} opens against. It closes nothing - the adaptive
     * owner closes the real factory exactly once - and it carries the reader hand-off between the
     * per-open selectivity estimate and the delegate that estimate selects.
     * <p>
     * The owner pins the cursor its estimate ran on; the first delegate that asks for a cursor in the
     * same scan direction, over the same column set, gets that cursor back rather than a second reader
     * the pool would position at the latest transaction. When the delegate resolves to the opposite
     * direction the pin is released and a fresh cursor is opened, with the owner rebuilding both key
     * lists against it, so that route reads one coherent transaction too. It just pays the second
     * acquisition. The shape that resolves the opposite way is a DESCENDING BASE ORDER: the owner's
     * {@code getCursor()} pins ORDER_ASC unconditionally, while the index and scan delegates ask
     * ORDER_ANY, which {@link #resolveDirection} resolves to the base order. The page-frame path never
     * mismatches - it pins the very order it is asked for, and every delegate then asks for that same
     * order, the async filter's negative-limit backward path included.
     * <p>
     * The pin is per-open mutable state and carries the same single-open constraint the owner's
     * {@code effectiveKeys} list already carries: one cursor open at a time per compiled factory.
     * <p>
     * Non-owning is the DEFAULT, not an invariant: the owner closes the real factory exactly once, so
     * this wrapper closes nothing. {@link #assumeDelegateOwnership()} flips that when a parent steals
     * the owner's filter, because the owner is then dismantled while the base factory holding this
     * wrapper lives on.
     */
    public static final class NonOwningPartitionFrameCursorFactory implements PartitionFrameCursorFactory {
        private PartitionFrameCursorFactory delegate;
        private boolean isDelegateOwned;
        private AdaptiveSymbolPatternRecordCursorFactory owner;
        private IntList pinnedColumnIndexes;
        private PartitionFrameCursor pinnedCursor;
        private int pinnedDirection;

        public NonOwningPartitionFrameCursorFactory(PartitionFrameCursorFactory delegate) {
            this.delegate = delegate;
        }

        @Override
        public void close() {
            if (isDelegateOwned) {
                delegate = Misc.free(delegate);
            }
        }

        @Override
        public PartitionFrameCursor getCursor(SqlExecutionContext executionContext, IntList columnIndexes, int order) throws SqlException {
            final PartitionFrameCursor pinned = pinnedCursor;
            if (pinned != null) {
                final boolean isReusable = pinnedColumnIndexes == columnIndexes
                        && pinnedDirection == resolveDirection(order);
                pinnedCursor = null;
                pinnedColumnIndexes = null;
                if (isReusable) {
                    // Same reader, same transaction the owner built its key lists from. The estimate
                    // walked this cursor to the end, so rewind it.
                    pinned.toTop();
                    return pinned;
                }
                Misc.free(pinned);
            }
            final PartitionFrameCursor cursor = delegate.getCursor(executionContext, columnIndexes, order);
            try {
                // A fresh reader may already carry a newer transaction than the estimate costed, so the
                // key lists have to be rebuilt from THIS reader's symbol dictionary before the delegate
                // binds them.
                owner.prepareKeysFor(cursor, executionContext, false);
            } catch (Throwable th) {
                Misc.free(cursor);
                throw th;
            }
            return cursor;
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

        /**
         * Takes over closing the real partition-frame factory. The owner calls this from
         * {@link AdaptiveSymbolPatternRecordCursorFactory#halfClose()}, where it stops being the
         * closer, and the base factory that survives the steal becomes the only remaining holder of
         * this wrapper - and therefore the one that closes it.
         */
        void assumeDelegateOwnership() {
            isDelegateOwned = true;
        }

        void of(AdaptiveSymbolPatternRecordCursorFactory owner) {
            this.owner = owner;
        }

        void pinCursor(PartitionFrameCursor cursor, IntList columnIndexes, int order) {
            assert pinnedCursor == null : "an unreleased pin would strand its reader";
            pinnedCursor = cursor;
            pinnedColumnIndexes = columnIndexes;
            pinnedDirection = resolveDirection(order);
        }

        void releasePinnedCursor() {
            Misc.free(takePinnedCursor());
        }

        /**
         * Detaches the pinned cursor and hands it to the caller, which then owns closing it.
         */
        PartitionFrameCursor takePinnedCursor() {
            final PartitionFrameCursor pinned = pinnedCursor;
            pinnedCursor = null;
            pinnedColumnIndexes = null;
            return pinned;
        }

        /**
         * The scan direction {@code order} selects, resolving ORDER_ANY the way every
         * {@link PartitionFrameCursorFactory} implementation resolves it: to the factory's own base
         * order, ascending unless that base order is descending.
         */
        private int resolveDirection(int order) {
            if (order == ORDER_ASC || order == ORDER_DESC) {
                return order;
            }
            return delegate.getOrder() == ORDER_DESC ? ORDER_DESC : ORDER_ASC;
        }
    }

    public static final class PreparedSymbolPatternFilter extends BooleanFunction {
        private final boolean isNegated;
        private final SymbolKeySetProvider provider;
        private final Function providerFunction;
        private final ExpressionNode providerExpression;
        private final Function residualFilter;
        private final ExpressionNode residualExpression;
        private final int symbolColumnIndex;
        // Set by prepare(). getBool() asserts it, because isThreadSafe() rests on it - see that override.
        private boolean hasPreparedKeySet;

        public PreparedSymbolPatternFilter(
                @NotNull Function providerFunction,
                @Nullable Function residualFilter,
                boolean isNegated,
                int symbolColumnIndex
        ) {
            this(providerFunction, residualFilter, isNegated, symbolColumnIndex, null, null);
        }

        public PreparedSymbolPatternFilter(
                @NotNull Function providerFunction,
                @Nullable Function residualFilter,
                boolean isNegated,
                int symbolColumnIndex,
                @Nullable ExpressionNode providerExpression,
                @Nullable ExpressionNode residualExpression
        ) {
            this.providerFunction = providerFunction;
            this.provider = (SymbolKeySetProvider) providerFunction;
            this.providerExpression = providerExpression;
            this.residualFilter = residualFilter;
            this.residualExpression = residualExpression;
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

        public ExpressionNode getProviderExpression() {
            return providerExpression;
        }

        public ExpressionNode getResidualExpression() {
            return residualExpression;
        }

        public int getSymbolColumnIndex() {
            return symbolColumnIndex;
        }

        public boolean isNegated() {
            return isNegated;
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

        @Override
        public void offerStateTo(Function that) {
            if (that instanceof PreparedSymbolPatternFilter target) {
                providerFunction.offerStateTo(target.providerFunction);
                if (residualFilter != null && target.residualFilter != null) {
                    residualFilter.offerStateTo(target.residualFilter);
                }
                target.hasPreparedKeySet = hasPreparedKeySet;
            }
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
