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
import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.PageFrameCursor;
import io.questdb.cairo.sql.PartitionFrameCursorFactory;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordMetadata;
import io.questdb.cairo.sql.RowCursorFactory;
import io.questdb.cairo.sql.TimeFrameCursor;
import io.questdb.griffin.PlanSink;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.std.IntList;
import io.questdb.std.Misc;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import static io.questdb.cairo.sql.PartitionFrameCursorFactory.ORDER_DESC;

/**
 * Base-table scan factory for a composite (time + non-time dimension) table. Extends
 * {@link PageFrameRecordCursorFactory} but replaces the record cursor with a
 * {@link CompositeMergePartitionRecordCursor}, so {@link #getCursor(SqlExecutionContext)} yields a genuinely
 * global-designated-timestamp-ordered stream (the plain factory's per-cell-concatenated stream is
 * misordered for a composite table -- see the merge cursor's class doc).
 * <p>
 * Because the merged stream IS ordered, {@link #getScanDirection()} is truthful, which is exactly what the
 * order-consuming plan-time decisions rely on (ORDER BY sort-skip, SAMPLE BY eligibility, join
 * both-ascending validation). {@link #supportsPageFrameCursor()} still returns false: the merge is
 * row-granular (a page frame is one cell's contiguous memory and cannot interleave two cells), so every
 * ORDER-SENSITIVE page-frame consumer -- async filter, fast/window/horizon joins, parquet/QWP raw-frame
 * export -- must keep degrading to the row-based {@link #getCursor(SqlExecutionContext)} path, which is
 * correct.
 * <p>
 * Aggregation, however, is provably order-indifferent (vector aggregates and any
 * {@code GroupByFunction.supportsParallelism()} opt-in combine partials commutatively), so the base's
 * REAL, cell-blind page-frame cursor -- built directly over the per-cell {@code partitionFrameCursorFactory}
 * ({@link #getPageFrameCursor} is no longer overridden to null here; it now falls through to the inherited
 * {@link PageFrameRecordCursorFactory} implementation) -- is safe for it.
 * {@link #supportsPageFrameCursorForUnorderedAggregation()} overrides to {@code true} to expose exactly
 * that; it is consulted ONLY by the four vectorized/parallel group-by selection sites in
 * {@code SqlCodeGenerator#generateSelectGroupBy}. Every other {@link #getPageFrameCursor} caller keeps
 * gating on the unchanged {@link #supportsPageFrameCursor()} (still false) and therefore never observes
 * these frames -- see {@link #supportsPageFrameCursorForUnorderedAggregation()}'s javadoc on
 * {@link io.questdb.cairo.sql.RecordCursorFactory} for the inverted-invariant hazard this creates for any
 * NEW caller that reaches {@link #getPageFrameCursor} without gating on {@link #supportsPageFrameCursor()}
 * first.
 * <p>
 * It DOES, however, advertise a (forward-only) <em>time-frame</em> cursor
 * ({@link #supportsTimeFrameCursor()} returns {@code forward}, {@link #getTimeFrameCursor} builds a
 * {@link CompositeTimeFrameRecordCursor} over the merged per-day permutation), so a composite table can
 * be a WINDOW / HORIZON join SLAVE. That merged cursor is SYNTHETIC and single-threaded: its frames are
 * per-day cross-cell merges and its rowIds are merge ordinals (not physical partitions/native rows), and
 * there is no per-worker concurrent twin ({@link #newTimeFrameCursor()} stays null). So
 * {@link #supportsConcurrentTimeFrameCursor()} returns false, which keeps such a slave off the
 * ASYNC/PARALLEL WINDOW / HORIZON join (whose atom would NPE on the null concurrent cursor) and off the
 * fast ASOF / LT time-frame factory, routing those to the SERIAL window/horizon join and the LIGHT
 * ASOF / LT join respectively (preserving the composite-read design).
 * <p>
 * CORRECTION (merge audit 2026-08-10): an earlier version of this note also claimed a composite slave
 * never reaches the FAST symbol-indexed WINDOW join. That is NOT true, and was not true before the
 * master merge either -- {@code SqlCodeGenerator} selects {@code WindowJoinFastRecordCursorFactory} on
 * {@code supportsTimeFrameCursor()} alone (which is {@code forward} here), so a keyed window join with a
 * composite slave does take it. Measured with a probe rather than argued: 5 cases in
 * {@code CompositeWindowHorizonEndToEndTest} reach it, and they are differential
 * {@code ...MatchPlainTwin} tests that PASS -- the fast factory is twin-correct over the merged cursor
 * for those shapes. Do NOT "fix" this by gating the fast path: that would disable behaviour currently
 * proven correct. Trust the differential tests over this comment.
 * <p>
 * {@code convertToSampleByIndexPageFrameCursorFactory()} is deliberately NOT overridden: it is a separate
 * gate -- independent of {@link #supportsPageFrameCursor()} / {@link #getPageFrameCursor} -- that
 * {@code SqlCodeGenerator}'s SAMPLE BY FIRST/LAST path checks BEFORE ever calling
 * {@code getPageFrameCursor()}. The inherited default unconditionally returns null, so
 * {@code SampleByFirstLastRecordCursorFactory} is never constructed over a composite base, regardless of
 * what {@link #getPageFrameCursor} itself now returns.
 */
public class CompositePageFrameRecordCursorFactory extends PageFrameRecordCursorFactory {
    private final CairoConfiguration configuration;
    private final boolean forward;
    private final CompositeMergePartitionRecordCursor mergeCursor;
    // Lazily built on the first getTimeFrameCursor() call (composite table as a SERIAL WINDOW/HORIZON
    // join slave); reused across cursors and freed in _close(). Null until first used.
    private CompositeTimeFrameRecordCursor compositeTimeFrameCursor;

    public CompositePageFrameRecordCursorFactory(
            @NotNull CairoConfiguration configuration,
            RecordMetadata metadata,
            PartitionFrameCursorFactory partitionFrameCursorFactory,
            RowCursorFactory rowCursorFactory,
            boolean followsOrderByAdvice,
            @Nullable Function filter,
            boolean framingSupported,
            @NotNull IntList columnIndexes,
            @NotNull IntList columnSizeShifts,
            boolean supportsRandomAccess,
            boolean singleRowFactory
    ) {
        super(
                configuration,
                metadata,
                partitionFrameCursorFactory,
                rowCursorFactory,
                followsOrderByAdvice,
                filter,
                framingSupported,
                columnIndexes,
                columnSizeShifts,
                supportsRandomAccess,
                singleRowFactory
        );
        this.configuration = configuration;
        // ORDER_ASC/ORDER_ANY -> forward (min-heap merge), ORDER_DESC -> backward (max-heap merge). Mirrors
        // AbstractPageFrameRecordCursorFactory.initPageFrameCursor's Fwd/Bwd choice.
        this.forward = partitionFrameCursorFactory.getOrder() != ORDER_DESC;
        this.mergeCursor = new CompositeMergePartitionRecordCursor(
                configuration,
                metadata,
                metadata.getTimestampIndex(),
                forward
        );
    }

    @Override
    public int getScanDirection() {
        // Truthful: the merged stream really is ordered in this direction.
        return forward ? SCAN_DIRECTION_FORWARD : SCAN_DIRECTION_BACKWARD;
    }

    @Override
    public TimeFrameCursor getTimeFrameCursor(SqlExecutionContext executionContext) throws SqlException {
        // Serial WINDOW/HORIZON join slave seam: hand the join helpers a merged, per-day
        // designated-timestamp-ordered time-frame cursor over the composite cells. Forward (ASC/ANY)
        // only -- the permutation is built in a single ascending pass (CompositeTimeFrameRecordCursor is
        // forward-only), so a backward composite scan yields null here, kept consistent with
        // supportsTimeFrameCursor()==forward so no consumer ever dereferences a null cursor.
        if (!forward) {
            return null;
        }
        final TablePageFrameCursor pageFrameCursor = initPageFrameCursor(executionContext);
        if (compositeTimeFrameCursor == null) {
            compositeTimeFrameCursor = new CompositeTimeFrameRecordCursor(configuration, getMetadata());
        }
        return compositeTimeFrameCursor.of(pageFrameCursor, executionContext);
    }

    @Override
    public ConcurrentTimeFrameCursor newTimeFrameCursor() {
        // Deferred non-goal: there is no concurrent (per-worker) composite time-frame cursor twin, so the
        // async/parallel join atoms must never select this factory. supportsConcurrentTimeFrameCursor()
        // returns false to force the serial path; this stays null as the belt-and-braces backstop.
        return null;
    }

    @Override
    public boolean supportsConcurrentTimeFrameCursor() {
        // The merged cursor is synthetic: its frames are per-day cross-cell merges and its rowIds are merge
        // ordinals, NOT physical partitions/native rows, and there is no per-worker twin (newTimeFrameCursor()
        // == null). Returning false keeps a composite slave off BOTH the async WINDOW/HORIZON path (would NPE
        // on the null concurrent cursor) AND the fast ASOF/LT/window path (would mis-address the synthetic
        // frames -- e.g. jumpTo() by a physical frame index out of the merged range). The generator routes it
        // to the SERIAL non-fast WINDOW/HORIZON join and the LIGHT ASOF/LT join, both of which are correct.
        return false;
    }

    @Override
    public boolean supportsPageFrameCursor() {
        return false;
    }

    /**
     * Narrow opt-in: {@link #getPageFrameCursor} now returns the inherited real, cell-blind page-frame
     * cursor (see the class doc), which is wrong for anything order-sensitive but correct for
     * order-indifferent aggregation. Only the four vectorized/parallel group-by selection sites in
     * {@code SqlCodeGenerator} consult this capability; every other page-frame consumer keeps gating on
     * the unchanged {@link #supportsPageFrameCursor()} ({@code false}) above and therefore never reaches
     * {@link #getPageFrameCursor}.
     *
     * @return true -- composite aggregation may consume the cell-blind frames
     */
    @Override
    public boolean supportsPageFrameCursorForUnorderedAggregation() {
        return true;
    }

    @Override
    public boolean supportsTimeFrameCursor() {
        // Forward-only: the merged per-day permutation is built by a single ascending pass, so a backward
        // composite scan cannot serve a time-frame cursor. Consistent with getTimeFrameCursor() returning
        // null when !forward, so the join slave gate never accepts a scan we cannot satisfy.
        return forward;
    }

    @Override
    public void toPlan(PlanSink sink) {
        // 6a Minor (b): a distinct label from the inherited PageFrameRecordCursorFactory.toPlan()'s
        // "PageFrame" so EXPLAIN visibly distinguishes the merged composite scan from a plain one;
        // toPlanInner (inherited) still lists the same rowCursorFactory/partitionFrameCursorFactory
        // children.
        sink.type("Composite cross-cell merge scan");
        toPlanInner(sink);
    }

    @Override
    protected void _close() {
        super._close();
        Misc.free(mergeCursor);
        Misc.free(compositeTimeFrameCursor);
    }

    @Override
    protected RecordCursor initRecordCursor(
            PageFrameCursor frameCursor,
            SqlExecutionContext executionContext
    ) throws SqlException {
        mergeCursor.of(frameCursor, executionContext);
        return mergeCursor;
    }
}
