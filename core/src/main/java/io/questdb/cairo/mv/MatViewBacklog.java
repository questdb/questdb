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

package io.questdb.cairo.mv;

import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.CairoEngine;
import io.questdb.cairo.CairoException;
import io.questdb.cairo.TableToken;
import io.questdb.cairo.wal.WalWriter;
import io.questdb.cairo.wal.seq.SeqTxnTracker;
import io.questdb.griffin.model.IntervalUtils;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.std.LongList;
import io.questdb.std.str.Path;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * The backlog of an invalid materialized view: the time ranges of the base table changes the view misses while it
 * is invalid. {@code REFRESH MATERIALIZED VIEW v RESUME} keeps the view's refresh position and its backlog, and the
 * view's next incremental refresh recomputes them.
 * <p>
 * The backlog lives in the view's refresh-interval cache, the mechanism that lets a valid MANUAL or TIMER view
 * release its base table's WAL: the cached intervals cover the base table's data commits up to the cache position,
 * {@code max(lastRefreshBaseTxn, refreshIntervalsBaseTxn)}, and the base table's WAL from the cache position covers
 * the rest. A cache update scans the base table's WAL from the cache position, applies the TRUNCATE cut, merges what
 * it finds into the cache, which keeps its cap of {@code cairo.mat.view.max.refresh.intervals} intervals, and
 * persists the cache through a view state event that keeps the view's invalid flag and reason.
 * <p>
 * A view tracks a backlog once it has completed a refresh and its state knows the id of its base table, see
 * {@link MatViewState#hasTrackedBacklog()}. The backlog is lost, and tracking stops, when a cache update finds:
 * <ul>
 *     <li>no base table id in the view's state, {@link #LOST_NO_BASE_TABLE_ID}, as for a view invalidated before its
 *     first refresh on this version;</li>
 *     <li>no table under the base table's name, one that is not a WAL table or a materialized view, or one with
 *     another id than the persisted one, {@link #LOST_BASE_TABLE_REPLACED}: the base was dropped, renamed, rebased or
 *     re-created;</li>
 *     <li>a cache position beyond the base table's applied transaction, {@link #LOST_VIEW_AHEAD_OF_BASE_TABLE};</li>
 *     <li>the base table's WAL from the cache position missing, {@link #LOST_BASE_TABLE_WAL_MISSING}.</li>
 * </ul>
 * A cache update that fails for any other reason loses the backlog too, {@link #LOST_UPDATE_FAILED}, and leaves the
 * view's invalidation reason as it is. The marker, the cause of the loss, is part of every view state event, and
 * REFRESH ... RESUME refuses a lost backlog with its cause until SKIP BACKLOG is given. RESUME SKIP BACKLOG and
 * REFRESH ... FULL clear it.
 */
public final class MatViewBacklog {
    // Set by ALTER MATERIALIZED VIEW ... WITH BASE, which moves the view to another base table.
    public static final String LOST_BASE_TABLE_CHANGED = "base table changed";
    public static final String LOST_BASE_TABLE_REPLACED = "base table replaced";
    public static final String LOST_BASE_TABLE_WAL_MISSING = "base table WAL missing";
    public static final String LOST_NO_BASE_TABLE_ID = "no base table id";
    public static final String LOST_UPDATE_FAILED = "backlog update failed";
    public static final String LOST_VIEW_AHEAD_OF_BASE_TABLE = "view ahead of base table";
    private static final Log LOG = LogFactory.getLog(MatViewBacklog.class);

    private MatViewBacklog() {
    }

    /**
     * Returns the cause of a lost backlog that the view's state and its base table give away without a WAL scan, or
     * null when there is none.
     *
     * @param baseTableToken the table under the base table's name now, or null when there is none
     * @param appliedTxn     the base table's latest applied transaction
     */
    public static @Nullable String findLostCause(
            @NotNull MatViewState viewState,
            @Nullable TableToken baseTableToken,
            long appliedTxn
    ) {
        final int baseTableId = viewState.getBaseTableId();
        if (baseTableId == MatViewState.BASE_TABLE_ID_UNKNOWN) {
            return LOST_NO_BASE_TABLE_ID;
        }
        if (!isSameBaseTable(baseTableId, baseTableToken)) {
            return LOST_BASE_TABLE_REPLACED;
        }
        if (getCachePosition(viewState) > appliedTxn) {
            return LOST_VIEW_AHEAD_OF_BASE_TABLE;
        }
        return null;
    }

    /**
     * Returns the base table transaction up to which the view's refresh position and its cached refresh intervals
     * cover the base table's changes; the base table's WAL from there on holds the rest.
     */
    public static long getCachePosition(@NotNull MatViewState viewState) {
        return Math.max(viewState.getLastRefreshBaseTxn(), viewState.getRefreshIntervalsBaseTxn());
    }

    /**
     * Returns true when the table under the base table's name now is the base table that the view's backlog belongs
     * to: a WAL table or a materialized view with the id that the view's state persists. A table that is gone, that
     * the view cannot refresh from, or that has another id replaced it.
     *
     * @param baseTableId    the base table id that the view's state persists
     * @param baseTableToken the table under the base table's name now, or null when there is none
     */
    public static boolean isSameBaseTable(int baseTableId, @Nullable TableToken baseTableToken) {
        return baseTableToken != null && !isUnusableBase(baseTableToken) && baseTableToken.getTableId() == baseTableId;
    }

    /**
     * Returns true while the view's own WAL apply is suspended, by ALTER ... SUSPEND WAL, an apply error or the
     * {@code cairo.wal.apply.suspended.tables} list. A suspended view cannot persist its backlog, so it neither takes
     * cache updates nor holds back its base table's WAL purging meanwhile.
     */
    public static boolean isViewWalSuspended(@NotNull CairoEngine engine, @NotNull TableToken viewToken) {
        final SeqTxnTracker viewSeqTracker = engine.getTableSequencerAPI().getTxnTracker(viewToken);
        return viewSeqTracker.isSuspended() || engine.isWalApplySuspended(viewToken, viewSeqTracker);
    }

    /**
     * Marks the view's backlog lost with the cause and persists the marker with a view state event that keeps the rest
     * of the view's state. When the event cannot be written, because there is no writer, the node is read-only or the
     * write fails, the marker lives in memory until the next view state event: after a restart the base table's WAL
     * from the cache position is gone by then, since the view no longer holds it back, and a cache update finds the
     * backlog lost again. The caller holds the view latch.
     */
    public static void markLost(
            @NotNull CairoEngine engine,
            @Nullable WalWriter walWriter,
            @NotNull MatViewState viewState,
            @NotNull String cause
    ) {
        assert viewState.isLocked();
        viewState.markBacklogLost(cause);
        final TableToken viewToken = viewState.getViewDefinition().getMatViewToken();
        LOG.info().$("materialized view backlog is lost, RESUME needs SKIP BACKLOG [view=").$(viewToken)
                .$(", reason=").$(cause)
                .I$();
        if (walWriter != null && !engine.isReadOnlyMode()) {
            try {
                persist(engine, walWriter, viewState, viewState.getRefreshIntervals(), viewState.getRefreshIntervalsBaseTxn());
            } catch (Throwable th) {
                engine.handleDataSyncFailure(th);
                LOG.error().$("could not persist lost materialized view backlog, keeping the marker in memory [view=").$(viewToken)
                        .$(", reason=").$(cause)
                        .$(", ex=").$(th)
                        .I$();
            }
        }
    }

    /**
     * Unions the intervals, sorted and disjoint, with the view's cached refresh intervals into the cache. Past
     * {@code cairo.mat.view.max.refresh.intervals} intervals, the newest ones merge into one, so the cache stays
     * bounded however long the view stays invalid. The caller holds the view latch.
     */
    public static void mergeIntoCachedIntervals(
            @NotNull CairoConfiguration configuration,
            @NotNull MatViewState viewState,
            @NotNull LongList intervals
    ) {
        unionCapped(configuration, intervals, viewState.getRefreshIntervals());
        viewState.setRefreshIntervals(intervals);
    }

    /**
     * Brings the backlog of an invalid view up to the base table transaction {@code scanToTxn}: it checks the lost
     * conditions, scans the base table's WAL from the cache position, merges what it finds into the view's cache and,
     * unless the caller persists it, persists the cache with a view state event that keeps the view's invalid flag and
     * reason. A base table with no new transaction writes nothing. Returns true when the backlog is tracked, and false
     * when it is lost, in which case the view's state carries the cause. A view that never completed a refresh tracks
     * no backlog, since its next refresh builds it from scratch, so callers never pass one.
     * <p>
     * The caller holds the view latch and handles a throwable: a failure to persist the cache, which the caller turns
     * into {@link #LOST_UPDATE_FAILED} unless the node turned read-only.
     *
     * @param baseTableToken   the table under the base table's name now, or null when there is none
     * @param scanToTxn        the base table transaction to scan up to, never beyond its applied transaction
     * @param appliedTxn       the base table's latest applied transaction, or a later committed one, against which the
     *                         cache position counts as ahead of the base table
     * @param isLossPersisted  whether a loss persists the marker with a view state event; REFRESH ... RESUME, whose
     *                         refusal writes no view state, keeps it in memory, see {@link #markLost}
     * @param isCachePersisted whether the cache the scan brings up to date is persisted here. A caller that persists the
     *                         view's state right after, such as the task that adds a base range to the backlog, passes
     *                         false: the view's state then takes the cache before it is persisted, and the caller turns a
     *                         failure of its own write into a lost backlog
     */
    public static boolean update(
            @NotNull CairoEngine engine,
            @NotNull WalTxnRangeLoader txnRangeLoader,
            @NotNull LongList intervals,
            @NotNull MatViewState viewState,
            @NotNull WalWriter walWriter,
            @Nullable TableToken baseTableToken,
            long scanToTxn,
            long appliedTxn,
            boolean isLossPersisted,
            boolean isCachePersisted
    ) {
        assert viewState.isLocked();
        // The refresh job takes only a tracked backlog, and RESUME skips a view that never completed a refresh.
        assert viewState.getLastRefreshBaseTxn() != -1;
        if (viewState.getBacklogLostReason() != null) {
            return false;
        }
        final TableToken viewToken = viewState.getViewDefinition().getMatViewToken();
        String cause = findLostCause(viewState, baseTableToken, appliedTxn);
        if (cause == null) {
            // findLostCause() found the base table
            assert baseTableToken != null;
            final long cachePosition = getCachePosition(viewState);
            if (cachePosition >= scanToTxn) {
                return true;
            }
            intervals.clear();
            try {
                txnRangeLoader.load(engine, Path.PATH.get(), baseTableToken, intervals, cachePosition, scanToTxn);
            } catch (CairoException e) {
                LOG.error().$("could not read base table WAL transactions for materialized view backlog [view=").$(viewToken)
                        .$(", baseTable=").$(baseTableToken)
                        .$(", fromTxn=").$(cachePosition)
                        .$(", toTxn=").$(scanToTxn)
                        .$(", ex=").$safe(e.getFlyweightMessage())
                        .$(", errno=").$(e.getErrno())
                        .I$();
                cause = LOST_BASE_TABLE_WAL_MISSING;
            }
            if (cause == null) {
                // The new cache goes into the intervals list; the view's state takes it once it is persisted, so a
                // failed write leaves the view's state as it was. A caller that persists the state itself handles
                // its own failed write.
                if (txnRangeLoader.hasTruncate()) {
                    // The TRUNCATE cut: the base table no longer holds the rows of the cached intervals, and
                    // recomputing them would delete the view's rows there.
                    LOG.info().$("base table was truncated, dropping materialized view backlog queued before the truncate [view=").$(viewToken)
                            .$(", baseTable=").$(baseTableToken)
                            .$(", fromTxn=").$(cachePosition)
                            .$(", toTxn=").$(scanToTxn)
                            .I$();
                    unionCapped(engine.getConfiguration(), intervals, null);
                } else {
                    unionCapped(engine.getConfiguration(), intervals, viewState.getRefreshIntervals());
                }
                if (isCachePersisted) {
                    persist(engine, walWriter, viewState, intervals, scanToTxn);
                }
                viewState.setRefreshIntervals(intervals);
                viewState.setRefreshIntervalsBaseTxn(scanToTxn);
                LOG.info().$("updated materialized view backlog [view=").$(viewToken)
                        .$(", baseTable=").$(baseTableToken)
                        .$(", fromTxn=").$(cachePosition)
                        .$(", toTxn=").$(scanToTxn)
                        .$(", intervalCount=").$(viewState.getRefreshIntervals().size() / 2)
                        .I$();
                return true;
            }
        }
        markLost(engine, isLossPersisted ? walWriter : null, viewState, cause);
        return false;
    }

    // Writes the view's state, invalid flag and reason included, with the cache and the backlog-lost marker.
    private static void persist(
            CairoEngine engine,
            WalWriter walWriter,
            MatViewState viewState,
            LongList refreshIntervals,
            long refreshIntervalsBaseTxn
    ) {
        MatViewCommitFence.resetMatViewState(
                engine,
                walWriter,
                viewState.getLastRefreshBaseTxn(),
                viewState.getLastRefreshFinishTimestampUs(),
                viewState.isInvalid(),
                viewState.getInvalidationReason(),
                viewState.getLastPeriodHi(),
                refreshIntervals,
                refreshIntervalsBaseTxn,
                viewState.getBaseTableId(),
                viewState.getBacklogLostReason()
        );
    }

    // Unions the intervals, sorted and disjoint, with the cached ones, when given, in place. Past
    // cairo.mat.view.max.refresh.intervals intervals, the newest ones merge into one.
    private static void unionCapped(CairoConfiguration configuration, LongList intervals, @Nullable LongList cached) {
        if (cached != null) {
            final int dividerIndex = intervals.size();
            intervals.addAll(cached);
            IntervalUtils.unionInPlace(intervals, dividerIndex);
        }
        final int cacheCapacity = configuration.getMatViewMaxRefreshIntervals() << 1;
        if (intervals.size() > cacheCapacity) {
            // Squash the latest intervals into a single one.
            intervals.setQuick(cacheCapacity - 1, intervals.getQuick(intervals.size() - 1));
            intervals.setPos(cacheCapacity);
        }
    }

    // A base table the view cannot refresh from: not a WAL table, or a regular or live view.
    static boolean isUnusableBase(TableToken baseTableToken) {
        return !baseTableToken.isWal() || baseTableToken.isView() || baseTableToken.isLiveView();
    }
}
