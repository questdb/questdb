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

import io.questdb.cairo.CairoEngine;
import io.questdb.cairo.TableToken;
import io.questdb.cairo.sql.SqlExecutionCircuitBreaker;
import io.questdb.cairo.wal.WalWriter;
import io.questdb.cairo.wal.seq.SeqTxnTracker;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.std.ObjHashSet;
import org.jetbrains.annotations.NotNull;

/**
 * {@code ALTER MATERIALIZED VIEW v [WITH BASE b] AS (<select>)}, once the statement has validated the new SQL: it
 * writes the {@code MAT_VIEW_DEFINITION} event to the view's WAL, under the role-switch commit fence, and every node
 * replaces the view's base table name and SQL text when the event applies.
 * <p>
 * Without a base change, the statement takes no latch: the view keeps its validity, rows and refresh position, and a
 * refresh that runs meanwhile finishes with the old SQL.
 * <p>
 * A base change leaves the view invalid ("base table changed") until {@code REFRESH ... RESUME SKIP BACKLOG} positions
 * it on the new base table. The statement takes the view latch, whether or not the view is valid, waiting while
 * another holder has it as RESUME does, and refuses at once while a FULL refresh is pending. Under the latch it writes
 * the invalidation first, with the backlog-lost marker of a view that has completed a refresh, since the backlog
 * belongs to the old base table, and then the definition event, so that every node sees the view invalid before it
 * sees the new base table, and no RESUME or FULL positioned on the old base table can follow the definition event in
 * the view's WAL. It writes the invalidation also when the view is invalid in memory already: a refresh that failed
 * without a WAL writer, or on a read-only node, left the persisted state valid.
 * <p>
 * Either way the node records the sequencer txn of the definition event, see
 * {@link MatViewState#getPendingDefinitionSeqTxn()}, so that a REFRESH issued right after the statement waits for the
 * new definition, and so does a view ADD COLUMN, DROP COLUMN or ALTER ... AS, which checks itself against it.
 */
public final class MatViewAlter {
    private static final Log LOG = LogFactory.getLog(MatViewAlter.class);

    private MatViewAlter() {
    }

    /**
     * Writes the definition event of a validated {@code ALTER MATERIALIZED VIEW ... AS}; see the class description.
     *
     * @param baseTableName     the base table name of the new definition: the current one without a base change
     * @param matViewSql        the new SQL text
     * @param isBaseTableChange true when the statement moves the view to another base table
     * @throws SqlException                    a refusal of a base change: a pending FULL refresh, or a latch that
     *                                         stays busy until the statement times out
     * @throws io.questdb.cairo.CairoException a read-only refusal, a cancellation or a failed write
     */
    public static void alter(
            @NotNull CairoEngine engine,
            @NotNull SqlExecutionContext executionContext,
            @NotNull TableToken viewToken,
            int viewNamePosition,
            @NotNull String baseTableName,
            @NotNull String matViewSql,
            boolean isBaseTableChange,
            @NotNull MatViewColumnMapping.Summary columnMappingSummary
    ) throws SqlException {
        final MatViewStateStore stateStore = engine.getMatViewStateStore();
        final MatViewState viewState = stateStore.getViewState(viewToken);
        if (viewState == null || viewState.isDropped()) {
            throw SqlException.$(viewNamePosition, "materialized view does not exist or is not ready for refresh");
        }
        // The mapping the validation built, for materialized_views() once the statement returns.
        viewState.setColumnMappingSummary(columnMappingSummary);
        viewState.runAlterValidatedSeamForTesting();

        final long seqTxn;
        if (isBaseTableChange) {
            seqTxn = changeBaseTable(engine, executionContext, viewToken, viewState, viewNamePosition, baseTableName, matViewSql);
        } else {
            try (WalWriter walWriter = engine.getWalWriter(viewToken)) {
                seqTxn = MatViewCommitFence.appendMatViewDefinition(engine, walWriter, baseTableName, matViewSql, false);
            }
        }
        viewState.recordPendingDefinitionSeqTxn(seqTxn);
        viewState.alterSuccess(isBaseTableChange);
        LOG.info().$("altered materialized view SQL [view=").$(viewToken)
                .$(", baseTable=").$safe(baseTableName)
                .$(", baseTableChange=").$(isBaseTableChange)
                .$(", seqTxn=").$(seqTxn)
                .$(", unmappedViewColumns=").$safe(columnMappingSummary.getUnmappedViewColumns())
                .$(", ignoredQueryColumns=").$safe(columnMappingSummary.getIgnoredQueryColumns())
                .I$();
    }

    /**
     * Waits until the view's WAL is applied up to the definition event of the last {@code ALTER MATERIALIZED VIEW ...
     * AS} this node ran, see {@link MatViewState#getPendingDefinitionSeqTxn()}, so that a REFRESH issued right after
     * the statement runs the new SQL. Returns at once with no such statement pending, and when the view's WAL is
     * suspended or dropped, which leaves REFRESH as it was before the statement. Honours the statement timeout and
     * cancellation, as RESUME's wait for the view's WAL does.
     */
    public static void awaitPendingDefinition(
            @NotNull CairoEngine engine,
            @NotNull SqlExecutionContext executionContext,
            @NotNull TableToken viewToken
    ) {
        final MatViewState viewState = engine.getMatViewStateStore().getViewState(viewToken);
        if (viewState == null) {
            return;
        }
        final long pendingSeqTxn = viewState.getPendingDefinitionSeqTxn();
        if (pendingSeqTxn < 0) {
            return;
        }
        final SeqTxnTracker viewSeqTracker = engine.getTableSequencerAPI().getTxnTracker(viewToken);
        final SqlExecutionCircuitBreaker circuitBreaker = executionContext.getCircuitBreaker();
        while (viewSeqTracker.getWriterTxn() < pendingSeqTxn) {
            if (viewSeqTracker.isSuspended() || viewSeqTracker.isDropped()) {
                LOG.info().$("materialized view WAL is suspended, not waiting for its new definition [view=").$(viewToken)
                        .$(", definitionSeqTxn=").$(pendingSeqTxn)
                        .I$();
                return;
            }
            viewState.runDefinitionWaitSeamForTesting();
            circuitBreaker.statefulThrowExceptionIfTrippedNoThrottle();
            MatViewResume.pauseWait(engine);
        }
    }

    /**
     * Waits until the view's WAL is applied up to the definition event of the last {@code ALTER MATERIALIZED VIEW ...
     * AS} this node ran, see {@link MatViewState#getPendingDefinitionSeqTxn()}, so that a view ADD COLUMN, DROP COLUMN
     * or ALTER ... AS issued right after the statement checks itself against the definition that its own WAL entry
     * applies after. Returns at once with no such statement pending, and once the view's WAL applied the event, or
     * passed it, whatever the WAL's state is then. While the event waits, it refuses a dropped view, and a view whose WAL
     * apply is suspended, see {@link MatViewBacklog#isViewWalSuspended(CairoEngine, TableToken, SeqTxnTracker)}: the WAL
     * then reaches the event only after a user acts, and the definition the statement's entry would apply after is
     * unknown until then. Honours the statement timeout and cancellation, as REFRESH's wait does.
     *
     * @param viewNamePosition the position of the view name in the statement, where refusals point
     * @throws SqlException a dropped view, or a view WAL whose apply is suspended
     */
    public static void awaitPendingDefinitionForAlter(
            @NotNull CairoEngine engine,
            @NotNull SqlExecutionContext executionContext,
            @NotNull TableToken viewToken,
            int viewNamePosition
    ) throws SqlException {
        final MatViewState viewState = engine.getMatViewStateStore().getViewState(viewToken);
        if (viewState == null) {
            // no ALTER ... AS ran on this node, which alter() refuses for a view without a state
            return;
        }
        final long pendingSeqTxn = viewState.getPendingDefinitionSeqTxn();
        if (pendingSeqTxn < 0) {
            return;
        }
        // the loop reads every state from this tracker, see MatViewBacklog.isViewWalSuspended()
        final SeqTxnTracker viewSeqTracker = engine.getTableSequencerAPI().getTxnTracker(viewToken);
        final SqlExecutionCircuitBreaker circuitBreaker = executionContext.getCircuitBreaker();
        // The record never resets, so the applied event counts first, whatever the WAL's state is now: a suspension
        // that came after the event applied stops no statement.
        while (viewSeqTracker.getWriterTxn() < pendingSeqTxn) {
            // a dropped view does not exist, whatever stops the apply of its WAL
            if (viewSeqTracker.isDropped()) {
                throw SqlException.matViewDoesNotExist(viewNamePosition, viewToken.getTableName());
            }
            if (MatViewBacklog.isViewWalSuspended(engine, viewToken, viewSeqTracker)) {
                throw definitionNotApplied(engine, viewToken, viewNamePosition);
            }
            viewState.runDefinitionWaitSeamForTesting();
            circuitBreaker.statefulThrowExceptionIfTrippedNoThrottle();
            MatViewResume.pauseWait(engine);
        }
    }

    private static long changeBaseTable(
            CairoEngine engine,
            SqlExecutionContext executionContext,
            TableToken viewToken,
            MatViewState viewState,
            int viewNamePosition,
            String baseTableName,
            String matViewSql
    ) throws SqlException {
        if (viewState.isPendingFullRefresh()) {
            throw fullRefreshPending(viewToken, viewNamePosition);
        }
        final SqlExecutionCircuitBreaker circuitBreaker = executionContext.getCircuitBreaker();
        MatViewResume.lockView(engine, viewState, circuitBreaker, viewToken, viewNamePosition);
        try {
            if (viewState.isPendingFullRefresh()) {
                throw fullRefreshPending(viewToken, viewNamePosition);
            }
            if (viewState.isDropped()) {
                throw SqlException.matViewDoesNotExist(viewNamePosition, viewToken.getTableName());
            }
            try (WalWriter walWriter = engine.getWalWriter(viewToken)) {
                // The view's backlog belongs to the old base table, so a view that has completed a refresh needs RESUME
                // SKIP BACKLOG, also when the new base is the old one renamed, with the same table id. A view that never
                // completed a refresh has no backlog, and its next refresh does the initial build.
                final boolean hasRefreshed = viewState.getLastRefreshBaseTxn() != -1;
                final String backlogLostReason = hasRefreshed ? MatViewBacklog.LOST_BASE_TABLE_CHANGED : viewState.getBacklogLostReason();
                MatViewCommitFence.resetMatViewState(
                        engine,
                        walWriter,
                        viewState.getLastRefreshBaseTxn(),
                        viewState.getLastRefreshFinishTimestampUs(),
                        true,
                        MatViewBacklog.LOST_BASE_TABLE_CHANGED,
                        viewState.getLastPeriodHi(),
                        viewState.getRefreshIntervals(),
                        viewState.getRefreshIntervalsBaseTxn(),
                        viewState.getBaseTableId(),
                        backlogLostReason
                );
                viewState.markAsInvalid(MatViewBacklog.LOST_BASE_TABLE_CHANGED);
                if (hasRefreshed) {
                    viewState.markBacklogLost(MatViewBacklog.LOST_BASE_TABLE_CHANGED);
                }
                return MatViewCommitFence.appendMatViewDefinition(engine, walWriter, baseTableName, matViewSql, true);
            }
        } finally {
            // The release wakes the work that published its intent while the statement held the latch, as for RESUME.
            // The statement's outcome is decided by then, so a failure is logged.
            try {
                MatViewRefreshJob.finalizeAndUnlock(engine, engine.getMatViewStateStore(), viewToken, viewState, false);
            } catch (Throwable th) {
                LOG.error().$("could not deliver deferred materialized view work after base table change [view=").$(viewToken)
                        .$(", ex=").$(th)
                        .I$();
            }
        }
    }

    // The refusal of a view ALTER while the WAL apply of the view stops short of a pending definition event. RESUME WAL
    // lifts a suspension; a view in the cairo.wal.apply.suspended.tables list also needs its entry, by dir name, out of
    // the list, which RESUME WAL does not lift.
    private static SqlException definitionNotApplied(CairoEngine engine, TableToken viewToken, int viewNamePosition) {
        final SqlException e = SqlException.$(viewNamePosition, "materialized view WAL is suspended with an earlier ALTER MATERIALIZED VIEW ... AS not applied, ");
        final ObjHashSet<String> suspendedTables = engine.getConfiguration().getWalApplySuspendedTables();
        if (suspendedTables != null && suspendedTables.contains(viewToken.getDirName())) {
            e.put("remove ").put(viewToken.getDirName()).put(" from cairo.wal.apply.suspended.tables and ");
        }
        return e.put("run ALTER MATERIALIZED VIEW ").put(viewToken.getTableName()).put(" RESUME WAL first");
    }

    private static SqlException fullRefreshPending(TableToken viewToken, int viewNamePosition) {
        return SqlException.$(viewNamePosition, "materialized view full refresh is pending, retry later [view=")
                .put(viewToken.getTableName()).put(']');
    }
}
