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
import io.questdb.std.Os;
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
 * new definition.
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
            Os.sleep(1);
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

    private static SqlException fullRefreshPending(TableToken viewToken, int viewNamePosition) {
        return SqlException.$(viewNamePosition, "materialized view full refresh is pending, retry later [view=")
                .put(viewToken.getTableName()).put(']');
    }
}
