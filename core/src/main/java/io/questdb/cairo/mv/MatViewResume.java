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
import io.questdb.cairo.CairoException;
import io.questdb.cairo.TableReader;
import io.questdb.cairo.TableToken;
import io.questdb.cairo.sql.RecordMetadata;
import io.questdb.cairo.sql.SqlExecutionCircuitBreaker;
import io.questdb.cairo.sql.TableRecordMetadata;
import io.questdb.cairo.wal.WalWriter;
import io.questdb.cairo.wal.seq.SeqTxnTracker;
import io.questdb.griffin.MatViewSqlValidator;
import io.questdb.griffin.SqlCompiler;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.std.Os;
import org.jetbrains.annotations.NotNull;

/**
 * {@code REFRESH MATERIALIZED VIEW v RESUME}: makes an invalid view valid again and keeps its rows. This version
 * jumps: it sets the view's refresh position to the base table's latest applied transaction, clears the view's cached
 * refresh intervals and recomputes nothing for the gap between the old position and the new one, which the user
 * backfills with {@code REFRESH ... RANGE}. A view that never completed a refresh keeps its "never refreshed" position,
 * so its next refresh does the initial build.
 * <p>
 * The statement is synchronous:
 * <ol>
 *     <li>it does nothing on a valid view;</li>
 *     <li>it waits until the view's own WAL is applied, without holding the view latch and within the statement
 *     timeout, so that it can follow a view column change directly, and refuses a view whose WAL is suspended;</li>
 *     <li>it refuses at once while a FULL refresh is pending, which recovers the view itself; otherwise it takes the
 *     view latch, polling it within the statement timeout while another holder has it, and refuses with "retry
 *     later" only when the timeout expires first. Under the latch it refuses a FULL refresh that became pending
 *     meanwhile, and goes back to the wait when a view WAL transaction arrived after it;</li>
 *     <li>it compiles the view's SQL under the caller's security context, reading every bare name as a column, and
 *     checks the column mapping and casts;</li>
 *     <li>it resolves the base table by name, which must be a WAL table or a materialized view with the view's
 *     designated timestamp type, and reads its latest applied transaction from a reader;</li>
 *     <li>it persists the new state, which carries the base table id, through the view's WAL under the role-switch
 *     commit fence, and updates the view's in-memory state;</li>
 *     <li>it releases the latch through {@link MatViewRefreshJob#finalizeAndUnlock}, which re-drives an invalidation
 *     published while it held the latch, and enqueues an incremental refresh for an IMMEDIATE view. A failure of
 *     either is logged and does not fail the statement.</li>
 * </ol>
 * While it holds the latch, the view counts as recovering, see {@link MatViewState#isResuming()}: an invalidation
 * that arrives meanwhile is kept, and invalidates the view again once RESUME has made it valid. A RESUME that refuses
 * drops those invalidations, since the view stays invalid.
 */
public final class MatViewResume {
    public static final String MODE_SKIP_BACKLOG = "skip backlog";
    private static final Log LOG = LogFactory.getLog(MatViewResume.class);

    private MatViewResume() {
    }

    /**
     * Resumes the view; see the class description.
     *
     * @param viewNamePosition the position of the view name in the statement, where refusals point
     * @throws SqlException   a refusal
     * @throws CairoException an authorization error, a read-only refusal, a cancellation or a timeout of the wait
     *                        for the view's WAL
     */
    public static void resume(
            @NotNull CairoEngine engine,
            @NotNull SqlExecutionContext executionContext,
            @NotNull TableToken viewToken,
            int viewNamePosition,
            @NotNull MatViewSqlValidator validator
    ) throws SqlException {
        final MatViewStateStore stateStore = engine.getMatViewStateStore();
        final MatViewState viewState = stateStore.getViewState(viewToken);
        if (viewState == null || viewState.isDropped()) {
            throw SqlException.$(viewNamePosition, "materialized view does not exist or is not ready for refresh");
        }
        if (!viewState.isInvalid()) {
            LOG.info().$("materialized view is valid, nothing to resume [view=").$(viewToken).I$();
            return;
        }

        // The compile of the statement started the statement's timer, which both waits honour.
        final SqlExecutionCircuitBreaker circuitBreaker = executionContext.getCircuitBreaker();
        final SeqTxnTracker viewSeqTracker = engine.getTableSequencerAPI().getTxnTracker(viewToken);
        while (true) {
            awaitViewWal(viewState, viewSeqTracker, circuitBreaker, viewToken, viewNamePosition);
            viewState.runResumeWalAppliedSeamForTesting();
            if (viewState.isPendingFullRefresh()) {
                throw fullRefreshPending(viewToken, viewNamePosition);
            }
            lockView(viewState, circuitBreaker, viewToken, viewNamePosition);
            if (resumeLocked(engine, executionContext, viewToken, viewState, viewSeqTracker, viewNamePosition, validator)) {
                return;
            }
            // a view WAL transaction arrived after the wait, which the latch was released for
        }
    }

    private static void awaitViewWal(
            MatViewState viewState,
            SeqTxnTracker viewSeqTracker,
            SqlExecutionCircuitBreaker circuitBreaker,
            TableToken viewToken,
            int viewNamePosition
    ) throws SqlException {
        final long targetSeqTxn = viewSeqTracker.getSeqTxn();
        while (true) {
            if (viewSeqTracker.isSuspended()) {
                throw SqlException.$(viewNamePosition, "materialized view WAL is suspended, run ALTER MATERIALIZED VIEW ")
                        .put(viewToken.getTableName()).put(" RESUME WAL first");
            }
            if (viewSeqTracker.isDropped()) {
                throw SqlException.matViewDoesNotExist(viewNamePosition, viewToken.getTableName());
            }
            if (viewSeqTracker.getWriterTxn() >= targetSeqTxn) {
                return;
            }
            viewState.runResumeWalWaitSeamForTesting();
            circuitBreaker.statefulThrowExceptionIfTrippedNoThrottle();
            Os.sleep(1);
        }
    }

    private static SqlException fullRefreshPending(TableToken viewToken, int viewNamePosition) {
        return SqlException.$(viewNamePosition, "materialized view full refresh is pending, it recovers the view [view=")
                .put(viewToken.getTableName()).put(']');
    }

    private static boolean hasUnappliedViewWal(SeqTxnTracker viewSeqTracker) {
        return viewSeqTracker.getWriterTxn() < viewSeqTracker.getSeqTxn();
    }

    // Takes the view latch, waiting while another holder has it. Every other holder of an invalid view's latch is
    // short, so RESUME polls the latch within the statement timeout instead of refusing a busy latch at once, and
    // refuses with "retry later" only when the timeout expires first. A cancelled statement stops.
    private static void lockView(
            MatViewState viewState,
            SqlExecutionCircuitBreaker circuitBreaker,
            TableToken viewToken,
            int viewNamePosition
    ) throws SqlException {
        if (viewState.tryLock()) {
            return;
        }
        viewState.runResumeLatchBusySeamForTesting();
        while (!viewState.tryLock()) {
            try {
                circuitBreaker.statefulThrowExceptionIfTrippedNoThrottle();
            } catch (CairoException e) {
                if (e.getInterruptionReason() == SqlExecutionCircuitBreaker.STATE_TIMEOUT) {
                    throw SqlException.$(viewNamePosition, "materialized view is busy, retry later [view=")
                            .put(viewToken.getTableName()).put(']');
                }
                throw e;
            }
            Os.sleep(1);
        }
    }

    // The release wakes any invalidation, FULL refresh or incremental refresh that published its intent while losing
    // the latch. The statement's outcome is decided by then, and the store's retry flags keep the work that could not
    // be enqueued discoverable, so a failure is logged, as for REFRESH ... STATS.
    private static void releaseLatch(CairoEngine engine, MatViewStateStore stateStore, TableToken viewToken, MatViewState viewState) {
        try {
            MatViewRefreshJob.finalizeAndUnlock(engine, stateStore, viewToken, viewState, false);
        } catch (Throwable th) {
            LOG.error().$("could not deliver deferred materialized view work after resume [view=").$(viewToken)
                    .$(", ex=").$(th)
                    .I$();
        }
    }

    // Runs under the view latch and releases it. Returns false when a view WAL transaction arrived after the wait, so
    // the caller waits again, and true when the statement is done.
    private static boolean resumeLocked(
            CairoEngine engine,
            SqlExecutionContext executionContext,
            TableToken viewToken,
            MatViewState viewState,
            SeqTxnTracker viewSeqTracker,
            int viewNamePosition,
            MatViewSqlValidator validator
    ) throws SqlException {
        final MatViewStateStore stateStore = engine.getMatViewStateStore();
        boolean isResumed = false;
        // From here on the view counts as recovering: an invalidation that arrives while RESUME holds the latch is
        // kept, so that it invalidates the view again once RESUME has made it valid.
        viewState.setResuming(true);
        try {
            viewState.runResumeHoldingLockSeamForTesting();
            if (viewState.isPendingFullRefresh()) {
                throw fullRefreshPending(viewToken, viewNamePosition);
            }
            if (viewState.isDropped()) {
                throw SqlException.matViewDoesNotExist(viewNamePosition, viewToken.getTableName());
            }
            if (!viewState.isInvalid()) {
                LOG.info().$("materialized view became valid, nothing to resume [view=").$(viewToken).I$();
                return true;
            }
            if (hasUnappliedViewWal(viewSeqTracker)) {
                LOG.info().$("materialized view WAL received a transaction, waiting for it again [view=").$(viewToken).I$();
                return false;
            }
            final MatViewDefinition viewDefinition = viewState.getViewDefinition();
            try (WalWriter walWriter = engine.getWalWriter(viewToken)) {
                final TableRecordMetadata viewMetadata = walWriter.getMetadata();
                validateSql(engine, executionContext, viewToken, viewState, viewDefinition, viewMetadata, viewNamePosition, validator);

                // The table under the base table's name now; the view follows it, which may be a new table.
                final TableToken baseTableToken = engine.getTableTokenIfExists(viewDefinition.getBaseTableName());
                if (baseTableToken == null) {
                    throw SqlException.$(viewNamePosition, "base table does not exist [view=").put(viewToken.getTableName())
                            .put(", baseTable=").put(viewDefinition.getBaseTableName()).put(']');
                }
                CharSequence refusal = MatViewRefreshJob.getBaseTableKindRefusal(viewToken, baseTableToken);
                if (refusal != null) {
                    throw SqlException.$(viewNamePosition, refusal);
                }
                // The base table's latest applied transaction, from a reader: a position the next refresh's reader
                // cannot see would fail that refresh and invalidate the view again.
                final long baseTableTxn;
                try (TableReader baseTableReader = engine.getReader(baseTableToken)) {
                    refusal = MatViewRefreshJob.getBaseTableTimestampRefusal(
                            viewToken,
                            baseTableToken,
                            viewMetadata.getTimestampType(),
                            baseTableReader.getMetadata().getTimestampType()
                    );
                    if (refusal != null) {
                        throw SqlException.$(viewNamePosition, refusal);
                    }
                    baseTableTxn = baseTableReader.getSeqTxn();
                }

                // Jump. A view that never completed a refresh keeps its position, so its next refresh does the
                // initial build. A period view's last refreshed period moves to the current period boundary.
                final boolean isNeverRefreshed = viewState.getLastRefreshBaseTxn() == -1;
                final long refreshBaseTxn = isNeverRefreshed ? -1 : baseTableTxn;
                long periodHi = viewState.getLastPeriodHi();
                if (!isNeverRefreshed && viewDefinition.getPeriodLength() > 0) {
                    final long now = viewDefinition.getBaseTableTimestampDriver().getTicks();
                    // lastPeriodHi is exclusive, the complete period hi inclusive
                    periodHi = MatViewRefreshJob.getCompletePeriodHi(viewDefinition, now) + 1;
                }
                MatViewCommitFence.resetMatViewState(
                        engine,
                        walWriter,
                        refreshBaseTxn,
                        viewState.getLastRefreshFinishTimestampUs(),
                        false,
                        null,
                        periodHi,
                        null,
                        -1,
                        baseTableToken.getTableId()
                );
                viewState.resumeSuccess(refreshBaseTxn, periodHi, baseTableToken.getTableId(), MODE_SKIP_BACKLOG);
                isResumed = true;
                LOG.info().$("resumed materialized view [view=").$(viewToken)
                        .$(", baseTable=").$(baseTableToken)
                        .$(", refreshBaseTxn=").$(refreshBaseTxn)
                        .$(", periodHi=").$ts(viewDefinition.getBaseTableTimestampDriver(), periodHi)
                        .$(", mode=").$(MODE_SKIP_BACKLOG)
                        .I$();
            }
        } finally {
            if (!isResumed && viewState.isInvalid()) {
                // The view stays invalid, so the invalidations published while RESUME held the latch have nothing to
                // invalidate, as for any invalidation that reaches an invalid view.
                viewState.clearPendingInvalidationReason();
            }
            viewState.setResuming(false);
            releaseLatch(engine, stateStore, viewToken, viewState);
        }

        if (viewState.getViewDefinition().getRefreshType() == MatViewDefinition.REFRESH_TYPE_IMMEDIATE) {
            try {
                stateStore.enqueueIncrementalRefresh(viewToken);
            } catch (Throwable th) {
                LOG.error().$("could not enqueue incremental refresh after resume [view=").$(viewToken)
                        .$(", ex=").$(th)
                        .I$();
            }
        }
        return true;
    }

    // Compiles the view's SQL under the caller's security context, reading every bare name as a column, and checks
    // the column mapping and casts as refresh builds them. An error keeps the view invalid until ALTER MATERIALIZED
    // VIEW ... AS fixes its SQL: a SQL that reads a dropped column, or calls a zero-argument function without
    // parentheses, fails here.
    private static void validateSql(
            CairoEngine engine,
            SqlExecutionContext executionContext,
            TableToken viewToken,
            MatViewState viewState,
            MatViewDefinition viewDefinition,
            TableRecordMetadata viewMetadata,
            int viewNamePosition,
            MatViewSqlValidator validator
    ) throws SqlException {
        viewState.setColumnMappingSummary(null);
        validator.validate(viewDefinition, true, true, executionContext);
        if (!validator.isCompiled()) {
            throw SqlException.$(viewNamePosition, "materialized view SQL does not compile, fix it with ALTER MATERIALIZED VIEW ... AS first [view=")
                    .put(viewToken.getTableName())
                    .put(", error=").put(validator.getError())
                    .put(']');
        }
        final RecordMetadata queryMetadata = validator.getQueryMetadata();
        final boolean isPositional = validator.hasGeneratedColumnNames();
        try {
            if (isPositional) {
                MatViewRefreshJob.validateViewSchema(viewToken, queryMetadata, viewMetadata, false);
            }
            try (SqlCompiler compiler = engine.getSqlCompiler()) {
                MatViewColumnMapping.newRecordToRowCopier(
                        compiler.getAsm(),
                        queryMetadata,
                        viewMetadata,
                        isPositional,
                        engine.getConfiguration()
                );
            }
        } catch (CairoException e) {
            // the mapping and cast checks, which read no table, fail with a mismatch only
            throw SqlException.$(viewNamePosition, "materialized view SQL does not match the view's columns, fix it with ALTER MATERIALIZED VIEW ... AS first [view=")
                    .put(viewToken.getTableName())
                    .put(", error=").put(e.getFlyweightMessage())
                    .put(']');
        }
        viewState.setColumnMappingSummary(MatViewColumnMapping.summarize(queryMetadata, viewMetadata, isPositional));
    }
}
