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
import io.questdb.griffin.SqlExecutionSuspension;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.mp.continuation.Fiber;
import io.questdb.mp.continuation.FiberWaitCoordinator;
import io.questdb.std.Chars;
import io.questdb.std.LongList;
import io.questdb.std.LowerCaseCharSequenceHashSet;
import io.questdb.std.Numbers;
import io.questdb.std.ObjList;
import io.questdb.std.Os;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * {@code REFRESH MATERIALIZED VIEW v RESUME [SKIP BACKLOG]}: makes an invalid view valid again and keeps its rows.
 * <p>
 * By default it catches up: it keeps the view's refresh position and its backlog, the base table changes the view
 * recorded in its refresh-interval cache while it was invalid (see {@link MatViewBacklog}), and the view's next
 * incremental refresh recomputes them. When the backlog is lost, it refuses and names the cause, so a gap in the view
 * is always one the user accepted. With {@code SKIP BACKLOG} it jumps: it sets the view's refresh position to the base
 * table's latest applied transaction, clears the view's cached refresh intervals and recomputes nothing for the gap
 * between the old position and the new one, which the user backfills with {@code REFRESH ... RANGE}. A view that never
 * completed a refresh has no backlog and keeps its "never refreshed" position either way, so its next refresh does the
 * initial build.
 * <p>
 * The statement is synchronous:
 * <ol>
 *     <li>it does nothing on a valid view;</li>
 *     <li>it waits until the view's own WAL is applied, without holding the view latch and within the statement
 *     timeout, so that it can follow a view column change directly, and refuses a view whose WAL is suspended;</li>
 *     <li>it refuses at once while a FULL refresh is pending, which recovers the view itself; otherwise it takes the
 *     view latch, polling it within the statement timeout while another holder has it, and refuses with "retry
 *     later" when the timeout expires first, or when a role switch waits for the role-switch read lock that the
 *     statement holds. Under the latch it refuses a FULL refresh that became pending meanwhile, and goes back to the
 *     wait when a view WAL transaction arrived after it;</li>
 *     <li>it compiles the view's SQL under the caller's security context, reading every bare name as a column, and
 *     checks the column mapping and casts;</li>
 *     <li>it resolves the base table by name, which must be a WAL table or a materialized view with the view's
 *     designated timestamp type, and reads its latest applied transaction from a reader;</li>
 *     <li>it refuses a view that completed a refresh from that table and whose SQL uses bare a name that a base column
 *     change switched between a column and a call since the view's rows were computed, see
 *     {@link MatViewBareNameGuard}: a switch that the guard recorded, unless a FULL refresh rebuilt the view from a
 *     base snapshot that includes it, or the guard's invalidation that the view's state keeps;</li>
 *     <li>without SKIP BACKLOG, it brings the view's backlog up to that transaction, which checks every condition
 *     that loses the backlog, and refuses a lost backlog with its cause;</li>
 *     <li>it persists the new state, which carries the base table id, through the view's WAL under the role-switch
 *     commit fence, and updates the view's in-memory state;</li>
 *     <li>it releases the latch through {@link MatViewRefreshJob#finalizeAndUnlock}, which re-drives an invalidation
 *     published while it held the latch, and enqueues an incremental refresh for an IMMEDIATE view. A failure of
 *     either is logged and does not fail the statement.</li>
 * </ol>
 * While it holds the latch, the view counts as recovering, see {@link MatViewState#isResuming()}: an invalidation
 * that arrives meanwhile is kept, and invalidates the view again once RESUME has made it valid. A RESUME that refuses
 * drops those invalidations, since the view stays invalid. An invalidation that was already pending when RESUME took
 * the latch, which a FULL refresh or another refresh that failed and left the view invalid did not consume, is
 * dropped once RESUME has made the view valid, as long as RESUME read past it: RESUME resolved the base table and
 * compiled the view's SQL after it, unless it stands for a base table change that applied after the compile.
 */
public final class MatViewResume {
    public static final String MODE_CATCH_UP = "catch-up";
    public static final String MODE_SKIP_BACKLOG = "skip backlog";
    private static final Log LOG = LogFactory.getLog(MatViewResume.class);

    private MatViewResume() {
    }

    /**
     * Resumes the view; see the class description.
     *
     * @param viewNamePosition the position of the view name in the statement, where refusals point
     * @param isSkipBacklog    true for RESUME SKIP BACKLOG, which jumps over the view's backlog
     * @throws SqlException   a refusal
     * @throws CairoException an authorization error, a read-only refusal, a cancellation or a timeout of the wait
     *                        for the view's WAL
     */
    public static void resume(
            @NotNull CairoEngine engine,
            @NotNull SqlExecutionContext executionContext,
            @NotNull TableToken viewToken,
            int viewNamePosition,
            @NotNull MatViewSqlValidator validator,
            boolean isSkipBacklog
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
            awaitViewWal(engine, viewState, viewSeqTracker, circuitBreaker, viewToken, viewNamePosition);
            viewState.runResumeWalAppliedSeamForTesting();
            if (viewState.isPendingFullRefresh()) {
                throw fullRefreshPending(viewToken, viewNamePosition);
            }
            lockView(engine, viewState, circuitBreaker, viewToken, viewNamePosition);
            if (resumeLocked(engine, executionContext, viewToken, viewState, viewSeqTracker, viewNamePosition, validator, isSkipBacklog)) {
                return;
            }
            // a view WAL transaction arrived after the wait, which the latch was released for
        }
    }

    private static void awaitViewWal(
            CairoEngine engine,
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
            pauseWait(engine);
        }
    }

    private static SqlException backlogLost(TableToken viewToken, int viewNamePosition, CharSequence cause) {
        return SqlException.$(viewNamePosition, "materialized view backlog is lost [reason=").put(cause)
                .put("]; RESUME cannot catch up on base changes made while the view was invalid. Run REFRESH MATERIALIZED VIEW ")
                .put(viewToken.getTableName())
                .put(" RESUME SKIP BACKLOG, then REFRESH ... RANGE over the gap");
    }

    private static SqlException busy(TableToken viewToken, int viewNamePosition) {
        return SqlException.$(viewNamePosition, "materialized view is busy, retry later [view=")
                .put(viewToken.getTableName()).put(']');
    }

    // Consumes the invalidation that was pending when RESUME took the latch, once RESUME has made the view valid, when
    // RESUME read past it. Such an invalidation reached the view while it was invalid: under a FULL refresh that then
    // failed, under an earlier pass of RESUME that waited again for the view's WAL, or while another refresh that then
    // failed held the latch. RESUME resolved the base table by name and compiled the view's SQL after its publication,
    // so an invalidation without a base table change, such as a base table drop, says nothing that RESUME did not
    // see, as for an invalidation published before a FULL refresh fixes its base reader. The bare-name guard publishes
    // an invalidation for a base table change before the change applies, so RESUME read past it only when the change
    // belongs to a table the view no longer follows, or had applied when RESUME took the latch, before its compile; a
    // change that switches a name the SQL still uses bare was refused before, by findSwitchedBareName(). Otherwise the
    // change may switch a bare name after RESUME's compile, and the invalidation stays pending: the release of the
    // latch invalidates the view again. An invalidation published from the latch on, which minted another marker,
    // stays pending too.
    private static void consumePriorInvalidation(
            TableToken viewToken,
            MatViewState viewState,
            Object priorMarker,
            TableToken baseTableToken,
            int snapshotBaseTableId,
            long snapshotBaseTableTxn
    ) {
        final String reason = viewState.getPendingInvalidationReason(priorMarker);
        if (reason == null) {
            return;
        }
        if (viewState.hasPendingInvalidationBaseTableChange(priorMarker)) {
            final TableToken changedTableToken = viewState.getPendingInvalidationBaseTableToken(priorMarker);
            // a merge with another publication lost the change's table and txn, so RESUME cannot tell that it saw it
            if (changedTableToken == null) {
                return;
            }
            // A change of the base table that RESUME positions the view on is one its compile saw when the table had
            // the base table's name and had applied it when RESUME took the latch.
            final int changedTableId = changedTableToken.getTableId();
            if (changedTableId == baseTableToken.getTableId()
                    && (changedTableId != snapshotBaseTableId
                    || viewState.getPendingInvalidationBaseTxn(priorMarker) > snapshotBaseTableTxn)) {
                return;
            }
        }
        if (viewState.clearPendingInvalidation(priorMarker)) {
            LOG.info().$("materialized view resumed past an earlier invalidation, dropping it [view=").$(viewToken)
                    .$(", reason=").$safe(reason)
                    .I$();
        }
    }

    // The cause of a lost backlog that RESUME SKIP BACKLOG reports in its telemetry event without a WAL scan: the
    // marker, or a condition that the view's state and the base table give away.
    private static @Nullable String findKnownLostCause(MatViewState viewState, TableToken baseTableToken, long baseTableTxn) {
        final String cause = viewState.getBacklogLostReason();
        return cause != null ? cause : MatViewBacklog.findLostCause(viewState, baseTableToken, baseTableTxn);
    }

    // Returns the bare select-list name of the SQL that RESUME compiled, as the SQL writes it, whose meaning a column
    // change of the base table that RESUME positions the view on switched after the view's rows were computed, or
    // null. Only a view that completed a refresh keeps rows that its next refresh does not recompute, and RESUME judges
    // only rows of that base table. A switch counts when the bare-name guard recorded it, unless the last FULL refresh
    // rebuilt the view from a snapshot that includes it: no other refresh rewrites every row, and RESUME SKIP BACKLOG
    // moves the refresh position without computing a row. The guard's invalidation of a valid view counts too: the
    // view's state keeps it across a restart, which loses the recorded switches, until a FULL refresh succeeds.
    private static @Nullable CharSequence findSwitchedBareName(
            MatViewState viewState,
            ObjList<MatViewState.BareNameSwitch> bareNameSwitches,
            LowerCaseCharSequenceHashSet bareSelectNames,
            int baseTableId
    ) {
        if (viewState.getLastRefreshBaseTxn() == -1) {
            return null;
        }
        final int viewBaseTableId = viewState.getBaseTableId();
        if (viewBaseTableId != MatViewState.BASE_TABLE_ID_UNKNOWN && viewBaseTableId != baseTableId) {
            return null;
        }
        for (int i = 0, n = bareNameSwitches.size(); i < n; i++) {
            final MatViewState.BareNameSwitch bareNameSwitch = bareNameSwitches.getQuick(i);
            if (bareNameSwitch.baseTableId == baseTableId) {
                final int keyIndex = bareSelectNames.keyIndex(bareNameSwitch.name);
                if (keyIndex < 0 && !viewState.isCoveredByLastFullRefresh(baseTableId, bareNameSwitch.baseTableTxn)) {
                    return bareSelectNames.keyAt(keyIndex);
                }
            }
        }
        final String invalidationReason = viewState.getInvalidationReason();
        for (int i = 0, n = bareSelectNames.getKeyCount(); i < n; i++) {
            final CharSequence bareName = bareSelectNames.getKey(i);
            if (bareName != null && Chars.equalsIgnoreCaseNc(MatViewBareNameGuard.invalidationReason(bareName), invalidationReason)) {
                return bareName;
            }
        }
        return null;
    }

    private static SqlException fullRefreshPending(TableToken viewToken, int viewNamePosition) {
        return SqlException.$(viewNamePosition, "materialized view full refresh is pending, it recovers the view [view=")
                .put(viewToken.getTableName()).put(']');
    }

    // The last sequencer txn that the WAL table has applied, a lower bound of what a compile that follows the call
    // sees: below every txn for a table whose WAL apply this process has not tracked yet, and LONG_NULL for a missing
    // table and for a table without a WAL, which RESUME refuses as a base table.
    private static long getAppliedTxn(CairoEngine engine, @Nullable TableToken tableToken) {
        return tableToken != null && tableToken.isWal()
                ? engine.getTableSequencerAPI().getTxnTracker(tableToken).getWriterTxn()
                : Numbers.LONG_NULL;
    }

    private static boolean hasUnappliedViewWal(SeqTxnTracker viewSeqTracker) {
        return viewSeqTracker.getWriterTxn() < viewSeqTracker.getSeqTxn();
    }

    // Returns false when the fiber cannot park; throws when its runtime or the engine's timers shut down.
    private static boolean parkOnTimer(CairoEngine engine, Fiber fiber) {
        final long token = fiber.tryBeginWaitBuild(1);
        if (token == Fiber.TOKEN_REFUSED) {
            throw serverClosing();
        }
        final FiberWaitCoordinator coordinator = fiber.getWaitCoordinator();
        try {
            if (!coordinator.armTimer(token, engine.getTimerShards(), engine.getConfiguration().getMillisecondClock(), 1)) {
                throw serverClosing();
            }
            final int reason = fiber.suspendWait(token, FiberWaitCoordinator.REASON_NONE);
            if (reason == FiberWaitCoordinator.REASON_SHUTDOWN) {
                throw serverClosing();
            }
            return reason != FiberWaitCoordinator.REASON_NONE;
        } finally {
            coordinator.teardownWait(token);
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
            MatViewSqlValidator validator,
            boolean isSkipBacklog
    ) throws SqlException {
        final MatViewStateStore stateStore = engine.getMatViewStateStore();
        // The invalidation pending when RESUME takes the latch, which a resumed view consumes when RESUME read past it,
        // see consumePriorInvalidation(). Each publication mints a new marker, so this identity never stands for an
        // invalidation published from here on.
        final Object priorMarker = viewState.getPendingInvalidationMarker();
        // The table under the base table's name and the transaction it has applied by now, which RESUME's compile sees
        // at least: a base table change of that table up to the transaction is one the compile saw.
        final TableToken snapshotBaseTableToken = engine.getTableTokenIfExists(viewState.getViewDefinition().getBaseTableName());
        final int snapshotBaseTableId = snapshotBaseTableToken != null ? snapshotBaseTableToken.getTableId() : MatViewState.BASE_TABLE_ID_UNKNOWN;
        final long snapshotBaseTableTxn = getAppliedTxn(engine, snapshotBaseTableToken);
        boolean isResumed = false;
        // Set when a view WAL transaction arrived after the wait: the statement waits again and takes the latch again,
        // so the invalidations published meanwhile stay pending for it.
        boolean isWaitingAgain = false;
        // From here on the view counts as recovering: an invalidation that arrives while RESUME holds the latch is
        // kept, so that it invalidates the view again once RESUME has made it valid.
        viewState.setResuming(true);
        try {
            viewState.runResumeFlagSetSeamForTesting();
            // The bare-name switches recorded when RESUME takes the latch, see findSwitchedBareName(). The guard records
            // a switch before it reads the resuming flag, so the guard of a switch recorded from here on keeps its
            // invalidation pending, and the release of the latch invalidates the view again.
            final ObjList<MatViewState.BareNameSwitch> bareNameSwitches = viewState.getBareNameSwitches();
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
                isWaitingAgain = true;
                return false;
            }
            final MatViewDefinition viewDefinition = viewState.getViewDefinition();
            // Two concurrent base changes of ALTER MATERIALIZED VIEW ... WITH BASE can leave the view on a dependency
            // loop, which no refresh can follow.
            if (engine.getDependentViewGraph().isOnDependencyLoop(viewDefinition)) {
                throw SqlException.$(viewNamePosition, MatViewRefreshJob.getDependencyLoopRefusal(viewToken, viewDefinition.getBaseTableName()));
            }
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

                // The check writes no view state, so it comes before the backlog update, which can write the backlog.
                final CharSequence switchedBareName = findSwitchedBareName(
                        viewState,
                        bareNameSwitches,
                        validator.getBareSelectNames(),
                        baseTableToken.getTableId()
                );
                if (switchedBareName != null) {
                    throw SqlException.$(viewNamePosition, "materialized view SQL reads a bare name whose meaning a base column change switched, ")
                            .put("fix it with ALTER MATERIALIZED VIEW ... AS first [view=").put(viewToken.getTableName())
                            .put(", error=").put(MatViewBareNameGuard.invalidationReason(switchedBareName))
                            .put(']');
                }

                // A view that never completed a refresh has no backlog: it keeps its position, so its next refresh
                // does the initial build, and RESUME neither refuses it nor needs SKIP BACKLOG for it.
                final boolean isNeverRefreshed = viewState.getLastRefreshBaseTxn() == -1;
                final boolean isCatchUp = !isSkipBacklog;
                final CharSequence mode;
                if (isSkipBacklog) {
                    final String lostCause = isNeverRefreshed ? null : findKnownLostCause(viewState, baseTableToken, baseTableTxn);
                    mode = lostCause == null ? MODE_SKIP_BACKLOG : MODE_SKIP_BACKLOG + " (lost: " + lostCause + ')';
                } else {
                    if (!isNeverRefreshed) {
                        // Bring the backlog up to the applied transaction. The update checks every condition that
                        // loses the backlog, so a backlog whose base WAL was purged is found lost here, not at the
                        // next refresh, which would recompute the base table's whole min..max range instead.
                        updateBacklog(engine, viewState, walWriter, baseTableToken, baseTableTxn);
                        final String lostCause = viewState.getBacklogLostReason();
                        if (lostCause != null) {
                            throw backlogLost(viewToken, viewNamePosition, lostCause);
                        }
                    }
                    mode = MODE_CATCH_UP;
                }

                // Catching up keeps the refresh position, a period view's last refreshed period and the cached
                // refresh intervals. Jumping moves the refresh position to the applied transaction and a period
                // view's last refreshed period to the current period boundary, and clears the cache.
                final long refreshBaseTxn = isNeverRefreshed || isCatchUp ? viewState.getLastRefreshBaseTxn() : baseTableTxn;
                long periodHi = viewState.getLastPeriodHi();
                if (!isNeverRefreshed && !isCatchUp && viewDefinition.getPeriodLength() > 0) {
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
                        isCatchUp ? viewState.getRefreshIntervals() : null,
                        isCatchUp ? viewState.getRefreshIntervalsBaseTxn() : -1,
                        baseTableToken.getTableId(),
                        null
                );
                viewState.resumeSuccess(refreshBaseTxn, periodHi, baseTableToken.getTableId(), isCatchUp, mode);
                isResumed = true;
                consumePriorInvalidation(viewToken, viewState, priorMarker, baseTableToken, snapshotBaseTableId, snapshotBaseTableTxn);
                LOG.info().$("resumed materialized view [view=").$(viewToken)
                        .$(", baseTable=").$(baseTableToken)
                        .$(", refreshBaseTxn=").$(refreshBaseTxn)
                        .$(", periodHi=").$ts(viewDefinition.getBaseTableTimestampDriver(), periodHi)
                        .$(", mode=").$(mode)
                        .I$();
            }
        } finally {
            if (!isResumed && !isWaitingAgain && viewState.isInvalid()) {
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

    // A parked statement aborts when the server shuts its fiber runtime or the engine's timers down, as PerWorkerLocks
    // does, so that the shutdown does not wait for the statement. Every wait comes before the statement's writes.
    private static CairoException serverClosing() {
        return CairoException.nonCritical().put("query aborted, server is closing").setInterruption(true);
    }

    // Brings the view's backlog up to the base table's applied transaction under the latch, see MatViewBacklog. A
    // failure loses the backlog, as for the refresh job's cache update, except for a read-only refusal, which fails the
    // statement. A refusal writes no view state, so a lost backlog keeps its marker in memory: the view's next view
    // state event persists it, and after a restart the next cache update finds the backlog lost again.
    private static void updateBacklog(
            CairoEngine engine,
            MatViewState viewState,
            WalWriter walWriter,
            TableToken baseTableToken,
            long baseTableTxn
    ) {
        try (WalTxnRangeLoader txnRangeLoader = new WalTxnRangeLoader(engine.getConfiguration())) {
            try {
                MatViewBacklog.update(
                        engine,
                        txnRangeLoader,
                        new LongList(),
                        viewState,
                        walWriter,
                        baseTableToken,
                        baseTableTxn,
                        baseTableTxn,
                        false
                );
            } catch (CairoException e) {
                if (e.isAuthorizationError()) {
                    throw e;
                }
                LOG.error().$("could not update materialized view backlog for resume [view=").$(viewState.getViewDefinition().getMatViewToken())
                        .$(", ex=").$((Throwable) e)
                        .I$();
                MatViewBacklog.markLost(engine, null, viewState, MatViewBacklog.LOST_UPDATE_FAILED);
            }
        }
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

    // Takes the view latch, waiting while another holder has it. Every other holder of an invalid view's latch is
    // short, so RESUME polls the latch within the statement timeout instead of refusing a busy latch at once, and
    // refuses with "retry later" only when the timeout expires first. A cancelled statement stops. The base change of
    // ALTER MATERIALIZED VIEW ... WITH BASE takes the latch the same way, see MatViewAlter.
    //
    // pg-wire re-executes a prepared statement under the role-switch read lock (PGPipelineEntry.executeFenced()). A
    // latch holder that commits takes that read lock at the commit fence on its own thread, which waits while a role
    // switch, such as a demote, waits for the write lock, and the role switch waits for the statement's read hold. So
    // the statement stops waiting with "retry later" as soon as a role switch waits for its read hold: neither the
    // role switch nor the fenced commits queued behind it wait for the statement's timeout. Under that read hold the
    // statement sleeps between its polls, since its fiber must not park there, see pauseWait().
    static void lockView(
            CairoEngine engine,
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
            if (engine.isRoleSwitchWaitingForCurrentReadHold()) {
                throw busy(viewToken, viewNamePosition);
            }
            try {
                circuitBreaker.statefulThrowExceptionIfTrippedNoThrottle();
            } catch (CairoException e) {
                if (e.getInterruptionReason() == SqlExecutionCircuitBreaker.STATE_TIMEOUT) {
                    throw busy(viewToken, viewNamePosition);
                }
                throw e;
            }
            pauseWait(engine);
        }
    }

    // Pauses a wait loop of RESUME, ALTER ... WITH BASE or REFRESH for about a millisecond; the loop then checks its
    // condition and the statement's circuit breaker again. A statement that runs on a mounted fiber, as pg-wire, HTTP
    // and QWP run one, parks the fiber on a timer, which leaves the network worker to other connections. Off a fiber,
    // and on a fiber that holds the role-switch read lock (pg-wire's PGPipelineEntry.executeFenced() runs a
    // re-executed prepared statement so), where a fiber must not park, or when the fiber cannot park, it sleeps.
    static void pauseWait(CairoEngine engine) {
        final Fiber fiber = SqlExecutionSuspension.currentFiber();
        if (fiber == null || !parkOnTimer(engine, fiber)) {
            Os.sleep(1);
        }
    }
}
