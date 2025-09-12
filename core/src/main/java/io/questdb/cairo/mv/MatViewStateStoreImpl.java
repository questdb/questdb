/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2024 QuestDB
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

import io.questdb.Telemetry;
import io.questdb.cairo.CairoEngine;
import io.questdb.cairo.CairoException;
import io.questdb.cairo.TableToken;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.mp.ConcurrentQueue;
import io.questdb.mp.Queue;
import io.questdb.std.ConcurrentHashMap;
import io.questdb.std.Misc;
import io.questdb.std.Numbers;
import io.questdb.std.ThreadLocal;
import io.questdb.std.datetime.MicrosecondClock;
import io.questdb.tasks.TelemetryMatViewTask;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.jetbrains.annotations.TestOnly;

import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;

public class MatViewStateStoreImpl implements MatViewStateStore {
    private static final Log LOG = LogFactory.getLog(MatViewStateStoreImpl.class);
    private static final ThreadLocal<MatViewTimerTask> tlTimerTask = new ThreadLocal<>(MatViewTimerTask::new);
    private final Function<CharSequence, AtomicLong> createLastNotifiedTxn;
    // Table name to last notified base table txn.
    // Flips to negative value once a refresh message is processed. Long.MIN_VALUE stands for "just invalidated" state.
    // The goal is to avoid sending excessive incremental refresh messages to the underlying queue.
    // Note: this map is grow-only, i.e. keys are never removed.
    private final ConcurrentHashMap<AtomicLong> lastNotifiedTxnByTableName = new ConcurrentHashMap<>(false);
    private final MicrosecondClock microsecondClock;
    private final ConcurrentHashMap<MatViewState> stateByTableDirName = new ConcurrentHashMap<>();
    private final ThreadLocal<MatViewRefreshTask> taskHolder = new ThreadLocal<>(MatViewRefreshTask::new);
    private final Queue<MatViewRefreshTask> taskQueue = ConcurrentQueue.createConcurrentQueue(MatViewRefreshTask::new);
    private final Telemetry<TelemetryMatViewTask> telemetry;
    private final MatViewTelemetryFacade telemetryFacade;
    private final Queue<MatViewTimerTask> timerTaskQueue;

    public MatViewStateStoreImpl(CairoEngine engine) {
        this.telemetry = engine.getTelemetryMatView();
        this.telemetryFacade = telemetry.isEnabled()
                ? this::storeMatViewTelemetry
                : (event, tableToken, baseTableTxn, errorMessage, latencyUs) -> { /* no-op */ };
        this.timerTaskQueue = engine.getMatViewTimerQueue();
        this.microsecondClock = engine.getConfiguration().getMicrosecondClock();
        this.createLastNotifiedTxn = name -> new AtomicLong();
    }

    // kept public for tests
    public static boolean notifyBaseTableCommit(AtomicLong lastNotifiedBaseTableTxn, long seqTxn) {
        long lastNotified;
        boolean retry;
        do {
            lastNotified = lastNotifiedBaseTableTxn.get();
            retry = Math.abs(lastNotified) < seqTxn && !lastNotifiedBaseTableTxn.compareAndSet(lastNotified, seqTxn);
        } while (retry);
        // The job is allowed to send a notification once incremental refresh finishes.
        return lastNotified <= 0;
    }

    // kept public for tests
    public static boolean notifyOnBaseTableRefreshed(AtomicLong lastNotifiedTxn, long seqTxn) {
        // Flip the sign bit in the last notified base table txn number.
        lastNotifiedTxn.compareAndSet(seqTxn, -seqTxn);
        long lastNotified = lastNotifiedTxn.get();
        // No need to notify if the view was just invalidated or there is no newer txn.
        return lastNotified != Long.MIN_VALUE && lastNotified != -seqTxn;
    }

    // kept public for tests
    @Override
    public MatViewState addViewState(MatViewDefinition viewDefinition) {
        final TableToken viewToken = viewDefinition.getMatViewToken();
        final MatViewState state = new MatViewState(viewDefinition, telemetryFacade);

        final MatViewState prevState = stateByTableDirName.putIfAbsent(viewToken.getDirName(), state);
        // WAL table directories are unique, so we don't expect previous value
        if (prevState != null) {
            Misc.free(state);
            throw CairoException.critical(0).put("materialized view state already exists [dir=").put(viewToken.getDirName());
        }

        lastNotifiedTxnByTableName.computeIfAbsent(viewDefinition.getBaseTableName(), createLastNotifiedTxn);

        // Publish a timer creation task.
        // We need timer(s) in all cases, but immediate non-period view.
        if (viewDefinition.getRefreshType() != MatViewDefinition.REFRESH_TYPE_IMMEDIATE || viewDefinition.getPeriodLength() > 0) {
            final MatViewTimerTask timerTask = tlTimerTask.get();
            timerTaskQueue.enqueue(timerTask.ofAdd(viewToken));
        }

        return state;
    }

    @TestOnly
    public void clear() {
        close();
        taskQueue.clear();
        stateByTableDirName.clear();
        lastNotifiedTxnByTableName.clear();
    }

    @Override
    public void close() {
        for (MatViewState state : stateByTableDirName.values()) {
            Misc.free(state);
        }
    }

    @Override
    public void createViewState(MatViewDefinition viewDefinition) {
        addViewState(viewDefinition).init();
    }

    @Override
    public void enqueueFullRefresh(TableToken matViewToken) {
        enqueueTaskIfStateExists(matViewToken, MatViewRefreshTask.FULL_REFRESH, null);
    }

    @Override
    public void enqueueIncrementalRefresh(TableToken matViewToken) {
        enqueueTaskIfStateExists(matViewToken, MatViewRefreshTask.INCREMENTAL_REFRESH, null);
    }

    @Override
    public void enqueueInvalidate(TableToken matViewToken, String invalidationReason) {
        enqueueTaskIfStateExists(matViewToken, MatViewRefreshTask.INVALIDATE, invalidationReason);
    }

    @Override
    public void enqueueInvalidateDependentViews(TableToken baseTableToken, String invalidationReason) {
        enqueueMatViewTask(
                null,
                baseTableToken,
                MatViewRefreshTask.INVALIDATE,
                invalidationReason,
                Numbers.LONG_NULL,
                Numbers.LONG_NULL
        );
    }

    @Override
    public void enqueueRangeRefresh(TableToken matViewToken, long rangeFrom, long rangeTo) {
        enqueueTaskIfStateExists(matViewToken, MatViewRefreshTask.RANGE_REFRESH, null, rangeFrom, rangeTo);
    }

    public void enqueueTaskIfStateExists(
            TableToken matViewToken,
            int operation,
            String invalidationReason,
            long rangeFrom,
            long rangeTo
    ) {
        final MatViewState state = stateByTableDirName.get(matViewToken.getDirName());
        if (state != null && !state.isDropped()) {
            enqueueMatViewTask(matViewToken, null, operation, invalidationReason, rangeFrom, rangeTo);
        }
    }

    @Override
    public void enqueueUpdateRefreshIntervals(TableToken matViewToken) {
        enqueueTaskIfStateExists(matViewToken, MatViewRefreshTask.UPDATE_REFRESH_INTERVALS, null);
    }

    @Override
    public MatViewState getViewState(TableToken matViewToken) {
        final MatViewState state = stateByTableDirName.get(matViewToken.getDirName());
        if (state != null) {
            if (state.isDropped()) {
                // Housekeeping
                stateByTableDirName.remove(matViewToken.getDirName(), state);
            }
            return state;
        }
        return null;
    }

    @Override
    public void notifyBaseInvalidated(TableToken baseTableToken) {
        final AtomicLong lastNotifiedTxn = lastNotifiedTxnByTableName.get(baseTableToken.getTableName());
        if (lastNotifiedTxn != null) {
            lastNotifiedTxn.set(Long.MIN_VALUE);
        }
    }

    @Override
    public void notifyBaseRefreshed(MatViewRefreshTask task, long seqTxn) {
        final AtomicLong lastNotifiedTxn = lastNotifiedTxnByTableName.get(task.baseTableToken.getTableName());
        if (lastNotifiedTxn != null && notifyOnBaseTableRefreshed(lastNotifiedTxn, seqTxn)) {
            // While refreshing more txn were committed. Refresh will need to re-run.
            task.refreshTriggerTimestamp = microsecondClock.getTicks();
            taskQueue.enqueue(task);
        }
    }

    @Override
    public void notifyBaseTableCommit(MatViewRefreshTask task, long seqTxn) {
        final TableToken baseTableToken = task.baseTableToken;
        final AtomicLong lastNotifiedBaseTableTxn = lastNotifiedTxnByTableName.get(baseTableToken.getTableName());
        task.refreshTriggerTimestamp = microsecondClock.getTicks();
        // Always notify refresh job in case of mat view invalidation or full refresh.
        if (task.operation != MatViewRefreshTask.INCREMENTAL_REFRESH) {
            taskQueue.enqueue(task);
            LOG.debug().$("notified [baseTable=").$(baseTableToken)
                    .$(", op=").$(MatViewRefreshTask.getRefreshOperationName(task.operation))
                    .I$();
        } else if (lastNotifiedBaseTableTxn != null) {
            // For incremental refresh we check if we haven't already notified on the given txn.
            if (notifyBaseTableCommit(lastNotifiedBaseTableTxn, seqTxn)) {
                taskQueue.enqueue(task);
                LOG.debug().$("notified [baseTable=").$(baseTableToken)
                        .$(", op=incremental_refresh")
                        .I$();
            } else {
                LOG.debug().$("no need to notify to refresh job [baseTable=").$(baseTableToken).I$();
            }
        }
    }

    @Override
    public void removeViewState(TableToken matViewToken) {
        final MatViewState state = stateByTableDirName.remove(matViewToken.getDirName());
        if (state != null) {
            state.markAsDropped();
            state.tryCloseIfDropped();
            // Make sure to remove all timers associated with the mat view.
            final MatViewTimerTask timerTask = tlTimerTask.get();
            timerTaskQueue.enqueue(timerTask.ofRemove(matViewToken));
        }
    }

    @Override
    public boolean tryDequeueRefreshTask(MatViewRefreshTask task) {
        return taskQueue.tryDequeue(task);
    }

    @Override
    public void updateViewDefinition(@NotNull TableToken matViewToken, @NotNull MatViewDefinition newDefinition) {
        final MatViewState state = stateByTableDirName.get(matViewToken.getDirName());
        if (state != null) {
            state.setViewDefinition(newDefinition);
            // Make sure to recreate all timers associated with the mat view.
            final MatViewTimerTask timerTask = tlTimerTask.get();
            timerTaskQueue.enqueue(timerTask.ofUpdate(matViewToken));
        }
    }

    private void enqueueMatViewTask(
            @Nullable TableToken matViewToken,
            @Nullable TableToken baseTableToken,
            int operation,
            String invalidationReason,
            long rangeFrom,
            long rangeTo
    ) {
        final MatViewRefreshTask task = taskHolder.get();
        task.clear();
        task.matViewToken = matViewToken;
        task.baseTableToken = baseTableToken;
        task.operation = operation;
        task.invalidationReason = invalidationReason;
        task.rangeFrom = rangeFrom;
        task.rangeTo = rangeTo;
        if (MatViewRefreshTask.isRefreshOperation(operation)) {
            task.refreshTriggerTimestamp = microsecondClock.getTicks();
        }
        taskQueue.enqueue(task);
    }

    private void enqueueTaskIfStateExists(TableToken matViewToken, int operation, String invalidationReason) {
        enqueueTaskIfStateExists(matViewToken, operation, invalidationReason, Numbers.LONG_NULL, Numbers.LONG_NULL);
    }

    private void storeMatViewTelemetry(short event, TableToken tableToken, long baseTableTxn, CharSequence errorMessage, long latencyUs) {
        TelemetryMatViewTask.store(telemetry, event, tableToken.getTableId(), baseTableTxn, errorMessage, latencyUs);
    }
}
