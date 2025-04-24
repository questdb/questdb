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
import io.questdb.std.ThreadLocal;
import io.questdb.std.datetime.microtime.MicrosecondClock;
import io.questdb.tasks.TelemetryMatViewTask;
import org.jetbrains.annotations.TestOnly;

import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;

public class MatViewStateStoreImpl implements MatViewStateStore {
    private static final Log LOG = LogFactory.getLog(MatViewStateStoreImpl.class);
    private final Function<CharSequence, AtomicLong> createLastNotifiedTxn;
    // Table name to last notified base table txn.
    // Flips to negative value once a refresh message is processed. Long.MIN_VALUE stands for "just invalidated" state.
    // The goal is to avoid sending excessive incremental refresh messages to the underlying queue.
    // Note: this map is grow-only, i.e. keys are never removed.
    private final ConcurrentHashMap<AtomicLong> lastNotifiedTxnByTableName = new ConcurrentHashMap<>();
    private final MicrosecondClock microsecondClock;
    private final ConcurrentHashMap<MatViewState> stateByTableDirName = new ConcurrentHashMap<>();
    private final ThreadLocal<MatViewRefreshTask> taskHolder = new ThreadLocal<>(MatViewRefreshTask::new);
    private final Queue<MatViewRefreshTask> taskQueue = new ConcurrentQueue<>(MatViewRefreshTask::new);
    private final Telemetry<TelemetryMatViewTask> telemetry;
    private final MatViewTelemetryFacade telemetryFacade;

    public MatViewStateStoreImpl(CairoEngine engine) {
        this.telemetry = engine.getTelemetryMatView();
        this.telemetryFacade = telemetry.isEnabled()
                ? this::storeMatViewTelemetry
                : (event, tableToken, baseTableTxn, errorMessage, latencyUs) -> { /* no-op */ };
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

    @Override
    public MatViewState addViewState(MatViewDefinition viewDefinition) {
        final TableToken matViewToken = viewDefinition.getMatViewToken();
        final MatViewState state = new MatViewState(viewDefinition, telemetryFacade);

        final MatViewState prevState = stateByTableDirName.putIfAbsent(matViewToken.getDirName(), state);
        // WAL table directories are unique, so we don't expect previous value
        if (prevState != null) {
            Misc.free(state);
            throw CairoException.critical(0).put("materialized view state already exists [dir=").put(matViewToken.getDirName());
        }

        lastNotifiedTxnByTableName.computeIfAbsent(viewDefinition.getBaseTableName(), createLastNotifiedTxn);
        return state;
    }

    @TestOnly
    public void clear() {
        close();
    }

    @Override
    public void close() {
        for (MatViewState state : stateByTableDirName.values()) {
            Misc.free(state);
        }
        stateByTableDirName.clear();
        lastNotifiedTxnByTableName.clear();
    }

    @Override
    public void createViewState(MatViewDefinition viewDefinition) {
        addViewState(viewDefinition).init();
        enqueueMatViewTask(viewDefinition.getMatViewToken(), MatViewRefreshTask.INCREMENTAL_REFRESH, null);
    }

    @Override
    public void enqueueFullRefresh(TableToken matViewToken) {
        enqueueRefreshTaskIfStateExists(matViewToken, MatViewRefreshTask.FULL_REFRESH, null);
    }

    @Override
    public void enqueueIncrementalRefresh(TableToken matViewToken) {
        enqueueRefreshTaskIfStateExists(matViewToken, MatViewRefreshTask.INCREMENTAL_REFRESH, null);
    }

    @Override
    public void enqueueInvalidate(TableToken matViewToken, String invalidationReason) {
        enqueueRefreshTaskIfStateExists(matViewToken, MatViewRefreshTask.INVALIDATE, invalidationReason);
    }

    public void enqueueRefreshTaskIfStateExists(TableToken matViewToken, int operation, String invalidationReason) {
        final MatViewState state = stateByTableDirName.get(matViewToken.getDirName());
        if (state != null && !state.isDropped()) {
            enqueueMatViewTask(matViewToken, operation, invalidationReason);
        }
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
            task.refreshTriggeredTimestamp = microsecondClock.getTicks();
            taskQueue.enqueue(task);
        }
    }

    @Override
    public void notifyBaseTableCommit(MatViewRefreshTask task, long seqTxn) {
        final TableToken baseTableToken = task.baseTableToken;
        final AtomicLong lastNotifiedBaseTableTxn = lastNotifiedTxnByTableName.get(baseTableToken.getTableName());
        if (lastNotifiedBaseTableTxn != null) {
            // Always notify refresh job in case of mat view invalidation or full refresh.
            // For incremental refresh we check if we haven't already notified on the given txn.
            if (task.operation != MatViewRefreshTask.INCREMENTAL_REFRESH || notifyBaseTableCommit(lastNotifiedBaseTableTxn, seqTxn)) {
                task.refreshTriggeredTimestamp = microsecondClock.getTicks();
                taskQueue.enqueue(task);
                if (task.operation == MatViewRefreshTask.INVALIDATE) {
                    LOG.error()
                            .$("will invalidate all views for [baseTable=").$(baseTableToken)
                            .$(", reason=").$(task.invalidationReason)
                            .I$();
                } else {
                    LOG.debug().$("refresh job notified [baseTable=").$(baseTableToken)
                            .$(", op=").$(task.operation)
                            .I$();
                }
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
        }
    }

    @Override
    public boolean tryDequeueRefreshTask(MatViewRefreshTask task) {
        return taskQueue.tryDequeue(task);
    }

    private void enqueueMatViewTask(TableToken matViewToken, int operation, String invalidationReason) {
        final MatViewRefreshTask task = taskHolder.get();
        task.clear();
        task.matViewToken = matViewToken;
        task.operation = operation;
        task.invalidationReason = invalidationReason;
        if (operation == MatViewRefreshTask.INCREMENTAL_REFRESH || operation == MatViewRefreshTask.FULL_REFRESH) {
            task.refreshTriggeredTimestamp = microsecondClock.getTicks();
        }
        taskQueue.enqueue(task);
    }

    private void storeMatViewTelemetry(short event, TableToken tableToken, long baseTableTxn, CharSequence errorMessage, long latencyUs) {
        TelemetryMatViewTask.store(telemetry, event, tableToken.getTableId(), baseTableTxn, errorMessage, latencyUs);
    }
}
