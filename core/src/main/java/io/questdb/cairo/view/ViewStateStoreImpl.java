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

package io.questdb.cairo.view;

import io.questdb.cairo.CairoEngine;
import io.questdb.cairo.CairoException;
import io.questdb.cairo.MetadataCacheWriter;
import io.questdb.cairo.TableToken;
import io.questdb.cairo.sql.RecordMetadata;
import io.questdb.mp.ConcurrentQueue;
import io.questdb.mp.Queue;
import io.questdb.std.ConcurrentHashMap;
import io.questdb.std.CarrierLocal;
import io.questdb.std.LongList;
import io.questdb.std.Numbers;
import io.questdb.std.ObjList;
import io.questdb.std.datetime.MicrosecondClock;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.jetbrains.annotations.TestOnly;

import java.util.concurrent.atomic.AtomicLong;

public class ViewStateStoreImpl implements ViewStateStore {
    // guards the pending busy retries, the task that queues them, and the writes of their next due time
    private final Object compileBusyRetryLock = new Object();
    private final ViewCompilerTask compileBusyRetryTask = new ViewCompilerTask();
    // one word, so that a reader gets both counts from one read, see getCompileRunActivity()
    private final AtomicLong compileRunActivity = new AtomicLong();
    private final CairoEngine engine;
    private final MicrosecondClock microsecondClock;
    // the pending busy retries, at most one per view with a state: the view, the update timestamp
    // the retry compiles it with, and the time the retry is due at
    private final LongList pendingCompileBusyRetryDueTimestamps = new LongList();
    private final LongList pendingCompileBusyRetryUpdateTimestamps = new LongList();
    private final ObjList<TableToken> pendingCompileBusyRetryViews = new ObjList<>();
    private final ConcurrentHashMap<ViewState> stateByTableDirName = new ConcurrentHashMap<>();
    private final CarrierLocal<ViewCompilerTask> taskHolder = new CarrierLocal<>(ViewCompilerTask::new);
    // using unlimited concurrent queue to avoid dropping notifications when the queue is full,
    // generally not expecting too many view compile notifications, but if table schema changes
    // are very frequent, then the number of notifications can be high too
    private final Queue<ViewCompilerTask> taskQueue = ConcurrentQueue.createConcurrentQueue(ViewCompilerTask::new);
    // The earliest due time of the pending busy retries, or Long.MAX_VALUE when none is ever due.
    // Written under the lock and read without it: a stale value only delays a retry to the next
    // drain, or takes the lock for nothing.
    private volatile long nextCompileBusyRetryDueMicros = Long.MAX_VALUE;

    public ViewStateStoreImpl(CairoEngine engine) {
        this.engine = engine;
        this.microsecondClock = engine.getConfiguration().getMicrosecondClock();
    }

    // Leaves the compile run counts alone: a run that is still running would finish after the
    // reset and drive the running count below zero.
    @TestOnly
    @Override
    public void clear() {
        stateByTableDirName.clear();
        taskQueue.clear();
        synchronized (compileBusyRetryLock) {
            pendingCompileBusyRetryViews.clear();
            pendingCompileBusyRetryUpdateTimestamps.clear();
            pendingCompileBusyRetryDueTimestamps.clear();
            nextCompileBusyRetryDueMicros = Long.MAX_VALUE;
        }
    }

    @Override
    public void createViewState(ViewDefinition viewDefinition, @Nullable RecordMetadata metadata) {
        final TableToken viewToken = viewDefinition.getViewToken();
        final ViewMetadata viewMetadata = metadata != null
                ? ViewMetadata.newInstance(viewToken, metadata)
                : ViewMetadata.newInstance(viewToken);
        final ViewState state = new ViewState(viewDefinition, viewMetadata, microsecondClock.getTicks());

        final ViewState prevState = stateByTableDirName.putIfAbsent(viewToken.getDirName(), state);
        if (prevState != null) {
            throw CairoException.critical(0).put("view state already exists [dir=").put(viewToken.getDirName());
        }

        try (MetadataCacheWriter metadataRW = engine.getMetadataCache().writeLock()) {
            metadataRW.hydrateTable(state.getViewMetadata());
        }
    }

    @Override
    public void enqueueCompile(@NotNull TableToken tableToken) {
        final ViewCompilerTask task = taskHolder.get();
        task.isBusyRetry = false;
        task.tableToken = tableToken;
        task.updateTimestamp = microsecondClock.getTicks();
        taskQueue.enqueue(task);
    }

    @Override
    public void enqueueDueCompileBusyRetries() {
        final long nextDueMicros = nextCompileBusyRetryDueMicros;
        if (nextDueMicros == Long.MAX_VALUE) {
            // nothing pending, or only retries that are never due: one read on every drain
            return;
        }
        final long now = microsecondClock.getTicks();
        if (now < nextDueMicros) {
            return;
        }
        synchronized (compileBusyRetryLock) {
            long nextDue = Long.MAX_VALUE;
            for (int i = 0; i < pendingCompileBusyRetryViews.size(); ) {
                final long dueMicros = pendingCompileBusyRetryDueTimestamps.getQuick(i);
                if (dueMicros <= now) {
                    compileBusyRetryTask.isBusyRetry = true;
                    compileBusyRetryTask.tableToken = pendingCompileBusyRetryViews.getQuick(i);
                    compileBusyRetryTask.updateTimestamp = pendingCompileBusyRetryUpdateTimestamps.getQuick(i);
                    taskQueue.enqueue(compileBusyRetryTask);
                    removePendingCompileBusyRetry(i);
                } else {
                    nextDue = Math.min(nextDue, dueMicros);
                    i++;
                }
            }
            nextCompileBusyRetryDueMicros = nextDue;
        }
    }

    @Override
    public void finishCompileRun() {
        compileRunActivity.decrementAndGet();
    }

    @Override
    public long getCompileRunActivity() {
        return compileRunActivity.get();
    }

    /**
     * Copies the pending busy retries, one per view, into the sinks, and leaves them pending.
     */
    @TestOnly
    public void getPendingCompileBusyRetries(ObjList<TableToken> viewTokens, LongList updateTimestamps, LongList dueTimestamps) {
        synchronized (compileBusyRetryLock) {
            viewTokens.addAll(pendingCompileBusyRetryViews);
            updateTimestamps.add(pendingCompileBusyRetryUpdateTimestamps);
            dueTimestamps.add(pendingCompileBusyRetryDueTimestamps);
        }
    }

    @Override
    public ViewState getViewState(TableToken viewToken) {
        return stateByTableDirName.get(viewToken.getDirName());
    }

    @Override
    public void removeViewState(TableToken viewToken) {
        stateByTableDirName.remove(viewToken.getDirName());
        // After the state: a retry scheduled before this point is removed here, and one scheduled
        // after it finds no state, so no retry outlives its view.
        synchronized (compileBusyRetryLock) {
            final int index = indexOfPendingCompileBusyRetry(viewToken);
            if (index > -1) {
                removePendingCompileBusyRetry(index);
            }
        }
    }

    @Override
    public long scheduleCompileBusyRetry(@NotNull TableToken viewToken, long updateTimestamp) {
        synchronized (compileBusyRetryLock) {
            if (getViewState(viewToken) == null) {
                return Numbers.LONG_NULL;
            }
            final int index = indexOfPendingCompileBusyRetry(viewToken);
            if (index > -1) {
                pendingCompileBusyRetryUpdateTimestamps.setQuick(index, Math.max(pendingCompileBusyRetryUpdateTimestamps.getQuick(index), updateTimestamp));
                return pendingCompileBusyRetryDueTimestamps.getQuick(index);
            }
            // A retry whose due time does not fit a long is never due. The setting is read at
            // each schedule, which only a lock failure makes.
            final long delayMillis = engine.getConfiguration().getViewCompileBusyRetryTimeout();
            final long delayMicros = delayMillis > Long.MAX_VALUE / 1_000 ? Long.MAX_VALUE : delayMillis * 1_000;
            final long now = microsecondClock.getTicks();
            final long dueMicros = delayMicros > Long.MAX_VALUE - now ? Long.MAX_VALUE : now + delayMicros;
            pendingCompileBusyRetryViews.add(viewToken);
            pendingCompileBusyRetryUpdateTimestamps.add(updateTimestamp);
            pendingCompileBusyRetryDueTimestamps.add(dueMicros);
            nextCompileBusyRetryDueMicros = Math.min(nextCompileBusyRetryDueMicros, dueMicros);
            return dueMicros;
        }
    }

    @Override
    public void startCompileRun() {
        // one more run started, and one more running
        compileRunActivity.addAndGet((1L << 32) + 1);
    }

    @Override
    public boolean tryDequeueCompilerTask(ViewCompilerTask task) {
        return taskQueue.tryDequeue(task);
    }

    private int indexOfPendingCompileBusyRetry(TableToken viewToken) {
        final String dirName = viewToken.getDirName();
        for (int i = 0, n = pendingCompileBusyRetryViews.size(); i < n; i++) {
            if (dirName.equals(pendingCompileBusyRetryViews.getQuick(i).getDirName())) {
                return i;
            }
        }
        return -1;
    }

    private void removePendingCompileBusyRetry(int index) {
        pendingCompileBusyRetryViews.remove(index);
        pendingCompileBusyRetryUpdateTimestamps.removeIndex(index);
        pendingCompileBusyRetryDueTimestamps.removeIndex(index);
    }
}
