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

import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.CairoEngine;
import io.questdb.cairo.TableToken;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.mp.Queue;
import io.questdb.mp.SynchronizedJob;
import io.questdb.std.ObjList;
import io.questdb.std.ObjObjHashMap;

/**
 * Acts as an incremental refresh scheduler for mat views with interval refresh.
 */
public class MatViewRefreshIntervalJob extends SynchronizedJob {
    private static final Log LOG = LogFactory.getLog(MatViewRefreshIntervalJob.class);
    private final ObjList<RefreshIntervalTimingWheel.Timer> expired = new ObjList<>();
    private final MatViewGraph matViewGraph;
    private final MatViewStateStore matViewStateStore;
    private final MatViewRefreshIntervalTask refreshIntervalTask = new MatViewRefreshIntervalTask();
    private final ObjObjHashMap<TableToken, RefreshIntervalTimingWheel.Timer> refreshIntervalTimers = new ObjObjHashMap<>();
    private final Queue<MatViewRefreshIntervalTask> taskQueue;
    private final RefreshIntervalTimingWheel timingWheel;

    public MatViewRefreshIntervalJob(CairoEngine engine) {
        this.taskQueue = engine.getMatViewRefreshIntervalQueue();
        this.matViewGraph = engine.getMatViewGraph();
        this.matViewStateStore = engine.getMatViewStateStore();
        final CairoConfiguration configuration = engine.getConfiguration();
        this.timingWheel = new RefreshIntervalTimingWheel(
                configuration.getMicrosecondClock(),
                configuration.getMatViewIntervalJobTick(),
                configuration.getMatViewIntervalJobWheelSize()
        );
    }

    private void createTimer(TableToken viewToken) {
        final MatViewDefinition viewDefinition = matViewGraph.getViewDefinition(viewToken);
        if (viewDefinition == null) {
            LOG.info().$("materialized view definition not found [view=").$(viewToken).I$();
            return;
        }
        try {
            final RefreshIntervalTimingWheel.Timer newTimer = timingWheel.addRefreshInterval(viewDefinition);
            refreshIntervalTimers.put(viewToken, newTimer);
        } catch (Throwable th) {
            LOG.error()
                    .$("could not initialize timer for materialized view [view=").$(viewToken)
                    .$(", ex=").$(th)
                    .I$();
        }
    }

    private void removeTimer(TableToken viewToken) {
        final RefreshIntervalTimingWheel.Timer existingTimer = refreshIntervalTimers.get(viewToken);
        if (existingTimer != null) {
            existingTimer.remove();
            refreshIntervalTimers.remove(viewToken);
        } else {
            LOG.info().$("refresh interval timer for materialized view not found [view=").$(viewToken).I$();
        }
    }

    @Override
    protected boolean runSerially() {
        boolean ran = false;
        // check created/dropped event queue
        while (taskQueue.tryDequeue(refreshIntervalTask)) {
            final TableToken viewToken = refreshIntervalTask.getMatViewToken();
            switch (refreshIntervalTask.getOperation()) {
                case MatViewRefreshIntervalTask.CREATED:
                    createTimer(viewToken);
                    break;
                case MatViewRefreshIntervalTask.DROPPED:
                    removeTimer(viewToken);
                    break;
                default:
                    LOG.error().$("unknown refresh interval operation [op=").$(refreshIntervalTask.getOperation()).I$();
            }
            ran = true;
        }
        // process ticks
        ran |= timingWheel.tick(expired);
        for (int i = 0, n = expired.size(); i < n; i++) {
            final RefreshIntervalTimingWheel.Timer timer = expired.getQuick(i);
            final MatViewDefinition viewDefinition = timer.getViewDefinition();
            final TableToken viewToken = viewDefinition.getMatViewToken();
            final MatViewState state = matViewStateStore.getViewState(viewToken);
            if (state != null) {
                if (state.isDropped()) {
                    removeTimer(viewToken);
                } else if (!state.isPendingInvalidation() && !state.isInvalid()) {
                    matViewStateStore.enqueueIncrementalRefresh(viewToken);
                }
            } else {
                LOG.info().$("state for materialized view not found [view=").$(viewToken).I$();
            }
        }
        return ran;
    }
}
