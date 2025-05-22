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
import io.questdb.cairo.sql.TableMetadata;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.mp.Queue;
import io.questdb.mp.SynchronizedJob;
import io.questdb.std.CharSequenceObjHashMap;
import io.questdb.std.ObjList;

/**
 * A scheduler for mat views with timer refresh.
 */
public class MatViewTimerJob extends SynchronizedJob {
    private static final Log LOG = LogFactory.getLog(MatViewTimerJob.class);
    private final CairoEngine engine;
    private final ObjList<MatViewTimingWheel.Timer> expired = new ObjList<>();
    private final MatViewGraph matViewGraph;
    private final MatViewStateStore matViewStateStore;
    private final MatViewTimerTask timerTask = new MatViewTimerTask();
    private final Queue<MatViewTimerTask> timerTaskQueue;
    private final CharSequenceObjHashMap<MatViewTimingWheel.Timer> timersByTableDirName = new CharSequenceObjHashMap<>();
    private final MatViewTimingWheel timingWheel;

    public MatViewTimerJob(CairoEngine engine) {
        this.engine = engine;
        this.timerTaskQueue = engine.getMatViewTimerQueue();
        this.matViewGraph = engine.getMatViewGraph();
        this.matViewStateStore = engine.getMatViewStateStore();
        final CairoConfiguration configuration = engine.getConfiguration();
        this.timingWheel = new MatViewTimingWheel(
                configuration.getMicrosecondClock(),
                configuration.getMatViewIntervalJobTick(),
                configuration.getMatViewIntervalJobWheelSize()
        );
    }

    private void addTimer(TableToken viewToken) {
        final MatViewDefinition viewDefinition = matViewGraph.getViewDefinition(viewToken);
        if (viewDefinition == null) {
            LOG.info().$("materialized view definition not found [view=").$(viewToken).I$();
            return;
        }
        try (TableMetadata matViewMeta = engine.getTableMetadata(viewToken)) {
            final long start = matViewMeta.getMatViewTimerStart();
            final int interval = matViewMeta.getMatViewTimerInterval();
            final char unit = matViewMeta.getMatViewTimerIntervalUnit();
            final MatViewTimingWheel.Timer newTimer = timingWheel.addTimer(viewToken, start, interval, unit);
            timersByTableDirName.put(viewToken.getDirName(), newTimer);
            LOG.info().$("registered timer for materialized view [view=").$(viewToken)
                    .$(", start=").$ts(start)
                    .$(", interval=").$(interval).$(unit)
                    .I$();
        } catch (Throwable th) {
            LOG.error()
                    .$("could not initialize timer for materialized view [view=").$(viewToken)
                    .$(", ex=").$(th)
                    .I$();
        }
    }

    private void removeTimer(TableToken viewToken) {
        final MatViewTimingWheel.Timer existingTimer = timersByTableDirName.get(viewToken.getDirName());
        if (existingTimer != null) {
            existingTimer.remove();
            timersByTableDirName.remove(viewToken.getDirName());
            LOG.info().$("unregistered timer for materialized view [view=").$(viewToken).I$();
        } else {
            LOG.info().$("refresh timer for materialized view not found [view=").$(viewToken).I$();
        }
    }

    @Override
    protected boolean runSerially() {
        boolean ran = false;
        // check created/dropped event queue
        while (timerTaskQueue.tryDequeue(timerTask)) {
            final TableToken viewToken = timerTask.getMatViewToken();
            switch (timerTask.getOperation()) {
                case MatViewTimerTask.ADD:
                    addTimer(viewToken);
                    break;
                case MatViewTimerTask.REMOVE:
                    removeTimer(viewToken);
                    break;
                case MatViewTimerTask.UPDATE:
                    removeTimer(viewToken);
                    addTimer(viewToken);
                    break;
                default:
                    LOG.error().$("unknown refresh timer operation [op=").$(timerTask.getOperation()).I$();
            }
            ran = true;
        }
        // process ticks
        while (timingWheel.tick(expired)) {
            for (int i = 0, n = expired.size(); i < n; i++) {
                final MatViewTimingWheel.Timer timer = expired.getQuick(i);
                final TableToken viewToken = timer.getMatViewToken();
                final MatViewState state = matViewStateStore.getViewState(viewToken);
                if (state != null) {
                    if (state.isDropped()) {
                        removeTimer(viewToken);
                    } else if (!state.isPendingInvalidation() && !state.isInvalid()) {
                        // Check if the view has refreshed since the last timer expiration.
                        // If not, don't schedule refresh to avoid unbounded growth of the queue.
                        final long refreshSeq = state.getRefreshSeq();
                        if (timer.getKnownRefreshSeq() != refreshSeq) {
                            matViewStateStore.enqueueIncrementalRefresh(viewToken);
                            timer.setKnownRefreshSeq(refreshSeq);
                        }
                    }
                } else {
                    LOG.info().$("state for materialized view not found [view=").$(viewToken).I$();
                }
            }
            ran = true;
        }
        return ran;
    }
}
