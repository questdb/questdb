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

package io.questdb.cairo.lv;

import io.questdb.cairo.CairoEngine;
import io.questdb.cairo.TableToken;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.mp.SynchronizedJob;
import io.questdb.std.ObjList;
import io.questdb.std.datetime.MicrosecondClock;

/**
 * Periodic job that force-drains idle live view merge buffers.
 * <p>
 * When a view's merge buffer still holds rows more than {@code LAG} microseconds
 * after the last refresh, this job enqueues a force-drain task on the refresh
 * queue. The refresh worker pool then drains the buffer with the watermark set
 * to the maximum observed timestamp, emitting every held-back row.
 * <p>
 * Event-driven flushes (new WAL commits for the base table) advance
 * {@link LiveViewInstance#getLastRefreshTimeUs()} and naturally suppress idle
 * flushes while data is flowing.
 */
public class LiveViewTimerJob extends SynchronizedJob {
    private static final Log LOG = LogFactory.getLog(LiveViewTimerJob.class);
    private final MicrosecondClock clock;
    private final CairoEngine engine;
    private final ObjList<LiveViewInstance> viewInstanceSink = new ObjList<>();

    public LiveViewTimerJob(CairoEngine engine) {
        this.engine = engine;
        this.clock = engine.getConfiguration().getMicrosecondClock();
    }

    @Override
    protected boolean runSerially() {
        final long nowUs = clock.getTicks();
        final LiveViewRegistry registry = engine.getLiveViewRegistry();
        final LiveViewStateStore stateStore = engine.getLiveViewStateStore();

        viewInstanceSink.clear();
        registry.getViews(viewInstanceSink);

        boolean didWork = false;
        for (int i = 0, n = viewInstanceSink.size(); i < n; i++) {
            LiveViewInstance instance = viewInstanceSink.getQuick(i);
            if (instance.isDropped() || instance.isInvalid()) {
                continue;
            }
            TableToken baseTableToken = instance.getDefinition().getBaseTableToken();
            if (baseTableToken == null) {
                continue;
            }

            // Retry a refresh that the worker bailed out of because readers still pinned
            // the write buffer. The flag is cleared by publishWriteBuffer on success.
            if (instance.isPendingRefresh()) {
                LOG.debug().$("enqueueing live view pending-refresh retry [view=")
                        .$(instance.getDefinition().getViewName())
                        .$(", base=").$(baseTableToken).I$();
                stateStore.enqueueForceDrain(baseTableToken);
                didWork = true;
                continue;
            }

            MergeBuffer mergeBuffer = instance.getMergeBuffer();
            if (mergeBuffer == null || mergeBuffer.isEmpty()) {
                continue;
            }
            long lagMicros = instance.getDefinition().getLagMicros();
            long lastRefreshUs = instance.getLastRefreshTimeUs();
            if (nowUs - lastRefreshUs < lagMicros) {
                continue;
            }
            LOG.debug().$("enqueueing live view idle flush [view=").$(instance.getDefinition().getViewName())
                    .$(", base=").$(baseTableToken)
                    .$(", idleUs=").$(nowUs - lastRefreshUs)
                    .$(", lagUs=").$(lagMicros)
                    .I$();
            stateStore.enqueueForceDrain(baseTableToken);
            // Prevent re-enqueueing on every tick while the task is in flight.
            instance.setLastRefreshTimeUs(nowUs);
            didWork = true;
        }
        return didWork;
    }
}
