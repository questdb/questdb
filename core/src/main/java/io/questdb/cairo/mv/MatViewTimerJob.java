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
import io.questdb.cairo.CairoException;
import io.questdb.cairo.TableToken;
import io.questdb.cairo.sql.TableMetadata;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.engine.groupby.TimestampSampler;
import io.questdb.griffin.engine.groupby.TimestampSamplerFactory;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.mp.Queue;
import io.questdb.mp.SynchronizedJob;
import io.questdb.std.ObjList;
import io.questdb.std.datetime.TimeZoneRules;
import io.questdb.std.datetime.microtime.MicrosecondClock;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.Comparator;
import java.util.PriorityQueue;
import java.util.function.Predicate;

/**
 * A scheduler for mat views with timer refresh.
 */
public class MatViewTimerJob extends SynchronizedJob {
    private static final int INITIAL_QUEUE_CAPACITY = 16;
    private static final Log LOG = LogFactory.getLog(MatViewTimerJob.class);
    private static final Comparator<Timer> timerComparator = Comparator.comparingLong(t -> t.deadlineUtc);
    private final MicrosecondClock clock;
    private final CairoConfiguration configuration;
    private final CairoEngine engine;
    private final ObjList<Timer> expired = new ObjList<>();
    private final Predicate<Timer> filterByDirName;
    private final MatViewGraph matViewGraph;
    private final MatViewStateStore matViewStateStore;
    private final PriorityQueue<Timer> timerQueue = new PriorityQueue<>(INITIAL_QUEUE_CAPACITY, timerComparator);
    private final MatViewTimerTask timerTask = new MatViewTimerTask();
    private final Queue<MatViewTimerTask> timerTaskQueue;
    private String filteredDirName; // temporary value used by filterByDirName

    public MatViewTimerJob(CairoEngine engine) {
        this.engine = engine;
        this.configuration = engine.getConfiguration();
        this.clock = configuration.getMicrosecondClock();
        this.timerTaskQueue = engine.getMatViewTimerQueue();
        this.matViewGraph = engine.getMatViewGraph();
        this.matViewStateStore = engine.getMatViewStateStore();
        this.filterByDirName = this::filterByDirName;
    }

    private void addTimer(TableToken viewToken, long now) {
        final MatViewDefinition viewDefinition = matViewGraph.getViewDefinition(viewToken);
        if (viewDefinition == null) {
            LOG.info().$("materialized view definition not found [view=").$(viewToken).I$();
            return;
        }
        try (TableMetadata matViewMeta = engine.getTableMetadata(viewToken)) {
            final long start = matViewMeta.getMatViewTimerStart();
            final int interval = matViewMeta.getMatViewTimerInterval();
            final char unit = matViewMeta.getMatViewTimerUnit();
            final TimestampSampler sampler;
            try {
                sampler = TimestampSamplerFactory.getInstance(interval, unit, 0);
            } catch (SqlException e) {
                throw CairoException.critical(0).put("invalid EVERY interval and/or unit: ").put(interval)
                        .put(", ").put(unit);
            }
            final Timer timer = new Timer(
                    viewToken,
                    sampler,
                    viewDefinition.getTimerTzRules(),
                    start,
                    configuration.getMatViewTimerStartEpsilon(),
                    now
            );
            timerQueue.add(timer);
            LOG.info().$("registered timer for materialized view [view=").$(viewToken)
                    .$(", start=").$ts(start)
                    .$(", interval=").$(interval).$(unit)
                    .I$();
        } catch (Throwable th) {
            LOG.critical()
                    .$("could not initialize timer for materialized view [view=").$(viewToken)
                    .$(", ex=").$(th)
                    .I$();
        }
    }

    private boolean filterByDirName(Timer timer) {
        return filteredDirName != null && filteredDirName.equals(timer.getMatViewToken().getDirName());
    }

    private boolean processExpiredTimers(long now) {
        expired.clear();
        boolean ran = false;
        Timer timer;
        while ((timer = timerQueue.peek()) != null && timer.deadlineUtc <= now) {
            timer = timerQueue.poll();
            expired.add(timer);
            final TableToken viewToken = timer.getMatViewToken();
            final MatViewState state = matViewStateStore.getViewState(viewToken);
            if (state != null) {
                if (state.isDropped()) {
                    expired.remove(expired.size() - 1);
                    LOG.info().$("unregistered timer for dropped materialized view [view=").$(viewToken).I$();
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
            ran = true;
        }
        // Re-schedule expired timers.
        for (int i = 0, n = expired.size(); i < n; i++) {
            timer = expired.getQuick(i);
            timer.nextDeadline();
            timerQueue.add(timer);
        }
        return ran;
    }

    private boolean removeTimer(TableToken viewToken) {
        filteredDirName = viewToken.getDirName();
        try {
            if (timerQueue.removeIf(filterByDirName)) {
                LOG.info().$("unregistered timer for materialized view [view=").$(viewToken).I$();
                return true;
            }
        } finally {
            filteredDirName = null;
        }
        LOG.info().$("refresh timer for materialized view not found [view=").$(viewToken).I$();
        return false;
    }

    @Override
    protected boolean runSerially() {
        boolean ran = false;
        final long now = clock.getTicks();
        // check created/dropped event queue
        while (timerTaskQueue.tryDequeue(timerTask)) {
            final TableToken viewToken = timerTask.getMatViewToken();
            switch (timerTask.getOperation()) {
                case MatViewTimerTask.ADD:
                    addTimer(viewToken, now);
                    break;
                case MatViewTimerTask.REMOVE:
                    removeTimer(viewToken);
                    break;
                case MatViewTimerTask.UPDATE:
                    if (removeTimer(viewToken)) {
                        addTimer(viewToken, now);
                    }
                    break;
                default:
                    LOG.error().$("unknown refresh timer operation [op=").$(timerTask.getOperation()).I$();
            }
            ran = true;
        }
        ran |= processExpiredTimers(now);
        return ran;
    }

    private static class Timer {
        private final TableToken matViewToken;
        private final TimeZoneRules rules;
        private final TimestampSampler sampler;
        private long deadlineLocal; // used for sampler interaction only
        private long deadlineUtc;
        private long knownRefreshSeq = -1;

        private Timer(
                @NotNull TableToken matViewToken,
                @NotNull TimestampSampler sampler,
                @Nullable TimeZoneRules rules,
                long start,
                long startEpsilon,
                long now
        ) {
            this.matViewToken = matViewToken;
            this.sampler = sampler;
            this.rules = rules;
            sampler.setStart(start);
            // It's fine if the timer triggers immediately.
            deadlineUtc = now > start + startEpsilon ? sampler.nextTimestamp(sampler.round(now - 1)) : start;
            deadlineLocal = rules != null ? deadlineUtc + rules.getOffset(deadlineUtc) : deadlineUtc;
        }

        @Override
        public final boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (!(o instanceof Timer)) {
                return false;
            }

            Timer timer = (Timer) o;
            return matViewToken.getDirName().equals(timer.matViewToken.getDirName());
        }

        public long getKnownRefreshSeq() {
            return knownRefreshSeq;
        }

        public TableToken getMatViewToken() {
            return matViewToken;
        }

        @Override
        public int hashCode() {
            return matViewToken.getDirName().hashCode();
        }

        public void setKnownRefreshSeq(long knownRefreshSeq) {
            this.knownRefreshSeq = knownRefreshSeq;
        }

        private void nextDeadline() {
            deadlineLocal = sampler.nextTimestamp(deadlineLocal);
            deadlineUtc = rules != null ? deadlineLocal - rules.getLocalOffset(deadlineLocal) : deadlineLocal;
        }
    }
}
