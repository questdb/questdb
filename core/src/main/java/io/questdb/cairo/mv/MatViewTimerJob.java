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

import io.questdb.cairo.CairoEngine;
import io.questdb.cairo.CairoException;
import io.questdb.cairo.TableToken;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.engine.groupby.TimestampSampler;
import io.questdb.griffin.engine.groupby.TimestampSamplerFactory;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.mp.Queue;
import io.questdb.mp.SynchronizedJob;
import io.questdb.std.Numbers;
import io.questdb.std.ObjList;
import io.questdb.std.datetime.TimeZoneRules;
import io.questdb.std.datetime.microtime.MicrosecondClock;
import io.questdb.std.datetime.microtime.Timestamps;
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
    private static final Comparator<Timer> timerComparator = Comparator.comparingLong(Timer::getDeadline);
    private final MicrosecondClock clock;
    private final ObjList<Timer> expired = new ObjList<>();
    private final Predicate<Timer> filterByDirName;
    private final MatViewGraph matViewGraph;
    private final MatViewStateStore matViewStateStore;
    private final PriorityQueue<Timer> timerQueue = new PriorityQueue<>(INITIAL_QUEUE_CAPACITY, timerComparator);
    private final MatViewTimerTask timerTask = new MatViewTimerTask();
    private final Queue<MatViewTimerTask> timerTaskQueue;
    private String filteredDirName; // temporary value used by filterByDirName

    public MatViewTimerJob(CairoEngine engine) {
        this.clock = engine.getConfiguration().getMicrosecondClock();
        this.timerTaskQueue = engine.getMatViewTimerQueue();
        this.matViewGraph = engine.getMatViewGraph();
        this.matViewStateStore = engine.getMatViewStateStore();
        this.filterByDirName = this::filterByDirName;
    }

    public static long periodDelayMicros(int periodDelay, char periodDelayUnit) {
        switch (periodDelayUnit) {
            case 'm':
                return periodDelay * Timestamps.MINUTE_MICROS;
            case 'h':
                return periodDelay * Timestamps.HOUR_MICROS;
            case 'd':
                return periodDelay * Timestamps.DAY_MICROS;
        }
        return 0;
    }

    private void addTimers(TableToken viewToken, long now) {
        final MatViewDefinition viewDefinition = matViewGraph.getViewDefinition(viewToken);
        if (viewDefinition == null) {
            LOG.info().$("materialized view definition not found [view=").$(viewToken).I$();
            return;
        }
        try {
            long timerStart = viewDefinition.getTimerStart();
            TimeZoneRules timerTzRules = viewDefinition.getTimerTzRules();

            if (viewDefinition.getPeriodLength() > 0) {
                // It's a period mat view, so first add the period timer.
                final long start = viewDefinition.getTimerStart();
                final int periodLength = viewDefinition.getPeriodLength();
                final char periodLengthUnit = viewDefinition.getPeriodLengthUnit();
                final TimestampSampler periodSampler;
                try {
                    periodSampler = TimestampSamplerFactory.getInstance(periodLength, periodLengthUnit, -1);
                } catch (SqlException e) {
                    throw CairoException.critical(0).put("invalid LENGTH interval and/or unit: ").put(periodLength)
                            .put(", ").put(periodLengthUnit);
                }
                final int periodDelay = viewDefinition.getPeriodDelay();
                final char periodDelayUnit = viewDefinition.getPeriodDelayUnit();
                final long delay = periodDelayMicros(periodDelay, periodDelayUnit);
                final Timer periodTimer = new Timer(
                        Timer.PERIOD_REFRESH_TYPE,
                        viewToken,
                        periodSampler,
                        viewDefinition.getTimerTzRules(),
                        delay,
                        start,
                        now
                );
                timerQueue.add(periodTimer);
                LOG.info().$("registered period timer for materialized view [view=").$(viewToken)
                        .$(", start=").$ts(start)
                        .$(", tz=").$(viewDefinition.getTimerTimeZone())
                        .$(", length=").$(periodLength).$(periodLengthUnit)
                        .$(", delay=").$(periodDelay).$(periodDelayUnit)
                        .I$();

                // "Normal" timer start is volatile in case of period mat views.
                timerStart = now;
                timerTzRules = null;
            }

            if (viewDefinition.getRefreshType() == MatViewDefinition.REFRESH_TYPE_TIMER) {
                // The view has timer refresh, so add a "normal" timer for it.
                final int timerInterval = viewDefinition.getTimerInterval();
                final char timerUnit = viewDefinition.getTimerUnit();
                final TimestampSampler timerSampler;
                try {
                    timerSampler = TimestampSamplerFactory.getInstance(timerInterval, timerUnit, -1);
                } catch (SqlException e) {
                    throw CairoException.critical(0).put("invalid EVERY interval and/or unit: ").put(timerInterval)
                            .put(", ").put(timerUnit);
                }
                final Timer timer = new Timer(
                        Timer.INCREMENTAL_REFRESH_TYPE,
                        viewToken,
                        timerSampler,
                        timerTzRules,
                        0,
                        timerStart,
                        now
                );
                timerQueue.add(timer);
                LOG.info().$("registered timer for materialized view [view=").$(viewToken)
                        .$(", start=").$ts(timerStart)
                        .$(", tz=").$(viewDefinition.getTimerTimeZone())
                        .$(", interval=").$(timerInterval).$(timerUnit)
                        .I$();
            }
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
        while ((timer = timerQueue.peek()) != null && timer.getDeadline() <= now) {
            timer = timerQueue.poll();
            expired.add(timer);
            final TableToken viewToken = timer.getMatViewToken();
            final MatViewState state = matViewStateStore.getViewState(viewToken);
            if (state != null) {
                if (state.isDropped()) {
                    expired.remove(expired.size() - 1);
                    LOG.info().$("unregistered timer for dropped materialized view [view=").$(viewToken)
                            .$(", type=").$(timer.getType())
                            .I$();
                } else if (!state.isPendingInvalidation() && !state.isInvalid()) {
                    switch (timer.getType()) {
                        case Timer.INCREMENTAL_REFRESH_TYPE:
                            // Check if the view has refreshed since the last timer expiration.
                            // If not, don't schedule refresh to avoid unbounded growth of the queue.
                            final long refreshSeq = state.getRefreshSeq();
                            if (timer.getKnownRefreshSeq() != refreshSeq) {
                                matViewStateStore.enqueueIncrementalRefresh(viewToken);
                                timer.setKnownRefreshSeq(refreshSeq);
                            }
                            break;
                        case Timer.PERIOD_REFRESH_TYPE:
                            // range hi boundary is inclusive
                            matViewStateStore.enqueueRangeRefresh(viewToken, Numbers.LONG_NULL, timer.getPeriodHi() - 1);
                            break;
                        default:
                            LOG.error().$("unexpected timer type [view=").$(viewToken)
                                    .$(", type=").$(timer.getType())
                                    .I$();
                            break;
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

    private boolean removeTimers(TableToken viewToken) {
        filteredDirName = viewToken.getDirName();
        try {
            // Remove both incremental refresh and period refresh timers for the given view, if any.
            if (timerQueue.removeIf(filterByDirName)) {
                LOG.info().$("unregistered timers for materialized view [view=").$(viewToken).I$();
                return true;
            }
        } finally {
            filteredDirName = null;
        }
        LOG.info().$("timers for materialized view not found [view=").$(viewToken).I$();
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
                    addTimers(viewToken, now);
                    break;
                case MatViewTimerTask.REMOVE:
                    removeTimers(viewToken);
                    break;
                case MatViewTimerTask.UPDATE:
                    if (removeTimers(viewToken)) {
                        addTimers(viewToken, now);
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

    /**
     * May stand for either incremental refresh timer or period refresh timer.
     */
    private static class Timer {
        private static final byte INCREMENTAL_REFRESH_TYPE = 0;
        private static final byte PERIOD_REFRESH_TYPE = 1;
        private final long delay; // used in period timers
        private final TableToken matViewToken;
        private final TimestampSampler sampler;
        private final byte type;
        private final TimeZoneRules tzRules;
        private long deadlineLocal; // used for sampler interaction only
        private long deadlineUtc;
        private long knownRefreshSeq = -1;

        public Timer(
                byte type,
                @NotNull TableToken matViewToken,
                @NotNull TimestampSampler sampler,
                @Nullable TimeZoneRules tzRules,
                long delay,
                long start,
                long now
        ) {
            this.type = type;
            this.matViewToken = matViewToken;
            this.sampler = sampler;
            this.tzRules = tzRules;
            this.delay = delay;
            sampler.setStart(start);
            final long nowLocal = toLocal(now, tzRules);
            switch (type) {
                case INCREMENTAL_REFRESH_TYPE:
                    // It's fine if the timer triggers immediately.
                    deadlineLocal = nowLocal > start ? sampler.nextTimestamp(sampler.round(nowLocal - 1)) : start;
                    break;
                case PERIOD_REFRESH_TYPE:
                    // Unlike with incremental timer views, we want to trigger the timer
                    // for all complete periods, if they exist.
                    deadlineLocal = nowLocal > start ? sampler.round(nowLocal) : start;
                    break;
                default:
                    throw new IllegalStateException("unexpected timer type: " + type);
            }
            deadlineUtc = toUtc(deadlineLocal, tzRules);
        }

        public long getDeadline() {
            return deadlineUtc + delay;
        }

        public long getKnownRefreshSeq() {
            return knownRefreshSeq;
        }

        public TableToken getMatViewToken() {
            return matViewToken;
        }

        // returns currently awaited period's right boundary, in UTC
        public long getPeriodHi() {
            return deadlineUtc;
        }

        public byte getType() {
            return type;
        }

        public void setKnownRefreshSeq(long knownRefreshSeq) {
            this.knownRefreshSeq = knownRefreshSeq;
        }

        private static long toLocal(long utcTime, TimeZoneRules tzRules) {
            return tzRules != null ? utcTime + tzRules.getOffset(utcTime) : utcTime;
        }

        private static long toUtc(long localTime, TimeZoneRules tzRules) {
            return tzRules != null ? localTime - tzRules.getOffset(localTime) : localTime;
        }

        private void nextDeadline() {
            deadlineLocal = sampler.nextTimestamp(deadlineLocal);
            deadlineUtc = toUtc(deadlineLocal, tzRules);
        }
    }
}
