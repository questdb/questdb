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
import io.questdb.cairo.MicrosTimestampDriver;
import io.questdb.cairo.TableToken;
import io.questdb.cairo.TimestampDriver;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.engine.groupby.TimestampSampler;
import io.questdb.griffin.engine.groupby.TimestampSamplerFactory;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.mp.Queue;
import io.questdb.mp.SynchronizedJob;
import io.questdb.std.Numbers;
import io.questdb.std.ObjList;
import io.questdb.std.datetime.Clock;
import io.questdb.std.datetime.TimeZoneRules;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.Comparator;
import java.util.PriorityQueue;
import java.util.function.Predicate;

/**
 * A scheduler for mat views with timer refresh.
 * Also, runs special timers for period views and for updating refresh intervals for manual/timer views.
 */
public class MatViewTimerJob extends SynchronizedJob {
    private static final int INITIAL_QUEUE_CAPACITY = 16;
    private static final Log LOG = LogFactory.getLog(MatViewTimerJob.class);
    private static final Comparator<Timer> timerComparator = Comparator.comparingLong(Timer::getDeadlineMicros);
    private final Clock clock;
    private final CairoConfiguration configuration;
    private final ObjList<Timer> expired = new ObjList<>();
    private final Predicate<Timer> filterByDirName;
    private final MatViewGraph matViewGraph;
    private final MatViewStateStore matViewStateStore;
    private final PriorityQueue<Timer> timerQueue = new PriorityQueue<>(INITIAL_QUEUE_CAPACITY, timerComparator);
    private final MatViewTimerTask timerTask = new MatViewTimerTask();
    private final Queue<MatViewTimerTask> timerTaskQueue;
    private String filteredDirName; // temporary value used by filterByDirName

    public MatViewTimerJob(CairoEngine engine) {
        this.configuration = engine.getConfiguration();
        this.clock = configuration.getMicrosecondClock();
        this.timerTaskQueue = engine.getMatViewTimerQueue();
        this.matViewGraph = engine.getMatViewGraph();
        this.matViewStateStore = engine.getMatViewStateStore();
        this.filterByDirName = this::filterByDirName;
    }

    public static long periodDelay(TimestampDriver driver, int periodDelay, char periodDelayUnit) {
        switch (periodDelayUnit) {
            case 'm':
                return driver.fromMinutes(periodDelay);
            case 'h':
                return driver.fromHours(periodDelay);
            case 'd':
                return driver.fromDays(periodDelay);
        }
        return 0;
    }

    private void addTimers(TableToken viewToken) {
        final MatViewDefinition viewDefinition = matViewGraph.getViewDefinition(viewToken);
        if (viewDefinition == null) {
            LOG.info().$("materialized view definition not found [view=").$(viewToken).I$();
            return;
        }
        long now = viewDefinition.getBaseTableTimestampDriver().getTicks();
        try {
            if (viewDefinition.getRefreshType() != MatViewDefinition.REFRESH_TYPE_IMMEDIATE) {
                // The refresh is not immediate, i.e. it's either manual or timer.
                // Create a special timer that will enqueue refresh intervals update tasks.
                // We could cache the intervals right in the refresh job when there is a new base table commit,
                // but that might create many redundant WAL MAT_VIEW_INVALIDATE transactions with mat view state
                // values. To throttle refresh intervals caching, we have this special timer.
                // The end goal of this caching is unblocking WalPurgeJob to delete old WAL segments.
                createUpdateRefreshIntervalsTimer(viewDefinition, now);
            }

            long timerStart = viewDefinition.getTimerStart();
            TimeZoneRules timerTzRules = viewDefinition.getTimerTzRules();

            if (viewDefinition.getPeriodLength() > 0) {
                // It's a period mat view, so first add the period timer.
                createPeriodTimer(viewDefinition, now);

                // "Normal" timer start is volatile in case of period mat views.
                timerStart = now;
                timerTzRules = null;
            }

            if (viewDefinition.getRefreshType() == MatViewDefinition.REFRESH_TYPE_TIMER) {
                // The view has timer refresh, so add a "normal" timer for it.
                createTimer(viewDefinition, timerStart, timerTzRules, now);
            }
        } catch (Throwable th) {
            LOG.error()
                    .$("could not initialize timer for materialized view [view=").$(viewToken)
                    .$(", ex=").$(th)
                    .I$();
        }
    }

    private void createPeriodTimer(@NotNull MatViewDefinition viewDefinition, long now) {
        final TableToken viewToken = viewDefinition.getMatViewToken();
        final long start = viewDefinition.getTimerStart();
        final int length = viewDefinition.getPeriodLength();
        final char lengthUnit = viewDefinition.getPeriodLengthUnit();
        final TimestampSampler sampler;
        try {
            sampler = TimestampSamplerFactory.getInstance(viewDefinition.getBaseTableTimestampDriver(), length, lengthUnit, -1);
        } catch (SqlException e) {
            throw CairoException.nonCritical().put("invalid LENGTH interval and/or unit: ").put(length)
                    .put(", ").put(lengthUnit);
        }
        final int delay = viewDefinition.getPeriodDelay();
        final char delayUnit = viewDefinition.getPeriodDelayUnit();
        final long delayMicros = periodDelay(viewDefinition.getBaseTableTimestampDriver(), delay, delayUnit);
        final Timer periodTimer = new Timer(
                Timer.PERIOD_REFRESH_TYPE,
                viewToken,
                sampler,
                viewDefinition.getTimerTzRules(),
                delayMicros,
                start,
                now,
                viewDefinition.getBaseTableTimestampType()
        );
        timerQueue.add(periodTimer);
        LOG.info().$("created period timer for materialized view [view=").$(viewToken)
                .$(", start=").$ts(start)
                .$(", tz=").$(viewDefinition.getTimerTimeZone())
                .$(", length=").$(length).$(lengthUnit)
                .$(", delay=").$(delay).$(delayUnit)
                .I$();
    }

    private void createTimer(
            @NotNull MatViewDefinition viewDefinition,
            long timerStart,
            @Nullable TimeZoneRules timerTzRules,
            long now
    ) {
        final TableToken viewToken = viewDefinition.getMatViewToken();
        final int interval = viewDefinition.getTimerInterval();
        final char unit = viewDefinition.getTimerUnit();
        final TimestampSampler sampler;
        try {
            sampler = TimestampSamplerFactory.getInstance(viewDefinition.getBaseTableTimestampDriver(), interval, unit, -1);
        } catch (SqlException e) {
            throw CairoException.nonCritical().put("invalid EVERY interval and/or unit: ").put(interval)
                    .put(", ").put(unit);
        }
        final Timer timer = new Timer(
                Timer.INCREMENTAL_REFRESH_TYPE,
                viewToken,
                sampler,
                timerTzRules,
                0,
                timerStart,
                now,
                viewDefinition.getBaseTableTimestampType()
        );
        timerQueue.add(timer);
        LOG.info().$("created timer for materialized view [view=").$(viewToken)
                .$(", start=").$ts(timerStart)
                .$(", tz=").$(viewDefinition.getTimerTimeZone())
                .$(", interval=").$(interval).$(unit)
                .I$();
    }

    private void createUpdateRefreshIntervalsTimer(@NotNull MatViewDefinition viewDefinition, long now) {
        final TableToken viewToken = viewDefinition.getMatViewToken();
        final long periodMs = configuration.getMatViewRefreshIntervalsUpdatePeriod();
        final TimestampSampler sampler;
        try {
            sampler = TimestampSamplerFactory.getInstance(viewDefinition.getBaseTableTimestampDriver(), periodMs, 'T', -1);
        } catch (SqlException e) {
            throw CairoException.nonCritical().put("invalid refresh intervals update period: ").put(periodMs);
        }
        final Timer timer = new Timer(
                Timer.UPDATE_REFRESH_INTERVALS_TYPE,
                viewToken,
                sampler,
                null,
                0,
                now, // the timer should start immediately
                now,
                viewDefinition.getBaseTableTimestampType()
        );
        timerQueue.add(timer);
        LOG.info().$("created refresh intervals update timer for materialized view [view=").$(viewToken)
                .$(", start=").$ts(now)
                .$(", interval=").$(periodMs).$('T')
                .I$();
    }

    private boolean filterByDirName(Timer timer) {
        return filteredDirName != null && filteredDirName.equals(timer.getMatViewToken().getDirName());
    }

    private boolean processExpiredTimers(long microNow) {
        expired.clear();
        boolean ran = false;
        Timer timer;
        while ((timer = timerQueue.peek()) != null && timer.getDeadlineMicros() <= microNow) {
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
                            if (timer.getKnownSeq() != refreshSeq) {
                                matViewStateStore.enqueueIncrementalRefresh(viewToken);
                                timer.setKnownSeq(refreshSeq);
                            }
                            break;
                        case Timer.PERIOD_REFRESH_TYPE:
                            // range hi boundary is inclusive
                            matViewStateStore.enqueueRangeRefresh(viewToken, Numbers.LONG_NULL, timer.getPeriodHi() - 1);
                            break;
                        case Timer.UPDATE_REFRESH_INTERVALS_TYPE:
                            // Enqueue refresh intervals update only if the base table had new transactions
                            // since the last caching.
                            final long refreshIntervalsSeq = state.getRefreshIntervalsSeq();
                            if (timer.getKnownSeq() != refreshIntervalsSeq) {
                                matViewStateStore.enqueueUpdateRefreshIntervals(viewToken);
                                timer.setKnownSeq(refreshIntervalsSeq);
                            }
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
            // Remove all timers for the given view, if any.
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
        // check created/dropped event queue
        while (timerTaskQueue.tryDequeue(timerTask)) {
            final TableToken viewToken = timerTask.getMatViewToken();
            switch (timerTask.getOperation()) {
                case MatViewTimerTask.ADD:
                    addTimers(viewToken);
                    break;
                case MatViewTimerTask.REMOVE:
                    removeTimers(viewToken);
                    break;
                case MatViewTimerTask.UPDATE:
                    if (removeTimers(viewToken)) {
                        addTimers(viewToken);
                    }
                    break;
                default:
                    LOG.error().$("unknown refresh timer operation [op=").$(timerTask.getOperation()).I$();
            }
            ran = true;
        }
        ran |= processExpiredTimers(clock.getTicks());
        return ran;
    }

    /**
     * May stand for either incremental refresh timer or period refresh timer.
     */
    private static class Timer {
        private static final byte INCREMENTAL_REFRESH_TYPE = 0;
        private static final byte PERIOD_REFRESH_TYPE = 1;
        private static final byte UPDATE_REFRESH_INTERVALS_TYPE = 2;
        private final long delay; // used in period timers
        private final TableToken matViewToken;
        private final TimestampSampler sampler;
        private final int timestampType;
        private final byte type;
        private final TimeZoneRules tzRules;
        private long deadlineLocal; // used for sampler interaction only
        private long deadlineUtc;
        private long deadlineUtcMicro;
        // Holds refresh sequence number for "normal" timers
        // or caching sequence for refresh intervals update timers.
        private long knownSeq = -1;

        public Timer(
                byte type,
                @NotNull TableToken matViewToken,
                @NotNull TimestampSampler sampler,
                @Nullable TimeZoneRules tzRules,
                long delay,
                long start,
                long now,
                int timestampType
        ) {
            this.type = type;
            this.matViewToken = matViewToken;
            this.sampler = sampler;
            this.tzRules = tzRules;
            this.delay = delay;
            this.timestampType = timestampType;
            sampler.setStart(start);
            final long nowLocal = toLocal(now, tzRules);
            switch (type) {
                case INCREMENTAL_REFRESH_TYPE:
                case UPDATE_REFRESH_INTERVALS_TYPE:
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
            deadlineUtcMicro = MicrosTimestampDriver.INSTANCE.from(deadlineUtc + delay, timestampType);
        }

        public long getDeadlineMicros() {
            return deadlineUtcMicro;
        }

        public long getKnownSeq() {
            return knownSeq;
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

        public void setKnownSeq(long knownSeq) {
            this.knownSeq = knownSeq;
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
            deadlineUtcMicro = MicrosTimestampDriver.INSTANCE.from(deadlineUtc + delay, timestampType);
        }
    }
}
