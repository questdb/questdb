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

import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.CairoEngine;
import io.questdb.cairo.CairoException;
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.MicrosTimestampDriver;
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
import io.questdb.std.datetime.MicrosecondClock;
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
    private static final Comparator<RetryEntry> retryComparator = Comparator.comparingLong(RetryEntry::getDeadlineUs);
    private static final Comparator<Timer> timerComparator = Comparator.comparingLong(Timer::getDeadlineMicros);
    private final MicrosecondClock clock;
    private final CairoConfiguration configuration;
    private final ObjList<Timer> expired = new ObjList<>();
    private final Predicate<Timer> filterByDirName;
    private final DependentViewGraph dependentViewGraph;
    private final MatViewStateStore matViewStateStore;
    // Pool of reusable retry heap entries, to avoid per-retry allocation during a retry storm.
    private final ObjList<RetryEntry> retryEntryPool = new ObjList<>();
    // (deadline, view) min-heap of pending refresh retries, fed by RETRY timer tasks. Only the
    // entries that have come due are popped on each tick, so the common case is a single peek().
    // Accessed only from runSerially() (under the SynchronizedJob lock), so no extra synchronization.
    private final PriorityQueue<RetryEntry> retryQueue = new PriorityQueue<>(INITIAL_QUEUE_CAPACITY, retryComparator);
    private final PriorityQueue<Timer> timerQueue = new PriorityQueue<>(INITIAL_QUEUE_CAPACITY, timerComparator);
    private final MatViewTimerTask timerTask = new MatViewTimerTask();
    private final Queue<MatViewTimerTask> timerTaskQueue;
    private String filteredDirName; // temporary value used by filterByDirName

    public MatViewTimerJob(CairoEngine engine) {
        this.configuration = engine.getConfiguration();
        this.clock = configuration.getMicrosecondClock();
        this.timerTaskQueue = engine.getMatViewTimerQueue();
        this.dependentViewGraph = engine.getDependentViewGraph();
        this.matViewStateStore = engine.getMatViewStateStore();
        this.filterByDirName = this::filterByDirName;
    }

    private RetryEntry acquireRetryEntry(TableToken viewToken, long deadlineUs) {
        final int n = retryEntryPool.size();
        final RetryEntry entry;
        if (n > 0) {
            entry = retryEntryPool.getQuick(n - 1);
            retryEntryPool.setPos(n - 1);
        } else {
            entry = new RetryEntry();
        }
        entry.viewToken = viewToken;
        entry.deadlineUs = deadlineUs;
        return entry;
    }

    private void addTimers(TableToken viewToken, long nowUs) {
        final MatViewDefinition viewDefinition = dependentViewGraph.getViewDefinition(viewToken);
        if (viewDefinition == null) {
            LOG.info().$("materialized view definition not found [view=").$(viewToken).I$();
            return;
        }

        try {
            final int refreshType = viewDefinition.getRefreshType();
            if (refreshType != MatViewDefinition.REFRESH_TYPE_IMMEDIATE) {
                // The refresh is not immediate, i.e. it's either manual or timer.
                // Create a special timer that will enqueue refresh intervals update tasks.
                // We could cache the intervals right in the refresh job when there is a new base table commit,
                // but that might create many redundant WAL MAT_VIEW_INVALIDATE transactions with mat view state
                // values. To throttle refresh intervals caching, we have this special timer.
                // The end goal of this caching is unblocking WalPurgeJob to delete old WAL segments.
                createUpdateRefreshIntervalsTimer(viewDefinition, nowUs);
            }

            long timerStartUs = viewDefinition.getTimerStartUs();
            TimeZoneRules timerTzRules = viewDefinition.getTimerTzRulesUs();

            if (viewDefinition.getPeriodLength() > 0 && refreshType != MatViewDefinition.REFRESH_TYPE_MANUAL) {
                // It's a non-manual period mat view, so first add the period timer.
                createPeriodTimer(viewDefinition, nowUs);
                // "Normal" timer start is volatile in case of period mat views.
                timerStartUs = nowUs;
                timerTzRules = null;
            }

            if (refreshType == MatViewDefinition.REFRESH_TYPE_TIMER) {
                // The view has timer refresh, so add a "normal" timer for it.
                createTimer(viewDefinition, timerStartUs, timerTzRules, nowUs);
            }
        } catch (Throwable th) {
            LOG.error()
                    .$("could not initialize timer for materialized view [view=").$(viewToken)
                    .$(", ex=").$(th)
                    .I$();
        }
    }

    private void createPeriodTimer(@NotNull MatViewDefinition viewDefinition, long nowMicros) {
        final TableToken viewToken = viewDefinition.getMatViewToken();
        final long startUs = viewDefinition.getTimerStartUs();
        final int length = viewDefinition.getPeriodLength();
        final char lengthUnit = viewDefinition.getPeriodLengthUnit();
        final TimestampSampler sampler;
        try {
            sampler = TimestampSamplerFactory.getInstance(MicrosTimestampDriver.INSTANCE, length, lengthUnit, -1);
        } catch (SqlException e) {
            throw CairoException.nonCritical().put("invalid LENGTH interval and/or unit: ").put(length)
                    .put(", ").put(lengthUnit);
        }
        final int delayInterval = viewDefinition.getPeriodDelay();
        final char delayUnit = viewDefinition.getPeriodDelayUnit();
        final long delayUs = MicrosTimestampDriver.INSTANCE.from(delayInterval, delayUnit);
        final Timer periodTimer = new Timer(
                Timer.PERIOD_REFRESH_TYPE,
                viewToken,
                sampler,
                viewDefinition.getTimerTzRulesUs(),
                delayUs,
                startUs,
                nowMicros
        );
        timerQueue.add(periodTimer);
        LOG.info().$("created period timer for materialized view [view=").$(viewToken)
                .$(", start=").$ts(MicrosTimestampDriver.INSTANCE, startUs)
                .$(", tz=").$(viewDefinition.getTimerTimeZone())
                .$(", length=").$(length).$(lengthUnit)
                .$(", delay=").$(delayInterval).$(delayUnit != 0 ? delayUnit : ' ')
                .I$();
    }

    private void createTimer(
            @NotNull MatViewDefinition viewDefinition,
            long timerStartUs,
            @Nullable TimeZoneRules timerTzRules,
            long nowMicros
    ) {
        final TableToken viewToken = viewDefinition.getMatViewToken();
        final int interval = viewDefinition.getTimerInterval();
        final char unit = viewDefinition.getTimerUnit();
        final TimestampSampler sampler;
        try {
            sampler = TimestampSamplerFactory.getInstance(MicrosTimestampDriver.INSTANCE, interval, unit, -1);
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
                timerStartUs,
                nowMicros
        );
        timerQueue.add(timer);
        LOG.info().$("created timer for materialized view [view=").$(viewToken)
                .$(", start=").$ts(MicrosTimestampDriver.INSTANCE, timerStartUs)
                .$(", tz=").$(viewDefinition.getTimerTimeZone())
                .$(", interval=").$(interval).$(unit)
                .I$();
    }

    private void createUpdateRefreshIntervalsTimer(@NotNull MatViewDefinition viewDefinition, long nowUs) {
        final TableToken viewToken = viewDefinition.getMatViewToken();
        final long periodMillis = configuration.getMatViewRefreshIntervalsUpdatePeriod();
        final TimestampSampler sampler;
        try {
            sampler = TimestampSamplerFactory.getInstance(MicrosTimestampDriver.INSTANCE, periodMillis, 'T', -1);
        } catch (SqlException e) {
            throw CairoException.nonCritical().put("invalid refresh intervals update period: ").put(periodMillis);
        }
        final Timer timer = new Timer(
                Timer.UPDATE_REFRESH_INTERVALS_TYPE,
                viewToken,
                sampler,
                null,
                0,
                nowUs, // the timer should start immediately
                nowUs
        );
        timerQueue.add(timer);
        LOG.info().$("created refresh intervals update timer for materialized view [view=").$(viewToken)
                .$(", start=").$ts(MicrosTimestampDriver.INSTANCE, nowUs)
                .$(", interval=").$(periodMillis).$('T')
                .I$();
    }

    private boolean filterByDirName(Timer timer) {
        return filteredDirName != null && filteredDirName.equals(timer.getMatViewToken().getDirName());
    }

    private boolean processExpiredTimers(long nowMicros) {
        expired.clear();
        boolean ran = false;
        Timer timer;
        while ((timer = timerQueue.peek()) != null && timer.getDeadlineMicros() <= nowMicros) {
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
                            final MatViewDefinition viewDefinition = state.getViewDefinition();
                            final long periodHi = viewDefinition.getBaseTableTimestampDriver().fromMicros(timer.getPeriodHiUs()) - 1;
                            matViewStateStore.enqueueRangeRefresh(viewToken, Numbers.LONG_NULL, periodHi);
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

    /**
     * Re-drives materialized views whose incremental refresh was deferred after a transient "table
     * busy" error (see {@link MatViewRefreshJob}). Once the per-view backoff deadline elapses, an
     * incremental refresh is enqueued instead of the view being invalidated. This is the only path
     * that wakes up immediate views, which have no timer of their own.
     * <p>
     * Pending retries live in {@link #retryQueue}, a (deadline, view) min-heap fed by RETRY timer
     * tasks. Only the entries that have actually come due are popped, so a tick costs O(k log n) in
     * the number of due retries k rather than an O(V) full-fleet scan. The common no-pending-retry
     * case is a single {@code peek()} returning null.
     */
    private boolean processRefreshRetries(long nowMicros) {
        boolean ran = false;
        RetryEntry entry;
        while ((entry = retryQueue.peek()) != null && entry.deadlineUs <= nowMicros) {
            entry = retryQueue.poll();
            final TableToken viewToken = entry.viewToken;
            releaseRetryEntry(entry);
            final MatViewState state = matViewStateStore.getViewState(viewToken);
            if (state == null || state.isDropped() || state.isInvalid() || state.isPendingInvalidation()) {
                // The view went away or no longer needs re-driving; drop the stale heap entry.
                continue;
            }
            final long retryAfter = state.getRefreshRetryAfterMicros();
            if (retryAfter == Numbers.LONG_NULL) {
                // Already cleared (re-driven via another entry or reset by a successful refresh).
                continue;
            }
            if (nowMicros >= retryAfter) {
                // Clear before enqueue so a refresh that fails busy again can re-arm a fresh backoff.
                // CAS on the exact deadline we observed due: if a concurrent under-latch refresh
                // re-armed a fresher backoff (a different deadline) since we read it, the CAS fails
                // and we leave that deadline intact -- the re-arm queued its own RETRY heap entry,
                // which re-drives the view when it comes due. This closes the off-latch clobber
                // window without ever taking the view latch in the timer.
                if (state.clearRefreshRetry(retryAfter)) {
                    matViewStateStore.enqueueIncrementalRefresh(viewToken);
                    LOG.info().$("re-driving deferred materialized view refresh [view=").$(viewToken).I$();
                    ran = true;
                }
                // else: a concurrent re-arm won the race; its newer heap entry will re-drive the view.
            }
            // else: the deadline was pushed out by a later re-arm queued after this entry; the newer
            // entry is already in the heap, so this stale one is simply dropped.
        }
        return ran;
    }

    private void releaseRetryEntry(RetryEntry entry) {
        entry.viewToken = null;
        retryEntryPool.add(entry);
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
        final long nowUs = clock.getTicks();
        // check created/dropped event queue
        while (timerTaskQueue.tryDequeue(timerTask)) {
            final TableToken viewToken = timerTask.getMatViewToken();
            switch (timerTask.getOperation()) {
                case MatViewTimerTask.ADD:
                    addTimers(viewToken, nowUs);
                    break;
                case MatViewTimerTask.REMOVE:
                    removeTimers(viewToken);
                    break;
                case MatViewTimerTask.UPDATE:
                    if (removeTimers(viewToken)) {
                        addTimers(viewToken, nowUs);
                    }
                    break;
                case MatViewTimerTask.RETRY:
                    // A refresh was deferred after a transient "table busy" error. Queue a
                    // (deadline, view) entry so processRefreshRetries re-drives the view once the
                    // backoff elapses, without scanning the full view fleet.
                    retryQueue.add(acquireRetryEntry(viewToken, timerTask.getRetryAfterMicros()));
                    break;
                default:
                    LOG.error().$("unknown refresh timer operation [op=").$(timerTask.getOperation()).I$();
            }
            ran = true;
        }
        final long now = clock.getTicks();
        ran |= processExpiredTimers(now);
        ran |= processRefreshRetries(now);
        return ran;
    }

    /**
     * A pending refresh-retry heap entry: the UTC deadline at which a deferred view should be
     * re-driven, plus the view token. Pooled and reused via {@link #acquireRetryEntry} /
     * {@link #releaseRetryEntry} to avoid allocation during a retry storm.
     */
    private static class RetryEntry {
        private long deadlineUs;
        private TableToken viewToken;

        private long getDeadlineUs() {
            return deadlineUs;
        }
    }

    /**
     * May stand for either incremental refresh timer or period refresh timer.
     */
    private static class Timer {
        private static final byte INCREMENTAL_REFRESH_TYPE = 0;
        private static final byte PERIOD_REFRESH_TYPE = 1;
        private static final byte UPDATE_REFRESH_INTERVALS_TYPE = 2;
        private final long delayUs; // used in period timers
        private final TableToken matViewToken;
        private final TimestampSampler sampler;
        private final byte type;
        private final TimeZoneRules tzRules;
        private long deadlineLocalUs; // used for sampler interaction only
        private long deadlineUtcUs;
        // Holds refresh sequence number for "normal" timers
        // or caching sequence for refresh intervals update timers.
        private long knownSeq = -1;

        public Timer(
                byte type,
                @NotNull TableToken matViewToken,
                @NotNull TimestampSampler sampler,
                @Nullable TimeZoneRules tzRules,
                long delayUs,
                long startUs,
                long nowUs
        ) {
            assert ColumnType.isTimestampMicro(sampler.getTimestampType());

            this.type = type;
            this.matViewToken = matViewToken;
            this.sampler = sampler;
            this.tzRules = tzRules;
            this.delayUs = delayUs;
            sampler.setStart(startUs);
            final long nowLocalUs = toLocal(nowUs, tzRules);
            switch (type) {
                case INCREMENTAL_REFRESH_TYPE:
                case UPDATE_REFRESH_INTERVALS_TYPE:
                    // It's fine if the timer triggers immediately.
                    deadlineLocalUs = nowLocalUs > startUs ? sampler.nextTimestamp(sampler.round(nowLocalUs - 1)) : startUs;
                    break;
                case PERIOD_REFRESH_TYPE:
                    // Unlike with incremental timer views, we want to trigger the timer
                    // for all complete periods, if they exist.
                    deadlineLocalUs = nowLocalUs > startUs ? sampler.round(nowLocalUs) : startUs;
                    break;
                default:
                    throw new IllegalStateException("unexpected timer type: " + type);
            }
            deadlineUtcUs = toUtc(deadlineLocalUs, tzRules);
        }

        public long getDeadlineMicros() {
            return deadlineUtcUs + delayUs;
        }

        public long getKnownSeq() {
            return knownSeq;
        }

        public TableToken getMatViewToken() {
            return matViewToken;
        }

        // returns currently awaited period's right boundary, in UTC
        public long getPeriodHiUs() {
            return deadlineUtcUs;
        }

        public byte getType() {
            return type;
        }

        public void setKnownSeq(long knownSeq) {
            this.knownSeq = knownSeq;
        }

        private static long toLocal(long utcTimeUs, TimeZoneRules tzRules) {
            return tzRules != null ? utcTimeUs + tzRules.getOffset(utcTimeUs) : utcTimeUs;
        }

        private static long toUtc(long localTimeUs, TimeZoneRules tzRules) {
            return tzRules != null ? localTimeUs - tzRules.getOffset(localTimeUs) : localTimeUs;
        }

        private void nextDeadline() {
            deadlineLocalUs = sampler.nextTimestamp(deadlineLocalUs);
            deadlineUtcUs = toUtc(deadlineLocalUs, tzRules);
        }
    }
}
