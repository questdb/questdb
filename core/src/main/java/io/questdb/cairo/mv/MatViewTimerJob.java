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
import io.questdb.log.LogRecord;
import io.questdb.mp.Queue;
import io.questdb.mp.SynchronizedJob;
import io.questdb.std.Numbers;
import io.questdb.std.ObjList;
import io.questdb.std.datetime.MicrosecondClock;
import io.questdb.std.datetime.TimeZoneRules;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.jetbrains.annotations.TestOnly;

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
    // Consecutive firings a timer may find its view unrefreshed before the job says so. Firings are
    // one timer interval apart, and the shortest interval a user can set is a minute, so three is
    // long enough that a single slow refresh does not trip it.
    private static final int MISSED_FIRINGS_REPORT_THRESHOLD = 3;
    // A gap this wide between two ticks means the job did not run: a healthy worker sleeps at most
    // worker.sleep.timeout (10ms by default) between passes. One minute is also the shortest
    // interval a timer view can carry, so a gap this wide has already cost a scheduled refresh.
    private static final long TICK_GAP_STALL_THRESHOLD_NANOS = 60_000_000_000L;
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
    // Monotonic timestamp of the previous tick, Numbers.LONG_NULL until the job runs for the first
    // time. See reportTickGap().
    private long lastTickNanos = Numbers.LONG_NULL;
    private int removedTimerCount; // temporary value used by filterByDirName
    @TestOnly
    private long tickGapStallThresholdNanos = TICK_GAP_STALL_THRESHOLD_NANOS;

    public MatViewTimerJob(CairoEngine engine) {
        this.configuration = engine.getConfiguration();
        this.clock = configuration.getMicrosecondClock();
        this.timerTaskQueue = engine.getMatViewTimerQueue();
        this.dependentViewGraph = engine.getDependentViewGraph();
        this.matViewStateStore = engine.getMatViewStateStore();
        this.filterByDirName = this::filterByDirName;
    }

    /**
     * Test seam: the stall report measures real elapsed time, which no test can afford to wait a
     * minute of. Lowering the threshold makes the report reachable; raising it suppresses it.
     */
    @TestOnly
    public void setTickGapStallThresholdForTesting(long thresholdNanos) {
        this.tickGapStallThresholdNanos = thresholdNanos;
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

    /**
     * Publishes a change in the number of timers this job holds for the view onto its
     * {@link MatViewState}, where {@code materialized_views()} reads it as {@code timers_registered}.
     * The state is missing for a view that has already been dropped, in which case there is nothing
     * left to report to.
     */
    private void addRegisteredTimers(TableToken viewToken, int delta) {
        if (delta == 0) {
            return;
        }
        final MatViewState state = matViewStateStore.getViewState(viewToken);
        if (state != null) {
            state.addRegisteredTimers(delta);
        }
    }

    private void addTimers(TableToken viewToken, long nowUs) {
        final MatViewDefinition viewDefinition = dependentViewGraph.getViewDefinition(viewToken);
        if (viewDefinition == null) {
            LOG.info().$("materialized view definition not found [view=").$(viewToken).I$();
            return;
        }

        // Counts what this registration actually created, so a throw part way through publishes the
        // partial set rather than the intended one: materialized_views() then shows a timer view
        // with one timer instead of two, which is the whole point of the column.
        int created = 0;
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
                created++;
            }

            long timerStartUs = viewDefinition.getTimerStartUs();
            TimeZoneRules timerTzRules = viewDefinition.getTimerTzRulesUs();

            if (viewDefinition.getPeriodLength() > 0 && refreshType != MatViewDefinition.REFRESH_TYPE_MANUAL) {
                // It's a non-manual period mat view, so first add the period timer.
                createPeriodTimer(viewDefinition, nowUs);
                created++;
                // "Normal" timer start is volatile in case of period mat views.
                timerStartUs = nowUs;
                timerTzRules = null;
            }

            if (refreshType == MatViewDefinition.REFRESH_TYPE_TIMER) {
                // The view has timer refresh, so add a "normal" timer for it.
                createTimer(viewDefinition, timerStartUs, timerTzRules, nowUs);
                created++;
            }

            if (created == 0) {
                // An immediate, non-period view refreshes off base table commits alone, so it owns no
                // timers. Say so: zero timers is the expected reading here, and the same reading on
                // any other refresh type means the view is scheduled by nothing.
                LOG.info().$("materialized view requires no timers [view=").$(viewToken).I$();
            }
        } catch (Throwable th) {
            LOG.error()
                    .$("could not initialize timer for materialized view [view=").$(viewToken)
                    .$(", ex=").$(th)
                    .I$();
        } finally {
            addRegisteredTimers(viewToken, created);
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
        if (filteredDirName == null || !filteredDirName.equals(timer.getMatViewToken().getDirName())) {
            return false;
        }
        // removeIf() calls this predicate exactly once per timer and removes every one it accepts,
        // so counting the matches here counts the removals.
        removedTimerCount++;
        return true;
    }

    private boolean processExpiredTimers(long nowMicros) {
        expired.clear();
        boolean ran = false;
        try {
            Timer timer;
            while ((timer = timerQueue.peek()) != null && timer.getDeadlineMicros() <= nowMicros) {
                timer = timerQueue.poll();
                expired.add(timer);
                final TableToken viewToken = timer.getMatViewToken();
                final MatViewState state = matViewStateStore.getViewState(viewToken);
                if (state != null) {
                    if (state.isDropped()) {
                        expired.remove(expired.size() - 1);
                        state.addRegisteredTimers(-1);
                        LOG.info().$("unregistered timer for dropped materialized view [view=").$(viewToken)
                                .$(", type=").$(timer.getType())
                                .I$();
                    } else if (!state.hasPendingInvalidationReason() && !state.isInvalid()) {
                        switch (timer.getType()) {
                            case Timer.INCREMENTAL_REFRESH_TYPE:
                                // Check if the view has refreshed since the last timer expiration.
                                // If not, don't schedule refresh to avoid unbounded growth of the queue.
                                final long refreshSeq = state.getRefreshSeq();
                                if (timer.getKnownSeq() != refreshSeq) {
                                    matViewStateStore.enqueueIncrementalRefresh(viewToken);
                                    timer.setKnownSeq(refreshSeq);
                                    reportFiring(timer, viewToken);
                                } else {
                                    // The refresh the previous firing enqueued has not completed yet, so
                                    // this firing schedules nothing. Steady state for a view whose refreshes
                                    // cannot keep up with its timer, or whose refresh job is backed up.
                                    reportMissedFiring(timer, viewToken, "previous refresh has not completed");
                                }
                                break;
                            case Timer.PERIOD_REFRESH_TYPE:
                                // range hi boundary is inclusive
                                final MatViewDefinition viewDefinition = state.getViewDefinition();
                                final long periodHi = viewDefinition.getBaseTableTimestampDriver().fromMicros(timer.getPeriodHiUs()) - 1;
                                matViewStateStore.enqueueRangeRefresh(viewToken, Numbers.LONG_NULL, periodHi);
                                reportFiring(timer, viewToken);
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
                    } else {
                        reportMissedFiring(timer, viewToken, state.isInvalid() ? "view is invalid" : "view is pending invalidation");
                    }
                } else {
                    LOG.info().$("state for materialized view not found [view=").$(viewToken).I$();
                }
                ran = true;
            }
        } finally {
            // Re-schedule expired timers. The finally is load-bearing: the loop above has already
            // polled these timers out of timerQueue, so a throw part way through, e.g. an enqueue
            // that fails to grow its queue, would otherwise drop every timer it had taken so far.
            // Nothing re-creates them, so the views they drive would stop refreshing until restart.
            rescheduleExpiredTimers();
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
            if (state == null || state.isDropped() || state.isInvalid() || state.hasPendingInvalidationReason()) {
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

    // Returns nothing on purpose: no caller may branch on whether the view owned timers. An
    // immediate, non-period view legitimately owns none, and gating the follow-up addTimers() on
    // this having found something is what stranded such a view with no scheduler after an ALTER.
    private void removeTimers(TableToken viewToken) {
        filteredDirName = viewToken.getDirName();
        removedTimerCount = 0;
        final boolean isRemoved;
        try {
            // Remove all timers for the given view, if any.
            isRemoved = timerQueue.removeIf(filterByDirName);
        } finally {
            filteredDirName = null;
        }
        addRegisteredTimers(viewToken, -removedTimerCount);
        if (isRemoved) {
            LOG.info().$("unregistered timers for materialized view [view=").$(viewToken)
                    .$(", count=").$(removedTimerCount)
                    .I$();
        } else {
            LOG.info().$("timers for materialized view not found [view=").$(viewToken).I$();
        }
    }

    /**
     * Reports a timer firing that did schedule work, closing out a stretch of missed firings if the
     * view was in one. Edge-triggered, like {@link #reportMissedFiring}: one line when the view
     * falls behind, one when it comes back.
     */
    private void reportFiring(Timer timer, TableToken viewToken) {
        final int missedFirings = timer.resetMissedFirings();
        if (missedFirings >= MISSED_FIRINGS_REPORT_THRESHOLD) {
            LOG.info().$("materialized view timer is scheduling refreshes again [view=").$(viewToken)
                    .$(", type=").$(timer.getType())
                    .$(", missedFirings=").$(missedFirings)
                    .I$();
        }
    }

    /**
     * Reports a timer firing that scheduled nothing. Level-triggered logging would repeat every
     * interval for as long as the view stays invalid or backed up, and say nothing the first line
     * did not, so this logs once per stretch: when the count reaches the threshold, and never again
     * until {@link #reportFiring} resets it.
     */
    private void reportMissedFiring(Timer timer, TableToken viewToken, String reason) {
        if (timer.getType() == Timer.UPDATE_REFRESH_INTERVALS_TYPE) {
            // This timer only caches WAL txn intervals, and skipping it costs base table WAL
            // retention rather than freshness. Its skip is also the common case: no new base table
            // transactions since the last caching.
            return;
        }
        if (timer.incrementMissedFirings() == MISSED_FIRINGS_REPORT_THRESHOLD) {
            LOG.info().$("materialized view has not refreshed across ").$(MISSED_FIRINGS_REPORT_THRESHOLD)
                    .$(" timer firings [view=").$(viewToken)
                    .$(", type=").$(timer.getType())
                    .$(", reason=").$(reason)
                    .I$();
        }
    }

    /**
     * Reports how long the job went without a tick. A starved job cannot report itself while it is
     * starved -- it is not running -- but it can report the gap the moment it resumes, and that is
     * the one in-process signal that tells a starved job apart from an idle one. The job is assigned
     * behind {@link MatViewRefreshJob} on every worker of the mat view pool, so an unbounded drain
     * there keeps it from ticking at all, and every timer and period view goes unscheduled for as
     * long as it lasts.
     * <p>
     * Measured with {@link System#nanoTime()} rather than the configured clock: liveness is a
     * real-time property, and the configured clock is settable, so tests that jump it forward would
     * otherwise report stalls that never happened.
     */
    private void reportTickGap() {
        final long nowNanos = System.nanoTime();
        final long prevTickNanos = lastTickNanos;
        lastTickNanos = nowNanos;
        if (prevTickNanos != Numbers.LONG_NULL && nowNanos - prevTickNanos > tickGapStallThresholdNanos) {
            LOG.error().$("materialized view timer job resumed after a long pause, no timers fired meanwhile [pauseMs=")
                    .$((nowNanos - prevTickNanos) / 1_000_000)
                    .$(", timers=").$(timerQueue.size())
                    .I$();
        }
    }

    /**
     * Puts every timer the current tick polled out of {@link #timerQueue} back, advanced to its next
     * deadline. Each timer is re-added under its own try/catch so that one failing to compute a next
     * deadline costs only that timer rather than every timer left in the batch.
     */
    private void rescheduleExpiredTimers() {
        for (int i = 0, n = expired.size(); i < n; i++) {
            final Timer timer = expired.getQuick(i);
            try {
                timer.nextDeadline();
                timerQueue.add(timer);
            } catch (Throwable th) {
                // The timer is gone for good, so take it off the view's registered count: a timer view
                // reporting one timer instead of two is how materialized_views() surfaces this.
                // Guarded on its own, since this runs on the way out of a failed re-schedule.
                try {
                    addRegisteredTimers(timer.getMatViewToken(), -1);
                } catch (Throwable ignore) {
                }
                // processExpiredTimers() calls this method from a finally, so the logging carries its
                // own swallow: a throw escaping here would replace the in-flight exception AND abandon
                // every timer this loop has not put back yet -- the batch loss the finally exists to
                // prevent. AsyncLogRecord.$(Throwable) releases the log ring slot and RETHROWS when
                // formatting `th` fails, which an OutOfMemoryError can do in exactly the OOM case this
                // catch exists for. In this chain $(Sinkable) (the TableToken) and $(Throwable)
                // self-release the slot; $(CharSequence) and $(int) do not -- getType() returns byte,
                // which widens to int -- so the trailing rec.I$() returns it if one of THOSE throws,
                // and I$() no-ops unless a record is in progress, so it cannot double-release.
                // PostingIndexWriter.close() documents this pattern at length.
                // error(), not critical(): critical() routes through Sequence.nextBully(), which spins
                // until a ring slot frees. ServerMain.setupMatViewJobs assigns this job to the dedicated
                // mat view pool that also runs MatViewRefreshJob, so under the saturated log an OOM
                // produces, critical() would park that pool, refreshes included, rather than drop a
                // line. A lost timer stalls one view's refresh until restart or an ALTER; it corrupts
                // nothing, and materialized_views() keeps reporting view_status='valid', with only the
                // last_refresh timestamps standing still.
                try {
                    LogRecord rec = LOG.error();
                    try {
                        rec.$("could not re-schedule timer for materialized view [view=").$(timer.getMatViewToken())
                                .$(", type=").$(timer.getType())
                                .$(", ex=").$(th);
                    } catch (Throwable ignore) {
                    }
                    rec.I$();
                } catch (Throwable ignore) {
                }
            }
        }
        expired.clear();
    }

    @Override
    protected boolean runSerially() {
        boolean ran = false;
        reportTickGap();
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
                    // Re-register unconditionally. A view that currently owns no timers, i.e. an
                    // immediate, non-period one, still needs them the moment an ALTER turns it into
                    // a timer, period or manual view. Gating the add on the remove having found
                    // something strands exactly that view with no scheduler at all, and since timers
                    // live only in this job's heap nothing but a restart brings them back.
                    // The opposite direction stays correct too: addTimers() is a no-op for an
                    // immediate, non-period definition, and the remove always runs first, so no
                    // duplicate timers are possible.
                    removeTimers(viewToken);
                    addTimers(viewToken, nowUs);
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
        // Consecutive firings that scheduled nothing, e.g. because the view is invalid or its
        // previous refresh has not completed. Reset by the first firing that does schedule work.
        private int missedFirings;

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

        public int incrementMissedFirings() {
            return ++missedFirings;
        }

        public int resetMissedFirings() {
            final int missedFirings = this.missedFirings;
            this.missedFirings = 0;
            return missedFirings;
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
