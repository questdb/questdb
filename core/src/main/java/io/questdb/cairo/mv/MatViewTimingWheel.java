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

import io.questdb.cairo.CairoException;
import io.questdb.cairo.TableToken;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.engine.groupby.TimestampSampler;
import io.questdb.griffin.engine.groupby.TimestampSamplerFactory;
import io.questdb.std.Numbers;
import io.questdb.std.ObjList;
import io.questdb.std.datetime.microtime.MicrosecondClock;
import org.jetbrains.annotations.NotNull;

import java.util.Comparator;
import java.util.PriorityQueue;

/**
 * A timer data structure optimized for approximated scheduling, a.k.a.
 * hashed timing wheel.
 * <p>
 * Based on George Varghese and Tony Lauck's paper, "Hashed and Hierarchical
 * Timing Wheels: data structures to efficiently implement a timer facility".
 */
public class MatViewTimingWheel {
    private static final int DEFAULT_QUEUE_CAPACITY = 16;
    private static final Comparator<Timer> timerComparator = Comparator.comparingLong(t -> t.deadline);
    private final MicrosecondClock clock;
    private final int mask;
    private final ObjList<Timer> pendingTimers = new ObjList<>();
    private final long tickDuration;
    private final PriorityQueue<Timer>[] wheel;
    private long tickDeadline;

    @SuppressWarnings("unchecked")
    public MatViewTimingWheel(MicrosecondClock clock, long tickDuration, int ticksPerWheel) {
        this.clock = clock;
        this.tickDuration = tickDuration;
        this.wheel = (PriorityQueue<Timer>[]) new PriorityQueue[Numbers.ceilPow2(ticksPerWheel)];
        for (int i = 0, n = wheel.length; i < n; i++) {
            wheel[i] = new PriorityQueue<>(DEFAULT_QUEUE_CAPACITY, timerComparator);
        }
        this.mask = wheel.length - 1;
        final long now = clock.getTicks();
        this.tickDeadline = now - now % tickDuration;
    }

    public Timer addTimer(@NotNull MatViewDefinition viewDefinition) {
        final TimestampSampler sampler;
        try {
            sampler = TimestampSamplerFactory.getInstance(viewDefinition.getTimerInterval(), viewDefinition.getTimerIntervalUnit(), 0);
        } catch (SqlException e) {
            throw CairoException.critical(0).put("invalid EVERY interval and/or unit: ").put(viewDefinition.getTimerInterval())
                    .put(", ").put(viewDefinition.getTimerIntervalUnit());
        }
        final Timer timer = new Timer(viewDefinition.getMatViewToken(), sampler, viewDefinition.getTimerStart());
        pendingTimers.add(timer);
        return timer;
    }

    public void expireTimers(PriorityQueue<Timer> bucket, ObjList<Timer> expired, long now) {
        Timer timer;
        while ((timer = bucket.peek()) != null) {
            if (timer.deadline <= now) {
                expired.add(bucket.poll());
            } else {
                break;
            }
        }
    }

    public boolean tick(ObjList<Timer> expired) {
        expired.clear();

        if (clock.getTicks() < tickDeadline) {
            return false;
        }

        final long tick = tickDeadline;
        final long tickIdx = tick / tickDuration;
        final int idx = (int) (tickIdx & mask);
        addTimers(pendingTimers, tickIdx, tick);
        pendingTimers.clear();
        expireTimers(wheel[idx], expired, tick);
        // reschedule expired timers
        addTimers(expired, tickIdx, tick);
        tickDeadline += tickDuration;
        return true;
    }

    private void addTimers(ObjList<Timer> timers, long tickIdx, long now) {
        for (int i = 0, n = timers.size(); i < n; i++) {
            final Timer timer = timers.get(i);
            final long deadline = timer.nextDeadline(now);
            final long calculated = deadline / tickDuration;
            final long ticks = Math.max(calculated, tickIdx);
            final int stopIndex = (int) (ticks & mask);
            final PriorityQueue<Timer> bucket = wheel[stopIndex];
            bucket.add(timer);
            timer.bucket = bucket;
        }
    }

    public static class Timer {
        private final TableToken matViewToken;
        private final TimestampSampler sampler;
        private final long start;
        private PriorityQueue<Timer> bucket;
        private long deadline = Long.MIN_VALUE;
        private long knownRefreshSeq = -1;

        private Timer(@NotNull TableToken matViewToken, @NotNull TimestampSampler sampler, long start) {
            this.matViewToken = matViewToken;
            this.start = start;
            this.sampler = sampler;
            sampler.setStart(start);
        }

        @Override
        public final boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (!(o instanceof Timer)) {
                return false;
            }

            // bucket.remove() does bin search based on deadlines before the equality check,
            // hence the deadline should be included into equals/hashCode.
            Timer timer = (Timer) o;
            return deadline == timer.deadline && matViewToken.equals(timer.matViewToken);
        }

        public long getKnownRefreshSeq() {
            return knownRefreshSeq;
        }

        public TableToken getMatViewToken() {
            return matViewToken;
        }

        @Override
        public int hashCode() {
            int result = matViewToken.hashCode();
            result = 31 * result + Long.hashCode(deadline);
            return result;
        }

        public void remove() {
            if (bucket != null) {
                bucket.remove(this);
                bucket = null;
            }
        }

        public void setKnownRefreshSeq(long knownRefreshSeq) {
            this.knownRefreshSeq = knownRefreshSeq;
        }

        private long nextDeadline(long now) {
            if (deadline != Long.MIN_VALUE) {
                deadline = sampler.nextTimestamp(deadline);
            } else {
                // The timer is added for the first time, so it's fine if it triggers immediately.
                deadline = now > start ? sampler.nextTimestamp(sampler.round(now - 1)) : start;
            }
            return deadline;
        }
    }
}
