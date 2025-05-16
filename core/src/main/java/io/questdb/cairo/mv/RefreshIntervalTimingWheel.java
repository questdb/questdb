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
import io.questdb.griffin.SqlException;
import io.questdb.griffin.engine.groupby.TimestampSampler;
import io.questdb.griffin.engine.groupby.TimestampSamplerFactory;
import io.questdb.std.Numbers;
import io.questdb.std.ObjList;
import io.questdb.std.datetime.microtime.MicrosecondClock;

/**
 * A refresh interval timer optimized for approximated scheduling, a.k.a.
 * hashed timing wheel.
 * <p>
 * Based on George Varghese and Tony Lauck's paper, "Hashed and Hierarchical
 * Timing Wheels: data structures to efficiently implement a timer facility".
 */
// TODOs:
// - 1 sec interval
// - deadlines should expire within the bucket
// - sort timers within bucket
// - is tail needed?
public class RefreshIntervalTimingWheel {
    private final MicrosecondClock clock;
    private final int mask;
    private final ObjList<Timer> pendingTimers = new ObjList<>();
    private final long tickDuration;
    private final Bucket[] wheel;
    private long nextTick;

    public RefreshIntervalTimingWheel(MicrosecondClock clock, long tickDuration, int ticksPerWheel) {
        this.clock = clock;
        this.tickDuration = tickDuration;
        this.wheel = new Bucket[Numbers.ceilPow2(ticksPerWheel)];
        for (int i = 0, n = wheel.length; i < n; i++) {
            wheel[i] = new Bucket();
        }
        this.mask = wheel.length - 1;
        final long now = clock.getTicks();
        this.nextTick = now - now % tickDuration;
    }

    public Timer addRefreshInterval(MatViewDefinition viewDefinition) {
        final TimestampSampler sampler;
        try {
            sampler = TimestampSamplerFactory.getInstance(viewDefinition.getIntervalStride(), viewDefinition.getIntervalUnit(), 0);
        } catch (SqlException e) {
            throw CairoException.critical(0).put("invalid EVERY interval and/or unit: ").put(viewDefinition.getIntervalStride())
                    .put(", ").put(viewDefinition.getIntervalUnit());
        }
        final Timer timer = new Timer(viewDefinition, sampler);
        pendingTimers.add(timer);
        return timer;
    }

    public boolean tick(ObjList<Timer> expired) {
        expired.clear();

        final long now = clock.getTicks();
        if (now < nextTick) {
            return false;
        }

        final long tickIdx = now / tickDuration;
        final int idx = (int) (tickIdx & mask);
        final Bucket bucket = wheel[idx];
        addTimers(pendingTimers, tickIdx, now);
        pendingTimers.clear();
        bucket.expireTimers(expired, now);
        // reschedule expired timers
        addTimers(expired, tickIdx, now);
        nextTick = (now - now % tickDuration) + tickDuration;
        return true;
    }

    private void addTimers(ObjList<Timer> timers, long tickIdx, long now) {
        for (int i = 0, n = timers.size(); i < n; i++) {
            final Timer timer = timers.get(i);
            final long deadline = timer.nextDeadline(now);
            final long calculated = deadline / tickDuration;
            final long ticks = Math.max(calculated, tickIdx);
            final int stopIndex = (int) (ticks & mask);
            final Bucket bucket = wheel[stopIndex];
            bucket.addTimer(timer);
        }
    }

    private static class Bucket {
        private Timer head;
        private Timer tail;

        public void addTimer(Timer timer) {
            assert timer.bucket == null;

            timer.bucket = this;
            if (head == null) {
                this.head = this.tail = timer;
            } else {
                tail.next = timer;
                timer.prev = tail;
                this.tail = timer;
            }
        }

        public void expireTimers(ObjList<Timer> expired, long now) {
            Timer next;
            for (Timer timer = head; timer != null; timer = next) {
                next = timer.next;
                // TODO(puzpuzpuz): keep the list sorted
                if (timer.deadline <= now) {
                    next = removeTimer(timer);
                    expired.add(timer);
                }
            }
        }

        public Timer removeTimer(Timer timer) {
            Timer next = timer.next;
            if (timer.prev != null) {
                timer.prev.next = next;
            }

            if (timer.next != null) {
                timer.next.prev = timer.prev;
            }

            if (timer == head) {
                if (timer == tail) {
                    this.tail = null;
                    this.head = null;
                } else {
                    this.head = next;
                }
            } else if (timer == tail) {
                this.tail = timer.prev;
            }

            timer.prev = null;
            timer.next = null;
            timer.bucket = null;
            return next;
        }
    }

    public static class Timer {
        private final TimestampSampler sampler;
        private final MatViewDefinition viewDefinition;
        private Bucket bucket;
        private long deadline = Long.MIN_VALUE;
        private Timer next;
        private Timer prev;

        private Timer(MatViewDefinition viewDefinition, TimestampSampler sampler) {
            this.viewDefinition = viewDefinition;
            this.sampler = sampler;
        }

        public MatViewDefinition getViewDefinition() {
            return viewDefinition;
        }

        public void remove() {
            if (bucket != null) {
                bucket.removeTimer(this);
            }
        }

        private long nextDeadline(long now) {
            if (deadline == Long.MIN_VALUE) {
                sampler.setStart(viewDefinition.getIntervalStart());
                deadline = sampler.round(now);
            } else {
                deadline = sampler.nextTimestamp(now);
            }
            return deadline;
        }
    }
}
