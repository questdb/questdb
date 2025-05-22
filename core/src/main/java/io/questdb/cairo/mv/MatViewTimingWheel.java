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
 * A timer data structure optimized for approximated scheduling, a.k.a.
 * hashed timing wheel.
 * <p>
 * Based on George Varghese and Tony Lauck's paper, "Hashed and Hierarchical
 * Timing Wheels: data structures to efficiently implement a timer facility".
 */
public class MatViewTimingWheel {
    private final MicrosecondClock clock;
    private final int mask;
    private final ObjList<Timer> pendingTimers = new ObjList<>();
    private final long tickDuration;
    private final Bucket[] wheel;
    private long tickDeadline;

    public MatViewTimingWheel(MicrosecondClock clock, long tickDuration, int ticksPerWheel) {
        this.clock = clock;
        this.tickDuration = tickDuration;
        this.wheel = new Bucket[Numbers.ceilPow2(ticksPerWheel)];
        for (int i = 0, n = wheel.length; i < n; i++) {
            wheel[i] = new Bucket();
        }
        this.mask = wheel.length - 1;
        final long now = clock.getTicks();
        this.tickDeadline = now - now % tickDuration;
    }

    public Timer addTimer(MatViewDefinition viewDefinition) {
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

        if (clock.getTicks() < tickDeadline) {
            return false;
        }

        final long tick = tickDeadline;
        final long tickIdx = tick / tickDuration;
        final int idx = (int) (tickIdx & mask);
        final Bucket bucket = wheel[idx];
        addTimers(pendingTimers, tickIdx, tick);
        pendingTimers.clear();
        bucket.expireTimers(expired, tick);
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
            final Bucket bucket = wheel[stopIndex];
            bucket.addTimer(timer);
        }
    }

    private static class Bucket {
        private Timer head;
        private Timer tail;

        // the list is kept sorted to speed up expiration checks
        public void addTimer(Timer timer) {
            assert timer.bucket == null;

            timer.bucket = this;
            if (head == null) {
                this.head = this.tail = timer;
            } else if (head.deadline >= timer.deadline) {
                head.prev = timer;
                timer.next = head;
                this.head = timer;
            } else if (tail.deadline <= timer.deadline) {
                tail.next = timer;
                timer.prev = tail;
                this.tail = timer;
            } else {
                for (Timer t = head.next; t != null; t = t.next) {
                    if (t.deadline >= timer.deadline) {
                        timer.next = t;
                        timer.prev = t.prev;
                        t.prev.next = timer;
                        t.prev = timer;
                        break;
                    }
                }
            }
        }

        public void expireTimers(ObjList<Timer> expired, long now) {
            Timer next;
            for (Timer timer = head; timer != null; timer = next) {
                if (timer.deadline <= now) {
                    next = removeTimer(timer);
                    expired.add(timer);
                } else {
                    break;
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
            sampler.setStart(viewDefinition.getIntervalStart());
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
            if (deadline != Long.MIN_VALUE) {
                deadline = sampler.nextTimestamp(deadline);
            } else {
                // The timer is added for the first time, so it's fine if it triggers immediately.
                deadline = now > viewDefinition.getIntervalStart()
                        ? sampler.nextTimestamp(sampler.round(now - 1))
                        : viewDefinition.getIntervalStart();
            }
            return deadline;
        }
    }
}
