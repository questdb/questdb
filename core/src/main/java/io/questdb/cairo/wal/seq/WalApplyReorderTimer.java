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

package io.questdb.cairo.wal.seq;

import io.questdb.cairo.CairoEngine;
import io.questdb.cairo.CairoException;
import io.questdb.cairo.TableToken;
import io.questdb.mp.continuation.DelayedFireable;
import io.questdb.mp.continuation.TimerShards;
import io.questdb.std.Unsafe;
import io.questdb.std.datetime.MicrosecondClock;
import org.jetbrains.annotations.NotNull;

import java.util.concurrent.Delayed;
import java.util.concurrent.TimeUnit;

public final class WalApplyReorderTimer implements DelayedFireable {
    static final int STATE_CANCELLED = 2;
    static final int STATE_FIRED = 1;
    static final int STATE_PENDING = 0;
    private static final long STATE_OFFSET = Unsafe.getFieldOffset(WalApplyReorderTimer.class, "state");
    private final long deadlineMicros;
    private final CairoEngine engine;
    private final long generation;
    private final MicrosecondClock microClock;
    private final TableToken tableToken;
    private final TimerShards timerShards;
    private final SeqTxnTracker tracker;
    @SuppressWarnings("FieldMayBeFinal")
    private volatile int state = STATE_PENDING;

    public WalApplyReorderTimer(
            CairoEngine engine,
            TableToken tableToken,
            SeqTxnTracker tracker,
            long generation,
            long deadlineMicros
    ) {
        this.engine = engine;
        this.tableToken = tableToken;
        this.tracker = tracker;
        this.generation = generation;
        this.deadlineMicros = deadlineMicros;
        this.microClock = engine.getConfiguration().getMicrosecondClock();
        this.timerShards = engine.getTimerShards();
    }

    public void cancel() {
        if (Unsafe.cas(this, STATE_OFFSET, STATE_PENDING, STATE_CANCELLED)) {
            timerShards.unregister(this);
        }
    }

    @Override
    public int compareTo(@NotNull Delayed other) {
        return Long.compare(getDelay(TimeUnit.NANOSECONDS), other.getDelay(TimeUnit.NANOSECONDS));
    }

    @Override
    public void expire() {
        if (Unsafe.cas(this, STATE_OFFSET, STATE_PENDING, STATE_FIRED)
                && tracker.releaseReorderWindow(generation)) {
            final TableToken updatedToken = engine.getUpdatedTableToken(tableToken);
            if (!engine.isClosing()
                    && updatedToken != null
                    && !engine.isTableDropped(tableToken)
                    && !engine.isWalApplySuspended(updatedToken)) {
                engine.notifyWalTxnCommitted(updatedToken);
            }
        }
    }

    @Override
    public long getDelay(@NotNull TimeUnit unit) {
        final long now = microClock.getTicks();
        // Clock ticks are non-negative; deadlineMicros may be Long.MAX_VALUE via saturating add, so subtraction cannot overflow.
        final long remaining = Math.max(0, deadlineMicros - now);
        return unit.convert(remaining, TimeUnit.MICROSECONDS);
    }

    public boolean isCancelled() {
        return state == STATE_CANCELLED;
    }

    public void register() {
        try {
            timerShards.register(this);
        } catch (CairoException ex) {
            if (!engine.isClosing()) {
                throw ex;
            }
            shutdown();
        }
        if (state != STATE_PENDING) {
            timerShards.unregister(this);
        }
    }

    @Override
    public void shutdown() {
        if (Unsafe.cas(this, STATE_OFFSET, STATE_PENDING, STATE_CANCELLED)) {
            tracker.clearReorderTimerOnShutdown(generation, this);
        }
    }
}
