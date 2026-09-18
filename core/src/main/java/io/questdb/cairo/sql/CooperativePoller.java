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

package io.questdb.cairo.sql;

import io.questdb.cairo.CairoEngine;
import io.questdb.std.datetime.NanosecondClock;
import org.jetbrains.annotations.Nullable;

import static io.questdb.cairo.sql.SqlExecutionCircuitBreaker.COOPERATIVE_POLL_INTERVAL_NANOS;
import static io.questdb.cairo.sql.SqlExecutionCircuitBreaker.COOPERATIVE_POLL_STRIDE;
import static io.questdb.cairo.sql.SqlExecutionCircuitBreaker.STATEFUL_COOPERATIVE_POLL_STRIDE;

/**
 * Cooperative poll cadence of the {@code ...OrYield} breaker variants; null on engines that do
 * not enable cooperative polling.
 */
final class CooperativePoller {
    private final NanosecondClock clock;
    private final CairoEngine engine;
    private int countdown;
    private long lastPollNanos = Long.MIN_VALUE;

    private CooperativePoller(CairoEngine engine) {
        this.clock = engine.getConfiguration().getNanosecondClock();
        this.engine = engine;
    }

    static @Nullable CooperativePoller newInstance(CairoEngine engine) {
        return engine.isSqlExecutionCooperativePollingEnabled() ? new CooperativePoller(engine) : null;
    }

    void poll() {
        if (countdown == 0) {
            countdown = COOPERATIVE_POLL_STRIDE;
            engine.onSqlExecutionCooperativePoll();
        }
        countdown--;
    }

    // Large throttles sample the clock on real breaker checks and at stride boundaries only, and
    // then poll at most once per COOPERATIVE_POLL_INTERVAL_NANOS.
    void pollStateful(int visitCount, int throttle) {
        if (throttle <= STATEFUL_COOPERATIVE_POLL_STRIDE) {
            poll();
        } else if ((visitCount & (STATEFUL_COOPERATIVE_POLL_STRIDE - 1)) == 0 || visitCount >= throttle) {
            pollTimed();
        }
    }

    void reset() {
        countdown = 0;
        lastPollNanos = Long.MIN_VALUE;
    }

    private void pollTimed() {
        final long now = clock.getTicks();
        if (lastPollNanos == Long.MIN_VALUE
                || now < lastPollNanos
                || now - lastPollNanos >= COOPERATIVE_POLL_INTERVAL_NANOS) {
            lastPollNanos = now;
            engine.onSqlExecutionCooperativePoll();
        }
    }
}
