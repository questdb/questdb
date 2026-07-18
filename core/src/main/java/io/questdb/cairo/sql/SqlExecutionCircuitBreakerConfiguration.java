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

import io.questdb.network.NetworkFacade;
import io.questdb.std.datetime.millitime.MillisecondClock;
import org.jetbrains.annotations.NotNull;

public interface SqlExecutionCircuitBreakerConfiguration {

    boolean checkConnection();

    int getCircuitBreakerThrottle();

    /**
     * Minimum wall-clock interval, in milliseconds, between heavy connection probes performed by
     * {@link SqlExecutionCircuitBreaker#statefulThrowExceptionIfTrippedTimeThrottled()},
     * {@link SqlExecutionCircuitBreaker#checkIfTripped(long, long)} and
     * {@link SqlExecutionCircuitBreaker#getState(long, long)}. Cancellation and timeout are still
     * checked on every call; only the hangup-poll connection probe is throttled, so a coarse,
     * re-scanned check site (e.g. a per-page-frame scan re-run once per master row by a
     * nested-loop join) cannot turn into one syscall per re-scan.
     */
    default long getCircuitBreakerConnectionCheckThrottle() {
        return 100;
    }

    @NotNull
    MillisecondClock getClock();

    @NotNull
    NetworkFacade getNetworkFacade();

    /**
     * Maximum SQL execution time in millis.
     *
     * @return maximum SQL execution time in millis
     */
    long getQueryTimeout();

    boolean isEnabled();
}
