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

package io.questdb.test.tools;

import io.questdb.cairo.CairoEngine;
import io.questdb.cairo.sql.NetworkSqlExecutionCircuitBreaker;
import io.questdb.cairo.sql.SqlExecutionCircuitBreakerConfiguration;
import io.questdb.griffin.DefaultSqlExecutionCircuitBreakerConfiguration;
import io.questdb.std.datetime.millitime.MillisecondClock;
import org.jetbrains.annotations.NotNull;

/**
 * A {@link NetworkSqlExecutionCircuitBreaker} with a fake connection probe: set
 * {@link #isConnectionBroken} to control the probe's verdict and read {@link #probeCount}
 * to observe how often the throttled paths actually probed.
 */
public class TestNetworkSqlExecutionCircuitBreaker extends NetworkSqlExecutionCircuitBreaker {
    public boolean isConnectionBroken;
    public int probeCount;

    public TestNetworkSqlExecutionCircuitBreaker(CairoEngine engine, SqlExecutionCircuitBreakerConfiguration configuration) {
        super(engine, configuration);
    }

    public static TestNetworkSqlExecutionCircuitBreaker create(
            CairoEngine engine,
            MillisecondClock clock,
            long connectionCheckThrottle,
            long queryTimeout
    ) {
        return new TestNetworkSqlExecutionCircuitBreaker(engine, new DefaultSqlExecutionCircuitBreakerConfiguration() {
            @Override
            public long getCircuitBreakerConnectionCheckThrottle() {
                return connectionCheckThrottle;
            }

            @Override
            public @NotNull MillisecondClock getClock() {
                return clock;
            }

            @Override
            public long getQueryTimeout() {
                return queryTimeout;
            }
        });
    }

    @Override
    protected boolean testConnection(long fd) {
        probeCount++;
        return isConnectionBroken;
    }
}
