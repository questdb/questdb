/*******************************************************************************
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

package io.questdb.griffin;

import io.questdb.cairo.sql.SqlExecutionCircuitBreakerConfiguration;
import io.questdb.network.NetworkFacade;
import io.questdb.network.NetworkFacadeImpl;
import io.questdb.std.datetime.millitime.MillisecondClock;
import io.questdb.std.datetime.millitime.MillisecondClockImpl;
import org.jetbrains.annotations.NotNull;

public class DefaultSqlExecutionCircuitBreakerConfiguration implements SqlExecutionCircuitBreakerConfiguration {
    @Override
    public boolean checkConnection() {
        return true;
    }

    @Override
    public int getBufferSize() {
        return 64;
    }

    @Override
    public int getCircuitBreakerThrottle() {
        return 5;
    }

    @Override
    public @NotNull MillisecondClock getClock() {
        return MillisecondClockImpl.INSTANCE;
    }

    @Override
    public @NotNull NetworkFacade getNetworkFacade() {
        return NetworkFacadeImpl.INSTANCE;
    }

    @Override
    public long getQueryTimeout() {
        return Long.MAX_VALUE;
    }

    @Override
    public boolean isEnabled() {
        return true;
    }
}
