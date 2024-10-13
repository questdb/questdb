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

package io.questdb.network;

import io.questdb.cairo.sql.SqlExecutionCircuitBreaker;
import io.questdb.cairo.sql.SqlExecutionCircuitBreakerConfiguration;
import io.questdb.std.ThreadLocal;

public class QueryPausedException extends Exception {
    private static final ThreadLocal<QueryPausedException> tlException = new ThreadLocal<>(QueryPausedException::new);

    private SuspendEvent event;

    public static QueryPausedException instance(SuspendEvent event, SqlExecutionCircuitBreaker circuitBreaker) {
        QueryPausedException ex = tlException.get();
        SqlExecutionCircuitBreakerConfiguration circuitBreakerConfiguration = circuitBreaker.getConfiguration();
        if (circuitBreakerConfiguration != null) {
            long timeout = circuitBreakerConfiguration.getQueryTimeout();
            if (timeout != Long.MAX_VALUE) {
                long deadline = circuitBreakerConfiguration.getClock().getTicks() + timeout;
                event.setDeadline(deadline);
            }
        }
        ex.event = event;
        return ex;
    }

    public SuspendEvent getEvent() {
        return event;
    }
}
