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

package io.questdb.cutlass.pgwire;

import io.questdb.cairo.sql.NetworkSqlExecutionCircuitBreaker;

/**
 * This circuit breaker registry is used in hex tests - where we want deterministic client id and secrets.
 * <p>
 * It does not support cancellation of queries since hex tests do not need it. At least for now.
 */
public final class PGHexTestsCircuitBreakRegistry implements PGCircuitBreakerRegistry {
    public static final PGHexTestsCircuitBreakRegistry INSTANCE = new PGHexTestsCircuitBreakRegistry();
    private static final int CLIENT_ID = 63;
    private static final int SECRET = -1148479920;

    private PGHexTestsCircuitBreakRegistry() {
        // private
    }

    @Override
    public int add(NetworkSqlExecutionCircuitBreaker cb) {
        return CLIENT_ID;
    }

    @Override
    public void cancel(int circuitBreakerIdx, int secret) {
        throw new UnsupportedOperationException(this.getClass().getSimpleName() + " does not support query cancellation. " +
                "It's meant to be used in hex tests where we want deterministic client id and secrets. ");
    }

    @Override
    public void close() {
        // intentionally empty
    }

    @Override
    public int getNewSecret() {
        return SECRET;
    }

    @Override
    public void remove(int contextId) {
        // intentionally empty
    }
}
