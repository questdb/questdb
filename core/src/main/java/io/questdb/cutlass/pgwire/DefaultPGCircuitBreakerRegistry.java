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

import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.CairoException;
import io.questdb.cairo.sql.NetworkSqlExecutionCircuitBreaker;
import io.questdb.mp.SimpleSpinLock;
import io.questdb.std.IntList;
import io.questdb.std.ObjList;
import io.questdb.std.Rnd;

// registry of circuit breakers utilized by pg wire contexts 
// makes it possible to cancel queries executed in other connections/threads
public final class DefaultPGCircuitBreakerRegistry implements PGCircuitBreakerRegistry {

    private final ObjList<NetworkSqlExecutionCircuitBreaker> circuitBreakers;
    private final IntList freeIdx;

    private final SimpleSpinLock lock;

    private final Rnd random;

    private volatile boolean closed = false;

    public DefaultPGCircuitBreakerRegistry(PGConfiguration configuration, CairoConfiguration cairoConfig) {
        lock = new SimpleSpinLock();
        int limit = configuration.getLimit();
        circuitBreakers = new ObjList<>(limit);
        freeIdx = new IntList(limit);
        if (configuration.getRandom() != null) {
            // use separate rnd to avoid changing existing pg test results 
            random = new Rnd(configuration.getRandom().getSeed0(), configuration.getRandom().getSeed1());
        } else {
            random = new Rnd(
                    cairoConfig.getNanosecondClock().getTicks(),
                    cairoConfig.getMicrosecondClock().getTicks());
        }

        for (int i = 0; i < limit; i++) {
            circuitBreakers.add(null);
            freeIdx.add(i);
        }
    }

    @Override
    public int add(NetworkSqlExecutionCircuitBreaker cb) {
        if (closed) {
            return -1;
        }

        int idx;
        lock.lock();
        try {
            int size = freeIdx.size();
            if (size > 0) {
                idx = freeIdx.getQuick(size - 1);
                freeIdx.setPos(size - 1);
                circuitBreakers.setQuick(idx, cb);
            } else {
                idx = circuitBreakers.size();
                circuitBreakers.add(cb);
            }
        } finally {
            lock.unlock();
        }

        return idx;
    }

    @Override
    public void cancel(int circuitBreakerIdx, int secret) {
        if (circuitBreakers.size() < circuitBreakerIdx || circuitBreakerIdx < 0) {
            throw CairoException.nonCritical().put("wrong circuit breaker idx [idx=").put(circuitBreakerIdx).put("]");
        }

        NetworkSqlExecutionCircuitBreaker cb = circuitBreakers.getQuick(circuitBreakerIdx);
        if (cb == null) {
            throw CairoException.nonCritical().put("empty circuit breaker slot [idx=").put(circuitBreakerIdx).put("]");
        }

        if (cb.getSecret() != secret) {
            throw CairoException.nonCritical().put("wrong circuit breaker secret [idx=").put(circuitBreakerIdx).put("]");
        }

        cb.cancel();
    }

    @Override
    public void close() {
        if (closed) {
            return;
        }

        lock.lock();
        closed = true;
        try {
            for (int i = 0, n = circuitBreakers.size(); i < n; i++) {
                NetworkSqlExecutionCircuitBreaker cb = circuitBreakers.getQuick(i);
                if (cb != null) {
                    cb.cancel();
                    circuitBreakers.setQuick(i, null);
                }
            }
            freeIdx.clear();
        } finally {
            lock.unlock();
        }
    }

    @Override
    public int getNewSecret() {
        return random.nextInt();
    }

    @Override
    public void remove(int contextId) {
        lock.lock();
        try {
            if (!closed) {
                circuitBreakers.setQuick(contextId, null);
                freeIdx.add(contextId);
            }
        } finally {
            lock.unlock();
        }
    }
}
