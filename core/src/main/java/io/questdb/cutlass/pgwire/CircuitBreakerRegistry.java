/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2023 QuestDB
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
import io.questdb.std.*;

import java.io.Closeable;

// registry of circuit breakers utilized by pg wire contexts 
// makes it possible to cancel queries executed in other connections/threads
public class CircuitBreakerRegistry implements Closeable {

    private final ObjList<NetworkSqlExecutionCircuitBreaker> circuitBreakers;
    private final IntList freeIdx;

    private final SimpleSpinLock lock;

    private final Rnd random;

    private volatile boolean closed = false;

    public CircuitBreakerRegistry(PGWireConfiguration configuration, CairoConfiguration cairoConfig) {
        lock = new SimpleSpinLock();
        int limit = configuration.getDispatcherConfiguration().getLimit();
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
            circuitBreakers.add(new NetworkSqlExecutionCircuitBreaker(configuration.getCircuitBreakerConfiguration(), MemoryTag.NATIVE_CB5));
            freeIdx.add(i);
        }
    }

    public int acquire() {
        if (closed) {
            return -1;
        }

        int idx;
        int secret = random.nextInt();
        lock.lock();
        try {
            int size = freeIdx.size();
            idx = freeIdx.getQuick(size - 1);
            freeIdx.setPos(size - 1);
        } finally {
            lock.unlock();
        }

        circuitBreakers.getQuick(idx).setSecret(secret);
        return idx;
    }

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
            for (int i = 0, n = freeIdx.size(); i < n; i++) {
                int idx = freeIdx.getQuick(i);
                Misc.free(circuitBreakers.getQuick(idx));
                circuitBreakers.setQuick(idx, null);
            }
            freeIdx.clear();
        } finally {
            lock.unlock();
        }
    }

    public NetworkSqlExecutionCircuitBreaker getCircuitBreaker(int idx) {
        return circuitBreakers.getQuick(idx);
    }

    public void release(int contextId) {
        circuitBreakers.getQuick(contextId).clear();
        lock.lock();
        try {
            if (!closed) {
                freeIdx.add(contextId);
            } else {
                Misc.free(circuitBreakers.getQuick(contextId));
                circuitBreakers.setQuick(contextId, null);
            }
        } finally {
            lock.unlock();
        }
    }
}
