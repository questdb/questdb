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

package io.questdb.cairo;


import io.questdb.std.ConcurrentHashMap;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiFunction;

import static io.questdb.cairo.pool.AbstractMultiTenantPool.ENTRY_SIZE;

public class TxnScoreboardPoolV2 implements TxnScoreboardPool {
    private final CairoConfiguration configuration;
    private final BiFunction<CharSequence, ScoreboardPoolTenant, ScoreboardPoolTenant> getOrCreateScoreboard;
    private final ConcurrentHashMap<ScoreboardPoolTenant> pool = new ConcurrentHashMap<>();

    public TxnScoreboardPoolV2(CairoConfiguration configuration) {
        this.configuration = configuration;
        getOrCreateScoreboard = (key, value) -> {
            if (value == null || value.closed) {
                //noinspection resource
                value = new ScoreboardPoolTenant(configuration);
            }
            value.refCount.incrementAndGet();
            return value;
        };
    }

    @Override
    public void clear() {
        for (var tt : pool.keySet()) {
            var scoreboard = pool.remove(tt);
            if (scoreboard != null) {
                scoreboard.closed = true;
                if (scoreboard.refCount.get() == 0) {
                    scoreboard.doClose();
                }
            }
        }
    }

    @Override
    public void remove(TableToken token) {
        var scoreboard = pool.remove(token.getDirName());
        if (scoreboard != null) {
            scoreboard.closed = true;
            if (scoreboard.refCount.get() == 0) {
                scoreboard.doClose();
            }
        }
    }

    @Override
    public TxnScoreboard getTxnScoreboard(TableToken token) {
        return pool.compute(token.getDirName(), getOrCreateScoreboard);
    }

    private static class ScoreboardPoolTenant extends TxnScoreboardV2 {
        private final AtomicInteger refCount = new AtomicInteger();
        private volatile boolean closed = false;

        public ScoreboardPoolTenant(CairoConfiguration configuration) {
            super(configuration.getReaderPoolMaxSegments() * ENTRY_SIZE);
        }

        @Override
        public void close() {
            if (refCount.decrementAndGet() == 0 && closed) {
                doClose();
            }
        }

        private void doClose() {
            super.close();
        }
    }
}
