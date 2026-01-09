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

package io.questdb.cairo;


import io.questdb.std.ConcurrentHashMap;
import org.jetbrains.annotations.NonNls;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiFunction;

public class TxnScoreboardPoolV2 implements TxnScoreboardPool {
    private final BiFunction<CharSequence, ScoreboardPoolTenant, ScoreboardPoolTenant> getOrCreateScoreboard;
    private final ConcurrentHashMap<ScoreboardPoolTenant> pool = new ConcurrentHashMap<>();

    public TxnScoreboardPoolV2(CairoConfiguration configuration) {
        getOrCreateScoreboard = (key, value) -> {
            if (value == null || !value.incrementRefCount()) {
                //noinspection resource
                value = new ScoreboardPoolTenant(configuration.getReaderPoolMaxSegments() * configuration.getPoolSegmentSize());
            }
            return value;
        };
    }

    @Override
    public void clear() {
        for (CharSequence tableDir : pool.keySet()) {
            final var scoreboard = pool.get(tableDir);
            if (scoreboard != null) {
                // Don't use iterator.remove(), it can remove new entry
                pool.remove(tableDir, scoreboard);
                if (!scoreboard.tryFullClose()) {
                    scoreboard.closePending = true;
                    scoreboard.tryFullClose();
                }
            }
        }
    }

    @Override
    public TxnScoreboard getTxnScoreboard(@NonNls TableToken token) {
        ScoreboardPoolTenant val = pool.compute(token.getDirName(), getOrCreateScoreboard);
        val.setTableToken(token);
        return val;
    }

    @Override
    public boolean releaseInactive() {
        // Remove all with ref count == 0
        boolean removed = false;
        for (CharSequence tableDir : pool.keySet()) {
            final var scoreboard = pool.get(tableDir);
            if (scoreboard != null && scoreboard.tryFullClose()) {
                // Don't use iterator.remove(), it can remove new entry
                pool.remove(tableDir, scoreboard);
                removed = true;
            }
        }
        return removed;
    }

    @Override
    public void remove(TableToken token) {
        final var scoreboard = pool.remove(token.getDirName());
        if (scoreboard != null) {
            scoreboard.closePending = true;
            scoreboard.tryFullClose();
        }
    }

    private static class ScoreboardPoolTenant extends TxnScoreboardV2 {
        private final AtomicInteger refCount = new AtomicInteger(1);
        private volatile boolean closePending = false;

        public ScoreboardPoolTenant(int entryCount) {
            super(entryCount);
        }

        @Override
        public void close() {
            if (refCount.decrementAndGet() == 0 && closePending) {
                tryFullClose();
            }
        }

        public boolean incrementRefCount() {
            do {
                int count = refCount.get();
                if (count < 0 || closePending) {
                    return false;
                }
                if (refCount.compareAndSet(count, count + 1)) {
                    return true;
                }
            } while (true);
        }

        public boolean tryFullClose() {
            if (refCount.compareAndSet(0, -1)) {
                super.close();
                return true;
            }
            return false;
        }
    }
}
