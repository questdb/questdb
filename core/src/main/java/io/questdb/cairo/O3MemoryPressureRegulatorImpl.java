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

import io.questdb.cairo.wal.seq.SeqTxnTracker;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.std.Rnd;
import io.questdb.std.datetime.microtime.MicrosecondClock;
import org.jetbrains.annotations.TestOnly;

/**
 * This class is responsible for regulating the memory pressure in the O3 WAL apply system.
 * <p>
 * It is used to control the parallelism of the O3 merge process and to introduce backoff
 * when the system is under memory pressure.
 * <p>
 * This class is NOT thread-safe.
 */
public final class O3MemoryPressureRegulatorImpl implements O3MemoryPressureRegulator {
    private static final Log LOG = LogFactory.getLog(O3MemoryPressureRegulatorImpl.class);

    private static final int MAX_LEVEL = 10;
    private static final int MIN_LEVEL = 0;
    private static final int PARALLELISM_THROTTLING_LEVEL = 5; // when exceeded we start introducing backoff
    private final Rnd rnd;
    private final SeqTxnTracker txnTracker;
    private int level;
    private long walBackoff = -1;

    public O3MemoryPressureRegulatorImpl(Rnd rnd, MicrosecondClock clock, SeqTxnTracker txnTracker) {
        this.rnd = rnd;
        this.txnTracker = txnTracker;
        this.level = txnTracker.getMemoryPressureLevel();
        adjustWalBackoff(clock.getTicks());
    }

    @TestOnly
    public int getLevel() {
        return level;
    }

    @Override
    public int getMaxO3MergeParallelism() {
        if (level == 0) {
            // fast path
            return Integer.MAX_VALUE;
        }

        int adjustedLevel = Math.min(level, PARALLELISM_THROTTLING_LEVEL) * 10;
        int partitionParallelism = 51 - adjustedLevel;
        assert partitionParallelism > 0;
        // level 1 = parallelism 41
        // level 2 = parallelism 31
        // level 3 = parallelism 21
        // level 4 = parallelism 11
        // level 5 = parallelism 1
        // level 6 = parallelism 1 + backoff
        // level 7 = parallelism 1 + backoff
        // level 8 = parallelism 1 + backoff
        // level 9 = parallelism 1 + backoff
        // level 10 = parallelism 1 + backoff
        return partitionParallelism;
    }

    @Override
    public void onPressureDecreased(long nowMicros) {
        if (level == MIN_LEVEL) {
            // fast path
            return;
        }

        if (level > PARALLELISM_THROTTLING_LEVEL) {
            // we always reduce max. backoff
            level--;
            LOG.infoW().$("Memory pressure building up, new level=").$(level).$();
            txnTracker.setMemoryPressureLevel(level);
            adjustWalBackoff(nowMicros);
        } else {
            // we increase parallelism probabilistically - the more pressure the lesser chance of increasing it
            int minTrailingZeros = (level + 2);
            // if memoryPressure == 1  we require 3 trailing zeros -> that's 1 in 8 chance
            // if memoryPressure == 2  we require 4 trailing zeros -> that's 1 in 16 chance
            // ...
            // if memoryPressure == 5 we require 7 trailing zeros -> that's 1 in 128 chance
            // yes, we are assuming that random number generator is reasonably good
            int i = rnd.nextInt();
            if (Integer.numberOfTrailingZeros(i) >= minTrailingZeros) {
                level--;
                txnTracker.setMemoryPressureLevel(level);
                adjustWalBackoff(nowMicros);
            }
        }
    }

    @Override
    public boolean onPressureIncreased(long nowMicros) {
        // we might consider more aggressive increase in the future to quickly reduce parallelism
        // when memory pressure is high. For now, we are conservative. It might take multiple cycles
        // to find the right level.

        if (level == MAX_LEVEL) {
            // we are already at max level, we can't increase it further
            // return false to indicate that we can't retry the operation
            // this will likely suspend the table
            adjustWalBackoff(nowMicros);
            return false;
        }
        level++;
        LOG.infoW().$("Memory pressure easing off, new level=").$(level).$();
        txnTracker.setMemoryPressureLevel(level);
        adjustWalBackoff(nowMicros);
        return true;
    }

    @Override
    public boolean shouldBackoff(long nowMicros) {
        return nowMicros < walBackoff;
    }

    private void adjustWalBackoff(long nowMicros) {
        if (level > 5) {
            int backoffMillis = (1 << (level + 3));
            // level 6 -> 512 ms
            // level 7 -> 1024 ms
            // level 8 -> 2048 ms
            // level 9 -> 4096 ms
            // level 10 -> 8192 ms
            long backoffMicros = backoffMillis * 1_000L;

            walBackoff = nowMicros + backoffMicros;
        } else {
            // we reduce parallelism, that's already aggressive enough
            // so no backoff
            walBackoff = -1;
        }
    }
}
