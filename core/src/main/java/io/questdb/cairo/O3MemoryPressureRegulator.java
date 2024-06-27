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

import io.questdb.std.Rnd;
import io.questdb.std.datetime.microtime.MicrosecondClock;

/**
 * This class is responsible for regulating the memory pressure in the O3 WAL apply system.
 * <p>
 * It is used to control the parallelism of the O3 merge process and to introduce backoff
 * when the system is under memory pressure.
 * <p>
 * This class is NOT thread-safe.
 */
public final class O3MemoryPressureRegulator {
    private static final int MAX_LEVEL = 10;
    private static final int MIN_LEVEL = 0;
    private static final int PARALLELISM_THROTTLING_LEVEL = 5; // 5 is the level where we start reducing parallelism
    private final MicrosecondClock clock;
    private final Rnd rnd;
    private int level;
    private long walBackoff = -1;

    O3MemoryPressureRegulator(Rnd rnd, MicrosecondClock clock) {
        this.rnd = rnd;
        this.clock = clock;
    }

    public int getMaxO3MergeParallelism() {
        int partitionParallelism = Integer.MAX_VALUE;
        if (level >= PARALLELISM_THROTTLING_LEVEL) {
            partitionParallelism = 101 - (level * 10);
        }
        assert partitionParallelism > 0;
        return partitionParallelism;
    }

    public void onPressureDecreased() {
        if (level < PARALLELISM_THROTTLING_LEVEL) {
            // we always reduce max. backoff
            level = Math.max(MIN_LEVEL, level - 1);
        } else {
            // we increase parallelism probabilistically - the more pressure the lesser chance of increasing it
            int minTrailingZeros = (level - 2);
            // if memoryPressure == 5  we require 3 trailing zeros -> that's 1 in 8 chance
            // if memoryPressure == 6  we require 4 trailing zeros -> that's 1 in 16 chance
            // ...
            // if memoryPressure == 10 we require 8 trailing zeros -> that's 1 in 256 chance
            // yes, we are assuming that random number generator is reasonably good
            int i = rnd.nextInt();
            if (Integer.numberOfTrailingZeros(i) >= minTrailingZeros) {
                level--;
            }
        }
        adjustWalBackoff();
    }

    public void onPressureIncreased() {
        // we might consider more aggressive increase in the future to quickly reduce parallelism
        // when memory pressure is high. For now, we are conservative. It might take multiple cycles
        // to find the right level.
        level = Math.min(level + 1, MAX_LEVEL);
        adjustWalBackoff();
    }

    public boolean shouldBackoff() {
        return clock.getTicks() < walBackoff;
    }

    private void adjustWalBackoff() {
        if (level < 5) {
            long postponeMillis = (1L << level) - 1; // 0, 1, 3, 7, 15
            walBackoff = clock.getTicks() + (postponeMillis * 1000);
        } else {
            // we reduce parallelism, that's already aggressive enough
            // so no backoff
            walBackoff = -1;
        }
    }
}
