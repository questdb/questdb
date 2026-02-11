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

package io.questdb.cairo.sql.async;

import io.questdb.std.Os;

import java.util.concurrent.atomic.AtomicInteger;

/**
 * Forces busy spinning in situations when a sufficient number of tasks is picked up
 * by other threads. The goal of this strategy is to decrease query latency
 * on large multicore machines.
 */
public class AdaptiveWorkStealingStrategy implements WorkStealingStrategy {
    private final int noStealingThreshold;
    private final long spinTimeoutNanos;
    private AtomicInteger startedCounter;

    public AdaptiveWorkStealingStrategy(int noStealingThreshold, long spinTimeoutNanos) {
        this.noStealingThreshold = noStealingThreshold;
        this.spinTimeoutNanos = spinTimeoutNanos;
    }

    @Override
    public WorkStealingStrategy of(AtomicInteger startedCounter) {
        this.startedCounter = startedCounter;
        return this;
    }

    @Override
    public boolean shouldSteal(int finishedCount) {
        // Give shared workers a chance to pick up the tasks.
        // The spin duration is time-based to ensure consistent behavior
        // across different CPU architectures (Intel vs AMD have very different
        // PAUSE instruction latencies).
        final int initialStartedCount = startedCounter.get();
        if (initialStartedCount - finishedCount >= noStealingThreshold) {
            // A sufficient number of tasks is taken by other workers,
            // so let's spin while those workers are doing their job.
            return false;
        }

        // Give the OS scheduler a chance to kick in.
        Os.pause();

        final long deadline = System.nanoTime() + spinTimeoutNanos;
        do {
            Thread.onSpinWait();
            // Check if someone picked up a task.
            if (startedCounter.get() > initialStartedCount) {
                return false;
            }
        } while (System.nanoTime() < deadline);
        return true;
    }
}
