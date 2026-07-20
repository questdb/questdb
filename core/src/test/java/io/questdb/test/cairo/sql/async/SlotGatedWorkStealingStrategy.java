/*+*****************************************************************************
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

package io.questdb.test.cairo.sql.async;

import io.questdb.DefaultFactoryProvider;
import io.questdb.FactoryProvider;
import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.sql.StatefulAtom;
import io.questdb.cairo.sql.async.AdaptiveWorkStealingStrategy;
import io.questdb.cairo.sql.async.WorkStealingStrategy;
import io.questdb.griffin.engine.PerWorkerLockOwner;
import io.questdb.griffin.engine.PerWorkerLocks;
import org.jetbrains.annotations.NotNull;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

/**
 * Holds the query owner off until a worker has taken a per-worker slot.
 * <p>
 * A slot-leak test asserts that no slot is still held once a query has failed. That assertion is
 * vacuous unless a worker actually took one: the owner reduces with its own state and takes no
 * slot, so an owner that steals every frame leaves the acquire/release path under test unexercised,
 * and the count reads zero for the wrong reason.
 * <p>
 * {@link AdaptiveWorkStealingStrategy} on its own does not give that. It spins for
 * {@code CairoConfiguration.getSqlParallelWorkStealingSpinTimeout()}, 50us by default, which a
 * worker thread routinely fails to wake up inside; over repeated runs of
 * {@code ParallelWindowJoinMemoryTrackerTest} the owner won often enough to fail roughly one run in
 * three. Waiting on the latch is deterministic instead, and is the coordination hook the repo
 * sanctions in place of a timing guess.
 * <p>
 * Assign {@link #newFactoryProvider()} to {@code AbstractCairoTest.factoryProvider}, then gate a
 * query by installing a latch with {@link PerWorkerLocks#setTestAcquireLatch(CountDownLatch)}. Any
 * query whose atom carries no latch runs as the plain adaptive strategy, so a whole test class can
 * leave this in place.
 */
public class SlotGatedWorkStealingStrategy extends AdaptiveWorkStealingStrategy {
    /**
     * Returns a factory provider that gates every parallel query it hands a strategy to. Assign it
     * to {@code AbstractCairoTest.factoryProvider} from a {@code @Before} running before
     * {@code super.setUp()}.
     * <p>
     * Going through {@code Overrides.setFactoryProvider()} would not work: {@code StaticOverrides}
     * overrides the getter to read {@code AbstractCairoTest.factoryProvider}, so the overrides
     * object's own field is never read and the provider would be dropped without a word.
     */
    public static FactoryProvider newFactoryProvider() {
        return new SlotGatedFactoryProvider();
    }

    private final StatefulAtom atom;

    public SlotGatedWorkStealingStrategy(int noStealingThreshold, long spinTimeoutNanos, StatefulAtom atom) {
        super(noStealingThreshold, spinTimeoutNanos);
        this.atom = atom;
    }

    @Override
    public boolean shouldSteal(int finishedCount) {
        final CountDownLatch acquired = testAcquireLatch();
        if (acquired != null) {
            try {
                if (!acquired.await(30, TimeUnit.SECONDS)) {
                    throw new AssertionError("timed out waiting for a worker slot acquisition");
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new AssertionError("interrupted waiting for a worker slot acquisition", e);
            }
        }
        return super.shouldSteal(finishedCount);
    }

    private CountDownLatch testAcquireLatch() {
        if (atom instanceof PerWorkerLockOwner owner) {
            final PerWorkerLocks locks = owner.getPerWorkerLocks();
            if (locks != null) {
                return locks.getTestAcquireLatch();
            }
        }
        return null;
    }

    private static class SlotGatedFactoryProvider extends DefaultFactoryProvider {
        @Override
        public @NotNull WorkStealingStrategy getWorkStealingStrategy(
                @NotNull CairoConfiguration configuration,
                int workerCount,
                @NotNull StatefulAtom atom
        ) {
            return new SlotGatedWorkStealingStrategy(
                    configuration.getSqlParallelWorkStealingThreshold(),
                    configuration.getSqlParallelWorkStealingSpinTimeout(),
                    atom
            );
        }
    }
}
