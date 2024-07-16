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

package io.questdb.test.cairo;

import io.questdb.cairo.wal.seq.SeqTxnTracker;
import io.questdb.std.Rnd;
import io.questdb.std.datetime.microtime.MicrosecondClock;
import io.questdb.std.datetime.microtime.MicrosecondClockImpl;
import io.questdb.test.AbstractTest;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

public class O3MemoryPressureRegulationTest extends AbstractTest {

    private static final int[] EXPECTED_BACKOFF_MICROS = {
            0, // level 0
            0, // level 1
            0, // level 2
            0, // level 3
            0, // level 4
            0, // level 5
            512_000, // level 6
            1_024_000, // level 7
            2_048_000, // level 8
            4_096_000, // level 9
            8_192_000 // level 10
    };

    private static final int[] EXPECTED_PARALLELISMS = {
            Integer.MAX_VALUE, // level 0
            41, // level 1
            31, // level 2
            21, // level 3
            11, // level 4
            1, // level 5
            1, // level 6
            1, // level 7
            1, // level 8
            1, // level 9
            1 // level 10
    };

    @Test
    public void testDecreasingPressure() {
        Rnd rnd = TestUtils.generateRandom(LOG);
        SeqTxnTracker txnTracker = new SeqTxnTracker(rnd);
        MicrosecondClock clock = MicrosecondClockImpl.INSTANCE;

        long now = clock.getTicks();
        for (int i = 0; i < 10; i++) {
            now += 1_000;
            txnTracker.onPressureIncreased(now);
        }

        int expectedLevel = 10;

        // in the level 6..10 range, txnTracker decrease pressure level on every onPressureDecrease() call
        for (int i = 0; i < 5; i++) {
            now += 1_000;
            txnTracker.onPressureReduced(now);
            expectedLevel--;

            assertRegulationState(txnTracker, expectedLevel, now);
        }

        // in the level 1..5 range, txnTracker decrease pressure level only with some probability
        for (int i = 0; i < 5; i++) {
            int decreaseCycles = 0;
            do {
                decreaseCycles++;
                now += 1_000;
                txnTracker.onPressureReduced(now);
                assertRegulationState(txnTracker, txnTracker.getMemPressureLevel(), now);
            } while (txnTracker.getMemPressureLevel() == expectedLevel);
            System.out.println("Decreasing pressure level from " + expectedLevel + " to " + txnTracker.getMemPressureLevel() + " took " + decreaseCycles + " cycles");
            expectedLevel--;
            assertRegulationState(txnTracker, expectedLevel, now);
        }

        // level 0 is a fast path, it does not change pressure level
        for (int i = 0; i < 5; i++) {
            now += 1_000;
            txnTracker.onPressureReduced(now);
            assertRegulationState(txnTracker, 0, now);
        }
    }

    @Test
    public void testDefaultState() {
        Rnd rnd = TestUtils.generateRandom(LOG);
        SeqTxnTracker txnTracker = new SeqTxnTracker(rnd);

        Assert.assertEquals(Integer.MAX_VALUE, txnTracker.getMaxO3MergeParallelism());
        Assert.assertFalse(txnTracker.shouldBackOff(MicrosecondClockImpl.INSTANCE.getTicks()));
    }

    @Test
    public void testIncreasingPressure() {
        Rnd rnd = TestUtils.generateRandom(LOG);
        SeqTxnTracker txnTracker = new SeqTxnTracker(rnd);
        MicrosecondClock clock = MicrosecondClockImpl.INSTANCE;

        long now = clock.getTicks();
        for (int i = 1; i <= 20; i++) {
            now += 1_000;
            boolean canRetry = txnTracker.onPressureIncreased(now);
            Assert.assertEquals(i <= 10, canRetry);

            int expectedLevel = Math.min(i, 10);
            assertRegulationState(txnTracker, expectedLevel, now);
        }
    }

    private static void assertRegulationState(SeqTxnTracker txnTracker, int expectedLevel, long now) {
        Assert.assertEquals(expectedLevel, txnTracker.getMemPressureLevel());

        int expectedParallelism = EXPECTED_PARALLELISMS[expectedLevel];
        Assert.assertEquals(expectedParallelism, txnTracker.getMaxO3MergeParallelism());

        int expectedBackoff = EXPECTED_BACKOFF_MICROS[expectedLevel];
        if (expectedBackoff == 0) {
            Assert.assertFalse(txnTracker.shouldBackOff(now));
        } else {
            Assert.assertTrue(txnTracker.shouldBackOff(now));
            Assert.assertTrue(txnTracker.shouldBackOff(now + expectedBackoff - 1));
            Assert.assertFalse(txnTracker.shouldBackOff(now + expectedBackoff));
        }
    }
}
