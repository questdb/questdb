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

import io.questdb.cairo.O3MemoryPressureRegulatorImpl;
import io.questdb.cairo.wal.seq.SeqTxnTracker;
import io.questdb.std.Rnd;
import io.questdb.std.datetime.microtime.MicrosecondClock;
import io.questdb.std.datetime.microtime.MicrosecondClockImpl;
import io.questdb.test.AbstractTest;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

public class O3MemoryPressureRegulatorImplTest extends AbstractTest {

    private static final int[] EXPECTED_BACKOFF_MICROS = {
            0, // level 0
            0, // level 1
            0, // level 2
            0, // level 3
            0, // level 4
            0, // level 5
            512_000, // level 6
            1024_000, // level 7
            2048_000, // level 8
            4096_000, // level 9
            8192_000 // level 10
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
        SeqTxnTracker txnTracker = new SeqTxnTracker();
        MicrosecondClock clock = MicrosecondClockImpl.INSTANCE;
        O3MemoryPressureRegulatorImpl regulator = new O3MemoryPressureRegulatorImpl(rnd, clock, txnTracker);

        long now = clock.getTicks();
        for (int i = 0; i < 10; i++) {
            now += 1_000;
            regulator.onPressureIncreased(now);
        }

        int expectedLevel = 10;

        // in the level 6..10 range, regulator decrease pressure level on every onPressureDecrease() call
        for (int i = 0; i < 5; i++) {
            now += 1_000;
            regulator.onPressureDecreased(now);
            expectedLevel--;

            assertRegulatorState(regulator, expectedLevel, now);
        }

        // in the level 1..5 range, regulator decrease pressure level only with some probability
        for (int i = 0; i < 5; i++) {
            int decreaseCycles = 0;
            do {
                decreaseCycles++;
                now += 1_000;
                regulator.onPressureDecreased(now);
            } while (regulator.getLevel() == expectedLevel);
            System.out.println("Decreasing pressure level from " + expectedLevel + " to " + regulator.getLevel() + " took " + decreaseCycles + " cycles");
            expectedLevel--;
            assertRegulatorState(regulator, expectedLevel, now);
        }

        // level 0 is a fast path, it does not change pressure level
        for (int i = 0; i < 5; i++) {
            now += 1_000;
            regulator.onPressureDecreased(now);
            assertRegulatorState(regulator, 0, now);
        }
    }

    @Test
    public void testDefaultState() {
        Rnd rnd = TestUtils.generateRandom(LOG);
        SeqTxnTracker txnTracker = new SeqTxnTracker();
        O3MemoryPressureRegulatorImpl regulator = new O3MemoryPressureRegulatorImpl(rnd, MicrosecondClockImpl.INSTANCE, txnTracker);

        Assert.assertEquals(Integer.MAX_VALUE, regulator.getMaxO3MergeParallelism());
        Assert.assertFalse(regulator.shouldBackoff(MicrosecondClockImpl.INSTANCE.getTicks()));
    }

    @Test
    public void testIncreasingPressure() {
        Rnd rnd = TestUtils.generateRandom(LOG);
        SeqTxnTracker txnTracker = new SeqTxnTracker();
        MicrosecondClock clock = MicrosecondClockImpl.INSTANCE;
        O3MemoryPressureRegulatorImpl regulator = new O3MemoryPressureRegulatorImpl(rnd, clock, txnTracker);

        long now = clock.getTicks();
        for (int i = 1; i <= 20; i++) {
            now += 1_000;
            boolean canRetry = regulator.onPressureIncreased(now);
            Assert.assertEquals(i <= 10, canRetry);

            int expectedLevel = Math.min(i, 10);
            assertRegulatorState(regulator, expectedLevel, now);
        }
    }

    private static void assertRegulatorState(O3MemoryPressureRegulatorImpl regulator, int expectedLevel, long now) {
        Assert.assertEquals(expectedLevel, regulator.getLevel());

        int expectedParallelism = EXPECTED_PARALLELISMS[expectedLevel];
        Assert.assertEquals(expectedParallelism, regulator.getMaxO3MergeParallelism());

        int expectedBackoff = EXPECTED_BACKOFF_MICROS[expectedLevel];
        if (expectedBackoff == 0) {
            Assert.assertFalse(regulator.shouldBackoff(now));
        } else {
            Assert.assertTrue(regulator.shouldBackoff(now));
            Assert.assertTrue(regulator.shouldBackoff(now + expectedBackoff - 1));
            Assert.assertFalse(regulator.shouldBackoff(now + expectedBackoff));
        }
    }
}
