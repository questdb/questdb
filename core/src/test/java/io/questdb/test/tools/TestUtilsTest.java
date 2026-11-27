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

package io.questdb.test.tools;

import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.griffin.SqlException;
import io.questdb.mp.WorkerPoolMode;
import io.questdb.std.Rnd;
import io.questdb.std.str.Utf8String;
import io.questdb.test.AbstractCairoTest;
import org.junit.Assert;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;
import java.util.SplittableRandom;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

public final class TestUtilsTest extends AbstractCairoTest {

    @Test
    public void testAssertAsciiCompliance() {
        TestUtils.assertAsciiCompliance(null);
        TestUtils.assertAsciiCompliance(new Utf8String(new byte[]{'a'}, true));
        TestUtils.assertAsciiCompliance(new Utf8String(new byte[]{'a'}, false));

        Assert.assertThrows(
                AssertionError.class,
                () -> TestUtils.assertAsciiCompliance(new Utf8String(new byte[]{(byte) 0xc3, (byte) 0xa9}, true))
        );
    }

    @Test
    public void testAssertReverseLinesEqual() {
        Assert.assertThrows(AssertionError.class, () -> {
            TestUtils.assertReverseLinesEqual(null, "123\n456\n789\n", "123\n456\n789\n");
            TestUtils.assertReverseLinesEqual(null, "1234\n56\n789\n", "789\n456\n123\n");
        });
        TestUtils.assertReverseLinesEqual(null, "123\n456\n789\n", "789\n456\n123\n");
        TestUtils.assertReverseLinesEqual(null, "1234\n56\n789\n", "789\n56\n1234\n");
        TestUtils.assertReverseLinesEqual(null, "1234\n", "1234\n");
    }

    @Test
    public void testGenerateRandomMixesClockReadings() {
        // Without its override, a seed of generateRandom() is a clock reading mixed by mixSeed(): s0 reads
        // System.nanoTime(), s1 System.currentTimeMillis(). The override of the other seed holds it at 1. Each step
        // of Rnd maps its state one to one, so the same state after the warm-up means the same seeds.
        final Rnd expected = new Rnd();
        withSeedOverrides(null, "1", () -> {
            final long nanosBefore = System.nanoTime();
            final Rnd actual = TestUtils.generateRandom(null);
            final long nanosAfter = System.nanoTime();
            for (long nanos = nanosBefore; nanos <= nanosAfter; nanos++) {
                warmUp(expected, TestUtils.mixSeed(nanos), 1);
                if (expected.getSeed0() == actual.getSeed0() && expected.getSeed1() == actual.getSeed1()) {
                    return;
                }
            }
            Assert.fail("s0 is not a mixed System.nanoTime() reading");
        });
        withSeedOverrides("1", null, () -> {
            final long millisBefore = System.currentTimeMillis();
            final Rnd actual = TestUtils.generateRandom(null);
            final long millisAfter = System.currentTimeMillis();
            for (long millis = millisBefore; millis <= millisAfter; millis++) {
                warmUp(expected, 1, TestUtils.mixSeed(millis));
                if (expected.getSeed0() == actual.getSeed0() && expected.getSeed1() == actual.getSeed1()) {
                    return;
                }
            }
            Assert.fail("s1 is not a mixed System.currentTimeMillis() reading");
        });
    }

    @Test
    public void testGenerateRandomUsesSeedOverridesAsTheyAre() {
        // -Dfuzz.s0 and -Dfuzz.s1 set to the seeds that generateRandom() printed reproduce the run, so
        // generateRandom() does not mix them; mixed seeds use all 64 bits, the sign bit too
        withSeedOverrides("-9223372036854775808", "9223372036854775807", () -> {
            final Rnd expected = TestUtils.generateRandom(null, Long.MIN_VALUE, Long.MAX_VALUE);
            final Rnd actual = TestUtils.generateRandom(null);
            Assert.assertEquals(expected.getSeed0(), actual.getSeed0());
            Assert.assertEquals(expected.getSeed1(), actual.getSeed1());
        });
    }

    @Test
    public void testJoinThreadsInterruptsTimedOutWorker() throws Exception {
        final CountDownLatch started = new CountDownLatch(1);
        final AtomicBoolean interrupted = new AtomicBoolean();
        final Thread worker = new Thread(() -> {
            started.countDown();
            try {
                new CountDownLatch(1).await();
            } catch (InterruptedException e) {
                interrupted.set(true);
                Thread.currentThread().interrupt();
            }
        }, "timed-out-test-worker");
        worker.setDaemon(true);
        worker.start();
        Assert.assertTrue(started.await(5, TimeUnit.SECONDS));

        final AssertionError error = Assert.assertThrows(
                AssertionError.class,
                () -> TestUtils.joinThreads(50, worker)
        );
        Assert.assertTrue(error.getMessage().contains("did not finish within"));
        Assert.assertTrue(interrupted.get());
        Assert.assertFalse(worker.isAlive());
    }

    @Test
    public void testMixSeedDecorrelatesCloseReadings() {
        // Tests that start close together read close clock values. Rnd carries the bits of its seeds into the high
        // bits of its values slowly: seeded with the raw readings below, about 4 in 5 of the Rnds choose true first,
        // so most of the tests would make the same random choice. Mixed, the readings make independent choices.
        assertFairFirstChoices(1_000, 0); // calls 1 microsecond apart, in one millisecond
        assertFairFirstChoices(37_000_000, 37); // tests 37 milliseconds apart
    }

    @Test
    public void testMixSeedIsSplitMix64() {
        // mixSeed(x) is the first value of SplitMix64 seeded with x, which java.util.SplittableRandom implements
        final long[] values = {
                0,
                1,
                -1,
                Long.MIN_VALUE,
                Long.MIN_VALUE + 1,
                Long.MAX_VALUE,
                Long.MAX_VALUE - 1,
                // a System.nanoTime() reading
                8_057_884_243_203_250L,
                // a System.currentTimeMillis() reading
                1_795_817_470_513L
        };
        for (long value : values) {
            Assert.assertEquals("value: " + value, new SplittableRandom(value).nextLong(), TestUtils.mixSeed(value));
        }
        // the first values of the reference SplitMix64 seeded with 0
        Assert.assertEquals(0xe220a8397b1dcdafL, TestUtils.mixSeed(0));
        Assert.assertEquals(0x6e789e6aa1b965f4L, TestUtils.mixSeed(0x9e3779b97f4a7c15L));
        Assert.assertEquals(0x06c45d188009454fL, TestUtils.mixSeed(2 * 0x9e3779b97f4a7c15L));
    }

    @Test
    public void testOrderTolerantRecordComparison() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table x (x long, ts timestamp) timestamp(ts) partition by day");
            execute("create table y (x long, ts timestamp) timestamp(ts) partition by day");

            execute("insert into x values (1, '2022-02-24T00:00:01.000000Z')");
            execute("insert into x values (2, '2022-02-24T00:00:01.000000Z')");
            execute("insert into x values (3, '2022-02-24T00:00:02.000000Z')");

            execute("insert into y values (2, '2022-02-24T00:00:01.000000Z')");
            execute("insert into y values (1, '2022-02-24T00:00:01.000000Z')");
            execute("insert into y values (3, '2022-02-24T00:00:02.000000Z')");

            HashMap<String, Integer> mapX = new HashMap<>();
            HashMap<String, Integer> mapY = new HashMap<>();

            addAllRecordsToMap("x", mapX);
            addAllRecordsToMap("y", mapY);

            Assert.assertEquals(mapX, mapY);

            execute("insert into y values (2, '2022-02-24T00:00:01.000000Z')");

            // now the maps should be different since we've added an extra record

            mapY.clear();
            addAllRecordsToMap("y", mapY);

            Assert.assertNotEquals(mapX, mapY);
        });
    }

    @Test
    public void testWorkerPoolModeSeededSelectionIsStable() {
        final Rnd expected = new Rnd(123, 456);
        final Rnd actual = new Rnd(123, 456);
        for (int i = 0; i < 100; i++) {
            Assert.assertSame(
                    expected.nextBoolean() ? WorkerPoolMode.FIBER_HOST : WorkerPoolMode.LEGACY,
                    TestUtils.getWorkerPoolMode(actual)
            );
        }
    }

    @Test
    public void testWorkerPoolModeSelectsBothModes() {
        final Rnd rnd = new Rnd(123, 456);
        boolean hasFiberHost = false;
        boolean hasLegacy = false;
        for (int i = 0; i < 100 && !(hasFiberHost && hasLegacy); i++) {
            switch (TestUtils.getWorkerPoolMode(rnd)) {
                case FIBER_HOST -> hasFiberHost = true;
                case LEGACY -> hasLegacy = true;
            }
        }
        Assert.assertTrue(hasFiberHost);
        Assert.assertTrue(hasLegacy);
    }

    @Test
    public void testRunConcurrentlyRethrowsWorkerFailure() {
        final AssertionError error = Assert.assertThrows(
                AssertionError.class,
                () -> TestUtils.runConcurrently(2, worker -> {
                    if (worker == 1) {
                        throw new IllegalStateException("worker failure");
                    }
                })
        );
        Assert.assertTrue(error.getCause() instanceof IllegalStateException);
        Assert.assertEquals("worker failure", error.getCause().getMessage());
    }

    private static void addAllRecordsToMap(String query, Map<String, Integer> map) throws SqlException {
        try (
                RecordCursorFactory factory = select(query);
                RecordCursor cursor = factory.getCursor(sqlExecutionContext)
        ) {
            TestUtils.addAllRecordsToMap(sink, cursor, factory.getMetadata(), map);
        }
    }

    // Asserts that Rnds seeded and warmed up as generateRandom() does it, with 1000 mixed readings of each clock a step
    // apart, choose true first about half the time: a fair coin makes 500 +/- 47 (3 standard deviations) true
    // choices of 1000.
    private static void assertFairFirstChoices(long nanosStep, long millisStep) {
        final long nanos = 8_057_884_243_203_250L;
        final long millis = 1_795_817_470_513L;
        final Rnd rnd = new Rnd();
        int trueCount = 0;
        for (int i = 0; i < 1_000; i++) {
            warmUp(rnd, TestUtils.mixSeed(nanos + i * nanosStep), TestUtils.mixSeed(millis + i * millisStep));
            if (rnd.nextBoolean()) {
                trueCount++;
            }
        }
        Assert.assertTrue("true choices: " + trueCount, trueCount >= 453 && trueCount <= 547);
    }

    private static void setSystemProperty(String name, String value) {
        if (value != null) {
            System.setProperty(name, value);
        } else {
            System.clearProperty(name);
        }
    }

    // seeds rnd and draws the values that generateRandom() draws before it returns its Rnd
    private static void warmUp(Rnd rnd, long s0, long s1) {
        rnd.reset(s0, s1);
        rnd.nextBoolean();
        rnd.nextBoolean();
    }

    // runs code with -Dfuzz.s0 and -Dfuzz.s1 set to s0 and s1, or cleared where null
    private static void withSeedOverrides(String s0, String s1, Runnable code) {
        final String oldS0 = System.getProperty("fuzz.s0");
        final String oldS1 = System.getProperty("fuzz.s1");
        try {
            setSystemProperty("fuzz.s0", s0);
            setSystemProperty("fuzz.s1", s1);
            code.run();
        } finally {
            setSystemProperty("fuzz.s0", oldS0);
            setSystemProperty("fuzz.s1", oldS1);
        }
    }
}
