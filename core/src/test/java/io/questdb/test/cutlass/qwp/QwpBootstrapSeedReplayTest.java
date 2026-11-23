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

package io.questdb.test.cutlass.qwp;

import io.questdb.test.tools.TestUtils;
import org.junit.After;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.JUnitCore;
import org.junit.runner.Request;
import org.junit.runner.Result;
import org.junit.runner.notification.Failure;

/**
 * A test that extends {@link AbstractQwpBootstrapTest} or {@link AbstractReusedServerQwpEgressTest}
 * logs the seeds of its Rnds, which {@code -Dfuzz.s0} and {@code -Dfuzz.s1} pass back to replay a
 * failing run. The override seeds every Rnd alike, so it replays only a test that draws from one
 * Rnd: both base classes draw the chunks and the data of a test from the Rnd {@link OneSeedPairCheck}
 * seeds for it, and fail a test or a test class that seeds another Rnd.
 * <p>
 * Not an AbstractTest itself: it runs such tests through {@link JUnitCore}, and the two would share
 * the static test root.
 */
public class QwpBootstrapSeedReplayTest {
    // lets the probes run their tests; run on their own, they skip them
    static volatile boolean isEnabled;
    private static final String REPLAY_NOTE = "; -Dfuzz.s0 and -Dfuzz.s1 seed every Rnd alike, so they can replay"
            + " a test only if it draws from random alone";
    // what the last Draws or ReusedServerDraws probe drew
    private static volatile String draws;

    @Test
    public void testFixedSeedRandomPasses() {
        // a Rnd of seeds the test fixes replays as it is
        assertPasses(run(FixedSeedRandom.class));
    }

    @Test
    public void testOwnRandomInBeforeClassFails() {
        assertFailsForOwnRandomOutsideTests(OwnRandomInBeforeClass.class);
    }

    @Test
    public void testOwnRandomInFailingTestFailsTwice() {
        // the seeds a failing test logged cannot replay it, so it says so next to its failure
        final Result result = run(OwnRandomInFailingTest.class);
        Assert.assertEquals(1, result.getRunCount());
        Assert.assertEquals(2, result.getFailureCount());
        Assert.assertEquals("the test fails", result.getFailures().get(0).getMessage());
        Assert.assertEquals(
                "seeded 1 Rnd(s) of its own through TestUtils.generateRandom(Log)" + REPLAY_NOTE,
                result.getFailures().get(1).getMessage()
        );
    }

    @Test
    public void testOwnRandomInSetUpFails() {
        assertFailsForOwnRandom(OwnRandomInSetUp.class);
    }

    @Test
    public void testOwnRandomInSkippedTestFails() {
        // a skipped test that seeded a Rnd of its own fails all the same
        Assert.assertEquals(0, assertFailsForOwnRandom(OwnRandomInSkippedTest.class).getAssumptionFailureCount());
    }

    @Test
    public void testOwnRandomInTearDownFails() {
        assertFailsForOwnRandom(OwnRandomInTearDown.class);
    }

    @Test
    public void testOwnRandomInTestFails() {
        assertFailsForOwnRandom(OwnRandomInTest.class);
    }

    @Test
    public void testReusedServerOwnRandomInBeforeClassFails() {
        assertFailsForOwnRandomOutsideTests(ReusedServerOwnRandomInBeforeClass.class);
    }

    @Test
    public void testReusedServerOwnRandomInSetUpFails() {
        assertFailsForOwnRandom(ReusedServerOwnRandomInSetUp.class);
    }

    @Test
    public void testReusedServerSeedsReplayEveryDraw() {
        assertSeedsReplayEveryDraw(ReusedServerDraws.class);
    }

    @Test
    public void testSecondRandomInBaseSetUpFails() {
        // the Rnd counts for the test, though a base class of another nest seeds it
        assertFailsForOwnRandom(SecondRandomInBaseSetUp.class);
    }

    @Test
    public void testSeedsReplayEveryDraw() {
        assertSeedsReplayEveryDraw(Draws.class);
    }

    private static Result assertFailsForOwnRandom(Class<?> probe) {
        final Result result = run(probe);
        Assert.assertEquals(1, result.getRunCount());
        Assert.assertEquals(1, result.getFailureCount());
        final Failure failure = result.getFailures().getFirst();
        Assert.assertEquals(probe, failure.getDescription().getTestClass());
        Assert.assertEquals("test", failure.getDescription().getMethodName());
        Assert.assertEquals(
                "seeded 1 Rnd(s) of its own through TestUtils.generateRandom(Log)" + REPLAY_NOTE,
                failure.getMessage()
        );
        return result;
    }

    private static void assertFailsForOwnRandomOutsideTests(Class<?> probe) {
        // the test passes, and the class fails
        final Result result = run(probe);
        Assert.assertEquals(1, result.getRunCount());
        Assert.assertEquals(1, result.getFailureCount());
        final Failure failure = result.getFailures().getFirst();
        Assert.assertEquals(probe, failure.getDescription().getTestClass());
        Assert.assertNull(failure.getDescription().getMethodName());
        Assert.assertEquals(
                "seeded 1 Rnd(s) of its own through TestUtils.generateRandom(Log) outside its tests" + REPLAY_NOTE,
                failure.getMessage()
        );
    }

    private static void assertPasses(Result result) {
        Assert.assertEquals(1, result.getRunCount());
        Assert.assertEquals(0, result.getAssumptionFailureCount());
        Assert.assertEquals(result.getFailures().toString(), 0, result.getFailureCount());
    }

    private static void assertSeedsReplayEveryDraw(Class<?> probe) {
        // the pair of seeds gives back every draw of a test, and another pair gives other draws
        final String s0 = System.getProperty("fuzz.s0");
        final String s1 = System.getProperty("fuzz.s1");
        try {
            System.setProperty("fuzz.s0", "123_456_789");
            System.setProperty("fuzz.s1", "987_654_321");
            final String firstDraws = runDraws(probe);
            Assert.assertEquals(firstDraws, runDraws(probe));

            System.setProperty("fuzz.s0", "1");
            System.setProperty("fuzz.s1", "2");
            Assert.assertNotEquals(firstDraws, runDraws(probe));
        } finally {
            restoreProperty("fuzz.s0", s0);
            restoreProperty("fuzz.s1", s1);
        }
    }

    private static void restoreProperty(String name, String value) {
        if (value != null) {
            System.setProperty(name, value);
        } else {
            System.clearProperty(name);
        }
    }

    private static Result run(Class<?> probe) {
        isEnabled = true;
        try {
            return new JUnitCore().run(Request.aClass(probe));
        } finally {
            isEnabled = false;
        }
    }

    private static String runDraws(Class<?> probe) {
        draws = null;
        assertPasses(run(probe));
        return draws;
    }

    // The probes below run only from QwpBootstrapSeedReplayTest: run on their own, they skip their test.

    public static class Draws extends AbstractQwpBootstrapTest {
        @Test
        public void test() {
            Assume.assumeTrue(isEnabled);
            draws = "recvChunk=" + recvChunk + ", sendChunk=" + sendChunk
                    + ", isReadAheadWillNeed=" + isReadAheadWillNeed + ", isReadAheadCold=" + isReadAheadCold
                    + ", data=" + random.nextLong() + ", " + random.nextLong();
        }
    }

    public static class FixedSeedRandom extends AbstractQwpBootstrapTest {
        @Test
        public void test() {
            Assume.assumeTrue(isEnabled);
            TestUtils.generateRandom(null, 42, 4_242);
        }
    }

    public static class OwnRandomInBeforeClass extends AbstractQwpBootstrapTest {
        @BeforeClass
        public static void setUpOwnRandom() {
            if (isEnabled) {
                TestUtils.generateRandom(null);
            }
        }

        @Test
        public void test() {
            Assume.assumeTrue(isEnabled);
        }
    }

    public static class OwnRandomInFailingTest extends AbstractQwpBootstrapTest {
        @Test
        public void test() {
            Assume.assumeTrue(isEnabled);
            TestUtils.generateRandom(null);
            Assert.fail("the test fails");
        }
    }

    public static class OwnRandomInSetUp extends AbstractQwpBootstrapTest {
        @Before
        public void setUpOwnRandom() {
            Assume.assumeTrue(isEnabled);
            TestUtils.generateRandom(null);
        }

        @Test
        public void test() {
        }
    }

    public static class OwnRandomInSkippedTest extends AbstractQwpBootstrapTest {
        @Test
        public void test() {
            Assume.assumeTrue(isEnabled);
            TestUtils.generateRandom(null);
            Assume.assumeTrue("the test skips", false);
        }
    }

    public static class OwnRandomInTearDown extends AbstractQwpBootstrapTest {
        @After
        public void tearDownOwnRandom() {
            if (isEnabled) {
                TestUtils.generateRandom(null);
            }
        }

        @Test
        public void test() {
            Assume.assumeTrue(isEnabled);
        }
    }

    public static class OwnRandomInTest extends AbstractQwpBootstrapTest {
        @Test
        public void test() {
            Assume.assumeTrue(isEnabled);
            TestUtils.generateRandom(null);
        }
    }

    public static class ReusedServerDraws extends ReusedServerProbe {
        @Override
        @Test
        public void test() {
            Assume.assumeTrue(isEnabled);
            draws = "recvChunk=" + recvChunk + ", sendChunk=" + sendChunk
                    + ", data=" + random.nextLong() + ", " + random.nextLong();
        }
    }

    public static class ReusedServerOwnRandomInBeforeClass extends ReusedServerProbe {
        @BeforeClass
        public static void setUpOwnRandom() {
            if (isEnabled) {
                TestUtils.generateRandom(null);
            }
        }
    }

    public static class ReusedServerOwnRandomInSetUp extends ReusedServerProbe {
        @Before
        public void setUpOwnRandom() {
            Assume.assumeTrue(isEnabled);
            TestUtils.generateRandom(null);
        }
    }

    // The probes on AbstractReusedServerQwpEgressTest share its test. A probe and this class
    // share a nest, which the check counts once.
    public abstract static class ReusedServerProbe extends AbstractReusedServerQwpEgressTest {
        @Test
        public void test() {
            Assume.assumeTrue(isEnabled);
        }
    }

    public static class SecondRandomInBaseSetUp extends AbstractSecondRandomTest {
        @Test
        public void test() {
        }
    }
}
