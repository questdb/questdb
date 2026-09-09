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

import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.std.Os;
import io.questdb.test.AbstractCairoTest;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class LogCaptureTest extends AbstractCairoTest {
    // Not final: guaranteed-logging reflection re-fetches this field, the same constraint that
    // keeps EntCairoEngine.LOG non-final.
    private static Log LOG = LogFactory.getLog(LogCaptureTest.class);
    // Mirrors the budget LogCapture.waitForRegex(String) applies internally; it takes no timeout
    // argument, so the fake clock has to jump by exactly this much to land on its deadline.
    private static final long WAIT_FOR_REGEX_MAX_WAIT_MS = 120_000;
    private static final LogCapture capture = new LogCapture();

    @Before
    @Override
    public void setUp() {
        LogFactory.enableGuaranteedLogging(LogCaptureTest.class);
        super.setUp();
        capture.start();
    }

    @After
    @Override
    public void tearDown() throws Exception {
        capture.stop();
        super.tearDown();
        LogFactory.disableGuaranteedLogging(LogCaptureTest.class);
    }

    @Test
    public void testAssertOnlyOnceAcceptsARegexWithCapturingGroups() {
        final String marker = "log-capture-one-group-" + System.nanoTime();
        final String barrier = "log-capture-barrier-" + System.nanoTime();
        LOG.info().$(marker).$();
        LOG.info().$(barrier).$();
        capture.waitFor(barrier, 5_000);

        capture.assertOnlyOnce("(" + marker + ")");
    }

    @Test
    public void testAssertOnlyOnceRejectsASecondMatch() {
        final String marker = "log-capture-duplicate-" + System.nanoTime();
        final String barrier = "log-capture-barrier-" + System.nanoTime();
        LOG.info().$(marker).$();
        LOG.info().$(marker).$();
        LOG.info().$(barrier).$();
        capture.waitFor(barrier, 5_000);

        Assert.assertThrows(AssertionError.class, () -> capture.assertOnlyOnce(marker));
    }

    @Test
    public void testWaitForBoundedOverloadThrowsOnAbsentLine() {
        final String marker = "log-capture-witness-absent-" + System.nanoTime();
        AssertionError error = Assert.assertThrows(AssertionError.class, () -> capture.waitFor(marker, 50));
        Assert.assertTrue(
                "expected the AssertionError message to name the awaited string, got: " + error.getMessage(),
                error.getMessage().contains(marker)
        );
    }

    @Test
    public void testWaitForBoundedOverloadThrowsOnExactDeadline() {
        // Real wall-clock sleeps always overshoot the deadline by some margin, so a real-time test
        // can never pin the EXACT elapsed==timeoutMs boundary -- a fake clock/sleeper does. The
        // sleeper jumps the clock straight to start+timeoutMs on its first call; the marker is
        // never logged, so waitFor must treat elapsed==timeoutMs as timed out (the `>=` check), not
        // let it slip through (an old `<` loop / `>` post-check shape treats exact equality as
        // neither "still waiting" nor "timed out" and returns silently instead of throwing).
        final String marker = "log-capture-witness-exact-deadline-" + System.nanoTime();
        final long timeoutMs = 50;
        final long[] clockValue = {1_000_000L};
        capture.setClockForTest(() -> clockValue[0]);
        capture.setSleeperForTest(ms -> clockValue[0] = 1_000_000L + timeoutMs);
        try {
            AssertionError error = Assert.assertThrows(AssertionError.class, () -> capture.waitFor(marker, timeoutMs));
            Assert.assertTrue(
                    "expected the AssertionError message to name the awaited string, got: " + error.getMessage(),
                    error.getMessage().contains(marker)
            );
        } finally {
            capture.setClockForTest(System::currentTimeMillis);
            capture.setSleeperForTest(Os::sleep);
        }
    }

    @Test
    public void testWaitForRegexReturnsOnAMatchEvenPastTheDeadline() {
        // The deadline check must never pre-empt a match that is already in the log: the loop
        // tests find() first and only then the clock. Pinned with a clock frozen past the
        // deadline, so moving the timeout check ahead of find() turns this RED.
        final String marker = "log-capture-regex-present-" + System.nanoTime();
        LOG.info().$(marker).$();
        capture.waitFor(marker, 5_000);

        capture.setClockForTest(() -> 1_000_000L + WAIT_FOR_REGEX_MAX_WAIT_MS);
        capture.setSleeperForTest(ms -> Assert.fail("waitForRegex must not sleep when the regex already matches"));
        try {
            capture.waitForRegex(marker);
        } finally {
            capture.setClockForTest(System::currentTimeMillis);
            capture.setSleeperForTest(Os::sleep);
        }
    }

    @Test
    public void testWaitForRegexThrowsOnExactDeadline() {
        // The waitFor(CharSequence, long) boundary case, for the regex overload: the sleeper jumps
        // the clock straight to start + the built-in 120s budget and the regex never matches, so
        // waitForRegex must treat elapsed == maxWait as timed out. The old shape looped on
        // `elapsed < maxWait` but threw only on `elapsed > maxWait`, returning silently at exactly
        // the deadline -- a timed-out wait indistinguishable from a matched one.
        final String marker = "log-capture-regex-exact-deadline-" + System.nanoTime();
        final long[] clockValue = {1_000_000L};
        capture.setClockForTest(() -> clockValue[0]);
        capture.setSleeperForTest(ms -> clockValue[0] = 1_000_000L + WAIT_FOR_REGEX_MAX_WAIT_MS);
        try {
            AssertionError error = Assert.assertThrows(AssertionError.class, () -> capture.waitForRegex(marker));
            Assert.assertTrue(
                    "expected the AssertionError message to name the awaited regex, got: " + error.getMessage(),
                    error.getMessage().contains(marker)
            );
        } finally {
            capture.setClockForTest(System::currentTimeMillis);
            capture.setSleeperForTest(Os::sleep);
        }
    }
}
