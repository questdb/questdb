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


package io.questdb.test.cairo;

import io.questdb.PropertyKey;
import io.questdb.cairo.CommitMode;
import io.questdb.test.AbstractCairoTest;
import org.junit.Assert;
import org.junit.Test;

/**
 * A fuzz test class - this one, by its name - runs under a commit mode of its own, drawn by
 * {@code AbstractTest.fuzzCommitModeRule} independently of the suite's. Asserts that the draw reaches the
 * configuration paths {@link TestCommitModeSwitchTest} covers, and that a mode the test pins still wins.
 */
public class FuzzCommitModeTest extends AbstractCairoTest {

    @Test
    public void testFuzzClassRunsUnderItsOwnCommitMode() {
        final String fuzzCommitMode = Overrides.getFuzzCommitMode();
        Assert.assertNotNull("a fuzz test class must draw a commit mode", fuzzCommitMode);
        if (Overrides.FUZZ_COMMIT_MODE != null) {
            Assert.assertEquals("-Dquestdb.test.fuzz.commit.mode must pin the draw", Overrides.FUZZ_COMMIT_MODE, fuzzCommitMode);
        }
        final int expected = CommitMode.fromString(fuzzCommitMode);
        Assert.assertNotEquals("the draw must name a real commit mode, was: " + fuzzCommitMode, CommitMode.UNKNOWN, expected);
        Assert.assertEquals(fuzzCommitMode, Overrides.getTestCommitMode());
        Assert.assertEquals("the running test configuration must use the draw", expected, engine.getConfiguration().getCommitMode());
        Assert.assertEquals("DefaultTestCairoConfiguration must use the draw", expected, new DefaultTestCairoConfiguration(root).getCommitMode());
    }

    @Test
    public void testPinnedCommitModeWinsOverTheDraw() {
        final int pinned = CommitMode.fromString(Overrides.getFuzzCommitMode()) == CommitMode.SYNC ? CommitMode.NOSYNC : CommitMode.SYNC;
        setProperty(PropertyKey.CAIRO_COMMIT_MODE, CommitMode.toString(pinned));
        Assert.assertEquals(pinned, engine.getConfiguration().getCommitMode());
    }
}
