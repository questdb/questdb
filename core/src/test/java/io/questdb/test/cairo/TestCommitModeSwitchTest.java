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

import io.questdb.cairo.CommitMode;
import io.questdb.test.AbstractCairoTest;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.Test;

/**
 * The suite must run one commit mode, chosen in one place.
 * <p>
 * There are three ways a test ends up with a configuration -- {@link Overrides} for anything extending
 * {@code AbstractCairoTest}, {@link DefaultTestCairoConfiguration} for direct instantiation, and the
 * generated {@code server.conf} for the {@code AbstractBootstrapTest} suites that run a real server. They
 * used to disagree: the last of those set nothing at all, so 74 end-to-end classes silently followed
 * whatever the shipped default happened to be, and changing that default moved their coverage without
 * failing anything.
 * <p>
 * This fails if a fourth path is added and forgets the switch, or if one of the three drifts.
 * <p>
 * Paths 1 and 2 are asserted here. Path 3 is asserted by {@link BootstrapCommitModeTest}, which this class
 * cannot reach: it extends {@code AbstractCairoTest}, while {@code createDummyConfiguration} lives on
 * {@code AbstractBootstrapTest}.
 */
public class TestCommitModeSwitchTest extends AbstractCairoTest {

    @Test
    public void testEveryConfigurationPathAgreesOnTheSuiteCommitMode() {
        final int expected = CommitMode.fromString(Overrides.TEST_COMMIT_MODE);
        Assert.assertNotEquals(
                "questdb.test.commit.mode must name a real commit mode, was: " + Overrides.TEST_COMMIT_MODE,
                CommitMode.UNKNOWN,
                expected
        );

        // Path 1: the configuration this test itself is running under, built through Overrides.
        Assert.assertEquals(
                "the running test configuration must use the suite commit mode",
                expected,
                configuration.getCommitMode()
        );

        // Path 2: direct instantiation, used by tests that build their own configuration.
        Assert.assertEquals(
                "DefaultTestCairoConfiguration must use the suite commit mode",
                expected,
                new DefaultTestCairoConfiguration(root).getCommitMode()
        );
    }

    /**
     * The suite's mode is deliberately NOT the shipped one, so that the durable path is exercised
     * everywhere while users keep the cheaper default. If these two ever coincide it should be because
     * someone moved {@link CommitMode#DEFAULT} on purpose, having run the benchmark -- not by accident.
     */
    @Test
    public void testShippedDefaultIsSeparateFromTheSuiteDefault() {
        // The shipped default is asserted UNCONDITIONALLY: no sweep flag may move CommitMode.DEFAULT.
        Assert.assertEquals(
                "the shipped default is nosync until an ingest benchmark justifies moving it",
                CommitMode.NOSYNC,
                CommitMode.DEFAULT
        );
        // The SUITE default is asserted only when nobody asked for something else. An explicit
        // -Dquestdb.test.commit.mode=X is a deliberate sweep, not a regression in the default, so it skips
        // this half rather than failing it -- otherwise the sweep documented on Overrides.TEST_COMMIT_MODE
        // cannot run at all. Skip, not early return: a skip is counted, a return would be a false green.
        Assume.assumeFalse(
                "explicit -Dquestdb.test.commit.mode=" + Overrides.TEST_COMMIT_MODE
                        + ": sweeping, so the adaptive-default pin does not apply",
                Overrides.TEST_COMMIT_MODE_EXPLICIT
        );
        Assert.assertEquals(
                "the suite runs adaptive so the durable path is covered by every test",
                CommitMode.ADAPTIVE,
                CommitMode.fromString(Overrides.TEST_COMMIT_MODE)
        );
    }
}
