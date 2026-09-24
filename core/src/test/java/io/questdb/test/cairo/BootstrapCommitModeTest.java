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

package io.questdb.test.cairo;

import io.questdb.PropertyKey;
import io.questdb.cairo.CommitMode;
import io.questdb.test.AbstractBootstrapTest;
import org.junit.Assert;
import org.junit.Test;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;

/**
 * The third configuration path from {@link TestCommitModeSwitchTest}'s javadoc, which that class cannot
 * reach: it extends {@code AbstractCairoTest}, while {@code createDummyConfiguration} lives on
 * {@link AbstractBootstrapTest}.
 * <p>
 * Until this existed, the ~75 end-to-end classes that run a real server took their commit mode from a
 * {@code server.conf} line that nothing asserted, so deleting that line would have moved all of their
 * coverage to the SHIPPED default without failing anything -- the precise regression
 * {@code TestCommitModeSwitchTest} exists to prevent, on the one path it was not actually checking.
 * <p>
 * Mirrors {@code com.questdb.EntSuiteCommitModeTest#testEntBootstrapServerConfNamesTheSuiteCommitMode},
 * which has covered the enterprise side of the same seam all along.
 */
public class BootstrapCommitModeTest extends AbstractBootstrapTest {

    @Test
    public void testBootstrapServerConfNamesTheSuiteCommitMode() throws Exception {
        createDummyConfiguration();

        final String propertyPath = PropertyKey.CAIRO_COMMIT_MODE.getPropertyPath();
        final String value = readCommitMode(propertyPath);

        Assert.assertNotNull(
                "the generated server.conf must name " + propertyPath + ", otherwise every "
                        + "AbstractBootstrapTest suite silently runs the SHIPPED default",
                value
        );
        Assert.assertEquals(
                "the generated server.conf must carry the suite commit mode",
                Overrides.TEST_COMMIT_MODE,
                value
        );
        Assert.assertNotEquals(
                "the value must resolve to a real commit mode, was: " + value,
                CommitMode.UNKNOWN,
                CommitMode.fromString(value)
        );
    }

    @Test
    public void testExplicitExtraOverridesTheSuiteCommitMode() throws Exception {
        // The `extra` escape hatch createDummyConfiguration documents ("A caller that wants a different mode
        // passes it in `extra`, which is written after this and wins on a repeated key"). Asserted, because
        // it is what lets a bootstrap test be adaptive-only or nosync-only by PINNING rather than by
        // skipping -- and a pinned test keeps running under every sweep, while a skipped one does not.
        final String propertyPath = PropertyKey.CAIRO_COMMIT_MODE.getPropertyPath();
        createDummyConfiguration(propertyPath + "=sync");

        Assert.assertEquals(
                "an explicit extra must win over the suite default",
                "sync",
                readCommitMode(propertyPath)
        );
    }

    private static String readCommitMode(String propertyPath) throws Exception {
        final Path confFile = Paths.get(root, "conf").resolve("server.conf");
        final List<String> lines = Files.readAllLines(confFile);
        String value = null;
        for (int i = 0, n = lines.size(); i < n; i++) {
            final String line = lines.get(i);
            if (line.startsWith(propertyPath + "=")) {
                // Last occurrence wins, matching the parser: `extra` is written after the defaults.
                value = line.substring(propertyPath.length() + 1).trim();
            }
        }
        return value;
    }
}
