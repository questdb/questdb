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
import io.questdb.test.AbstractBootstrapTest;
import org.junit.Assert;
import org.junit.Test;

/**
 * {@link FuzzCommitModeTest} for the generated {@code server.conf}: a fuzz test class - this one, by its name - that
 * runs a real server takes the commit mode it drew, not the suite's.
 */
public class BootstrapFuzzCommitModeTest extends AbstractBootstrapTest {

    @Test
    public void testBootstrapServerConfNamesTheFuzzCommitMode() throws Exception {
        final String fuzzCommitMode = Overrides.getFuzzCommitMode();
        Assert.assertNotNull("a fuzz test class must draw a commit mode", fuzzCommitMode);
        createDummyConfiguration();
        Assert.assertEquals(
                "the generated server.conf must carry the fuzz class's commit mode",
                fuzzCommitMode,
                BootstrapCommitModeTest.readCommitMode(PropertyKey.CAIRO_COMMIT_MODE.getPropertyPath())
        );
    }
}
