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

package io.questdb.test.griffin;

import io.questdb.griffin.SqlException;
import io.questdb.std.Misc;
import io.questdb.std.Os;
import io.questdb.std.str.Path;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.tools.TestUtils;
import org.junit.*;

/**
 * OS specific test that verifies errors returned on snapshot statement execution on Windows.
 */
public class CheckpointWindowsTest extends AbstractCairoTest {

    private static Path path = new Path();
    private int rootLen;

    @BeforeClass
    public static void setUpStatic() throws Exception {
        path = new Path();
        AbstractCairoTest.setUpStatic();
    }

    @AfterClass
    public static void tearDownStatic() {
        path = Misc.free(path);
        AbstractCairoTest.tearDownStatic();
    }

    @Before
    public void setUp() {
        // Windows-only tests.
        Assume.assumeTrue(Os.isWindows());

        super.setUp();
        path.of(configuration.getCheckpointRoot()).slash();
        rootLen = path.size();
    }

    @After
    public void tearDown() throws Exception {
        super.tearDown();
        path.trimTo(rootLen);
        configuration.getFilesFacade().rmdir(path.slash());
    }

    @Test
    public void testCheckpointCreate() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table test (ts timestamp, name symbol, val int)");
            try {
                assertExceptionNoLeakCheck("checkpoint create");
            } catch (SqlException ex) {
                TestUtils.assertContains(ex.getFlyweightMessage(), "Checkpoint is not supported on Windows");
            }
        });
    }
}
