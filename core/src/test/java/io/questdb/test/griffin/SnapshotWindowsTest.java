/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2023 QuestDB
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
import io.questdb.test.AbstractGriffinTest;
import org.junit.*;

/**
 * OS specific test that verifies errors returned on snapshot statement execution on Windows.
 */
public class SnapshotWindowsTest extends AbstractGriffinTest {

    private static Path path = new Path();
    private int rootLen;

    @BeforeClass
    public static void setUpStatic() throws Exception {
        path = new Path();
        AbstractGriffinTest.setUpStatic();
    }

    @AfterClass
    public static void tearDownStatic() throws Exception {
        path = Misc.free(path);
        AbstractGriffinTest.tearDownStatic();
    }

    @Before
    public void setUp() {
        // Windows-only tests.
        Assume.assumeTrue(Os.isWindows());

        super.setUp();
        path.of(configuration.getSnapshotRoot()).slash();
        rootLen = path.length();
    }

    @After
    public void tearDown() throws Exception {
        super.tearDown();
        path.trimTo(rootLen);
        configuration.getFilesFacade().rmdir(path.slash$());
    }

    @Test
    public void testSnapshotPrepare() throws Exception {
        assertMemoryLeak(() -> {
            compile("create table test (ts timestamp, name symbol, val int)", sqlExecutionContext);
            try {
                compiler.compile("snapshot prepare", sqlExecutionContext);
                Assert.fail();
            } catch (SqlException ex) {
                Assert.assertTrue(ex.getMessage().startsWith("[0] Snapshots are not supported on Windows"));
            }
        });
    }
}
