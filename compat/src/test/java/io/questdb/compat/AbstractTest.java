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

package io.questdb.compat;

import io.questdb.Bootstrap;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.std.Files;
import io.questdb.std.FilesFacade;
import io.questdb.std.FilesFacadeImpl;
import io.questdb.std.str.Path;
import org.junit.*;
import org.junit.rules.TemporaryFolder;
import org.junit.rules.TestName;

public class AbstractTest {
    @ClassRule
    public static final TemporaryFolder temp = new TemporaryFolder();
    protected static final Log LOG = LogFactory.getLog(AbstractTest.class);
    protected static String root;
    @Rule
    public final TestName testName = new TestName();

    public static void createTestPath() {
        final Path path = Path.getThreadLocal(root);
        if (Files.exists(path)) {
            return;
        }
        Files.mkdirs(path.of(root).slash$(), 509);
    }

    public static void removeTestPath() {
        final Path path = Path.getThreadLocal(root);
        FilesFacade ff = FilesFacadeImpl.INSTANCE;
        path.slash$();
        Assert.assertTrue("Test dir cleanup error", !ff.exists(path) || ff.rmdir(path.slash$()));
    }

    @BeforeClass
    public static void setUpStatic() throws Exception {
        // it is necessary to initialise logger before tests start
        // logger doesn't relinquish memory until JVM stops
        // which causes memory leak detector to fail should logger be
        // created mid-test
        LOG.info().$("setup logger").$();
        root = temp.newFolder("dbRoot").getAbsolutePath();
    }

    @AfterClass
    public static void tearDownStatic() {
        removeTestPath();
    }

    @Before
    public void setUp() {
        LOG.info().$("Starting test ").$(getClass().getSimpleName()).$('#').$(testName.getMethodName()).$();
        createTestPath();
    }

    @After
    public void tearDown() throws Exception {
        LOG.info().$("Finished test ").$(getClass().getSimpleName()).$('#').$(testName.getMethodName()).$();
        removeTestPath();
    }

    protected static String[] getServerMainArgs() {
        return Bootstrap.getServerMainArgs(root);
    }
}
