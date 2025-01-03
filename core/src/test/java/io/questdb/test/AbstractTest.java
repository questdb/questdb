/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2024 QuestDB
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

package io.questdb.test;

import io.questdb.Bootstrap;
import io.questdb.Metrics;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.test.tools.TestUtils;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.rules.TemporaryFolder;
import org.junit.rules.TestName;
import org.junit.runner.OrderWith;

@OrderWith(RandomOrder.class)
public class AbstractTest {
    @ClassRule
    public static final TemporaryFolder temp = new TemporaryFolder();
    protected static final Log LOG = LogFactory.getLog(AbstractTest.class);
    protected static String root;
    @Rule
    public final TestName testName = new TestName();

    @BeforeClass
    public static void setUpStatic() throws Exception {
        TestOs.init();
        // it is necessary to initialise logger before tests start
        // logger doesn't relinquish memory until JVM stops
        // which causes memory leak detector to fail should logger be
        // created mid-test
        LOG.info().$("setup logger").$();
        root = temp.newFolder("dbRoot").getAbsolutePath();
    }

    @AfterClass
    public static void tearDownStatic() {
        TestUtils.removeTestPath(root);
    }

    @Before
    public void setUp() {
        LOG.info().$("Starting test ").$(getClass().getSimpleName()).$('#').$(testName.getMethodName()).$();
        TestUtils.createTestPath(root);
        Metrics.ENABLED.clear();
    }

    @After
    public void tearDown() throws Exception {
        LOG.info().$("Finished test ").$(getClass().getSimpleName()).$('#').$(testName.getMethodName()).$();
        TestUtils.removeTestPath(root);
    }

    protected static String[] getServerMainArgs() {
        return Bootstrap.getServerMainArgs(root);
    }
}
