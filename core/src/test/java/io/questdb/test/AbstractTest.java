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

package io.questdb.test;

import io.questdb.*;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.test.tools.TestUtils;
import org.junit.*;
import org.junit.rules.TemporaryFolder;
import org.junit.rules.TestName;

public class AbstractTest {
    protected static final Log LOG = LogFactory.getLog(AbstractTest.class);
    @ClassRule
    public static TemporaryFolder temp = new TemporaryFolder();
    protected static String root;
    @Rule
    public TestName testName = new TestName();

    @SuppressWarnings("unused")
    public static ServerMain newServer(boolean enableHttp, boolean enableLineTcp, boolean enablePgWire, int workerCountShared, FactoryProvider factoryProvider) {
        return new ServerMain(new Bootstrap(new DefaultBootstrapConfiguration() {

            // although `root` is supplied to the server main, it is ultimately ignored in favour of that provided by the configuration
            // We only supply it for server main to pass argument validation
            @Override
            public ServerConfiguration getServerConfiguration(Bootstrap bootstrap) {
                return new TestServerConfiguration(
                        root,
                        enableHttp,
                        enableLineTcp,
                        enablePgWire,
                        workerCountShared,
                        0,
                        0,
                        0,
                        factoryProvider
                );
            }
        }, TestUtils.getServerMainArgs(root)));
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
    public static void tearDownStatic() throws Exception {
        TestUtils.removeTestPath(root);
    }

    @Before
    public void setUp() {
        LOG.info().$("Starting test ").$(getClass().getSimpleName()).$('#').$(testName.getMethodName()).$();
        TestUtils.createTestPath(root);
    }

    @After
    public void tearDown() throws Exception {
        TestUtils.removeTestPath(root);
    }

    protected static String[] getServerMainArgs() {
        return TestUtils.getServerMainArgs(root);
    }
}
