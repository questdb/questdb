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

import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.std.ObjList;
import io.questdb.test.cairo.ConfigurationOverrides;
import io.questdb.test.cairo.Overrides;
import io.questdb.test.tools.TestUtils;
import org.junit.*;
import org.junit.rules.TemporaryFolder;
import org.junit.rules.TestName;

public class AbstractTest {
    protected static final Log LOG = LogFactory.getLog(AbstractTest.class);
    @ClassRule
    public static TemporaryFolder temp = new TemporaryFolder();
    protected static QuestDBTestNode node1;
    protected static ObjList<QuestDBTestNode> nodes = new ObjList<>();
    protected static CharSequence root;
    @Rule
    public TestName testName = new TestName();

    @BeforeClass
    public static void setUpStatic() throws Exception {
        // it is necessary to initialise logger before tests start
        // logger doesn't relinquish memory until JVM stops
        // which causes memory leak detector to fail should logger be
        // created mid-test
        LOG.info().$("begin").$();
        node1 = newNode(temp, 1, "dbRoot", new StaticOverrides());
        root = node1.getRoot();
    }

    @AfterClass
    public static void tearDownStatic() throws Exception {
        forEachNode(QuestDBTestNode::closeCairo);
        nodes.clear();
    }

    @Before
    public void setUp() {
        LOG.info().$("Starting test ").$(getClass().getSimpleName()).$('#').$(testName.getMethodName()).$();
        forEachNode(QuestDBTestNode::setUpCairo);
    }

    @After
    public void tearDown() throws Exception {
        tearDown(true);
    }

    public void tearDown(boolean removeDir) {
        LOG.info().$("Tearing down test ").$(getClass().getSimpleName()).$('#').$(testName.getMethodName()).$();
        forEachNode(node -> node.tearDownCairo(removeDir));
    }

    protected static void forEachNode(AbstractCairoTest.QuestDBNodeTask task) {
        for (int i = 0; i < nodes.size(); i++) {
            task.run(nodes.get(i));
        }
    }

    protected static QuestDBTestNode newNode(TemporaryFolder temp, int nodeId, String dbRoot, ConfigurationOverrides overrides) {
        final QuestDBTestNode node = new QuestDBTestNode(nodeId);
        node.initCairo(temp, dbRoot, overrides);
        nodes.add(node);
        return node;
    }

    protected static QuestDBTestNode newNode(TemporaryFolder temp, int nodeId) {
        return newNode(temp, nodeId, "dbRoot" + nodeId, new Overrides());
    }

    protected static String[] getServerMainArgs() {
        return TestUtils.getServerMainArgs(root);
    }
}
