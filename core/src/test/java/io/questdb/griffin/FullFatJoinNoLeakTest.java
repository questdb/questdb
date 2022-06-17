/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2022 QuestDB
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

package io.questdb.griffin;

import io.questdb.cairo.AbstractCairoTest;
import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.CairoEngine;
import io.questdb.griffin.engine.LimitOverflowException;
import io.questdb.test.tools.TestUtils;
import org.junit.*;

import java.lang.reflect.Proxy;


public class FullFatJoinNoLeakTest extends AbstractCairoTest {

    protected static SqlExecutionContext sqlExecutionContext;
    protected static SqlCompiler compiler;

    @BeforeClass
    public static void setUpStatic() {
        AbstractCairoTest.setUpStatic();

        final CairoConfiguration ourConfig = configuration;
        configuration = (CairoConfiguration) Proxy.newProxyInstance(
                CairoConfiguration.class.getClassLoader(),
                new Class<?>[]{CairoConfiguration.class},
                (proxy, method, args) -> {
                    if (method.getName().equals("getSqlJoinMetadataPageSize")) {
                        return 10;
                    }
                    if (method.getName().equals("getSqlJoinMetadataMaxResizes")) {
                        return -1;
                    }
                    return method.invoke(ourConfig, args);
                }
        );
        engine = new CairoEngine(configuration, metrics);
        compiler = new SqlCompiler(engine, null, null);
        compiler.setFullFatJoins(true);
        sqlExecutionContext = new SqlExecutionContextImpl(engine, 1);
    }

    @AfterClass
    public static void tearDownStatic() {
        AbstractCairoTest.tearDownStatic();
        compiler.close();
    }

    @Before
    @Override
    public void setUp() {
        super.setUp();
        try {
            // ASKS
            compiler.compile(
                    "create table asks(ask int, ts timestamp) timestamp(ts) partition by none",
                    sqlExecutionContext
            );
            TestUtils.insert(compiler, sqlExecutionContext, "insert into asks values(100, 0)");
            TestUtils.insert(compiler, sqlExecutionContext, "insert into asks values(101, 2);");
            TestUtils.insert(compiler, sqlExecutionContext, "insert into asks values(102, 4);");

            // BIDS
            compiler.compile(
                    "create table bids(bid int, ts timestamp) timestamp(ts) partition by none",
                    sqlExecutionContext
            );
            TestUtils.insert(compiler, sqlExecutionContext, "insert into bids values(101, 1);");
            TestUtils.insert(compiler, sqlExecutionContext, "insert into bids values(102, 3);");
            TestUtils.insert(compiler, sqlExecutionContext, "insert into bids values(103, 5);");
        } catch (SqlException e) {
            Assert.fail(e.getMessage());
        }
    }

    @Test
    public void testAsOfJoinNoLeak() throws Exception {
        TestUtils.assertMemoryLeak(
                () -> {
                    try {
                        compiler.compile(
                                "SELECT \n" +
                                        "    b.timebid timebid,\n" +
                                        "    a.timeask timeask, \n" +
                                        "    b.b b, \n" +
                                        "    a.a a\n" +
                                        "FROM (select b.bid b, b.ts timebid from bids b) b \n" +
                                        "    ASOF JOIN\n" +
                                        "(select a.ask a, a.ts timeask from asks a) a\n" +
                                        "    ON (b.timebid != a.timeask);",
                                sqlExecutionContext
                        );
                        Assert.fail();
                    } catch (LimitOverflowException ex) {
                        TestUtils.assertContains(
                                ex.getFlyweightMessage(),
                                "limit of -1 resizes exceeded in FastMap"
                        );
                    }
                });
    }

    @Test
    public void testLtJoinNoLeak() throws Exception {
        TestUtils.assertMemoryLeak(
                () -> {
                    try {
                        compiler.compile(
                                "SELECT \n" +
                                        "    b.timebid timebid,\n" +
                                        "    a.timeask timeask, \n" +
                                        "    b.b b, \n" +
                                        "    a.a a\n" +
                                        "FROM (select b.bid b, b.ts timebid from bids b) b \n" +
                                        "    LT JOIN\n" +
                                        "(select a.ask a, a.ts timeask from asks a) a\n" +
                                        "    ON (b.timebid != a.timeask);",
                                sqlExecutionContext
                        );
                        Assert.fail();
                    } catch (LimitOverflowException ex) {
                        TestUtils.assertContains(
                                ex.getFlyweightMessage(),
                                "limit of -1 resizes exceeded in FastMap"
                        );
                    }
                });
    }
}
