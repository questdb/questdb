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

package io.questdb.test.cutlass.http;

import io.questdb.griffin.engine.functions.rnd.SharedRandom;
import io.questdb.std.MemoryTag;
import io.questdb.std.Rnd;
import io.questdb.std.Unsafe;
import io.questdb.test.AbstractTest;
import io.questdb.test.tools.TestUtils;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.Test;

import java.util.concurrent.atomic.AtomicInteger;

public class QueryRegistryTest extends AbstractTest {
    private static final TestHttpClient testHttpClient = new TestHttpClient();
    private final AtomicInteger counter = new AtomicInteger();

    @AfterClass
    public static void tearDownStatic() {
        testHttpClient.close();
        AbstractTest.tearDownStatic();
        assert Unsafe.getMemUsedByTag(MemoryTag.NATIVE_HTTP_CONN) == 0;
    }

    @Override
    @Before
    public void setUp() {
        super.setUp();
        SharedRandom.RANDOM.set(new Rnd());
        testHttpClient.setKeepConnection(false);
        counter.set(0);
    }

    @Test
    public void testCreateTable() throws Exception {
        TestUtils.assertMemoryLeak(() -> getSimpleTester().run((engine, sqlExecutionContext) -> {
            assertGet("{\"ddl\":\"OK\"}", "CREATE TABLE tab (value INT, ts TIMESTAMP) TIMESTAMP(ts)");
            assertGet(
                    "{\"query\":\"tab\"," +
                            "\"columns\":[{\"name\":\"value\",\"type\":\"INT\"},{\"name\":\"ts\",\"type\":\"TIMESTAMP\"}]," +
                            "\"timestamp\":1," +
                            "\"dataset\":[]," +
                            "\"count\":0}",
                    "tab"
            );
            assertQueryRegistry();
        }));
    }

    @Test
    public void testCreateTableAsSelect() throws Exception {
        TestUtils.assertMemoryLeak(() -> getSimpleTester().run((engine, sqlExecutionContext) -> {
            assertGet("{\"ddl\":\"OK\"}", "CREATE TABLE tab AS (SELECT x FROM long_sequence(10))");
            assertGet("{\"ddl\":\"OK\"}", "CREATE TABLE tab2 AS (tab WHERE x>8)");
            assertGet(
                    "{\"query\":\"tab2\"," +
                            "\"columns\":[{\"name\":\"x\",\"type\":\"LONG\"}]" +
                            ",\"timestamp\":-1," +
                            "\"dataset\":[[9],[10]]," +
                            "\"count\":2}",
                    "tab2"
            );
            assertQueryRegistry();
        }));
    }

    @Test
    public void testDropTable() throws Exception {
        TestUtils.assertMemoryLeak(() -> getSimpleTester().run((engine, sqlExecutionContext) -> {
            assertGet("{\"ddl\":\"OK\"}", "CREATE TABLE tab (value LONG)");
            assertGet("{\"ddl\":\"OK\"}", "DROP TABLE tab");
            assertGet(
                    "{\"query\":\"select id, table_name from tables()\"," +
                            "\"columns\":[{\"name\":\"id\",\"type\":\"INT\"},{\"name\":\"table_name\",\"type\":\"STRING\"}]," +
                            "\"timestamp\":-1," +
                            "\"dataset\":[]," +
                            "\"count\":0}",
                    "select id, table_name from tables()"
            );
            assertQueryRegistry();
        }));
    }

    @Test
    public void testExplain() throws Exception {
        TestUtils.assertMemoryLeak(() -> getSimpleTester().run((engine, sqlExecutionContext) -> {
            assertGet("{\"ddl\":\"OK\"}", "CREATE TABLE tab (value LONG)");
            assertGet("{\"dml\":\"OK\"}", "INSERT INTO tab select 10");
            assertGet(
                    "{\"query\":\"explain tab\"," +
                            "\"columns\":[{\"name\":\"QUERY PLAN\",\"type\":\"STRING\"}]," +
                            "\"timestamp\":-1," +
                            "\"dataset\":[[\"PageFrame\"],[\"&nbsp;&nbsp;&nbsp;&nbsp;Row forward scan\"],[\"&nbsp;&nbsp;&nbsp;&nbsp;Frame forward scan on: tab\"]]," +
                            "\"count\":3}",
                    "explain tab"
            );
            assertQueryRegistry();
        }));
    }

    @Test
    public void testInsertAsSelect() throws Exception {
        TestUtils.assertMemoryLeak(() -> getSimpleTester().run((engine, sqlExecutionContext) -> {
            assertGet("{\"ddl\":\"OK\"}", "CREATE TABLE tab (value LONG)");
            assertGet("{\"dml\":\"OK\"}", "INSERT INTO tab SELECT x+5 FROM long_sequence(2)");
            assertGet(
                    "{\"query\":\"tab\"," +
                            "\"columns\":[{\"name\":\"value\",\"type\":\"LONG\"}]," +
                            "\"timestamp\":-1," +
                            "\"dataset\":[[6],[7]]," +
                            "\"count\":2}",
                    "tab"
            );
            assertQueryRegistry();
        }));
    }

    @Test
    public void testTruncateTable() throws Exception {
        getSimpleTester().run((engine, sqlExecutionContext) -> {
            assertGet("{\"ddl\":\"OK\"}", "CREATE TABLE tab (value LONG)");
            assertGet("{\"dml\":\"OK\"}", "INSERT INTO tab SELECT x FROM long_sequence(3)");
            assertGet("{\"ddl\":\"OK\"}", "TRUNCATE TABLE tab");
            assertGet(
                    "{\"query\":\"tab\"," +
                            "\"columns\":[{\"name\":\"value\",\"type\":\"LONG\"}]," +
                            "\"timestamp\":-1," +
                            "\"dataset\":[]," +
                            "\"count\":0}",
                    "tab"
            );
            assertQueryRegistry();
        });
    }

    @Test
    public void testUpdateTable() throws Exception {
        TestUtils.assertMemoryLeak(() -> getSimpleTester().run((engine, sqlExecutionContext) -> {
            assertGet("{\"ddl\":\"OK\"}", "CREATE TABLE tab (value LONG)");
            assertGet("{\"dml\":\"OK\"}", "INSERT INTO tab select 6");
            assertGet("{\"dml\":\"OK\",\"updated\":1}", "UPDATE tab SET value=5");
            assertGet(
                    "{\"query\":\"tab\"," +
                            "\"columns\":[{\"name\":\"value\",\"type\":\"LONG\"}]," +
                            "\"timestamp\":-1," +
                            "\"dataset\":[[5]]," +
                            "\"count\":1}",
                    "tab"
            );
            assertQueryRegistry();
        }));
    }

    private void assertGet(CharSequence expectedResponse, CharSequence sql) {
        testHttpClient.assertGet(expectedResponse, sql);
        counter.incrementAndGet();
    }

    // assert that the only active query is query_activity(),
    // and that the query id is equal to the number of queries run previously,
    // meaning all queries were registered
    private void assertQueryRegistry() {
        testHttpClient.assertGet(
                "{\"query\":\"select query_id, query from query_activity()\"," +
                        "\"columns\":[{\"name\":\"query_id\",\"type\":\"LONG\"},{\"name\":\"query\",\"type\":\"STRING\"}]," +
                        "\"timestamp\":-1," +
                        "\"dataset\":[[" + counter.get() + ",\"select query_id, query from query_activity()\"]]," +
                        "\"count\":1}",
                "select query_id, query from query_activity()"
        );
    }
}
