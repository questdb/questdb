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

package io.questdb.test.cutlass.http;

import io.questdb.griffin.engine.functions.rnd.SharedRandom;
import io.questdb.std.Misc;
import io.questdb.std.Rnd;
import io.questdb.test.AbstractTest;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.Timeout;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

public class QueryRegistryTest extends AbstractTest {
    private static TestHttpClient testHttpClient;
    private final AtomicInteger counter = new AtomicInteger();
    @Rule
    public Timeout timeout = Timeout.builder()
            .withTimeout(10 * 60 * 1000, TimeUnit.MILLISECONDS)
            .withLookingForStuckThread(true)
            .build();

    @BeforeClass
    public static void setUpStatic() throws Exception {
        AbstractTest.setUpStatic();
        // this method could be called for multiple iterations within single test
        // we have some synthetic re-runs
        testHttpClient = Misc.free(testHttpClient);
        testHttpClient = new TestHttpClient();
    }

    @AfterClass
    public static void tearDownStatic() {
        testHttpClient = Misc.free(testHttpClient);
        AbstractTest.tearDownStatic();
    }

    @Before
    public void setUp() {
        super.setUp();
        SharedRandom.RANDOM.set(new Rnd());
        testHttpClient.setKeepConnection(false);
        counter.set(0);
    }

    @Test
    public void testAlterTable() throws Exception {
        getSimpleTester().run((engine, sqlExecutionContext) -> {
            assertGet("{\"ddl\":\"OK\"}", "CREATE TABLE tab (value LONG)");
            assertGet("{\"dml\":\"OK\"}", "INSERT INTO tab (value) VALUES (6)");
            assertGet("{\"ddl\":\"OK\"}", "ALTER TABLE tab ADD COLUMN text VARCHAR");
            assertGet("{\"dml\":\"OK\"}", "INSERT INTO tab VALUES (9, 'hoho')");
            assertGet(
                    "{\"query\":\"tab\"," +
                            "\"columns\":[{\"name\":\"value\",\"type\":\"LONG\"},{\"name\":\"text\",\"type\":\"VARCHAR\"}]," +
                            "\"timestamp\":-1," +
                            "\"dataset\":[[6,null],[9,\"hoho\"]]," +
                            "\"count\":2}",
                    "tab"
            );
            assertQueryRegistry();
        });
    }

    @Test
    public void testCreateTable() throws Exception {
        getSimpleTester().run((engine, sqlExecutionContext) -> {
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
        });
    }

    @Test
    public void testCreateTableAsSelect() throws Exception {
        getSimpleTester().run((engine, sqlExecutionContext) -> {
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
        });
    }

    @Test
    public void testDropTable() throws Exception {
        getSimpleTester().run((engine, sqlExecutionContext) -> {
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
        });
    }

    @Test
    public void testExplain() throws Exception {
        getSimpleTester().run((engine, sqlExecutionContext) -> {
            assertGet("{\"ddl\":\"OK\"}", "CREATE TABLE tab (value LONG)");
            assertGet("{\"dml\":\"OK\"}", "INSERT INTO tab (value) VALUES (10)");
            assertGet(
                    "{\"query\":\"explain tab\"," +
                            "\"columns\":[{\"name\":\"QUERY PLAN\",\"type\":\"STRING\"}]," +
                            "\"timestamp\":-1," +
                            "\"dataset\":[[\"PageFrame\"],[\"&nbsp;&nbsp;&nbsp;&nbsp;Row forward scan\"],[\"&nbsp;&nbsp;&nbsp;&nbsp;Frame forward scan on: tab\"]]," +
                            "\"count\":3}",
                    "explain tab"
            );
            assertQueryRegistry();
        });
    }

    @Test
    public void testInsertAsSelect() throws Exception {
        getSimpleTester().run((engine, sqlExecutionContext) -> {
            assertGet("{\"ddl\":\"OK\"}", "CREATE TABLE tab (value LONG)");
            assertGet("{\"dml\":\"OK\"}", "INSERT INTO tab (value) VALUES (1)");
            assertGet("{\"ddl\":\"OK\"}", "INSERT INTO tab SELECT x+5 FROM long_sequence(2)");
            assertGet(
                    "{\"query\":\"tab\"," +
                            "\"columns\":[{\"name\":\"value\",\"type\":\"LONG\"}]," +
                            "\"timestamp\":-1," +
                            "\"dataset\":[[1],[6],[7]]," +
                            "\"count\":3}",
                    "tab"
            );
            assertQueryRegistry();
        });
    }

    @Test
    public void testTruncateTable() throws Exception {
        getSimpleTester().run((engine, sqlExecutionContext) -> {
            assertGet("{\"ddl\":\"OK\"}", "CREATE TABLE tab (value LONG)");
            assertGet("{\"ddl\":\"OK\"}", "INSERT INTO tab SELECT x FROM long_sequence(3)");
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
        getSimpleTester().run((engine, sqlExecutionContext) -> {
            assertGet("{\"ddl\":\"OK\"}", "CREATE TABLE tab (value LONG)");
            assertGet("{\"dml\":\"OK\"}", "INSERT INTO tab (value) VALUES (6)");
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
        });
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

    private HttpQueryTestBuilder getSimpleTester() {
        return new HttpQueryTestBuilder()
                .withTempFolder(root)
                .withWorkerCount(1)
                .withHttpServerConfigBuilder(new HttpServerConfigurationBuilder())
                .withTelemetry(false);
    }
}
