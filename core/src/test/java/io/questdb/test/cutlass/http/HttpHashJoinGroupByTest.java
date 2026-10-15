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

package io.questdb.test.cutlass.http;

import io.questdb.cairo.CairoEngine;
import io.questdb.cairo.sql.Record;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.engine.functions.test.TestLatchedCounterFunctionFactory;
import io.questdb.std.str.Utf8StringSink;
import io.questdb.test.AbstractTest;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

import java.util.concurrent.atomic.AtomicInteger;

/**
 * Runs the fused hash join GROUP BY through the HTTP endpoints. Each connection caches its
 * compiled factories, and a cached fused factory throws TableReferenceOutOfDateException once its
 * build table holds more than twice the probe table's rows: /exec and /exp must compile the query
 * again and answer from the new plan.
 */
public class HttpHashJoinGroupByTest extends AbstractTest {
    // The test function closes with the factory that holds it, so the count of closes tells a
    // request that reused the cached factory from one that compiled the query again.
    private static final String SQL = """
            SELECT d.g, count(*) n, sum(f.v) v FROM f JOIN (SELECT * FROM d WHERE test_latched_counter()) d
            ON f.k = d.k ORDER BY d.g""";

    @Test
    public void testExportRecompilesCachedFusedQueryOnceBuildOutgrowsProbe() throws Exception {
        assertRecompile(
                "/exp",
                """
                        "g","n","v"\r
                        0,60,3000\r
                        1,70,3565\r
                        2,70,3535\r
                        """,
                """
                        "g","n","v"\r
                        0,490,24715\r
                        1,505,25515\r
                        2,500,25220\r
                        """
        );
    }

    @Test
    public void testJsonQueryRecompilesCachedFusedQueryOnceBuildOutgrowsProbe() throws Exception {
        assertRecompile(
                "/exec",
                "{\"query\":\"SELECT d.g, count(*) n, sum(f.v) v FROM f JOIN (SELECT * FROM d WHERE test_latched_counter()) d\\nON f.k = d.k ORDER BY d.g\","
                        + "\"columns\":[{\"name\":\"g\",\"type\":\"INT\"},{\"name\":\"n\",\"type\":\"LONG\"},{\"name\":\"v\",\"type\":\"LONG\"}],"
                        + "\"timestamp\":-1,\"dataset\":[[0,60,3000],[1,70,3565],[2,70,3535]],\"count\":3}",
                "{\"query\":\"SELECT d.g, count(*) n, sum(f.v) v FROM f JOIN (SELECT * FROM d WHERE test_latched_counter()) d\\nON f.k = d.k ORDER BY d.g\","
                        + "\"columns\":[{\"name\":\"g\",\"type\":\"INT\"},{\"name\":\"n\",\"type\":\"LONG\"},{\"name\":\"v\",\"type\":\"LONG\"}],"
                        + "\"timestamp\":-1,\"dataset\":[[0,490,24715],[1,505,25515],[2,500,25220]],\"count\":3}"
        );
    }

    private static void assertRecompile(String url, String expectedBefore, String expectedAfter) throws Exception {
        getSimpleTester().run((engine, _) -> {
            createTables(engine);
            final AtomicInteger closeCount = new AtomicInteger();
            TestLatchedCounterFunctionFactory.reset(new TestLatchedCounterFunctionFactory.Callback() {
                @Override
                public void onClose() {
                    closeCount.incrementAndGet();
                }

                @Override
                public boolean onGet(Record rec, int count) {
                    return true;
                }
            });
            try (TestHttpClient client = new TestHttpClient()) {
                // One connection keeps one select cache across the requests.
                client.setKeepConnection(true);
                final Utf8StringSink sink = new Utf8StringSink();
                client.toSink(url, SQL, sink);
                TestUtils.assertEquals(expectedBefore, sink.toString());
                assertFusedPlan("inputSwapped: false");
                final int compiledCloses = closeCount.get();
                // The cached factory builds d, which now holds 550 rows against 100 of f.
                engine.execute("INSERT INTO d SELECT (x % 40)::INT, (x % 3)::INT FROM long_sequence(500)");
                sink.clear();
                client.toSink(url, SQL, sink);
                TestUtils.assertEquals(expectedAfter, sink.toString());
                // A new compile builds f, the smaller table now.
                assertFusedPlan("inputSwapped: true");
                final int recompiledCloses = closeCount.get();
                Assert.assertTrue("expected the stale factory to close", recompiledCloses > compiledCloses);
                // The recompiled factory went back to the cache, and the next request reuses it.
                sink.clear();
                client.toSink(url, SQL, sink);
                TestUtils.assertEquals(expectedAfter, sink.toString());
                Assert.assertEquals(recompiledCloses, closeCount.get());
            } finally {
                TestLatchedCounterFunctionFactory.reset(null);
            }
        });
    }

    private static void assertFusedPlan(String orientation) {
        // A connection of its own leaves the select cache of the asserted one alone.
        try (TestHttpClient client = new TestHttpClient()) {
            final Utf8StringSink sink = new Utf8StringSink();
            client.toSink("/exec", "EXPLAIN " + SQL, sink);
            TestUtils.assertContains(sink.toString(), "Async Hash Join Group By workers: 1");
            TestUtils.assertContains(sink.toString(), orientation);
        }
    }

    // f holds 100 rows over the keys 0..19, and d 50 rows over the keys 0..24, so the INNER join
    // builds d, the smaller table.
    private static void createTables(CairoEngine engine) throws SqlException {
        engine.execute("CREATE TABLE f (k INT, v LONG)");
        engine.execute("CREATE TABLE d (k INT, g INT)");
        engine.execute("INSERT INTO f SELECT (x % 20)::INT, x FROM long_sequence(100)");
        engine.execute("INSERT INTO d SELECT (x % 25)::INT, (x % 3)::INT FROM long_sequence(50)");
    }
}
