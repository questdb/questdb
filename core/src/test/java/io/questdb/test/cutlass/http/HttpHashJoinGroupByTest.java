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
 * Runs the fused hash join GROUP BY through the HTTP endpoints. The HTTP server caches compiled
 * factories in one select cache that all its connections share, and a cached fused factory throws
 * TableReferenceOutOfDateException once its build table holds more than twice the probe table's
 * rows: /exec and /exp must close the cached factory, compile the query again, answer from the new
 * plan and cache the new factory in place of the stale one.
 */
public class HttpHashJoinGroupByTest extends AbstractTest {
    // Each instance of the test function reports to the callback that the test installed before the
    // server compiled the instance. A fresh callback before each request therefore tells a factory
    // that an earlier request compiled apart from one that the server compiles later.
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
            try (TestHttpClient client = new TestHttpClient()) {
                // The connection serves one request at a time, and the server returns the factory
                // of a request to the shared select cache before it reads the next request on the
                // connection, so each request after the first finds the factory that the request
                // before it cached.
                client.setKeepConnection(true);
                final Utf8StringSink sink = new Utf8StringSink();
                final FunctionCalls cachedCalls = FunctionCalls.install();
                client.toSink(url, SQL, sink);
                TestUtils.assertEquals(expectedBefore, sink.toString());
                // The planner closes the functions that it compiles to analyze the query before the
                // server answers, so the cached factory holds the only open function of cachedCalls.
                final int cachedCloses = cachedCalls.closes.get();
                // The functions that the server compiles from here on, the EXPLAIN's among them,
                // report to recompiledCalls. EXPLAIN does not run the filter.
                final FunctionCalls recompiledCalls = FunctionCalls.install();
                assertFusedPlan("inputSwapped: false");
                // The cached factory builds d, which now holds 550 rows against 100 of f.
                engine.execute("INSERT INTO d SELECT (x % 40)::INT, (x % 3)::INT FROM long_sequence(500)");
                sink.clear();
                client.toSink(url, SQL, sink);
                TestUtils.assertEquals(expectedAfter, sink.toString());
                // Both build orientations return the same rows, so the calls show which factory
                // answered: the server closes the stale cached factory before it answers, and a
                // factory that the server compiled for this request filters d.
                Assert.assertTrue("expected the stale cached factory to close", cachedCalls.closes.get() > cachedCloses);
                final int recompiledGets = recompiledCalls.gets.get();
                Assert.assertTrue("expected a factory compiled for the request to filter d", recompiledGets > 0);
                // A new compile builds f, the smaller table now.
                assertFusedPlan("inputSwapped: true");
                // The recompiled factory went back to the cache, and the next request reuses it, so
                // the server compiles no function for it that reports to reusedCalls.
                final FunctionCalls reusedCalls = FunctionCalls.install();
                sink.clear();
                client.toSink(url, SQL, sink);
                TestUtils.assertEquals(expectedAfter, sink.toString());
                Assert.assertTrue("expected the recompiled factory to filter d again", recompiledCalls.gets.get() > recompiledGets);
                Assert.assertEquals("expected no new compile to filter d", 0, reusedCalls.gets.get());
            } finally {
                TestLatchedCounterFunctionFactory.reset(null);
            }
        });
    }

    private static void assertFusedPlan(String orientation) {
        // The server compiles the EXPLAIN afresh: its text differs from the query's, so its select
        // cache key does too, and the server closes its factory instead of caching it.
        try (TestHttpClient client = new TestHttpClient()) {
            client.setKeepConnection(true);
            final Utf8StringSink sink = new Utf8StringSink();
            client.toSink("/exec", "EXPLAIN " + SQL, sink);
            TestUtils.assertContains(sink.toString(), "Async Hash Join Group By workers: 1");
            TestUtils.assertContains(sink.toString(), orientation);
            // The server closes the EXPLAIN factory, and the test function with it, only after it
            // sends the response, when it resets the connection for the next request. The
            // connection serves one request at a time, so the response to the next request on it
            // proves the close happened, and no close of the EXPLAIN lands in a later step of the
            // caller.
            sink.clear();
            client.toSink("/exec", "SELECT 1", sink);
            TestUtils.assertEquals(
                    "{\"query\":\"SELECT 1\",\"columns\":[{\"name\":\"1\",\"type\":\"INT\"}],\"timestamp\":-1,\"dataset\":[[1]],\"count\":1}",
                    sink.toString()
            );
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

    // Counts the calls of the test function instances that the server compiles between install()
    // and the next install() or reset(). Worker threads call it, hence the atomic counters.
    private static class FunctionCalls implements TestLatchedCounterFunctionFactory.Callback {
        private final AtomicInteger closes = new AtomicInteger();
        private final AtomicInteger gets = new AtomicInteger();

        static FunctionCalls install() {
            final FunctionCalls calls = new FunctionCalls();
            TestLatchedCounterFunctionFactory.reset(calls);
            return calls;
        }

        @Override
        public void onClose() {
            closes.incrementAndGet();
        }

        @Override
        public boolean onGet(Record rec, int count) {
            gets.incrementAndGet();
            return true;
        }
    }
}
