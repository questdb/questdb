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

package io.questdb.test.griffin.engine.window;

import io.questdb.PropertyKey;
import io.questdb.std.str.StringSink;
import io.questdb.test.AbstractCairoTest;
import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;

/**
 * Per-shape output-equivalence tests covering every query shape exercised by
 * {@code WindowLeadLagBenchmark}. Each test runs the same SQL once with the streaming-lead flag
 * disabled (cached path = master behaviour) and once enabled (Phase 6.1 dispatch). Outputs are
 * compared as sorted multisets to tolerate the partition-major emission contract of
 * {@code DeferredEmitWindowRecordCursorFactory}: rows must be identical, but ordering across
 * partitions may differ. Catches result drift between paths even when the planner's routing
 * decision changes between phases.
 */
public class StreamingLeadCachedEquivalenceTest extends AbstractCairoTest {

    private static final String CREATE_T = "create table t (x long, sym symbol, ts timestamp) timestamp(ts) partition by day";
    private static final String SEED_T =
            "insert into t " +
                    "select x, " +
                    "       case when x % 3 = 0 then 'A' when x % 3 = 1 then 'B' else 'C' end, " +
                    "       (1_000_000L * x)::timestamp " +
                    "from long_sequence(60)";

    @Test
    public void testQ1MixedNoOrder() throws Exception {
        assertEquivalent(
                "select x, ts, lag(x, 1) over () as l, lead(x, 1) over () as ld from t"
        );
    }

    @Test
    public void testQ10DualLag() throws Exception {
        assertEquivalent(
                "select x, ts, " +
                        "lag(x, 1) over (order by ts asc) as l1, " +
                        "lag(x, 3) over (order by ts asc) as l3 " +
                        "from t"
        );
    }

    @Test
    public void testQ11MixedDescOuterDesc() throws Exception {
        assertEquivalent(
                "select x, ts, " +
                        "lag(x, 1) over (order by ts desc) as l, " +
                        "lead(x, 1) over (order by ts desc) as ld " +
                        "from t order by ts desc"
        );
    }

    @Test
    public void testQ2MixedAsc() throws Exception {
        assertEquivalent(
                "select x, ts, " +
                        "lag(x, 1) over (order by ts asc) as l, " +
                        "lead(x, 1) over (order by ts asc) as ld " +
                        "from t"
        );
    }

    @Test
    public void testQ3MixedDesc() throws Exception {
        assertEquivalent(
                "select x, ts, " +
                        "lag(x, 1) over (order by ts desc) as l, " +
                        "lead(x, 1) over (order by ts desc) as ld " +
                        "from t"
        );
    }

    @Test
    public void testQ4MixedPartitionNoOrder() throws Exception {
        assertEquivalent(
                "select x, ts, sym, " +
                        "lag(x, 1) over (partition by sym) as l, " +
                        "lead(x, 1) over (partition by sym) as ld " +
                        "from t"
        );
    }

    @Test
    public void testQ5MixedPartitionAsc() throws Exception {
        assertEquivalent(
                "select x, ts, sym, " +
                        "lag(x, 1) over (partition by sym order by ts asc) as l, " +
                        "lead(x, 1) over (partition by sym order by ts asc) as ld " +
                        "from t"
        );
    }

    @Test
    public void testQ6MixedPartitionDesc() throws Exception {
        assertEquivalent(
                "select x, ts, sym, " +
                        "lag(x, 1) over (partition by sym order by ts desc) as l, " +
                        "lead(x, 1) over (partition by sym order by ts desc) as ld " +
                        "from t"
        );
    }

    @Test
    public void testQ7MixedInverseNoPartition() throws Exception {
        assertEquivalent(
                "select x, ts, " +
                        "lag(x, 1) over (order by ts desc) as l, " +
                        "lead(x, 1) over (order by ts asc) as ld " +
                        "from t"
        );
    }

    @Test
    public void testQ8MixedInversePartition() throws Exception {
        assertEquivalent(
                "select x, ts, sym, " +
                        "lag(x, 1) over (partition by sym order by ts desc) as l, " +
                        "lead(x, 1) over (partition by sym order by ts asc) as ld " +
                        "from t"
        );
    }

    @Test
    public void testQ9DualLead() throws Exception {
        assertEquivalent(
                "select x, ts, " +
                        "lead(x, 1) over (order by ts asc) as l1, " +
                        "lead(x, 3) over (order by ts asc) as l3 " +
                        "from t"
        );
    }

    @Test
    public void testS1LeadNoPartition() throws Exception {
        assertEquivalent("select x, ts, lead(x, 1) over () as lx from t");
    }

    @Test
    public void testS2LagDescNoPartition() throws Exception {
        assertEquivalent("select x, ts, lag(x, 1) over (order by ts desc) as lx from t");
    }

    @Test
    public void testS3LeadPartitioned() throws Exception {
        assertEquivalent("select x, ts, sym, lead(x, 1) over (partition by sym) as lx from t");
    }

    @Test
    public void testS4LagDescPartitioned() throws Exception {
        assertEquivalent(
                "select x, ts, sym, lag(x, 1) over (partition by sym order by ts desc) as lx from t"
        );
    }

    /**
     * Runs the query under the cached path (flag off) and the streaming path (flag on), captures
     * both outputs, sorts the data rows, and asserts equality. The header line (column names) is
     * compared verbatim before sorting.
     */
    private void assertEquivalent(String querySql) throws Exception {
        assertMemoryLeak(() -> {
            execute(CREATE_T);
            execute(SEED_T);

            StringSink cachedSink = new StringSink();
            setProperty(PropertyKey.CAIRO_SQL_WINDOW_STREAMING_LEAD_ENABLED, "false");
            printSql(querySql, cachedSink);

            StringSink streamingSink = new StringSink();
            setProperty(PropertyKey.CAIRO_SQL_WINDOW_STREAMING_LEAD_ENABLED, "true");
            printSql(querySql, streamingSink);

            String[] cachedLines = splitLines(cachedSink.toString());
            String[] streamingLines = splitLines(streamingSink.toString());

            Assert.assertEquals(
                    "row count differs between cached and streaming for query: " + querySql,
                    cachedLines.length,
                    streamingLines.length
            );
            // Header must match verbatim (column names and order).
            Assert.assertEquals(
                    "header differs for query: " + querySql,
                    cachedLines[0],
                    streamingLines[0]
            );
            // Sort data rows: streaming may emit partition-major while cached emits scan order;
            // both must contain the same multiset of rows.
            String[] cachedData = Arrays.copyOfRange(cachedLines, 1, cachedLines.length);
            String[] streamingData = Arrays.copyOfRange(streamingLines, 1, streamingLines.length);
            Arrays.sort(cachedData);
            Arrays.sort(streamingData);
            Assert.assertEquals(
                    "data rows differ between cached and streaming for query: " + querySql,
                    String.join("\n", cachedData),
                    String.join("\n", streamingData)
            );
        });
    }

    private static String[] splitLines(String s) {
        if (s.endsWith("\n")) {
            s = s.substring(0, s.length() - 1);
        }
        return s.split("\n", -1);
    }
}
