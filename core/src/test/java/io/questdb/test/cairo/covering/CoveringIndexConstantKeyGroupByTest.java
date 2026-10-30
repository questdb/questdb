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

package io.questdb.test.cairo.covering;

import io.questdb.griffin.engine.table.AsyncGroupByRecordCursorFactory;
import io.questdb.std.str.StringSink;
import org.junit.Assert;
import org.junit.Test;

/**
 * A GROUP BY over per-key covered frames, differentially against the {@code no_index} scan:
 * <ul>
 *     <li>grouped by the symbol key alone, a single-key frame is one group, aggregated through
 *     the non-keyed batch kernels after one probe of the map;</li>
 *     <li>grouped by the key and a time bucket, consecutive rows repeat the last key, which the
 *     map's batch key cache checks before hashing;</li>
 *     <li>with a var-size covered column the decode takes its rows one at a time, otherwise it
 *     passes the posting cursor's runs at once.</li>
 * </ul>
 * NULLs and many partitions (each key's rows split into frames) are in the data. The double
 * aggregates are compared to a relative 1e-9: the two plans add a group's rows in different
 * orders.
 */
public class CoveringIndexConstantKeyGroupByTest extends AbstractCoveringIndexQueryTest {

    private static final String KEYS = "('k0', 'k1', 'k3', 'k5', 'k6')";

    @Test
    public void testGroupByKeyAgreesWithScan() throws Exception {
        assertMemoryLeak(() -> {
            createQuotes();
            final String[] queries = {
                    "select sym, weighted_avg(bid, bsize) wb, weighted_avg(ask, asize) wa from q where sym in " + KEYS + " order by sym",
                    "select sym, sum((asize * ask)::double) s1, sum((bid * bsize)::double) s2, stddev_samp(asize) sd1, stddev_samp(bid) sd2,"
                            + " corr(ask, bid) c1, corr(asize, bsize) c2 from q where sym in " + KEYS + " order by sym",
                    "select sym, count() c, min(bid) mn, max(ask) mx, avg(bsize) ab, sum(asize) sa from q where sym in " + KEYS + " order by sym",
                    // the raw kernel arguments' other type pairs, and expression arguments
                    "select sym, weighted_avg(bsize, bid) w1, weighted_avg(bid * 2.0, asize + 1) w2, stddev_samp(bsize::long) s1,"
                            + " stddev_samp(ask * 1.0) s2, corr(bid, bsize) c1, corr(ask * 1.0, ask * 1.0) c2 from q where sym in " + KEYS + " order by sym",
                    // a single key, and a key with one row
                    "select sym, count() c, avg(bid) a from q where sym = 'k3' order by sym",
                    "select sym, count() c, avg(bid) a, sum(bsize) s from q where sym in ('k1', 'lone') order by sym",
            };
            for (String q : queries) {
                assertCoveringAgreesWithScan(q);
            }
        });
    }

    @Test
    public void testGroupByKeyAndBucketAgreesWithScan() throws Exception {
        assertMemoryLeak(() -> {
            createQuotes();
            assertCoveringAgreesWithScan(
                    "select sym, timestamp_floor('10m', ts) m, avg(ask - bid) sp, count() c from q where sym in " + KEYS + " order by sym, m",
                    false
            );
            assertCoveringAgreesWithScan(
                    "select timestamp_floor('1h', ts) h, sym, sum(bsize) s from q where sym in " + KEYS + " order by h, sym",
                    false
            );
        });
    }

    @Test
    public void testGroupByKeyWithVarSizeColumnAgreesWithScan() throws Exception {
        assertMemoryLeak(() -> {
            createQuotes();
            assertCoveringAgreesWithScan(
                    "select sym, count(cond) cc, sum(bsize) s, avg(bid) a from q where sym in " + KEYS + " order by sym"
            );
        });
    }

    private static void assertNumericallyEqual(String expected, String actual) {
        final String[] e = expected.split("\n");
        final String[] a = actual.split("\n");
        Assert.assertEquals("row count\n" + expected + "\nvs\n" + actual, e.length, a.length);
        for (int i = 0; i < e.length; i++) {
            final String[] ef = e[i].split("\t", -1);
            final String[] af = a[i].split("\t", -1);
            Assert.assertEquals("column count at row " + i, ef.length, af.length);
            for (int j = 0; j < ef.length; j++) {
                if (ef[j].equals(af[j])) {
                    continue;
                }
                final double ev;
                final double av;
                try {
                    ev = Double.parseDouble(ef[j]);
                    av = Double.parseDouble(af[j]);
                } catch (NumberFormatException ex) {
                    Assert.fail("row " + i + " col " + j + ": " + ef[j] + " vs " + af[j]);
                    return;
                }
                Assert.assertEquals("row " + i + " col " + j, ev, av, Math.max(1e-12, Math.abs(ev) * 1e-9));
            }
        }
    }

    private void assertCoveringAgreesWithScan(String sql) throws Exception {
        assertCoveringAgreesWithScan(sql, true);
    }

    // isOneGroupPerFrame: whether the covering arm must aggregate its frames as one group each,
    // see AsyncGroupByRecordCursorFactory.aggregateConstantKeyNonSharded(); checked by the frames
    // that path counted, so that a fixture or a gate that stops reaching it fails here
    private void assertCoveringAgreesWithScan(String sql, boolean isOneGroupPerFrame) throws Exception {
        final String covering = sql.replace("select ", "select /*+ use_index */ ");
        final String scan = sql.replace("select ", "select /*+ no_index */ ");
        assertPlanContains(covering, "CoveringIndex");
        assertPlanContains(covering, "Async Group By");
        final StringSink coveringResult = new StringSink();
        final StringSink scanResult = new StringSink();
        AsyncGroupByRecordCursorFactory.setConstantKeyFrameCountingForTesting(true);
        try {
            printSql(covering, coveringResult);
            final long frames = AsyncGroupByRecordCursorFactory.getConstantKeyFramesForTesting();
            if (isOneGroupPerFrame) {
                Assert.assertTrue("the one-group path did not run: " + covering, frames > 0);
            } else {
                Assert.assertEquals("the one-group path ran: " + covering, 0, frames);
            }
        } finally {
            AsyncGroupByRecordCursorFactory.setConstantKeyFrameCountingForTesting(false);
        }
        printSql(scan, scanResult);
        Assert.assertTrue("no rows: " + sql, scanResult.toString().split("\n").length > 1);
        assertNumericallyEqual(scanResult.toString(), coveringResult.toString());
    }

    private void assertPlanContains(String sql, String fragment) throws Exception {
        final StringSink plan = new StringSink();
        printSql("explain " + sql, plan);
        Assert.assertTrue("plan lacks " + fragment + ":\n" + plan, plan.toString().contains(fragment));
    }

    private void createQuotes() throws Exception {
        execute("CREATE TABLE q (" +
                "  ts TIMESTAMP," +
                "  sym SYMBOL INDEX TYPE POSTING INCLUDE (ts, bid, ask, bsize, asize, cond)," +
                "  bid FLOAT, ask FLOAT, bsize INT, asize INT, cond VARCHAR" +
                ") TIMESTAMP(ts) PARTITION BY HOUR BYPASS WAL");
        // 8 keys over ~22 hourly partitions; NULLs in every covered column
        execute("INSERT INTO q SELECT (x * 400_000L)::timestamp," +
                " 'k' || (x % 8)," +
                " CASE WHEN x % 97 = 0 THEN NULL ELSE ((x % 1000) / 8.0)::float END," +
                " CASE WHEN x % 89 = 0 THEN NULL ELSE ((x % 1000) / 8.0 + 0.125)::float END," +
                " CASE WHEN x % 83 = 0 THEN NULL ELSE (x % 50)::int END," +
                " CASE WHEN x % 79 = 0 THEN NULL ELSE (x % 70)::int END," +
                " CASE WHEN x % 13 = 0 THEN NULL ELSE 'c' || (x % 5) END" +
                " FROM long_sequence(200_000)");
        execute("INSERT INTO q VALUES ('1970-01-01T05:00:00.000001Z', 'lone', 1.5, 2.5, 3, 4, 'z')");
    }
}
