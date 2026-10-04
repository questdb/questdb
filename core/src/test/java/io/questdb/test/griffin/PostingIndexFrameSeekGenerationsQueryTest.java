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

package io.questdb.test.griffin;

import io.questdb.PropertyKey;
import io.questdb.cairo.idx.PostingIndexUtils;
import io.questdb.std.str.StringSink;
import io.questdb.test.AbstractCairoTest;
import org.junit.Before;
import org.junit.Test;

/**
 * Query-level identity of the posting-index frame seek when each partition holds many
 * generations (one sparse generation per small commit), after O3 commits into earlier
 * partitions, with fixed-size and var-size covered columns (DOUBLE, INT, VARCHAR, STRING), read
 * through ~1,000-row page frames. Covered values of cursors that start mid-partition depend on
 * the sidecar ordinal following the seek across a run of sparse generations, whose base comes
 * from the per-generation prefix sum. Each query must return what it returns on an unindexed
 * copy; ASOF and LT joins on the indexed table are checked too.
 */
public class PostingIndexFrameSeekGenerationsQueryTest extends AbstractCairoTest {
    private static final String[] SYMBOLS = {"L", "S", "B", "H", "F3", "nope"};

    @Override
    @Before
    public void setUp() {
        setProperty(PropertyKey.CAIRO_SQL_PAGE_FRAME_MAX_ROWS, 1_000);
        setProperty(PropertyKey.CAIRO_SQL_PAGE_FRAME_MIN_ROWS, 100);
        super.setUp();
    }

    @Test
    public void testManyGensAdaptiveCovering() throws Exception {
        assertIdentity("POSTING INCLUDE (px, qty, note, s)");
    }

    @Test
    public void testManyGensDeltaCovering() throws Exception {
        assertIdentity("POSTING DELTA INCLUDE (px, qty, note, s)");
    }

    @Test
    public void testManyGensEfCovering() throws Exception {
        assertIdentity("POSTING EF INCLUDE (px, qty, note, s)");
    }

    @Test
    public void testManyGensEfLegacyCovering() throws Exception {
        final boolean wasRanked = PostingIndexUtils.isEfRankTrailerEnabled;
        PostingIndexUtils.isEfRankTrailerEnabled = false;
        try {
            assertIdentity("POSTING EF INCLUDE (px, qty, note, s)");
        } finally {
            PostingIndexUtils.isEfRankTrailerEnabled = wasRanked;
        }
    }

    @Test
    public void testManyGensEfPlain() throws Exception {
        assertIdentity("POSTING EF");
    }

    private static String rowsSql(long lo, long hi, long tsOffsetMicros) {
        return "SELECT" +
                " CASE" +
                "   WHEN x % 20_000 BETWEEN 5_000 AND 7_000 THEN 'B'" +
                "   WHEN (x * 2_654_435_761) % 100 < 25 THEN 'L'" +
                "   WHEN (x * 2_654_435_761) % 100_003 < 60 THEN 'S'" +
                "   WHEN x < 30_000 AND (x * 40_503) % 13 = 1 THEN 'H'" +
                "   ELSE 'F' || ((x * 97) % 31)" +
                " END sym," +
                " (x % 1_000) / 8.0 px," +
                " (x % 77)::int qty," +
                " ('n' || (x % 11) || rpad('', (x % 23)::int, 'z'))::varchar note," +
                " ('s' || x)::string s," +
                " ('2024-01-01'::timestamp + x * 5_760_000 + " + tsOffsetMicros + ")::timestamp ts" +
                " FROM long_sequence(" + hi + ") WHERE x >= " + lo;
    }

    private void assertIdentity(String indexType) throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t_idx (sym SYMBOL INDEX TYPE " + indexType + ", px DOUBLE, qty INT, note VARCHAR, s STRING, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("CREATE TABLE t_ref (sym SYMBOL, px DOUBLE, qty INT, note VARCHAR, s STRING, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            // many small in-order commits: one generation per commit
            for (long lo = 1; lo <= 45_000; lo += 1_500) {
                final String sql = rowsSql(lo, lo + 1_499, 0);
                execute("INSERT INTO t_idx " + sql);
                execute("INSERT INTO t_ref " + sql);
            }
            // O3 commits into earlier partitions (odd offset so timestamps interleave)
            for (long lo = 2_001; lo <= 40_000; lo += 9_000) {
                final String sql = rowsSql(lo, lo + 700, 1);
                execute("INSERT INTO t_idx " + sql);
                execute("INSERT INTO t_ref " + sql);
            }
            for (long lo = 45_001; lo <= 60_000; lo += 700) {
                final String sql = rowsSql(lo, lo + 699, 0);
                execute("INSERT INTO t_idx " + sql);
                execute("INSERT INTO t_ref " + sql);
            }
            execute("CREATE TABLE trades (sym SYMBOL, tp DOUBLE, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("INSERT INTO trades SELECT rnd_symbol('L','S','B','H','F3','nope') sym, x::double tp, ('2024-01-01'::timestamp + x * 97_000_000)::timestamp ts FROM long_sequence(3_000)");

            final StringSink expected = new StringSink();
            final boolean covering = indexType.contains("INCLUDE");
            assertQuery("SELECT ts, px, qty, note, s FROM t_idx WHERE sym = 'S' AND ts BETWEEN '2024-01-01T13:07' AND '2024-01-03T07:31'").noLeakCheck()
                    .assertsPlanContaining(covering ? "CoveringIndex on: sym" : "Index forward scan on: sym");
            assertQuery("SELECT ts, px, qty, note, s FROM t_idx WHERE sym = 'S' AND ts BETWEEN '2024-01-01T13:07' AND '2024-01-03T07:31' ORDER BY ts DESC").noLeakCheck()
                    .assertsPlanContaining(covering ? "CoveringIndex on: sym" : "Index backward scan on: sym");
            assertQuery("SELECT /*+ asof_index(t q) */ t.ts, t.sym, t.tp, q.px, q.note, q.ts FROM trades t ASOF JOIN t_idx q ON t.sym = q.sym").noLeakCheck()
                    .assertsPlanContaining("Indexed");
            for (String sym : SYMBOLS) {
                final String[] queries = {
                        "SELECT * FROM %t WHERE sym = '" + sym + "'",
                        "SELECT * FROM %t WHERE sym = '" + sym + "' ORDER BY ts DESC",
                        "SELECT ts, px, qty, note, s FROM %t WHERE sym = '" + sym + "'",
                        "SELECT ts, px, qty, note, s FROM %t WHERE sym = '" + sym + "' ORDER BY ts DESC",
                        "SELECT ts, px, qty, note, s FROM %t WHERE sym = '" + sym + "' AND ts BETWEEN '2024-01-01T13:07' AND '2024-01-03T07:31'",
                        "SELECT ts, px, qty, note, s FROM %t WHERE sym = '" + sym + "' AND ts BETWEEN '2024-01-01T13:07' AND '2024-01-03T07:31' ORDER BY ts DESC",
                        "SELECT ts, note, s FROM %t WHERE sym = '" + sym + "' AND ts BETWEEN '2024-01-02T01:00' AND '2024-01-02T01:03'",
                        "SELECT ts, note, s FROM %t WHERE sym = '" + sym + "' AND ts BETWEEN '2024-01-02T01:00' AND '2024-01-02T01:03' ORDER BY ts DESC",
                        "SELECT ts, px, note FROM %t WHERE sym = '" + sym + "' AND ts > '2024-01-02T03:00' LIMIT 9",
                        "SELECT ts, px, note FROM %t WHERE sym = '" + sym + "' AND ts < '2024-01-03T19:00' ORDER BY ts DESC LIMIT 9",
                        "SELECT ts, px, qty, note, s FROM %t WHERE sym = '" + sym + "' AND ts < '2024-01-02T09:00' LATEST ON ts PARTITION BY sym",
                        "SELECT count(), sum(px), sum(qty), min(note), max(note), min(s), max(s) FROM %t WHERE sym = '" + sym + "' AND ts BETWEEN '2024-01-01T13:07' AND '2024-01-03T07:31'",
                        "SELECT * FROM %t WHERE sym IN ('" + sym + "', 'S', 'H') AND ts BETWEEN '2024-01-01T13:07' AND '2024-01-03T07:31'",
                        "SELECT * FROM (SELECT * FROM %t WHERE sym IN ('" + sym + "', 'S', 'H') AND ts < '2024-01-03' LATEST ON ts PARTITION BY sym) ORDER BY ts",
                };
                for (String q : queries) {
                    assertSame(q, expected);
                }
            }
            // ASOF indexed: right side opens backward cursors with per-row upper bounds
            expected.clear();
            printSql("SELECT t.ts, t.sym, t.tp, q.px, q.note, q.ts FROM trades t ASOF JOIN t_ref q ON t.sym = q.sym", expected);
            final String asof = "SELECT /*+ asof_index(t q) */ t.ts, t.sym, t.tp, q.px, q.note, q.ts FROM trades t ASOF JOIN t_idx q ON t.sym = q.sym";
            assertQuery(asof).noLeakCheck().inferTimestamp().inferRandomAccess().sizeMayVary().returns(expected.toString());
            expected.clear();
            printSql("SELECT t.ts, t.sym, t.tp, q.px, q.note, q.ts FROM trades t LT JOIN t_ref q ON t.sym = q.sym", expected);
            assertQuery("SELECT t.ts, t.sym, t.tp, q.px, q.note, q.ts FROM trades t LT JOIN t_idx q ON t.sym = q.sym").noLeakCheck().inferTimestamp().inferRandomAccess().sizeMayVary().returns(expected.toString());
        });
    }

    private void assertSame(String template, StringSink expected) throws Exception {
        expected.clear();
        printSql(template.replace("%t", "t_ref"), expected);
        assertQuery(template.replace("%t", "t_idx"))
                .noLeakCheck()
                .inferTimestamp()
                .inferRandomAccess()
                .sizeMayVary()
                .returns(expected.toString());
    }
}
