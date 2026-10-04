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
 * Query-level identity for symbol-filtered scans served by a POSTING index when every
 * partition is split into many small page frames (one index cursor per frame). Each query on
 * the indexed table must return exactly what the same query returns on an unindexed copy of
 * the data: forward and backward, interval-clipped, aggregated, LATEST ON, covering (INCLUDE),
 * multi-key, a symbol absent from some partitions, an absent symbol, and an indexed column
 * added mid-partition (column top).
 */
public class PostingIndexFrameSeekQueryTest extends AbstractCairoTest {
    private static final String[] SYMBOLS = {"L", "S", "B", "H", "F3", "F17", "nope"};

    @Override
    @Before
    public void setUp() {
        // about 60 frames over 4 partitions, so a symbol's list is read as many frames
        setProperty(PropertyKey.CAIRO_SQL_PAGE_FRAME_MAX_ROWS, 1_000);
        setProperty(PropertyKey.CAIRO_SQL_PAGE_FRAME_MIN_ROWS, 100);
        super.setUp();
    }

    @Test
    public void testPostingAdaptive() throws Exception {
        assertIdentity("POSTING");
    }

    @Test
    public void testPostingDelta() throws Exception {
        assertIdentity("POSTING DELTA");
    }

    @Test
    public void testPostingDeltaCovering() throws Exception {
        assertIdentity("POSTING DELTA INCLUDE (px, qty, note)");
    }

    @Test
    public void testPostingEf() throws Exception {
        assertIdentity("POSTING EF");
    }

    @Test
    public void testPostingEfCovering() throws Exception {
        assertIdentity("POSTING EF INCLUDE (px, qty, note)");
    }

    @Test
    public void testPostingEfLegacyCovering() throws Exception {
        // EF blobs without the ranked trailer, as written before it existed: the cursors take
        // the unranked seek
        final boolean wasRanked = PostingIndexUtils.isEfRankTrailerEnabled;
        PostingIndexUtils.isEfRankTrailerEnabled = false;
        try {
            assertIdentity("POSTING EF INCLUDE (px, qty, note)");
        } finally {
            PostingIndexUtils.isEfRankTrailerEnabled = wasRanked;
        }
    }

    private static String rowsSql(long lo, long hi) {
        // Deterministic, non-periodic symbol layout:
        // B is bursty (runs of ~2,000 consecutive rows), L is ~25% of rows, S is rare,
        // H appears only in the first two partitions, Fn are filler keys.
        return "SELECT" +
                " CASE" +
                "   WHEN x % 20_000 BETWEEN 5_000 AND 7_000 THEN 'B'" +
                "   WHEN (x * 2_654_435_761) % 100 < 25 THEN 'L'" +
                "   WHEN (x * 2_654_435_761) % 100_003 < 60 THEN 'S'" +
                "   WHEN x < 30_000 AND (x * 40_503) % 13 = 1 THEN 'H'" +
                "   ELSE 'F' || ((x * 97) % 31)" +
                " END sym," +
                // dyadic prices keep sums exact whatever order the plan adds them in
                " (x % 1_000) / 8.0 px," +
                " (x % 77)::int qty," +
                " ('n' || (x % 11))::varchar note," +
                " ('2024-01-01'::timestamp + x * 5_760_000)::timestamp ts" +
                " FROM long_sequence(" + hi + ") WHERE x >= " + lo;
    }

    private void assertIdentity(String indexType) throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t_idx (sym SYMBOL INDEX TYPE " + indexType + ", px DOUBLE, qty INT, note VARCHAR, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("CREATE TABLE t_ref (sym SYMBOL, px DOUBLE, qty INT, note VARCHAR, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            // first 1.5 days, then an indexed column added mid-partition (column top), then the rest
            execute("INSERT INTO t_idx " + rowsSql(1, 22_500));
            execute("INSERT INTO t_ref " + rowsSql(1, 22_500));
            execute("ALTER TABLE t_idx ADD COLUMN s2 SYMBOL INDEX TYPE " + indexType.replaceAll(" INCLUDE.*", ""));
            execute("ALTER TABLE t_ref ADD COLUMN s2 SYMBOL");
            final String rest = "SELECT sym, px, qty, note, ts, CASE WHEN qty % 3 = 0 THEN null ELSE sym END s2 FROM (" + rowsSql(22_501, 60_000) + ")";
            execute("INSERT INTO t_idx " + rest);
            execute("INSERT INTO t_ref " + rest);

            final StringSink expected = new StringSink();
            // the scans under test really are served by the index
            assertPlan("SELECT * FROM t_idx WHERE sym = 'S'", "Index forward scan on: sym");
            assertPlan("SELECT * FROM t_idx WHERE sym = 'S' ORDER BY ts DESC", "Index backward scan on: sym");
            assertPlan("SELECT * FROM t_idx WHERE s2 = 'S'", "Index forward scan on: s2");
            assertPlan("SELECT ts, px, qty, note FROM t_idx WHERE sym = 'S'", indexType.contains("INCLUDE") ? "CoveringIndex on: sym" : "Index forward scan on: sym");
            assertPlan("SELECT * FROM t_idx WHERE sym = 'S' LATEST ON ts PARTITION BY sym", "Index backward scan on: sym");
            for (String s : SYMBOLS) {
                final String[] queries = {
                        "SELECT * FROM %t WHERE sym = '" + s + "'",
                        "SELECT * FROM %t WHERE sym = '" + s + "' ORDER BY ts DESC",
                        "SELECT * FROM %t WHERE sym = '" + s + "' AND ts BETWEEN '2024-01-01T13:00' AND '2024-01-03T07:30'",
                        "SELECT * FROM %t WHERE sym = '" + s + "' AND ts BETWEEN '2024-01-01T13:00' AND '2024-01-03T07:30' ORDER BY ts DESC",
                        "SELECT count(), sum(px), min(ts), max(ts), sum(qty) FROM %t WHERE sym = '" + s + "'",
                        "SELECT * FROM %t WHERE sym = '" + s + "' LATEST ON ts PARTITION BY sym",
                        "SELECT ts, px, qty, note FROM %t WHERE sym = '" + s + "'",
                        "SELECT ts, px FROM %t WHERE sym = '" + s + "' ORDER BY ts DESC LIMIT 7",
                        // covered columns read through cursors that start mid-partition, in both
                        // directions: the sidecar ordinal must follow the seek
                        "SELECT ts, px, qty, note FROM %t WHERE sym = '" + s + "' AND ts BETWEEN '2024-01-01T13:00' AND '2024-01-03T07:30'",
                        "SELECT ts, px, qty, note FROM %t WHERE sym = '" + s + "' AND ts BETWEEN '2024-01-01T13:00' AND '2024-01-03T07:30' ORDER BY ts DESC",
                        "SELECT ts, px, qty, note FROM %t WHERE sym = '" + s + "' AND ts BETWEEN '2024-01-02T01:00' AND '2024-01-02T01:30'",
                        "SELECT ts, px, note FROM %t WHERE sym = '" + s + "' AND ts < '2024-01-03T19:00' ORDER BY ts DESC LIMIT 5",
                        "SELECT ts, px, note FROM %t WHERE sym = '" + s + "' AND ts > '2024-01-02T03:00' LIMIT 5",
                        "SELECT ts, px, qty FROM %t WHERE sym = '" + s + "' AND ts < '2024-01-02T09:00' LATEST ON ts PARTITION BY sym",
                        "SELECT count(), sum(px), sum(qty), min(note), max(note) FROM %t WHERE sym = '" + s + "' AND ts BETWEEN '2024-01-01T13:00' AND '2024-01-03T07:30'",
                        "SELECT * FROM %t WHERE s2 = '" + s + "'",
                        "SELECT * FROM %t WHERE s2 = '" + s + "' ORDER BY ts DESC",
                        "SELECT * FROM %t WHERE sym IN ('" + s + "', 'S', 'H')",
                        "SELECT * FROM %t WHERE sym IN ('" + s + "', 'S', 'H') LATEST ON ts PARTITION BY sym",
                        "SELECT * FROM %t WHERE s2 = '" + s + "' LATEST ON ts PARTITION BY s2",
                        "SELECT * FROM %t WHERE sym = '" + s + "' AND ts < '2024-01-02T09:00' LATEST ON ts PARTITION BY sym",
                };
                for (String q : queries) {
                    assertSame(q, expected);
                }
            }
            assertSame("SELECT count(), sum(qty), min(ts), max(ts) FROM %t WHERE s2 IS NULL", expected);
            assertSame("SELECT * FROM %t WHERE s2 IS NULL ORDER BY ts DESC LIMIT 20", expected);
            assertSame("SELECT * FROM %t WHERE s2 IS NULL AND ts BETWEEN '2024-01-01T23:00' AND '2024-01-02T04:00'", expected);
        });
    }

    private void assertPlan(String query, String fragment) throws Exception {
        assertQuery(query).noLeakCheck().assertsPlanContaining(fragment);
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
