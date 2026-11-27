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
import io.questdb.std.Chars;
import io.questdb.std.str.StringSink;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

/**
 * Runs operators that scan an OrderedMap while they keep adding to it, with map pages small
 * enough that the map grows mid-query and moves its memory. Each test sweeps the page size so
 * that some page leaves the map exactly full at the point where the operator starts reading from
 * it, which is when a cursor or address taken earlier goes stale.
 */
public class MapGrowthDuringScanTest extends AbstractCairoTest {

    @Test
    public void testFullJoin() throws Exception {
        testOuterJoin("FULL", false, false, """
                lv\trv
                null\t30
                10\t15
                20\t5
                40\tnull
                """);
    }

    @Test
    public void testFullJoinFiltered() throws Exception {
        testOuterJoin("FULL", true, false, """
                lv\trv
                null\t5
                null\t30
                10\t15
                20\tnull
                40\tnull
                """);
    }

    @Test
    public void testFullJoinFilteredFullFat() throws Exception {
        testOuterJoin("FULL", true, true, """
                lv\trv
                null\t5
                null\t30
                10\t15
                20\tnull
                40\tnull
                """);
    }

    @Test
    public void testFullJoinFullFat() throws Exception {
        testOuterJoin("FULL", false, true, """
                lv\trv
                null\t30
                10\t15
                20\t5
                40\tnull
                """);
    }

    @Test
    public void testRightJoin() throws Exception {
        testOuterJoin("RIGHT", false, false, """
                lv\trv
                null\t30
                10\t15
                20\t5
                """);
    }

    @Test
    public void testRightJoinFiltered() throws Exception {
        testOuterJoin("RIGHT", true, false, """
                lv\trv
                null\t5
                null\t30
                10\t15
                """);
    }

    @Test
    public void testRightJoinFilteredFullFat() throws Exception {
        testOuterJoin("RIGHT", true, true, """
                lv\trv
                null\t5
                null\t30
                10\t15
                """);
    }

    @Test
    public void testRightJoinFullFat() throws Exception {
        testOuterJoin("RIGHT", false, true, """
                lv\trv
                null\t30
                10\t15
                20\t5
                """);
    }

    @Test
    public void testSampleByFillKeyed() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (ts TIMESTAMP, k LONG) TIMESTAMP(ts) PARTITION BY DAY");
            // The base cursor yields the keys in the order 1, 2, 1, 3, so the first pass ends on
            // a new key and leaves the map exactly full on the page that fits three entries.
            execute("""
                    INSERT INTO t VALUES
                        ('2024-01-01T00:00:00', 1),
                        ('2024-01-01T00:10:00', 2),
                        ('2024-01-01T02:00:00', 1),
                        ('2024-01-01T03:00:00', 3)
                    """);
            final String expected = """
                    k\tc\tts
                    1\t1\t2024-01-01T00:00:00.000000Z
                    2\t1\t2024-01-01T00:00:00.000000Z
                    3\t0\t2024-01-01T00:00:00.000000Z
                    1\t0\t2024-01-01T01:00:00.000000Z
                    2\t0\t2024-01-01T01:00:00.000000Z
                    3\t0\t2024-01-01T01:00:00.000000Z
                    1\t1\t2024-01-01T02:00:00.000000Z
                    2\t0\t2024-01-01T02:00:00.000000Z
                    3\t0\t2024-01-01T02:00:00.000000Z
                    1\t0\t2024-01-01T03:00:00.000000Z
                    2\t0\t2024-01-01T03:00:00.000000Z
                    3\t1\t2024-01-01T03:00:00.000000Z
                    """;
            for (int pageSize = 40; pageSize <= 200; pageSize += 4) {
                setProperty(PropertyKey.CAIRO_SQL_SMALL_MAP_PAGE_SIZE, pageSize);
                assertQuery("SELECT * FROM (SELECT k, count() c, ts FROM t SAMPLE BY 1h FILL(0) ALIGN TO CALENDAR) ORDER BY ts, k")
                        .timestamp("ts")
                        .withPlanContaining("Sample By Fill")
                        .returns(expected);
            }
        });
    }

    @Test
    public void testSpliceJoin() throws Exception {
        testSpliceJoin("id");
    }

    @Test
    public void testSpliceJoinCompositeKey() throws Exception {
        testSpliceJoin("k, id");
    }

    // Both sides hold three distinct keys, so the build ends exactly full on the page that fits
    // three entries, and the probe's first lookup grows the map before the scan over the unmatched
    // build rows starts.
    private void testOuterJoin(String joinKind, boolean isFiltered, boolean isFullFat, String expected) throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE l (k1 LONG, k2 LONG, v INT)");
            execute("CREATE TABLE r (k1 LONG, k2 LONG, v INT)");
            execute("INSERT INTO l VALUES (1, 1, 10), (2, 2, 20), (4, 4, 40)");
            execute("INSERT INTO r VALUES (1, 1, 15), (2, 2, 5), (3, 3, 30)");
            final String sql = "SELECT l.v lv, r.v rv FROM l " + joinKind + " JOIN r ON l.k1 = r.k1 AND l.k2 = r.k2"
                    + (isFiltered ? " AND l.v < r.v" : "")
                    + " ORDER BY lv, rv";
            printSql("EXPLAIN " + sql, isFullFat);
            TestUtils.assertContains(sink, "Hash " + (joinKind.equals("FULL") ? "Full" : "Right") + " Outer Join" + (isFullFat ? "\n" : " Light\n"));
            Assert.assertEquals(isFiltered, Chars.contains(sink, "filter: l.v<r.v"));
            for (int pageSize = 48; pageSize <= 256; pageSize += 8) {
                setProperty(PropertyKey.CAIRO_SQL_SMALL_MAP_PAGE_SIZE, pageSize);
                assertQuery(sql)
                        .fullFatJoins(isFullFat)
                        .returns(expected);
            }
        });
    }

    // Every master row has a slave row at the same timestamp, and every other one shares its key,
    // so each step adds a new key to the map between caching a value and reading it.
    private void testSpliceJoin(String keys) throws Exception {
        final int rows = 1_024;
        assertMemoryLeak(() -> {
            execute("CREATE TABLE a AS (SELECT 2 * x AS id, x AS k, x::TIMESTAMP ts FROM long_sequence(" + rows + ")) TIMESTAMP(ts)");
            execute("CREATE TABLE b AS (SELECT 2 * x + x % 2 AS id, x AS k, x::TIMESTAMP ts FROM long_sequence(" + rows + ")) TIMESTAMP(ts)");
            final StringSink expected = new StringSink();
            expected.put("aid\tbid\n");
            for (int i = 1; i <= rows; i++) {
                expected.put(2 * i).put('\t');
                if (i % 2 == 0) {
                    expected.put(2 * i);
                } else {
                    expected.put("null");
                }
                expected.put('\n');
            }
            for (int pageSize = 64; pageSize <= 256; pageSize += 64) {
                setProperty(PropertyKey.CAIRO_SQL_SMALL_MAP_PAGE_SIZE, pageSize);
                assertQuery("SELECT a.id AS aid, b.id AS bid FROM a SPLICE JOIN b ON (" + keys + ") WHERE a.id IS NOT NULL")
                        .noRandomAccess()
                        .sizeMayVary()
                        .withPlanContaining("Splice Join")
                        .returns(expected);
            }
        });
    }
}
