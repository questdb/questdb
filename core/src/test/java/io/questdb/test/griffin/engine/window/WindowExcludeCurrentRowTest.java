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

import io.questdb.test.AbstractCairoTest;
import org.junit.Test;

/**
 * QuestDB implements {@code EXCLUDE CURRENT ROW} by rewriting a raw {@code CURRENT ROW} high
 * bound from {@code 0} to {@code -1}, in whatever unit the framing mode counts in
 * ({@code WindowContextImpl.getRowsHi()}). For ROWS that unit is one row, so the rewrite drops
 * exactly the current physical row and matches PostgreSQL. For RANGE it is one tick of the
 * designated timestamp, so the rewrite drops every row that shares the current row's timestamp,
 * not only the current row.
 * <p>
 * That makes QuestDB's RANGE {@code ... AND CURRENT ROW EXCLUDE CURRENT ROW} compute what
 * PostgreSQL calls {@code EXCLUDE GROUP}, and it compounds a second deviation these tests pin
 * beside it: an unexcluded RANGE frame ending at {@code CURRENT ROW} stops at the current
 * physical row rather than running through the last peer, so a tie group sees a frame that grows
 * row by row. PostgreSQL defines both bounds through the peer group and keeps
 * {@code EXCLUDE CURRENT ROW} and {@code EXCLUDE GROUP} distinct.
 * <p>
 * Neither deviation is fixed here - they are tracked as a separate SQL compatibility decision.
 * These tests exist so the behaviour is written down at the timestamps where the two framing
 * modes disagree, and so a later peer-semantics correction has to restate them deliberately
 * rather than silently. A live view reads the RANGE shape as a repair bound, so the same
 * correction has to widen that bound with it.
 */
public class WindowExcludeCurrentRowTest extends AbstractCairoTest {

    @Test
    public void testExcludeCurrentRowIsANoOpWhenTheFrameEndsBeforeTheCurrentRow() throws Exception {
        assertMemoryLeak(() -> {
            createTiedTable();

            // The rewrite only fires on a raw CURRENT ROW high bound, so a frame that already
            // ends at N PRECEDING is unaffected in both framing modes - including the RANGE
            // frame, whose high bound sits a whole second below the tie group either way.
            assertQuery("""
                    SELECT x,
                      sum(x) OVER (ORDER BY ts ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING) AS rows_plain,
                      sum(x) OVER (ORDER BY ts ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING EXCLUDE CURRENT ROW) AS rows_excluded,
                      sum(x) OVER (ORDER BY ts RANGE BETWEEN 2 SECOND PRECEDING AND 1 SECOND PRECEDING) AS range_plain,
                      sum(x) OVER (ORDER BY ts RANGE BETWEEN 2 SECOND PRECEDING AND 1 SECOND PRECEDING EXCLUDE CURRENT ROW) AS range_excluded
                    FROM tab""")
                    .noRandomAccess()
                    .expectSize()
                    .returns("""
                            x\trows_plain\trows_excluded\trange_plain\trange_excluded
                            1\tnull\tnull\tnull\tnull
                            2\t1.0\t1.0\t1.0\t1.0
                            3\t3.0\t3.0\t1.0\t1.0
                            4\t5.0\t5.0\t1.0\t1.0
                            5\t7.0\t7.0\t10.0\t10.0
                            """);
        });
    }

    @Test
    public void testPartitionedExcludeCurrentRowOverTies() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE tab (ts TIMESTAMP, sym SYMBOL, x LONG) TIMESTAMP(ts) PARTITION BY DAY");
            // Partition 'a' carries a two-row tie at 00:00:01, partition 'b' a single row there.
            execute("""
                    INSERT INTO tab VALUES
                      ('2024-01-01T00:00:00.000000Z', 'a', 1),
                      ('2024-01-01T00:00:00.000000Z', 'b', 10),
                      ('2024-01-01T00:00:01.000000Z', 'a', 2),
                      ('2024-01-01T00:00:01.000000Z', 'b', 20),
                      ('2024-01-01T00:00:01.000000Z', 'a', 3),
                      ('2024-01-01T00:00:02.000000Z', 'a', 4),
                      ('2024-01-01T00:00:02.000000Z', 'b', 30)""");

            // The tie is what separates the two framing modes, and it separates them per
            // partition: 'a' row 3 sees only row 1 under RANGE but rows 1 and 2 under ROWS,
            // while 'b', which never ties within its own partition, agrees with itself.
            assertQuery("""
                    SELECT ts, sym, x,
                      sum(x) OVER rows_w AS rows_sum,
                      count(*) OVER rows_w AS rows_count,
                      sum(x) OVER range_w AS range_sum,
                      count(*) OVER range_w AS range_count
                    FROM tab
                    WINDOW rows_w AS (PARTITION BY sym ORDER BY ts ROWS BETWEEN 2 PRECEDING AND CURRENT ROW EXCLUDE CURRENT ROW),
                           range_w AS (PARTITION BY sym ORDER BY ts RANGE BETWEEN 2 SECOND PRECEDING AND CURRENT ROW EXCLUDE CURRENT ROW)""")
                    .noRandomAccess()
                    .expectSize()
                    .timestamp("ts")
                    .returns("""
                            ts\tsym\tx\trows_sum\trows_count\trange_sum\trange_count
                            2024-01-01T00:00:00.000000Z\ta\t1\tnull\t0\tnull\t0
                            2024-01-01T00:00:00.000000Z\tb\t10\tnull\t0\tnull\t0
                            2024-01-01T00:00:01.000000Z\ta\t2\t1.0\t1\t1.0\t1
                            2024-01-01T00:00:01.000000Z\tb\t20\t10.0\t1\t10.0\t1
                            2024-01-01T00:00:01.000000Z\ta\t3\t3.0\t2\t1.0\t1
                            2024-01-01T00:00:02.000000Z\ta\t4\t5.0\t2\t6.0\t3
                            2024-01-01T00:00:02.000000Z\tb\t30\t30.0\t2\t30.0\t2
                            """);
        });
    }

    @Test
    public void testRangeAndRowsAgreeWhenNoDesignatedTimestampsTie() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE tab (ts TIMESTAMP, x LONG) TIMESTAMP(ts) PARTITION BY DAY");
            execute("""
                    INSERT INTO tab VALUES
                      ('2024-01-01T00:00:00.000000Z', 1),
                      ('2024-01-01T00:00:01.000000Z', 2),
                      ('2024-01-01T00:00:02.000000Z', 3),
                      ('2024-01-01T00:00:03.000000Z', 4)""");

            // One row per second, so a two-row ROWS look-behind and a two-second RANGE
            // look-behind cover the same rows. Both modes exclude one row, and the tie group is
            // the only thing that can make them disagree.
            assertQuery("""
                    SELECT x,
                      sum(x) OVER rows_w AS rows_sum,
                      count(*) OVER rows_w AS rows_count,
                      sum(x) OVER range_w AS range_sum,
                      count(*) OVER range_w AS range_count
                    FROM tab
                    WINDOW rows_w AS (ORDER BY ts ROWS BETWEEN 2 PRECEDING AND CURRENT ROW EXCLUDE CURRENT ROW),
                           range_w AS (ORDER BY ts RANGE BETWEEN 2 SECOND PRECEDING AND CURRENT ROW EXCLUDE CURRENT ROW)""")
                    .noRandomAccess()
                    .expectSize()
                    .returns("""
                            x\trows_sum\trows_count\trange_sum\trange_count
                            1\tnull\t0\tnull\t0
                            2\t1.0\t1\t1.0\t1
                            3\t3.0\t2\t3.0\t2
                            4\t5.0\t2\t5.0\t2
                            """);
        });
    }

    @Test
    public void testRangeExcludeCurrentRowDropsTheWholeTieGroup() throws Exception {
        assertMemoryLeak(() -> {
            createTiedTable();

            // The high bound is one microsecond below the current row's timestamp, so rows 2, 3
            // and 4 - all at 00:00:01 - each see only row 1, and none of them sees a sibling.
            // PostgreSQL's EXCLUDE CURRENT ROW would leave the siblings in the frame and give
            // row 2 the sum 1 + 3 + 4 = 8; what QuestDB computes here is PostgreSQL's
            // EXCLUDE GROUP.
            assertQuery("""
                    SELECT ts, x,
                      sum(x) OVER w AS s,
                      count(*) OVER w AS c,
                      first_value(x) OVER w AS fv,
                      last_value(x) OVER w AS lv
                    FROM tab
                    WINDOW w AS (ORDER BY ts RANGE BETWEEN 2 SECOND PRECEDING AND CURRENT ROW EXCLUDE CURRENT ROW)""")
                    .noRandomAccess()
                    .expectSize()
                    .timestamp("ts")
                    .withPlanContaining("range between 2000000 preceding and 1 preceding")
                    .returns("""
                            ts\tx\ts\tc\tfv\tlv
                            2024-01-01T00:00:00.000000Z\t1\tnull\t0\tnull\tnull
                            2024-01-01T00:00:01.000000Z\t2\t1.0\t1\t1\t1
                            2024-01-01T00:00:01.000000Z\t3\t1.0\t1\t1\t1
                            2024-01-01T00:00:01.000000Z\t4\t1.0\t1\t1\t1
                            2024-01-01T00:00:02.000000Z\t5\t10.0\t4\t1\t4
                            """);
        });
    }

    @Test
    public void testRangeExcludeCurrentRowMovesTheHighBoundByOneNativeTick() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE tab_us (ts TIMESTAMP, x LONG) TIMESTAMP(ts) PARTITION BY DAY");
            execute("INSERT INTO tab_us VALUES (1000::TIMESTAMP, 1), (1001::TIMESTAMP, 2), (1001::TIMESTAMP, 3), (1002::TIMESTAMP, 4)");
            execute("CREATE TABLE tab_ns (ts TIMESTAMP_NS, x LONG) TIMESTAMP(ts) PARTITION BY DAY");
            execute("INSERT INTO tab_ns VALUES (1000::TIMESTAMP_NS, 1), (1001::TIMESTAMP_NS, 2), (1001::TIMESTAMP_NS, 3), (1002::TIMESTAMP_NS, 4)");

            // The written width converts into the designated timestamp's units, and here one
            // microsecond is one tick, so the frame collapses to the single tick [ts - 1us,
            // ts - 1us]: row 4 reads the tie group one microsecond back and nothing else.
            assertQuery("""
                    SELECT x, sum(x) OVER (ORDER BY ts RANGE BETWEEN 1 MICROSECOND PRECEDING AND CURRENT ROW EXCLUDE CURRENT ROW) AS s
                    FROM tab_us""")
                    .noRandomAccess()
                    .expectSize()
                    .withPlanContaining("range between 1 preceding and 1 preceding")
                    .returns("""
                            x\ts
                            1\tnull
                            2\t1.0
                            3\t1.0
                            4\t5.0
                            """);

            // The same query and the same timestamp numbers against a nanosecond designated
            // timestamp: the written width is now 1000 ticks while the exclusion still subtracts
            // one, so row 4's frame is [ts - 1000ns, ts - 1ns] and reaches all three predecessors.
            // The exclusion is a tick of the driver, not a fixed amount of time.
            assertQuery("""
                    SELECT x, sum(x) OVER (ORDER BY ts RANGE BETWEEN 1 MICROSECOND PRECEDING AND CURRENT ROW EXCLUDE CURRENT ROW) AS s
                    FROM tab_ns""")
                    .noRandomAccess()
                    .expectSize()
                    .withPlanContaining("range between 1000 preceding and 1 preceding")
                    .returns("""
                            x\ts
                            1\tnull
                            2\t1.0
                            3\t1.0
                            4\t6.0
                            """);
        });
    }

    @Test
    public void testRangeWithoutExclusionStopsAtTheCurrentPhysicalRow() throws Exception {
        assertMemoryLeak(() -> {
            createTiedTable();

            // The baseline the exclusion is applied on top of, and a deviation in its own right:
            // the frame grows row by row inside the tie group instead of covering it whole, so
            // rows 2, 3 and 4 read three different frames despite being peers. PostgreSQL gives
            // all three the sum 1 + 2 + 3 + 4 = 10 and last_value 4.
            assertQuery("""
                    SELECT ts, x,
                      sum(x) OVER w AS s,
                      count(*) OVER w AS c,
                      first_value(x) OVER w AS fv,
                      last_value(x) OVER w AS lv
                    FROM tab
                    WINDOW w AS (ORDER BY ts RANGE BETWEEN 2 SECOND PRECEDING AND CURRENT ROW)""")
                    .noRandomAccess()
                    .expectSize()
                    .timestamp("ts")
                    .returns("""
                            ts\tx\ts\tc\tfv\tlv
                            2024-01-01T00:00:00.000000Z\t1\t1.0\t1\t1\t1
                            2024-01-01T00:00:01.000000Z\t2\t3.0\t2\t1\t2
                            2024-01-01T00:00:01.000000Z\t3\t6.0\t3\t1\t3
                            2024-01-01T00:00:01.000000Z\t4\t10.0\t4\t1\t4
                            2024-01-01T00:00:02.000000Z\t5\t15.0\t5\t1\t5
                            """);
        });
    }

    @Test
    public void testRowsExcludeCurrentRowDropsOnlyTheCurrentRow() throws Exception {
        assertMemoryLeak(() -> {
            createTiedTable();

            // One row leaves the frame, never a tie group: row 3 still reads row 2, its peer at
            // the same timestamp, and row 4 still reads rows 2 and 3. This is PostgreSQL's
            // EXCLUDE CURRENT ROW.
            assertQuery("""
                    SELECT ts, x,
                      sum(x) OVER w AS s,
                      count(*) OVER w AS c,
                      first_value(x) OVER w AS fv,
                      last_value(x) OVER w AS lv
                    FROM tab
                    WINDOW w AS (ORDER BY ts ROWS BETWEEN 2 PRECEDING AND CURRENT ROW EXCLUDE CURRENT ROW)""")
                    .noRandomAccess()
                    .expectSize()
                    .timestamp("ts")
                    .withPlanContaining("rows between 2 preceding and 1 preceding")
                    .returns("""
                            ts\tx\ts\tc\tfv\tlv
                            2024-01-01T00:00:00.000000Z\t1\tnull\t0\tnull\tnull
                            2024-01-01T00:00:01.000000Z\t2\t1.0\t1\t1\t1
                            2024-01-01T00:00:01.000000Z\t3\t3.0\t2\t1\t2
                            2024-01-01T00:00:01.000000Z\t4\t5.0\t2\t2\t3
                            2024-01-01T00:00:02.000000Z\t5\t7.0\t2\t3\t4
                            """);
        });
    }

    /**
     * Five rows carrying a three-row tie at 00:00:01, so every frame that ends at the current row
     * has to decide what to do with the peers on either side of it.
     */
    private static void createTiedTable() throws Exception {
        execute("CREATE TABLE tab (ts TIMESTAMP, x LONG) TIMESTAMP(ts) PARTITION BY DAY");
        execute("""
                INSERT INTO tab VALUES
                  ('2024-01-01T00:00:00.000000Z', 1),
                  ('2024-01-01T00:00:01.000000Z', 2),
                  ('2024-01-01T00:00:01.000000Z', 3),
                  ('2024-01-01T00:00:01.000000Z', 4),
                  ('2024-01-01T00:00:02.000000Z', 5)""");
    }
}
