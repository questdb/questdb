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

package io.questdb.test.cairo.wal.sortedruns;

import io.questdb.cairo.TableReader;
import io.questdb.cairo.TableToken;
import io.questdb.cairo.sql.PartitionFormat;
import io.questdb.test.AbstractCairoTest;
import org.junit.Assert;
import org.junit.Test;

/**
 * End-to-end parity tests for partitions in
 * {@link io.questdb.cairo.sql.PartitionFormat#INDEXED_SORTED_RUNS} format.
 * <p>
 * Each test populates a table that spans <b>three day-bucketed partitions</b>
 * with <b>three commits per partition</b>, where commits within a partition
 * arrive in out-of-order timestamp ranges (cross-commit O3) so the K-way
 * merge over multiple sorted runs is exercised in every test. After
 * inserts, {@link SortedRunsApplyDriver} drains the WAL transactions and
 * the assertions then run SQL queries through the standard read path.
 * <p>
 * Layout produced by {@link #insertMultiPartitionFixture()}:
 * <pre>
 * Partition 1970-01-01 (day 0): 6 rows over 3 sorted runs
 *   Run A (commit order 1): ts=3000, 4000
 *   Run B (commit order 4): ts=1000, 2000   [earlier ts than run A]
 *   Run C (commit order 7): ts=5000, 6000
 * Partition 1970-01-02 (day 1): 6 rows over 3 sorted runs
 *   Run A (commit order 2): ts=D+3000, D+4000
 *   Run B (commit order 5): ts=D+1000, D+2000
 *   Run C (commit order 8): ts=D+5000, D+6000
 * Partition 1970-01-03 (day 2): 6 rows over 3 sorted runs
 *   Run A (commit order 3): ts=2D+3000, 2D+4000
 *   Run B (commit order 6): ts=2D+1000, 2D+2000
 *   Run C (commit order 9): ts=2D+5000, 2D+6000
 * </pre>
 * where {@code D = 86_400_000_000} (one day in microseconds).
 */
public class SortedRunsSqlParityTest extends AbstractCairoTest {

    private static final long DAY_MICROS = 86_400_000_000L;

    @Test
    public void testCountStarSpansAllPartitions() throws Exception {
        assertMemoryLeak(() -> {
            final TableToken token = insertMultiPartitionFixture();
            try (SortedRunsApplyDriver driver = new SortedRunsApplyDriver(engine)) {
                driver.apply(token);
            }
            assertSql(
                    "count\n" +
                            "18\n",
                    "SELECT count(*) FROM x"
            );
        });
    }

    @Test
    public void testIntervalAcrossPartitionsReturnsBracketedRows() throws Exception {
        assertMemoryLeak(() -> {
            final TableToken token = insertMultiPartitionFixture();
            try (SortedRunsApplyDriver driver = new SortedRunsApplyDriver(engine)) {
                driver.apply(token);
            }
            // Range from day 0 row 5 (ts=5000) through day 1 row 2 (ts=D+2000),
            // crossing the partition boundary.
            assertSql(
                    "ts\tval\n" +
                            "1970-01-01T00:00:00.005000Z\t50\n" +
                            "1970-01-01T00:00:00.006000Z\t60\n" +
                            "1970-01-02T00:00:00.001000Z\t110\n" +
                            "1970-01-02T00:00:00.002000Z\t120\n",
                    "SELECT * FROM x WHERE ts BETWEEN '1970-01-01T00:00:00.004500Z' AND '1970-01-02T00:00:00.002500Z'"
            );
        });
    }

    @Test
    public void testIntervalWithinSinglePartition() throws Exception {
        assertMemoryLeak(() -> {
            final TableToken token = insertMultiPartitionFixture();
            try (SortedRunsApplyDriver driver = new SortedRunsApplyDriver(engine)) {
                driver.apply(token);
            }
            // Range entirely within day 1: rows 3 to 5 of that partition.
            assertSql(
                    "ts\tval\n" +
                            "1970-01-02T00:00:00.003000Z\t130\n" +
                            "1970-01-02T00:00:00.004000Z\t140\n" +
                            "1970-01-02T00:00:00.005000Z\t150\n",
                    "SELECT * FROM x WHERE ts BETWEEN '1970-01-02T00:00:00.002500Z' AND '1970-01-02T00:00:00.005500Z'"
            );
        });
    }

    @Test
    public void testOrderByTsLimitCrossesPartitionBoundary() throws Exception {
        assertMemoryLeak(() -> {
            final TableToken token = insertMultiPartitionFixture();
            try (SortedRunsApplyDriver driver = new SortedRunsApplyDriver(engine)) {
                driver.apply(token);
            }
            // First 8 rows in ts-asc order: 6 from day 0 plus first 2 of day 1.
            assertSql(
                    "ts\tval\n" +
                            "1970-01-01T00:00:00.001000Z\t10\n" +
                            "1970-01-01T00:00:00.002000Z\t20\n" +
                            "1970-01-01T00:00:00.003000Z\t30\n" +
                            "1970-01-01T00:00:00.004000Z\t40\n" +
                            "1970-01-01T00:00:00.005000Z\t50\n" +
                            "1970-01-01T00:00:00.006000Z\t60\n" +
                            "1970-01-02T00:00:00.001000Z\t110\n" +
                            "1970-01-02T00:00:00.002000Z\t120\n",
                    "SELECT * FROM x ORDER BY ts ASC LIMIT 8"
            );
        });
    }

    @Test
    public void testPartitionLayoutAfterDriverApply() throws Exception {
        assertMemoryLeak(() -> {
            final TableToken token = insertMultiPartitionFixture();
            try (SortedRunsApplyDriver driver = new SortedRunsApplyDriver(engine)) {
                driver.apply(token);
            }
            try (TableReader reader = engine.getReader(token)) {
                Assert.assertEquals(3, reader.getPartitionCount());
                for (int i = 0; i < 3; i++) {
                    Assert.assertEquals(
                            "partition " + i + " format",
                            PartitionFormat.INDEXED_SORTED_RUNS,
                            reader.getPartitionFormatFromMetadata(i)
                    );
                    Assert.assertEquals(
                            "partition " + i + " row count",
                            6L,
                            reader.getTxFile().getPartitionSize(i)
                    );
                    Assert.assertEquals(
                            "partition " + i + " sorted-runs count",
                            3,
                            reader.getAndInitSortedRunsReader(i).recordCount()
                    );
                }
            }
        });
    }

    @Test
    public void testSelectStarReturnsTsAscAcrossAllPartitions() throws Exception {
        assertMemoryLeak(() -> {
            final TableToken token = insertMultiPartitionFixture();
            try (SortedRunsApplyDriver driver = new SortedRunsApplyDriver(engine)) {
                driver.apply(token);
            }
            assertSql(
                    "ts\tval\n" +
                            "1970-01-01T00:00:00.001000Z\t10\n" +
                            "1970-01-01T00:00:00.002000Z\t20\n" +
                            "1970-01-01T00:00:00.003000Z\t30\n" +
                            "1970-01-01T00:00:00.004000Z\t40\n" +
                            "1970-01-01T00:00:00.005000Z\t50\n" +
                            "1970-01-01T00:00:00.006000Z\t60\n" +
                            "1970-01-02T00:00:00.001000Z\t110\n" +
                            "1970-01-02T00:00:00.002000Z\t120\n" +
                            "1970-01-02T00:00:00.003000Z\t130\n" +
                            "1970-01-02T00:00:00.004000Z\t140\n" +
                            "1970-01-02T00:00:00.005000Z\t150\n" +
                            "1970-01-02T00:00:00.006000Z\t160\n" +
                            "1970-01-03T00:00:00.001000Z\t210\n" +
                            "1970-01-03T00:00:00.002000Z\t220\n" +
                            "1970-01-03T00:00:00.003000Z\t230\n" +
                            "1970-01-03T00:00:00.004000Z\t240\n" +
                            "1970-01-03T00:00:00.005000Z\t250\n" +
                            "1970-01-03T00:00:00.006000Z\t260\n",
                    "SELECT * FROM x"
            );
        });
    }

    /**
     * Creates {@code x} as a WAL table and populates it with 9 commits that
     * interleave 3 days and target each day's middle, then earliest, then
     * latest ts range, so every partition ends up with 3 sorted runs whose
     * commit order does not match their ts order (cross-commit O3 per
     * partition). Returns the table token.
     */
    private TableToken insertMultiPartitionFixture() throws Exception {
        execute("CREATE TABLE x (ts TIMESTAMP, val LONG) TIMESTAMP(ts) PARTITION BY DAY WAL");
        final long d0 = 0L;
        final long d1 = DAY_MICROS;
        final long d2 = 2 * DAY_MICROS;

        // Pass 1: middle-of-day ts ranges (commits A) in day order.
        execute("INSERT INTO x VALUES (" + (d0 + 3000) + ", 30), (" + (d0 + 4000) + ", 40)");
        execute("INSERT INTO x VALUES (" + (d1 + 3000) + ", 130), (" + (d1 + 4000) + ", 140)");
        execute("INSERT INTO x VALUES (" + (d2 + 3000) + ", 230), (" + (d2 + 4000) + ", 240)");

        // Pass 2: earliest-of-day ts ranges (commits B) — within each
        // partition this is a cross-commit O3 vs pass 1.
        execute("INSERT INTO x VALUES (" + (d0 + 1000) + ", 10), (" + (d0 + 2000) + ", 20)");
        execute("INSERT INTO x VALUES (" + (d1 + 1000) + ", 110), (" + (d1 + 2000) + ", 120)");
        execute("INSERT INTO x VALUES (" + (d2 + 1000) + ", 210), (" + (d2 + 2000) + ", 220)");

        // Pass 3: latest-of-day ts ranges (commits C).
        execute("INSERT INTO x VALUES (" + (d0 + 5000) + ", 50), (" + (d0 + 6000) + ", 60)");
        execute("INSERT INTO x VALUES (" + (d1 + 5000) + ", 150), (" + (d1 + 6000) + ", 160)");
        execute("INSERT INTO x VALUES (" + (d2 + 5000) + ", 250), (" + (d2 + 6000) + ", 260)");

        return engine.verifyTableName("x");
    }
}
