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

import io.questdb.PropertyKey;
import io.questdb.cairo.TableReader;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.griffin.engine.table.CoveringIndexRecordCursorFactory;
import io.questdb.std.IntList;
import io.questdb.std.str.StringSink;
import io.questdb.test.tools.TestUtils;
import org.junit.After;
import org.junit.Assert;
import org.junit.Test;

/**
 * The covering index's estimates, each bounded to at most 64 distinct partitions and 4096 posting
 * counts per call: the selection estimate (the plan choice at compile time, and the parallel rows
 * of a covered projection at open) and the rows per (key, partition) pair of a GROUP BY that may
 * read per key. The probed partitions are the floor(j * G / min(G, 64))-th of the G partitions the
 * walk yields frames for; a probed partition counts up to 4096 / (min(G, 64) * kS) of its frames;
 * each counts kS = min(K, max(NULL + 1, 4096 / min(G, 64))) keys: NULL, when listed, as it is, the
 * other keys evenly spread and scaled by the share of the non-NULL keys they stand for. Expected
 * values come from that rule in plain Java over the fixtures' known per-partition counts, or from
 * arithmetic on the rows; the rows of every query are compared with the {@code no_index} plan.
 * <p>
 * The sparse fixture: partition 0 holds only {@code 'z'}; each later partition holds keys
 * {@code k0..k399} once each.
 */
public class CoveringIndexEstimateBudgetTest extends AbstractCoveringIndexQueryTest {
    private static final int MAX_COUNTS = 4096;
    private static final int SAMPLE_PARTITIONS = 64;
    private static final int SPARSE_KEYS = 400;

    @Override
    @After
    public void tearDown() throws Exception {
        CoveringIndexRecordCursorFactory.resetModeSelectionsForTesting();
        super.tearDown();
    }

    @Test
    public void testBindVariableKeyHasNoSelectionEstimate() throws Exception {
        assertMemoryLeak(() -> {
            createSparse("t", 100, false);
            bindVariableService.clear();
            bindVariableService.setStr(0, "k1");
            try (RecordCursorFactory factory = select("SELECT ts, a FROM t WHERE s = $1")) {
                final CoveringIndexRecordCursorFactory covering = covering(factory);
                final long[] sink = {-7, -9};
                Assert.assertFalse(covering.estimateSelection(sqlExecutionContext, 0, sink));
                Assert.assertEquals(-7, sink[0]);
                Assert.assertEquals(-9, sink[1]);
            }
            assertSqlCursors("SELECT /*+ no_index */ ts, a FROM t WHERE s = $1", "SELECT ts, a FROM t WHERE s = $1");
        });
    }

    @Test
    public void testCachedPerKeyFactoryResamplesNewPartitions() throws Exception {
        // one compiled GROUP BY, re-executed after an INSERT that adds 99 partitions: the second
        // open places its targets over the 200 partitions, WAL and non-WAL
        assertMemoryLeak(() -> {
            for (boolean wal : new boolean[]{false, true}) {
                final String table = wal ? "tw" : "t";
                createSparse(table, 100, wal);
                // the symbol map grew past its capacity during the first inserts, which the writer
                // rescales on the next commit, changing the table's metadata: let it happen first
                insertSparse(table, 100, 1);
                if (wal) {
                    drainWalQueue();
                }
                final String sql = "SELECT s, max(a) m FROM " + table + " WHERE s IN ('k0', 'k1') ORDER BY s";
                try (RecordCursorFactory factory = select(sql)) {
                    final CoveringIndexRecordCursorFactory covering = covering(factory);
                    drain(factory);
                    Assert.assertEquals(targets(101).toString(), covering.getLastEstimatePartitions().toString());
                    Assert.assertEquals(128, covering.getLastEstimateProbes());
                    insertSparse(table, 101, 99);
                    if (wal) {
                        drainWalQueue();
                    }
                    drain(factory);
                    Assert.assertEquals(targets(200).toString(), covering.getLastEstimatePartitions().toString());
                    Assert.assertEquals(128, covering.getLastEstimateProbes());
                }
                assertSqlCursors(sql.replace("SELECT s", "SELECT /*+ no_index */ s"), sql);
            }
        });
    }

    @Test
    public void testColumnAddedAfterData() throws Exception {
        // s arrives after 64 of 128 hourly partitions: the older ones have no posting reader. The
        // selection counts their rows as selected; the per-key estimate skips them. The targets,
        // every second partition, fall 32 on each side
        assertMemoryLeak(() -> {
            execute("CREATE TABLE c (ts TIMESTAMP, a DOUBLE) TIMESTAMP(ts) PARTITION BY HOUR BYPASS WAL");
            execute("INSERT INTO c SELECT ((x - 1) * 36_000_000L)::TIMESTAMP, x FROM long_sequence(6_400)");
            execute("ALTER TABLE c ADD COLUMN s SYMBOL");
            execute("ALTER TABLE c ALTER COLUMN s ADD INDEX TYPE POSTING INCLUDE (a)");
            // 50 rows of k0 and 50 of k1 in each new partition
            execute("INSERT INTO c (ts, a, s) SELECT ((x + 6_399) * 36_000_000L)::TIMESTAMP, x, 'k' || (x % 2) FROM long_sequence(6_400)");
            final String rows = "SELECT ts, a FROM c WHERE s IN ('k0', 'k1')";
            try (RecordCursorFactory factory = select(rows)) {
                final CoveringIndexRecordCursorFactory covering = covering(factory);
                final long[] sink = new long[2];
                Assert.assertTrue(covering.estimateSelection(sqlExecutionContext, 0, sink));
                // 32 old targets x 100 rows, all counted, and 32 new x 100 selected, over 64 x 100
                Assert.assertEquals((long) ((double) (32 * 100 + 32 * 100) * 12_800 / (64 * 100)), sink[0]);
                Assert.assertEquals(12_800, sink[1]);
                Assert.assertEquals(targets(128).toString(), covering.getLastEstimatePartitions().toString());
                // the counts of the 32 new targets only
                Assert.assertEquals(32 * 2, covering.getLastEstimateProbes());
            }
            assertSqlCursors(rows.replace("SELECT ts", "SELECT /*+ no_index */ ts"), rows);

            final String groupBy = "SELECT s, max(a) m FROM c WHERE s IN ('k0', 'k1') ORDER BY s";
            try (RecordCursorFactory factory = select(groupBy)) {
                final CoveringIndexRecordCursorFactory covering = covering(factory);
                CoveringIndexRecordCursorFactory.resetModeSelectionsForTesting();
                drain(factory);
                Assert.assertEquals(targets(128).toString(), covering.getLastEstimatePartitions().toString());
                Assert.assertEquals(32 * 2, covering.getLastEstimateProbes());
            }
            // 50 rows in each sampled pair of the new partitions; the old ones count for nothing
            assertFrameMode("c", "merged", 2, 50, 64);
            assertSqlCursors(groupBy.replace("SELECT s", "SELECT /*+ no_index */ s"), groupBy);
        });
    }

    @Test
    public void testColumnAddedToActivePartitionWithOnlyNulls() throws Exception {
        // s arrives after 50 rows of hour 0, and the next 50 rows leave it NULL: hour 0's
        // posting index holds no entry, and counts its 100 rows as NULL
        assertMemoryLeak(() -> {
            execute("CREATE TABLE c (ts TIMESTAMP, a DOUBLE) TIMESTAMP(ts) PARTITION BY HOUR BYPASS WAL");
            execute("INSERT INTO c SELECT ((x - 1) * 1_000_000L)::TIMESTAMP, x FROM long_sequence(50)");
            execute("ALTER TABLE c ADD COLUMN s SYMBOL");
            execute("INSERT INTO c (ts, a, s) SELECT ((x + 49) * 1_000_000L)::TIMESTAMP, x, NULL FROM long_sequence(50)");
            execute("INSERT INTO c (ts, a, s) SELECT (3_600_000_000L + (x - 1) * 1_000_000L)::TIMESTAMP, x, 'k' || (x % 2) FROM long_sequence(100)");
            execute("ALTER TABLE c ALTER COLUMN s ADD INDEX TYPE POSTING INCLUDE (a)");
            final String sql = "SELECT ts, a FROM c WHERE s IN (NULL, 'k1')";
            try (RecordCursorFactory factory = select(sql)) {
                final long[] sink = new long[2];
                Assert.assertTrue(covering(factory).estimateSelection(sqlExecutionContext, 0, sink));
                // hour 0: 100 NULL rows; hour 1: 50 k1 rows
                Assert.assertEquals(150, sink[0]);
                Assert.assertEquals(200, sink[1]);
                Assert.assertEquals("[0,1]", covering(factory).getLastEstimatePartitions().toString());
                Assert.assertEquals(4, covering(factory).getLastEstimateProbes());
            }
            assertSqlCursors(sql.replace("SELECT ts", "SELECT /*+ no_index */ ts"), sql);
        });
    }

    @Test
    public void testCompositePartitionCountsOneGroup() throws Exception {
        // the pieces of a composite partition share its index: one group, one target, and every
        // piece's frame counted under it
        setProperty(PropertyKey.CAIRO_O3_PARTITION_SPLIT_MIN_SIZE, "1K");
        setProperty(PropertyKey.CAIRO_PARTITION_COMPACTION_AVG_ROWS_PIECE_LIM, 8);
        assertMemoryLeak(() -> {
            execute("CREATE TABLE m (ts TIMESTAMP, sym SYMBOL INDEX TYPE POSTING INCLUDE (price), price DOUBLE) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("INSERT INTO m SELECT timestamp_sequence('2020-02-03', 15 * 1000000L) ts, 'F'::SYMBOL sym, x * 1.0 price FROM long_sequence(5760)");
            drainWalQueue();
            execute("INSERT INTO m SELECT timestamp_sequence('2020-02-03T02:00:07', 5 * 1000000L) ts, 'G'::SYMBOL sym, x * 1.0 price FROM long_sequence(120)");
            execute("INSERT INTO m SELECT timestamp_sequence('2020-02-03T08:00:11', 5 * 1000000L) ts, 'H'::SYMBOL sym, x * 1.0 price FROM long_sequence(120)");
            drainWalQueue();
            final int pieceCount;
            try (TableReader reader = getReader("m")) {
                pieceCount = reader.getGeometry().getPieceCount(0);
            }
            Assert.assertTrue("test precondition: partition 0 must be composite", pieceCount > 1);
            final String sql = "SELECT sym, max(price) m FROM m WHERE sym IN ('G', 'H', 'F') ORDER BY sym";
            try (RecordCursorFactory factory = select(sql)) {
                final CoveringIndexRecordCursorFactory covering = covering(factory);
                drain(factory);
                Assert.assertEquals("[0]", covering.getLastEstimatePartitions().toString());
                Assert.assertEquals(3L * pieceCount, covering.getLastEstimateProbes());
            }
            // the projection's selection counts the same frames: every row is listed
            try (RecordCursorFactory factory = select("SELECT ts, price FROM m WHERE sym IN ('G', 'H', 'F')")) {
                final long[] sink = new long[2];
                Assert.assertTrue(covering(factory).estimateSelection(sqlExecutionContext, 0, sink));
                Assert.assertEquals(6_000, sink[0]);
                Assert.assertEquals(6_000, sink[1]);
                Assert.assertEquals("[0]", covering(factory).getLastEstimatePartitions().toString());
            }
            assertSqlCursors(sql.replace("SELECT sym", "SELECT /*+ no_index */ sym"), sql);
        });
    }

    @Test
    public void testIntervalFramesPerTargetAreBounded() throws Exception {
        // 1000 daily windows over 3 YEAR partitions: each target partition yields ~365 frames and
        // counts 4096 / (3 * 64) = 21 of them, 64 keys each
        assertMemoryLeak(() -> {
            execute("CREATE TABLE y (ts TIMESTAMP, s SYMBOL INDEX TYPE POSTING INCLUDE (a), a DOUBLE) TIMESTAMP(ts) PARTITION BY YEAR BYPASS WAL");
            // 128 rows in the first hour of each of 1000 days from 2020-01-01: 2 rows per key per day
            execute("INSERT INTO y SELECT ('2020-01-01'::TIMESTAMP + ((x - 1) / 128) * 86_400_000_000L + ((x - 1) % 128) * 20_000_000L)::TIMESTAMP, "
                    + "'k' || ((x - 1) % 64), x FROM long_sequence(128_000)");
            final String sql = "SELECT s, max(a) m FROM y WHERE s IN (" + keyList(0, 64) + ") AND ts IN '2020-01-01T00:00:00;1h;1d;1000' ORDER BY s";
            try (RecordCursorFactory factory = select(sql)) {
                final CoveringIndexRecordCursorFactory covering = covering(factory);
                CoveringIndexRecordCursorFactory.resetModeSelectionsForTesting();
                drain(factory);
                Assert.assertEquals("[0,1,2]", covering.getLastEstimatePartitions().toString());
                Assert.assertEquals(3 * (MAX_COUNTS / (3 * 64)) * 64L, covering.getLastEstimateProbes());
            }
            // one key: 4096 / 3 frames per target, i.e. all 1000, 2 of each window's 128 rows
            final String rows = "SELECT ts, a FROM y WHERE s = 'k0' AND ts IN '2020-01-01T00:00:00;1h;1d;1000'";
            try (RecordCursorFactory factory = select(rows)) {
                final long[] sink = new long[2];
                Assert.assertTrue(covering(factory).estimateSelection(sqlExecutionContext, 0, sink));
                Assert.assertEquals(2_000, sink[0]);
                Assert.assertEquals(128_000, sink[1]);
                Assert.assertEquals("[0,1,2]", covering(factory).getLastEstimatePartitions().toString());
                Assert.assertEquals(1_000, covering(factory).getLastEstimateProbes());
            }
            assertSqlCursors(rows.replace("SELECT ts", "SELECT /*+ no_index */ ts"), rows);
            assertFrameMode("y", "merged", 64, 2, 64);
            assertSqlCursors(sql.replace("SELECT s", "SELECT /*+ no_index */ s"), sql);
        });
    }

    @Test
    public void testLegacyEfBlocksCountEveryRow() throws Exception {
        // an EF block written without its rank trailer cannot count a window that clips it: the
        // selection counts the frame's every row, and the per-key estimate has no bounded answer
        assertMemoryLeak(() -> {
            final String create = "CREATE TABLE l (ts TIMESTAMP, s SYMBOL INDEX TYPE POSTING EF INCLUDE (a), a LONG) TIMESTAMP(ts) PARTITION BY HOUR BYPASS WAL";
            final String insert = "INSERT INTO l SELECT ((x - 1) * 36_000_000L)::TIMESTAMP, 'k' || ((x - 1) % 2), x FROM long_sequence(400)";
            // rows 17-49 of each hour fall in the window, 17 of them k1 and 16 k0
            final String window = " AND ts IN '1970-01-01T00:10;20m;1h;4'";
            final String rows = "SELECT ts, a FROM l WHERE s = 'k1'" + window;
            final String groupBy = "SELECT s, max(a) m FROM l WHERE s IN ('k0', 'k1')" + window + " ORDER BY s";
            for (boolean hasTrailer : new boolean[]{false, true}) {
                execute(create);
                io.questdb.cairo.idx.PostingIndexUtils.isEfRankTrailerEnabled = hasTrailer;
                try {
                    execute(insert);
                } finally {
                    io.questdb.cairo.idx.PostingIndexUtils.isEfRankTrailerEnabled = true;
                }
                engine.releaseAllReaders();
                engine.releaseAllWriters();
                try (RecordCursorFactory factory = select(rows)) {
                    final long[] sink = new long[2];
                    Assert.assertTrue(covering(factory).estimateSelection(sqlExecutionContext, 0, sink));
                    Assert.assertEquals(hasTrailer ? 4 * 17 : 4 * 33, sink[0]);
                    Assert.assertEquals(4 * 33, sink[1]);
                }
                assertSqlCursors(rows.replace("SELECT ts", "SELECT /*+ no_index */ ts"), rows);
                CoveringIndexRecordCursorFactory.resetModeSelectionsForTesting();
                printSql(groupBy, new StringSink());
                // 33 rows over the 2 pairs of each of the 4 windows
                assertFrameMode("l", "merged", 2, hasTrailer ? 4 * 33 / 8 : -1, 64);
                assertSqlCursors(groupBy.replace("SELECT s", "SELECT /*+ no_index */ s"), groupBy);
                execute("DROP TABLE l");
            }
        });
    }

    @Test
    public void testNoPartitionInRangeHasNoPerKeyEstimate() throws Exception {
        // a window past the data: no partition yields a frame, nothing is probed, and the
        // estimate has no answer
        assertMemoryLeak(() -> {
            createSparse("t", 100, false);
            final String sql = "SELECT s, max(a) m FROM t WHERE s IN ('k0', 'k1') AND ts IN '2100-01-01' ORDER BY s";
            try (RecordCursorFactory factory = select(sql)) {
                final CoveringIndexRecordCursorFactory covering = covering(factory);
                CoveringIndexRecordCursorFactory.resetModeSelectionsForTesting();
                drain(factory);
                Assert.assertEquals("[]", covering.getLastEstimatePartitions().toString());
                Assert.assertEquals(0, covering.getLastEstimateProbes());
            }
            assertFrameMode("t", "merged", 2, -1, 64);
            assertRows(sql, "s\tm\n");
        });
    }

    @Test
    public void testNullKeyAbsentFromSomePartitions() throws Exception {
        // NULL rows in even partitions only: a target without them adds no NULL pair. Every pair
        // holds 100 rows, so counting an empty NULL pair would pull the answer under 100
        assertMemoryLeak(() -> {
            execute("CREATE TABLE h (ts TIMESTAMP, s SYMBOL INDEX TYPE POSTING INCLUDE (a), a DOUBLE) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL");
            execute("INSERT INTO h SELECT (((x - 1) / 500) * 86_400_000_000L + ((x - 1) % 500) * 1000L)::TIMESTAMP, "
                    + "CASE WHEN (x - 1) % 500 >= 400 THEN (CASE WHEN ((x - 1) / 500) % 2 = 0 THEN NULL ELSE 'z' END) "
                    + "ELSE 'k' || ((x - 1) % 500 % 4) END, x FROM long_sequence(50_000)");
            final String sql = "SELECT /*+ force_use_covering */ s, max(a) m FROM h WHERE s IN (NULL, 'k0', 'k1', 'k2', 'k3') ORDER BY s";
            CoveringIndexRecordCursorFactory.resetModeSelectionsForTesting();
            printSql(sql, new StringSink());
            assertFrameMode("h", "per-key", 5, 100, 64);
            assertSqlCursors(sql.replace("/*+ force_use_covering */", "/*+ no_index */"), sql);
        });
    }

    @Test
    public void testNullKeyCountsUnscaled() throws Exception {
        // 100 partitions of 4200 rows: k0..k639 4 rows each, 640 NULL, 1000 'z'. The list holds
        // NULL and the 640 keys: 64 counts per target, NULL and 63 others scaled by 640 / 63
        assertMemoryLeak(() -> {
            execute("CREATE TABLE n (ts TIMESTAMP, s SYMBOL INDEX TYPE POSTING INCLUDE (a), a DOUBLE) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL");
            execute("INSERT INTO n SELECT (((x - 1) / 4200) * 86_400_000_000L + ((x - 1) % 4200) * 1000L)::TIMESTAMP, "
                    + "CASE WHEN (x - 1) % 4200 < 2560 THEN 'k' || ((x - 1) % 4200 % 640) "
                    + "WHEN (x - 1) % 4200 < 3200 THEN NULL ELSE 'z' END, x FROM long_sequence(420_000)");
            final String inList = "NULL, " + keyList(0, 640);
            final String rows = "SELECT ts, a FROM n WHERE s IN (" + inList + ")";
            try (RecordCursorFactory factory = select(rows)) {
                final CoveringIndexRecordCursorFactory covering = covering(factory);
                final long[] sink = new long[2];
                Assert.assertTrue(covering.estimateSelection(sqlExecutionContext, 0, sink));
                // per target: 640 NULL + (63 x 4) x 640 / 63 = 3200 of 4200, i.e. 100 x 3200 over the table
                Assert.assertEquals(320_000, sink[0]);
                Assert.assertEquals(420_000, sink[1]);
                Assert.assertEquals(64L * 64, covering.getLastEstimateProbes());
                Assert.assertEquals(targets(100).toString(), covering.getLastEstimatePartitions().toString());
            }
            final String nullOnly = "SELECT ts, a FROM n WHERE s IN (NULL)";
            try (RecordCursorFactory factory = select(nullOnly)) {
                final long[] sink = new long[2];
                Assert.assertTrue(covering(factory).estimateSelection(sqlExecutionContext, 0, sink));
                Assert.assertEquals(64_000, sink[0]);
                Assert.assertEquals(420_000, sink[1]);
                Assert.assertEquals(64, covering(factory).getLastEstimateProbes());
            }
            assertSqlCursors(nullOnly.replace("SELECT ts", "SELECT /*+ no_index */ ts"), nullOnly);

            // the backup a NULL key brings advertises no page frames: the hint drops it
            final String groupBy = "SELECT /*+ force_use_covering */ s, max(a) m FROM n WHERE s IN (" + inList + ") ORDER BY s";
            try (RecordCursorFactory factory = select(groupBy)) {
                final CoveringIndexRecordCursorFactory covering = covering(factory);
                CoveringIndexRecordCursorFactory.resetModeSelectionsForTesting();
                drain(factory);
                Assert.assertEquals(64L * 64, covering.getLastEstimateProbes());
            }
            // per target: NULL 640 rows in 1 pair, 63 keys x 4 rows in 63 pairs scaled by 640 / 63:
            // (640 + 2560) / (1 + 640) = 4
            final double scale = 640.0 / 63;
            final long rowsPerPair = (long) ((64 * 640 + 64 * 252 * scale) / (64 + 64 * 63 * scale));
            Assert.assertEquals(4, rowsPerPair);
            assertFrameMode("n", "merged", 641, rowsPerPair, 64);
            assertSqlCursors(groupBy.replace("/*+ force_use_covering */", "/*+ no_index */"), groupBy);
        });
    }

    @Test
    public void testPairBudgetSamplesEveryTarget() throws Exception {
        // 300 keys used to eat the (partition, key) pair budget, which then probed only the oldest
        // partition, here one without the keys: the estimate was 0 for a list selecting 75%
        assertMemoryLeak(() -> {
            createSparse("t", 1000, false);
            final String rows = "SELECT ts, a FROM t WHERE s IN (" + keyList(0, 300) + ")";
            try (RecordCursorFactory factory = select(rows)) {
                final CoveringIndexRecordCursorFactory covering = covering(factory);
                final long[] sink = new long[2];
                Assert.assertTrue(covering.estimateSelection(sqlExecutionContext, 0, sink));
                Assert.assertEquals(sparseSelectionModel(1000, 300), sink[0]);
                Assert.assertEquals(400_000, sink[1]);
                // within 2% of the 999 x 300 rows the list selects
                final long truth = 999L * 300;
                Assert.assertTrue(sink[0] + " vs " + truth, Math.abs(sink[0] - truth) <= truth / 50);
                Assert.assertEquals(targets(1000).toString(), covering.getLastEstimatePartitions().toString());
                Assert.assertEquals(MAX_COUNTS, covering.getLastEstimateProbes());
            }
            assertSqlCursors(rows.replace("SELECT ts", "SELECT /*+ no_index */ ts"), rows);

            // 1.2M rows: the compile-time choice reads the estimate and takes the scan (75% > 15%)
            createSparse("t3", 3000, false);
            final String big = "SELECT ts, a FROM t3 WHERE s IN (" + keyList(0, 300) + ")";
            assertPlanLacks(big, "CoveringIndex");
            assertSqlCursors(big.replace("SELECT ts", "SELECT /*+ no_index */ ts"), big);
            // an ASOF input keeps the covering plan only under 1% selected
            execute("CREATE TABLE q (ts TIMESTAMP, b DOUBLE) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL");
            execute("INSERT INTO q SELECT (x * 86_400_000_000L)::TIMESTAMP, x FROM long_sequence(100)");
            final String join = "SELECT t.ts, t.a, q.b FROM (" + big + ") t ASOF JOIN q";
            assertPlanLacks(join, "CoveringIndex");
            assertSqlCursors(join.replace("SELECT t.ts", "SELECT /*+ asof_linear(t q) */ t.ts"), join);
        });
    }

    @Test
    public void testPerKeyEstimateProbesSixtyFourPartitions() throws Exception {
        assertMemoryLeak(() -> {
            // sparse: 1 row per pair, merged
            createSparse("t", 1000, false);
            final String sparse = "SELECT s, max(a) m FROM t WHERE s IN ('k0', 'k1') ORDER BY s";
            assertPerKeyEstimate("t", sparse, 1000, 2);
            // target 0 holds neither key; the 63 others one row of each
            assertFrameMode("t", "merged", 2, 1, 64);
            Assert.assertEquals(0, CoveringIndexRecordCursorFactory.getPerKeyModeOpensForTesting());
            Assert.assertEquals(1, CoveringIndexRecordCursorFactory.getMergedModeOpensForTesting());
            assertSqlCursors(sparse.replace("SELECT s", "SELECT /*+ no_index */ s"), sparse);

            // dense: 400 rows per pair over 200 partitions, per-key
            execute("CREATE TABLE d (ts TIMESTAMP, s SYMBOL INDEX TYPE POSTING INCLUDE (a), a DOUBLE) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL");
            execute("INSERT INTO d SELECT (((x - 1) / 800) * 86_400_000_000L + ((x - 1) % 800) * 1000L)::TIMESTAMP, "
                    + "'k' || ((x - 1) % 2), x FROM long_sequence(160_000)");
            final String dense = "SELECT s, max(a) m FROM d WHERE s IN ('k0', 'k1') ORDER BY s";
            assertPerKeyEstimate("d", dense, 200, 2);
            assertFrameMode("d", "per-key", 2, 400, 64);
            Assert.assertEquals(1, CoveringIndexRecordCursorFactory.getPerKeyModeOpensForTesting());
            Assert.assertEquals(0, CoveringIndexRecordCursorFactory.getMergedModeOpensForTesting());
            assertSqlCursors(dense.replace("SELECT s", "SELECT /*+ no_index */ s"), dense);
        });
    }

    @Test
    public void testParquetPartitionsCountTheirPostingIndex() throws Exception {
        // half of 100 partitions in parquet: the covered scan reads their posting index, and so
        // does the estimate
        assertMemoryLeak(() -> {
            createSparse("t", 100, false);
            execute("ALTER TABLE t CONVERT PARTITION TO PARQUET WHERE ts < " + 50 * 86_400_000_000L);
            try (TableReader reader = getReader("t")) {
                Assert.assertEquals(io.questdb.cairo.sql.PartitionFormat.PARQUET, reader.getPartitionFormat(49));
                Assert.assertEquals(io.questdb.cairo.sql.PartitionFormat.NATIVE, reader.getPartitionFormat(50));
            }
            final String rows = "SELECT ts, a FROM t WHERE s IN ('k0', 'k1')";
            try (RecordCursorFactory factory = select(rows)) {
                final CoveringIndexRecordCursorFactory covering = covering(factory);
                final long[] sink = new long[2];
                Assert.assertTrue(covering.estimateSelection(sqlExecutionContext, 0, sink));
                Assert.assertEquals(sparseSelectionModel(100, 2), sink[0]);
                Assert.assertEquals(40_000, sink[1]);
                Assert.assertEquals(targets(100).toString(), covering.getLastEstimatePartitions().toString());
            }
            assertSqlCursors(rows.replace("SELECT ts", "SELECT /*+ no_index */ ts"), rows);
        });
    }

    @Test
    public void testProjectionEstimateProbesSixtyFourPartitions() throws Exception {
        // 400,000 rows over 1000 partitions: past the 262,144 scanned rows under which an open
        // estimates nothing
        assertMemoryLeak(() -> {
            createSparse("t", 1000, false);
            for (int keys = 1; keys <= 2; keys++) {
                final String sql = "SELECT ts, a FROM t WHERE s IN (" + keyList(0, keys) + ")";
                assertProjectionSample(sql, 1000, keys, 0);
                assertSqlCursors(sql.replace("SELECT ts", "SELECT /*+ no_index */ ts"), sql);
            }
        });
    }

    @Test
    public void testProjectionEstimateShapes() throws Exception {
        assertMemoryLeak(() -> {
            // empty table: nothing scanned, nothing probed
            execute("CREATE TABLE e (ts TIMESTAMP, s SYMBOL INDEX TYPE POSTING INCLUDE (a), a DOUBLE) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL");
            try (RecordCursorFactory factory = select("SELECT ts, a FROM e WHERE s = 'k0'")) {
                final CoveringIndexRecordCursorFactory covering = covering(factory);
                final long[] sink = {-1, -1};
                Assert.assertTrue(covering.estimateSelection(sqlExecutionContext, 0, sink));
                Assert.assertEquals(0, sink[0]);
                Assert.assertEquals(0, sink[1]);
                Assert.assertEquals("[]", covering.getLastEstimatePartitions().toString());
                Assert.assertEquals(0, covering.getLastEstimateProbes());
            }
            assertRows("SELECT ts, a FROM e WHERE s = 'k0'", "ts\ta\n");

            // a key the table does not hold: the rows scanned, nothing probed
            createSparse("t", 1000, false);
            try (RecordCursorFactory factory = select("SELECT ts, a FROM t WHERE s = 'nope'")) {
                final CoveringIndexRecordCursorFactory covering = covering(factory);
                final long[] sink = {-1, -1};
                Assert.assertTrue(covering.estimateSelection(sqlExecutionContext, 0, sink));
                Assert.assertEquals(0, sink[0]);
                Assert.assertEquals(400_000, sink[1]);
                Assert.assertEquals("[]", covering.getLastEstimatePartitions().toString());
                Assert.assertEquals(0, covering.getLastEstimateProbes());
                Assert.assertEquals(0, covering.getLastEstimateOpenedPartitions());
            }
            assertRows("SELECT ts, a FROM t WHERE s = 'nope'", "ts\ta\n");

            // one partition of 300,000 rows, 4 keys selecting all of them: parallel rows
            assertUniformProjection("p1", 1, 300_000, 4, 4, 1);
            // under 64 partitions every one is probed, every key counted
            assertUniformProjection("p40", 40, 6_600, 4, 2, 0);
            assertUniformProjection("p64", 64, 4_200, 4, 2, 0);
            // 65: the 64 targets are partitions 0..63
            assertUniformProjection("p65", 65, 4_100, 4, 2, 0);
            // 4000 partitions: 64 opened
            assertUniformProjection("p4000", 4000, 70, 70, 1, 0);
        });
    }

    @Test
    public void testRecentOnlyKeysFallBackToMerge() throws Exception {
        // The declared tradeoff: the targets floor(j * 1000 / 64) stop at partition 984, so keys
        // that only the newest 15 partitions hold are never counted. The estimate finds no pair
        // (-1) and the open takes the merge, which the 1000 rows per pair would have beaten
        // per-key; the rows are the same either way
        assertMemoryLeak(() -> {
            execute("CREATE TABLE r (ts TIMESTAMP, s SYMBOL INDEX TYPE POSTING INCLUDE (a), a DOUBLE) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL");
            execute("INSERT INTO r SELECT ((x - 1) * 86_400_000_000L)::TIMESTAMP, 'z', x FROM long_sequence(985)");
            execute("INSERT INTO r SELECT ((985 + (x - 1) / 4000) * 86_400_000_000L + ((x - 1) % 4000) * 1000L)::TIMESTAMP, "
                    + "'k' || ((x - 1) % 4), x FROM long_sequence(60_000)");
            final String sql = "SELECT s, max(a) m FROM r WHERE s IN ('k0', 'k1', 'k2', 'k3') ORDER BY s";
            Assert.assertEquals(984, targets(1000).getQuick(SAMPLE_PARTITIONS - 1));
            CoveringIndexRecordCursorFactory.resetModeSelectionsForTesting();
            printSql(sql, new StringSink());
            assertFrameMode("r", "merged", 4, -1, 64);
            Assert.assertEquals(0, CoveringIndexRecordCursorFactory.getPerKeyModeOpensForTesting());
            Assert.assertEquals(1, CoveringIndexRecordCursorFactory.getMergedModeOpensForTesting());
            assertSqlCursors(sql.replace("SELECT s", "SELECT /*+ no_index */ s"), sql);
        });
    }

    private static void assertFrameMode(String table, String mode, long keys, long rowsPerPair, int crossover) {
        final String record = CoveringIndexRecordCursorFactory.getLastFrameModeForTesting();
        Assert.assertNotNull("no frame mode decision since the last reset", record);
        final String regex = "covering scan frame mode \\[table=" + table + ", mode=" + mode + ", reason=density, keys=" + keys
                + ", partitionsUpper=\\d+, framesUpper=\\d+, frameCeiling=\\d+, rowsPerPair=" + rowsPerPair
                + ", crossover=" + crossover + "]";
        Assert.assertTrue(record, record.matches(regex));
    }

    private static CoveringIndexRecordCursorFactory covering(RecordCursorFactory factory) {
        for (RecordCursorFactory f = factory; f != null; f = f.getBaseFactory()) {
            if (f instanceof CoveringIndexRecordCursorFactory covering) {
                return covering;
            }
        }
        throw new AssertionError("no covering scan in the plan");
    }

    private static void drain(RecordCursorFactory factory) throws Exception {
        try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
            while (cursor.hasNext()) {
                // the rows are compared elsewhere
            }
        }
    }

    private static String keyList(int from, int count) {
        final StringBuilder sb = new StringBuilder();
        for (int i = from; i < from + count; i++) {
            sb.append(i > from ? ", 'k" : "'k").append(i).append('\'');
        }
        return sb.toString();
    }

    // the sparse fixture's selection: target 0 selects nothing, the others the counted keys, one
    // row each, scaled by K / kS; the sample's share of 400 rows per partition, over the table
    private static long sparseSelectionModel(int partitions, int keys) {
        final int m = Math.min(partitions, SAMPLE_PARTITIONS);
        final int kS = Math.min(keys, Math.max(1, MAX_COUNTS / m));
        long sampledSelected = 0;
        for (int j = 0; j < m; j++) {
            final int partition = (int) ((long) j * partitions / m);
            final long counted = partition == 0 ? 0 : kS;
            sampledSelected += Math.min(kS == keys ? counted : (long) ((double) counted * keys / kS), SPARSE_KEYS);
        }
        return (long) ((double) sampledSelected * partitions * SPARSE_KEYS / ((long) m * SPARSE_KEYS));
    }

    private static IntList targets(int groups) {
        final int m = Math.min(groups, SAMPLE_PARTITIONS);
        final IntList targets = new IntList();
        for (int j = 0; j < m; j++) {
            targets.add((int) ((long) j * groups / m));
        }
        return targets;
    }

    private void assertPerKeyEstimate(String table, String sql, int partitions, int keys) throws Exception {
        try (RecordCursorFactory factory = select(sql)) {
            final CoveringIndexRecordCursorFactory covering = covering(factory);
            engine.releaseAllReaders();
            CoveringIndexRecordCursorFactory.resetModeSelectionsForTesting();
            drain(factory);
            Assert.assertEquals(table, targets(partitions).toString(), covering.getLastEstimatePartitions().toString());
            Assert.assertEquals(table, (long) Math.min(partitions, SAMPLE_PARTITIONS) * keys, covering.getLastEstimateProbes());
            Assert.assertEquals(table, Math.min(partitions, SAMPLE_PARTITIONS), covering.getLastEstimateOpenedPartitions());
        }
    }

    // opens sql's cursor on a fresh reader and asserts the estimate it ran: the targets, keys
    // counts each, partitions opened, and whether the rows went parallel
    private void assertProjectionSample(String sql, int partitions, int keys, int parallelOpens) throws Exception {
        try (RecordCursorFactory factory = select(sql)) {
            final CoveringIndexRecordCursorFactory covering = covering(factory);
            engine.releaseAllReaders();
            drain(factory);
            final int m = Math.min(partitions, SAMPLE_PARTITIONS);
            Assert.assertEquals(sql, targets(partitions).toString(), covering.getLastEstimatePartitions().toString());
            Assert.assertEquals(sql, (long) m * keys, covering.getLastEstimateProbes());
            Assert.assertEquals(sql, m, covering.getLastEstimateOpenedPartitions());
            Assert.assertEquals(sql, parallelOpens, covering.getParallelRowsOpens());
        }
    }

    // `partitions` partitions of `rows` rows, keys k0..k(distinct - 1) round robin; the list holds
    // the first `keys`. Every partition holds the same counts, so the estimate is exact
    private void assertUniformProjection(String table, int partitions, int rows, int distinct, int keys, int parallelOpens) throws Exception {
        execute("CREATE TABLE " + table + " (ts TIMESTAMP, s SYMBOL INDEX TYPE POSTING INCLUDE (a), a DOUBLE) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL");
        execute("INSERT INTO " + table + " SELECT (((x - 1) / " + rows + ") * 86_400_000_000L + ((x - 1) % " + rows + ") * 100L)::TIMESTAMP, "
                + "'k' || ((x - 1) % " + distinct + "), x FROM long_sequence(" + (long) partitions * rows + ")");
        final String sql = "SELECT ts, a FROM " + table + " WHERE s IN (" + keyList(0, keys) + ")";
        assertProjectionSample(sql, partitions, keys, parallelOpens);
        try (RecordCursorFactory factory = select(sql)) {
            final long[] sink = new long[2];
            Assert.assertTrue(covering(factory).estimateSelection(sqlExecutionContext, 0, sink));
            Assert.assertEquals(table, (long) partitions * rows * keys / distinct, sink[0]);
            Assert.assertEquals(table, (long) partitions * rows, sink[1]);
        }
        assertSqlCursors(sql.replace("SELECT ts", "SELECT /*+ no_index */ ts"), sql);
    }

    private void createSparse(String table, int partitions, boolean wal) throws Exception {
        execute("CREATE TABLE " + table + " (ts TIMESTAMP, s SYMBOL INDEX TYPE POSTING INCLUDE (a), a DOUBLE) TIMESTAMP(ts) PARTITION BY DAY "
                + (wal ? "WAL" : "BYPASS WAL"));
        execute("INSERT INTO " + table + " SELECT ((x - 1) * 1000L)::TIMESTAMP, 'z', x FROM long_sequence(" + SPARSE_KEYS + ")");
        insertSparse(table, 1, partitions - 1);
        if (wal) {
            drainWalQueue();
        }
    }

    // partitions first..first + count - 1, each with k0..k399 once
    private void insertSparse(String table, int first, int count) throws Exception {
        execute("INSERT INTO " + table + " SELECT ((" + first + " + (x - 1) / " + SPARSE_KEYS + ") * 86_400_000_000L + ((x - 1) % " + SPARSE_KEYS + ") * 1000L)::TIMESTAMP, "
                + "'k' || ((x - 1) % " + SPARSE_KEYS + "), x FROM long_sequence(" + (long) count * SPARSE_KEYS + ")");
    }

    private void assertRows(String sql, String expected) throws Exception {
        final StringSink rows = new StringSink();
        printSql(sql, rows);
        TestUtils.assertEquals(expected, rows);
    }

    private void assertPlanLacks(String sql, String text) throws Exception {
        final StringSink plan = new StringSink();
        printSql("EXPLAIN " + sql, plan);
        TestUtils.assertNotContains(plan, text);
    }
}
