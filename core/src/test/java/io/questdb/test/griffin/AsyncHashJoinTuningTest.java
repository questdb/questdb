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
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.engine.join.HashJoinPayloadSource;
import io.questdb.griffin.engine.table.AsyncHashJoinGroupByAtom;
import io.questdb.griffin.engine.table.AsyncHashJoinRecordCursorFactory;
import io.questdb.griffin.engine.table.HashJoinBuildFrames;
import io.questdb.griffin.engine.table.HashJoinBuildStage;
import io.questdb.griffin.engine.table.HashJoinTupleRing;
import io.questdb.test.AbstractCairoTest;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import static io.questdb.test.griffin.ParallelHashJoinTestUtils.*;

/**
 * The parallel hash join's tuned defaults: the ring budget, which holds the full window of stacks of
 * up to five joins over full-size frames; the Parquet decode budget of each stage's output reader,
 * which takes the whole budget, as the serial join's reader does, while the probe slots' readers keep
 * their shares; and the payload copy, which a probe under a filter that the join stole makes only
 * for a Parquet build that would decode past a probe reader's share. Every result equals the
 * ordinary plan's, in order.
 */
public class AsyncHashJoinTuningTest extends AbstractCairoTest {

    @Override
    @Before
    public void setUp() {
        super.setUp();
        sqlExecutionContext.setParallelHashJoinEnabled(true);
    }

    @After
    public void restorePageFrameSizes() {
        sqlExecutionContext.restoreToDefaultPageFrameSizes();
        HashJoinTupleRing.setTupleLayoutForTesting(HashJoinTupleRing.TUPLE_LAYOUT_AUTO);
    }

    @Test
    public void testConsumerReadsOfParquetVarcharDecodeEachRowGroupOnce() throws Exception {
        // Row groups of 50 rows, eight of them, whose decoded payload, about 300 bytes a row with the
        // VARCHAR's 250 characters, takes about a third of the budget: the output reader keeps every
        // row group decoded, where a quarter of the budget, a probe reader's share, holds fewer than
        // the eight that the matches go round.
        final long budget = 384 * 1024;
        setProperty(PropertyKey.CAIRO_PARTITION_ENCODER_PARQUET_ROW_GROUP_SIZE, 50);
        setProperty(PropertyKey.CAIRO_SQL_PARQUET_CACHE_MEMORY_SIZE, budget);
        assertMemoryLeak(() -> {
            // d's keys 1 to 400 run through its row groups in order, and t's probe keys step by 53, so
            // that consecutive matches land on row groups far apart. t holds more rows than d, so the
            // join hashes d.
            execute("CREATE TABLE t AS (SELECT ((x * 53) % 400 + 1)::INT k, x v, timestamp_sequence('2024-01-01', 60_000_000) ts " +
                    "FROM long_sequence(800)) TIMESTAMP(ts) PARTITION BY DAY");
            execute("CREATE TABLE d AS (SELECT x::INT k, (rpad('v' || x, 250, 'x'))::VARCHAR vc, " +
                    "timestamp_sequence('2024-01-01', 432_000_000) dts FROM long_sequence(400)) TIMESTAMP(dts) PARTITION BY DAY");
            // The active partition stays native: one row whose key no probe row asks for.
            execute("INSERT INTO d VALUES (-1, 'last', '2024-02-01')");
            execute("ALTER TABLE d CONVERT PARTITION TO PARQUET WHERE dts < '2024-02-01'");
            final String sql = "SELECT t.v, d.vc FROM t JOIN d ON t.k = d.k";
            assertParallelHashJoin(sql, sqlExecutionContext);
            try (RecordCursorFactory factory = select(sql)) {
                final AsyncHashJoinRecordCursorFactory join = find(factory);
                Assert.assertNotNull(join);
                final int workerCount = sqlExecutionContext.getSharedQueryWorkerCount();
                for (int execution = 0; execution < 2; execution++) {
                    try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                        long rows = 0;
                        while (cursor.hasNext()) {
                            Assert.assertNotNull(cursor.getRecord().getVarcharA(1));
                            rows++;
                        }
                        Assert.assertEquals(800, rows);
                        final HashJoinBuildStage stage = join.getAtom().getStage(0);
                        // The build reads eight Parquet frames, row groups, and the active partition's.
                        Assert.assertEquals(9, stage.getBuildFrameCount());
                        final HashJoinPayloadSource.Reader output = stage.getPayloadReader(workerCount);
                        Assert.assertEquals(budget, HashJoinBuildFrames.getReaderCacheSize(output));
                        Assert.assertEquals(8, HashJoinBuildFrames.getReaderFrameDecodeCount(output));
                        // The reader keeps every row group it decoded, within its budget.
                        final long cachedBytes = HashJoinBuildFrames.getReaderCachedBytes(output);
                        Assert.assertTrue(String.valueOf(cachedBytes), cachedBytes > 8 * 50 * 250 && cachedBytes <= budget);
                        Assert.assertFalse(stage.isPayloadCopied());
                    }
                }
            }
        });
    }

    @Test
    public void testOutputReaderTakesTheWholeDecodeBudget() throws Exception {
        final long budget = 1024 * 1024;
        setProperty(PropertyKey.CAIRO_SQL_PARQUET_CACHE_MEMORY_SIZE, budget);
        assertMemoryLeak(() -> {
            createProbeTable();
            createBuildTable("dp", true);
            createBuildTable("dn", false);
            final int workerCount = sqlExecutionContext.getSharedQueryWorkerCount();
            Assert.assertTrue(workerCount > 0);
            // Every reader of the probe slots, the owner's and the workers', takes an even share of
            // the budget, but no less than a quarter of it.
            final long share = Math.max(budget / (workerCount + 2), budget / 4);
            for (String sql : new String[]{
                    "SELECT t.v, d.w FROM t JOIN dp d ON t.k = d.k",
                    "SELECT t.v, d.w FROM t LEFT JOIN dn d ON t.k = d.k",
                    // A stack: each stage's output reader takes the whole budget of its own.
                    "SELECT t.v, d.w, e.w ew FROM t LEFT JOIN dp d ON t.k = d.k LEFT JOIN dn e ON t.k = e.k JOIN dp f ON t.k = f.k",
            }) {
                assertParallelHashJoin(sql, sqlExecutionContext);
                try (RecordCursorFactory factory = select(sql)) {
                    final AsyncHashJoinRecordCursorFactory join = find(factory);
                    Assert.assertNotNull(join);
                    try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                        // The probes, and with them their readers, come with the build.
                        Assert.assertNull(join.getAtom().getStage(0).getPayloadReader(workerCount));
                        Assert.assertEquals(0, join.getAtom().getStage(0).getPayloadRecordBindCount());
                        Assert.assertTrue(cursor.hasNext());
                        for (int k = 0, n = join.getSpec().getStageCount(); k < n; k++) {
                            final HashJoinBuildStage stage = join.getAtom().getStage(k);
                            for (int slot = -1; slot < workerCount; slot++) {
                                Assert.assertEquals(sql, share, HashJoinBuildFrames.getReaderCacheSize(stage.getPayloadReader(slot)));
                            }
                            Assert.assertEquals(sql, budget, HashJoinBuildFrames.getReaderCacheSize(stage.getPayloadReader(workerCount)));
                        }
                    }
                }
            }
        });
    }

    @Test
    public void testPayloadCopyRule() throws Exception {
        // Serial builds, and a copy ratio of zero: an unfiltered probe copies any build whose payload
        // fits the copy.
        setProperty(PropertyKey.CAIRO_SQL_PARALLEL_HASH_JOIN_GROUPBY_BUILD_PARALLEL_MIN_ROWS, Long.MAX_VALUE);
        setProperty(PropertyKey.CAIRO_SQL_PARALLEL_HASH_JOIN_GROUPBY_PAYLOAD_COPY_MIN_PROBE_RATIO, "0");
        assertMemoryLeak(() -> {
            createProbeTable();
            createBuildTable("dp", true);
            createBuildTable("dn", false);
            final String unfiltered = "SELECT t.v, d.w FROM t JOIN #d d ON t.k = d.k";
            // The probe input's WHERE, which the join steals, keeps half of t's rows.
            final String filtered = "SELECT t.v, d.w FROM t JOIN #d d ON t.k = d.k WHERE t.v % 2 = 0";
            // An unfiltered probe copies as before, a native and a Parquet build alike; a filtered one
            // copies neither while the Parquet build's frames fit a probe reader's share of the decode
            // budget, the default's here.
            assertCopied(unfiltered.replace("#d", "dn"), true);
            assertCopied(unfiltered.replace("#d", "dp"), true);
            assertCopied(filtered.replace("#d", "dn"), false);
            assertCopied(filtered.replace("#d", "dp"), false);
            // A LEFT join and a stack, whose stages each decide alone.
            assertCopied("SELECT t.v, d.w, e.w ew FROM t LEFT JOIN dn d ON t.k = d.k LEFT JOIN dp e ON t.k = e.k WHERE t.v % 2 = 0", false, false);
            assertCopied("SELECT t.v, d.w, e.w ew FROM t LEFT JOIN dn d ON t.k = d.k LEFT JOIN dp e ON t.k = e.k", true, true);
            // The fused GROUP BY keeps its copy decision, which a probe filter does not change.
            final String groupBy = "SELECT d.w, count(*) c FROM t JOIN dn d ON t.k = d.k WHERE t.v % 2 = 0 ORDER BY d.w";
            sqlExecutionContext.setParallelHashJoinGroupByEnabled(false);
            final String expected;
            try (RecordCursorFactory factory = select(groupBy)) {
                expected = result(factory, sqlExecutionContext);
            } finally {
                sqlExecutionContext.setParallelHashJoinGroupByEnabled(true);
            }
            try (RecordCursorFactory factory = select(groupBy)) {
                final AsyncHashJoinGroupByAtom atom = HashJoinGroupByQualificationTest.fused(factory).getAtom();
                Assert.assertEquals(expected, result(factory, sqlExecutionContext));
                try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                    Assert.assertTrue(cursor.hasNext());
                    Assert.assertTrue(atom.isPayloadCopied());
                }
            }
        });
    }

    @Test
    public void testPayloadCopyRulePastTheReaderShare() throws Exception {
        // A decode budget of 256 bytes: a probe reader's share of it, at most a third, holds fewer of
        // dp's payload rows, a key and w, 12 bytes each, than the twelve of its Parquet frames, which
        // therefore decode past the share. A copy ratio that no probe here reaches keeps the native
        // build from copying.
        setProperty(PropertyKey.CAIRO_SQL_PARQUET_CACHE_MEMORY_SIZE, 256);
        setProperty(PropertyKey.CAIRO_SQL_PARALLEL_HASH_JOIN_GROUPBY_BUILD_PARALLEL_MIN_ROWS, Long.MAX_VALUE);
        setProperty(PropertyKey.CAIRO_SQL_PARALLEL_HASH_JOIN_GROUPBY_PAYLOAD_COPY_MIN_PROBE_RATIO, "1000000");
        assertMemoryLeak(() -> {
            createProbeTable();
            createBuildTable("dp", true);
            createBuildTable("dn", false);
            final String filtered = "SELECT t.v, d.w FROM t JOIN #d d ON t.k = d.k WHERE t.v % 2 = 0";
            // A filtered probe copies the Parquet build past the share, as an unfiltered one does,
            // and leaves the native build alone, as an unfiltered one does at this ratio.
            assertCopied(filtered.replace("#d", "dp"), true);
            assertCopied(filtered.replace("#d", "dn"), false);
            assertCopied("SELECT t.v, d.w FROM t JOIN dp d ON t.k = d.k", true);
            assertCopied("SELECT t.v, d.w FROM t JOIN dn d ON t.k = d.k", false);
            // Builds in rounds copy on the workers, by the same rule.
            setProperty(PropertyKey.CAIRO_SQL_PARALLEL_HASH_JOIN_GROUPBY_BUILD_PARALLEL_MIN_ROWS, 0);
            setProperty(PropertyKey.CAIRO_SQL_PARALLEL_HASH_JOIN_GROUPBY_PAYLOAD_COPY_PARALLEL_MIN_PROBE_RATIO, "1000000");
            assertCopied(filtered.replace("#d", "dp"), true);
            assertCopied(filtered.replace("#d", "dn"), false);
        });
    }

    @Test
    public void testRingBudgetDefaultKeepsTheWindowOfFiveWideOrElevenNarrowStages() throws Exception {
        // Frames of four rows, the configured page frame size, and a dispatch limit of four: the
        // default budget holds four frames of tuples of six 8-byte slots, a probe row and five
        // stages' entries, 4 * 4 * 6 * 8 bytes, the most that the wide layout takes, and so four
        // frames of tuples of twelve narrow slots of 4 bytes, eleven stages' entries.
        setProperty(PropertyKey.CAIRO_SQL_PAGE_FRAME_MAX_ROWS, 4);
        setProperty(PropertyKey.CAIRO_SQL_PARALLEL_FILTER_DISPATCH_LIMIT, 4);
        assertMemoryLeak(() -> {
            final int queueCapacity = configuration.getPageFrameReduceQueueCapacity();
            Assert.assertTrue(queueCapacity >= 4);
            Assert.assertEquals(4 * 4 * 6 * Long.BYTES, configuration.getSqlParallelHashJoinTupleRingMaxSize());
            createProbeTable();
            execute("CREATE TABLE u AS (SELECT x::INT k, x * 3 w FROM long_sequence(5))");
            sqlExecutionContext.changePageFrameSizes(4, 4);
            final StringBuilder sql = new StringBuilder("SELECT t.v");
            final StringBuilder joins = new StringBuilder(" FROM t");
            for (int stage = 1; stage <= 12; stage++) {
                sql.append(", u").append(stage).append(".w w").append(stage);
                joins.append(" LEFT JOIN u u").append(stage).append(" ON t.k = u").append(stage).append(".k");
                if (stage == 5 || stage == 6) {
                    // Five stages in wide slots keep the dispatch limit of four frames; six, whose
                    // frames take 4 * 7 * 8 bytes, keep three.
                    HashJoinTupleRing.setTupleLayoutForTesting(HashJoinTupleRing.TUPLE_LAYOUT_WIDE);
                    assertWindow(sql + joins.toString(), stage, Long.BYTES, stage == 5 ? 4 : 3);
                    HashJoinTupleRing.setTupleLayoutForTesting(HashJoinTupleRing.TUPLE_LAYOUT_AUTO);
                }
                if (stage >= 5) {
                    // In narrow slots, eleven stages keep four frames; twelve, whose frames take
                    // 4 * 13 * 4 bytes, keep three.
                    assertWindow(sql + joins.toString(), stage, Integer.BYTES, stage <= 11 ? 4 : 3);
                }
            }
        });
    }

    private static void assertCopied(String sql, boolean... isCopied) throws SqlException {
        assertParallelHashJoin(sql, sqlExecutionContext);
        try (RecordCursorFactory factory = select(sql)) {
            final AsyncHashJoinRecordCursorFactory join = find(factory);
            Assert.assertNotNull(join);
            Assert.assertEquals(sql, isCopied.length, join.getSpec().getStageCount());
            try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                Assert.assertTrue(cursor.hasNext());
                for (int k = 0; k < isCopied.length; k++) {
                    final HashJoinBuildStage stage = join.getAtom().getStage(k);
                    Assert.assertEquals(sql + ", stage " + k, isCopied[k], stage.isPayloadCopied());
                    // A copied row holds the key, an INT, and w, a LONG, eight bytes apart.
                    Assert.assertEquals(sql + ", stage " + k, isCopied[k] ? 16 * stage.getFrozenBuild().getRowCount() : 0, stage.getPayloadCopySize());
                }
            }
        }
    }

    // While a cursor of the query reads its rows, its ring holds the stages' tuples in slots of the
    // given bytes, in a window of as many frames as given.
    private static void assertWindow(String sql, int stageCount, int slotBytes, int dispatchLimit) throws SqlException {
        assertParallelHashJoin(sql, sqlExecutionContext);
        try (RecordCursorFactory factory = select(sql)) {
            final AsyncHashJoinRecordCursorFactory join = find(factory);
            Assert.assertNotNull(join);
            Assert.assertEquals(sql, stageCount, join.getSpec().getStageCount());
            try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                Assert.assertTrue(cursor.hasNext());
                Assert.assertEquals(sql, 1 + stageCount, join.getAtom().getRing().getTupleStride());
                Assert.assertEquals(sql, (1 + stageCount) * slotBytes, join.getAtom().getRing().getTupleBytes());
                Assert.assertEquals(sql, dispatchLimit, join.getAtom().getRing().getDispatchLimit());
                while (cursor.hasNext()) {
                    Assert.assertEquals(sql, dispatchLimit, join.getAtom().getRing().getDispatchLimit());
                }
            }
        }
    }

    // A dimension of 16 rows over keys 0 to 7, two rows a key, in three daily partitions, which all
    // but the active one convert to Parquet for a Parquet build.
    private static void createBuildTable(String name, boolean isParquet) throws Exception {
        execute("CREATE TABLE " + name + " AS (SELECT (x % 8)::INT k, x * 10 w, timestamp_sequence('2024-01-01', 14_400_000_000) dts " +
                "FROM long_sequence(16)) TIMESTAMP(dts) PARTITION BY DAY");
        if (isParquet) {
            execute("ALTER TABLE " + name + " CONVERT PARTITION TO PARQUET WHERE dts < '2024-01-03'");
        }
    }

    // t, the probe: 32 rows over keys 0 to 9, some of which no dimension holds.
    private static void createProbeTable() throws Exception {
        execute("CREATE TABLE t AS (SELECT (x % 10)::INT k, x v, timestamp_sequence('2024-01-01', 60_000_000) ts " +
                "FROM long_sequence(32)) TIMESTAMP(ts) PARTITION BY DAY");
    }
}
