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
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.FullPartitionFrameCursorFactory;
import io.questdb.cairo.GenericRecordMetadata;
import io.questdb.cairo.TableReader;
import io.questdb.cairo.TableToken;
import io.questdb.cairo.TableUtils;
import io.questdb.cairo.idx.BitmapIndexFwdReader;
import io.questdb.cairo.idx.IndexReader;
import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.PartitionFrameCursorFactory;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.cairo.sql.RecordMetadata;
import io.questdb.cairo.sql.RowCursor;
import io.questdb.griffin.FunctionParser;
import io.questdb.griffin.OrderByMnemonic;
import io.questdb.griffin.PlanSink;
import io.questdb.griffin.SqlCompiler;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContextImpl;
import io.questdb.griffin.engine.functions.BooleanFunction;
import io.questdb.griffin.engine.functions.regex.SymbolKeySetProvider;
import io.questdb.griffin.engine.table.AdaptiveSymbolPatternRecordCursorFactory;
import io.questdb.griffin.engine.table.HeapRowCursorFactory;
import io.questdb.griffin.engine.table.SymbolPatternIndexRecordCursorFactory;
import io.questdb.griffin.model.ExpressionNode;
import io.questdb.griffin.model.QueryModel;
import io.questdb.std.IntList;
import io.questdb.std.Numbers;
import io.questdb.std.ObjList;
import io.questdb.std.str.StringSink;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

public class SymbolPatternIndexTest extends AbstractCairoTest {

    /**
     * Compiles {@code predicate} (e.g. {@code "sym like 'A%'"}) as a standalone
     * boolean function bound to table {@code t}'s reader, then returns the matched
     * symbol keys that the fast index path would use.
     * <p>
     * Mechanism:
     * <ol>
     *   <li>Open a {@link TableReader} for table {@code t}.</li>
     *   <li>Build a {@link GenericRecordMetadata} copy of the reader's metadata so
     *       that the {@code sym} column carries {@code isSymbolTableStatic=true}.</li>
     *   <li>Parse the expression via {@link FunctionParser} backed by the engine's
     *       full {@link io.questdb.griffin.FunctionFactoryCache}.</li>
     *   <li>Call {@code f.init(reader, sqlExecutionContext)} — {@link TableReader}
     *       implements {@link io.questdb.cairo.sql.SymbolTableSource}, so this
     *       gives the function access to the real symbol table.</li>
     *   <li>Assert the compiled function implements {@link SymbolKeySetProvider},
     *       then read and return its key list.</li>
     * </ol>
     * Using this approach the test directly exercises the provider interface rather
     * than running an end-to-end SQL query (which would pass regardless of this
     * interface because the per-row filter path already works).
     */
    private IntList matchedKeys(String predicate) throws Exception {
        try (TableReader reader = engine.getReader("t")) {
            // Copy metadata so FunctionParser sees the real symbolTableStatic flag
            GenericRecordMetadata meta = GenericRecordMetadata.copyOf(reader.getMetadata());

            // Build a parser backed by the full engine factory cache (includes LIKE/~ factories)
            FunctionParser functionParser = new FunctionParser(configuration, engine.getFunctionFactoryCache());

            // Parse the expression AST
            ExpressionNode node;
            QueryModel qm = QueryModel.FACTORY.newInstance();
            try (SqlCompiler compiler = engine.getSqlCompiler()) {
                node = compiler.testParseExpression(predicate, qm);
            }

            // Compile to a Function; may throw if predicate is malformed
            Function f = functionParser.parseFunction(node, meta, sqlExecutionContext);

            Assert.assertTrue(
                    predicate + " did not compile to a SymbolKeySetProvider: " + f.getClass().getName(),
                    f instanceof SymbolKeySetProvider
            );

            // Bind the function to the reader's static symbol table
            // TableReader implements SymbolTableSource directly
            f.init(reader, sqlExecutionContext);

            IntList out = new IntList();
            out.addAll(((SymbolKeySetProvider) f).getMatchedSymbolKeys());
            f.close();
            return out;
        }
    }

    @Test
    public void testAdaptiveCoveringPageFrameRoutesSelectiveAndBroadPatterns() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (sym SYMBOL INDEX TYPE POSTING INCLUDE (price), price DOUBLE, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("INSERT INTO t VALUES ('AA', 1.0, 0), ('AB', 2.0, 1)");
            execute("INSERT INTO t SELECT 'BA', x::DOUBLE, timestamp_sequence(2, 1) FROM long_sequence(1_000)");

            // A covering delegate exists here, so the policy line must render the COVERING route's
            // admitted share (2%), not the bitmap index route's 5% -- the two are separate constants.
            assertQuery("SELECT sum(price) FROM t WHERE sym LIKE 'A%'")
                    .noLeakCheck()
                    .assertsPlanContaining("AdaptiveSymbolPattern policy: matching rows <= 2%, bounded probes");
            assertQuery("SELECT sum(price) FROM t WHERE sym LIKE 'A%'")
                    .noLeakCheck()
                    .assertsPlanContaining("CoveringIndex");
            assertQuery("SELECT sum(price) FROM t WHERE sym LIKE 'A%'")
                    .noLeakCheck()
                    .assertsPlanContaining("PageFrame");

            AdaptiveSymbolPatternRecordCursorFactory.resetTestCounters();
            TestUtils.assertEquals(
                    select("SELECT /*+ no_symbol_pattern_index(t) no_covering(t) */ sum(price) FROM t WHERE sym LIKE 'A%'"),
                    select("SELECT sum(price) FROM t WHERE sym LIKE 'A%'")
            );
            Assert.assertTrue(AdaptiveSymbolPatternRecordCursorFactory.testCoveringInvocations.get() > 0);
            Assert.assertEquals(0, AdaptiveSymbolPatternRecordCursorFactory.testScanInvocations.get());

            AdaptiveSymbolPatternRecordCursorFactory.resetTestCounters();
            TestUtils.assertEquals(
                    select("SELECT /*+ no_symbol_pattern_index(t) no_covering(t) */ sum(price) FROM t WHERE sym LIKE 'B%'"),
                    select("SELECT sum(price) FROM t WHERE sym LIKE 'B%'")
            );
            Assert.assertEquals(0, AdaptiveSymbolPatternRecordCursorFactory.testCoveringInvocations.get());
            Assert.assertTrue(AdaptiveSymbolPatternRecordCursorFactory.testScanInvocations.get() > 0);
        });
    }

    @Test
    public void testAdaptiveRecordRoutesHotOneKeyAndManyColdKeys() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (sym SYMBOL INDEX, v LONG, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("INSERT INTO t SELECT 'HOT', x, timestamp_sequence(0, 1) FROM long_sequence(10_000)");
            execute("INSERT INTO t SELECT 'C' || x, 10_000 + x, timestamp_sequence(10_000, 1) FROM long_sequence(100)");

            assertQuery("SELECT sym, v FROM t WHERE sym LIKE 'C%' ORDER BY v")
                    .noLeakCheck()
                    .assertsPlanContaining("AdaptiveSymbolPattern");
            assertQuery("SELECT sym, v FROM t WHERE sym LIKE 'C%' ORDER BY v")
                    .noLeakCheck()
                    .assertsPlanContaining("SymbolPatternIndex");
            assertQuery("SELECT sym, v FROM t WHERE sym LIKE 'C%' ORDER BY v")
                    .noLeakCheck()
                    .assertsPlanContaining("PageFrame");

            SymbolPatternIndexRecordCursorFactory.resetTestCounters();
            TestUtils.assertEquals(
                    select("SELECT /*+ no_symbol_pattern_index(t) */ sym, v FROM t WHERE sym LIKE 'HOT' ORDER BY v"),
                    select("SELECT sym, v FROM t WHERE sym LIKE 'HOT' ORDER BY v")
            );
            Assert.assertEquals(0, SymbolPatternIndexRecordCursorFactory.testIndexInvocations.get());
            Assert.assertTrue(SymbolPatternIndexRecordCursorFactory.testFallbackInvocations.get() > 0);

            SymbolPatternIndexRecordCursorFactory.resetTestCounters();
            TestUtils.assertEquals(
                    select("SELECT /*+ no_symbol_pattern_index(t) */ sym, v FROM t WHERE sym LIKE 'C%' ORDER BY v"),
                    select("SELECT sym, v FROM t WHERE sym LIKE 'C%' ORDER BY v")
            );
            Assert.assertTrue(SymbolPatternIndexRecordCursorFactory.testIndexInvocations.get() > 0);
            Assert.assertEquals(0, SymbolPatternIndexRecordCursorFactory.testFallbackInvocations.get());
        });
    }

    @Test
    public void testAdaptiveRecordRoutesNegatedSelectiveAndBroadPatterns() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (sym SYMBOL INDEX, v LONG, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("INSERT INTO t SELECT 'AA', x, timestamp_sequence(0, 1) FROM long_sequence(10_000)");
            execute("INSERT INTO t VALUES ('BA', 10_001, 10_001), ('BB', 10_002, 10_002)");

            SymbolPatternIndexRecordCursorFactory.resetTestCounters();
            TestUtils.assertEquals(
                    select("SELECT /*+ no_symbol_pattern_index(t) */ sym, v FROM t WHERE sym NOT LIKE 'A%' ORDER BY v"),
                    select("SELECT sym, v FROM t WHERE sym NOT LIKE 'A%' ORDER BY v")
            );
            Assert.assertTrue(SymbolPatternIndexRecordCursorFactory.testIndexInvocations.get() > 0);
            Assert.assertEquals(0, SymbolPatternIndexRecordCursorFactory.testFallbackInvocations.get());

            SymbolPatternIndexRecordCursorFactory.resetTestCounters();
            TestUtils.assertEquals(
                    select("SELECT /*+ no_symbol_pattern_index(t) */ sym, v FROM t WHERE sym NOT LIKE 'Z%' ORDER BY v"),
                    select("SELECT sym, v FROM t WHERE sym NOT LIKE 'Z%' ORDER BY v")
            );
            Assert.assertEquals(0, SymbolPatternIndexRecordCursorFactory.testIndexInvocations.get());
            Assert.assertTrue(SymbolPatternIndexRecordCursorFactory.testFallbackInvocations.get() > 0);
        });
    }

    /**
     * M2-a. The probe budget bounds PLANNING work, and it has to bound the two dimensions it spends
     * that work on -- partition frames and matched dictionary keys -- independently. Multiplying them
     * into one counter makes a table's partition count alone exhaust the budget: at the default
     * threshold of 100, a 40-partition table with four matched keys hits 160 probes and the estimate
     * rejects the route at frame 26, no matter how selective the pattern is. Measured on a 20M-row,
     * 50-partition table, that flip costs 40x (0.51 ms on the index route against 20.2 ms on the
     * parallel scan) the moment a LIKE matches a third symbol.
     */
    @Test
    public void testManyPartitionsWithSeveralMatchedKeysUsesIndexRoute() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (sym SYMBOL INDEX, v LONG, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            // 4_000 rows at one row per 864s spread over 40 daily partitions; 'r1'..'r4' carry one row
            // each, so the matched share is 4/4_000 -- far inside the 1/20 the index route admits.
            execute("INSERT INTO t SELECT CASE WHEN x % 1_000 = 0 THEN 'r' || (x / 1_000) ELSE 'common' END, " +
                    "x, timestamp_sequence(0, 864_000_000L) FROM long_sequence(4_000)");
            Assert.assertEquals(40, countOf("SELECT count() FROM table_partitions('t')"));
            Assert.assertEquals(4, countOf("SELECT count_distinct(sym) FROM t WHERE sym LIKE 'r%'"));

            SymbolPatternIndexRecordCursorFactory.resetTestCounters();
            TestUtils.assertEquals(
                    select("SELECT /*+ no_symbol_pattern_index(t) */ sym, v FROM t WHERE sym LIKE 'r%' ORDER BY v"),
                    select("SELECT sym, v FROM t WHERE sym LIKE 'r%' ORDER BY v")
            );
            Assert.assertTrue(
                    "40 frames x 4 keys must not exhaust a budget of 100 frames and 100 keys per frame",
                    SymbolPatternIndexRecordCursorFactory.testIndexInvocations.get() > 0
            );
            Assert.assertEquals(0, SymbolPatternIndexRecordCursorFactory.testFallbackInvocations.get());
        });
    }

    /**
     * M2-b. A timestamp predicate builds an {@code AbstractIntervalPartitionFrameCursor}, whose
     * {@code size()} answers -1 rather than counting rows it has not walked yet. Reading that -1 as
     * "unknown row count, reject" put every time-filtered query -- the archetypal time-series query,
     * and the one whose row set is already narrowed to where an index route wins -- on the fallback
     * scan permanently. The estimate now counts the selected rows off the frames it walks anyway.
     */
    @Test
    public void testTimestampFilteredQueryUsesIndexRoute() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (sym SYMBOL INDEX, v LONG, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            // 400 rows spread over 10 daily partitions; 'rare' carries every hundredth row.
            execute("INSERT INTO t SELECT CASE WHEN x % 100 = 0 THEN 'rare' ELSE 'common' END, " +
                    "x, timestamp_sequence(0, 2_160_000_000L) FROM long_sequence(400)");
            Assert.assertEquals(10, countOf("SELECT count() FROM table_partitions('t')"));

            final String predicate = "sym LIKE 'r%' AND ts >= '1970-01-01T00:00:00.000000Z' "
                    + "AND ts < '1970-01-06T00:00:00.000000Z'";
            SymbolPatternIndexRecordCursorFactory.resetTestCounters();
            TestUtils.assertEquals(
                    select("SELECT /*+ no_symbol_pattern_index(t) */ sym, v FROM t WHERE " + predicate + " ORDER BY v"),
                    select("SELECT sym, v FROM t WHERE " + predicate + " ORDER BY v")
            );
            Assert.assertTrue(
                    "an interval cursor reports size() == -1; the estimate must count selected rows instead",
                    SymbolPatternIndexRecordCursorFactory.testIndexInvocations.get() > 0
            );
            Assert.assertEquals(0, SymbolPatternIndexRecordCursorFactory.testFallbackInvocations.get());
        });
    }

    /**
     * Pins both probe caps and pins that they stay independent. With the budget at 4:
     * three frames and four keys is admitted (the old cumulative counter multiplied them to 12 and
     * rejected); five keys in three frames is rejected by the key cap; one key over ten frames is
     * rejected by the frame cap. The frame cap is the guard that keeps the index route out of the
     * partition counts where it loses to the parallel scan, so a change that drops it fails here.
     * <p>
     * The frame cap has two implementations and this pins both. A full cursor is rejected in O(1)
     * off the reader's partition count; an interval cursor cannot be, because it answers
     * {@code size()} with -1 and its frames are only the partitions IN RANGE. The interval leg is
     * therefore the only test in this class that reaches the in-loop {@code frames} counter, and it
     * asserts the exact frame count so that the frame cap, not the row-share test, is provably what
     * rejected the route.
     */
    @Test
    public void testFrameAndKeyProbeCapsApplyIndependently() throws Exception {
        node1.setProperty(PropertyKey.CAIRO_SQL_SYMBOL_PATTERN_INDEX_THRESHOLD, "4");
        assertMemoryLeak(() -> {
            // 400 rows over 3 daily partitions, four matched keys: 3 <= 4 frames, 4 <= 4 keys.
            execute("CREATE TABLE caps_within (sym SYMBOL INDEX, v LONG, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("INSERT INTO caps_within SELECT CASE WHEN x % 100 = 0 THEN 'r' || (x / 100) ELSE 'common' END, " +
                    "x, timestamp_sequence(0, 648_000_000L) FROM long_sequence(400)");
            Assert.assertEquals(3, countOf("SELECT count() FROM table_partitions('caps_within')"));
            assertPatternRoute("caps_within", "sym LIKE 'r%'", true);

            // Same three frames, five matched keys: past the key cap.
            execute("CREATE TABLE caps_past_keys (sym SYMBOL INDEX, v LONG, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("INSERT INTO caps_past_keys SELECT CASE WHEN x % 80 = 0 THEN 'r' || (x / 80) ELSE 'common' END, " +
                    "x, timestamp_sequence(0, 648_000_000L) FROM long_sequence(400)");
            Assert.assertEquals(3, countOf("SELECT count() FROM table_partitions('caps_past_keys')"));
            Assert.assertEquals(5, countOf("SELECT count_distinct(sym) FROM caps_past_keys WHERE sym LIKE 'r%'"));
            assertPatternRoute("caps_past_keys", "sym LIKE 'r%'", false);

            // One matched key over ten frames: past the frame cap.
            execute("CREATE TABLE caps_past_frames (sym SYMBOL INDEX, v LONG, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("INSERT INTO caps_past_frames SELECT CASE WHEN x % 100 = 0 THEN 'rare' ELSE 'common' END, " +
                    "x, timestamp_sequence(0, 2_160_000_000L) FROM long_sequence(400)");
            Assert.assertEquals(10, countOf("SELECT count() FROM table_partitions('caps_past_frames')"));
            assertPatternRoute("caps_past_frames", "sym LIKE 'r%'", false);

            // One matched key over six IN-RANGE frames of a ten-partition table. An interval cursor
            // reports size() == -1, so the O(1) pre-check must skip it and only the in-loop frame cap
            // can reject: the estimate has to pull maxEstimateProbes + 1 == 5 frames and stop there.
            // The matched rows are 2 of the 240 the interval selects (0.8%), far inside the admitted
            // 5% share, so the row-share test cannot be what rejects this - the exact frame count is
            // what proves the frame cap did.
            execute("CREATE TABLE caps_past_interval_frames (sym SYMBOL INDEX, v LONG, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("INSERT INTO caps_past_interval_frames SELECT CASE WHEN x % 100 = 0 THEN 'rare' ELSE 'common' END, " +
                    "x, timestamp_sequence('2024-01-01T00:00:00.000000Z', 2_160_000_000L) FROM long_sequence(400)");
            Assert.assertEquals(10, countOf("SELECT count() FROM table_partitions('caps_past_interval_frames')"));
            final String sixPartitionInterval = "ts >= '2024-01-02T00:00:00.000000Z' AND ts < '2024-01-08T00:00:00.000000Z'";
            Assert.assertEquals(
                    6,
                    countOf("SELECT count() FROM (SELECT DISTINCT timestamp_floor('d', ts) FROM caps_past_interval_frames WHERE "
                            + sixPartitionInterval + ")")
            );
            Assert.assertEquals(240, countOf("SELECT count() FROM caps_past_interval_frames WHERE " + sixPartitionInterval));
            Assert.assertEquals(2, countOf("SELECT count() FROM caps_past_interval_frames WHERE sym LIKE 'r%' AND " + sixPartitionInterval));

            AdaptiveSymbolPatternRecordCursorFactory.resetTestCounters();
            AdaptiveSymbolPatternRecordCursorFactory.isEstimatorCounterEnabled = true;
            try {
                assertPatternRoute("caps_past_interval_frames", "sym LIKE 'r%' AND " + sixPartitionInterval, false);
                Assert.assertEquals(
                        "the in-loop frame cap must reject on the frame after the cap, not the row share",
                        5,
                        AdaptiveSymbolPatternRecordCursorFactory.testEstimatorFramesWalked.get()
                );
            } finally {
                AdaptiveSymbolPatternRecordCursorFactory.isEstimatorCounterEnabled = false;
                AdaptiveSymbolPatternRecordCursorFactory.resetTestCounters();
            }
        });
    }

    /**
     * The frame cap has an O(1) form for cursors that walk whole partitions: their frame count is
     * bounded by the reader's partition count, so a table past the cap can be rejected before the
     * first {@code next()}. Route counters cannot see the difference -- both forms pick the fallback
     * scan -- so this counts the frames the estimator pulled. Measured, the walk it skips costs about
     * 0.02 ms per frame, so roughly 2 ms per open at the default cap.
     */
    @Test
    public void testManyPartitionScanRejectsWithoutWalkingFrames() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (sym SYMBOL INDEX, v LONG, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            // 20_000 rows over 200 daily partitions, one 'rare' row per partition: 1% of the table,
            // well inside the admitted share, so only the frame cap can reject this.
            execute("INSERT INTO t SELECT CASE WHEN x % 100 = 0 THEN 'rare' ELSE 'common' END, " +
                    "x, timestamp_sequence(0, 864_000_000L) FROM long_sequence(20_000)");
            Assert.assertEquals(200, countOf("SELECT count() FROM table_partitions('t')"));

            AdaptiveSymbolPatternRecordCursorFactory.resetTestCounters();
            SymbolPatternIndexRecordCursorFactory.resetTestCounters();
            AdaptiveSymbolPatternRecordCursorFactory.isEstimatorCounterEnabled = true;
            try {
                TestUtils.assertEquals(
                        select("SELECT /*+ no_symbol_pattern_index(t) */ sym, v FROM t WHERE sym LIKE 'r%' ORDER BY v"),
                        select("SELECT sym, v FROM t WHERE sym LIKE 'r%' ORDER BY v")
                );
                Assert.assertTrue(
                        "200 partitions is past the cap, so the route must be the fallback scan",
                        SymbolPatternIndexRecordCursorFactory.testFallbackInvocations.get() > 0
                );
                Assert.assertEquals(0, SymbolPatternIndexRecordCursorFactory.testIndexInvocations.get());
                Assert.assertEquals(
                        "the partition count already decides this; the estimate must not pull a frame",
                        0,
                        AdaptiveSymbolPatternRecordCursorFactory.testEstimatorFramesWalked.get()
                );
            } finally {
                AdaptiveSymbolPatternRecordCursorFactory.isEstimatorCounterEnabled = false;
                AdaptiveSymbolPatternRecordCursorFactory.resetTestCounters();
                SymbolPatternIndexRecordCursorFactory.resetTestCounters();
            }
        });
    }

    /**
     * The O(1) rejection above must NOT extend to interval cursors. Their frames are the partitions
     * IN RANGE, which the table's total partition count does not bound usefully: a narrow filter on a
     * long-lived table walks a handful of frames and is precisely where the index route wins --
     * measured 0.81 ms against the parallel scan's 2.71 ms on a 100-partition, 20M-row table with a
     * 30-day filter. Rejecting it off the total partition count would be cheap and wrong.
     */
    @Test
    public void testNarrowIntervalOnManyPartitionTableUsesIndexRoute() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (sym SYMBOL INDEX, v LONG, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("INSERT INTO t SELECT CASE WHEN x % 100 = 0 THEN 'rare' ELSE 'common' END, " +
                    "x, timestamp_sequence(0, 864_000_000L) FROM long_sequence(20_000)");
            Assert.assertEquals(200, countOf("SELECT count() FROM table_partitions('t')"));

            final String predicate = "sym LIKE 'r%' AND ts >= '1970-01-01T00:00:00.000000Z' "
                    + "AND ts < '1970-01-03T00:00:00.000000Z'";
            AdaptiveSymbolPatternRecordCursorFactory.resetTestCounters();
            SymbolPatternIndexRecordCursorFactory.resetTestCounters();
            AdaptiveSymbolPatternRecordCursorFactory.isEstimatorCounterEnabled = true;
            try {
                TestUtils.assertEquals(
                        select("SELECT /*+ no_symbol_pattern_index(t) */ sym, v FROM t WHERE " + predicate + " ORDER BY v"),
                        select("SELECT sym, v FROM t WHERE " + predicate + " ORDER BY v")
                );
                Assert.assertTrue(
                        "two frames in range is well inside the cap, however many partitions the table has",
                        SymbolPatternIndexRecordCursorFactory.testIndexInvocations.get() > 0
                );
                Assert.assertEquals(0, SymbolPatternIndexRecordCursorFactory.testFallbackInvocations.get());
                Assert.assertEquals(
                        "the estimate must walk the two frames the interval selects, not the table's 200",
                        2,
                        AdaptiveSymbolPatternRecordCursorFactory.testEstimatorFramesWalked.get()
                );
            } finally {
                AdaptiveSymbolPatternRecordCursorFactory.isEstimatorCounterEnabled = false;
                AdaptiveSymbolPatternRecordCursorFactory.resetTestCounters();
                SymbolPatternIndexRecordCursorFactory.resetTestCounters();
            }
        });
    }

    @Test
    public void testBindVariableKeysRefreshAcrossCursorReuse() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (sym SYMBOL INDEX, v LONG, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("INSERT INTO t VALUES ('AA', 1, 0), ('BB', 2, 1)");
            bindVariableService.setStr("pattern", "A%");
            final String query = "SELECT sym, v FROM t WHERE sym LIKE :pattern ORDER BY v";
            try (RecordCursorFactory factory = engine.select(query, sqlExecutionContext)) {
                TestUtils.assertEquals(select("SELECT /*+ no_symbol_pattern_index(t) */ sym, v FROM t WHERE sym LIKE :pattern ORDER BY v"), printFactory(factory));

                execute("INSERT INTO t VALUES ('AC', 3, 2)");
                TestUtils.assertEquals(select("SELECT /*+ no_symbol_pattern_index(t) */ sym, v FROM t WHERE sym LIKE :pattern ORDER BY v"), printFactory(factory));

                bindVariableService.setStr("pattern", null);
                TestUtils.assertEquals(select("SELECT /*+ no_symbol_pattern_index(t) */ sym, v FROM t WHERE sym LIKE :pattern ORDER BY v"), printFactory(factory));

                bindVariableService.setStr("pattern", "");
                TestUtils.assertEquals(select("SELECT /*+ no_symbol_pattern_index(t) */ sym, v FROM t WHERE sym LIKE :pattern ORDER BY v"), printFactory(factory));
            }
        });
    }

    @Test
    public void testBindVariableKeysRefreshOnCoveringPageFrameCursorReuse() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (sym SYMBOL INDEX TYPE POSTING INCLUDE (price), price DOUBLE, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("INSERT INTO t VALUES ('AA', 1.0, 0), ('BB', 2.0, 1)");
            bindVariableService.setStr("pattern", "A%");
            final String query = "SELECT sym, sum(price) total FROM t WHERE sym LIKE :pattern ORDER BY sym";
            final String oracle = "SELECT /*+ no_symbol_pattern_index(t) no_covering(t) */ sym, sum(price) total FROM t WHERE sym LIKE :pattern ORDER BY sym";
            try (RecordCursorFactory factory = engine.select(query, sqlExecutionContext)) {
                TestUtils.assertEquals(select(oracle), printFactory(factory));
                execute("INSERT INTO t VALUES ('AC', 3.0, 2)");
                TestUtils.assertEquals(select(oracle), printFactory(factory));
                bindVariableService.setStr("pattern", null);
                TestUtils.assertEquals(select(oracle), printFactory(factory));
                bindVariableService.setStr("pattern", "");
                TestUtils.assertEquals(select(oracle), printFactory(factory));
            }
        });
    }

    @Test
    public void testBindVariableKeysRefreshOnCoveringRecordCursorReuse() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (sym SYMBOL INDEX TYPE POSTING INCLUDE (price), price DOUBLE, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("INSERT INTO t VALUES ('AA', 1.0, 0), ('BB', 2.0, 1)");
            bindVariableService.setStr("pattern", "A%");
            final String query = "SELECT sym, price FROM t WHERE sym LIKE :pattern ORDER BY price";
            final String oracle = "SELECT /*+ no_symbol_pattern_index(t) no_covering(t) */ sym, price FROM t WHERE sym LIKE :pattern ORDER BY price";
            try (RecordCursorFactory factory = engine.select(query, sqlExecutionContext)) {
                TestUtils.assertEquals(select(oracle), printFactory(factory));
                execute("INSERT INTO t VALUES ('AC', 3.0, 2)");
                TestUtils.assertEquals(select(oracle), printFactory(factory));
                bindVariableService.setStr("pattern", null);
                TestUtils.assertEquals(select(oracle), printFactory(factory));
                bindVariableService.setStr("pattern", "");
                TestUtils.assertEquals(select(oracle), printFactory(factory));
            }
        });
    }

    @Test
    public void testBindVariableKeysRefreshOnNegatedCursorReuse() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (sym SYMBOL INDEX, v LONG, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("INSERT INTO t VALUES ('AA', 1, 0), ('BB', 2, 1)");
            bindVariableService.setStr("pattern", "A%");
            final String query = "SELECT sym, v FROM t WHERE sym NOT LIKE :pattern ORDER BY v";
            final String oracle = "SELECT /*+ no_symbol_pattern_index(t) */ sym, v FROM t WHERE sym NOT LIKE :pattern ORDER BY v";
            try (RecordCursorFactory factory = engine.select(query, sqlExecutionContext)) {
                TestUtils.assertEquals(select(oracle), printFactory(factory));
                execute("INSERT INTO t VALUES ('AC', 3, 2)");
                TestUtils.assertEquals(select(oracle), printFactory(factory));
                bindVariableService.setStr("pattern", null);
                TestUtils.assertEquals(select(oracle), printFactory(factory));
                bindVariableService.setStr("pattern", "");
                TestUtils.assertEquals(select(oracle), printFactory(factory));
            }
        });
    }

    /**
     * The heap row cursor must open one row cursor per LIVE symbol key, not one per per-key factory
     * ever allocated. {@link SymbolPatternIndexRecordCursorFactory} grows its per-key factory list
     * monotonically and re-arms only the first {@code cursorFactoriesIdx[0]} entries, so re-running a
     * prepared statement with a narrower pattern leaves factories behind that still carry a symbol key
     * from the previous execution. Their rows never reach the result set -- the heap seeds itself only
     * up to the live count -- so the waste shows up solely as extra index seeks, one per stale key per
     * page frame, which is why this test counts cursor opens instead of asserting rows.
     * <p>
     * The reused factory is compared against a freshly compiled one running the identical query, so the
     * assertion does not depend on how many page frames the table happens to have.
     */
    @Test
    public void testHeapRowCursorOpensOneCursorPerLiveKey() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (sym SYMBOL INDEX, v LONG, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("""
                    INSERT INTO t VALUES
                      ('aa', 1, '2024-01-01T00:00:00.000000Z'),
                      ('ab', 2, '2024-01-01T00:00:01.000000Z'),
                      ('ac', 3, '2024-01-01T00:00:02.000000Z'),
                      ('zz', 4, '2024-01-01T00:00:03.000000Z')""");
            // Padding so that both patterns stay under the index route's 5% selectivity policy:
            // 'a%' matches 3 of 100 rows, 'z%' matches 1.
            execute("INSERT INTO t SELECT 'bb', 100 + x, timestamp_sequence('2024-01-01T00:01:00.000000Z', 1_000_000) FROM long_sequence(96)");

            final String query = "SELECT sym, v FROM t WHERE sym LIKE :pattern";
            final String oracle = "SELECT /*+ no_symbol_pattern_index(t) */ sym, v FROM t WHERE sym LIKE :pattern";
            bindVariableService.setStr("pattern", "z%");
            final String expectedNarrow = select(oracle);
            bindVariableService.setStr("pattern", "a%");
            final String expectedWide = select(oracle);

            HeapRowCursorFactory.isRowCursorCounterEnabled = true;
            try {
                // Baseline: a factory that never saw the wider pattern holds exactly one per-key factory.
                bindVariableService.setStr("pattern", "z%");
                final long freshOpens;
                try (RecordCursorFactory factory = engine.select(query, sqlExecutionContext)) {
                    HeapRowCursorFactory.testRowCursorsOpened.set(0);
                    TestUtils.assertEquals(expectedNarrow, printFactory(factory));
                    freshOpens = HeapRowCursorFactory.testRowCursorsOpened.get();
                }
                Assert.assertTrue("the index route opened no row cursor at all", freshOpens > 0);

                // Same query on a factory that first ran 'a%' and so allocated three per-key factories:
                // the narrower re-execution must not open the two it no longer needs.
                bindVariableService.setStr("pattern", "a%");
                try (RecordCursorFactory factory = engine.select(query, sqlExecutionContext)) {
                    HeapRowCursorFactory.testRowCursorsOpened.set(0);
                    TestUtils.assertEquals(expectedWide, printFactory(factory));
                    final long wideOpens = HeapRowCursorFactory.testRowCursorsOpened.get();
                    // Guards the guard: without this the whole test decays to vacuous. If 'a%' ever
                    // resolved to one key, or the route fell back to the scan delegate, the reuse
                    // assertion below would hold trivially while covering nothing. Both drains walk
                    // the same table and therefore the same page frames, and the counter adds the
                    // live key count once per frame, so the ratio is exactly the key ratio: three
                    // keys ('aa', 'ab', 'ac') against one ('zz').
                    Assert.assertEquals(
                            "the wide pattern did not open one row cursor per matched key",
                            3 * freshOpens,
                            wideOpens
                    );
                    bindVariableService.setStr("pattern", "z%");
                    HeapRowCursorFactory.testRowCursorsOpened.set(0);
                    TestUtils.assertEquals(expectedNarrow, printFactory(factory));
                    Assert.assertEquals(freshOpens, HeapRowCursorFactory.testRowCursorsOpened.get());
                }
            } finally {
                HeapRowCursorFactory.isRowCursorCounterEnabled = false;
                HeapRowCursorFactory.testRowCursorsOpened.set(0);
            }
        });
    }

    @Test
    public void testHintConstantWiring() {
        // A plain string-equality check on the constant is tautological: it would still pass if
        // SqlHints never consulted the constant. Assert the real wiring instead -- that a model
        // carrying the hint is detected by hasNoSymbolPatternIndexHint(), and a model without it is not.
        final QueryModel withHint = QueryModel.FACTORY.newInstance();
        withHint.addHint(io.questdb.griffin.SqlHints.NO_SYMBOL_PATTERN_INDEX_HINT, "");
        Assert.assertTrue(io.questdb.griffin.SqlHints.hasNoSymbolPatternIndexHint(withHint));

        final QueryModel withoutHint = QueryModel.FACTORY.newInstance();
        Assert.assertFalse(io.questdb.griffin.SqlHints.hasNoSymbolPatternIndexHint(withoutHint));
    }

    @Test
    public void testConfigDefaults() {
        Assert.assertTrue(configuration.isSymbolPatternIndexEnabled());
        Assert.assertEquals(100, configuration.getSymbolPatternIndexThreshold());
    }

    // M-A: cairo.sql.symbol.pattern.index.enabled=false must disable the whole fast path (SqlCodeGenerator
    // gates on isSymbolPatternIndexEnabled()) -> the pattern falls back to scan+filter, plan has no
    // "SymbolPatternIndex". A regression ignoring the config key would ship green without this.
    @Test
    public void testDisabledConfigRevertsToScanFilter() throws Exception {
        node1.setProperty(PropertyKey.CAIRO_SQL_SYMBOL_PATTERN_INDEX_ENABLED, "false");
        assertMemoryLeak(() -> {
            execute("create table t (sym symbol index, v long, ts timestamp) timestamp(ts) partition by day");
            execute("insert into t select rnd_symbol('AA','AB','BA'), x, timestamp_sequence(0, 60000000) from long_sequence(200)");
            assertQuery("select sym, v from t where sym like 'A%'").noLeakCheck().assertsPlanNotContaining("SymbolPatternIndex");
        });
    }

    // M-A: a NON-default threshold (cairo.sql.symbol.pattern.index.threshold) must be honored -- with
    // threshold=2, three matched keys (> 2) route to the fallback scan+filter, where the default 100 would
    // use the index. Proven via the fast/fallback invocation counters, not just row parity.
    @Test
    public void testCustomThresholdConfigForcesFallback() throws Exception {
        node1.setProperty(PropertyKey.CAIRO_SQL_SYMBOL_PATTERN_INDEX_THRESHOLD, "2");
        assertMemoryLeak(() -> {
            execute("create table t (sym symbol index, v long, ts timestamp) timestamp(ts) partition by day");
            // Three cold matching keys require three posting probes. The configured budget of two
            // conservatively selects the scan even though the matching-row estimate is selective.
            execute("INSERT INTO t VALUES ('AA', 1, 0), ('AB', 2, 1), ('AC', 3, 2)");
            execute("INSERT INTO t SELECT 'BA', x, timestamp_sequence(3, 1) FROM long_sequence(100)");
            String expected = select("SELECT /*+ no_symbol_pattern_index(t) */ sym, v FROM t WHERE sym LIKE 'A%' ORDER BY v");
            SymbolPatternIndexRecordCursorFactory.resetTestCounters();
            String actual = select("SELECT sym, v FROM t WHERE sym LIKE 'A%' ORDER BY v");
            io.questdb.test.tools.TestUtils.assertEquals(expected, actual);
            Assert.assertTrue(
                    "custom threshold=2 must force the fallback for 3 matched keys, got fallback="
                            + SymbolPatternIndexRecordCursorFactory.testFallbackInvocations.get(),
                    SymbolPatternIndexRecordCursorFactory.testFallbackInvocations.get() > 0
            );
            Assert.assertEquals(
                    "index path must not fire under threshold=2 with 3 matched keys",
                    0,
                    SymbolPatternIndexRecordCursorFactory.testIndexInvocations.get()
            );
        });
    }

    @Test
    public void testCustomThresholdExactBoundaryUsesIndex() throws Exception {
        node1.setProperty(PropertyKey.CAIRO_SQL_SYMBOL_PATTERN_INDEX_THRESHOLD, "2");
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (sym SYMBOL INDEX, v LONG, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("INSERT INTO t VALUES ('AA', 1, 0), ('AB', 2, 1)");
            execute("INSERT INTO t SELECT 'BA', x, timestamp_sequence(2, 1) FROM long_sequence(100)");

            SymbolPatternIndexRecordCursorFactory.resetTestCounters();
            select("SELECT sym, v FROM t WHERE sym LIKE 'A%' ORDER BY v");
            Assert.assertEquals(0, SymbolPatternIndexRecordCursorFactory.testFallbackInvocations.get());
            Assert.assertTrue(SymbolPatternIndexRecordCursorFactory.testIndexInvocations.get() > 0);

            execute("CREATE TABLE t2 (sym SYMBOL INDEX, v LONG, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("INSERT INTO t2 VALUES ('BA', 1, 0), ('BB', 2, 1)");
            execute("INSERT INTO t2 SELECT 'AA', x, timestamp_sequence(2, 1) FROM long_sequence(100)");
            SymbolPatternIndexRecordCursorFactory.resetTestCounters();
            select("SELECT sym, v FROM t2 WHERE sym NOT LIKE 'A%' ORDER BY v");
            Assert.assertEquals(0, SymbolPatternIndexRecordCursorFactory.testFallbackInvocations.get());
            Assert.assertTrue(SymbolPatternIndexRecordCursorFactory.testIndexInvocations.get() > 0);
        });
    }

    // Pins the exact matching-row selectivity boundary in AdaptiveSymbolPatternRecordCursorFactory:
    // the cost cutoff is `matchedRows > maxIndexRows` with
    // maxIndexRows = max(1, totalRows / MAX_INDEX_ROUTE_ROW_SHARE_DIVISOR).
    // At the boundary (matchedRows == maxIndexRows) the estimate is still selective and must use the
    // index; one matched row past it must fall back to scan. A regression flipping `>` to `>=` would
    // route the boundary case to the scan and this test would fail. Single matched key keeps the probe
    // count at one, so the default probe budget (100) is never the deciding factor here.
    @Test
    public void testSelectivityBoundaryAtAdmittedShareUsesIndexThenScan() throws Exception {
        assertMemoryLeak(() -> {
            // 100 rows total -> maxIndexRows = 5. Key 'A' has exactly 5 rows == the boundary.
            execute("CREATE TABLE t (sym SYMBOL INDEX, v LONG, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("INSERT INTO t SELECT 'A', x, timestamp_sequence(0, 1) FROM long_sequence(5)");
            execute("INSERT INTO t SELECT 'B', x, timestamp_sequence(5, 1) FROM long_sequence(95)");

            SymbolPatternIndexRecordCursorFactory.resetTestCounters();
            select("SELECT sym, v FROM t WHERE sym LIKE 'A%' ORDER BY v");
            Assert.assertEquals(
                    "matchedRows == maxIndexRows is still selective and must use the index",
                    0,
                    SymbolPatternIndexRecordCursorFactory.testFallbackInvocations.get()
            );
            Assert.assertTrue(SymbolPatternIndexRecordCursorFactory.testIndexInvocations.get() > 0);

            // 100 rows total -> maxIndexRows = 5. Key 'A' has 6 rows, one past the boundary -> scan.
            execute("CREATE TABLE t2 (sym SYMBOL INDEX, v LONG, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("INSERT INTO t2 SELECT 'A', x, timestamp_sequence(0, 1) FROM long_sequence(6)");
            execute("INSERT INTO t2 SELECT 'B', x, timestamp_sequence(6, 1) FROM long_sequence(94)");

            SymbolPatternIndexRecordCursorFactory.resetTestCounters();
            select("SELECT sym, v FROM t2 WHERE sym LIKE 'A%' ORDER BY v");
            Assert.assertEquals(
                    "matchedRows one past maxIndexRows must fall back to scan",
                    0,
                    SymbolPatternIndexRecordCursorFactory.testIndexInvocations.get()
            );
            Assert.assertTrue(SymbolPatternIndexRecordCursorFactory.testFallbackInvocations.get() > 0);
        });
    }

    /**
     * {@link BitmapIndexFwdReader#countMatchesInRange} is the estimator's exact-count primitive, so a
     * count that is merely close is a wrong-plan bug. The key entry's stored value count covers the
     * key's whole posting list in the partition, which equals the range count only for a range that
     * covers all of it; a partition frame narrowed by an interval scan or a transaction boundary is
     * not that. This drives every key against a spread of sub-ranges -- inside one value block, on
     * block boundaries, straddling both ends, empty, and past the end of the data -- and requires the
     * count to equal what draining the cursor over the identical range yields.
     */
    private void assertBitmapExactCountMatchesCursor(String tableName) {
        try (TableReader reader = engine.getReader(tableName)) {
            final long partitionRows = reader.openPartition(0);
            Assert.assertTrue("the fixture must land in a single partition", partitionRows == reader.size());
            final int columnIndex = reader.getMetadata().getColumnIndex("sym");
            final BitmapIndexFwdReader index =
                    (BitmapIndexFwdReader) reader.getIndexReader(0, columnIndex, IndexReader.DIR_FORWARD);
            // 499/500/501 straddle t2's columnTop and 1_499/1_500/1_501 the first row of its trailing
            // NULL batch, so key 0 gets ranges that carry a null prefix and real postings together, one
            // of the two alone, and neither. Both are in range for t as well, where they are ordinary
            // interior bounds.
            final long[] bounds = {
                    0, 1, 255, 256, 257, 499, 500, 501, 511, 512,
                    1_499, 1_500, 1_501, partitionRows / 2, partitionRows - 1, partitionRows, Long.MAX_VALUE
            };
            long comparisons = 0;
            for (int key = 0, keyCount = index.getKeyCount(); key < keyCount; key++) {
                for (long lo : bounds) {
                    if (lo == Long.MAX_VALUE) {
                        continue;
                    }
                    for (long hi : bounds) {
                        long viaCursor = 0;
                        try (RowCursor rowCursor = index.getCursor(key, lo, hi)) {
                            while (rowCursor.hasNext()) {
                                rowCursor.next();
                                viaCursor++;
                            }
                        }
                        Assert.assertEquals(
                                tableName + " key=" + key + " lo=" + lo + " hi=" + hi,
                                viaCursor,
                                index.countMatchesInRange(key, lo, hi)
                        );
                        comparisons++;
                    }
                }
            }
            Assert.assertTrue("the fixture produced no keys to compare", comparisons > 0);
        }
    }

    @Test
    public void testBitmapExactCountMatchesCursorOverSubRanges() throws Exception {
        assertMemoryLeak(() -> {
            // Seven keys over 3_000 rows puts several hundred postings on each, so the seeks have to
            // cross value blocks (default block size 256) instead of resolving inside the first one.
            execute("CREATE TABLE t (sym SYMBOL INDEX, v LONG, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("INSERT INTO t SELECT 'k' || (x % 7), x, timestamp_sequence(0, 1_000) FROM long_sequence(3_000)");
            assertBitmapExactCountMatchesCursor("t");

            // Column added after the fact: rows below columnTop carry no index entry at all and the
            // cursor synthesizes them for the NULL key, so the count has to add the same prefix. The
            // trailing NULL batch then gives key 0 real postings ABOVE columnTop as well, which is the
            // only shape where the prefix term and the posting count have to compose -- exactly what
            // NullCursor does when it exhausts its synthetic ids and falls through to Cursor.hasNext().
            // Without it a count that returned the prefix alone for key 0 would still pass.
            execute("CREATE TABLE t2 (v LONG, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("INSERT INTO t2 SELECT x, timestamp_sequence(0, 1_000) FROM long_sequence(500)");
            execute("ALTER TABLE t2 ADD COLUMN sym SYMBOL INDEX");
            execute("INSERT INTO t2 SELECT 500 + x, timestamp_sequence(500_000, 1_000), 'k' || (x % 3) FROM long_sequence(1_000)");
            execute("INSERT INTO t2 SELECT 1_500 + x, timestamp_sequence(1_500_000, 1_000), NULL::SYMBOL FROM long_sequence(200)");
            assertBitmapExactCountMatchesCursor("t2");
        });
    }

    /**
     * The per-open selectivity estimate must cost O(1) metadata reads per key and partition, never a
     * walk of the index. Row counts cannot tell the two apart -- both produce the same answer -- so
     * this counts the index entries the estimator itself consumed. On the default BITMAP symbol index
     * the estimate resolves from the key entry's stored value count plus the two block seeks the
     * cursor would do anyway, so the count must be exactly zero.
     * <p>
     * The pattern deliberately matches half the table: that is the shape where the estimate has to
     * reject the index route, and where a walk is most expensive, since it runs to the whole
     * {@code maxIndexRows} budget before the rejection and does so before the caller sees a row.
     */
    @Test
    public void testEstimatorReadsNoBitmapIndexEntries() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (sym SYMBOL INDEX, v LONG, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("INSERT INTO t SELECT 'c1', x, timestamp_sequence(0, 1_000) FROM long_sequence(10_000)");
            execute("INSERT INTO t SELECT 'zz', 10_000 + x, timestamp_sequence(10_000_000, 1_000) FROM long_sequence(10_000)");

            final String query = "SELECT sym, v FROM t WHERE sym LIKE 'c%' ORDER BY v";
            final String expected = select("SELECT /*+ no_symbol_pattern_index(t) */ sym, v FROM t WHERE sym LIKE 'c%' ORDER BY v");

            AdaptiveSymbolPatternRecordCursorFactory.resetTestCounters();
            SymbolPatternIndexRecordCursorFactory.resetTestCounters();
            AdaptiveSymbolPatternRecordCursorFactory.isEstimatorCounterEnabled = true;
            try {
                TestUtils.assertEquals(expected, select(query));
                Assert.assertEquals(
                        "the estimate must not walk bitmap index entries",
                        0,
                        AdaptiveSymbolPatternRecordCursorFactory.testEstimatorIndexEntryReads.get()
                );
                // Guards the guard: a shape that never reached the estimate would also read zero
                // entries. Half the table is far past the admitted share, so the estimate must have
                // run and rejected the index route.
                Assert.assertTrue(
                        "the broad pattern must have been costed and routed to the scan",
                        SymbolPatternIndexRecordCursorFactory.testFallbackInvocations.get() > 0
                );
                Assert.assertEquals(
                        "the index route must not fire for a pattern matching half the table",
                        0,
                        SymbolPatternIndexRecordCursorFactory.testIndexInvocations.get()
                );
            } finally {
                AdaptiveSymbolPatternRecordCursorFactory.isEstimatorCounterEnabled = false;
                AdaptiveSymbolPatternRecordCursorFactory.resetTestCounters();
                SymbolPatternIndexRecordCursorFactory.resetTestCounters();
            }
        });
    }

    /**
     * Execution-mode coverage for the estimate. {@code matchedRows} accumulates across every frame
     * the cursor yields, so a per-partition count that was not partition-local, or that leaked one
     * partition's postings into another's frame, would mis-add here and pick the wrong route. The
     * non-partitioned table pins the single-frame shape, and the interval-restricted shape pins the
     * unknown-size case: an interval frame cursor reports {@code size() == -1}, so the estimate takes
     * its denominator from the frames it walks anyway rather than rejecting the route outright, and
     * it must reach that denominator without walking a single index entry.
     */
    @Test
    public void testEstimatorAcrossPartitionsAndIntervalFrames() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (sym SYMBOL INDEX, v LONG, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            // Five daily partitions of 1_000 rows: 'a%' is 1% of the table, 'b%' is 19%.
            execute("""
                    INSERT INTO t SELECT
                      CASE WHEN x % 100 = 0 THEN 'a1' WHEN x % 5 = 0 THEN 'b1' ELSE 'z1' END,
                      x,
                      timestamp_sequence('2024-01-01T00:00:00.000000Z', 86_400_000)
                    FROM long_sequence(5_000)""");

            AdaptiveSymbolPatternRecordCursorFactory.resetTestCounters();
            AdaptiveSymbolPatternRecordCursorFactory.isEstimatorCounterEnabled = true;
            try {
                SymbolPatternIndexRecordCursorFactory.resetTestCounters();
                TestUtils.assertEquals(
                        select("SELECT /*+ no_symbol_pattern_index(t) */ sym, v FROM t WHERE sym LIKE 'a%' ORDER BY v"),
                        select("SELECT sym, v FROM t WHERE sym LIKE 'a%' ORDER BY v")
                );
                Assert.assertTrue(
                        "1% spread over five partitions must still use the index",
                        SymbolPatternIndexRecordCursorFactory.testIndexInvocations.get() > 0
                );

                SymbolPatternIndexRecordCursorFactory.resetTestCounters();
                TestUtils.assertEquals(
                        select("SELECT /*+ no_symbol_pattern_index(t) */ sym, v FROM t WHERE sym LIKE 'b%' ORDER BY v"),
                        select("SELECT sym, v FROM t WHERE sym LIKE 'b%' ORDER BY v")
                );
                Assert.assertTrue(
                        "19% summed over five partitions must fall back to the scan",
                        SymbolPatternIndexRecordCursorFactory.testFallbackInvocations.get() > 0
                );

                // Interval shape: the frame cursor reports size() == -1, so the estimate counts the
                // selected rows off the frames instead. The route is not pinned here -- the interval
                // regime has its own tests -- but the rows must still agree and no index entry may be
                // walked to decide it.
                TestUtils.assertEquals(
                        select("SELECT /*+ no_symbol_pattern_index(t) */ sym, v FROM t WHERE sym LIKE 'a%' AND ts IN '2024-01-03' ORDER BY v"),
                        select("SELECT sym, v FROM t WHERE sym LIKE 'a%' AND ts IN '2024-01-03' ORDER BY v")
                );

                // Non-partitioned table: one frame, one partition index, no accumulation at all.
                execute("CREATE TABLE tn (sym SYMBOL INDEX, v LONG)");
                execute("INSERT INTO tn SELECT CASE WHEN x % 100 = 0 THEN 'a1' ELSE 'z1' END, x FROM long_sequence(5_000)");
                SymbolPatternIndexRecordCursorFactory.resetTestCounters();
                TestUtils.assertEquals(
                        select("SELECT /*+ no_symbol_pattern_index(tn) */ sym, v FROM tn WHERE sym LIKE 'a%' ORDER BY v"),
                        select("SELECT sym, v FROM tn WHERE sym LIKE 'a%' ORDER BY v")
                );
                Assert.assertTrue(
                        "1% of a non-partitioned table must use the index",
                        SymbolPatternIndexRecordCursorFactory.testIndexInvocations.get() > 0
                );

                Assert.assertEquals(
                        "no execution mode may make the estimate walk index entries",
                        0,
                        AdaptiveSymbolPatternRecordCursorFactory.testEstimatorIndexEntryReads.get()
                );
            } finally {
                AdaptiveSymbolPatternRecordCursorFactory.isEstimatorCounterEnabled = false;
                AdaptiveSymbolPatternRecordCursorFactory.resetTestCounters();
                SymbolPatternIndexRecordCursorFactory.resetTestCounters();
            }
        });
    }

    /**
     * The admitted share of matched rows must sit below the measured crossover with the parallel
     * scan, not above it. Benchmarks on a 2M-row table put that crossover at 8-10% of rows for a
     * bare filter: at 4% the index route is still ~1.8x faster, at 10% it is ~1.3x slower. The
     * estimator admits up to {@code totalRows / 20}, so a pattern matching 1% of rows takes the
     * index and one matching 10% must not.
     */
    @Test
    public void testEstimatorRejectsSharesAboveMeasuredCrossover() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (sym SYMBOL INDEX, v LONG, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            // 10_000 rows: 'a%' matches 100 (1%), 'b%' matches 1_000 (10%).
            execute("INSERT INTO t SELECT 'a1', x, timestamp_sequence(0, 1_000) FROM long_sequence(100)");
            execute("INSERT INTO t SELECT 'b1', 100 + x, timestamp_sequence(100_000, 1_000) FROM long_sequence(1_000)");
            execute("INSERT INTO t SELECT 'zz', 1_100 + x, timestamp_sequence(1_100_000, 1_000) FROM long_sequence(8_900)");

            SymbolPatternIndexRecordCursorFactory.resetTestCounters();
            TestUtils.assertEquals(
                    select("SELECT /*+ no_symbol_pattern_index(t) */ sym, v FROM t WHERE sym LIKE 'a%' ORDER BY v"),
                    select("SELECT sym, v FROM t WHERE sym LIKE 'a%' ORDER BY v")
            );
            Assert.assertTrue(
                    "1% of rows is well inside the admitted share and must use the index",
                    SymbolPatternIndexRecordCursorFactory.testIndexInvocations.get() > 0
            );
            Assert.assertEquals(0, SymbolPatternIndexRecordCursorFactory.testFallbackInvocations.get());

            SymbolPatternIndexRecordCursorFactory.resetTestCounters();
            TestUtils.assertEquals(
                    select("SELECT /*+ no_symbol_pattern_index(t) */ sym, v FROM t WHERE sym LIKE 'b%' ORDER BY v"),
                    select("SELECT sym, v FROM t WHERE sym LIKE 'b%' ORDER BY v")
            );
            Assert.assertTrue(
                    "10% of rows is past the measured crossover and must fall back to the scan",
                    SymbolPatternIndexRecordCursorFactory.testFallbackInvocations.get() > 0
            );
            Assert.assertEquals(0, SymbolPatternIndexRecordCursorFactory.testIndexInvocations.get());
        });
    }

    /**
     * The covering route and the index route do not share an admission threshold, and must not: the
     * covering route reads a narrow projection out of the posting index but produces its page frames on
     * the opening thread, so only the filter above it scales with the shared query workers while the
     * fallback scan scales end to end. Benchmarks on a 2M-row table put the covering route's crossover
     * with that scan at ~4% of rows on 4 workers and ~2.5-3% on 8, against 8-10% for the index route,
     * so one constant sized for the index route admits the covering route across a band where it is
     * measurably slower -- 1.25x at 5% and 1.90x at 8% on 4 workers, 2.0x at 5% on 8.
     * <p>
     * This pins the split rather than either number: 2% of a covered table takes the covering route,
     * 4% falls back to the scan, and the very same 4% on a bitmap index still takes the index route.
     * Collapsing the two constants back into one would fail the second or the third assertion.
     */
    @Test
    public void testCoveringRouteAdmitsSmallerShareThanIndexRoute() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE tc (sym SYMBOL INDEX TYPE POSTING INCLUDE (price), price DOUBLE, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            // 10_000 rows -> the covering route admits 10_000/50 = 200. 'a%' matches exactly that, 'b%' twice it.
            execute("INSERT INTO tc SELECT 'a1', x::DOUBLE, timestamp_sequence(0, 1_000) FROM long_sequence(200)");
            execute("INSERT INTO tc SELECT 'b1', (200 + x)::DOUBLE, timestamp_sequence(200_000, 1_000) FROM long_sequence(400)");
            execute("INSERT INTO tc SELECT 'z1', (600 + x)::DOUBLE, timestamp_sequence(600_000, 1_000) FROM long_sequence(9_400)");

            AdaptiveSymbolPatternRecordCursorFactory.resetTestCounters();
            TestUtils.assertEquals(
                    select("SELECT /*+ no_symbol_pattern_index(tc) no_covering(tc) */ sym, price FROM tc WHERE sym LIKE 'a%' ORDER BY price"),
                    select("SELECT sym, price FROM tc WHERE sym LIKE 'a%' ORDER BY price")
            );
            Assert.assertTrue(
                    "2% of a covered table is inside the covering route's admitted share",
                    AdaptiveSymbolPatternRecordCursorFactory.testCoveringInvocations.get() > 0
            );
            Assert.assertEquals(0, AdaptiveSymbolPatternRecordCursorFactory.testScanInvocations.get());

            AdaptiveSymbolPatternRecordCursorFactory.resetTestCounters();
            TestUtils.assertEquals(
                    select("SELECT /*+ no_symbol_pattern_index(tc) no_covering(tc) */ sym, price FROM tc WHERE sym LIKE 'b%' ORDER BY price"),
                    select("SELECT sym, price FROM tc WHERE sym LIKE 'b%' ORDER BY price")
            );
            Assert.assertEquals(
                    "4% is past the covering route's measured crossover and must fall back to the scan",
                    0,
                    AdaptiveSymbolPatternRecordCursorFactory.testCoveringInvocations.get()
            );
            Assert.assertTrue(AdaptiveSymbolPatternRecordCursorFactory.testScanInvocations.get() > 0);

            // The same 4% share on a bitmap index, where no covering delegate exists, still takes the
            // index route: the two thresholds are genuinely separate, not one constant renamed.
            execute("CREATE TABLE ti (sym SYMBOL INDEX, v LONG, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("INSERT INTO ti SELECT 'b1', x, timestamp_sequence(0, 1_000) FROM long_sequence(400)");
            execute("INSERT INTO ti SELECT 'z1', 400 + x, timestamp_sequence(400_000, 1_000) FROM long_sequence(9_600)");

            SymbolPatternIndexRecordCursorFactory.resetTestCounters();
            TestUtils.assertEquals(
                    select("SELECT /*+ no_symbol_pattern_index(ti) */ sym, v FROM ti WHERE sym LIKE 'b%' ORDER BY v"),
                    select("SELECT sym, v FROM ti WHERE sym LIKE 'b%' ORDER BY v")
            );
            Assert.assertTrue(
                    "4% is still inside the index route's admitted share",
                    SymbolPatternIndexRecordCursorFactory.testIndexInvocations.get() > 0
            );
            Assert.assertEquals(0, SymbolPatternIndexRecordCursorFactory.testFallbackInvocations.get());
        });
    }

    // Regression: at the DEFAULT threshold (100), a pattern that matches MORE than 100 distinct symbol keys
    // routes to the > threshold full-scan fallback inside SymbolPatternIndexRecordCursorFactory. That fallback
    // must still apply the pattern predicate. Before the fix the fallback returned the plain, UNFILTERED full
    // scan (its PageFrameRowCursorFactory emits every row and PageFrameRecordCursorImpl never evaluates its
    // filter arg per row), so `sym like 'A%'` silently returned ALL rows -- including non-matching B-keys.
    // 200 distinct A-keys (> 100) force the fallback at default config; the count must equal the A-only rows,
    // matching the no-index scan+filter oracle, NOT the whole-table count.
    @Test
    public void testFallbackAboveThresholdStillAppliesPatternFilter() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table t (sym symbol index, v long, ts timestamp) timestamp(ts) partition by day");
            execute("insert into t select 'A' || (x%200), x, timestamp_sequence(0, 60000000) from long_sequence(6000)");
            execute("insert into t select 'B' || (x%50), x, timestamp_sequence(600000000000, 60000000) from long_sequence(3000)");

            SymbolPatternIndexRecordCursorFactory.resetTestCounters();
            long viaFastPath = countOf("select count() from t where sym like 'A%'");
            // Oracle: the ordinary scan+filter path (fast path disabled by hint).
            long viaScanFilter = countOf("select /*+ no_symbol_pattern_index(t) */ count() from t where sym like 'A%'");

            Assert.assertEquals("fallback must match the scan+filter oracle (A-only rows)", viaScanFilter, viaFastPath);
            Assert.assertEquals("only the 6000 A-prefixed rows match 'A%'", 6000, viaFastPath);
            Assert.assertTrue(
                    "200 matched keys (> default threshold 100) must take the fallback, got fallback="
                            + SymbolPatternIndexRecordCursorFactory.testFallbackInvocations.get(),
                    SymbolPatternIndexRecordCursorFactory.testFallbackInvocations.get() > 0
            );
            Assert.assertEquals(
                    "index path must not fire when the match set exceeds the threshold",
                    0,
                    SymbolPatternIndexRecordCursorFactory.testIndexInvocations.get()
            );
        });
    }

    // Regression companion for the NEGATED (NOT LIKE) complement path, which shares the same > threshold
    // fallback cursor. With 150 A-keys and 150 B-keys the complement of 'A%' (the NOT-LIKE match set) is
    // 150 keys > 100, forcing the fallback; it must still apply the negated predicate and return only the
    // B rows, not the whole table. Before the fix this returned every row exactly like the positive path.
    @Test
    public void testNegatedFallbackAboveThresholdStillAppliesPatternFilter() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table t (sym symbol index, v long, ts timestamp) timestamp(ts) partition by day");
            execute("insert into t select 'A' || (x%150), x, timestamp_sequence(0, 60000000) from long_sequence(4500)");
            execute("insert into t select 'B' || (x%150), x, timestamp_sequence(600000000000, 60000000) from long_sequence(3000)");

            SymbolPatternIndexRecordCursorFactory.resetTestCounters();
            long viaFastPath = countOf("select count() from t where sym not like 'A%'");
            long viaScanFilter = countOf("select /*+ no_symbol_pattern_index(t) */ count() from t where sym not like 'A%'");

            Assert.assertEquals("negated fallback must match the scan+filter oracle (B-only rows)", viaScanFilter, viaFastPath);
            Assert.assertEquals("only the 3000 B-prefixed rows satisfy NOT LIKE 'A%'", 3000, viaFastPath);
            Assert.assertTrue(
                    "complement of 150 A-keys (150 B-keys > threshold 100) must take the fallback, got fallback="
                            + SymbolPatternIndexRecordCursorFactory.testFallbackInvocations.get(),
                    SymbolPatternIndexRecordCursorFactory.testFallbackInvocations.get() > 0
            );
            Assert.assertEquals(
                    "index path must not fire when the complement exceeds the threshold",
                    0,
                    SymbolPatternIndexRecordCursorFactory.testIndexInvocations.get()
            );
        });
    }

    @Test
    public void testProviderExposesStartsWithKeys() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table t (sym symbol index, ts timestamp) timestamp(ts)");
            execute("insert into t values ('AA', 0::timestamp),('AB',1::timestamp),('BA',2::timestamp),('BB',3::timestamp)");
            // symbol keys are assigned in insertion order: AA=0, AB=1, BA=2, BB=3
            IntList keys = matchedKeys("sym like 'A%'");
            Assert.assertEquals("[0,1]", keys.toString());
        });
    }

    /**
     * End-to-end row parity: the index fast path (unhinted) must return exactly the same rows as the
     * scan+filter path forced by the opt-out hint. The hint is the ground truth; the two must agree.
     */
    @Test
    public void testStartsWithMatchesScanFilter_indexPath() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table t (sym symbol index, v long, ts timestamp) timestamp(ts) partition by day");
            execute("insert into t select rnd_symbol('AA','AB','BA','BB','AC'), x, timestamp_sequence(0, 60000000) from long_sequence(2000)");
            // Ground truth: force the scan+filter plan with the opt-out hint (hints go right after SELECT).
            String expected = select("select /*+ no_symbol_pattern_index(t) */ sym, v, ts from t where sym like 'A%' order by ts, v");
            String actual = select("select sym, v, ts from t where sym like 'A%' order by ts, v");
            io.questdb.test.tools.TestUtils.assertEquals(expected, actual);
        });
    }

    /**
     * Proves recognition fired: the unhinted plan routes through the SymbolPatternIndex fast path, while
     * the hinted (opt-out) plan does not. This is the meaningful RED-&gt;GREEN signal for the codegen branch,
     * because the row-parity test alone passes even when both queries scan+filter.
     */
    @Test
    public void testPlanUsesSymbolPatternIndex() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table t (sym symbol index, v long, ts timestamp) timestamp(ts) partition by day");
            execute("insert into t select rnd_symbol('AA','AB','BA'), x, timestamp_sequence(0, 60000000) from long_sequence(100)");
            assertQuery("select sym, v from t where sym like 'A%'").noLeakCheck().assertsPlanContaining("SymbolPatternIndex");
            assertQuery("select /*+ no_symbol_pattern_index(t) */ sym, v from t where sym like 'A%'").noLeakCheck().assertsPlanNotContaining("SymbolPatternIndex");
        });
    }

    /**
     * Residual conjunct ({@code AND v > 1000}) must be applied on the index rows: index-path result must
     * equal the scan+filter (hinted) ground truth.
     */
    @Test
    public void testResidualFilterMatchesScanFilter() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table t (sym symbol index, v long, ts timestamp) timestamp(ts) partition by day");
            execute("insert into t select rnd_symbol('AA','AB','BA','BB','AC'), x, timestamp_sequence(0, 60000000) from long_sequence(2000)");
            String expected = select("select /*+ no_symbol_pattern_index(t) */ sym, v, ts from t where sym like 'A%' and v > 1000 order by ts, v");
            String actual = select("select sym, v, ts from t where sym like 'A%' and v > 1000 order by ts, v");
            io.questdb.test.tools.TestUtils.assertEquals(expected, actual);
            assertQuery("select sym, v from t where sym like 'A%' and v > 1000").noLeakCheck().assertsPlanContaining("SymbolPatternIndex");
        });
    }

    /**
     * Regex ({@code ~}) and ILIKE variants must also route through the index and match ground truth.
     */
    @Test
    public void testRegexAndIlikeMatchScanFilter() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table t (sym symbol index, v long, ts timestamp) timestamp(ts) partition by day");
            execute("insert into t select rnd_symbol('AA','ab','BA','Bb','aC'), x, timestamp_sequence(0, 60000000) from long_sequence(1500)");
            String reExpected = select("select /*+ no_symbol_pattern_index(t) */ sym, v, ts from t where sym ~ '^A' order by ts, v");
            String reActual = select("select sym, v, ts from t where sym ~ '^A' order by ts, v");
            io.questdb.test.tools.TestUtils.assertEquals(reExpected, reActual);

            String ilExpected = select("select /*+ no_symbol_pattern_index(t) */ sym, v, ts from t where sym ilike 'a%' order by ts, v");
            String ilActual = select("select sym, v, ts from t where sym ilike 'a%' order by ts, v");
            io.questdb.test.tools.TestUtils.assertEquals(ilExpected, ilActual);
        });
    }

    /**
     * Descending designated-timestamp order flips the index scan direction; index-path result must still
     * equal the scan+filter ground truth (same rows, same DESC order).
     */
    @Test
    public void testDescOrderMatchesScanFilter() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table t (sym symbol index, v long, ts timestamp) timestamp(ts) partition by day");
            execute("insert into t select rnd_symbol('AA','AB','BA','BB','AC'), x, timestamp_sequence(0, 60000000) from long_sequence(2000)");
            String expected = select("select /*+ no_symbol_pattern_index(t) */ sym, v, ts from t where sym like 'A%' order by ts desc");
            String actual = select("select sym, v, ts from t where sym like 'A%' order by ts desc");
            io.questdb.test.tools.TestUtils.assertEquals(expected, actual);
        });
    }

    /**
     * SP2: negation ({@code NOT LIKE}) IS now lifted to the index fast path via a complement scan, and the
     * result must still equal the scan+filter ground truth (same rows, including NULL-symbol rows). The
     * opt-out hint still forces the scan+filter path.
     */
    @Test
    public void testNegationLiftedToIndex() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table t (sym symbol index, v long, ts timestamp) timestamp(ts) partition by day");
            execute("insert into t select rnd_symbol('AA','AB','BA','BB','AC'), x, timestamp_sequence(0, 60000000) from long_sequence(1500)");
            assertQuery("select sym, v from t where sym not like 'A%'").noLeakCheck().assertsPlanContaining("SymbolPatternIndex");
            assertQuery("select /*+ no_symbol_pattern_index(t) */ sym, v from t where sym not like 'A%'").noLeakCheck().assertsPlanNotContaining("SymbolPatternIndex");
            String expected = select("select /*+ no_symbol_pattern_index(t) */ sym, v, ts from t where sym not like 'A%' order by ts, v");
            String actual = select("select sym, v, ts from t where sym not like 'A%' order by ts, v");
            io.questdb.test.tools.TestUtils.assertEquals(expected, actual);
        });
    }

    /**
     * {@code no_index(t)} is the documented escape hatch that forces a full scan, and every other index
     * route in SqlCodeGenerator consults it (the LATEST BY index route, the sorted-symbol-index route,
     * the FilterOnValues route, and -- via {@link io.questdb.griffin.SqlHints#hasNoCoveringHint} ORing in
     * {@code no_index} -- the covering route). The symbol-pattern route must honour it too, otherwise
     * {@code no_index} leaves the pattern route as the ONLY surviving index path, which is the opposite
     * of what the hint promises.
     * <p>
     * Asserts on the {@code AdaptiveSymbolPattern} plan node, not on {@code SymbolPatternIndex}:
     * {@code AdaptiveSymbolPatternRecordCursorFactory.toPlan()} prints all three delegates
     * unconditionally, so only the adaptive node itself proves the route was constructed.
     */
    @Test
    public void testNoIndexHintDisablesSymbolPatternRoute() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (sym SYMBOL INDEX, v LONG, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("INSERT INTO t SELECT rnd_symbol('AA','AB','BA','BB'), x, timestamp_sequence(0, 60_000_000) FROM long_sequence(1_000)");
            execute("INSERT INTO t SELECT null, x, timestamp_sequence(1_000*60_000_000, 60_000_000) FROM long_sequence(100)");

            // control: without a hint both the positive and the negated pattern take the adaptive route
            assertQuery("SELECT sym, v FROM t WHERE sym LIKE 'A%'").noLeakCheck().assertsPlanContaining("AdaptiveSymbolPattern");
            assertQuery("SELECT sym, v FROM t WHERE sym NOT LIKE 'A%'").noLeakCheck().assertsPlanContaining("AdaptiveSymbolPattern");
            assertQuery("SELECT sym, v FROM t WHERE sym !~ '^A'").noLeakCheck().assertsPlanContaining("AdaptiveSymbolPattern");

            // positive pattern: no_index(t) must force the scan, with identical rows
            String positive = select("SELECT /*+ no_symbol_pattern_index(t) */ sym, v, ts FROM t WHERE sym LIKE 'A%' ORDER BY ts, v");
            assertQuery("SELECT /*+ no_index(t) */ sym, v, ts FROM t WHERE sym LIKE 'A%' ORDER BY ts, v")
                    .noLeakCheck()
                    .timestamp("ts")
                    .withPlanNotContaining("AdaptiveSymbolPattern")
                    .returns(positive);

            // negated pattern: a different construction path (no covering delegate) that must also honour it
            String negated = select("SELECT /*+ no_symbol_pattern_index(t) */ sym, v, ts FROM t WHERE sym NOT LIKE 'A%' ORDER BY ts, v");
            assertQuery("SELECT /*+ no_index(t) */ sym, v, ts FROM t WHERE sym NOT LIKE 'A%' ORDER BY ts, v")
                    .noLeakCheck()
                    .timestamp("ts")
                    .withPlanNotContaining("AdaptiveSymbolPattern")
                    .returns(negated);

            // binary !~ negation, which the codegen lifts through a synthesized positive '~' provider node
            String negatedRegex = select("SELECT /*+ no_symbol_pattern_index(t) */ sym, v, ts FROM t WHERE sym !~ '^A' ORDER BY ts, v");
            assertQuery("SELECT /*+ no_index(t) */ sym, v, ts FROM t WHERE sym !~ '^A' ORDER BY ts, v")
                    .noLeakCheck()
                    .timestamp("ts")
                    .withPlanNotContaining("AdaptiveSymbolPattern")
                    .returns(negatedRegex);

            // the two hints stay independent and compose: each one alone, and both together, suppress the route
            assertQuery("SELECT /*+ no_symbol_pattern_index(t) */ sym, v FROM t WHERE sym LIKE 'A%'")
                    .noLeakCheck()
                    .assertsPlanNotContaining("AdaptiveSymbolPattern");
            assertQuery("SELECT /*+ no_index(t) no_symbol_pattern_index(t) */ sym, v FROM t WHERE sym LIKE 'A%'")
                    .noLeakCheck()
                    .assertsPlanNotContaining("AdaptiveSymbolPattern");
        });
    }

    /**
     * {@code no_index(t)} must also suppress the pattern route when the posting index makes the
     * projection fully covered -- that shape reaches the covering delegate, which
     * {@link io.questdb.griffin.SqlHints#hasNoCoveringHint} already suppresses, so without the fix
     * {@code no_index} would leave the adaptive owner holding only its bitmap and scan delegates.
     */
    @Test
    public void testNoIndexHintDisablesSymbolPatternRouteOnCoveredProjection() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (sym SYMBOL INDEX TYPE POSTING INCLUDE (price), price DOUBLE, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("INSERT INTO t SELECT rnd_symbol('AA','AB','BA'), x::DOUBLE, timestamp_sequence(0, 60_000_000) FROM long_sequence(500)");

            assertQuery("SELECT sym, price FROM t WHERE sym LIKE 'A%'").noLeakCheck().assertsPlanContaining("AdaptiveSymbolPattern");

            String expected = select("SELECT /*+ no_symbol_pattern_index(t) no_covering(t) */ sym, price, ts FROM t WHERE sym LIKE 'A%' ORDER BY ts, price");
            assertQuery("SELECT /*+ no_index(t) */ sym, price, ts FROM t WHERE sym LIKE 'A%' ORDER BY ts, price")
                    .noLeakCheck()
                    .timestamp("ts")
                    .withPlanNotContaining("AdaptiveSymbolPattern", "CoveringIndex")
                    .returns(expected);
        });
    }

    /**
     * SP2 end-to-end row parity for the NEGATED complement fast path: {@code NOT LIKE 'A%'} via the index
     * (unhinted) must return byte-identical rows to the scan+filter ground truth (hinted), including the
     * NULL-symbol rows that {@code NOT LIKE} includes.
     */
    @Test
    public void testNotLikeMatchesScanFilter_indexPath() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table t (sym symbol index, v long, ts timestamp) timestamp(ts) partition by day");
            execute("insert into t select rnd_symbol('AA','AB','BA','BB','AC'), x, timestamp_sequence(0, 60000000) from long_sequence(2000)");
            execute("insert into t select null, x, timestamp_sequence(2000*60000000, 60000000) from long_sequence(300)");
            String expected = select("select /*+ no_symbol_pattern_index(t) */ sym, v, ts from t where sym not like 'A%' order by ts, v");
            String actual = select("select sym, v, ts from t where sym not like 'A%' order by ts, v");
            io.questdb.test.tools.TestUtils.assertEquals(expected, actual);
        });
    }

    /**
     * SP2 recognition proof for negation: the unhinted {@code NOT LIKE} plan routes through the
     * SymbolPatternIndex complement fast path, while the hinted (opt-out) plan does not. This is the
     * meaningful RED-&gt;GREEN signal for the negated codegen branch.
     */
    @Test
    public void testNotLikePlanUsesSymbolPatternIndex() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table t (sym symbol index, v long, ts timestamp) timestamp(ts) partition by day");
            execute("insert into t select rnd_symbol('AA','AB','BA'), x, timestamp_sequence(0, 60000000) from long_sequence(100)");
            assertQuery("select sym, v from t where sym not like 'A%'").noLeakCheck().assertsPlanContaining("SymbolPatternIndex");
            assertQuery("select /*+ no_symbol_pattern_index(t) */ sym, v from t where sym not like 'A%'").noLeakCheck().assertsPlanNotContaining("SymbolPatternIndex");
        });
    }

    /**
     * SP2 Case B: the binary {@code !~} operator (which parses as a single binary node, not {@code not(~)})
     * must also be lifted to the complement fast path via a synthesized positive {@code ~} provider node.
     * Asserts both recognition (plan routes through / opts out of SymbolPatternIndex) and row parity —
     * including NULL-symbol rows — against the scan+filter ground truth.
     */
    @Test
    public void testNotRegexMatchesScanFilter_indexPath() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table t (sym symbol index, v long, ts timestamp) timestamp(ts) partition by day");
            execute("insert into t select rnd_symbol('AA','AB','BA','BB','AC'), x, timestamp_sequence(0, 60000000) from long_sequence(2000)");
            execute("insert into t select null, x, timestamp_sequence(2000*60000000, 60000000) from long_sequence(300)");
            assertQuery("select sym, v from t where sym !~ '^A'").noLeakCheck().assertsPlanContaining("SymbolPatternIndex");
            assertQuery("select /*+ no_symbol_pattern_index(t) */ sym, v from t where sym !~ '^A'").noLeakCheck().assertsPlanNotContaining("SymbolPatternIndex");
            String expected = select("select /*+ no_symbol_pattern_index(t) */ sym, v, ts from t where sym !~ '^A' order by ts, v");
            String actual = select("select sym, v, ts from t where sym !~ '^A' order by ts, v");
            io.questdb.test.tools.TestUtils.assertEquals(expected, actual);
        });
    }

    /**
     * The cheaper SequentialRowCursorFactory ("Cursor-order scan") drains one symbol key at a time, so
     * its rows are not in designated-timestamp order. Like FilterOnValuesRecordCursorFactory, the index
     * route may pick it only when the model declares its row order invariant: an outer ORDER BY that
     * re-sorts anyway, or an aggregation that ignores row order.
     */
    @Test
    public void testOrderInvariantModelUsesSequentialScan() throws Exception {
        assertMemoryLeak(() -> {
            createKeyOrderVersusTimestampOrderFixture();
            assertQuery("SELECT sym, v, ts FROM t WHERE sym LIKE 'a%' ORDER BY v")
                    .withPlanContaining("Cursor-order scan")
                    .returns("sym\tv\tts\n" +
                            "ab\t1\t2024-01-01T00:00:00.000000Z\n" +
                            "aa\t2\t2024-01-01T01:00:00.000000Z\n");
            assertQuery("SELECT sum(v) FROM t WHERE sym LIKE 'a%'")
                    .noRandomAccess()
                    .expectSize()
                    .withPlanContaining("Cursor-order scan")
                    .returns("sum\n3\n");
        });
    }

    /**
     * A query with no ORDER BY still promises rows in designated-timestamp order, so the index route
     * must merge its per-key cursors ("Table-order scan"). The fixture assigns symbol key 0 to the row
     * with the LATER timestamp, so a key-at-a-time drain would emit 'aa' before 'ab' -- the LIMIT case
     * would then return the wrong rows outright, not merely a different order.
     */
    @Test
    public void testUnorderedScanStaysTimestampOrdered() throws Exception {
        assertMemoryLeak(() -> {
            createKeyOrderVersusTimestampOrderFixture();
            assertQuery("SELECT sym, v, ts FROM t WHERE sym LIKE 'a%'")
                    .timestamp("ts")
                    .withPlanContaining("Table-order scan")
                    .returns("sym\tv\tts\n" +
                            "ab\t1\t2024-01-01T00:00:00.000000Z\n" +
                            "aa\t2\t2024-01-01T01:00:00.000000Z\n");
            assertQuery("SELECT sym, v, ts FROM t WHERE sym LIKE 'a%' LIMIT 1")
                    .timestamp("ts")
                    .withPlanContaining("Table-order scan")
                    .returns("sym\tv\tts\n" +
                            "ab\t1\t2024-01-01T00:00:00.000000Z\n");
        });
    }

    /**
     * The reproducer filed against this PR: {@code WHERE sym LIKE ... LIMIT N} must return the same rows
     * as the same statement without the index route. The two matched keys interleave in time, so a
     * key-at-a-time drain would take all four 'a12' rows instead of the first four rows in
     * designated-timestamp order -- a different ROW SET, not merely a different order, and the web
     * console appends LIMIT to every query. Pinned against two independent ground truths: the
     * no_symbol_pattern_index scan+filter oracle, and the equivalent IN list, whose established index
     * route this one is required to match.
     */
    @Test
    public void testPatternLimitWithoutOrderByReturnsScanFilterRows() throws Exception {
        assertMemoryLeak(() -> {
            createInterleavedKeyFixture();
            final String firstFourInTimestampOrder = """
                    sym	v
                    a12	12
                    a0	24
                    a12	36
                    a0	48
                    """;
            assertQuery("SELECT sym, v FROM o WHERE sym LIKE 'a%' LIMIT 4")
                    .withPlanContaining("Table-order scan")
                    .returns(firstFourInTimestampOrder);
            assertQuery("SELECT /*+ no_symbol_pattern_index(o) */ sym, v FROM o WHERE sym LIKE 'a%' LIMIT 4")
                    .withPlanNotContaining("SymbolPatternIndex")
                    .returns(firstFourInTimestampOrder);
            assertQuery("SELECT sym, v FROM o WHERE sym IN ('a0','a12') LIMIT 4")
                    .returns(firstFourInTimestampOrder);

            // Without LIMIT the row set already agreed; what the per-key drain changed was the emission
            // order (a12,a12,a12,a12,a0,a0,a0,a0 instead of a12,a0,a12,a0,...). Pin the order too.
            final String allInTimestampOrder = """
                    sym	v
                    a12	12
                    a0	24
                    a12	36
                    a0	48
                    a12	60
                    a0	72
                    a12	84
                    a0	96
                    """;
            assertQuery("SELECT sym, v FROM o WHERE sym LIKE 'a%'")
                    .withPlanContaining("Table-order scan")
                    .returns(allInTimestampOrder);
            assertQuery("SELECT /*+ no_symbol_pattern_index(o) */ sym, v FROM o WHERE sym LIKE 'a%'")
                    .withPlanNotContaining("SymbolPatternIndex")
                    .returns(allInTimestampOrder);
        });
    }

    /**
     * Every 12th row takes a symbol matching 'a%', alternating between 'a12' and 'a0', so the two
     * matched keys interleave in designated-timestamp order. The 'z%' rows keep the pattern selective
     * enough for the adaptive factory to choose an index delegate.
     */
    private void createInterleavedKeyFixture() throws SqlException {
        execute("CREATE TABLE o (sym SYMBOL INDEX, v LONG, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY YEAR");
        execute("""
                INSERT INTO o SELECT CASE WHEN x % 12 = 0 THEN 'a' || (x % 24) ELSE 'z' || (x % 7) END, x,
                  timestamp_sequence('2024-01-01T00:00:00.000000Z', 60_000_000L) FROM long_sequence(100)""");
    }

    /**
     * Symbol keys are assigned in insertion order, so key 0 is 'aa' at 01:00 and key 1 is 'ab' at 00:00:
     * key order is the reverse of timestamp order. The 'bb' rows keep the pattern selective enough for
     * the adaptive factory to choose an index delegate.
     */
    private void createKeyOrderVersusTimestampOrderFixture() throws SqlException {
        execute("CREATE TABLE t (sym SYMBOL INDEX, v LONG, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
        execute("INSERT INTO t VALUES ('aa', 2, '2024-01-01T01:00:00.000000Z'), ('ab', 1, '2024-01-01T00:00:00.000000Z')");
        execute("INSERT INTO t SELECT 'bb', x, timestamp_sequence('2024-01-01T02:00:00.000000Z', 1_000_000) FROM long_sequence(1_000)");
    }

    /**
     * For all orderings (none, ASC ts, DESC ts) the fast-path must return exactly the same rows as the
     * hinted scan+filter ground truth. Hint goes right after SELECT (a WHERE-clause hint is a silent no-op).
     * For the unordered case ("") both queries are given a stable tiebreaker sort so that the comparison
     * is deterministic regardless of cursor-order differences between the index and full-scan paths.
     */
    @Test
    public void testOrderByTimestampParity() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table t (sym symbol index, v long, ts timestamp) timestamp(ts) partition by day");
            execute("insert into t select rnd_symbol('AA','AB','BA','BB'), x, timestamp_sequence(0, 60000000) from long_sequence(3000)");
            final ObjList<String> orders = new ObjList<>("order by ts", "order by ts desc");
            for (int i = 0, n = orders.size(); i < n; i++) {
                final String order = orders.getQuick(i);
                String fastPath = "select sym, v, ts from t where sym like 'A%%' " + order;
                String hinted = "select /*+ no_symbol_pattern_index(t) */ sym, v, ts from t where sym like 'A%%' " + order;
                String expected = select(hinted.trim());
                String actual = select(fastPath.trim());
                io.questdb.test.tools.TestUtils.assertEquals("order=[" + order + "]", expected, actual);
            }
            // Unordered: apply a stable sort on both sides so the row-set comparison is deterministic.
            String fastPath = "select sym, v, ts from t where sym like 'A%%' order by ts, sym, v";
            String hinted = "select /*+ no_symbol_pattern_index(t) */ sym, v, ts from t where sym like 'A%%' order by ts, sym, v";
            io.questdb.test.tools.TestUtils.assertEquals("order=[unordered-stabilised]", select(hinted), select(fastPath));
        });
    }

    /**
     * When matched-key count exceeds the default threshold (100), the factory must fall back to a
     * full scan+filter cursor. We trigger this by inserting 150 distinct symbols that all match the
     * pattern {@code 'A%'}, giving 150 matched keys &gt; 100. The fallback counter must increment and
     * the index counter must stay at zero; rows must match the hinted scan+filter ground truth.
     * <p>
     * The table ALSO holds non-matching B-keys so the ground-truth comparison is not vacuous: a
     * fallback that dropped the pattern predicate (and returned the whole table) would fail here.
     */
    @Test
    public void testHighSelectivityFallsBackToScan() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table t (sym symbol index, v long, ts timestamp) timestamp(ts) partition by day");
            // 'A' || (x % 150) produces A0..A149 = 150 distinct symbols, all matching 'A%'.
            // 150 > default threshold (100), so the factory must choose the fallback path.
            execute("insert into t select cast('A' || (x % 150) as symbol), x, timestamp_sequence(0, 60000000) from long_sequence(1500)");
            // Non-matching rows: without these every row matches 'A%' and a dropped filter is undetectable.
            execute("insert into t select cast('B' || (x % 40) as symbol), x, timestamp_sequence(600000000000, 60000000) from long_sequence(800)");
            // Ground truth: force scan+filter with the opt-out hint immediately after SELECT.
            String expected = select("select /*+ no_symbol_pattern_index(t) */ sym, count() from t where sym like 'A%' order by sym");
            SymbolPatternIndexRecordCursorFactory.resetTestCounters();
            String actual = select("select sym, count() from t where sym like 'A%' order by sym");
            io.questdb.test.tools.TestUtils.assertEquals(expected, actual);
            // Prove the fallback branch actually fired (not just that rows are correct).
            Assert.assertTrue(
                    "expected fallbackInvocations > 0, got " + SymbolPatternIndexRecordCursorFactory.testFallbackInvocations.get(),
                    SymbolPatternIndexRecordCursorFactory.testFallbackInvocations.get() > 0
            );
            Assert.assertEquals(
                    "expected indexInvocations == 0, got " + SymbolPatternIndexRecordCursorFactory.testIndexInvocations.get(),
                    0,
                    SymbolPatternIndexRecordCursorFactory.testIndexInvocations.get()
            );
        });
    }

    /**
     * When matched-key count is at or below the default threshold (100), the factory must use the
     * index-merge path. With only 3 matching symbol keys (AA, AB, AC out of AA/AB/BA/BB/AC), the
     * index counter must increment and the fallback counter must stay at zero.
     */
    @Test
    public void testLowSelectivityUsesIndex() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table t (sym symbol index, v long, ts timestamp) timestamp(ts) partition by day");
            execute("INSERT INTO t VALUES ('AA', 1, 0), ('AB', 2, 1), ('AC', 3, 2)");
            execute("INSERT INTO t SELECT 'BA', x, timestamp_sequence(3, 1) FROM long_sequence(2_000)");
            // Ground truth: hint goes immediately after SELECT (not in WHERE).
            String expected = select("select /*+ no_symbol_pattern_index(t) */ sym, v, ts from t where sym like 'A%' order by ts, v");
            SymbolPatternIndexRecordCursorFactory.resetTestCounters();
            String actual = select("select sym, v, ts from t where sym like 'A%' order by ts, v");
            io.questdb.test.tools.TestUtils.assertEquals(expected, actual);
            // Prove the index branch actually fired.
            Assert.assertTrue(
                    "expected indexInvocations > 0, got " + SymbolPatternIndexRecordCursorFactory.testIndexInvocations.get(),
                    SymbolPatternIndexRecordCursorFactory.testIndexInvocations.get() > 0
            );
            Assert.assertEquals(
                    "expected fallbackInvocations == 0, got " + SymbolPatternIndexRecordCursorFactory.testFallbackInvocations.get(),
                    0,
                    SymbolPatternIndexRecordCursorFactory.testFallbackInvocations.get()
            );
        });
    }

    /**
     * SP2 negated adaptive threshold: the fast-vs-fallback decision is measured on the COMPLEMENT (included)
     * size, not the matched size. With 150 distinct {@code B}-prefixed symbols and pattern {@code NOT LIKE 'A%'},
     * the positive pattern matches 0 keys but the complement is 150 &gt; 100, so the factory must fall back to
     * scan+filter (fallback counter increments, index counter stays zero) while still matching ground truth.
     */
    @Test
    public void testNegatedHighComplementFallsBackToScan() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table t (sym symbol index, v long, ts timestamp) timestamp(ts) partition by day");
            // 'B' || (x % 150) => B0..B149 = 150 distinct symbols, none matching 'A%'.
            // NOT LIKE 'A%' therefore includes all 150 keys > default threshold (100) => fallback path.
            execute("insert into t select cast('B' || (x % 150) as symbol), x, timestamp_sequence(0, 60000000) from long_sequence(1500)");
            String expected = select("select /*+ no_symbol_pattern_index(t) */ sym, count() from t where sym not like 'A%' order by sym");
            SymbolPatternIndexRecordCursorFactory.resetTestCounters();
            String actual = select("select sym, count() from t where sym not like 'A%' order by sym");
            io.questdb.test.tools.TestUtils.assertEquals(expected, actual);
            Assert.assertTrue(
                    "expected fallbackInvocations > 0, got " + SymbolPatternIndexRecordCursorFactory.testFallbackInvocations.get(),
                    SymbolPatternIndexRecordCursorFactory.testFallbackInvocations.get() > 0
            );
            Assert.assertEquals(
                    "expected indexInvocations == 0, got " + SymbolPatternIndexRecordCursorFactory.testIndexInvocations.get(),
                    0,
                    SymbolPatternIndexRecordCursorFactory.testIndexInvocations.get()
            );
        });
    }

    /**
     * SP2 negated index path: when the complement is small enough (&le; threshold) the factory uses the
     * index-merge complement scan. With 5 distinct symbols and {@code NOT LIKE 'A%'} the complement is
     * 2 keys (BA, BB) — well under 100 — so the index counter must increment and the fallback stay at zero.
     */
    @Test
    public void testNegatedLowComplementUsesIndex() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table t (sym symbol index, v long, ts timestamp) timestamp(ts) partition by day");
            execute("INSERT INTO t VALUES ('BA', 1, 0), ('BB', 2, 1)");
            execute("INSERT INTO t SELECT 'AA', x, timestamp_sequence(2, 1) FROM long_sequence(2_000)");
            String expected = select("select /*+ no_symbol_pattern_index(t) */ sym, v, ts from t where sym not like 'A%' order by ts, v");
            SymbolPatternIndexRecordCursorFactory.resetTestCounters();
            String actual = select("select sym, v, ts from t where sym not like 'A%' order by ts, v");
            io.questdb.test.tools.TestUtils.assertEquals(expected, actual);
            Assert.assertTrue(
                    "expected indexInvocations > 0, got " + SymbolPatternIndexRecordCursorFactory.testIndexInvocations.get(),
                    SymbolPatternIndexRecordCursorFactory.testIndexInvocations.get() > 0
            );
            Assert.assertEquals(
                    "expected fallbackInvocations == 0, got " + SymbolPatternIndexRecordCursorFactory.testFallbackInvocations.get(),
                    0,
                    SymbolPatternIndexRecordCursorFactory.testFallbackInvocations.get()
            );
        });
    }

    /**
     * Parity oracle sweep: for a matrix of predicate shapes the fast index path (unhinted) must return
     * byte-identical rows to the scan+filter ground truth (hint immediately after SELECT). Covers
     * LIKE, ILIKE, regex (~), underscore-escaped LIKE, residual conjunct, empty match, and a no-match
     * pattern — on a table that includes NULL symbols and multiple partitions.
     */
    @Test
    public void testParitySweep() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table t (sym symbol index, v long, ts timestamp) timestamp(ts) partition by day");
            execute("insert into t select rnd_symbol('alpha','alto','beta','ALPHA','al_x','gamma',null), x, timestamp_sequence(0, 3600000000) from long_sequence(5000)");
            final ObjList<String> predicates = new ObjList<>(
                    "sym like 'al%'",
                    "sym like '%ta'",
                    "sym like '%lph%'",
                    "sym ilike 'al%'",
                    "sym ilike 'ALPHA'",
                    "sym ~ '^al'",
                    "sym ~ 'a'",
                    "sym ~ 'zzz'",
                    "sym like 'al\\_x'",
                    "sym like 'al%' and v > 100",
                    "sym like 'no_such%'"
            );
            for (int i = 0, n = predicates.size(); i < n; i++) {
                final String p = predicates.getQuick(i);
                // Hint goes right after SELECT (a WHERE-position hint is a silent no-op).
                // Use %s as a placeholder for the hint (or empty string), then escape any real % in pred.
                String base = "select %s sym, v, ts from t where " + p.replace("%", "%%") + " order by ts, v";
                String expected = select(String.format(base, "/*+ no_symbol_pattern_index(t) */ "));
                String actual = select(String.format(base, ""));
                io.questdb.test.tools.TestUtils.assertEquals("pred=[" + p + "]", expected, actual);
            }
        });
    }

    /**
     * Ground-truth oracle for SP2: QuestDB's symbol-pattern functions evaluate a NULL symbol as no match,
     * so {@code NOT LIKE} and {@code !~} include NULL-symbol rows. The adaptive complement route must
     * reproduce that established engine behavior exactly.
     *
     * <p>Concretely: with 300 rows of {@code rnd_symbol('alpha','beta','gamma')} and 50 explicit NULL rows,
     * {@code sym NOT LIKE 'al%'} must return exactly (non-alpha non-null rows) + (null rows).
     */
    @Test
    public void testNegationIncludesNullRows_groundTruth() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table t (sym symbol index, v long, ts timestamp) timestamp(ts) partition by day");
            // 3 non-null symbols + explicit NULL symbols
            execute("insert into t select rnd_symbol('alpha','beta','gamma'), x, timestamp_sequence(0, 60000000) from long_sequence(300)");
            execute("insert into t select null, x, timestamp_sequence(300*60000000, 60000000) from long_sequence(50)");
            // Ground truth: NOT LIKE 'al%' must INCLUDE the 50 null-symbol rows (like(null)=false -> not=true).
            long notLikeCount = countOf("select count() from t where sym not like 'al%'");
            long nullCount = countOf("select count() from t where sym is null");
            long nonAlphaNonNull = countOf("select count() from t where sym is not null and sym not like 'al%'");
            Assert.assertEquals(50, nullCount);
            Assert.assertEquals("NOT LIKE must include NULL-symbol rows", nonAlphaNonNull + nullCount, notLikeCount);
            // Same for !~
            long notRegexCount = countOf("select count() from t where sym !~ '^al'");
            long nonAlphaRegexNonNull = countOf("select count() from t where sym is not null and sym !~ '^al'");
            Assert.assertEquals(nonAlphaRegexNonNull + nullCount, notRegexCount);
        });
    }

    /**
     * SP2 negation parity sweep: for a matrix of negated predicate shapes the fast index path
     * (unhinted) must return byte-identical rows to the scan+filter ground truth (hint immediately
     * after SELECT). Covers NOT LIKE, NOT ILIKE, !~, underscore escape, residual conjunct, no-match
     * pattern, and the non-lift {@code NOT LIKE '%'} case — on a table WITH NULL symbols and
     * MULTIPLE partitions.
     *
     * <p>The {@code not like '%'} case: {@code LIKE '%'} compiles to a not-null check (not a
     * SymbolKeySetProvider), so the {@code instanceof} gate in {@code tryGenerateSymbolPatternIndex}
     * returns null and the query falls back to scan+filter. Parity must hold regardless.
     */
    @Test
    public void testNegationParitySweep() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table t (sym symbol index, v long, ts timestamp) timestamp(ts) partition by day");
            execute("insert into t select rnd_symbol('alpha','alto','beta','ALPHA','al_x','gamma'), x, timestamp_sequence(0, 3600000000) from long_sequence(4000)");
            execute("insert into t select null, x, timestamp_sequence(4000*3600000000L, 3600000000) from long_sequence(400)");
            final ObjList<String> predicates = new ObjList<>(
                    "sym not like 'al%'",
                    "sym not like '%ta'",
                    "sym not like '%lph%'",
                    "sym not ilike 'al%'",
                    "sym !~ '^al'",
                    "sym !~ 'a'",
                    "sym not like 'zzz%'",
                    "sym not like '%'",
                    "sym not like 'al\\_x'",
                    "sym not like 'al%' and v > 100"
            );
            for (int i = 0, n = predicates.size(); i < n; i++) {
                final String p = predicates.getQuick(i);
                // Hint goes right after SELECT (a WHERE-position hint is a silent no-op).
                // Use %s as placeholder for the hint; escape any real % in the predicate.
                String base = "select %s sym, v, ts from t where " + p.replace("%", "%%") + " order by ts, v";
                String expected = select(String.format(base, "/*+ no_symbol_pattern_index(t) */ "));
                String actual = select(String.format(base, ""));
                io.questdb.test.tools.TestUtils.assertEquals("pred=[" + p + "]", expected, actual);
            }
        });
    }

    /**
     * Deferred-symbol test: compile a fast-path factory, then insert a new matching symbol, then
     * execute the cached factory — asserts the new symbol's rows are included (keys resolved at
     * execution time, not at compile time).
     */
    @Test
    public void testDeferredSymbolAddedAfterCompile() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table t (sym symbol index, v long, ts timestamp) timestamp(ts) partition by day");
            execute("insert into t select rnd_symbol('AA','BB'), x, timestamp_sequence(0, 60000000) from long_sequence(50)");
            // Compile the fast-path factory (engine.select returns a RecordCursorFactory)
            try (RecordCursorFactory factory = engine.select("select sym, v from t where sym like 'A%' order by v", sqlExecutionContext)) {
                // Insert a new matching symbol AFTER the factory was compiled
                execute("insert into t values ('AC', 999, 100000000::timestamp)");
                // Execute the cached plan now — must see the new 'AC' row
                String actual = printFactory(factory);
                String expected = select("select /*+ no_symbol_pattern_index(t) */ sym, v from t where sym like 'A%' order by v");
                io.questdb.test.tools.TestUtils.assertEquals(expected, actual);
            }
        });
    }

    /**
     * SP3: a POSITIVE pattern over a COVERED projection (all selected columns are the indexed symbol or
     * INCLUDE-d covered columns) must route through the {@code CoveringIndex} merge (reading covered
     * values from the posting sidecars), NOT the bitmap {@code SymbolPatternIndex}. Asserts recognition
     * (unhinted plan contains {@code CoveringIndex}) and byte-identical row parity vs the scan+filter
     * ground truth.
     *
     * <p>Ground-truth hint note: {@code no_symbol_pattern_index(t)} alone does NOT disable the covering
     * path (a positive covered pattern would still hit {@code CoveringIndex} via this new route), so the
     * scan+filter oracle must ALSO carry {@code no_covering(t)}. Both hints are space-separated in a
     * single hint block immediately after SELECT.
     */
    @Test
    public void testCoveringPositivePlanAndParity() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table t (sym symbol index type posting include (price), price double, ts timestamp) timestamp(ts) partition by day");
            execute("insert into t select rnd_symbol('AA','AB','BA','BB','AC'), x::double, timestamp_sequence(0, 60000000) from long_sequence(2000)");
            // Covered projection (sym known from WHERE, price covered) -> CoveringIndex, not the bitmap SymbolPatternIndex.
            assertQuery("select sym, price from t where sym like 'A%'").noLeakCheck().assertsPlanContaining("CoveringIndex");
            // Ground truth: force a plain scan+filter by disabling BOTH the pattern-index and the covering path.
            String expected = select("select /*+ no_symbol_pattern_index(t) no_covering(t) */ price, sym from t where sym like 'A%' order by price");
            String actual = select("select price, sym from t where sym like 'A%' order by price");
            io.questdb.test.tools.TestUtils.assertEquals(expected, actual);
        });
    }

    /**
     * SP3 covering + residual: a positive covered pattern with a residual conjunct on a covered column
     * ({@code AND price > 1000}) must still route through {@code CoveringIndex} (wrapped by a residual
     * filter) and match the scan+filter ground truth. The residual predicate references only covered
     * columns so the covering projection remains valid.
     */
    @Test
    public void testCoveringPositiveWithResidualParity() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table t (sym symbol index type posting include (price), price double, ts timestamp) timestamp(ts) partition by day");
            execute("insert into t select rnd_symbol('AA','AB','BA','BB','AC'), x::double, timestamp_sequence(0, 60000000) from long_sequence(2000)");
            assertQuery("select sym, price from t where sym like 'A%' and price > 1000").noLeakCheck().assertsPlanContaining("CoveringIndex");
            String expected = select("select /*+ no_symbol_pattern_index(t) no_covering(t) */ price, sym from t where sym like 'A%' and price > 1000 order by price");
            String actual = select("select price, sym from t where sym like 'A%' and price > 1000 order by price");
            io.questdb.test.tools.TestUtils.assertEquals(expected, actual);
        });
    }

    /**
     * SP3 covering + deferred symbol: keys are resolved at execution time from the provider, so a matching
     * symbol inserted AFTER the covering factory is compiled must appear in the covered result. Exercises
     * the getCursor provider seam re-populating {@code multiKeys} per execution (never cached at compile).
     */
    @Test
    public void testCoveringPositiveDeferredSymbol() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table t (sym symbol index type posting include (price), price double, ts timestamp) timestamp(ts) partition by day");
            execute("insert into t select rnd_symbol('AA','BB'), x::double, timestamp_sequence(0, 60000000) from long_sequence(50)");
            try (RecordCursorFactory factory = engine.select("select price, sym from t where sym like 'A%' order by price", sqlExecutionContext)) {
                // Insert a new matching symbol AFTER the covering factory was compiled.
                execute("insert into t values ('AC', 999.0, 100000000::timestamp)");
                String actual = printFactory(factory);
                String expected = select("select /*+ no_symbol_pattern_index(t) no_covering(t) */ price, sym from t where sym like 'A%' order by price");
                io.questdb.test.tools.TestUtils.assertEquals(expected, actual);
            }
        });
    }

    /**
     * SP3 covering page-frame path: a parallel GROUP BY aggregation ({@code sum(price)}) over a positive
     * covered pattern drives the covering factory's {@code getPageFrameCursor} multi-key provider branch
     * (the parallel/vectorized aggregation consumes page frames, not the record cursor). Asserts the plan
     * routes through {@code CoveringIndex} and that the aggregate equals the scan+filter ground truth --
     * proving the page-frame provider seam fills {@code multiKeys} correctly (right symbol-table basis,
     * no NULL key, deferred-safe).
     */
    @Test
    public void testCoveringPositivePageFrameAggregationParity() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table t (sym symbol index type posting include (price), price double, ts timestamp) timestamp(ts) partition by day");
            execute("insert into t select rnd_symbol('AA','AB','BA','BB','AC'), x::double, timestamp_sequence(0, 60000000) from long_sequence(4000)");
            // GROUP BY on the covered symbol drives page frames (parallel/vectorized aggregation) through the covering factory.
            assertQuery("select sym, sum(price) from t where sym like 'A%'").noLeakCheck().assertsPlanContaining("CoveringIndex");
            String expected = select("select /*+ no_symbol_pattern_index(t) no_covering(t) */ sym, sum(price) s from t where sym like 'A%' order by sym");
            String actual = select("select sym, sum(price) s from t where sym like 'A%' order by sym");
            io.questdb.test.tools.TestUtils.assertEquals(expected, actual);
        });
    }

    /**
     * SP3 negated stays classic: a NEGATED pattern ({@code NOT LIKE}) over a covered projection must NOT use
     * the covering route (covering only serves the positive match set; NOT LIKE includes NULL-symbol rows
     * which the covered merge cannot produce). It stays on the SP2 classic complement scan.
     */
    @Test
    public void testCoveringNegatedStaysClassic() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table t (sym symbol index type posting include (price), price double, ts timestamp) timestamp(ts) partition by day");
            execute("insert into t select rnd_symbol('AA','AB','BA','BB','AC'), x::double, timestamp_sequence(0, 60000000) from long_sequence(1500)");
            execute("insert into t select null, x::double, timestamp_sequence(1500*60000000, 60000000) from long_sequence(200)");
            assertQuery("select sym, price from t where sym not like 'A%'").noLeakCheck().assertsPlanContaining("SymbolPatternIndex");
            assertQuery("select sym, price from t where sym not like 'A%'").noLeakCheck().assertsPlanNotContaining("CoveringIndex");
            String expected = select("select /*+ no_symbol_pattern_index(t) no_covering(t) */ price, sym, ts from t where sym not like 'A%' order by ts, price");
            String actual = select("select price, sym, ts from t where sym not like 'A%' order by ts, price");
            io.questdb.test.tools.TestUtils.assertEquals(expected, actual);
        });
    }

    /**
     * SP3 covering parity sweep: a data-driven sweep on a COVERED-projection table that includes explicit
     * NULL-symbol rows and MULTIPLE partitions. For each predicate shape the covered fast path (unhinted)
     * must return byte-identical rows to the scan+filter oracle (forced by
     * {@code no_symbol_pattern_index(t) no_covering(t)} hints). KEY invariant: a POSITIVE pattern never
     * matches NULL, so the covered result must contain ZERO null-symbol rows for every predicate. This tests
     * the NULL-exclusion property of the covering path (correct key-set from provider excludes key 0 / the
     * NULL sentinel).
     *
     * <p>Covered DDL: {@code sym symbol index type posting include (price)}, so {@code price} and {@code sym}
     * are both covered. The projection selects ONLY covered columns ({@code price, sym}) and orders by
     * {@code price}; {@code ts} is intentionally excluded because it is NOT in the INCLUDE set and would
     * cause {@code buildCoveringIndexMapping} to return null, silently falling back to the bitmap path.
     * {@code price} values come from {@code x::double} over {@code long_sequence(4000)}, yielding unique
     * values 1.0–4000.0 for non-null rows, so ORDER BY price is deterministic within the compared row-sets.
     */
    @Test
    public void testCoveringParitySweep() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table t (sym symbol index type posting include (price), price double, ts timestamp) timestamp(ts) partition by day");
            execute("insert into t select rnd_symbol('alpha','alto','beta','ALPHA','al_x','gamma'), x::double, timestamp_sequence(0, 3600000000L) from long_sequence(4000)");
            // Explicit NULL-symbol rows in a separate partition to prove NULL exclusion on the covered path.
            execute("insert into t select null, x::double, timestamp_sequence(4000*3600000000L, 3600000000L) from long_sequence(300)");
            final ObjList<String> predicates = new ObjList<>(
                    "sym like 'al%'",
                    "sym like '%ta'",
                    "sym like '%lph%'",
                    "sym ilike 'al%'",
                    "sym ~ '^al'",
                    "sym like 'zzz%'",
                    "sym like 'al\\_x'",
                    "sym like 'al%' and price > 100"
            );
            for (int i = 0, n = predicates.size(); i < n; i++) {
                final String p = predicates.getQuick(i);
                // Projection: only covered columns (price from INCLUDE, sym as the index key).
                // ts is NOT in INCLUDE and must NOT appear here — it would cause buildCoveringIndexMapping
                // to return null and silently drop to the bitmap SymbolPatternIndex path.
                // ORDER BY price is deterministic: non-null price values are unique 1.0–4000.0.
                String base = "select %s price, sym from t where " + p.replace("%", "%%") + " order by price";
                String expected = select(String.format(base, "/*+ no_symbol_pattern_index(t) no_covering(t) */ "));
                String actual = select(String.format(base, ""));
                io.questdb.test.tools.TestUtils.assertEquals("pred=[" + p + "]", expected, actual);
                // Guard: the unhinted plan MUST route through CoveringIndex for every predicate.
                // If any predicate silently falls back to the bitmap path this assertion fails loudly.
                String guardSql = "select price, sym from t where " + p + " order by price";
                assertQuery(guardSql).noLeakCheck().assertsPlanContaining("CoveringIndex");
            }
        });
    }

    /**
     * SP3 covering routing: asserts that the query planner routes correctly based on whether the
     * projection is fully covered by the posting index.
     *
     * <ul>
     *   <li>Covered projection ({@code select sym, price}) -&gt; plan must contain {@code CoveringIndex}.</li>
     *   <li>NOT-covered projection (column {@code extra} is not in the INCLUDE set) -&gt; plan must fall
     *       back to the classic bitmap {@code SymbolPatternIndex}.</li>
     *   <li>{@code no_covering(t)} hint on a covered query -&gt; forces the classic bitmap
     *       {@code SymbolPatternIndex} (not covering, but still a fast path).</li>
     * </ul>
     */
    @Test
    public void testCoveringVsBitmapRouting() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table t (sym symbol index type posting include (price), price double, extra long, ts timestamp) timestamp(ts) partition by day");
            execute("insert into t select rnd_symbol('AA','AB','BA'), x::double, x, timestamp_sequence(0, 60000000) from long_sequence(500)");
            // Covered projection: all selected columns are sym (from WHERE) + price (INCLUDE) -> CoveringIndex.
            assertQuery("select sym, price from t where sym like 'A%'").noLeakCheck().assertsPlanContaining("CoveringIndex");
            // NOT-covered projection: 'extra' is not in the INCLUDE set -> falls back to classic SymbolPatternIndex.
            assertQuery("select sym, extra from t where sym like 'A%'").noLeakCheck().assertsPlanContaining("SymbolPatternIndex");
            // Hint disables covering -> falls back to bitmap SymbolPatternIndex (still a fast path, just not covering).
            assertQuery("select /*+ no_covering(t) */ sym, price from t where sym like 'A%'").noLeakCheck().assertsPlanContaining("SymbolPatternIndex");
        });
    }

    /**
     * Fixture for the time-series consumers -- ASOF/LT/SPLICE join, SAMPLE BY with FILL, a nested model
     * under a timestamp-requiring parent -- that compile only over a base factory reporting
     * {@code SCAN_DIRECTION_FORWARD}. Each of those queries compiled before the symbol-pattern index
     * route existed, so the route must keep them compiling and returning the scan+filter rows.
     * <p>
     * The slave keeps 'bb' rows both strictly between and exactly on master timestamps, which separates
     * ASOF (last row at or before) from LT (last row strictly before) on the final master row.
     */
    private void createTimeSeriesJoinFixture() throws SqlException {
        execute("CREATE TABLE t (sym SYMBOL INDEX, v LONG, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
        execute("""
                INSERT INTO t VALUES
                  ('aa', 1, '2024-01-01T00:00:00.000000Z'),
                  ('bb', 2, '2024-01-01T01:00:00.000000Z'),
                  ('ab', 3, '2024-01-01T02:00:00.000000Z'),
                  ('bb', 4, '2024-01-01T03:00:00.000000Z'),
                  ('aa', 5, '2024-01-02T00:00:00.000000Z'),
                  ('bb', 6, '2024-01-02T01:00:00.000000Z'),
                  ('ab', 7, '2024-01-02T02:00:00.000000Z'),
                  ('bb', 8, '2024-01-02T02:00:00.000000Z')""");
    }

    @Test
    public void testAsOfJoinOnPatternFilteredMaster() throws Exception {
        assertMemoryLeak(() -> {
            createTimeSeriesJoinFixture();
            assertQuery("""
                    SELECT a.ts, a.sym, b.v FROM (t WHERE sym LIKE 'a%') a
                    ASOF JOIN (SELECT * FROM t WHERE sym = 'bb') b""")
                    .timestamp("ts")
                    .noRandomAccess()
                    .withPlanContaining("SymbolPatternIndex")
                    .returns("ts\tsym\tv\n" +
                            "2024-01-01T00:00:00.000000Z\taa\tnull\n" +
                            "2024-01-01T02:00:00.000000Z\tab\t2\n" +
                            "2024-01-02T00:00:00.000000Z\taa\t4\n" +
                            "2024-01-02T02:00:00.000000Z\tab\t8\n");
        });
    }

    /**
     * Same shape as {@link #testAsOfJoinOnPatternFilteredMaster()}, but the master declares its
     * designated timestamp explicitly. {@code generateSelectChoose()} pushes a {@code false}
     * timestamp-required flag for such a model, so the factory cannot lean on that flag: it has to
     * scan in timestamp order and say so.
     */
    @Test
    public void testAsOfJoinOnPatternFilteredMasterWithExplicitTimestamp() throws Exception {
        assertMemoryLeak(() -> {
            createTimeSeriesJoinFixture();
            assertQuery("""
                    SELECT a.ts, a.sym, b.v FROM (SELECT * FROM t WHERE sym LIKE 'a%') a timestamp(ts)
                    ASOF JOIN (SELECT * FROM t WHERE sym = 'bb') b""")
                    .timestamp("ts")
                    .noRandomAccess()
                    .returns("ts\tsym\tv\n" +
                            "2024-01-01T00:00:00.000000Z\taa\tnull\n" +
                            "2024-01-01T02:00:00.000000Z\tab\t2\n" +
                            "2024-01-02T00:00:00.000000Z\taa\t4\n" +
                            "2024-01-02T02:00:00.000000Z\tab\t8\n");
        });
    }

    /**
     * Same shape again, but the explicit {@code timestamp(ts)} sits on a parenthesised sub-query that
     * the join then aliases. That extra nesting level puts a {@code generateSelectChoose()} model
     * between the pattern route and {@code validateBothTimestampOrders()}, and
     * {@code generateSelectChoose()} pushes {@code timestampRequired = false} for a model carrying its
     * own timestamp clause. So neither the factory nor its parent can lean on the execution context
     * here: only an honest {@code SCAN_DIRECTION_FORWARD} keeps this query compiling.
     */
    @Test
    public void testAsOfJoinOnPatternFilteredMasterWithNestedExplicitTimestamp() throws Exception {
        assertMemoryLeak(() -> {
            createTimeSeriesJoinFixture();
            assertQuery("""
                    SELECT a.ts, a.sym, b.v FROM ((SELECT * FROM t WHERE sym LIKE 'a%') timestamp(ts)) a
                    ASOF JOIN (SELECT * FROM t WHERE sym = 'bb') b""")
                    .timestamp("ts")
                    .noRandomAccess()
                    .withPlanContaining("AdaptiveSymbolPattern")
                    .returns("ts\tsym\tv\n" +
                            "2024-01-01T00:00:00.000000Z\taa\tnull\n" +
                            "2024-01-01T02:00:00.000000Z\tab\t2\n" +
                            "2024-01-02T00:00:00.000000Z\taa\t4\n" +
                            "2024-01-02T02:00:00.000000Z\tab\t8\n");
        });
    }

    @Test
    public void testLtJoinOnPatternFilteredMaster() throws Exception {
        assertMemoryLeak(() -> {
            createTimeSeriesJoinFixture();
            assertQuery("""
                    SELECT a.ts, a.sym, b.v FROM (t WHERE sym LIKE 'a%') a
                    LT JOIN (SELECT * FROM t WHERE sym = 'bb') b""")
                    .timestamp("ts")
                    .noRandomAccess()
                    .returns("ts\tsym\tv\n" +
                            "2024-01-01T00:00:00.000000Z\taa\tnull\n" +
                            "2024-01-01T02:00:00.000000Z\tab\t2\n" +
                            "2024-01-02T00:00:00.000000Z\taa\t4\n" +
                            "2024-01-02T02:00:00.000000Z\tab\t6\n");
        });
    }

    @Test
    public void testSpliceJoinOnPatternFilteredMaster() throws Exception {
        assertMemoryLeak(() -> {
            createTimeSeriesJoinFixture();
            assertQuery("""
                    SELECT a.ts, a.sym, b.v FROM (t WHERE sym LIKE 'a%') a
                    SPLICE JOIN (SELECT * FROM t WHERE sym = 'bb') b""")
                    .noRandomAccess()
                    .returns("ts\tsym\tv\n" +
                            "2024-01-01T00:00:00.000000Z\taa\tnull\n" +
                            "2024-01-01T00:00:00.000000Z\taa\t2\n" +
                            "2024-01-01T02:00:00.000000Z\tab\t2\n" +
                            "2024-01-01T02:00:00.000000Z\tab\t4\n" +
                            "2024-01-02T00:00:00.000000Z\taa\t4\n" +
                            "2024-01-02T00:00:00.000000Z\taa\t6\n" +
                            "2024-01-02T02:00:00.000000Z\tab\t8\n");
        });
    }

    /**
     * SAMPLE BY over a nested pattern-filtered model: {@code generateSelectChoose()} demands ASC
     * timestamp order from the sub-query whenever the context requires a timestamp, and reports
     * {@code [25] ASC order over TIMESTAMP column is required but not provided} when the base does not.
     */
    @Test
    public void testSampleByOverPatternFilteredSubQuery() throws Exception {
        assertMemoryLeak(() -> {
            createTimeSeriesJoinFixture();
            assertQuery("SELECT ts, count() FROM (SELECT * FROM t WHERE sym LIKE 'a%') SAMPLE BY 1d")
                    .timestamp("ts")
                    .noRandomAccess()
                    .returns("ts\tcount\n" +
                            "2024-01-01T00:00:00.000000Z\t2\n" +
                            "2024-01-02T00:00:00.000000Z\t2\n");
        });
    }

    /**
     * SAMPLE BY with FILL takes the non-parallel route, which rejects a base factory that does not
     * report ASC designated-timestamp order.
     */
    @Test
    public void testSampleByFillOverPatternFilter() throws Exception {
        assertMemoryLeak(() -> {
            createTimeSeriesJoinFixture();
            assertQuery("SELECT ts, count() FROM t WHERE sym LIKE 'a%' SAMPLE BY 1d FILL(LINEAR)")
                    .timestamp("ts")
                    .expectSize()
                    .returns("ts\tcount\n" +
                            "2024-01-01T00:00:00.000000Z\t2\n" +
                            "2024-01-02T00:00:00.000000Z\t2\n");
        });
    }

    /**
     * SAMPLE BY with FILL over a nested model that declares its own designated timestamp. The FILL
     * route rejects a base factory that does not report ASC designated-timestamp order, and the
     * nested {@code timestamp(ts)} model reaches it with {@code timestampRequired = false}, so the
     * rejection depends only on what the pattern route advertises.
     */
    @Test
    public void testSampleByFillOverPatternFilteredSubQueryWithExplicitTimestamp() throws Exception {
        assertMemoryLeak(() -> {
            createTimeSeriesJoinFixture();
            assertQuery("SELECT ts, count() FROM (SELECT * FROM t WHERE sym LIKE 'a%') timestamp(ts) SAMPLE BY 1d FILL(LINEAR)")
                    .timestamp("ts")
                    .expectSize()
                    .withPlanContaining("AdaptiveSymbolPattern")
                    .returns("ts\tcount\n" +
                            "2024-01-01T00:00:00.000000Z\t2\n" +
                            "2024-01-02T00:00:00.000000Z\t2\n");
        });
    }

    /**
     * C3: with no covering delegate the bitmap index route is reachable, and it cannot supply page
     * frames, so the only wrapper the code generator could put above the adaptive factory was a serial
     * filter -- on EVERY open, including the majority that fall back to a full scan. The plan must now
     * show the adaptive node itself dispatching between a serial index child and a parallel
     * {@code Async Filter} child, and must keep the base plan's parallelism for the fallback.
     * <p>
     * Asserts the nested plan text rather than bare node names: the adaptive factory prints all of its
     * delegates unconditionally, so only the parent/child shape proves which wrapper carries the filter.
     */
    @Test
    public void testBitmapPatternRouteFiltersFallbackScanInParallel() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (sym SYMBOL INDEX, v LONG, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("INSERT INTO t SELECT rnd_symbol('aa','ab','ba','bb'), x, timestamp_sequence(0, 60_000_000) FROM long_sequence(1_000)");

            assertQuery("SELECT * FROM t WHERE sym LIKE 'a%'")
                    .noLeakCheck()
                    .assertsPlanContaining(
                            """
                                    AdaptiveSymbolPattern policy: matching rows <= 5%, bounded probes route: one child per open
                                      indexRouteFilter: sym like a%
                                        SymbolPatternIndex""",
                            """
                                    Async Filter workers: 1
                                          filter: sym like a%
                                            PageFrame""");

            // the negated route never builds a covering delegate on any index type, so it regressed too
            assertQuery("SELECT * FROM t WHERE sym NOT LIKE 'a%'")
                    .noLeakCheck()
                    .assertsPlanContaining(
                            """
                                    AdaptiveSymbolPattern policy: matching rows <= 5%, bounded probes route: one child per open
                                      indexRouteFilter: not(sym like a%)
                                        SymbolPatternIndex""",
                            """
                                    Async Filter workers: 1
                                          filter: not(sym like a%)
                                            PageFrame""");

            // both branches must still agree with the scan+filter oracle, row for row
            String positive = select("SELECT /*+ no_symbol_pattern_index(t) */ sym, v, ts FROM t WHERE sym LIKE 'a%' ORDER BY ts, v");
            assertQuery("SELECT sym, v, ts FROM t WHERE sym LIKE 'a%' ORDER BY ts, v")
                    .noLeakCheck()
                    .timestamp("ts")
                    .returns(positive);

            String negated = select("SELECT /*+ no_symbol_pattern_index(t) */ sym, v, ts FROM t WHERE sym NOT LIKE 'a%' ORDER BY ts, v");
            assertQuery("SELECT sym, v, ts FROM t WHERE sym NOT LIKE 'a%' ORDER BY ts, v")
                    .noLeakCheck()
                    .timestamp("ts")
                    .returns(negated);
        });
    }

    /**
     * A POSTING index without an {@code INCLUDE (...)} clause, and a POSTING index whose {@code INCLUDE}
     * does not cover the projection, both reach the same no-covering-delegate shape as a plain BITMAP
     * index. So does a negated pattern on a fully covered projection, because
     * {@code tryGenerateSymbolPatternIndex} guards the covering delegate with {@code !isNegated}. All
     * three must keep the parallel filter on the fallback scan.
     */
    @Test
    public void testPostingPatternRouteFiltersFallbackScanInParallel() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (sym SYMBOL INDEX TYPE POSTING, v LONG, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("INSERT INTO t SELECT rnd_symbol('aa','ab','ba','bb'), x, timestamp_sequence(0, 60_000_000) FROM long_sequence(1_000)");
            execute("CREATE TABLE c (sym SYMBOL INDEX TYPE POSTING INCLUDE (v), v LONG, extra LONG, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("INSERT INTO c SELECT rnd_symbol('aa','ab','ba','bb'), x, x, timestamp_sequence(0, 60_000_000) FROM long_sequence(1_000)");

            // posting, no INCLUDE at all
            assertQuery("SELECT * FROM t WHERE sym LIKE 'a%'")
                    .noLeakCheck()
                    .assertsPlanContaining("AdaptiveSymbolPattern", "indexRouteFilter: sym like a%", "Async Filter workers: 1");

            // posting + INCLUDE, but the projection reaches a column the sidecar does not carry
            assertQuery("SELECT sym, extra FROM c WHERE sym LIKE 'a%'")
                    .noLeakCheck()
                    .assertsPlanContaining("AdaptiveSymbolPattern", "indexRouteFilter: sym like a%", "Async Filter workers: 1");

            // posting + INCLUDE, fully covered projection, but negated -- no covering delegate is built
            assertQuery("SELECT sym, v FROM c WHERE sym NOT LIKE 'a%'")
                    .noLeakCheck()
                    .assertsPlanContaining("AdaptiveSymbolPattern", "indexRouteFilter: not(sym like a%)", "Async Filter workers: 1");

            String negated = select("SELECT /*+ no_symbol_pattern_index(t) no_covering(t) */ sym, v, ts FROM c WHERE sym NOT LIKE 'a%' ORDER BY ts, v");
            assertQuery("SELECT sym, v, ts FROM c WHERE sym NOT LIKE 'a%' ORDER BY ts, v")
                    .noLeakCheck()
                    .timestamp("ts")
                    .returns(negated);
        });
    }

    /**
     * The covered positive pattern keeps the OTHER wrapper shape: the async filter stays ABOVE the
     * adaptive factory, which is what lets a downstream group by steal the filter and run as
     * {@code Async Group By}. A fix that moved filtering inside the factory for every shape would have
     * silently given that up, so pin the parent/child order and the group-by node.
     */
    @Test
    public void testCoveredPositivePatternKeepsAsyncFilterAboveAdaptiveFactory() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (sym SYMBOL INDEX TYPE POSTING INCLUDE (v), v LONG, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("INSERT INTO t SELECT rnd_symbol('aa','ab','ba','bb'), x, timestamp_sequence(0, 60_000_000) FROM long_sequence(1_000)");

            assertQuery("SELECT * FROM t WHERE sym LIKE 'a%'")
                    .noLeakCheck()
                    .assertsPlanContaining(
                            """
                                    Async Filter workers: 1
                                      filter: sym like a%
                                        AdaptiveSymbolPattern""")
                    ;

            assertQuery("SELECT sym, count() FROM t WHERE sym LIKE 'a%'")
                    .noLeakCheck()
                    .assertsPlanContaining("Async Group By workers: 1", "AdaptiveSymbolPattern", "CoveringIndex");
        });
    }

    /**
     * With the parallel filter switched off there is no async wrapper to hand the scan route, so the
     * factory falls back to the pre-existing serial shape. Rows must be identical either way.
     */
    @Test
    public void testPatternRouteStaysSerialWhenParallelFilterDisabled() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (sym SYMBOL INDEX, v LONG, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("INSERT INTO t SELECT rnd_symbol('aa','ab','ba','bb'), x, timestamp_sequence(0, 60_000_000) FROM long_sequence(1_000)");

            String expected = select("SELECT /*+ no_symbol_pattern_index(t) */ sym, v, ts FROM t WHERE sym LIKE 'a%' ORDER BY ts, v");
            sqlExecutionContext.setParallelFilterEnabled(false);
            try {
                assertQuery("SELECT * FROM t WHERE sym LIKE 'a%'")
                        .noLeakCheck()
                        .assertsPlanContaining(
                                """
                                        Filter filter: sym like a%
                                            AdaptiveSymbolPattern""");
                assertQuery("SELECT * FROM t WHERE sym LIKE 'a%'")
                        .noLeakCheck()
                        .assertsPlanNotContaining("Async Filter");
                assertQuery("SELECT sym, v, ts FROM t WHERE sym LIKE 'a%' ORDER BY ts, v")
                        .noLeakCheck()
                        .timestamp("ts")
                        .returns(expected);
            } finally {
                sqlExecutionContext.setParallelFilterEnabled(true);
            }
        });
    }

    /**
     * Self-filtering mode adds owned objects to the construction sequence: the async wrapper takes the
     * scan factory AND the prepared pattern filter. This throws from
     * {@code isParallelFilterEnabled()} -- the first call after the index delegate, the scan delegate
     * and the prepared filter exist but before any of them changes owner -- and asserts nothing is
     * stranded and the partition frame factory is closed exactly once.
     */
    @Test
    public void testSelfFilteringConstructionFreesDelegatesExactlyOnceOnThrow() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (sym SYMBOL INDEX, v LONG, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL");
            execute("INSERT INTO t SELECT rnd_symbol('aa','ab','ba'), x, timestamp_sequence(0, 60_000_000) FROM long_sequence(100)");
            engine.releaseAllWriters();

            final int[] partitionFactoryCloseCount = new int[1];
            FullPartitionFrameCursorFactory.setCloseObserverForTesting(factory -> partitionFactoryCloseCount[0]++);
            try {
                final SqlExecutionContextImpl ctx = new SqlExecutionContextImpl(engine, 4) {
                    @Override
                    public boolean isParallelFilterEnabled() {
                        throw new RuntimeException("test self-filtering construction failure");
                    }
                };
                ctx.with(engine.getConfiguration().getFactoryProvider().getSecurityContextFactory().getRootContext());
                try (ctx) {
                    try (RecordCursorFactory ignored = engine.select("SELECT v FROM t WHERE sym LIKE 'a%' AND v > 0", ctx)) {
                        Assert.fail("expected isolated self-filtering construction failure");
                    } catch (RuntimeException e) {
                        TestUtils.assertContains(e.getMessage(), "test self-filtering construction failure");
                    }
                }
            } finally {
                FullPartitionFrameCursorFactory.clearCloseObserverForTesting();
            }
            Assert.assertEquals(1, partitionFactoryCloseCount[0]);
        });
    }

    /**
     * Same contract as
     * {@link #testIndexFactoryCloseReleasesResourcesWhenPartitionFactoryCloseThrowsWithHeapCursor()},
     * for the other row cursor factory the constructor can pick: ORDER_BY_INVARIANT without a
     * timestamp order builds a {@code SequentialRowCursorFactory} instead of a heap one, so it is a
     * different owned object on the same cleanup chain.
     */
    @Test
    public void testIndexFactoryCloseReleasesResourcesWhenPartitionFactoryCloseThrowsWithSequentialCursor() throws Exception {
        assertIndexFactoryCloseReleasesResourcesOnThrow(OrderByMnemonic.ORDER_BY_INVARIANT);
    }

    /**
     * {@code AbstractPageFrameRecordCursorFactory._close()} ends in
     * {@code CairoException.rethrowCleanupFailure(...)}, so a partition-frame cleanup failure
     * propagates into {@code SymbolPatternIndexRecordCursorFactory._close()} at its very first
     * statement. {@code AbstractRecordCursorFactory.close()} sets its closed flag BEFORE calling
     * {@code _close()}, so no retry ever comes and a sequential free chain strands everything the
     * throw skipped -- here the index cursor, whose {@code PageFrameAddressCache} holds four native
     * {@code DirectLongList}s allocated in its constructor.
     * <p>
     * The test injects that failure by handing the factory a partition frame cursor factory that
     * releases everything it owns and only then throws from {@code close()}, which is the shape a
     * real cleanup failure takes: the throw is the last thing that happens, so nothing below this
     * factory leaks and the only thing {@code assertMemoryLeak()} can still see is what this
     * factory's own chain abandoned.
     */
    @Test
    public void testIndexFactoryCloseReleasesResourcesWhenPartitionFactoryCloseThrowsWithHeapCursor() throws Exception {
        assertIndexFactoryCloseReleasesResourcesOnThrow(OrderByMnemonic.ORDER_BY_UNKNOWN);
    }

    /**
     * The serial index branch and the parallel scan branch share ONE filter instance, so a re-bound
     * pattern cannot make them disagree. Prove it on a single compiled factory whose selectivity, and
     * therefore whose branch, flips between executions: selective -&gt; index branch, broad -&gt; parallel
     * scan branch, and back. Each execution is compared against the scan+filter oracle, and the branch
     * counters prove the route actually flipped rather than both executions taking the same one.
     */
    @Test
    public void testBindVariablePatternAgreesAcrossRouteFlip() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (sym SYMBOL INDEX, v LONG, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY YEAR");
            // 'rare' is 1 row in 1000 (well under the 5% policy), 'bulk' is the rest (well over it)
            execute("""
                    INSERT INTO t SELECT
                      CASE WHEN x % 1_000 = 0 THEN 'rare' ELSE 'bulk' END, x, timestamp_sequence(0, 60_000_000)
                    FROM long_sequence(4_000)""");

            final String query = "SELECT sym, v FROM t WHERE sym LIKE :pattern ORDER BY v";
            final String oracle = "SELECT /*+ no_symbol_pattern_index(t) */ sym, v FROM t WHERE sym LIKE :pattern ORDER BY v";
            bindVariableService.setStr("pattern", "r%");
            try (RecordCursorFactory factory = engine.select(query, sqlExecutionContext)) {
                assertRouteFlip(factory, oracle, true);

                bindVariableService.setStr("pattern", "b%");
                assertRouteFlip(factory, oracle, false);

                // and back, on the same factory, to rule out a one-way latch
                bindVariableService.setStr("pattern", "r%");
                assertRouteFlip(factory, oracle, true);

                // a pattern no symbol matches short-circuits to an empty index cursor
                bindVariableService.setStr("pattern", "zz%");
                assertRouteFlip(factory, oracle, true);
            }
        });
    }

    /**
     * {@code PreparedSymbolPatternFilter.isThreadSafe()} answers true while ignoring
     * {@code providerFunction.isThreadSafe()}, which is false for every LIKE/ILIKE and regex provider.
     * That answer is only sound after {@code prepare()} has run: until then
     * {@code MatchStaticSymbolTableConstPatternFunction.getBool()} and its runtime-const sibling take a
     * lazy-init branch that rebuilds their key list through a shared {@code Matcher}, and the async
     * filter this factory builds passes {@code perWorkerFilters == null}, so every worker would drive
     * that branch on the same instance. Pin the precondition: {@code getBool()} before {@code prepare()}
     * fails the assertion, and succeeds after it.
     */
    @Test
    public void testPreparedFilterAssertsPrepareRanBeforeGetBool() {
        boolean isAssertionEnabled = false;
        //noinspection AssertWithSideEffects,ConstantValue
        assert isAssertionEnabled = true;
        Assert.assertTrue("this test needs -ea; core/pom.xml enables it", isAssertionEnabled);

        final AdaptiveSymbolPatternRecordCursorFactory.PreparedSymbolPatternFilter filter =
                new AdaptiveSymbolPatternRecordCursorFactory.PreparedSymbolPatternFilter(
                        new AlwaysMatchingKeySetProvider(), null, false, 0
                );
        try {
            Assert.assertThrows(AssertionError.class, () -> filter.getBool(null));
            filter.prepare(null, sqlExecutionContext);
            Assert.assertTrue(filter.getBool(null));
        } catch (SqlException e) {
            throw new AssertionError(e);
        } finally {
            filter.close();
        }
    }

    /**
     * Runs {@code factory} once and asserts it agrees with {@code oracle}, and that the open took the
     * index branch ({@code isIndexBranchExpected}) or the parallel scan branch.
     */
    private void assertRouteFlip(RecordCursorFactory factory, String oracle, boolean isIndexBranchExpected) throws SqlException {
        AdaptiveSymbolPatternRecordCursorFactory.resetTestCounters();
        SymbolPatternIndexRecordCursorFactory.resetTestCounters();
        TestUtils.assertEquals(select(oracle), printFactory(factory));
        if (isIndexBranchExpected) {
            Assert.assertTrue(
                    "expected the index branch",
                    SymbolPatternIndexRecordCursorFactory.testIndexInvocations.get() > 0
            );
            Assert.assertEquals(0, AdaptiveSymbolPatternRecordCursorFactory.testScanInvocations.get());
        } else {
            Assert.assertTrue(
                    "expected the parallel scan branch",
                    AdaptiveSymbolPatternRecordCursorFactory.testScanInvocations.get() > 0
            );
            Assert.assertEquals(0, SymbolPatternIndexRecordCursorFactory.testIndexInvocations.get());
        }
    }

    /**
     * Runs {@code table} filtered by {@code predicate}, asserts it agrees with the no-index oracle, and
     * asserts the open took the bitmap-index route or the fallback scan.
     */
    private void assertPatternRoute(String table, String predicate, boolean isIndexRouteExpected) throws SqlException {
        SymbolPatternIndexRecordCursorFactory.resetTestCounters();
        TestUtils.assertEquals(
                select("SELECT /*+ no_symbol_pattern_index(" + table + ") */ sym, v FROM " + table
                        + " WHERE " + predicate + " ORDER BY v"),
                select("SELECT sym, v FROM " + table + " WHERE " + predicate + " ORDER BY v")
        );
        final long indexInvocations = SymbolPatternIndexRecordCursorFactory.testIndexInvocations.get();
        final long fallbackInvocations = SymbolPatternIndexRecordCursorFactory.testFallbackInvocations.get();
        if (isIndexRouteExpected) {
            Assert.assertTrue("expected the index route on " + table + ", got fallback=" + fallbackInvocations, indexInvocations > 0);
            Assert.assertEquals(0, fallbackInvocations);
        } else {
            Assert.assertTrue("expected the fallback scan on " + table + ", got index=" + indexInvocations, fallbackInvocations > 0);
            Assert.assertEquals(0, indexInvocations);
        }
    }

    /**
     * Runs {@code sql} and returns its printed text (header + rows) captured into a private sink, so two
     * queries can be compared without clobbering the shared static test sink.
     */
    private String select(String sql) throws SqlException {
        StringSink localSink = new StringSink();
        printSql(sql, localSink);
        return localSink.toString();
    }

    /**
     * Runs a scalar {@code count()} query and returns the single long result.
     * The printed form is "count\n&lt;value&gt;\n"; we strip the header line and parse the first data token.
     */
    private long countOf(String sql) throws SqlException {
        String out = select(sql);
        // Format: "count\n<value>\n" — skip the header line, parse the number.
        int newline = out.indexOf('\n');
        Assert.assertTrue("countOf query returned no data: " + out, newline >= 0 && newline < out.length() - 1);
        String rest = out.substring(newline + 1).trim();
        int end = rest.indexOf('\n');
        String token = end >= 0 ? rest.substring(0, end).trim() : rest;
        return Long.parseLong(token);
    }

    private void assertIndexFactoryCloseReleasesResourcesOnThrow(int orderByMnemonic) throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (sym SYMBOL INDEX, v LONG, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL");
            execute("INSERT INTO t SELECT rnd_symbol('aa','ab','ba'), x, timestamp_sequence(0, 60_000_000) FROM long_sequence(1_000)");
            engine.releaseAllWriters();

            final TableToken tableToken = engine.verifyTableName("t");
            final GenericRecordMetadata metadata;
            final int symbolColumnIndex;
            final IntList effectiveKeys = new IntList();
            try (TableReader reader = engine.getReader(tableToken)) {
                metadata = GenericRecordMetadata.copyOf(reader.getMetadata());
                symbolColumnIndex = reader.getMetadata().getColumnIndexQuiet("sym");
                effectiveKeys.add(reader.getSymbolMapReader(symbolColumnIndex).keyOf("aa"));
                effectiveKeys.add(reader.getSymbolMapReader(symbolColumnIndex).keyOf("ab"));
            }
            final IntList columnIndexes = new IntList();
            final IntList columnSizeShifts = new IntList();
            for (int i = 0, n = metadata.getColumnCount(); i < n; i++) {
                columnIndexes.add(i);
                columnSizeShifts.add(Numbers.msb(ColumnType.sizeOf(metadata.getColumnType(i))));
            }

            final ThrowingClosePartitionFrameCursorFactory dfcFactory =
                    new ThrowingClosePartitionFrameCursorFactory(tableToken, metadata);
            final SymbolPatternIndexRecordCursorFactory factory = new SymbolPatternIndexRecordCursorFactory(
                    configuration,
                    metadata,
                    dfcFactory,
                    symbolColumnIndex,
                    effectiveKeys,
                    orderByMnemonic,
                    false,
                    IndexReader.DIR_FORWARD,
                    columnIndexes,
                    columnSizeShifts
            );
            // Drain a cursor first, so every resource the factory owns is live rather than merely
            // constructed: the per-key row cursor factories only exist after initRecordCursor(), and
            // the index cursor's native page-frame address cache only grows once frames are walked.
            // The cursor is deliberately left unclosed, because the factory - not the caller - owns
            // it: getCursor() hands back the factory's own singleton, and
            // AbstractPageFrameRecordCursorFactory.getCursor() itself abandons that singleton when
            // initRecordCursor() throws. _close() is then the only thing that can release it.
            final RecordCursor cursor = factory.getCursor(sqlExecutionContext);
            int rowCount = 0;
            while (cursor.hasNext()) {
                rowCount++;
            }
            Assert.assertTrue("the index route must have produced rows", rowCount > 0);

            try {
                factory.close();
                Assert.fail("expected the injected partition frame factory close failure");
            } catch (RuntimeException e) {
                TestUtils.assertContains(e.getMessage(), ThrowingClosePartitionFrameCursorFactory.CLOSE_FAILURE_MESSAGE);
            }
            Assert.assertTrue(
                    "the injected failure must fire only after the partition frame factory released its own resources",
                    dfcFactory.hasReleasedOwnResources
            );
        });
    }

    /**
     * Executes a pre-compiled {@link RecordCursorFactory} and returns its output as a string
     * (header + rows), using a private sink so it does not clobber the shared static test sink.
     */
    private String printFactory(RecordCursorFactory factory) throws SqlException {
        StringSink localSink = new StringSink();
        try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
            println(factory.getMetadata(), cursor, localSink);
        }
        return localSink.toString();
    }

    /**
     * Stands in for the real LIKE/regex providers in
     * {@link #testPreparedFilterAssertsPrepareRanBeforeGetBool()}: the only thing that test needs from a
     * provider is that it satisfies the {@link SymbolKeySetProvider} cast the prepared filter performs.
     */
    private static class AlwaysMatchingKeySetProvider extends BooleanFunction implements SymbolKeySetProvider {
        private final IntList matchedSymbolKeys = new IntList();

        @Override
        public boolean getBool(Record rec) {
            return true;
        }

        @Override
        public IntList getMatchedSymbolKeys() {
            return matchedSymbolKeys;
        }

        @Override
        public void toPlan(PlanSink sink) {
            sink.val("always-matching-stub");
        }
    }

    /**
     * Fault injection for the cleanup path: releases everything the real factory owns and only then
     * throws, so the failure reaching the caller's {@code _close()} chain is the sole remaining
     * source of stranded resources.
     */
    private static final class ThrowingClosePartitionFrameCursorFactory extends FullPartitionFrameCursorFactory {
        private static final String CLOSE_FAILURE_MESSAGE = "test partition frame factory close failure";
        private boolean hasReleasedOwnResources;

        private ThrowingClosePartitionFrameCursorFactory(TableToken tableToken, RecordMetadata metadata) {
            super(tableToken, TableUtils.ANY_TABLE_VERSION, metadata, PartitionFrameCursorFactory.ORDER_ASC, null, 0, false);
        }

        @Override
        public void close() {
            super.close();
            hasReleasedOwnResources = true;
            throw new RuntimeException(CLOSE_FAILURE_MESSAGE);
        }
    }
}
