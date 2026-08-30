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
import io.questdb.cairo.TableWriter;
import io.questdb.cairo.idx.AbstractPostingIndexReader;
import io.questdb.cairo.idx.BitmapIndexFwdReader;
import io.questdb.cairo.idx.BitmapIndexUtils;
import io.questdb.cairo.idx.IndexReader;
import io.questdb.cairo.idx.PostingIndexUtils;
import io.questdb.cairo.pool.PoolListener;
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
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.SqlExecutionContextImpl;
import io.questdb.griffin.engine.PerWorkerLocks;
import io.questdb.griffin.engine.functions.BooleanFunction;
import io.questdb.griffin.engine.functions.regex.AbstractLikeSymbolFunctionFactory;
import io.questdb.griffin.engine.functions.regex.SymbolKeySetProvider;
import io.questdb.griffin.engine.table.AdaptiveSymbolPatternRecordCursorFactory;
import io.questdb.griffin.engine.table.AsyncFilterAtom;
import io.questdb.griffin.engine.table.HeapRowCursorFactory;
import io.questdb.griffin.engine.table.SymbolPatternIndexRecordCursorFactory;
import io.questdb.griffin.model.ExpressionNode;
import io.questdb.griffin.model.QueryModel;
import io.questdb.mp.WorkerPool;
import io.questdb.std.Chars;
import io.questdb.std.IntList;
import io.questdb.std.Numbers;
import io.questdb.std.ObjList;
import io.questdb.std.str.StringSink;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.cairo.sql.async.SlotGatedWorkStealingStrategy;
import io.questdb.test.tools.TestUtils;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.lang.reflect.Field;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

public class SymbolPatternIndexTest extends AbstractCairoTest {
    private static final long COMMIT_PROBE_EXISTING_SYMBOL_TIMESTAMP = 172_800_000_000L; // 1970-01-03
    private static final long COMMIT_PROBE_NEW_SYMBOL_TIMESTAMP = 259_200_000_000L;      // 1970-01-04

    @Override
    @Before
    public void setUp() {
        setProperty(PropertyKey.CAIRO_PAGE_FRAME_REDUCE_QUEUE_CAPACITY, 2);
        factoryProvider = SlotGatedWorkStealingStrategy.newFactoryProvider();
        // Route counters are guarded off in production; nearly every test here asserts them, so the
        // class enables the guard once instead of 100+ per-test try/finally blocks.
        // testRouteCountersStayIdleAtProductionDefaults pins the production default locally.
        SymbolPatternIndexRecordCursorFactory.isRouteCounterEnabled = true;
        super.setUp();
    }

    @Override
    @After
    public void tearDown() throws Exception {
        SymbolPatternIndexRecordCursorFactory.isRouteCounterEnabled = false;
        super.tearDown();
    }

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

    private long countSymbolKeyScans(String query, SqlExecutionContextImpl executionContext) throws SqlException {
        try (RecordCursorFactory factory = engine.select(query, executionContext)) {
            return countSymbolKeyScans(factory, executionContext, null);
        }
    }

    private long countSymbolKeyScans(
            String query,
            SqlExecutionContextImpl executionContext,
            CharSequence expected
    ) throws SqlException {
        try (RecordCursorFactory factory = engine.select(query, executionContext)) {
            return countSymbolKeyScans(factory, executionContext, expected);
        }
    }

    private long countSymbolKeyScans(
            RecordCursorFactory factory,
            SqlExecutionContextImpl executionContext,
            CharSequence expected
    ) throws SqlException {
        AbstractLikeSymbolFunctionFactory.testSymbolKeyScans.set(0);
        AbstractLikeSymbolFunctionFactory.isSymbolKeyScanCounterEnabled = true;
        try (RecordCursor cursor = factory.getCursor(executionContext)) {
            if (expected != null) {
                assertCursor(expected, cursor, factory.getMetadata(), true);
            } else {
                //noinspection StatementWithEmptyBody
                while (cursor.hasNext()) {
                }
            }
        } finally {
            AbstractLikeSymbolFunctionFactory.isSymbolKeyScanCounterEnabled = false;
        }
        return AbstractLikeSymbolFunctionFactory.testSymbolKeyScans.get();
    }

    private Function findFilter(RecordCursorFactory factory) {
        for (RecordCursorFactory current = factory; current != null; current = current.getBaseFactory()) {
            if (current.getFilter() != null) {
                return current.getFilter();
            }
        }
        Assert.fail("no filter found under " + factory.getClass().getSimpleName());
        return null;
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
                    .assertsPlanContaining("CoveringIndex", "filter: sym matches pattern");
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

    // Plan assertions ensure that zero route counters do not pass vacuously.
    @Test
    public void testRouteCountersStayIdleAtProductionDefaults() throws Exception {
        assertMemoryLeak(() -> {
            // Covering and fallback scan routes, through AdaptiveSymbolPatternRecordCursorFactory.
            execute("CREATE TABLE tcov (sym SYMBOL INDEX TYPE POSTING INCLUDE (price), price DOUBLE, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("INSERT INTO tcov VALUES ('AA', 1.0, 0), ('AB', 2.0, 1)");
            execute("INSERT INTO tcov SELECT 'BA', x::DOUBLE, timestamp_sequence(2, 1) FROM long_sequence(1_000)");
            // Bitmap index route, through SymbolPatternIndexRecordCursorFactory.
            execute("CREATE TABLE tidx (sym SYMBOL INDEX, v LONG, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("INSERT INTO tidx SELECT 'A', x, timestamp_sequence(0, 1) FROM long_sequence(5)");
            execute("INSERT INTO tidx SELECT 'B', x, timestamp_sequence(5, 1) FROM long_sequence(95)");

            assertQuery("SELECT sum(price) FROM tcov WHERE sym LIKE 'A%'").noLeakCheck().assertsPlanContaining("CoveringIndex");
            assertQuery("SELECT sym, v FROM tidx WHERE sym LIKE 'A%'").noLeakCheck().assertsPlanContaining("SymbolPatternIndex");

            // setUp() enables the guard for the rest of the class; this test pins the production
            // default (false) to prove an unflagged open leaves the counters alone.
            SymbolPatternIndexRecordCursorFactory.isRouteCounterEnabled = false;
            try {
                AdaptiveSymbolPatternRecordCursorFactory.resetTestCounters();
                SymbolPatternIndexRecordCursorFactory.resetTestCounters();
                select("SELECT sum(price) FROM tcov WHERE sym LIKE 'A%'");        // covering route
                select("SELECT sum(price) FROM tcov WHERE sym LIKE 'B%'");        // fallback scan route
                select("SELECT sym, v FROM tidx WHERE sym LIKE 'A%' ORDER BY v"); // bitmap index route
                Assert.assertEquals(0, AdaptiveSymbolPatternRecordCursorFactory.testCoveringInvocations.get());
                Assert.assertEquals(0, AdaptiveSymbolPatternRecordCursorFactory.testScanInvocations.get());
                Assert.assertEquals(0, SymbolPatternIndexRecordCursorFactory.testFallbackInvocations.get());
                Assert.assertEquals(0, SymbolPatternIndexRecordCursorFactory.testIndexInvocations.get());
            } finally {
                SymbolPatternIndexRecordCursorFactory.isRouteCounterEnabled = true;
            }
        });
    }

    // A commit between estimation and delegate acquisition must not mix table snapshots.
    @Test
    public void testCommitAtEstimateReaderReturnKeepsCoveringRouteCoherent() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE tc (sym SYMBOL INDEX TYPE POSTING INCLUDE (v), v LONG, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("INSERT INTO tc VALUES ('AA', 1, 0), ('BB', 2, 86_400_000_000)");

            AdaptiveSymbolPatternRecordCursorFactory.resetTestCounters();
            assertCoherentSnapshotUnderCommitAtReaderReturn(
                    "tc",
                    "SELECT sym, v FROM tc WHERE sym LIKE 'A%' ORDER BY v",
                    3,
                    4,
                    1,
                    "sym\tv\nAA\t1\n",
                    "sym\tv\nAA\t1\nAA\t3\nAC\t4\n"
            );
            Assert.assertTrue(
                    "the probe must have taken the covering route",
                    AdaptiveSymbolPatternRecordCursorFactory.testCoveringInvocations.get() > 0
            );
        });
    }

    // The fallback filter and scan must use one symbol-table snapshot across a concurrent commit.
    @Test
    public void testCommitAtEstimateReaderReturnKeepsFallbackScanCoherent() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE tf (sym SYMBOL INDEX, v LONG, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("INSERT INTO tf SELECT 'AA', x, timestamp_sequence(0, 1) FROM long_sequence(5)");
            execute("INSERT INTO tf SELECT 'BB', 5 + x, timestamp_sequence(86_400_000_000, 1) FROM long_sequence(5)");

            AdaptiveSymbolPatternRecordCursorFactory.resetTestCounters();
            assertCoherentSnapshotUnderCommitAtReaderReturn(
                    "tf",
                    "SELECT sym, v FROM tf WHERE sym LIKE 'A%' ORDER BY v",
                    11,
                    12,
                    1,
                    "sym\tv\nAA\t1\nAA\t2\nAA\t3\nAA\t4\nAA\t5\n",
                    "sym\tv\nAA\t1\nAA\t2\nAA\t3\nAA\t4\nAA\t5\nAA\t11\nAC\t12\n"
            );
            Assert.assertTrue(
                    "the probe must have fallen back to the scan route",
                    AdaptiveSymbolPatternRecordCursorFactory.testScanInvocations.get() > 0
            );
        });
    }

    // A commit between estimation and delegate acquisition must not mix table snapshots.
    @Test
    public void testCommitAtEstimateReaderReturnKeepsIndexRouteCoherent() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE ti (sym SYMBOL INDEX, v LONG, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("INSERT INTO ti VALUES ('AA', 1, 0), ('BB', 2, 86_400_000_000)");

            SymbolPatternIndexRecordCursorFactory.resetTestCounters();
            assertCoherentSnapshotUnderCommitAtReaderReturn(
                    "ti",
                    "SELECT sym, v FROM ti WHERE sym LIKE 'A%' ORDER BY v",
                    3,
                    4,
                    1,
                    "sym\tv\nAA\t1\n",
                    "sym\tv\nAA\t1\nAA\t3\nAC\t4\n"
            );
            Assert.assertTrue(
                    "the probe must have taken the bitmap index route",
                    SymbolPatternIndexRecordCursorFactory.testIndexInvocations.get() > 0
            );
        });
    }

    // Every mode must hand the estimate cursor to the delegate and open one reader per cursor.
    @Test
    public void testEstimateHandsItsCursorToTheDelegateAcrossExecutionModes() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE tw (sym SYMBOL INDEX, v LONG, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("INSERT INTO tw VALUES ('AA', 1, 0), ('BB', 2, 86_400_000_000)");
            drainWalQueue();
            assertRowsAndReaderAcquisitions(
                    "tw",
                    "SELECT sym, v FROM tw WHERE sym LIKE 'A%' ORDER BY v",
                    1,
                    "sym\tv\nAA\t1\n"
            );

            execute("CREATE TABLE ti (sym SYMBOL INDEX, v LONG, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("INSERT INTO ti VALUES ('AA', 1, 0), ('AB', 2, 86_400_000_000), ('BB', 3, 172_800_000_000)");
            assertRowsAndReaderAcquisitions(
                    "ti",
                    "SELECT sym, v FROM ti WHERE sym LIKE 'A%' AND ts IN '1970-01-01' ORDER BY v",
                    1,
                    "sym\tv\nAA\t1\n"
            );
            assertRowsAndReaderAcquisitions(
                    "ti",
                    "SELECT sym, v FROM ti WHERE sym NOT LIKE 'A%' ORDER BY v",
                    1,
                    "sym\tv\nBB\t3\n"
            );
            assertRowsAndReaderAcquisitions(
                    "ti",
                    "SELECT sym, v FROM ti WHERE sym LIKE 'A%' ORDER BY ts DESC",
                    1,
                    "sym\tv\nAB\t2\nAA\t1\n"
            );
        });
    }

    // The commit callback must run after the descending scan drains its single reader.
    @Test
    public void testCommitAtReaderReturnKeepsDescendingScanCoherent() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE td (sym SYMBOL INDEX, v LONG, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("INSERT INTO td VALUES ('AA', 1, 0), ('BB', 2, 86_400_000_000)");

            assertCoherentSnapshotUnderCommitAtReaderReturn(
                    "td",
                    "SELECT sym, v FROM td WHERE sym LIKE 'A%' ORDER BY ts DESC",
                    3,
                    4,
                    1,
                    "sym\tv\nAA\t1\n",
                    "sym\tv\nAC\t4\nAA\t3\nAA\t1\n"
            );
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

    @Test
    public void testBitmapRoutesServeConvertedParquetAndNativePartitions() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (sym SYMBOL INDEX, v LONG, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            // Day 1 (1970-01-01, converted below): 2000 dominant 'AA' rows keep both patterns
            // far under the 5% index-route admission share; the sparse matches carry marker v
            // values so a row dropped from the parquet leg is visible in the literal expectation.
            execute("INSERT INTO t SELECT 'AA', x, timestamp_sequence(0, 1_000) FROM long_sequence(2_000)");
            execute("INSERT INTO t VALUES ('BA', 9_001, 3_000_000_000), ('BB', 9_002, 3_000_001_000), (NULL, 9_003, 3_000_002_000)");
            // Day 2 (1970-01-02) stays native: the active partition of a non-WAL table cannot convert.
            execute("INSERT INTO t SELECT 'AA', 2_000 + x, timestamp_sequence(86_400_000_000, 1_000) FROM long_sequence(2_000)");
            execute("INSERT INTO t VALUES ('BA', 9_004, 90_000_000_000), ('BB', 9_005, 90_000_001_000), (NULL, 9_006, 90_000_002_000)");

            execute("ALTER TABLE t CONVERT PARTITION TO PARQUET LIST '1970-01-01'");
            // The parquet leg must not be silently native: a non-WAL convert skips the active
            // partition without failing, so pin the format of both partitions.
            assertQuery("SELECT name, isParquet FROM table_partitions('t')")
                    .noRandomAccess()
                    .expectSize()
                    .noLeakCheck()
                    .returns("""
                            name\tisParquet
                            1970-01-01\ttrue
                            1970-01-02\tfalse
                            """);

            // Positive pattern: one serial cursor open, so the index route counts exactly once.
            SymbolPatternIndexRecordCursorFactory.resetTestCounters();
            final String positive = select("SELECT sym, v FROM t WHERE sym LIKE 'B%' ORDER BY v");
            TestUtils.assertEquals("""
                    sym\tv
                    BA\t9001
                    BB\t9002
                    BA\t9004
                    BB\t9005
                    """, positive);
            Assert.assertEquals(
                    "the positive pattern must take the bitmap index route over the mixed table",
                    1, SymbolPatternIndexRecordCursorFactory.testIndexInvocations.get()
            );
            Assert.assertEquals(0, SymbolPatternIndexRecordCursorFactory.testFallbackInvocations.get());
            TestUtils.assertEquals(
                    select("SELECT /*+ no_symbol_pattern_index(t) */ sym, v FROM t WHERE sym LIKE 'B%' ORDER BY v"),
                    positive
            );

            // Negated pattern: the complement key set is {BA, BB, NULL}, so the NULL-symbol rows
            // (9003, 9006) must come back from the parquet and the native partition alike.
            SymbolPatternIndexRecordCursorFactory.resetTestCounters();
            final String negated = select("SELECT sym, v FROM t WHERE sym NOT LIKE 'A%' ORDER BY v");
            TestUtils.assertEquals("""
                    sym\tv
                    BA\t9001
                    BB\t9002
                    \t9003
                    BA\t9004
                    BB\t9005
                    \t9006
                    """, negated);
            Assert.assertEquals(
                    "the negated pattern must take the bitmap index route over the mixed table",
                    1, SymbolPatternIndexRecordCursorFactory.testIndexInvocations.get()
            );
            Assert.assertEquals(0, SymbolPatternIndexRecordCursorFactory.testFallbackInvocations.get());
            TestUtils.assertEquals(
                    select("SELECT /*+ no_symbol_pattern_index(t) */ sym, v FROM t WHERE sym NOT LIKE 'A%' ORDER BY v"),
                    negated
            );
        });
    }

    @Test
    public void testCoveringRouteServesConvertedParquetAndNativePartitions() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE tc (sym SYMBOL INDEX TYPE POSTING INCLUDE (price), price DOUBLE, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            // Day 1 (converted): matched share 4/4004 sits far under the 2% covering admission share.
            execute("INSERT INTO tc SELECT 'AA', 1.0, timestamp_sequence(0, 1_000) FROM long_sequence(2_000)");
            execute("INSERT INTO tc VALUES ('BA', 10.0, 3_000_000_000), ('BB', 20.0, 3_000_001_000)");
            // Day 2 stays native.
            execute("INSERT INTO tc SELECT 'AA', 1.0, timestamp_sequence(86_400_000_000, 1_000) FROM long_sequence(2_000)");
            execute("INSERT INTO tc VALUES ('BA', 30.0, 90_000_000_000), ('BB', 40.0, 90_000_001_000)");

            execute("ALTER TABLE tc CONVERT PARTITION TO PARQUET LIST '1970-01-01'");
            assertQuery("SELECT name, isParquet FROM table_partitions('tc')")
                    .noRandomAccess()
                    .expectSize()
                    .noLeakCheck()
                    .returns("""
                            name\tisParquet
                            1970-01-01\ttrue
                            1970-01-02\tfalse
                            """);

            AdaptiveSymbolPatternRecordCursorFactory.resetTestCounters();
            final String covered = select("SELECT sum(price) FROM tc WHERE sym LIKE 'B%'");
            // 10 + 20 from the parquet day, 30 + 40 from the native day.
            TestUtils.assertEquals("sum\n100.0\n", covered);
            Assert.assertTrue(
                    "the covered pattern must take the covering route over the mixed table",
                    AdaptiveSymbolPatternRecordCursorFactory.testCoveringInvocations.get() > 0
            );
            Assert.assertEquals(0, AdaptiveSymbolPatternRecordCursorFactory.testScanInvocations.get());
            TestUtils.assertEquals(
                    select("SELECT /*+ no_symbol_pattern_index(tc) no_covering(tc) */ sum(price) FROM tc WHERE sym LIKE 'B%'"),
                    covered
            );
        });
    }

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

    // Interval frame cursors report size() == -1, so estimation must count their frames.
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

    @Test
    public void testZeroFrameIntervalAppliesEffectiveKeyProbeCap() throws Exception {
        node1.setProperty(PropertyKey.CAIRO_SQL_SYMBOL_PATTERN_INDEX_THRESHOLD, "4");
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (sym SYMBOL INDEX, v LONG, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("INSERT INTO t VALUES "
                    + "('keyA1', 1, '2024-01-01'), ('keyA2', 2, '2024-01-01'), ('keyA3', 3, '2024-01-01'), "
                    + "('keyB1', 4, '2024-01-01'), ('keyB2', 5, '2024-01-01'), ('keyB3', 6, '2024-01-01'), "
                    + "('keyB4', 7, '2024-01-01'), (NULL, 8, '2024-01-01')");

            // IntervalPartitionFrameCursor reports an unknown size and yields no frames here. The key
            // cap must still apply before the frame loop, at the same strict-greater-than boundary.
            assertZeroFramePatternRoute("sym LIKE 'keyB%'", true);       // four effective keys
            assertZeroFramePatternRoute("sym LIKE 'key%'", false);      // seven effective keys
            final String overBudgetPlan = select(
                    "EXPLAIN SELECT v FROM t WHERE sym LIKE 'key%' AND ts IN '1990-01-01'"
            );
            Assert.assertFalse(overBudgetPlan, overBudgetPlan.contains("Index forward scan"));
            assertZeroFramePatternRoute("sym NOT LIKE 'keyB%'", true);  // three keys plus NULL
            assertZeroFramePatternRoute("sym NOT LIKE 'keyA%'", false); // four keys plus NULL
        });
    }

    // Full cursors enforce the frame cap eagerly; interval cursors enforce it while iterating.
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

    // Interval cursors must apply the frame cap to in-range partitions, not the whole table.
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

    // A stolen filter relies on the frame wrapper to rebuild keys on every cursor open.
    @Test
    public void testBindVariableKeysRefreshOnStolenFilterCursorReuse() throws Exception {
        assertMemoryLeak(() -> {
            createSelfFilteringPatternFixture();
            bindVariableService.setStr("pattern", "a%");
            final String query = "SELECT k, sum(v) FROM t WHERE sym LIKE :pattern ORDER BY k";
            final String oracle = "SELECT /*+ no_symbol_pattern_index(t) */ k, sum(v) FROM t WHERE sym LIKE :pattern ORDER BY k";
            // 'a%' is 9% of the table and 'c%' is 1%, so the two patterns straddle the estimate's
            // policy threshold - but under a stealing parent both must run the scan route, since the
            // steal took the index delegate away. 'b%' is the 90% majority, and null/'' match nothing.
            final ObjList<String> patterns = new ObjList<>();
            patterns.add("a%");
            patterns.add("c%");
            patterns.add("b%");
            patterns.add("a%");
            patterns.add(null);
            patterns.add("");

            try (RecordCursorFactory factory = engine.select(query, sqlExecutionContext)) {
                planSink.of(factory, sqlExecutionContext);
                final String plan = planSink.getSink().toString();
                TestUtils.assertContains(plan, "Async Group By");
                Assert.assertFalse(
                        "a stolen filter leaves no adaptive factory in the plan, so the steal is what is under test: " + plan,
                        Chars.contains(plan, "AdaptiveSymbolPattern")
                );

                for (int i = 0, n = patterns.size(); i < n; i++) {
                    final String pattern = patterns.getQuick(i);
                    bindVariableService.setStr("pattern", pattern);
                    TestUtils.assertEquals(
                            "re-bind " + i + " to " + pattern,
                            select(oracle),
                            printFactory(factory)
                    );
                }
            }
        });
    }

    // Prepared factories retain old per-key factories, but the heap must open only live keys.
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
            execute("insert into t select rnd_symbol('AA','AB','BA'), x, timestamp_sequence(0, 60_000_000) from long_sequence(200)");
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
     * <p>
     * Every range runs a second time against a three-hop budget, the cap the adaptive estimate spends
     * on the same seeks. A capped count may refuse to answer, but it may never answer WRONGLY: the
     * short budget has to produce either the identical exact count or
     * {@link AbstractPostingIndexReader#ESTIMATE_REJECT}.
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
                        final String message = tableName + " key=" + key + " lo=" + lo + " hi=" + hi;
                        Assert.assertEquals(
                                message,
                                viaCursor,
                                index.countMatchesInRange(key, lo, hi, BitmapIndexUtils.UNBOUNDED_BLOCK_HOPS)
                        );
                        final long capped = index.countMatchesInRange(key, lo, hi, 3);
                        Assert.assertTrue(
                                message + " capped=" + capped,
                                capped == viaCursor || capped == AbstractPostingIndexReader.ESTIMATE_REJECT
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

    @Test
    public void testEstimatorBoundsColumnTopIndexEntryTraversal() throws Exception {
        node1.setProperty(PropertyKey.CAIRO_SQL_SYMBOL_PATTERN_INDEX_THRESHOLD, "3");
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (v LONG, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("INSERT INTO t SELECT x, timestamp_sequence(0, 1_000) FROM long_sequence(200)");
            execute("ALTER TABLE t ADD COLUMN sym SYMBOL INDEX");
            execute("INSERT INTO t SELECT 200 + x, timestamp_sequence(86_400_000_000, 1_000), CASE WHEN x = 1 THEN 'AA' ELSE 'BB' END FROM long_sequence(10)");

            final String query = "SELECT sym, v FROM t WHERE sym NOT LIKE 'A%' ORDER BY v";
            final String expected = select("SELECT /*+ no_symbol_pattern_index(t) */ sym, v FROM t WHERE sym NOT LIKE 'A%' ORDER BY v");

            AdaptiveSymbolPatternRecordCursorFactory.resetTestCounters();
            SymbolPatternIndexRecordCursorFactory.resetTestCounters();
            AdaptiveSymbolPatternRecordCursorFactory.isEstimatorCounterEnabled = true;
            try {
                TestUtils.assertEquals(expected, select(query));
                Assert.assertEquals(
                        "column-top metadata must avoid traversal",
                        0,
                        AdaptiveSymbolPatternRecordCursorFactory.testEstimatorIndexEntryReads.get()
                );
                Assert.assertTrue(
                        "the exact broad column-top count must select the scan",
                        SymbolPatternIndexRecordCursorFactory.testFallbackInvocations.get() > 0
                );
                Assert.assertEquals(0, SymbolPatternIndexRecordCursorFactory.testIndexInvocations.get());
            } finally {
                AdaptiveSymbolPatternRecordCursorFactory.isEstimatorCounterEnabled = false;
                AdaptiveSymbolPatternRecordCursorFactory.resetTestCounters();
                SymbolPatternIndexRecordCursorFactory.resetTestCounters();
            }
        });
    }

    @Test
    public void testEstimatorAdmitsExactColumnTopTraversalBelowBudget() throws Exception {
        node1.setProperty(PropertyKey.CAIRO_SQL_SYMBOL_PATTERN_INDEX_THRESHOLD, "3");
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (v LONG, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("INSERT INTO t VALUES (1, 0), (2, 1)");
            execute("ALTER TABLE t ADD COLUMN sym SYMBOL INDEX");
            execute("INSERT INTO t SELECT 2 + x, timestamp_sequence(86_400_000_000, 1_000), 'AA' FROM long_sequence(100)");

            final String query = "SELECT sym, v FROM t WHERE sym NOT LIKE 'A%' ORDER BY v";
            final String expected = select("SELECT /*+ no_symbol_pattern_index(t) */ sym, v FROM t WHERE sym NOT LIKE 'A%' ORDER BY v");

            AdaptiveSymbolPatternRecordCursorFactory.resetTestCounters();
            SymbolPatternIndexRecordCursorFactory.resetTestCounters();
            AdaptiveSymbolPatternRecordCursorFactory.isEstimatorCounterEnabled = true;
            try {
                TestUtils.assertEquals(expected, select(query));
                Assert.assertEquals(
                        "exact column-top metadata must avoid traversal",
                        0,
                        AdaptiveSymbolPatternRecordCursorFactory.testEstimatorIndexEntryReads.get()
                );
                Assert.assertEquals(0, SymbolPatternIndexRecordCursorFactory.testFallbackInvocations.get());
                Assert.assertTrue(
                        "an exact selective column-top estimate must use the index",
                        SymbolPatternIndexRecordCursorFactory.testIndexInvocations.get() > 0
                );
            } finally {
                AdaptiveSymbolPatternRecordCursorFactory.isEstimatorCounterEnabled = false;
                AdaptiveSymbolPatternRecordCursorFactory.resetTestCounters();
                SymbolPatternIndexRecordCursorFactory.resetTestCounters();
            }
        });
    }

    @Test
    public void testEstimatorAdmitsSelectivePostingRangePastTraversalCap() throws Exception {
        node1.setProperty(PropertyKey.CAIRO_SQL_SYMBOL_PATTERN_INDEX_THRESHOLD, "3");
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (sym SYMBOL INDEX TYPE POSTING INCLUDE (v), v LONG, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("INSERT INTO t SELECT 'ZZ', x, timestamp_sequence(0, 1) FROM long_sequence(99)");
            execute("INSERT INTO t SELECT 'AA', 99 + x, timestamp_sequence(99, 1) FROM long_sequence(5)");
            execute("INSERT INTO t SELECT 'ZZ', 104 + x, timestamp_sequence(104, 1) FROM long_sequence(396)");

            final String query = "SELECT v FROM t WHERE sym LIKE 'A%' AND ts >= 100 AND ts < 500 ORDER BY v";

            AdaptiveSymbolPatternRecordCursorFactory.resetTestCounters();
            SymbolPatternIndexRecordCursorFactory.resetTestCounters();
            AdaptiveSymbolPatternRecordCursorFactory.isEstimatorCounterEnabled = true;
            try {
                TestUtils.assertEquals("""
                        v
                        101
                        102
                        103
                        104
                        """, select(query));
                Assert.assertEquals(
                        "metadata must avoid traversal for a selective clipped POSTING range",
                        0,
                        AdaptiveSymbolPatternRecordCursorFactory.testEstimatorIndexEntryReads.get()
                );
                Assert.assertEquals(0, SymbolPatternIndexRecordCursorFactory.testFallbackInvocations.get());
                Assert.assertTrue(
                        "a selective cap-plus-one clipped POSTING range must use the covering route",
                        AdaptiveSymbolPatternRecordCursorFactory.testCoveringInvocations.get() > 0
                );
                Assert.assertEquals(0, AdaptiveSymbolPatternRecordCursorFactory.testScanInvocations.get());

                AdaptiveSymbolPatternRecordCursorFactory.resetTestCounters();
                SymbolPatternIndexRecordCursorFactory.resetTestCounters();
                TestUtils.assertEquals("""
                        v
                        101
                        102
                        103
                        104
                        """, select("SELECT v FROM t WHERE sym NOT LIKE 'Z%' AND ts >= 100 AND ts < 500 ORDER BY v"));
                Assert.assertEquals(0, AdaptiveSymbolPatternRecordCursorFactory.testEstimatorIndexEntryReads.get());
                Assert.assertTrue(SymbolPatternIndexRecordCursorFactory.testFallbackInvocations.get() > 0);
                Assert.assertEquals(0, AdaptiveSymbolPatternRecordCursorFactory.testCoveringInvocations.get());
                Assert.assertTrue(AdaptiveSymbolPatternRecordCursorFactory.testScanInvocations.get() > 0);
            } finally {
                AdaptiveSymbolPatternRecordCursorFactory.isEstimatorCounterEnabled = false;
                AdaptiveSymbolPatternRecordCursorFactory.resetTestCounters();
                SymbolPatternIndexRecordCursorFactory.resetTestCounters();
            }
        });
    }

    @Test
    public void testEstimatorAdmitsLargeClippedSparsePostingRange() throws Exception {
        node1.setProperty(PropertyKey.CAIRO_SQL_SYMBOL_PATTERN_INDEX_THRESHOLD, "3");
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (sym SYMBOL INDEX TYPE POSTING INCLUDE (v), v LONG, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("INSERT INTO t SELECT 'ZZ', x, timestamp_sequence(0, 1) FROM long_sequence(999)");
            execute("INSERT INTO t SELECT 'AA', 999 + x, timestamp_sequence(999, 1) FROM long_sequence(500)");
            execute("INSERT INTO t SELECT 'ZZ', 1_499 + x, timestamp_sequence(1_499, 1) FROM long_sequence(1_501)");

            final String query = "SELECT v FROM t WHERE sym LIKE 'A%' AND ts >= 1_495 AND ts < 3_000 ORDER BY v";
            AdaptiveSymbolPatternRecordCursorFactory.resetTestCounters();
            SymbolPatternIndexRecordCursorFactory.resetTestCounters();
            AdaptiveSymbolPatternRecordCursorFactory.isEstimatorCounterEnabled = true;
            try {
                TestUtils.assertEquals("""
                        v
                        1496
                        1497
                        1498
                        1499
                        """, select(query));
                Assert.assertEquals(
                        "metadata must avoid traversal for a large clipped sparse POSTING range",
                        0,
                        AdaptiveSymbolPatternRecordCursorFactory.testEstimatorIndexEntryReads.get()
                );
                Assert.assertEquals(0, SymbolPatternIndexRecordCursorFactory.testFallbackInvocations.get());
                Assert.assertTrue(
                        "a large sparse generation clipped to four rows must use the covering route",
                        AdaptiveSymbolPatternRecordCursorFactory.testCoveringInvocations.get() > 0
                );
                Assert.assertEquals(0, AdaptiveSymbolPatternRecordCursorFactory.testScanInvocations.get());
            } finally {
                AdaptiveSymbolPatternRecordCursorFactory.isEstimatorCounterEnabled = false;
                AdaptiveSymbolPatternRecordCursorFactory.resetTestCounters();
                SymbolPatternIndexRecordCursorFactory.resetTestCounters();
            }
        });
    }

    @Test
    public void testLegacyEfRoutesClippedScanAndUnclippedCovering() throws Exception {
        node1.setProperty(PropertyKey.CAIRO_SQL_SYMBOL_PATTERN_INDEX_THRESHOLD, "3");
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (sym SYMBOL INDEX TYPE POSTING EF INCLUDE (v), v LONG, ts TIMESTAMP) TIMESTAMP(ts)");
            PostingIndexUtils.isEfRankTrailerEnabled = false;
            try {
                execute("""
                        INSERT INTO t
                        SELECT CASE WHEN x % 100 = 0 THEN 'AA' ELSE 'ZZ' END, x, timestamp_sequence(1, 1)
                        FROM long_sequence(10_000)
                        """);
            } finally {
                PostingIndexUtils.isEfRankTrailerEnabled = true;
            }
            engine.releaseAllReaders();
            engine.releaseAllWriters();

            final String clippedQuery = "SELECT v FROM t WHERE sym LIKE 'A%' AND ts >= 5000 AND ts < 6000 ORDER BY v";
            AdaptiveSymbolPatternRecordCursorFactory.resetTestCounters();
            SymbolPatternIndexRecordCursorFactory.resetTestCounters();
            AdaptiveSymbolPatternRecordCursorFactory.isEstimatorCounterEnabled = true;
            try {
                assertQuery(clippedQuery).returns("""
                        v
                        5000
                        5100
                        5200
                        5300
                        5400
                        5500
                        5600
                        5700
                        5800
                        5900
                        """);
                AdaptiveSymbolPatternRecordCursorFactory.resetTestCounters();
                SymbolPatternIndexRecordCursorFactory.resetTestCounters();
                select(clippedQuery);
                Assert.assertEquals("legacy EF rejection must not traverse estimator entries",
                        0, AdaptiveSymbolPatternRecordCursorFactory.testEstimatorIndexEntryReads.get());
                Assert.assertEquals("the adaptive estimator must inspect the SQL frame",
                        1, AdaptiveSymbolPatternRecordCursorFactory.testEstimatorFramesWalked.get());
                Assert.assertTrue("clipped legacy EF must select scan",
                        AdaptiveSymbolPatternRecordCursorFactory.testScanInvocations.get() > 0);
                Assert.assertEquals(0, AdaptiveSymbolPatternRecordCursorFactory.testCoveringInvocations.get());
                Assert.assertTrue(SymbolPatternIndexRecordCursorFactory.testFallbackInvocations.get() > 0);
            } finally {
                AdaptiveSymbolPatternRecordCursorFactory.isEstimatorCounterEnabled = false;
                AdaptiveSymbolPatternRecordCursorFactory.resetTestCounters();
                SymbolPatternIndexRecordCursorFactory.resetTestCounters();
            }

            final String coveringQuery = "SELECT sum(v) FROM t WHERE sym LIKE 'A%'";
            assertQuery(coveringQuery)
                    .noLeakCheck()
                    .assertsPlanContaining("CoveringIndex", "PageFrame");
            AdaptiveSymbolPatternRecordCursorFactory.resetTestCounters();
            SymbolPatternIndexRecordCursorFactory.resetTestCounters();
            assertQuery(coveringQuery)
                    .noRandomAccess()
                    .expectSize()
                    .returns("""
                            sum
                            505000
                            """);
            Assert.assertTrue(
                    "the full legacy EF generation must use countMatchesClamped/selectKthMatch on the covering route",
                    AdaptiveSymbolPatternRecordCursorFactory.testCoveringInvocations.get() > 0
            );
            Assert.assertEquals(0, AdaptiveSymbolPatternRecordCursorFactory.testScanInvocations.get());
            Assert.assertEquals(0, SymbolPatternIndexRecordCursorFactory.testFallbackInvocations.get());
        });
    }

    @Test
    public void testEstimatorBoundsMixedPostingIndexEntryTraversal() throws Exception {
        node1.setProperty(PropertyKey.CAIRO_SQL_SYMBOL_PATTERN_INDEX_THRESHOLD, "3");
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (sym SYMBOL INDEX TYPE POSTING INCLUDE (v), v LONG, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("INSERT INTO t SELECT 'AA', x, timestamp_sequence(0, 1_000) FROM long_sequence(500)");
            execute("INSERT INTO t SELECT 'ZZ', 500 + x, timestamp_sequence(500_000, 1_000) FROM long_sequence(500)");

            final String interval = "ts >= 100_000 AND ts < 300_000";
            final String query = "SELECT sym, v FROM t WHERE sym LIKE 'A%' AND " + interval + " ORDER BY v";
            final String expected = select("SELECT /*+ no_symbol_pattern_index(t) */ sym, v FROM t WHERE sym LIKE 'A%' AND " + interval + " ORDER BY v");

            AdaptiveSymbolPatternRecordCursorFactory.resetTestCounters();
            SymbolPatternIndexRecordCursorFactory.resetTestCounters();
            AdaptiveSymbolPatternRecordCursorFactory.isEstimatorCounterEnabled = true;
            try {
                TestUtils.assertEquals(expected, select(query));
                Assert.assertEquals(
                        "mixed posting metadata must avoid traversal",
                        0,
                        AdaptiveSymbolPatternRecordCursorFactory.testEstimatorIndexEntryReads.get()
                );
                Assert.assertTrue(
                        "the exact broad mixed-posting count must select the scan",
                        SymbolPatternIndexRecordCursorFactory.testFallbackInvocations.get() > 0
                );
                Assert.assertEquals(0, SymbolPatternIndexRecordCursorFactory.testIndexInvocations.get());
            } finally {
                AdaptiveSymbolPatternRecordCursorFactory.isEstimatorCounterEnabled = false;
                AdaptiveSymbolPatternRecordCursorFactory.resetTestCounters();
                SymbolPatternIndexRecordCursorFactory.resetTestCounters();
            }
        });
    }

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

    // Probe work must scale with the selected range, not the key's complete posting list.
    @Test
    public void testEstimatorBoundsBitmapBlockHops() throws Exception {
        node1.setProperty(PropertyKey.CAIRO_SQL_SYMBOL_PATTERN_INDEX_THRESHOLD, "16");
        assertMemoryLeak(() -> {
            // One YEAR partition each, so all of the hot key's rows land on a single posting chain:
            // about 79 value blocks for the short table and about 782 for the long one, at the
            // default block size of 256.
            execute("CREATE TABLE t_short (sym SYMBOL INDEX, v LONG, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY YEAR");
            execute("INSERT INTO t_short SELECT 'HOT', x, timestamp_sequence(0, 1_000) FROM long_sequence(20_000)");
            execute("CREATE TABLE t_long (sym SYMBOL INDEX, v LONG, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY YEAR");
            execute("INSERT INTO t_long SELECT 'HOT', x, timestamp_sequence(0, 1_000) FROM long_sequence(200_000)");
            execute("CREATE TABLE t_wal (sym SYMBOL INDEX, v LONG, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY YEAR WAL");
            execute("INSERT INTO t_wal SELECT 'HOT', x, timestamp_sequence(0, 1_000) FROM long_sequence(200_000)");
            drainWalQueue();

            final String shortQuery = "SELECT v FROM t_short WHERE sym LIKE 'H%' AND ts >= 10_000_000 AND ts < 10_005_000";
            final String longQuery = "SELECT v FROM t_long WHERE sym LIKE 'H%' AND ts >= 100_000_000 AND ts < 100_005_000";
            final String walQuery = "SELECT v FROM t_wal WHERE sym LIKE 'H%' AND ts >= 100_000_000 AND ts < 100_005_000";
            assertQuery(shortQuery).returns("v\n10001\n10002\n10003\n10004\n10005\n");
            assertQuery(longQuery).returns("v\n100001\n100002\n100003\n100004\n100005\n");
            assertQuery(walQuery).returns("v\n100001\n100002\n100003\n100004\n100005\n");

            AdaptiveSymbolPatternRecordCursorFactory.resetTestCounters();
            SymbolPatternIndexRecordCursorFactory.resetTestCounters();
            AdaptiveSymbolPatternRecordCursorFactory.isEstimatorCounterEnabled = true;
            BitmapIndexFwdReader.isBlockHopCounterEnabled = true;
            try {
                BitmapIndexFwdReader.resetTestCounters();
                select(shortQuery);
                final long shortHops = BitmapIndexFwdReader.testRangeCountBlockHops.get();

                BitmapIndexFwdReader.resetTestCounters();
                select(longQuery);
                final long longHops = BitmapIndexFwdReader.testRangeCountBlockHops.get();

                BitmapIndexFwdReader.resetTestCounters();
                select(walQuery);
                final long walHops = BitmapIndexFwdReader.testRangeCountBlockHops.get();

                Assert.assertEquals(
                        "a WAL table's index must bound the probe the same way",
                        longHops,
                        walHops
                );
                Assert.assertEquals(
                        "a ten times longer posting list must not cost the probe ten times more block hops",
                        shortHops,
                        longHops
                );
                Assert.assertTrue(
                        "the configured probe budget of 16 must bound the block hops, got " + longHops,
                        longHops <= 16
                );
                // Guards the guard: a shape that never reached the estimate would hop zero blocks
                // too. Five matched rows out of five selected sit far above the admitted share, so
                // the estimate must have run on all three intervals and sent every one to the scan.
                Assert.assertEquals(
                        "the estimate must have costed one interval frame per query",
                        3,
                        AdaptiveSymbolPatternRecordCursorFactory.testEstimatorFramesWalked.get()
                );
                Assert.assertEquals(
                        "the estimate must reject the index route without walking index entries",
                        0,
                        AdaptiveSymbolPatternRecordCursorFactory.testEstimatorIndexEntryReads.get()
                );
                Assert.assertEquals(
                        "a key matching every selected row must not take the index route",
                        0,
                        SymbolPatternIndexRecordCursorFactory.testIndexInvocations.get()
                );
                Assert.assertTrue(
                        "the rejected route must land on the scan",
                        SymbolPatternIndexRecordCursorFactory.testFallbackInvocations.get() > 0
                );
            } finally {
                BitmapIndexFwdReader.isBlockHopCounterEnabled = false;
                BitmapIndexFwdReader.resetTestCounters();
                AdaptiveSymbolPatternRecordCursorFactory.isEstimatorCounterEnabled = false;
                AdaptiveSymbolPatternRecordCursorFactory.resetTestCounters();
                SymbolPatternIndexRecordCursorFactory.resetTestCounters();
            }
        });
    }

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

    // Covering and bitmap routes intentionally use different selectivity thresholds.
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
            execute("insert into t select 'A' || (x%200), x, timestamp_sequence(0, 60_000_000) from long_sequence(6000)");
            execute("insert into t select 'B' || (x%50), x, timestamp_sequence(600_000_000_000, 60_000_000) from long_sequence(3000)");

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
            execute("insert into t select 'A' || (x%150), x, timestamp_sequence(0, 60_000_000) from long_sequence(4500)");
            execute("insert into t select 'B' || (x%150), x, timestamp_sequence(600_000_000_000, 60_000_000) from long_sequence(3000)");

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

    @Test
    public void testStartsWithMatchesScanFilter_indexPath() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table t (sym symbol index, v long, ts timestamp) timestamp(ts) partition by day");
            execute("insert into t select rnd_symbol('AA','AB','BA','BB','AC'), x, timestamp_sequence(0, 60_000_000) from long_sequence(2000)");
            // Ground truth: force the scan+filter plan with the opt-out hint (hints go right after SELECT).
            String expected = select("select /*+ no_symbol_pattern_index(t) */ sym, v, ts from t where sym like 'A%' order by ts, v");
            String actual = select("select sym, v, ts from t where sym like 'A%' order by ts, v");
            io.questdb.test.tools.TestUtils.assertEquals(expected, actual);
        });
    }

    @Test
    public void testPlanUsesSymbolPatternIndex() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table t (sym symbol index, v long, ts timestamp) timestamp(ts) partition by day");
            execute("insert into t select rnd_symbol('AA','AB','BA'), x, timestamp_sequence(0, 60_000_000) from long_sequence(100)");
            assertQuery("select sym, v from t where sym like 'A%'").noLeakCheck().assertsPlanContaining("SymbolPatternIndex");
            assertQuery("select /*+ no_symbol_pattern_index(t) */ sym, v from t where sym like 'A%'").noLeakCheck().assertsPlanNotContaining("SymbolPatternIndex");
        });
    }

    @Test
    public void testResidualFilterMatchesScanFilter() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table t (sym symbol index, v long, ts timestamp) timestamp(ts) partition by day");
            execute("insert into t select rnd_symbol('AA','AB','BA','BB','AC'), x, timestamp_sequence(0, 60_000_000) from long_sequence(2000)");
            String expected = select("select /*+ no_symbol_pattern_index(t) */ sym, v, ts from t where sym like 'A%' and v > 1000 order by ts, v");
            String actual = select("select sym, v, ts from t where sym like 'A%' and v > 1000 order by ts, v");
            io.questdb.test.tools.TestUtils.assertEquals(expected, actual);
            assertQuery("select sym, v from t where sym like 'A%' and v > 1000").noLeakCheck().assertsPlanContaining("SymbolPatternIndex");
        });
    }

    @Test
    public void testRegexAndIlikeMatchScanFilter() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table t (sym symbol index, v long, ts timestamp) timestamp(ts) partition by day");
            execute("insert into t select rnd_symbol('AA','ab','BA','Bb','aC'), x, timestamp_sequence(0, 60_000_000) from long_sequence(1500)");
            String reExpected = select("select /*+ no_symbol_pattern_index(t) */ sym, v, ts from t where sym ~ '^A' order by ts, v");
            String reActual = select("select sym, v, ts from t where sym ~ '^A' order by ts, v");
            io.questdb.test.tools.TestUtils.assertEquals(reExpected, reActual);

            String ilExpected = select("select /*+ no_symbol_pattern_index(t) */ sym, v, ts from t where sym ilike 'a%' order by ts, v");
            String ilActual = select("select sym, v, ts from t where sym ilike 'a%' order by ts, v");
            io.questdb.test.tools.TestUtils.assertEquals(ilExpected, ilActual);
        });
    }

    @Test
    public void testInvalidPatternLimitCompilationClosesPartitionFactory() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (sym SYMBOL INDEX, v LONG, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");

            final int[] partitionFactoryCloseCount = new int[1];
            FullPartitionFrameCursorFactory.setCloseObserverForTesting(factory -> partitionFactoryCloseCount[0]++);
            try {
                assertExceptionNoLeakCheck(
                        "SELECT * FROM t WHERE sym LIKE 'a%' LIMIT 5 + 0.3",
                        44,
                        "invalid type: DOUBLE"
                );
            } finally {
                FullPartitionFrameCursorFactory.clearCloseObserverForTesting();
            }
            Assert.assertEquals(1, partitionFactoryCloseCount[0]);
        });
    }

    @Test
    public void testNegativeLimitPatternUsesBackwardLimitedFilter() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (sym SYMBOL INDEX, v LONG, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("""
                    INSERT INTO t VALUES
                        ('aa', 1, 0),
                        ('zz', 2, 1),
                        ('ab', 3, 2),
                        (null, 4, 3),
                        ('ba', 5, 4),
                        ('ac', 6, 5),
                        ('zz', 7, 6),
                        ('ad', 8, 7)
                    """);

            final String query = "SELECT sym, v FROM t WHERE sym LIKE 'a%' LIMIT -3";
            assertQuery(query)
                    .noLeakCheck()
                    .assertsPlanContaining("Async Filter workers: 1", "limit: 3", "Row backward scan");
            assertQuery(query).expectSize().returns("sym\tv\nab\t3\nac\t6\nad\t8\n");

            assertQuery("SELECT sym, v FROM t WHERE sym LIKE 'a%'")
                    .withPlanContaining("AdaptiveSymbolPattern")
                    .returns("sym\tv\naa\t1\nab\t3\nac\t6\nad\t8\n");
            assertQuery("SELECT sym, v FROM t WHERE sym LIKE 'a%' LIMIT 2")
                    .withPlanContaining("AdaptiveSymbolPattern")
                    .returns("sym\tv\naa\t1\nab\t3\n");
            assertQuery("SELECT sym, v FROM t WHERE sym LIKE 'a%' LIMIT 1+1")
                    .withPlanContaining("AdaptiveSymbolPattern")
                    .returns("sym\tv\naa\t1\nab\t3\n");
            assertQuery("SELECT sym, v FROM t WHERE sym LIKE 'a%' LIMIT 0")
                    .expectSize()
                    .returns("sym\tv\n");
            assertQuery("SELECT sym, v FROM t WHERE sym LIKE 'a%' LIMIT -(1+2)")
                    .expectSize()
                    .withPlanContaining("limit: 3", "Row backward scan")
                    .withPlanNotContaining("AdaptiveSymbolPattern")
                    .returns("sym\tv\nab\t3\nac\t6\nad\t8\n");
            assertQuery("SELECT sym, v FROM t WHERE sym LIKE 'a%' LIMIT null::long")
                    .expectSize()
                    .withPlanContaining("AdaptiveSymbolPattern")
                    .returns("sym\tv\naa\t1\nab\t3\nac\t6\nad\t8\n");
            assertQuery("SELECT sym, v FROM t WHERE sym LIKE 'a%' ORDER BY ts DESC LIMIT -2")
                    .expectSize()
                    .withPlanContaining("limit: 2", "Row forward scan")
                    .withPlanNotContaining("AdaptiveSymbolPattern")
                    .returns("sym\tv\nab\t3\naa\t1\n");

            final int maxNegativeLimit = configuration.getSqlMaxNegativeLimit();
            final String overflowQuery = "SELECT sym, v FROM t WHERE sym LIKE 'a%' LIMIT -" + (maxNegativeLimit + 1);
            try (RecordCursorFactory factory = engine.select(overflowQuery, sqlExecutionContext)) {
                try (RecordCursor ignored = factory.getCursor(sqlExecutionContext)) {
                    Assert.fail("negative LIMIT above the configured maximum must fail");
                }
            } catch (SqlException e) {
                TestUtils.assertContains(
                        e.getFlyweightMessage(),
                        "absolute LIMIT value is too large, maximum allowed value: " + maxNegativeLimit
                );
                Assert.assertEquals(overflowQuery.indexOf('-'), e.getPosition());
            }
        });
    }

    @Test
    public void testPatternRuntimeAndCoveringLimitModes() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (sym SYMBOL INDEX, v LONG, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("INSERT INTO t VALUES ('aa', 1, 0), ('zz', 2, 1), ('ab', 3, 2), (null, 4, 3), ('ac', 5, 4)");

            final String runtimeQuery = "SELECT sym, v FROM t WHERE sym LIKE 'a%' LIMIT :lim";
            bindVariableService.setLong("lim", -2);
            assertQuery(runtimeQuery)
                    .expectSize()
                    .withPlanContaining("limit: 2", "Row backward scan")
                    .withPlanNotContaining("AdaptiveSymbolPattern")
                    .returns("sym\tv\nab\t3\nac\t5\n");
            bindVariableService.setLong("lim", 2);
            assertQuery(runtimeQuery)
                    .withPlanContaining("limit: 2", "Row forward scan")
                    .withPlanNotContaining("AdaptiveSymbolPattern")
                    .returns("sym\tv\naa\t1\nab\t3\n");
            bindVariableService.setLong("lim", 0);
            assertQuery(runtimeQuery).returns("sym\tv\n");
            bindVariableService.setLong("lim", Numbers.LONG_NULL);
            assertQuery(runtimeQuery)
                    .withPlanNotContaining("AdaptiveSymbolPattern")
                    .returns("sym\tv\naa\t1\nab\t3\nac\t5\n");

            execute("CREATE TABLE tc (sym SYMBOL INDEX TYPE POSTING INCLUDE (v), v LONG, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("INSERT INTO tc SELECT * FROM t");
            assertQuery("SELECT sym, v FROM tc WHERE sym LIKE 'a%'")
                    .withPlanContaining("AdaptiveSymbolPattern", "CoveringIndex")
                    .returns("sym\tv\naa\t1\nab\t3\nac\t5\n");
            assertQuery("SELECT sym, v FROM tc WHERE sym LIKE 'a%' LIMIT 2")
                    .withPlanContaining("AdaptiveSymbolPattern", "CoveringIndex")
                    .returns("sym\tv\naa\t1\nab\t3\n");
            assertQuery("SELECT sym, v FROM tc WHERE sym LIKE 'a%' LIMIT -2")
                    .expectSize()
                    .withPlanContaining("limit: 2", "Row backward scan")
                    .withPlanNotContaining("AdaptiveSymbolPattern", "CoveringIndex")
                    .returns("sym\tv\nab\t3\nac\t5\n");
        });
    }

    @Test
    public void testDescOrderAvoidsSortWhenPatternRouteCannotStreamBackward() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (sym SYMBOL INDEX, v LONG, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("INSERT INTO t VALUES ('aa', 1, 0), ('ba', 2, 1), ('ab', 3, 2), (null, 4, 3)");

            assertQuery("SELECT sym, v FROM t WHERE sym LIKE 'a%' ORDER BY ts DESC")
                    .withPlanContaining("Row backward scan")
                    .withPlanNotContaining("Sort", "AdaptiveSymbolPattern")
                    .returns("sym\tv\nab\t3\naa\t1\n");
            assertQuery("SELECT sym, v FROM t WHERE sym LIKE 'a%'")
                    .withPlanContaining("AdaptiveSymbolPattern")
                    .returns("sym\tv\naa\t1\nab\t3\n");
            assertQuery("SELECT sym, v FROM t WHERE sym LIKE 'a%' ORDER BY ts")
                    .withPlanContaining("AdaptiveSymbolPattern")
                    .withPlanNotContaining("Sort")
                    .returns("sym\tv\naa\t1\nab\t3\n");
            assertQuery("SELECT sym, v FROM t WHERE sym LIKE 'a%' ORDER BY ts DESC LIMIT 1")
                    .expectSize()
                    .withPlanContaining("Async Top K")
                    .returns("sym\tv\nab\t3\n");
            assertQuery("SELECT sym, v FROM t WHERE sym LIKE 'a%' ORDER BY ts DESC LIMIT -1")
                    .expectSize()
                    .withPlanContaining("limit: 1", "Row forward scan")
                    .withPlanNotContaining("AdaptiveSymbolPattern")
                    .returns("sym\tv\naa\t1\n");
        });
    }

    @Test
    public void testDescOrderMatchesScanFilter() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table t (sym symbol index, v long, ts timestamp) timestamp(ts) partition by day");
            execute("insert into t select rnd_symbol('AA','AB','BA','BB','AC'), x, timestamp_sequence(0, 60_000_000) from long_sequence(2000)");
            String expected = select("select /*+ no_symbol_pattern_index(t) */ sym, v, ts from t where sym like 'A%' order by ts desc");
            String actual = select("select sym, v, ts from t where sym like 'A%' order by ts desc");
            io.questdb.test.tools.TestUtils.assertEquals(expected, actual);
        });
    }

    @Test
    public void testNegationLiftedToIndex() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table t (sym symbol index, v long, ts timestamp) timestamp(ts) partition by day");
            execute("insert into t select rnd_symbol('AA','AB','BA','BB','AC'), x, timestamp_sequence(0, 60_000_000) from long_sequence(1500)");
            assertQuery("select sym, v from t where sym not like 'A%'").noLeakCheck().assertsPlanContaining("SymbolPatternIndex");
            assertQuery("select /*+ no_symbol_pattern_index(t) */ sym, v from t where sym not like 'A%'").noLeakCheck().assertsPlanNotContaining("SymbolPatternIndex");
            String expected = select("select /*+ no_symbol_pattern_index(t) */ sym, v, ts from t where sym not like 'A%' order by ts, v");
            String actual = select("select sym, v, ts from t where sym not like 'A%' order by ts, v");
            io.questdb.test.tools.TestUtils.assertEquals(expected, actual);
        });
    }

    // Adaptive plans print every delegate, so only the AdaptiveSymbolPattern node proves routing.
    @Test
    public void testNoIndexHintDisablesSymbolPatternRoute() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (sym SYMBOL INDEX, v LONG, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("INSERT INTO t SELECT rnd_symbol('AA','AB','BA','BB'), x, timestamp_sequence(0, 60_000_000) FROM long_sequence(1_000)");
            execute("INSERT INTO t SELECT null, x, timestamp_sequence(1_000L*60_000_000, 60_000_000) FROM long_sequence(100)");

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

    @Test
    public void testNotLikeMatchesScanFilter_indexPath() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table t (sym symbol index, v long, ts timestamp) timestamp(ts) partition by day");
            execute("insert into t select rnd_symbol('AA','AB','BA','BB','AC'), x, timestamp_sequence(0, 60_000_000) from long_sequence(2000)");
            execute("insert into t select null, x, timestamp_sequence(2000L*60_000_000, 60_000_000) from long_sequence(300)");
            String expected = select("select /*+ no_symbol_pattern_index(t) */ sym, v, ts from t where sym not like 'A%' order by ts, v");
            String actual = select("select sym, v, ts from t where sym not like 'A%' order by ts, v");
            io.questdb.test.tools.TestUtils.assertEquals(expected, actual);
        });
    }

    @Test
    public void testNotLikePlanUsesSymbolPatternIndex() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table t (sym symbol index, v long, ts timestamp) timestamp(ts) partition by day");
            execute("insert into t select rnd_symbol('AA','AB','BA'), x, timestamp_sequence(0, 60_000_000) from long_sequence(100)");
            assertQuery("select sym, v from t where sym not like 'A%'").noLeakCheck().assertsPlanContaining("SymbolPatternIndex");
            assertQuery("select /*+ no_symbol_pattern_index(t) */ sym, v from t where sym not like 'A%'").noLeakCheck().assertsPlanNotContaining("SymbolPatternIndex");
        });
    }

    // The !~ operator has a binary AST shape distinct from NOT applied to regex.
    @Test
    public void testNotRegexMatchesScanFilter_indexPath() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table t (sym symbol index, v long, ts timestamp) timestamp(ts) partition by day");
            execute("insert into t select rnd_symbol('AA','AB','BA','BB','AC'), x, timestamp_sequence(0, 60_000_000) from long_sequence(2000)");
            execute("insert into t select null, x, timestamp_sequence(2000L*60_000_000, 60_000_000) from long_sequence(300)");
            assertQuery("select sym, v from t where sym !~ '^A'").noLeakCheck().assertsPlanContaining("SymbolPatternIndex");
            assertQuery("select /*+ no_symbol_pattern_index(t) */ sym, v from t where sym !~ '^A'").noLeakCheck().assertsPlanNotContaining("SymbolPatternIndex");
            String expected = select("select /*+ no_symbol_pattern_index(t) */ sym, v, ts from t where sym !~ '^A' order by ts, v");
            String actual = select("select sym, v, ts from t where sym !~ '^A' order by ts, v");
            io.questdb.test.tools.TestUtils.assertEquals(expected, actual);
        });
    }

    @Test
    public void testNotRegexOperandAcceptanceIsRouteIndependent() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE ti (sym SYMBOL INDEX, v LONG)");
            execute("CREATE TABLE tu (sym SYMBOL, v LONG)");
            execute("INSERT INTO ti VALUES ('AA', 1), ('BB', 2), (NULL, 3)");
            execute("INSERT INTO tu SELECT * FROM ti");

            bindVariableService.setStr(0, "^A");
            final String bindError = "there is no matching operator `!~` with the argument types: SYMBOL !~ STRING";
            final String indexedBind = "SELECT * FROM ti WHERE sym !~ $1";
            final String unindexedBind = "SELECT * FROM tu WHERE sym !~ $1";
            final String noIndexBind = "SELECT /*+ no_index(ti) */ * FROM ti WHERE sym !~ $1";
            final String noPatternIndexBind = "SELECT /*+ no_symbol_pattern_index(ti) */ * FROM ti WHERE sym !~ $1";
            assertExceptionNoLeakCheck(indexedBind, indexedBind.indexOf("!~"), bindError);
            assertExceptionNoLeakCheck(unindexedBind, unindexedBind.indexOf("!~"), bindError);
            assertExceptionNoLeakCheck(noIndexBind, noIndexBind.indexOf("!~"), bindError);
            assertExceptionNoLeakCheck(noPatternIndexBind, noPatternIndexBind.indexOf("!~"), bindError);

            final String dynamicError = "there is no matching operator `!~` with the argument types: SYMBOL !~ SYMBOL";
            final String indexedDynamic = "SELECT * FROM ti WHERE sym !~ sym";
            final String unindexedDynamic = "SELECT * FROM tu WHERE sym !~ sym";
            final String noIndexDynamic = "SELECT /*+ no_index(ti) */ * FROM ti WHERE sym !~ sym";
            final String noPatternIndexDynamic = "SELECT /*+ no_symbol_pattern_index(ti) */ * FROM ti WHERE sym !~ sym";
            assertExceptionNoLeakCheck(indexedDynamic, indexedDynamic.indexOf("!~"), dynamicError);
            assertExceptionNoLeakCheck(unindexedDynamic, unindexedDynamic.indexOf("!~"), dynamicError);
            assertExceptionNoLeakCheck(noIndexDynamic, noIndexDynamic.indexOf("!~"), dynamicError);
            assertExceptionNoLeakCheck(noPatternIndexDynamic, noPatternIndexDynamic.indexOf("!~"), dynamicError);

            final String constantExpected = "sym\tv\nBB\t2\n\t3\n";
            assertQuery("SELECT sym, v FROM ti WHERE sym !~ '^A' ORDER BY v")
                    .noLeakCheck()
                    .withPlanContaining("AdaptiveSymbolPattern")
                    .returns(constantExpected);
            assertQuery("SELECT sym, v FROM tu WHERE sym !~ '^A' ORDER BY v")
                    .noLeakCheck()
                    .withPlanNotContaining("AdaptiveSymbolPattern")
                    .returns(constantExpected);
            assertQuery("SELECT sym, v FROM ti WHERE sym !~ concat('^', 'A') ORDER BY v")
                    .noLeakCheck()
                    .withPlanContaining("AdaptiveSymbolPattern")
                    .returns(constantExpected);
            assertQuery("SELECT /*+ no_index(ti) */ sym, v FROM ti WHERE sym !~ '^A' ORDER BY v")
                    .noLeakCheck()
                    .withPlanNotContaining("AdaptiveSymbolPattern")
                    .returns(constantExpected);
            assertQuery("SELECT /*+ no_symbol_pattern_index(ti) */ sym, v FROM ti WHERE sym !~ '^A' ORDER BY v")
                    .noLeakCheck()
                    .withPlanNotContaining("AdaptiveSymbolPattern")
                    .returns(constantExpected);
            assertQuery("SELECT sym, v FROM ti WHERE sym !~ null ORDER BY v")
                    .noLeakCheck()
                    .withPlanNotContaining("AdaptiveSymbolPattern")
                    .returns("sym\tv\n");

            assertQuery("SELECT sym, v FROM ti WHERE sym ~ $1 ORDER BY v")
                    .noLeakCheck()
                    .withPlanContaining("AdaptiveSymbolPattern")
                    .returns("sym\tv\nAA\t1\n");
            bindVariableService.setStr(0, "A%");
            assertQuery("SELECT sym, v FROM ti WHERE sym LIKE $1 ORDER BY v")
                    .noLeakCheck()
                    .withPlanContaining("AdaptiveSymbolPattern")
                    .returns("sym\tv\nAA\t1\n");
        });
    }

    @Test
    public void testNotRegexOperandAcceptanceWithPatternIndexDisabled() throws Exception {
        node1.setProperty(PropertyKey.CAIRO_SQL_SYMBOL_PATTERN_INDEX_ENABLED, "false");
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (sym SYMBOL INDEX, v LONG)");
            execute("INSERT INTO t VALUES ('AA', 1), ('BB', 2), (NULL, 3)");

            bindVariableService.setStr(0, "^A");
            final String bindQuery = "SELECT * FROM t WHERE sym !~ $1";
            assertExceptionNoLeakCheck(
                    bindQuery,
                    bindQuery.indexOf("!~"),
                    "there is no matching operator `!~` with the argument types: SYMBOL !~ STRING"
            );
            final String dynamicQuery = "SELECT * FROM t WHERE sym !~ sym";
            assertExceptionNoLeakCheck(
                    dynamicQuery,
                    dynamicQuery.indexOf("!~"),
                    "there is no matching operator `!~` with the argument types: SYMBOL !~ SYMBOL"
            );
            assertQuery("SELECT sym, v FROM t WHERE sym !~ '^A' ORDER BY v")
                    .noLeakCheck()
                    .withPlanNotContaining("AdaptiveSymbolPattern")
                    .returns("sym\tv\nBB\t2\n\t3\n");
            assertQuery("SELECT sym, v FROM t WHERE sym ~ $1 ORDER BY v")
                    .noLeakCheck()
                    .withPlanNotContaining("AdaptiveSymbolPattern")
                    .returns("sym\tv\nAA\t1\n");
        });
    }

    // Disable parallel GROUP BY so filter stealing cannot bypass the sequential index cursor.
    @Test
    public void testOrderInvariantModelUsesSequentialScan() throws Exception {
        assertMemoryLeak(() -> {
            createKeyOrderVersusTimestampOrderFixture();
            assertQuery("SELECT sym, v, ts FROM t WHERE sym LIKE 'a%' ORDER BY v")
                    .withPlanContaining("Cursor-order scan")
                    .returns("sym\tv\tts\n" +
                            "ab\t1\t2024-01-01T00:00:00.000000Z\n" +
                            "aa\t2\t2024-01-01T01:00:00.000000Z\n");
            sqlExecutionContext.setParallelGroupByEnabled(false);
            try {
                assertQuery("SELECT sum(v) FROM t WHERE sym LIKE 'a%'")
                        .noRandomAccess()
                        .expectSize()
                        .withPlanContaining("Cursor-order scan")
                        .returns("sum\n3\n");
            } finally {
                sqlExecutionContext.setParallelGroupByEnabled(configuration.isSqlParallelGroupByEnabled());
            }
        });
    }

    // Without ORDER BY, the cursor must still preserve designated-timestamp order.
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

    // Per-key draining changes the LIMIT row set, not merely its order.
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

    @Test
    public void testOrderByTimestampParity() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table t (sym symbol index, v long, ts timestamp) timestamp(ts) partition by day");
            execute("insert into t select rnd_symbol('AA','AB','BA','BB'), x, timestamp_sequence(0, 60_000_000) from long_sequence(3000)");
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

    // Non-matching keys ensure fallback still applies the pattern predicate.
    @Test
    public void testHighSelectivityFallsBackToScan() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table t (sym symbol index, v long, ts timestamp) timestamp(ts) partition by day");
            // 'A' || (x % 150) produces A0..A149 = 150 distinct symbols, all matching 'A%'.
            // 150 > default threshold (100), so the factory must choose the fallback path.
            execute("insert into t select cast('A' || (x % 150) as symbol), x, timestamp_sequence(0, 60_000_000) from long_sequence(1500)");
            // Non-matching rows: without these every row matches 'A%' and a dropped filter is undetectable.
            execute("insert into t select cast('B' || (x % 40) as symbol), x, timestamp_sequence(600_000_000_000, 60_000_000) from long_sequence(800)");
            // Ground truth: force scan+filter with the opt-out hint immediately after SELECT.
            // The vehicle is a projection, not an aggregate: a parallel-eligible aggregate steals the
            // pattern filter at compile time and never opens the factory whose route this test counts.
            String expected = select("select /*+ no_symbol_pattern_index(t) */ sym, v from t where sym like 'A%' order by sym, v");
            SymbolPatternIndexRecordCursorFactory.resetTestCounters();
            String actual = select("select sym, v from t where sym like 'A%' order by sym, v");
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

    @Test
    public void testPositiveOverProbeCapDoesNotRetainEffectiveKeys() throws Exception {
        node1.setProperty(PropertyKey.CAIRO_SQL_SYMBOL_PATTERN_INDEX_THRESHOLD, "16");
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (sym SYMBOL INDEX, v LONG, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            // A1..A16 sit exactly on the probe cap. aZ adds the seventeenth non-NULL key, while the
            // final NULL row pins positive-pattern NULL semantics without changing either key count.
            execute("""
                    INSERT INTO t SELECT
                      CASE WHEN x <= 16 THEN 'A' || x WHEN x < 1_000 THEN 'aZ' ELSE NULL END,
                      x,
                      timestamp_sequence(0, 1_000)
                    FROM long_sequence(1_000)""");

            bindVariableService.setStr("pattern", "A%");
            final String query = "SELECT sym, v FROM t WHERE sym LIKE :pattern ORDER BY v";
            final String oracle = "SELECT /*+ no_symbol_pattern_index(t) */ sym, v FROM t WHERE sym LIKE :pattern ORDER BY v";
            try (RecordCursorFactory factory = engine.select(query, sqlExecutionContext)) {
                final IntList effectiveKeys = getAdaptiveEffectiveKeys(factory);
                final int initialCapacity = effectiveKeys.capacity();

                SymbolPatternIndexRecordCursorFactory.resetTestCounters();
                TestUtils.assertEquals(select(oracle), printFactory(factory));
                Assert.assertEquals("a set exactly at the cap must remain available to the index delegate", 16, effectiveKeys.size());
                Assert.assertEquals(initialCapacity, effectiveKeys.capacity());
                Assert.assertTrue("the equal-cap set must use the index route",
                        SymbolPatternIndexRecordCursorFactory.testIndexInvocations.get() > 0);
                Assert.assertEquals(0, SymbolPatternIndexRecordCursorFactory.testFallbackInvocations.get());

                bindVariableService.setStr("pattern", "%");
                for (int open = 0; open < 2; open++) {
                    SymbolPatternIndexRecordCursorFactory.resetTestCounters();
                    TestUtils.assertEquals(select(oracle), printFactory(factory));
                    Assert.assertEquals("an over-cap positive set must not be copied on open " + open,
                            0, effectiveKeys.size());
                    Assert.assertEquals("an over-cap positive set must not grow retained capacity on open " + open,
                            initialCapacity, effectiveKeys.capacity());
                    Assert.assertTrue("the over-cap set must use the scan route",
                            SymbolPatternIndexRecordCursorFactory.testFallbackInvocations.get() > 0);
                    Assert.assertEquals(0, SymbolPatternIndexRecordCursorFactory.testIndexInvocations.get());
                }

                bindVariableService.setStr("pattern", null);
                TestUtils.assertEquals(select(oracle), printFactory(factory));
                Assert.assertEquals("a NULL pattern must leave no effective keys", 0, effectiveKeys.size());
                Assert.assertEquals(initialCapacity, effectiveKeys.capacity());
            }

            assertOverProbeCapDoesNotRetainEffectiveKeys("sym ILIKE 'a%'");
            assertOverProbeCapDoesNotRetainEffectiveKeys("sym ~ '^[Aa]'");
        });
    }

    @Test
    public void testNegatedHighComplementFallsBackToScan() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table t (sym symbol index, v long, ts timestamp) timestamp(ts) partition by day");
            // 'B' || (x % 150) => B0..B149 = 150 distinct symbols, none matching 'A%'.
            // NOT LIKE 'A%' therefore includes all 150 keys > default threshold (100) => fallback path.
            execute("insert into t select cast('B' || (x % 150) as symbol), x, timestamp_sequence(0, 60_000_000) from long_sequence(1500)");
            // A projection, not an aggregate: a parallel-eligible aggregate steals the pattern filter at
            // compile time and never opens the factory whose route this test counts.
            String expected = select("select /*+ no_symbol_pattern_index(t) */ sym, v from t where sym not like 'A%' order by sym, v");
            SymbolPatternIndexRecordCursorFactory.resetTestCounters();
            try (RecordCursorFactory factory = engine.select(
                    "select sym, v from t where sym not like 'A%' order by sym, v",
                    sqlExecutionContext
            )) {
                RecordCursorFactory current = factory;
                while (current != null && !(current instanceof AdaptiveSymbolPatternRecordCursorFactory)) {
                    current = current.getBaseFactory();
                }
                Assert.assertNotNull("expected an adaptive symbol-pattern factory", current);
                final Field effectiveKeysField = AdaptiveSymbolPatternRecordCursorFactory.class.getDeclaredField("effectiveKeys");
                effectiveKeysField.setAccessible(true);
                final IntList effectiveKeys = (IntList) effectiveKeysField.get(current);
                final int initialCapacity = effectiveKeys.capacity();

                io.questdb.test.tools.TestUtils.assertEquals(expected, printFactory(factory));
                Assert.assertEquals(
                        "an over-budget complement must not grow the retained effective-key list",
                        initialCapacity,
                        effectiveKeys.capacity()
                );
            }
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

    @Test
    public void testParitySweep() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table t (sym symbol index, v long, ts timestamp) timestamp(ts) partition by day");
            execute("insert into t select rnd_symbol('alpha','alto','beta','ALPHA','al_x','gamma',null), x, timestamp_sequence(0, 3_600_000_000) from long_sequence(5000)");
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

    // QuestDB pattern functions do not match NULL, so their negations include NULL rows.
    @Test
    public void testNegationIncludesNullRows_groundTruth() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table t (sym symbol index, v long, ts timestamp) timestamp(ts) partition by day");
            // 3 non-null symbols + explicit NULL symbols
            execute("insert into t select rnd_symbol('alpha','beta','gamma'), x, timestamp_sequence(0, 60_000_000) from long_sequence(300)");
            execute("insert into t select null, x, timestamp_sequence(300L*60_000_000, 60_000_000) from long_sequence(50)");
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

    // LIKE '%' compiles to a not-null check rather than a SymbolKeySetProvider.
    @Test
    public void testNegationParitySweep() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table t (sym symbol index, v long, ts timestamp) timestamp(ts) partition by day");
            execute("insert into t select rnd_symbol('alpha','alto','beta','ALPHA','al_x','gamma'), x, timestamp_sequence(0, 3_600_000_000) from long_sequence(4000)");
            execute("insert into t select null, x, timestamp_sequence(4000*3_600_000_000L, 3_600_000_000) from long_sequence(400)");
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

    @Test
    public void testDeferredSymbolAddedAfterCompile() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table t (sym symbol index, v long, ts timestamp) timestamp(ts) partition by day");
            execute("insert into t select rnd_symbol('AA','BB'), x, timestamp_sequence(0, 60_000_000) from long_sequence(50)");
            // Compile the fast-path factory (engine.select returns a RecordCursorFactory)
            try (RecordCursorFactory factory = engine.select("select sym, v from t where sym like 'A%' order by v", sqlExecutionContext)) {
                // Insert a new matching symbol AFTER the factory was compiled
                execute("insert into t values ('AC', 999, 100_000_000::timestamp)");
                // Execute the cached plan now — must see the new 'AC' row
                String actual = printFactory(factory);
                String expected = select("select /*+ no_symbol_pattern_index(t) */ sym, v from t where sym like 'A%' order by v");
                io.questdb.test.tools.TestUtils.assertEquals(expected, actual);
            }
        });
    }

    // The scan oracle needs both hints because no_symbol_pattern_index does not disable covering.
    @Test
    public void testCoveringPositivePlanAndParity() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table t (sym symbol index type posting include (price), price double, ts timestamp) timestamp(ts) partition by day");
            execute("insert into t select rnd_symbol('AA','AB','BA','BB','AC'), x::double, timestamp_sequence(0, 60_000_000) from long_sequence(2000)");
            // Covered projection (sym known from WHERE, price covered) -> CoveringIndex, not the bitmap SymbolPatternIndex.
            assertQuery("select sym, price from t where sym like 'A%'").noLeakCheck().assertsPlanContaining("CoveringIndex");
            // Ground truth: force a plain scan+filter by disabling BOTH the pattern-index and the covering path.
            String expected = select("select /*+ no_symbol_pattern_index(t) no_covering(t) */ price, sym from t where sym like 'A%' order by price");
            String actual = select("select price, sym from t where sym like 'A%' order by price");
            io.questdb.test.tools.TestUtils.assertEquals(expected, actual);
        });
    }

    @Test
    public void testCoveringPositiveWithResidualParity() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table t (sym symbol index type posting include (price), price double, ts timestamp) timestamp(ts) partition by day");
            execute("insert into t select rnd_symbol('AA','AB','BA','BB','AC'), x::double, timestamp_sequence(0, 60_000_000) from long_sequence(2000)");
            assertQuery("select sym, price from t where sym like 'A%' and price > 1000").noLeakCheck().assertsPlanContaining("CoveringIndex");
            String expected = select("select /*+ no_symbol_pattern_index(t) no_covering(t) */ price, sym from t where sym like 'A%' and price > 1000 order by price");
            String actual = select("select price, sym from t where sym like 'A%' and price > 1000 order by price");
            io.questdb.test.tools.TestUtils.assertEquals(expected, actual);
        });
    }

    @Test
    public void testCoveringPositiveDeferredSymbol() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table t (sym symbol index type posting include (price), price double, ts timestamp) timestamp(ts) partition by day");
            execute("insert into t select rnd_symbol('AA','BB'), x::double, timestamp_sequence(0, 60_000_000) from long_sequence(50)");
            try (RecordCursorFactory factory = engine.select("select price, sym from t where sym like 'A%' order by price", sqlExecutionContext)) {
                // Insert a new matching symbol AFTER the covering factory was compiled.
                execute("insert into t values ('AC', 999.0, 100_000_000::timestamp)");
                String actual = printFactory(factory);
                String expected = select("select /*+ no_symbol_pattern_index(t) no_covering(t) */ price, sym from t where sym like 'A%' order by price");
                io.questdb.test.tools.TestUtils.assertEquals(expected, actual);
            }
        });
    }

    @Test
    public void testCoveringPositivePageFrameAggregationParity() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table t (sym symbol index type posting include (price), price double, ts timestamp) timestamp(ts) partition by day");
            execute("insert into t select rnd_symbol('AA','AB','BA','BB','AC'), x::double, timestamp_sequence(0, 60_000_000) from long_sequence(4000)");
            // GROUP BY on the covered symbol drives page frames (parallel/vectorized aggregation) through the covering factory.
            assertQuery("select sym, sum(price) from t where sym like 'A%'").noLeakCheck().assertsPlanContaining("CoveringIndex");
            String expected = select("select /*+ no_symbol_pattern_index(t) no_covering(t) */ sym, sum(price) s from t where sym like 'A%' order by sym");
            String actual = select("select sym, sum(price) s from t where sym like 'A%' order by sym");
            io.questdb.test.tools.TestUtils.assertEquals(expected, actual);
        });
    }

    @Test
    public void testCoveringNegatedStaysClassic() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table t (sym symbol index type posting include (price), price double, ts timestamp) timestamp(ts) partition by day");
            execute("insert into t select rnd_symbol('AA','AB','BA','BB','AC'), x::double, timestamp_sequence(0, 60_000_000) from long_sequence(1500)");
            execute("insert into t select null, x::double, timestamp_sequence(1500L*60_000_000, 60_000_000) from long_sequence(200)");
            assertQuery("select sym, price from t where sym not like 'A%'").noLeakCheck().assertsPlanContaining("SymbolPatternIndex");
            assertQuery("select sym, price from t where sym not like 'A%'").noLeakCheck().assertsPlanNotContaining("CoveringIndex");
            String expected = select("select /*+ no_symbol_pattern_index(t) no_covering(t) */ price, sym, ts from t where sym not like 'A%' order by ts, price");
            String actual = select("select price, sym, ts from t where sym not like 'A%' order by ts, price");
            io.questdb.test.tools.TestUtils.assertEquals(expected, actual);
        });
    }

    // Excluding ts keeps the projection covered; positive patterns must still exclude NULL keys.
    @Test
    public void testCoveringParitySweep() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table t (sym symbol index type posting include (price), price double, ts timestamp) timestamp(ts) partition by day");
            execute("insert into t select rnd_symbol('alpha','alto','beta','ALPHA','al_x','gamma'), x::double, timestamp_sequence(0, 3_600_000_000L) from long_sequence(4000)");
            // Explicit NULL-symbol rows in a separate partition to prove NULL exclusion on the covered path.
            execute("insert into t select null, x::double, timestamp_sequence(4000*3_600_000_000L, 3_600_000_000L) from long_sequence(300)");
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

    @Test
    public void testCoveringVsBitmapRouting() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table t (sym symbol index type posting include (price), price double, extra long, ts timestamp) timestamp(ts) partition by day");
            execute("insert into t select rnd_symbol('AA','AB','BA'), x::double, x, timestamp_sequence(0, 60_000_000) from long_sequence(500)");
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

    // An explicit timestamp clears timestampRequired, so the factory must advertise forward order.
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

    // Nested explicit timestamps also clear timestampRequired before join-order validation.
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

    // The explicit timestamp clears timestampRequired, leaving factory order as the only guard.
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

    // LIKE residuals own mutable matcher state and require one filter clone per worker.
    @Test
    public void testNonThreadSafeResidualPreservesParallelFilter() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (sym SYMBOL INDEX, txt STRING, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("""
                    INSERT INTO t VALUES
                        ('aa', 'xxaZbyy', 0),
                        ('ab', 'nomatch', 1),
                        ('ba', 'xxaZbyy', 2),
                        ('aa', null, 3),
                        (null, 'xxaZbyy', 4)
                    """);

            assertQuery("SELECT sym, txt FROM t WHERE sym LIKE 'a%' AND txt LIKE '%a_b%'")
                    .withPlanContaining("Async Filter workers: 1")
                    .withPlanNotContaining("AdaptiveSymbolPattern")
                    .returns("sym\ttxt\naa\txxaZbyy\n");
        });
    }

    @Test
    public void testNonThreadSafeResidualPreservesCoveredParallelFilter() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (sym SYMBOL INDEX TYPE POSTING INCLUDE (txt), txt STRING, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("""
                    INSERT INTO t VALUES
                        ('aa', 'xxaZbyy', 0),
                        ('ab', 'nomatch', 1),
                        ('ba', 'xxaZbyy', 2),
                        ('aa', null, 3),
                        (null, 'xxaZbyy', 4)
                    """);
            execute("INSERT INTO t SELECT 'zz', 'nomatch', timestamp_sequence(5, 1) FROM long_sequence(200)");

            final String query = "SELECT sym, txt FROM t WHERE sym LIKE 'a%' AND txt LIKE '%a_b%'";
            try (RecordCursorFactory factory = engine.select(query, sqlExecutionContext)) {
                final AsyncFilterAtom atom = (AsyncFilterAtom) TestUtils.findAtom(factory, query);
                TestUtils.findPerWorkerLocks(factory, query);
                Assert.assertNotSame(atom.getFilter(-1), atom.getFilter(0));
                Assert.assertEquals("PreparedSymbolPatternFilter", atom.getFilter(-1).getClass().getSimpleName());
                Assert.assertEquals("PreparedSymbolPatternFilter", atom.getFilter(0).getClass().getSimpleName());
            }
            assertQuery(query)
                    .noLeakCheck()
                    .assertsPlanContaining("Async Filter workers: 1", "AdaptiveSymbolPattern", "CoveringIndex");
            AdaptiveSymbolPatternRecordCursorFactory.resetTestCounters();
            assertQuery(query).returns("sym\ttxt\naa\txxaZbyy\n");
            Assert.assertTrue(AdaptiveSymbolPatternRecordCursorFactory.testCoveringInvocations.get() > 0);
        });
    }

    @Test
    public void testCoveredWorkerCloneEvaluatesPreparedFilter() throws Exception {
        assertMemoryLeak(() -> {
            final WorkerPool pool = new WorkerPool(() -> 4);
            TestUtils.execute(
                    pool,
                    (testEngine, compiler, executionContext) -> {
                        testEngine.execute(
                                "CREATE TABLE t (sym SYMBOL INDEX TYPE POSTING INCLUDE (txt), txt STRING, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY",
                                executionContext
                        );
                        testEngine.execute("""
                                INSERT INTO t VALUES
                                    ('aa', 'xxaZbyy', 0),
                                    ('ab', 'nomatch', 1),
                                    ('ba', 'xxaZbyy', 2),
                                    (null, 'xxaZbyy', 3)
                                """, executionContext);
                        testEngine.execute(
                                "INSERT INTO t SELECT CASE WHEN x % 100 = 0 THEN 'aa' ELSE 'zz' END, "
                                        + "CASE WHEN x % 100 = 0 THEN 'xxaZbyy' ELSE 'nomatch' END, "
                                        + "timestamp_sequence(4, 1_000_000_000) FROM long_sequence(1_000)",
                                executionContext
                        );

                        final String query = "SELECT ts FROM t WHERE sym LIKE 'a_%' AND txt LIKE '%a_b%' ORDER BY ts";
                        try (RecordCursorFactory factory = compiler.compile(query, executionContext).getRecordCursorFactory()) {
                            final PerWorkerLocks locks = TestUtils.findPerWorkerLocks(factory, query);
                            final CountDownLatch acquired = new CountDownLatch(1);
                            locks.setTestAcquireLatch(acquired);
                            AbstractLikeSymbolFunctionFactory.testSymbolKeyScans.set(0);
                            AbstractLikeSymbolFunctionFactory.isSymbolKeyScanCounterEnabled = true;
                            try (RecordCursor cursor = factory.getCursor(executionContext)) {
                                final Record record = cursor.getRecord();
                                int count = 0;
                                while (cursor.hasNext()) {
                                    if (count++ == 0) {
                                        Assert.assertEquals(0, record.getTimestamp(0));
                                    }
                                }
                                Assert.assertEquals(11, count);
                            } finally {
                                AbstractLikeSymbolFunctionFactory.isSymbolKeyScanCounterEnabled = false;
                                locks.setTestAcquireLatch(null);
                            }
                            Assert.assertEquals(
                                    "the prepared owner must donate keys without a worker clone rescan",
                                    1,
                                    AbstractLikeSymbolFunctionFactory.testSymbolKeyScans.get()
                            );
                            Assert.assertEquals("a worker must acquire the prepared-filter slot", 0, acquired.getCount());
                        }
                    },
                    configuration,
                    LOG
            );
        });
    }

    @Test
    public void testCoveredWorkerCloneEvaluatesConstantRegexProvider() throws Exception {
        assertCoveredWorkerCloneEvaluatesRegexProvider("'^a.*'", null);
    }

    @Test
    public void testCoveredWorkerCloneEvaluatesRuntimeRegexProvider() throws Exception {
        assertCoveredWorkerCloneEvaluatesRegexProvider("$1", "^a.*");
    }

    @Test
    public void testCoveredWorkerClonesInheritPreparedSymbolKeys() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (sym SYMBOL INDEX TYPE POSTING INCLUDE (txt), txt STRING, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("""
                    INSERT INTO t VALUES
                        ('aa', 'xxaZbyy', 0),
                        ('ab', 'nomatch', 1),
                        ('ba', 'xxaZbyy', 2),
                        ('aa', null, 3),
                        (null, 'xxaZbyy', 4)
                    """);
            execute("INSERT INTO t SELECT 'zz', 'nomatch', timestamp_sequence(5, 1) FROM long_sequence(200)");

            final String coveredQuery = "SELECT sym, txt FROM t WHERE sym LIKE 'a_%' AND txt LIKE '%a_b%'";
            final String plainQuery = "SELECT /*+ no_symbol_pattern_index(t) */ sym, txt FROM t WHERE sym LIKE 'a_%' AND txt LIKE '%a_b%'";
            for (int workerCount : new int[]{0, 1, 3}) {
                try (SqlExecutionContextImpl context = TestUtils.createSqlExecutionCtx(engine, workerCount)) {
                    AdaptiveSymbolPatternRecordCursorFactory.resetTestCounters();
                    Assert.assertEquals(
                            "the prepared owner must scan once independently of worker clone count " + workerCount,
                            1,
                            countSymbolKeyScans(coveredQuery, context)
                    );
                    Assert.assertTrue(
                            "the counter probe must take the adaptive covering route",
                            AdaptiveSymbolPatternRecordCursorFactory.testCoveringInvocations.get() > 0
                    );
                    Assert.assertEquals(
                            "the ordinary filter control must also scan once with worker count " + workerCount,
                            1,
                            countSymbolKeyScans(plainQuery, context)
                    );
                }
            }

            final String twoPatternQuery = "SELECT sym, txt FROM t WHERE sym LIKE 'a_%' AND sym LIKE '_a' AND txt LIKE '%a_b%'";
            try (SqlExecutionContextImpl context = TestUtils.createSqlExecutionCtx(engine, 3)) {
                Assert.assertEquals(
                        "each of two pattern conjuncts must scan once, independently of worker count",
                        2,
                        countSymbolKeyScans(twoPatternQuery, context, "sym\ttxt\naa\txxaZbyy\n")
                );
            }
        });
    }

    @Test
    public void testCoveredWorkerCloneStateMatchesReorderedProviders() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (before_sym SYMBOL, indexed_sym SYMBOL INDEX TYPE POSTING INCLUDE (before_sym, after_sym, txt), after_sym SYMBOL, txt STRING, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("""
                    INSERT INTO t VALUES
                        ('bad0', 'apple', 'bad0', 'xxaZbyy', 0),
                        ('bad1', 'banana', 'tail_match', 'nomatch', 1),
                        ('good_match', 'cherry', 'bad2', 'xxaZbyy', 2),
                        ('good_match', 'apple_one', 'tail_match', 'xxaZbyy', 3),
                        (null, null, null, 'xxaZbyy', 4)
                    """);
            execute("INSERT INTO t SELECT 'noise', 'noise', 'noise', 'nomatch', timestamp_sequence(5, 1) FROM long_sequence(200)");

            bindVariableService.setStr("pattern", "a_%");
            final String query = "SELECT before_sym, indexed_sym FROM t "
                    + "WHERE before_sym LIKE 'good_%' AND indexed_sym LIKE :pattern AND txt LIKE '%a_b%'";
            try (SqlExecutionContextImpl context = TestUtils.createSqlExecutionCtx(engine, bindVariableService, 3)) {
                AdaptiveSymbolPatternRecordCursorFactory.resetTestCounters();
                Assert.assertEquals(
                        "each logical provider must donate to its own worker clone",
                        2,
                        countSymbolKeyScans(
                                query,
                                context,
                                "before_sym\tindexed_sym\ngood_match\tapple_one\n"
                        )
                );
                Assert.assertTrue(
                        "the correspondence probe must take the adaptive covering route",
                        AdaptiveSymbolPatternRecordCursorFactory.testCoveringInvocations.get() > 0
                );

                final String sameClassQuery = "SELECT before_sym, indexed_sym FROM t "
                        + "WHERE before_sym LIKE 'good_%' AND indexed_sym LIKE 'a_%' AND txt LIKE '%a_b%'";
                Assert.assertEquals(
                        "same-class providers on different columns must not exchange key sets",
                        2,
                        countSymbolKeyScans(
                                sameClassQuery,
                                context,
                                "before_sym\tindexed_sym\ngood_match\tapple_one\n"
                        )
                );

                final String threeProviderQuery = "SELECT before_sym, indexed_sym, after_sym FROM t "
                        + "WHERE before_sym LIKE 'good_%' AND indexed_sym LIKE :pattern "
                        + "AND after_sym LIKE 'tail_%' AND txt LIKE '%a_b%'";
                Assert.assertEquals(
                        "residual regrouping must preserve all three provider correspondences",
                        3,
                        countSymbolKeyScans(
                                threeProviderQuery,
                                context,
                                "before_sym\tindexed_sym\tafter_sym\ngood_match\tapple_one\ttail_match\n"
                        )
                );
            }
        });
    }

    @Test
    public void testCoveredWorkerCloneStateRefreshesAcrossBindChangesAndDictionaryGrowth() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (sym SYMBOL INDEX TYPE POSTING INCLUDE (txt), txt STRING, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("""
                    INSERT INTO t VALUES
                        ('aa', 'xxaZbyy', 0),
                        ('ab', 'nomatch', 1),
                        ('ba', 'xxaZbyy', 2),
                        (null, 'xxaZbyy', 3)
                    """);
            execute("INSERT INTO t SELECT 'zz', 'nomatch', timestamp_sequence(4, 1) FROM long_sequence(200)");

            final String query = "SELECT sym, txt FROM t WHERE sym LIKE :pattern AND txt LIKE '%a_b%'";
            bindVariableService.setStr("pattern", "a_%");
            try (
                    SqlExecutionContextImpl context = TestUtils.createSqlExecutionCtx(engine, bindVariableService, 3);
                    RecordCursorFactory factory = engine.select(query, context)
            ) {
                Assert.assertEquals(1, countSymbolKeyScans(factory, context, "sym\ttxt\naa\txxaZbyy\n"));

                execute("INSERT INTO t VALUES ('ac', 'qqa1brr', 204), ('zz2', 'nomatch', 205)");
                Assert.assertEquals(
                        "an unchanged bind must rebuild against a grown dictionary only in the owner",
                        1,
                        countSymbolKeyScans(factory, context, "sym\ttxt\naa\txxaZbyy\nac\tqqa1brr\n")
                );

                bindVariableService.setStr("pattern", "b_%");
                Assert.assertEquals(
                        "a changed bind must replace every clone's inherited key set",
                        1,
                        countSymbolKeyScans(factory, context, "sym\ttxt\nba\txxaZbyy\n")
                );

                bindVariableService.setStr("pattern", null);
                Assert.assertEquals(
                        "a NULL bind clears owner and clone key sets without scanning",
                        0,
                        countSymbolKeyScans(factory, context, "sym\ttxt\n")
                );
            }
        });
    }

    @Test
    public void testNonThreadSafeCoveredResidualUnderParallelGroupBy() throws Exception {
        assertMemoryLeak(() -> {
            execute("""
                    CREATE TABLE t (
                        sym SYMBOL INDEX TYPE POSTING INCLUDE (txt, grp, v),
                        txt STRING,
                        grp SYMBOL,
                        v LONG,
                        ts TIMESTAMP
                    ) TIMESTAMP(ts) PARTITION BY DAY
                    """);
            execute("""
                    INSERT INTO t VALUES
                        ('aa', 'xxaZbyy', 'g1', 1, 0),
                        ('ab', 'nomatch', 'g1', 2, 1),
                        ('aa', 'qqa1brr', 'g2', 3, 2),
                        ('ba', 'xxaZbyy', 'g2', 4, 3),
                        ('aa', null, 'g3', 5, 4),
                        (null, 'xxaZbyy', 'g3', 6, 5)
                    """);

            try (TableReader reader = engine.getReader("t")) {
                final FunctionParser parser = new FunctionParser(configuration, engine.getFunctionFactoryCache());
                final QueryModel model = QueryModel.FACTORY.newInstance();
                final ExpressionNode expression;
                try (SqlCompiler compiler = engine.getSqlCompiler()) {
                    expression = compiler.testParseExpression("txt LIKE '%a_b%'", model);
                }
                final Function residual = parser.parseFunction(expression, reader.getMetadata(), sqlExecutionContext);
                try {
                    Assert.assertEquals("ConstLikeStrFunction", residual.getClass().getSimpleName());
                    Assert.assertFalse(residual.isThreadSafe());
                } finally {
                    residual.close();
                }
            }

            final String filteredQuery = "SELECT sym, txt, grp, v FROM t WHERE sym LIKE 'a%' AND txt LIKE '%a_b%'";
            try (RecordCursorFactory factory = engine.select(filteredQuery, sqlExecutionContext)) {
                final Function preparedFilter = findFilter(factory);
                Assert.assertEquals("PreparedSymbolPatternFilter", preparedFilter.getClass().getSimpleName());
                Assert.assertFalse(preparedFilter.isThreadSafe());
            }
            final String scanQuery = "SELECT /*+ no_symbol_pattern_index(t) */ sym, txt, grp, v FROM t WHERE sym LIKE 'a%' AND txt LIKE '%a_b%'";
            try (RecordCursorFactory factory = engine.select(scanQuery, sqlExecutionContext)) {
                final Function fullExpressionFilter = findFilter(factory);
                Assert.assertEquals("AndBooleanFunction", fullExpressionFilter.getClass().getSimpleName());
                Assert.assertFalse(fullExpressionFilter.isThreadSafe());
            }

            assertQuery("SELECT grp, sum(v) FROM t WHERE sym LIKE 'a%' AND txt LIKE '%a_b%' ORDER BY grp")
                    .expectSize()
                    .withPlanContaining("Async Group By", "AdaptiveSymbolPattern", "CoveringIndex")
                    .returns("grp\tsum\ng1\t1\ng2\t3\n");
            assertQuery("SELECT sum(v) FROM t WHERE sym LIKE 'a%' AND txt LIKE '%a_b%'")
                    .expectSize()
                    .noRandomAccess()
                    .withPlanContaining("Async Group By", "AdaptiveSymbolPattern", "CoveringIndex")
                    .returns("sum\n4\n");
            assertQuery("SELECT sym, txt, grp, v FROM t WHERE sym LIKE 'a%' AND txt LIKE '%a_b%' ORDER BY v LIMIT 1")
                    .expectSize()
                    .withPlanContaining("Async Top K", "AdaptiveSymbolPattern", "CoveringIndex")
                    .returns("sym\ttxt\tgrp\tv\naa\txxaZbyy\tg1\t1\n");
        });
    }

    // Nested plan shape matters because the adaptive factory prints every delegate.
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

    // Uncovered posting routes and negated covered routes have no covering delegate.
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

    // Keeping the async filter above the adaptive factory enables downstream filter stealing.
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
                                        AdaptiveSymbolPattern""");

            assertQuery("SELECT sym, count() FROM t WHERE sym LIKE 'a%'")
                    .noLeakCheck()
                    .assertsPlanContaining("Async Group By workers: 1", "AdaptiveSymbolPattern", "CoveringIndex");
        });
    }

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

    // Inject failure before the scan factory and prepared filter transfer ownership.
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

    @Test
    public void testIndexFactoryCloseReleasesResourcesWhenPartitionFactoryCloseThrowsWithSequentialCursor() throws Exception {
        assertIndexFactoryCloseReleasesResourcesOnThrow(OrderByMnemonic.ORDER_BY_INVARIANT);
    }

    // close() cannot retry after a partition-factory failure, so later native owners must still close.
    @Test
    public void testIndexFactoryCloseReleasesResourcesWhenPartitionFactoryCloseThrowsWithHeapCursor() throws Exception {
        assertIndexFactoryCloseReleasesResourcesOnThrow(OrderByMnemonic.ORDER_BY_UNKNOWN);
    }

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

    // The shared provider is thread-safe only after prepare() initializes its matcher-backed keys.
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

    // The opt-out plan proves that this fixture remains eligible for parallel aggregation.
    @Test
    public void testKeyedGroupByOverSelfFilteringPatternRunsParallelAggregate() throws Exception {
        assertMemoryLeak(() -> {
            createSelfFilteringPatternFixture();

            assertQuery("SELECT /*+ no_symbol_pattern_index(t) */ k, sum(v) FROM t WHERE sym LIKE 'a%' ORDER BY k")
                    .noLeakCheck()
                    .assertsPlanContaining("Async Group By");
            assertQuery("SELECT k, sum(v) FROM t WHERE sym LIKE 'a%' ORDER BY k")
                    .noLeakCheck()
                    .assertsPlanContaining("Async Group By");

            // A plan fix that silently changed rows would be worse than the regression it removes, so
            // pin the rows against the opt-out on BOTH routes the estimate can pick: 'a%' is 9% of the
            // table (over the 5% policy, scan route) and 'c%' is 1% (under it, index route).
            TestUtils.assertEquals(
                    select("SELECT /*+ no_symbol_pattern_index(t) */ k, sum(v) FROM t WHERE sym LIKE 'a%' ORDER BY k"),
                    select("SELECT k, sum(v) FROM t WHERE sym LIKE 'a%' ORDER BY k")
            );
            TestUtils.assertEquals(
                    select("SELECT /*+ no_symbol_pattern_index(t) */ k, sum(v) FROM t WHERE sym LIKE 'c%' ORDER BY k"),
                    select("SELECT k, sum(v) FROM t WHERE sym LIKE 'c%' ORDER BY k")
            );

            // Single-threaded: with parallel GROUP BY off no parent steals, so the adaptive factory stays
            // in the plan and both routes still have to agree with the opt-out.
            sqlExecutionContext.setParallelGroupByEnabled(false);
            try {
                assertQuery("SELECT k, sum(v) FROM t WHERE sym LIKE 'a%' ORDER BY k")
                        .noLeakCheck()
                        .assertsPlanContaining("AdaptiveSymbolPattern");
                TestUtils.assertEquals(
                        select("SELECT /*+ no_symbol_pattern_index(t) */ k, sum(v) FROM t WHERE sym LIKE 'a%' ORDER BY k"),
                        select("SELECT k, sum(v) FROM t WHERE sym LIKE 'a%' ORDER BY k")
                );
                TestUtils.assertEquals(
                        select("SELECT /*+ no_symbol_pattern_index(t) */ k, sum(v) FROM t WHERE sym LIKE 'c%' ORDER BY k"),
                        select("SELECT k, sum(v) FROM t WHERE sym LIKE 'c%' ORDER BY k")
                );
            } finally {
                sqlExecutionContext.setParallelGroupByEnabled(configuration.isSqlParallelGroupByEnabled());
            }
        });
    }

    @Test
    public void testKeyedGroupByOverSelfFilteringPatternRunsParallelAggregateOnWalTable() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE tw (ts TIMESTAMP, sym SYMBOL INDEX, k SYMBOL, v LONG) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("""
                    INSERT INTO tw SELECT
                      timestamp_sequence('2024-01-01T00:00:00.000000Z', 400_000_000),
                      CASE WHEN x % 100 = 0 THEN 'c' WHEN x % 10 = 0 THEN 'a' ELSE 'b' END,
                      'k' || (x % 5),
                      x
                    FROM long_sequence(1_000)""");
            drainWalQueue();

            assertQuery("SELECT k, sum(v) FROM tw WHERE sym LIKE 'a%' ORDER BY k")
                    .noLeakCheck()
                    .assertsPlanContaining("Async Group By");
            TestUtils.assertEquals(
                    select("SELECT /*+ no_symbol_pattern_index(tw) */ k, sum(v) FROM tw WHERE sym LIKE 'a%' ORDER BY k"),
                    select("SELECT k, sum(v) FROM tw WHERE sym LIKE 'a%' ORDER BY k")
            );
            TestUtils.assertEquals(
                    select("SELECT /*+ no_symbol_pattern_index(tw) */ k, sum(v) FROM tw WHERE sym LIKE 'c%' ORDER BY k"),
                    select("SELECT k, sum(v) FROM tw WHERE sym LIKE 'c%' ORDER BY k")
            );
        });
    }

    // Only the covered adaptive route supplies page frames while preserving its index route.
    @Test
    public void testKeyedGroupByOverWrappedPatternKeepsBothParallelismAndIndexRoute() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE tc (ts TIMESTAMP, sym SYMBOL INDEX TYPE POSTING INCLUDE (price), price DOUBLE) TIMESTAMP(ts) PARTITION BY DAY");
            execute("""
                    INSERT INTO tc SELECT
                      timestamp_sequence('2024-01-01T00:00:00.000000Z', 400_000_000),
                      CASE WHEN x % 1_000 = 0 THEN 'c' ELSE 'b' END,
                      x::DOUBLE
                    FROM long_sequence(1_000)""");

            assertQuery("SELECT sum(price) FROM tc WHERE sym LIKE 'c%'")
                    .noLeakCheck()
                    .assertsPlanContaining("Async Group By");
            assertQuery("SELECT sum(price) FROM tc WHERE sym LIKE 'c%'")
                    .noLeakCheck()
                    .assertsPlanContaining("AdaptiveSymbolPattern");

            AdaptiveSymbolPatternRecordCursorFactory.resetTestCounters();
            TestUtils.assertEquals(
                    select("SELECT /*+ no_symbol_pattern_index(tc) no_covering(tc) */ sum(price) FROM tc WHERE sym LIKE 'c%'"),
                    select("SELECT sum(price) FROM tc WHERE sym LIKE 'c%'")
            );
            Assert.assertTrue(
                    "an aggregating parent must still reach the covering route in wrapped mode",
                    AdaptiveSymbolPatternRecordCursorFactory.testCoveringInvocations.get() > 0
            );
        });
    }

    @Test
    public void testSampleByOverSelfFilteringPatternRunsParallelAggregate() throws Exception {
        assertMemoryLeak(() -> {
            createSelfFilteringPatternFixture();

            assertQuery("SELECT /*+ no_symbol_pattern_index(t) */ ts, count() FROM t WHERE sym LIKE 'a%' SAMPLE BY 1d")
                    .noLeakCheck()
                    .assertsPlanContaining("Async Group By");
            assertQuery("SELECT ts, count() FROM t WHERE sym LIKE 'a%' SAMPLE BY 1d")
                    .noLeakCheck()
                    .assertsPlanContaining("Async Group By");

            TestUtils.assertEquals(
                    select("SELECT /*+ no_symbol_pattern_index(t) */ ts, count() FROM t WHERE sym LIKE 'a%' SAMPLE BY 1d"),
                    select("SELECT ts, count() FROM t WHERE sym LIKE 'a%' SAMPLE BY 1d")
            );
            TestUtils.assertEquals(
                    select("SELECT /*+ no_symbol_pattern_index(t) */ ts, count() FROM t WHERE sym LIKE 'c%' SAMPLE BY 1d"),
                    select("SELECT ts, count() FROM t WHERE sym LIKE 'c%' SAMPLE BY 1d")
            );
        });
    }

    @Test
    public void testOrderByLimitOverSelfFilteringPatternRunsParallelTopK() throws Exception {
        assertMemoryLeak(() -> {
            createSelfFilteringPatternFixture();

            assertQuery("SELECT /*+ no_symbol_pattern_index(t) */ sym, v FROM t WHERE sym LIKE 'a%' ORDER BY v LIMIT 5")
                    .noLeakCheck()
                    .assertsPlanContaining("Async Top K");
            assertQuery("SELECT sym, v FROM t WHERE sym LIKE 'a%' ORDER BY v LIMIT 5")
                    .noLeakCheck()
                    .assertsPlanContaining("Async Top K");

            TestUtils.assertEquals(
                    select("SELECT /*+ no_symbol_pattern_index(t) */ sym, v FROM t WHERE sym LIKE 'a%' ORDER BY v LIMIT 5"),
                    select("SELECT sym, v FROM t WHERE sym LIKE 'a%' ORDER BY v LIMIT 5")
            );
            TestUtils.assertEquals(
                    select("SELECT /*+ no_symbol_pattern_index(t) */ sym, v FROM t WHERE sym LIKE 'c%' ORDER BY v LIMIT 5"),
                    select("SELECT sym, v FROM t WHERE sym LIKE 'c%' ORDER BY v LIMIT 5")
            );
            TestUtils.assertEquals(
                    select("SELECT /*+ no_symbol_pattern_index(t) */ sym, v FROM t WHERE sym LIKE 'a%' ORDER BY ts DESC LIMIT 5"),
                    select("SELECT sym, v FROM t WHERE sym LIKE 'a%' ORDER BY ts DESC LIMIT 5")
            );
            TestUtils.assertEquals(
                    select("SELECT /*+ no_symbol_pattern_index(t) */ sym, v FROM t WHERE sym LIKE 'c%' ORDER BY ts DESC LIMIT 5"),
                    select("SELECT sym, v FROM t WHERE sym LIKE 'c%' ORDER BY ts DESC LIMIT 5")
            );
        });
    }

    // Time-frame joins rely on the shared frame wrapper to prepare a stolen filter's keys.
    @Test
    public void testAsOfJoinStealsSelfFilteringPatternFromSlave() throws Exception {
        assertMemoryLeak(() -> {
            createSelfFilteringPatternFixture();
            execute("CREATE TABLE m (ts TIMESTAMP, mk SYMBOL, mv LONG) TIMESTAMP(ts) PARTITION BY DAY");
            execute("INSERT INTO m SELECT timestamp_sequence('2024-01-01T00:00:00.000000Z', 700_000_000), 'k' || (x % 5), x FROM long_sequence(200)");

            final String query = "SELECT m.ts, m.mv, t.v FROM m ASOF JOIN (SELECT ts, v FROM t WHERE sym LIKE 'a%') t";
            assertQuery(query).noLeakCheck().assertsPlanContaining("Filtered AsOf Join Fast");
            TestUtils.assertEquals(
                    select(query.replaceFirst("SELECT", "SELECT /*+ no_symbol_pattern_index(t) */")),
                    select(query)
            );

            final String selective = "SELECT m.ts, m.mv, t.v FROM m ASOF JOIN (SELECT ts, v FROM t WHERE sym LIKE 'c%') t";
            TestUtils.assertEquals(
                    select(selective.replaceFirst("SELECT", "SELECT /*+ no_symbol_pattern_index(t) */")),
                    select(selective)
            );
        });
    }

    @Test
    public void testSelectivePatternUnderParallelAggregateForgoesIndexRoute() throws Exception {
        assertMemoryLeak(() -> {
            createSelfFilteringPatternFixture();

            SymbolPatternIndexRecordCursorFactory.resetTestCounters();
            select("SELECT sym, v FROM t WHERE sym LIKE 'c%' ORDER BY v");
            Assert.assertTrue(
                    "the fixture must reach the index route without an aggregating parent",
                    SymbolPatternIndexRecordCursorFactory.testIndexInvocations.get() > 0
            );

            SymbolPatternIndexRecordCursorFactory.resetTestCounters();
            select("SELECT k, sum(v) FROM t WHERE sym LIKE 'c%' ORDER BY k");
            Assert.assertEquals(
                    "a parallel aggregate parent forgoes the index route",
                    0,
                    SymbolPatternIndexRecordCursorFactory.testIndexInvocations.get()
            );
        });
    }

    /**
     * A plain (non-covering) {@code SYMBOL INDEX} table that the adaptive factory enters in
     * self-filtering mode. It carries an over-threshold pattern ('a%', 9% of rows, scan route) and an
     * under-threshold one ('c%', 1%, index route), plus a second symbol column to key a GROUP BY on.
     * Five daily partitions keep the estimate's frame cap (default 100) out of the way.
     */
    private void createSelfFilteringPatternFixture() throws SqlException {
        execute("CREATE TABLE t (ts TIMESTAMP, sym SYMBOL INDEX, k SYMBOL, v LONG) TIMESTAMP(ts) PARTITION BY DAY");
        execute("""
                INSERT INTO t SELECT
                  timestamp_sequence('2024-01-01T00:00:00.000000Z', 400_000_000),
                  CASE WHEN x % 100 = 0 THEN 'c' WHEN x % 10 = 0 THEN 'a' ELSE 'b' END,
                  'k' || (x % 5),
                  x
                FROM long_sequence(1_000)""");
    }

    private void assertCoveredWorkerCloneEvaluatesRegexProvider(String patternExpression, String bindPattern) throws Exception {
        assertMemoryLeak(() -> {
            final WorkerPool pool = new WorkerPool(() -> 4);
            TestUtils.execute(
                    pool,
                    (testEngine, compiler, executionContext) -> {
                        testEngine.execute(
                                "CREATE TABLE t (sym SYMBOL INDEX TYPE POSTING INCLUDE (txt), txt STRING, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY",
                                executionContext
                        );
                        testEngine.execute("""
                                INSERT INTO t VALUES
                                    ('aa', 'xxaZbyy', 0),
                                    ('ab', 'nomatch', 1),
                                    ('ba', 'xxaZbyy', 2),
                                    (null, 'xxaZbyy', 3)
                                """, executionContext);
                        testEngine.execute(
                                "INSERT INTO t SELECT CASE WHEN x % 100 = 0 THEN 'aa' "
                                        + "WHEN x % 100 = 1 THEN 'ba' ELSE 'zz' END, "
                                        + "CASE WHEN x % 100 < 2 THEN 'xxaZbyy' ELSE 'nomatch' END, "
                                        + "timestamp_sequence(4, 1_000_000_000) FROM long_sequence(1_000)",
                                executionContext
                        );
                        if (bindPattern != null) {
                            executionContext.getBindVariableService().setStr(0, bindPattern);
                        }

                        final String query = "SELECT ts FROM t WHERE sym ~ " + patternExpression
                                + " AND txt LIKE '%a_b%' ORDER BY ts";
                        try (RecordCursorFactory factory = compiler.compile(query, executionContext).getRecordCursorFactory()) {
                            final PerWorkerLocks locks = TestUtils.findPerWorkerLocks(factory, query);
                            final CountDownLatch acquired = new CountDownLatch(1);
                            locks.setTestAcquireLatch(acquired);
                            try (RecordCursor cursor = factory.getCursor(executionContext)) {
                                final Record record = cursor.getRecord();
                                int count = 0;
                                while (cursor.hasNext()) {
                                    if (count++ == 0) {
                                        Assert.assertEquals(0, record.getTimestamp(0));
                                    }
                                }
                                Assert.assertEquals(11, count);
                            } finally {
                                locks.setTestAcquireLatch(null);
                            }
                            Assert.assertEquals("a worker must acquire the prepared regex-filter slot", 0, acquired.getCount());

                            testEngine.execute("INSERT INTO t VALUES ('ac', 'xxaZbyy', 1_100_000_000_000)", executionContext);
                            Assert.assertEquals(
                                    "a new cursor must refresh regex keys after dictionary growth",
                                    12,
                                    countRows(factory, executionContext)
                            );
                            if (bindPattern != null) {
                                executionContext.getBindVariableService().setStr(0, "^b.*");
                                Assert.assertEquals(
                                        "a rebound regex must replace every worker key set",
                                        11,
                                        countRows(factory, executionContext)
                                );
                                executionContext.getBindVariableService().setStr(0, null);
                                Assert.assertEquals(
                                        "a NULL regex bind must clear every worker key set",
                                        0,
                                        countRows(factory, executionContext)
                                );
                            }
                        }
                    },
                    configuration,
                    LOG
            );
        });
    }

    private static int countRows(RecordCursorFactory factory, SqlExecutionContext executionContext) throws SqlException {
        int count = 0;
        try (RecordCursor cursor = factory.getCursor(executionContext)) {
            while (cursor.hasNext()) {
                count++;
            }
        }
        return count;
    }

    private static IntList getAdaptiveEffectiveKeys(RecordCursorFactory factory) throws ReflectiveOperationException {
        RecordCursorFactory current = factory;
        while (current != null && !(current instanceof AdaptiveSymbolPatternRecordCursorFactory)) {
            current = current.getBaseFactory();
        }
        Assert.assertNotNull("expected an adaptive symbol-pattern factory", current);
        final Field effectiveKeysField = AdaptiveSymbolPatternRecordCursorFactory.class.getDeclaredField("effectiveKeys");
        effectiveKeysField.setAccessible(true);
        return (IntList) effectiveKeysField.get(current);
    }

    private void assertOverProbeCapDoesNotRetainEffectiveKeys(String predicate) throws Exception {
        final String query = "SELECT sym, v FROM t WHERE " + predicate + " ORDER BY v";
        final String oracle = "SELECT /*+ no_symbol_pattern_index(t) */ sym, v FROM t WHERE " + predicate + " ORDER BY v";
        try (RecordCursorFactory factory = engine.select(query, sqlExecutionContext)) {
            final IntList effectiveKeys = getAdaptiveEffectiveKeys(factory);
            final int initialCapacity = effectiveKeys.capacity();
            final String expected = select(oracle);
            for (int open = 0; open < 2; open++) {
                SymbolPatternIndexRecordCursorFactory.resetTestCounters();
                TestUtils.assertEquals(expected, printFactory(factory));
                Assert.assertEquals("an over-cap set must not be copied for " + predicate + " on open " + open,
                        0, effectiveKeys.size());
                Assert.assertEquals("an over-cap set must not grow retained capacity for " + predicate + " on open " + open,
                        initialCapacity, effectiveKeys.capacity());
                Assert.assertTrue("the over-cap set must use the scan route for " + predicate,
                        SymbolPatternIndexRecordCursorFactory.testFallbackInvocations.get() > 0);
                Assert.assertEquals(0, SymbolPatternIndexRecordCursorFactory.testIndexInvocations.get());
            }
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

    private void assertZeroFramePatternRoute(String patternPredicate, boolean isIndexRouteExpected) throws Exception {
        SymbolPatternIndexRecordCursorFactory.resetTestCounters();
        assertQuery("SELECT v FROM t WHERE " + patternPredicate + " AND ts IN '1990-01-01'")
                .returns("v\n");
        final long indexInvocations = SymbolPatternIndexRecordCursorFactory.testIndexInvocations.get();
        final long fallbackInvocations = SymbolPatternIndexRecordCursorFactory.testFallbackInvocations.get();
        if (isIndexRouteExpected) {
            Assert.assertTrue("expected the index route, got fallback=" + fallbackInvocations, indexInvocations > 0);
            Assert.assertEquals(0, fallbackInvocations);
        } else {
            Assert.assertTrue("expected the fallback scan, got index=" + indexInvocations, fallbackInvocations > 0);
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
     * Drains {@code query} while a writer commits {@code (AA, existingSymbolValue)} and
     * {@code (AC, newSymbolValue)} into {@code tableName} at the first table-reader return the pool
     * reports after the factory has been compiled. That return is the one the adaptive factory's
     * selectivity estimate produces when it releases its own partition-frame cursor, so the commit
     * lands in the window between the estimate and the delegate's own reader acquisition - no sleep
     * and no timing guess.
     * <p>
     * The assertion is the observable one: the drained rows must equal ONE of the two coherent
     * snapshots. Either the query ran entirely before the commit (rows {@code expectedBeforeCommit})
     * or entirely after it ({@code expectedAfterCommit}). A result that matches neither is a query
     * that combined two table snapshots, which is what a second reader acquisition at a newer
     * transaction produces: rows under an already-known symbol key appear while every row under the
     * symbol the same commit introduced is missing.
     * <p>
     * This cannot use the fluent {@code assertQuery(...).returns(...)} builder. The builder drains the
     * cursor twice and compares both passes against one expected string, and this probe deliberately
     * mutates the table during the first pass, so the second pass reads a different (and equally
     * legitimate) snapshot. The check here is a disjunction over two snapshots rather than one fixed
     * result, which the builder cannot express.
     * <p>
     * {@code expectedReaderAcquisitions} pins the mechanism alongside the row set: one acquisition
     * means either the estimate handed its cursor to the delegate or the query used one ordinary scan,
     * so no commit can split the result; two means an adaptive delegate needed a different cursor.
     */
    private void assertCoherentSnapshotUnderCommitAtReaderReturn(
            String tableName,
            String query,
            long existingSymbolValue,
            long newSymbolValue,
            int expectedReaderAcquisitions,
            String expectedBeforeCommit,
            String expectedAfterCommit
    ) throws Exception {
        final AtomicBoolean hasCommitted = new AtomicBoolean();
        final AtomicInteger readerAcquisitions = new AtomicInteger();
        final StringSink localSink = new StringSink();
        try (
                RecordCursorFactory factory = engine.select(query, sqlExecutionContext);
                TableWriter writer = getWriter(tableName)
        ) {
            engine.setPoolListener((factoryType, thread, tableToken, event, segment, position) -> {
                if (factoryType != PoolListener.SRC_READER
                        || tableToken == null
                        || !Chars.equals(tableToken.getTableName(), tableName)) {
                    return;
                }
                if (event == PoolListener.EV_GET) {
                    readerAcquisitions.incrementAndGet();
                }
                if (event == PoolListener.EV_RETURN && hasCommitted.compareAndSet(false, true)) {
                    TableWriter.Row existingSymbolRow = writer.newRow(COMMIT_PROBE_EXISTING_SYMBOL_TIMESTAMP);
                    existingSymbolRow.putSym(0, "AA");
                    existingSymbolRow.putLong(1, existingSymbolValue);
                    existingSymbolRow.append();
                    TableWriter.Row newSymbolRow = writer.newRow(COMMIT_PROBE_NEW_SYMBOL_TIMESTAMP);
                    newSymbolRow.putSym(0, "AC");
                    newSymbolRow.putLong(1, newSymbolValue);
                    newSymbolRow.append();
                    writer.commit();
                }
            });
            try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                println(factory.getMetadata(), cursor, localSink);
            }
        } finally {
            engine.setPoolListener(null);
        }
        Assert.assertTrue("the probe commit never ran, so the test proves nothing", hasCommitted.get());
        Assert.assertEquals(
                "table reader acquisitions while the query ran",
                expectedReaderAcquisitions,
                readerAcquisitions.get()
        );
        final String actual = localSink.toString();
        if (!expectedBeforeCommit.equals(actual) && !expectedAfterCommit.equals(actual)) {
            Assert.fail(
                    "query result matches neither coherent snapshot.\nbefore commit:\n" + expectedBeforeCommit
                            + "after commit:\n" + expectedAfterCommit + "actual:\n" + actual
            );
        }
    }

    /**
     * Drains {@code query} and asserts both its rows and how many table readers the open acquired.
     * One acquisition proves that either the estimate-to-delegate hand-off or an ordinary scan kept
     * the open on one reader; a second acquisition can expose the query to a concurrent commit.
     */
    private void assertRowsAndReaderAcquisitions(
            String tableName,
            String query,
            int expectedReaderAcquisitions,
            String expectedRows
    ) throws Exception {
        final AtomicInteger readerAcquisitions = new AtomicInteger();
        final StringSink localSink = new StringSink();
        try (RecordCursorFactory factory = engine.select(query, sqlExecutionContext)) {
            engine.setPoolListener((factoryType, thread, tableToken, event, segment, position) -> {
                if (factoryType == PoolListener.SRC_READER
                        && event == PoolListener.EV_GET
                        && tableToken != null
                        && Chars.equals(tableToken.getTableName(), tableName)) {
                    readerAcquisitions.incrementAndGet();
                }
            });
            try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                println(factory.getMetadata(), cursor, localSink);
            }
        } finally {
            engine.setPoolListener(null);
        }
        TestUtils.assertEquals(expectedRows, localSink);
        Assert.assertEquals(
                "table reader acquisitions while the query ran",
                expectedReaderAcquisitions,
                readerAcquisitions.get()
        );
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
