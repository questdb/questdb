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
import io.questdb.cairo.GenericRecordMetadata;
import io.questdb.cairo.TableReader;
import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.griffin.FunctionParser;
import io.questdb.griffin.SqlCompiler;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.engine.functions.regex.SymbolKeySetProvider;
import io.questdb.griffin.engine.table.AdaptiveSymbolPatternRecordCursorFactory;
import io.questdb.griffin.engine.table.SymbolPatternIndexRecordCursorFactory;
import io.questdb.griffin.model.ExpressionNode;
import io.questdb.griffin.model.QueryModel;
import io.questdb.std.IntList;
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

            assertQuery("SELECT sum(price) FROM t WHERE sym LIKE 'A%'")
                    .noLeakCheck()
                    .assertsPlanContaining("AdaptiveSymbolPattern");
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
            // conservatively selects the scan even though the posting-row estimate is selective.
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

    // Pins the exact posting-row selectivity boundary in AdaptiveSymbolPatternRecordCursorFactory:
    // the cost cutoff is `matchedRows > maxIndexRows` with maxIndexRows = max(1, totalRows / 4).
    // At the boundary (matchedRows == maxIndexRows) the estimate is still selective and must use the
    // index; one matched row past it must fall back to scan. A regression flipping `>` to `>=` would
    // route the boundary case to the scan and this test would fail. Single matched key keeps the probe
    // count at one, so the default probe budget (100) is never the deciding factor here.
    @Test
    public void testSelectivityBoundaryAtQuarterUsesIndexThenScan() throws Exception {
        assertMemoryLeak(() -> {
            // 16 rows total -> maxIndexRows = 4. Key 'A' has exactly 4 rows == the boundary.
            execute("CREATE TABLE t (sym SYMBOL INDEX, v LONG, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("INSERT INTO t SELECT 'A', x, timestamp_sequence(0, 1) FROM long_sequence(4)");
            execute("INSERT INTO t SELECT 'B', x, timestamp_sequence(4, 1) FROM long_sequence(12)");

            SymbolPatternIndexRecordCursorFactory.resetTestCounters();
            select("SELECT sym, v FROM t WHERE sym LIKE 'A%' ORDER BY v");
            Assert.assertEquals(
                    "matchedRows == maxIndexRows is still selective and must use the index",
                    0,
                    SymbolPatternIndexRecordCursorFactory.testFallbackInvocations.get()
            );
            Assert.assertTrue(SymbolPatternIndexRecordCursorFactory.testIndexInvocations.get() > 0);

            // 16 rows total -> maxIndexRows = 4. Key 'A' has 5 rows, one past the boundary -> scan.
            execute("CREATE TABLE t2 (sym SYMBOL INDEX, v LONG, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("INSERT INTO t2 SELECT 'A', x, timestamp_sequence(0, 1) FROM long_sequence(5)");
            execute("INSERT INTO t2 SELECT 'B', x, timestamp_sequence(5, 1) FROM long_sequence(11)");

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
     * Unordered queries (no ORDER BY, no timestamp requirement) should use the cheaper
     * SequentialRowCursorFactory ("Cursor-order scan") rather than the heap-based merge.
     */
    @Test
    public void testUnorderedUsesSequentialScan() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table t (sym symbol index, v long, ts timestamp) timestamp(ts) partition by day");
            execute("insert into t select rnd_symbol('AA','AB','BA'), x, timestamp_sequence(0, 60000000) from long_sequence(100)");
            // No ORDER BY and no timestamp requirement => cheaper Sequential (Cursor-order) scan.
            assertQuery("select sym, v from t where sym like 'A%'").noLeakCheck().assertsPlanContaining("Cursor-order scan");
        });
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
}
