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

import io.questdb.cairo.GenericRecordMetadata;
import io.questdb.cairo.TableReader;
import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.griffin.FunctionParser;
import io.questdb.griffin.SqlCompiler;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.engine.functions.regex.SymbolKeySetProvider;
import io.questdb.griffin.engine.table.SymbolPatternIndexRecordCursorFactory;
import io.questdb.griffin.model.ExpressionNode;
import io.questdb.griffin.model.QueryModel;
import io.questdb.std.IntList;
import io.questdb.std.str.StringSink;
import io.questdb.test.AbstractCairoTest;
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
    public void testHintConstantWiring() {
        Assert.assertEquals("no_symbol_pattern_index", io.questdb.griffin.SqlHints.NO_SYMBOL_PATTERN_INDEX_HINT);
    }

    @Test
    public void testConfigDefaults() {
        Assert.assertTrue(configuration.isSymbolPatternIndexEnabled());
        Assert.assertEquals(100, configuration.getSymbolPatternIndexThreshold());
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
     * Negation ({@code NOT LIKE}) must NOT be lifted to the index fast path; the plan must fall back to
     * scan+filter and still be correct.
     */
    @Test
    public void testNegationNotLiftedToIndex() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table t (sym symbol index, v long, ts timestamp) timestamp(ts) partition by day");
            execute("insert into t select rnd_symbol('AA','AB','BA','BB','AC'), x, timestamp_sequence(0, 60000000) from long_sequence(1500)");
            assertQuery("select sym, v from t where sym not like 'A%'").noLeakCheck().assertsPlanNotContaining("SymbolPatternIndex");
            String expected = select("select /*+ no_symbol_pattern_index(t) */ sym, v, ts from t where sym not like 'A%' order by ts, v");
            String actual = select("select sym, v, ts from t where sym not like 'A%' order by ts, v");
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
            for (String order : new String[]{"order by ts", "order by ts desc"}) {
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
     */
    @Test
    public void testHighSelectivityFallsBackToScan() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table t (sym symbol index, v long, ts timestamp) timestamp(ts) partition by day");
            // 'A' || (x % 150) produces A0..A149 = 150 distinct symbols, all matching 'A%'.
            // 150 > default threshold (100), so the factory must choose the fallback path.
            execute("insert into t select cast('A' || (x % 150) as symbol), x, timestamp_sequence(0, 60000000) from long_sequence(1500)");
            // Ground truth: force scan+filter with the opt-out hint immediately after SELECT.
            String expected = select("select /*+ no_symbol_pattern_index(t) */ sym, count() from t where sym like 'A%' order by sym");
            SymbolPatternIndexRecordCursorFactory.resetTestCounters();
            String actual = select("select sym, count() from t where sym like 'A%' order by sym");
            io.questdb.test.tools.TestUtils.assertEquals(expected, actual);
            // Prove the fallback branch actually fired (not just that rows are correct).
            Assert.assertTrue(
                    "expected fallbackInvocations > 0, got " + SymbolPatternIndexRecordCursorFactory.testFallbackInvocations,
                    SymbolPatternIndexRecordCursorFactory.testFallbackInvocations > 0
            );
            Assert.assertEquals(
                    "expected indexInvocations == 0, got " + SymbolPatternIndexRecordCursorFactory.testIndexInvocations,
                    0,
                    SymbolPatternIndexRecordCursorFactory.testIndexInvocations
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
            // rnd_symbol gives 5 distinct values; 3 match 'A%': AA, AB, AC.  3 <= 100 => index path.
            execute("insert into t select rnd_symbol('AA','AB','BA','BB','AC'), x, timestamp_sequence(0, 60000000) from long_sequence(2000)");
            // Ground truth: hint goes immediately after SELECT (not in WHERE).
            String expected = select("select /*+ no_symbol_pattern_index(t) */ sym, v, ts from t where sym like 'A%' order by ts, v");
            SymbolPatternIndexRecordCursorFactory.resetTestCounters();
            String actual = select("select sym, v, ts from t where sym like 'A%' order by ts, v");
            io.questdb.test.tools.TestUtils.assertEquals(expected, actual);
            // Prove the index branch actually fired.
            Assert.assertTrue(
                    "expected indexInvocations > 0, got " + SymbolPatternIndexRecordCursorFactory.testIndexInvocations,
                    SymbolPatternIndexRecordCursorFactory.testIndexInvocations > 0
            );
            Assert.assertEquals(
                    "expected fallbackInvocations == 0, got " + SymbolPatternIndexRecordCursorFactory.testFallbackInvocations,
                    0,
                    SymbolPatternIndexRecordCursorFactory.testFallbackInvocations
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
            String[] preds = {
                    "sym like 'al%'",
                    "sym like '%ta'",
                    "sym like '%lph%'",
                    "sym ilike 'al%'",
                    "sym ilike 'ALPHA'",
                    "sym ~ '^al'",
                    "sym ~ 'a'",
                    "sym ~ 'zzz'",                  // matches nothing
                    "sym like 'al\\_x'",             // underscore escape
                    "sym like 'al%' and v > 100",   // residual filter
                    "sym like 'no_such%'"            // empty match
            };
            for (String p : preds) {
                // Hint goes right after SELECT (a WHERE-position hint is a silent no-op).
                // Use %s as a placeholder for the hint (or empty string), then escape any real % in pred.
                String base = "select %s sym, v, ts from t where " + p.replace("%", "%%") + " order by ts, v";
                String expected = select(String.format(base, "/*+ no_symbol_pattern_index(t) */ "));
                String actual   = select(String.format(base, ""));
                io.questdb.test.tools.TestUtils.assertEquals("pred=[" + p + "]", expected, actual);
            }
        });
    }

    /**
     * Ground-truth oracle for SP2: pins that {@code NOT LIKE} and {@code !~} INCLUDE rows whose symbol
     * is NULL.  The semantics are: {@code like(NULL) = NULL} which is not TRUE, so {@code NOT like(NULL) = NULL}
     * which is also not TRUE… but QuestDB follows SQL three-valued logic where the WHERE clause only passes
     * rows for which the predicate is TRUE.  However, the current scan+filter engine evaluates
     * {@code not(null) = false} for NULL symbols in a NOT LIKE context (the NULL sentinel becomes false after
     * negation).  This test documents the ACTUAL observed behaviour so that the SP2 excluded-complement fast
     * path can reproduce it exactly.
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
                String actual   = printFactory(factory);
                String expected = select("select /*+ no_symbol_pattern_index(t) */ sym, v from t where sym like 'A%' order by v");
                io.questdb.test.tools.TestUtils.assertEquals(expected, actual);
            }
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
