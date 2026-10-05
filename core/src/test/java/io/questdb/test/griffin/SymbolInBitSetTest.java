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
import io.questdb.cairo.CursorPrinter;
import io.questdb.cairo.SqlJitMode;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.griffin.SqlException;
import io.questdb.jit.JitUtil;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.std.ObjList;
import io.questdb.std.Rnd;
import io.questdb.std.str.StringSink;
import io.questdb.std.str.Utf8String;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.Before;
import org.junit.Test;

/**
 * {@code symbol IN (...)} evaluated as a bitset over the symbol keys, by the JIT filter
 * ({@code cairo.sql.jit.symbol.in.bitset.enabled}) and by the Java filter
 * ({@code cairo.sql.symbol.in.bitset.enabled}). Every query runs in four modes and must print
 * exactly what the pre-bitset implementation - the Java filter probing a hash set - prints:
 * <ul>
 * <li>REFERENCE: JIT off, Java bitset off - the implementation before this change;</li>
 * <li>JAVA_BITSET: JIT off, Java bitset on;</li>
 * <li>JIT_VECTOR: JIT on with both bitsets, the AVX2 loop where the filter allows it;</li>
 * <li>JIT_SCALAR: JIT forced scalar with both bitsets.</li>
 * </ul>
 * The JIT modes also assert the filter compiled, so a silent fallback to Java cannot pass for
 * parity.
 */
public class SymbolInBitSetTest extends AbstractCairoTest {
    private static final Log LOG = LogFactory.getLog(SymbolInBitSetTest.class);
    private final StringSink sink = new StringSink();

    @Override
    @Before
    public void setUp() {
        Assume.assumeTrue(JitUtil.isJitSupported());
        super.setUp();
    }

    @Test
    public void testAbsentValuesMatchNothing() throws Exception {
        assertMemoryLeak(() -> {
            createTable(5_000, 50);
            // Every value absent: no row, and NOT IN keeps every row, NULLs included.
            final String absent = list("nope", 0, 20);
            assertEmpty("x where sym in " + absent);
            assertAllModes("x where sym not in " + absent, true);
            // Present and absent values mixed.
            assertAllModes("x where sym in " + list("nope", 0, 15) + " or sym in ('" + symbolAt(3) + "')", true);
            assertAllModes("x where sym in ('" + symbolAt(1) + "', '" + symbolAt(2) + "', " + list("nope", 0, 12).substring(1), true);
        });
    }

    @Test
    public void testBindVariablesRebindOnCachedFactory() throws Exception {
        assertMemoryLeak(() -> {
            createTable(5_000, 40);
            final int n = 14;
            final StringBuilder sb = new StringBuilder("x where sym in (");
            for (int i = 0; i < n; i++) {
                sb.append(i == 0 ? "" : ", ").append(":v").append(i);
            }
            final String query = sb.append(')').toString();

            for (Mode mode : Mode.values()) {
                bindVariableService.clear();
                bindValues(0);
                mode.apply();
                try (RecordCursorFactory factory = select(query)) {
                    Assert.assertEquals(mode.toString(), mode.isJit, factory.usesCompiledFilter());
                    // The same factory across rebinds, as the query cache reuses it: the set must be
                    // resolved per execution, not frozen at compile time.
                    for (int round = 0; round < 4; round++) {
                        bindValues(round);
                        final String expected = referenceOf(literalQuery(round, n));
                        assertNonEmptyUnlessAllAbsent(expected, round);
                        Assert.assertEquals(mode + ", round " + round, expected, print(factory));
                    }
                }
            }
        });
    }

    @Test
    public void testCharVarcharAndNullBindVariables() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table x as (select rnd_symbol('a','b','c','dd','ee','ff','g','hh','ii','jj','kk','ll',null) sym," +
                    " timestamp_sequence(0, 1_000_000) ts from long_sequence(2_000)) timestamp(ts) partition by hour bypass wal");
            final String query = "x where sym in (:c, :v, :s, :n, 'dd', 'ee', 'ff', 'hh', 'ii', 'jj', 'zz')";
            final String expected = referenceOf("x where sym in ('a', 'b', 'c', null, 'dd', 'ee', 'ff', 'hh', 'ii', 'jj', 'zz')");
            for (Mode mode : Mode.values()) {
                bindVariableService.clear();
                bindVariableService.setChar("c", 'a');
                bindVariableService.setVarchar("v", new Utf8String("b"));
                bindVariableService.setStr("s", "c");
                bindVariableService.setStr("n", null);
                mode.apply();
                try (RecordCursorFactory factory = select(query)) {
                    Assert.assertEquals(mode.toString(), mode.isJit, factory.usesCompiledFilter());
                    Assert.assertEquals(mode.toString(), expected, print(factory));
                }
            }
        });
    }

    @Test
    public void testColumnTop() throws Exception {
        // A SYMBOL column added after the first partitions were written reads as NULL there.
        assertMemoryLeak(() -> {
            createTable(4_000, 30);
            execute("alter table x add column sym2 symbol");
            execute("insert into x (sym, sym2, i, ts) select rnd_symbol('sym_1','sym_2'), rnd_symbol('t_0','t_1','t_2','t_3','t_4'," +
                    "'t_5','t_6','t_7','t_8','t_9','t_10','t_11',null), 1, '1970-01-04T00:00:00.000000Z'::timestamp + x * 1_000_000" +
                    " from long_sequence(3_000)");
            final String l = "('t_0', 't_2', 't_4', 't_6', 't_8', 't_10', 't_12', 'zz', 'yy', 'xx', 'ww')";
            assertAllModes("x where sym2 in " + l, true);
            assertAllModes("x where sym2 in (null, " + l.substring(1), true);
            assertAllModes("x where sym2 not in " + l, true);
            assertAllModes("x where sym2 not in (null, " + l.substring(1), true);
        });
    }

    @Test
    public void testConcurrentWriterAppendsSymbols() throws Exception {
        // A writer appends new symbols - some of them named in the list - while a cursor is open.
        // The open cursor reads its own snapshot: neither the rows nor their keys may leak into it,
        // and a key past the end of the bitset must test false rather than read past it.
        assertMemoryLeak(() -> {
            for (Mode mode : Mode.values()) {
                execute("drop table if exists x");
                createTable(3_000, 30);
                final String query = "x where sym in ('new0', 'new1', 'new2', " + list(null, 0, 12).substring(1);
                final String snapshot = referenceOf(query);
                Assert.assertTrue(countRows(snapshot) > 0);
                mode.apply();
                try (RecordCursorFactory factory = select(query)) {
                    Assert.assertEquals(mode.toString(), mode.isJit, factory.usesCompiledFilter());
                    try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                        execute("insert into x select rnd_symbol('new0','new1','new2','new3') sym, rnd_symbol('p','q') s2," +
                                " 1 i, 1 j, 0.5 d, 1L l, timestamp_sequence(0, 1_000_000) ts from long_sequence(500)");
                        sink.clear();
                        CursorPrinter.println(cursor, factory.getMetadata(), sink);
                    }
                    Assert.assertEquals(mode.toString(), snapshot, sink.toString());
                    // A fresh execution of the same factory sees the new rows and resolves their keys.
                    final String after = print(factory);
                    Assert.assertTrue(mode.toString(), after.contains("new1"));
                    Assert.assertFalse(mode.toString(), after.contains("new3"));
                    Assert.assertEquals(mode.toString(), referenceOf(query), after);
                }
            }
        });
    }

    @Test
    public void testDuplicatesAndNull() throws Exception {
        assertMemoryLeak(() -> {
            createTable(5_000, 60);
            final String s1 = symbolAt(1);
            final String s7 = symbolAt(7);
            final String dupes = "('" + s1 + "', '" + s1 + "', '" + s7 + "', '" + s1 + "', '" + s7 + "', " + list(null, 10, 8).substring(1);
            assertAllModes("x where sym in " + dupes, true);
            assertAllModes("x where sym not in " + dupes, true);
            // NULL in the list matches the NULL keys, and NOT IN then drops them.
            final String withNull = "(null, " + list(null, 20, 11).substring(1);
            assertAllModes("x where sym in " + withNull, true);
            assertAllModes("x where sym not in " + withNull, true);
            Assert.assertTrue(referenceOf("x where sym in " + withNull).contains("\n\t"));
            Assert.assertFalse(referenceOf("x where sym not in " + withNull).contains("\n\t"));
            // NULL spelled as NaN, and a list holding nothing but NULL and absent values.
            assertAllModes("x where sym in (nan, " + list("absent", 0, 11).substring(1), true);
            assertAllModes("x where sym in (null, null, " + list("absent", 0, 11).substring(1), true);
        });
    }

    @Test
    public void testEmptyStringCharAndQuotedValues() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table x (sym symbol, ts timestamp) timestamp(ts) partition by day bypass wal");
            execute("insert into x values ('', 0), ('a', 1), ('it''s', 2), ('b', 3), (null, 4), ('''', 5), ('a''', 6)," +
                    " ('c', 7), ('dd', 8), ('ee', 9), ('ff', 10), ('gg', 11), ('hh', 12)");
            final String tail = ", 'x1', 'x2', 'x3', 'x4', 'x5', 'x6', 'x7', 'x8', 'x9')";
            assertAllModes("x where sym in ('', 'a', 'it''s'" + tail, true);
            assertAllModes("x where sym in ('''', 'a''', 'b'" + tail, true);
            assertAllModes("x where sym not in ('', 'a', 'it''s'" + tail, true);
            Assert.assertEquals(3, countRows(referenceOf("x where sym in ('', 'a', 'it''s'" + tail)));
        });
    }

    @Test
    public void testExplainShowsJitFilterForLongList() throws Exception {
        assertMemoryLeak(() -> {
            createTable(1_000, 40);
            final String where = " where sym in " + list(null, 0, 30) + " and i + j > 0";
            final String query = "x" + where;
            setProperty(PropertyKey.CAIRO_SQL_JIT_SYMBOL_IN_BITSET_ENABLED, "true");
            assertPlanContains("explain " + query, "Async JIT Filter");
            assertPlanContains("explain select count() from x" + where, "Async JIT Filter");
            Assert.assertTrue(usesJit("select sym, sum(d) from x" + where));
            setProperty(PropertyKey.CAIRO_SQL_JIT_SYMBOL_IN_BITSET_ENABLED, "false");
            assertPlanContains("explain " + query, "Async Filter");
            assertPlanNotContains("explain " + query, "JIT");
        });
    }

    @Test
    public void testLargeSymbolTableFallsBack() throws Exception {
        assertMemoryLeak(() -> {
            createTable(5_000, 200);
            // The table holds 200 symbols. A cap below that keeps the JIT on the equality chain, so
            // an over-threshold list falls back to the Java filter; the Java filter keeps its bitset
            // while the keys the list names fit under the cap, and the hash set once they do not.
            setProperty(PropertyKey.CAIRO_SQL_SYMBOL_IN_BITSET_MAX_KEYS, 100);
            final String lowKeys = list(null, 0, 20);
            final String highKeys = list(null, 150, 20);
            for (String l : new String[]{lowKeys, highKeys}) {
                final String query = "x where sym in " + l;
                final String expected = referenceOf(query);
                Assert.assertTrue(countRows(expected) > 0);
                Mode.JIT_VECTOR.apply();
                setProperty(PropertyKey.CAIRO_SQL_SYMBOL_IN_BITSET_MAX_KEYS, 100);
                try (RecordCursorFactory factory = select(query)) {
                    Assert.assertFalse(factory.usesCompiledFilter());
                    Assert.assertEquals(expected, print(factory));
                }
                setProperty(PropertyKey.CAIRO_SQL_SYMBOL_IN_BITSET_MAX_KEYS, 1_000);
                try (RecordCursorFactory factory = select(query)) {
                    Assert.assertTrue(factory.usesCompiledFilter());
                    Assert.assertEquals(expected, print(factory));
                }
            }
        });
    }

    @Test
    public void testParquetPartitions() throws Exception {
        assertMemoryLeak(() -> {
            createTable(20_000, 80);
            execute("alter table x convert partition to parquet where ts < '1970-01-04'");
            sink.clear();
            printSql("select count() from table_partitions('x') where isParquet", sink);
            TestUtils.assertEquals("count\n3\n", sink);
            final Rnd rnd = TestUtils.generateRandom(LOG);
            for (int i = 0; i < 12; i++) {
                final String l = randomList(rnd, 80);
                assertAllModes("x where sym in " + l, false);
                assertAllModes("x where sym not in " + l + " and i > 0", false);
                assertAllModes("select sym, count(), sum(d) from x where sym in " + l + " order by sym", false);
            }
        });
    }

    @Test
    public void testRandomListsMatchReference() throws Exception {
        assertMemoryLeak(() -> {
            createTable(30_000, 300);
            final Rnd rnd = TestUtils.generateRandom(LOG);
            final String[] shapes = {
                    "x where sym in %s",
                    "x where sym not in %s",
                    "x where sym in %s and i > 0",
                    "x where sym in %s and i + j > 0",
                    "x where sym in %s and d > 0.5",
                    "x where sym in %s and l > 0",
                    "x where sym in %s or i < -2000000000",
                    "x where not (sym in %s) and j < 0",
                    "x where sym in %s and s2 in ('s2_a', 's2_b', 's2_c')",
                    "select count() from x where sym in %s",
                    "select count() from x where sym in %s and i + j > 0",
                    "select sym, count(), sum(i) from x where sym in %s order by sym",
                    "select * from x where sym in %s limit 7",
            };
            for (int i = 0; i < 60; i++) {
                final String shape = shapes[rnd.nextInt(shapes.length)];
                assertAllModes(String.format(shape, randomList(rnd, 300)), false);
            }
        });
    }

    @Test
    public void testStringComparisonsAndCasts() throws Exception {
        assertMemoryLeak(() -> {
            createTable(5_000, 40);
            final String l = list(null, 0, 15);
            // A SYMBOL cast to STRING / VARCHAR goes through the string IN functions, never the set.
            assertJavaOnly("x where sym::string in " + l);
            assertJavaOnly("x where cast(sym as varchar) in " + l);
            Assert.assertEquals(referenceOf("x where sym in " + l), referenceOf("x where sym::string in " + l));
            // Other symbol / string comparisons beside the set. A STRING comparison has no JIT form,
            // so it takes the whole filter to Java in every mode - with the same rows.
            assertAllModes("x where sym in " + l + " and sym != '" + symbolAt(3) + "'", true);
            assertAllModes("x where sym in " + l + " and sym::string != '" + symbolAt(4) + "'", true, false);
            // Two symbol columns, each with its own set.
            assertAllModes("x where sym in " + l + " and s2 in ('s2_a', 's2_b', 's2_c', 's2_d', 's2_e', 's2_f'," +
                    " 's2_g', 's2_h', 's2_i', 's2_j', 's2_k')", true);
        });
    }

    @Test
    public void testSymbolsBeyondTheListAreNotMembers() throws Exception {
        // The list names only the lowest keys, so most rows carry a key past the end of the bitset.
        assertMemoryLeak(() -> {
            createTable(10_000, 300);
            assertAllModes("x where sym in " + list(null, 0, 11), true);
            assertAllModes("x where sym not in " + list(null, 0, 11), true);
            assertAllModes("x where sym in " + list(null, 289, 11), true);
        });
    }

    private static int countRows(String printed) {
        int n = 0;
        for (int i = 0, len = printed.length(); i < len; i++) {
            if (printed.charAt(i) == '\n') {
                n++;
            }
        }
        return n - 1;
    }

    private static String literalQuery(int round, int n) {
        final StringBuilder sb = new StringBuilder("x where sym in (");
        for (int i = 0; i < n; i++) {
            sb.append(i == 0 ? "" : ", ").append('\'').append(roundValue(round, i)).append('\'');
        }
        return sb.append(')').toString();
    }

    private static String roundValue(int round, int i) {
        // Round 3 names only absent values.
        return round == 3 ? "absent" + i : "sym_" + ((round * 7 + i * 3) % 40);
    }

    private void assertAllModes(String query, boolean requireRows) throws SqlException {
        assertAllModes(query, requireRows, true);
    }

    private void assertAllModes(String query, boolean requireRows, boolean isJitPossible) throws SqlException {
        final String expected = referenceOf(query);
        if (requireRows) {
            Assert.assertTrue("expected rows: " + query, countRows(expected) > 0);
        }
        for (Mode mode : Mode.values()) {
            mode.apply();
            Assert.assertEquals(mode + " JIT usage: " + query, mode.isJit && isJitPossible, usesJit(query));
            try (RecordCursorFactory factory = select(query)) {
                TestUtils.assertEquals(mode + ": " + query, expected, print(factory));
            }
        }
    }

    private void assertEmpty(String query) throws SqlException {
        Assert.assertEquals(0, countRows(referenceOf(query)));
        assertAllModes(query, false);
    }

    private void assertJavaOnly(String query) throws SqlException {
        final String expected = referenceOf(query);
        Mode.JIT_VECTOR.apply();
        try (RecordCursorFactory factory = select(query)) {
            Assert.assertFalse(query, factory.usesCompiledFilter());
            Assert.assertEquals(query, expected, print(factory));
        }
    }

    private void assertNonEmptyUnlessAllAbsent(String expected, int round) {
        if (round == 3) {
            Assert.assertEquals(0, countRows(expected));
        } else {
            Assert.assertTrue(countRows(expected) > 0);
        }
    }

    private void assertPlanContains(String explain, String fragment) throws SqlException {
        sink.clear();
        printSql(explain, sink);
        TestUtils.assertContains(sink, fragment);
    }

    private void assertPlanNotContains(String explain, String fragment) throws SqlException {
        sink.clear();
        printSql(explain, sink);
        Assert.assertFalse(sink.toString(), sink.toString().contains(fragment));
    }

    private void bindValues(int round) throws SqlException {
        for (int i = 0; i < 14; i++) {
            bindVariableService.setStr("v" + i, roundValue(round, i));
        }
    }

    private void createTable(int rows, int symbols) throws SqlException {
        // Symbol values sym_0 .. sym_<symbols - 1>, inserted first and in order so that sym_<i>
        // holds key i, then rows that draw on them at random, NULLs included, over five days.
        execute("create table x (sym symbol, s2 symbol, i int, j int, d double, l long, ts timestamp)" +
                " timestamp(ts) partition by day bypass wal");
        execute("insert into x select cast(concat('sym_', x - 1) as symbol), 's2_a', 1, 1, 0.5, 1L, 0::timestamp" +
                " from long_sequence(" + symbols + ")");
        final StringBuilder sb = new StringBuilder("insert into x select rnd_symbol(");
        for (int i = 0; i < symbols; i++) {
            sb.append(i == 0 ? "" : ",").append("'sym_").append(i).append('\'');
        }
        sb.append(",null), rnd_symbol('s2_a','s2_b','s2_c','s2_d',null), rnd_int(), rnd_int(), rnd_double(), rnd_long()," +
                " timestamp_sequence(1, ").append(5 * 86_400_000_000L / rows).append(") from long_sequence(").append(rows).append(')');
        execute(sb);
    }

    /**
     * A list literal of {@code count} values, {@code prefix_<i>} for {@code i} from {@code from} on,
     * or the table's own {@code sym_<i>} when the prefix is null.
     */
    private String list(String prefix, int from, int count) {
        final StringBuilder sb = new StringBuilder("(");
        for (int i = 0; i < count; i++) {
            sb.append(i == 0 ? "" : ", ").append('\'').append(prefix == null ? "sym" : prefix).append('_').append(from + i).append('\'');
        }
        return sb.append(')').toString();
    }

    private boolean usesJit(String query) throws SqlException {
        try (RecordCursorFactory factory = select(query)) {
            if (factory.usesCompiledFilter()) {
                return true;
            }
        }
        sink.clear();
        printSql("explain " + query, sink);
        return sink.toString().contains(" JIT ");
    }

    private String print(RecordCursorFactory factory) throws SqlException {
        sink.clear();
        try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
            CursorPrinter.println(cursor, factory.getMetadata(), sink);
        }
        return sink.toString();
    }

    private String randomList(Rnd rnd, int symbols) {
        // 11 to 400 elements past the equality chain's threshold, drawn from present values,
        // absent values, NULL and duplicates.
        final int n = 11 + rnd.nextInt(390);
        final ObjList<String> values = new ObjList<>();
        for (int i = 0; i < n; i++) {
            final int kind = rnd.nextInt(20);
            if (kind == 0) {
                values.add("null");
            } else if (kind < 4) {
                values.add("'absent_" + rnd.nextInt(1000) + '\'');
            } else if (kind < 6 && values.size() > 0) {
                values.add(values.getQuick(rnd.nextInt(values.size())));
            } else {
                values.add("'sym_" + rnd.nextInt(symbols) + '\'');
            }
        }
        final StringBuilder sb = new StringBuilder("(");
        for (int i = 0; i < n; i++) {
            sb.append(i == 0 ? "" : ", ").append(values.getQuick(i));
        }
        return sb.append(')').toString();
    }

    private String referenceOf(String query) throws SqlException {
        Mode.REFERENCE.apply();
        try (RecordCursorFactory factory = select(query)) {
            Assert.assertFalse(factory.usesCompiledFilter());
            return print(factory);
        }
    }

    private String symbolAt(int i) {
        return "sym_" + i;
    }

    private enum Mode {
        REFERENCE(SqlJitMode.JIT_MODE_DISABLED, false),
        JAVA_BITSET(SqlJitMode.JIT_MODE_DISABLED, true),
        JIT_VECTOR(SqlJitMode.JIT_MODE_ENABLED, true),
        JIT_SCALAR(SqlJitMode.JIT_MODE_FORCE_SCALAR, true);

        final boolean isJit;
        private final boolean bitSet;
        private final int jitMode;

        Mode(int jitMode, boolean bitSet) {
            this.jitMode = jitMode;
            this.bitSet = bitSet;
            this.isJit = jitMode != SqlJitMode.JIT_MODE_DISABLED;
        }

        void apply() {
            setProperty(PropertyKey.CAIRO_SQL_SYMBOL_IN_BITSET_ENABLED, bitSet ? "true" : "false");
            setProperty(PropertyKey.CAIRO_SQL_JIT_SYMBOL_IN_BITSET_ENABLED, bitSet ? "true" : "false");
            sqlExecutionContext.setJitMode(jitMode);
        }
    }
}
