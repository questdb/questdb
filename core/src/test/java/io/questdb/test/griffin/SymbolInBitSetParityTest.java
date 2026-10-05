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
import io.questdb.std.Os;
import io.questdb.std.str.StringSink;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

/**
 * Edge cases of {@code symbol IN (...)} evaluated as a bitset over the symbol keys, beside
 * {@link SymbolInBitSetTest}: element-count boundaries, word boundaries of large symbol tables,
 * table aliases and joins, several sets in one filter, boolean combinations, indexed bind
 * variables rebound to other types, Parquet with a column top, WAL UPDATE and the stolen filters
 * of GROUP BY, top-K and WINDOW JOIN. Every query must print exactly what the reference (JIT off,
 * Java bitset off) prints, and print it again on a second execution of the same factory, in every
 * mode and against any native library. Where the library compiles the set (see
 * {@link JitUtil#isSymbolInSetSupported()}), a test that runs JIT modes must also see the JIT
 * engage at least once, so a silent fallback cannot pass for coverage.
 */
public class SymbolInBitSetParityTest extends AbstractCairoTest {
    private static final boolean JIT_SYM_IN_SET = Os.arch == Os.ARCH_X86_64 && JitUtil.isSymbolInSetSupported();
    private static final Log LOG = LogFactory.getLog(SymbolInBitSetParityTest.class);
    private final StringSink sink = new StringSink();
    private int jitExecutions;
    private String refOut;

    @Override
    @Before
    public void setUp() {
        super.setUp();
        jitExecutions = 0;
    }

    @Test
    public void testAliasAndJoinFilter() throws Exception {
        assertMemoryLeak(() -> {
            createTable(5_000, 60);
            execute("create table y (sym symbol, v int, ts timestamp) timestamp(ts) partition by day bypass wal");
            execute("insert into y select cast(concat('sym_', x % 60) as symbol), x::int, x::timestamp from long_sequence(60)");
            final String l = list(0, 25);
            parity("select * from x t where t.sym in " + l);
            parity("select x.sym, x.i, y.v from x join y on (sym) where x.sym in " + l + " and x.i > 0");
            parity("select * from (select * from x where sym in " + l + ") where i > 0");
            assertJitEngaged();
        });
    }

    @Test
    public void testBoundaryNineTenEleven() throws Exception {
        assertMemoryLeak(() -> {
            createTable(5_000, 60);
            for (int n = 1; n <= 13; n++) {
                parity("x where sym in " + list(3, n));
                parity("x where sym not in " + list(3, n));
                parity("x where sym in " + list(3, n) + " and i > 0");
            }
            // move the threshold
            setProperty(PropertyKey.CAIRO_SQL_JIT_MAX_IN_LIST_SIZE_THRESHOLD, 2);
            for (int n = 1; n <= 4; n++) {
                parity("x where sym in " + list(3, n));
                parity("x where sym in " + list(3, n) + " and d > 0.2");
            }
            assertJitEngaged();
        });
    }

    @Test
    public void testBooleanCombinations() throws Exception {
        assertMemoryLeak(() -> {
            createTable(6_000, 60);
            final String l1 = list(0, 25);
            final String l2 = "('s2_a', 's2_c', 'q1', 'q2', 'q3', 'q4', 'q5', 'q6', 'q7', 'q8', 'q9')";
            parity("x where (sym in " + l1 + ") = (s2 in " + l2 + ")");
            parity("x where (sym in " + l1 + ") != (i > 0)");
            parity("x where not (not (sym in " + l1 + "))");
            parity("x where (sym in " + l1 + ") = true");
            parity("x where (sym in " + l1 + " or i > 0) and (s2 in " + l2 + " or j < 0)");
            parity("x where sym in " + l1 + " and s2 in " + l2 + " and i > 0 and j > 0 and l > 0 and d < 0.9");
            parity("x where sym in " + l1 + " and i < l");
            parity("x where i < l");
            parity("x where sym in " + l1 + " and i + 1 < l");
            assertJitEngaged();
        });
    }

    @Test
    public void testCaseSensitivityAndCharConstants() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table x (sym symbol, ts timestamp) timestamp(ts) partition by day bypass wal");
            execute("insert into x values ('a', 0), ('A', 1), ('b', 2), ('B', 3), ('sym_1', 4), ('SYM_1', 5), (null, 6), ('é', 7), ('É', 8)");
            parity("x where sym in ('a', 'B', 'SYM_1', 'q1', 'q2', 'q3', 'q4', 'q5', 'q6', 'q7', 'q8')");
            parity("x where sym in ('A', 'b', 'sym_1', 'é', 'q2', 'q3', 'q4', 'q5', 'q6', 'q7', 'q8')");
            parity("x where sym not in ('A', 'b', 'sym_1', 'é', 'q2', 'q3', 'q4', 'q5', 'q6', 'q7', 'q8')");
            parity("x where sym not in (null, 'A', 'b', 'sym_1', 'q2', 'q3', 'q4', 'q5', 'q6', 'q7', 'q8')");
            parity("x where not (sym in (null, 'A', 'b', 'q1', 'q2', 'q3', 'q4', 'q5', 'q6', 'q7', 'q8')) or sym is null");
            assertJitEngaged();
        });
    }

    @Test
    public void testIndexedBindVariablesAndRebindTypes() throws Exception {
        assertMemoryLeak(() -> {
            createTable(3_000, 30);
            final String q = "x where sym in ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, 'sym_20')";
            for (Mode mode : Mode.values()) {
                mode.apply();
                bindVariableService.clear();
                for (int i = 0; i < 11; i++) {
                    bindVariableService.setStr(i, "sym_" + i);
                }
                final StringBuilder out = new StringBuilder();
                try (RecordCursorFactory factory = select(q)) {
                    if (factory.usesCompiledFilter()) {
                        jitExecutions++;
                    }
                    out.append("jit=").append(factory.usesCompiledFilter()).append('\n');
                    out.append(print(factory));
                    // rebind: CHAR, INT, LONG, NULL str into slots typed STRING at compile time
                    try {
                        bindVariableService.setChar(0, '7');
                        bindVariableService.setInt(1, 5);
                        bindVariableService.setLong(2, 9);
                        bindVariableService.setStr(3, null);
                        out.append(print(factory));
                    } catch (Throwable e) {
                        out.append("ERR ").append(e.getMessage());
                    }
                }
                if (mode == Mode.REFERENCE) {
                    refOut = out.toString().replace("jit=false\n", "");
                } else {
                    Assert.assertEquals(mode.toString(), refOut, out.toString().replaceFirst("jit=(true|false)\n", ""));
                }
            }
            assertJitEngaged();
        });
    }

    @Test
    public void testLargeSymbolTableWordBoundaries() throws Exception {
        assertMemoryLeak(() -> {
            createTable(60_000, 70_000);
            final StringBuilder sb = new StringBuilder("(");
            final int[] keys = {0, 30, 31, 32, 33, 62, 63, 64, 65, 127, 128, 4095, 4096, 65535, 65536, 65537, 69_998, 69_999};
            for (int i = 0; i < keys.length; i++) {
                sb.append(i == 0 ? "" : ", ").append("'sym_").append(keys[i]).append('\'');
            }
            sb.append(", null)");
            parity("x where sym in " + sb);
            parity("x where sym not in " + sb);
            parity("select count() from x where sym in " + sb);
            parity("x where sym in " + sb + " and l > 0");
            assertJitEngaged();
        });
    }

    @Test
    public void testMultipleSetsAndSymbolBindEq() throws Exception {
        assertMemoryLeak(() -> {
            createTable(8_000, 80);
            final String l1 = list(0, 30);
            final String l2 = list(20, 30);
            parity("x where sym in " + l1 + " and sym not in " + l2);
            parity("x where sym in " + l1 + " or sym in " + l2);
            parity("x where (sym in " + l1 + " and i > 0) or (sym in " + l2 + " and i < 0)");
            parity("x where sym in " + l1 + " and s2 in ('s2_a', 's2_b', 's2_c', 's2_d', 'q1', 'q2', 'q3', 'q4', 'q5', 'q6', 'q7', 'q8')");
            bindVariableService.clear();
            bindVariableService.setStr("b", "s2_c");
            parity("x where sym in " + l1 + " and s2 = :b");
            parity("x where sym in " + l1 + " and s2 != :b and sym != 'sym_3'");
            parity("x where sym in " + l1 + " and sym = 'sym_7'");
            parity("x where sym in " + l1 + " and sym in ('sym_5', 'sym_7')");
            assertJitEngaged();
        });
    }

    @Test
    public void testParquetColumnTop() throws Exception {
        assertMemoryLeak(() -> {
            createTable(20_000, 40);
            execute("alter table x add column sym3 symbol");
            execute("insert into x (sym, sym3, i, ts) select rnd_symbol('sym_1','sym_2'), rnd_symbol('t_0','t_1','t_2','t_3','t_4'," +
                    "'t_5','t_6','t_7','t_8','t_9','t_10','t_11',null), 1, '1970-01-04T12:00:00.000000Z'::timestamp + x * 1_000_000" +
                    " from long_sequence(3_000)");
            execute("alter table x convert partition to parquet where ts < '1970-01-05'");
            final String l = "('t_0', 't_2', 't_4', 't_6', 't_8', 't_10', 't_12', 'zz', 'yy', 'xx', 'ww')";
            parity("x where sym3 in " + l);
            parity("x where sym3 in (null, " + l.substring(1));
            parity("x where sym3 not in " + l);
            parity("x where sym3 not in (null, " + l.substring(1));
            parity("select sym3, count() from x where sym3 in (null, " + l.substring(1) + " order by sym3");
            assertJitEngaged();
        });
    }

    @Test
    public void testWalUpdateWithLongList() throws Exception {
        assertMemoryLeak(() -> {
            final String l = list(0, 15);
            String ref = null;
            for (Mode mode : Mode.values()) {
                mode.apply();
                execute("drop table if exists w");
                execute("create table w (sym symbol, i int, ts timestamp) timestamp(ts) partition by day wal");
                execute("insert into w select rnd_symbol(" + symArgs(40) + ",null), 0, timestamp_sequence(0, 10_000_000) from long_sequence(5000)");
                drainWalQueue();
                execute("update w set i = 7 where sym in " + l);
                drainWalQueue();
                sink.clear();
                Mode.REFERENCE.apply();
                printSql("select count() c from w where i = 7", sink);
                final String out = sink.toString();
                sink.clear();
                printSql("select count() c from w where sym in " + l, sink);
                Assert.assertEquals(mode.toString(), sink.toString(), out);
            }
        });
    }

    @Test
    public void testWindowJoinAndGroupByStolenFilter() throws Exception {
        assertMemoryLeak(() -> {
            createTable(8_000, 60);
            execute("create table p (sym symbol, price double, ts timestamp) timestamp(ts) partition by day bypass wal");
            execute("insert into p select rnd_symbol(" + symArgs(60) + "), rnd_double(), timestamp_sequence(0, 60_000_000) from long_sequence(5000)");
            final String l = list(0, 25);
            parity("select sym, count(), sum(d) from x where sym in " + l + " order by sym");
            parity("select * from x where sym in " + l + " order by d desc limit 9");
            parity("select x.sym, x.ts, sum(p.price) w from x window join p on (sym) range between 1 minute preceding and 1 minute following where x.sym in " + l + " order by x.ts, x.sym limit 200");
            assertJitEngaged();
        });
    }

    private void assertJitEngaged() {
        if (JIT_SYM_IN_SET) {
            Assert.assertTrue("no query compiled the set", jitExecutions > 0);
        }
    }

    private void createTable(int rows, int symbols) throws SqlException {
        execute("create table x (sym symbol capacity 131072, s2 symbol, i int, j int, d double, l long, ts timestamp)" +
                " timestamp(ts) partition by day bypass wal");
        execute("insert into x select cast(concat('sym_', x - 1) as symbol), 's2_a', 1, 1, 0.5, 1L, 0::timestamp" +
                " from long_sequence(" + symbols + ")");
        execute("insert into x select rnd_symbol(" + symArgs(Math.min(symbols, 2000)) + ",null), rnd_symbol('s2_a','s2_b','s2_c','s2_d',null), rnd_int(), rnd_int(), rnd_double(), rnd_long()," +
                " timestamp_sequence(1, " + (5 * 86_400_000_000L / rows) + ") from long_sequence(" + rows + ")");
        if (symbols > 2000) {
            // spread rows over the high keys too
            execute("insert into x select cast(concat('sym_', rnd_int(0, " + (symbols - 1) + ", 0)) as symbol), 's2_b', 2, 2, 0.1, 2L," +
                    " timestamp_sequence(2, " + (5 * 86_400_000_000L / rows) + ") from long_sequence(" + rows + ")");
        }
    }

    private static String symArgs(int n) {
        final StringBuilder sb = new StringBuilder();
        for (int i = 0; i < n; i++) {
            sb.append(i == 0 ? "" : ",").append("'sym_").append(i).append('\'');
        }
        return sb.toString();
    }

    private static String list(int from, int count) {
        final StringBuilder sb = new StringBuilder("(");
        for (int i = 0; i < count; i++) {
            sb.append(i == 0 ? "" : ", ").append("'sym_").append(from + i).append('\'');
        }
        return sb.append(')').toString();
    }

    private void parity(String query) throws SqlException {
        Mode.REFERENCE.apply();
        final String expected;
        try (RecordCursorFactory f = select(query)) {
            expected = print(f);
        }
        for (Mode mode : Mode.values()) {
            mode.apply();
            try (RecordCursorFactory f = select(query)) {
                final boolean jit = f.usesCompiledFilter();
                final String got = print(f);
                if (jit) {
                    jitExecutions++;
                }
                LOG.info().$("parity [mode=").$(mode).$(", jit=").$(jit).$(", query=").$safe(query).I$();
                TestUtils.assertEquals(mode + ": " + query, expected, got);
                // second execution of the same factory
                TestUtils.assertEquals(mode + " (re-exec): " + query, expected, print(f));
            }
        }
    }

    private String print(RecordCursorFactory factory) throws SqlException {
        sink.clear();
        try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
            CursorPrinter.println(cursor, factory.getMetadata(), sink);
        }
        return sink.toString();
    }

    private enum Mode {
        REFERENCE(SqlJitMode.JIT_MODE_DISABLED, false),
        JAVA_BITSET(SqlJitMode.JIT_MODE_DISABLED, true),
        JIT_VECTOR(SqlJitMode.JIT_MODE_ENABLED, true),
        JIT_SCALAR(SqlJitMode.JIT_MODE_FORCE_SCALAR, true);

        private final boolean bitSet;
        private final int jitMode;

        Mode(int jitMode, boolean bitSet) {
            this.jitMode = jitMode;
            this.bitSet = bitSet;
        }

        void apply() {
            setProperty(PropertyKey.CAIRO_SQL_SYMBOL_IN_BITSET_ENABLED, bitSet ? "true" : "false");
            setProperty(PropertyKey.CAIRO_SQL_JIT_SYMBOL_IN_BITSET_ENABLED, bitSet ? "true" : "false");
            sqlExecutionContext.setJitMode(jitMode);
        }
    }
}
