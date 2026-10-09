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
import io.questdb.cairo.SqlJitMode;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.griffin.SqlException;
import io.questdb.jit.JitUtil;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.tools.TestUtils;
import io.questdb.cairo.CursorPrinter;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.Test;

/**
 * The empty-string constant compared against a SYMBOL column ({@code sym = ''}, {@code sym != ''})
 * in a JIT filter ({@code cairo.sql.jit.symbol.empty.constant.enabled}): every filter must print
 * exactly the rows the Java filter prints, whether the column's symbol table holds the empty
 * string or not, with NULLs, with the rest of the predicate compiled alongside, and in every
 * consumer of a compiled filter.
 */
public class SymbolEmptyConstantJitTest extends AbstractCairoTest {
    private static final String[] FILTERS = {
            "stop != ''",
            "stop <> ''",
            "stop = ''",
            "'' = stop",
            "'' != stop",
            "stop IS NOT NULL AND stop != ''",
            "stop IS NULL OR stop = ''",
            "not (stop = '')",
            "stop != '' AND i > 3",
            "stop = '' OR i = 7",
            "sym IN ('a', 'c') AND stop IS NOT NULL AND stop != ''",
            "stop IN ('', 'S')",
            "stop NOT IN ('', 'S')",
            "stop != '' AND stop != 'S'",
    };

    @Test
    public void testColumnTopsParquetAndWal() throws Exception {
        // stop added by ALTER: the first partitions have a column top (NULL), then the later ones are
        // converted to Parquet; WAL and non-WAL tables
        assertMemoryLeak(() -> {
            for (boolean wal : new boolean[]{false, true}) {
                for (boolean withEmpty : new boolean[]{true, false}) {
                    execute("create table x as (select rnd_symbol('a', 'b', 'c', null) sym, x::int i, x * 0.5 d, timestamp_sequence(0, 1000000) ts " +
                            "from long_sequence(4000)) timestamp(ts) partition by hour " + (wal ? "wal" : "bypass wal"));
                    execute("alter table x add column stop symbol");
                    execute("insert into x (sym, i, d, ts, stop) select rnd_symbol('a', 'b', 'c', null), x::int, x * 0.5, " +
                            "'1970-01-01T01:30:00'::timestamp + x * 1000000, " +
                            "case x % 4 when 0 then null::string when 1 then " + (withEmpty ? "''" : "'T'") + " when 2 then 'S' else 'R' end " +
                            "from long_sequence(4000)");
                    if (wal) {
                        drainWalQueue();
                    }
                    execute("alter table x convert partition to parquet where ts < '1970-01-01T02:00'");
                    if (wal) {
                        drainWalQueue();
                    }
                    assertAllFilters();
                    // '' is not NULL: the column top reads as NULL
                    TestUtils.assertEquals("c\n" + (withEmpty ? 1000 : 0) + "\n", referenceOf("select count() c from x where stop = ''"));
                    TestUtils.assertEquals("c\n5000\n", referenceOf("select count() c from x where stop is null"));
                    execute("drop table x");
                    if (wal) {
                        drainWalQueue();
                    }
                }
            }
        });
    }

    @Test
    public void testEmptyStringAbsentFromSymbolTable() throws Exception {
        // the symbol table has no '': the constant resolves to no key, as in the Java filter
        assertMemoryLeak(() -> {
            createTable(false);
            assertAllFilters();
        });
    }

    @Test
    public void testEmptyStringInSymbolTable() throws Exception {
        assertMemoryLeak(() -> {
            createTable(true);
            assertAllFilters();
        });
    }

    @Test
    public void testFeatureOffKeepsJavaFilter() throws Exception {
        Assume.assumeTrue(JitUtil.isJitSupported());
        assertMemoryLeak(() -> {
            createTable(true);
            setProperty(PropertyKey.CAIRO_SQL_JIT_SYMBOL_EMPTY_CONSTANT_ENABLED, "false");
            sqlExecutionContext.setJitMode(SqlJitMode.JIT_MODE_ENABLED);
            assertPlanContains("explain select * from x where stop IS NOT NULL AND stop != ''", "Async Filter");
            setProperty(PropertyKey.CAIRO_SQL_JIT_SYMBOL_EMPTY_CONSTANT_ENABLED, "true");
            assertPlanContains("explain select * from x where stop IS NOT NULL AND stop != ''", "Async JIT Filter");
            // the TAQ idx 64 shape: a keyed GROUP BY over the filter
            assertPlanContains(
                    "explain select sym, timestamp_floor('5m', ts) m, first(d), sum(d) from x where sym IN ('a', 'c') AND stop IS NOT NULL AND stop != '' order by sym, m",
                    "Async JIT Group By"
            );
        });
    }

    @Test
    public void testRebindAfterEmptyStringAppears() throws Exception {
        // a cached factory compiled while the table had no '' keeps resolving it per execution
        assertMemoryLeak(() -> {
            createTable(false);
            final String query = "select * from x where stop != '' and i >= 0";
            final String count = "select count() from x where stop = ''";
            for (Mode mode : Mode.values()) {
                mode.apply();
                try (
                        RecordCursorFactory f1 = select(query);
                        RecordCursorFactory f2 = select(count)
                ) {
                    Assert.assertEquals(mode.isJit && JitUtil.isJitSupported(), f1.usesCompiledFilter());
                    final String before = print(f1);
                    final String beforeCount = print(f2);
                    execute("insert into x values ('', 'a', 100, 1.5, '1970-01-02T00:00:00.000000Z')");
                    final String after = print(f1);
                    final String afterCount = print(f2);
                    Assert.assertNotEquals(beforeCount, afterCount);
                    Mode.REFERENCE.apply();
                    TestUtils.assertEquals(mode + ": " + query, referenceOf(query), after);
                    TestUtils.assertEquals(mode + ": " + count, referenceOf(count), afterCount);
                    Assert.assertNotNull(before);
                }
                execute("drop table x");
                createTable(false);
            }
        });
    }

    private void assertAllFilters() throws SqlException {
        final String[] shapes = {
                "select * from x where %s",
                "select count() from x where %s",
                "select sym, count(), sum(d) from x where %s order by sym",
                "select * from x where %s order by d desc limit 7",
        };
        for (String filter : FILTERS) {
            for (String shape : shapes) {
                final String query = String.format(shape, filter);
                final String expected = referenceOf(query);
                for (Mode mode : Mode.values()) {
                    mode.apply();
                    try (RecordCursorFactory factory = select(query)) {
                        TestUtils.assertEquals(mode + ": " + query, expected, print(factory));
                    }
                }
            }
            // a filter alone must compile now that '' is accepted
            if (JitUtil.isJitSupported()) {
                Mode.JIT_VECTOR.apply();
                try (RecordCursorFactory factory = select("select * from x where " + filter)) {
                    Assert.assertTrue(filter, factory.usesCompiledFilter());
                }
            }
        }
    }

    private void assertPlanContains(String explain, String fragment) throws SqlException {
        sink.clear();
        printSql(explain, sink);
        TestUtils.assertContains(sink, fragment);
    }

    private void createTable(boolean withEmpty) throws SqlException {
        execute("create table x as (select " +
                "case x % 4 when 0 then null when 1 then " + (withEmpty ? "''" : "'T'") + " when 2 then 'S' else 'R' end::symbol stop, " +
                "rnd_symbol('a', 'b', 'c', null) sym, x::int i, x * 0.5 d, timestamp_sequence(0, 1000000) ts " +
                "from long_sequence(2000)) timestamp(ts) partition by hour bypass wal");
    }

    private String print(RecordCursorFactory factory) throws SqlException {
        sink.clear();
        try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
            CursorPrinter.println(cursor, factory.getMetadata(), sink);
        }
        return sink.toString();
    }

    private String referenceOf(String query) throws SqlException {
        Mode.REFERENCE.apply();
        try (RecordCursorFactory factory = select(query)) {
            Assert.assertFalse(factory.usesCompiledFilter());
            return print(factory);
        }
    }

    private enum Mode {
        REFERENCE(SqlJitMode.JIT_MODE_DISABLED, true),
        FEATURE_OFF(SqlJitMode.JIT_MODE_ENABLED, false),
        JIT_VECTOR(SqlJitMode.JIT_MODE_ENABLED, true),
        JIT_SCALAR(SqlJitMode.JIT_MODE_FORCE_SCALAR, true);

        final boolean isJit;
        private final boolean enabled;
        private final int jitMode;

        Mode(int jitMode, boolean enabled) {
            this.jitMode = jitMode;
            this.enabled = enabled;
            this.isJit = jitMode != SqlJitMode.JIT_MODE_DISABLED && enabled;
        }

        void apply() {
            setProperty(PropertyKey.CAIRO_SQL_JIT_SYMBOL_EMPTY_CONSTANT_ENABLED, enabled ? "true" : "false");
            sqlExecutionContext.setJitMode(jitMode);
        }
    }
}
