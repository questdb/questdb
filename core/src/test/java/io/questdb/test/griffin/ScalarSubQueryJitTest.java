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
import io.questdb.cairo.TableReader;
import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.engine.functions.lt.AbstractScalarCursorFunction;
import io.questdb.griffin.engine.table.AsyncJitFilteredRecordCursorFactory;
import io.questdb.griffin.engine.table.parquet.PartitionDescriptor;
import io.questdb.griffin.engine.table.parquet.PartitionEncoder;
import io.questdb.jit.JitUtil;
import io.questdb.std.Files;
import io.questdb.std.ObjList;
import io.questdb.std.str.Path;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.Before;
import org.junit.Test;

/**
 * Uncorrelated scalar sub-queries as bind variables of a JIT filter
 * ({@code cairo.sql.jit.scalar.subquery.enabled}): each filter prints exactly what the Java filter
 * prints, in every JIT mode and with the feature off, for every comparison, every numeric width the
 * cursor comparisons pair, sub-queries that return NULL or no row, sub-queries on either side,
 * re-executions of a cached factory after the sub-query's value changed, NULL/infinite/NaN values,
 * the DOUBLE tolerance, FLOAT against a DOUBLE no float holds, user bind variables, either order in
 * which a consumer initialises the Java filter and the bind variables, and read_parquet().
 */
public class ScalarSubQueryJitTest extends AbstractCairoTest {
    private static final String[] FILTERS = {
            // DOUBLE column, DOUBLE scalar, every operator
            "d > (select avg(d) from y)",
            "d >= (select avg(d) from y)",
            "d < (select avg(d) from y)",
            "d <= (select avg(d) from y)",
            "d = (select max(d) from y where k = 3)",
            "d != (select max(d) from y where k = 3)",
            // the sub-query on the left
            "(select avg(d) from y) < d",
            "(select max(l) from y) >= i",
            // FLOAT column against FLOAT, INT and LONG scalars: compared as DOUBLE
            "f >= (select max(f) from y where k < 5)",
            "f < (select max(i) from y)",
            "f <= (select min(l) from y)",
            // INT column against INT, LONG and DOUBLE scalars
            "i < (select max(i) from y where k < 5)",
            "i = (select max(i) from y where k = 4)",
            "i > (select max(l) from y where k < 5)",
            "i <= (select avg(d) from y)",
            // LONG column
            "l = (select max(l) from y where k = 2)",
            "l != (select max(l) from y where k = 2)",
            "l > (select max(i) from y)",
            "l < (select avg(d) from y)",
            // BYTE and SHORT columns
            "b > (select max(i) from y where k < 3)",
            "s <= (select min(i) from y where k > 3)",
            // NULL: a sub-query with no row, and one whose value is NULL
            "d > (select d from y where k < 0)",
            "d >= (select d from y where k < 0)",
            "d <= (select null::double from y limit 1)",
            "i >= (select i from y where k < 0)",
            "i < (select null::int from y limit 1)",
            "l <= (select null::long from y limit 1)",
            "l >= (select l from y where k < 0)",
            // in a larger predicate, the TAQ idx 58 shape included
            "sym = 'a' and (f >= (select min(f) from y where k > 6) or d <= (select max(d) from y where k < 2))",
            "not (d > (select avg(d) from y)) and i > 3",
            "d > (select avg(d) from y) and d < (select max(d) from y) and l > 30",
            "sym in ('a', 'c') and i > (select min(i) from y)",
    };

    // the reviewer's probes: values at the edges of the comparisons' semantics
    private static final String[] EDGE_FILTERS = {
            // BYTE / SHORT vs NULL INT scalar (Java: never true)
            "b > (select i from y where k < 0)",
            "b >= (select i from y where k < 0)",
            "b <= (select null::int from y limit 1)",
            "s < (select null::int from y limit 1)",
            "b = (select i from y where k < 0)",
            "b != (select i from y where k < 0)",
            "not (b >= (select i from y where k < 0))",
            // BYTE / SHORT vs NULL LONG / DOUBLE scalar
            "b >= (select null::long from y limit 1)",
            "b < (select null::long from y limit 1)",
            "s >= (select null::double from y limit 1)",
            "s < (select null::double from y limit 1)",
            "b != (select null::double from y limit 1)",
            // INT vs NULL DOUBLE / LONG scalar (Java widens INT NULL to NaN / LONG_NULL)
            "i >= (select d from y where k < 0)",
            "i <= (select null::double from y limit 1)",
            "i = (select null::double from y limit 1)",
            "i != (select null::double from y limit 1)",
            "i > (select null::long from y limit 1)",
            "i >= (select null::long from y limit 1)",
            "i = (select null::long from y limit 1)",
            "i != (select null::long from y limit 1)",
            "not (i >= (select null::long from y limit 1))",
            // LONG vs NULL INT scalar
            "l >= (select null::int from y limit 1)",
            "l = (select i from y where k < 0)",
            "l != (select i from y where k < 0)",
            // INT vs LONG beyond the int range
            "i < (select 3000000000L from y limit 1)",
            "i > (select -3000000000L from y limit 1)",
            // FLOAT vs a DOUBLE no float holds
            "f > (select 0.1 from y limit 1)",
            "f < (select 0.1 from y limit 1)",
            "f = (select 0.1 from y limit 1)",
            "f >= (select 0.3 from y limit 1)",
            "f != (select 0.1 from y limit 1)",
            // the DOUBLE tolerance, 1e-10
            "d > (select 1.0 from y limit 1)",
            "d >= (select 1.0 from y limit 1)",
            "d < (select 1.0 from y limit 1)",
            "d = (select 1.0 from y limit 1)",
            "d != (select 1.0 from y limit 1)",
            // NULL = NULL for doubles
            "d = (select null::double from y limit 1)",
            "d != (select null::double from y limit 1)",
            // infinities are NULL in QuestDB
            "d < (select 1.0 / 0.0 from y limit 1)",
            "d >= (select 1.0 / 0.0 from y limit 1)",
            "d = (select 1.0 / 0.0 from y limit 1)",
            "d > (select -1.0 / 0.0 from y limit 1)",
            "i < (select 1.0 / 0.0 from y limit 1)",
            // NaN produced as a value
            "d <= (select 0.0 / 0.0 from y limit 1)",
            // the same sub-query twice
            "(select max(d) from y) > d and i > (select min(i) from y)",
            // OR with sub-queries
            "d > (select max(d) from y) or i < (select min(i) from y)",
            // now() in the sub-query: uncorrelated, runs per execution
            "l > (select count() from y where now() > 0::timestamp)",
    };

    @Override
    @Before
    public void setUp() {
        super.setUp();
        inputRoot = root;
    }

    @Test
    public void testAllFilters() throws Exception {
        assertMemoryLeak(() -> {
            createTables();
            final String[] shapes = {
                    "select * from x where %s",
                    "select count() from x where %s",
                    "select sym, count(), sum(d), max(i) from x where %s order by sym",
                    "select * from x where %s order by l desc limit 9",
            };
            for (String filter : FILTERS) {
                for (String shape : shapes) {
                    assertAllModes(String.format(shape, filter));
                }
            }
        });
    }

    @Test
    public void testCachedFactorySeesNewValue() throws Exception {
        // the sub-query runs per execution: a cached factory binds the new value after y changes
        assertMemoryLeak(() -> {
            final String[] queries = {
                    "select * from x where d > (select max(d) - 30 from y)",
                    "select count() from x where i < (select min(i) + 10 from y) and l > 0",
                    "select sym, count() from x where l >= (select max(l) - 20 from y) order by sym",
            };
            for (String query : queries) {
                for (Mode mode : Mode.values()) {
                    createTables();
                    mode.apply();
                    try (RecordCursorFactory factory = select(query)) {
                        Assert.assertEquals(mode + ": " + query, mode.expectsJit(), factory.usesCompiledFilter());
                        for (int round = 0; round < 3; round++) {
                            final String actual = print(factory);
                            final String expected = referenceOf(query);
                            TestUtils.assertEquals(mode + ", round " + round + ": " + query, expected, actual);
                            // move every sub-query's value
                            execute("truncate table y");
                            execute("insert into y select x, (x * 37 % 500 + " + (round + 1) * 40 + ")::int, x * 41 + " + (round + 1) * 300
                                    + ", x * 13.25 + " + (round + 1) * 90 + ", (x * 7.5)::float from long_sequence(10)");
                            mode.apply();
                        }
                    }
                    execute("drop table x");
                    execute("drop table y");
                }
            }
        });
    }

    @Test
    public void testCorrelatedSubQuery() throws Exception {
        // a sub-query that reads the outer row is not a constant of the execution: whatever the
        // compiler makes of it, every mode returns the same rows or the same error
        assertMemoryLeak(() -> {
            createTables();
            final String query = "select * from x where d > (select max(y.d) from y where y.k = x.k % 10)";
            String expected;
            try {
                expected = referenceOf(query);
            } catch (SqlException e) {
                expected = "error: " + e.getFlyweightMessage();
            }
            for (Mode mode : Mode.values()) {
                mode.apply();
                String actual;
                try (RecordCursorFactory factory = select(query)) {
                    actual = print(factory);
                } catch (SqlException e) {
                    actual = "error: " + e.getFlyweightMessage();
                }
                TestUtils.assertEquals(mode + ": " + query, expected, actual);
            }
        });
    }

    @Test
    public void testExplainAndFeatureOff() throws Exception {
        assertMemoryLeak(() -> {
            createTables();
            final String query = "select * from x where sym = 'a' and (f >= (select min(f) from y where k > 6) or d <= (select max(d) from y where k < 2))";
            sqlExecutionContext.setJitMode(SqlJitMode.JIT_MODE_ENABLED);
            setProperty(PropertyKey.CAIRO_SQL_JIT_SCALAR_SUBQUERY_ENABLED, "false");
            assertPlanContains("explain " + query, "Async Filter");
            setProperty(PropertyKey.CAIRO_SQL_JIT_SCALAR_SUBQUERY_ENABLED, "true");
            if (JitUtil.isJitSupported()) {
                assertPlanContains("explain " + query, "Async JIT Filter");
                assertPlanContains("explain select count() from x where d > (select avg(d) from y)", "Async JIT");
            }
        });
    }

    @Test
    public void testNotEligibleSubQueriesKeepJavaFilter() throws Exception {
        // a sub-query that is not a comparison operand runs in the Java filter, with the same rows
        assertMemoryLeak(() -> {
            createTables();
            final String[] queries = {
                    "select * from x where sym in (select sym from x where k < 3)",
                    "select * from x where ts > (select min(ts) from x) and d > 0",
                    "select * from x where (select count() from y) > 3 and d > 0",
                    "select * from x where length(sym) > (select max(k) from y where k < 2)",
            };
            for (String query : queries) {
                final String expected = referenceOf(query);
                for (Mode mode : Mode.values()) {
                    mode.apply();
                    try (RecordCursorFactory factory = select(query)) {
                        TestUtils.assertEquals(mode + ": " + query, expected, print(factory));
                    }
                }
            }
        });
    }

    @Test
    public void testEdgeValues() throws Exception {
        assertMemoryLeak(() -> {
            createTables();
            // values around the DOUBLE tolerance and floats no double equals
            execute("insert into x (d, f, i, l, b, s, sym, k, ts) values " +
                    "(1.00000000005, 0.1, 5, 5, 1, 1, 'a', 2000, '1970-01-02T00:00:00.000000Z')," +
                    "(0.99999999995, 0.3, 6, 6, 1, 1, 'a', 2001, '1970-01-02T00:00:01.000000Z')," +
                    "(1.0, 0.1, 7, 7, 1, 1, 'a', 2002, '1970-01-02T00:00:02.000000Z')");
            final String[] shapes = {
                    "select * from x where %s",
                    "select count() from x where %s",
                    "select sym, count(), sum(d) from x where %s order by sym",
            };
            for (String filter : EDGE_FILTERS) {
                for (String shape : shapes) {
                    assertAllModes(String.format(shape, filter));
                }
            }
        });
    }

    @Test
    public void testJavaFilterAndLinkInitialisedInEitherOrder() throws Exception {
        // A consumer may initialise the bind variables before the Java filter. Each execution must
        // still bind its own value of the sub-query, never the one an earlier execution read.
        Assume.assumeTrue(JitUtil.isJitSupported());
        assertMemoryLeak(() -> {
            createTables();
            Mode.JIT_VECTOR.apply();
            try (RecordCursorFactory factory = select("select * from x where d > (select max(d) from y)")) {
                final AsyncJitFilteredRecordCursorFactory jitFactory = findJitFactory(factory);
                final Function filter = jitFactory.getFilter();
                Assert.assertTrue(filter instanceof AbstractScalarCursorFunction);
                final AbstractScalarCursorFunction source = (AbstractScalarCursorFunction) filter;
                final ObjList<Function> bindVars = jitFactory.getBindVarFunctions();
                Assert.assertEquals(1, bindVars.size());
                final Function link = bindVars.getQuick(0);
                double value = 1000;
                // the link first, over several executions: the value it binds is read before the
                // Java filter's init() runs, so it must be this execution's
                for (int execution = 0; execution < 3; execution++) {
                    value += 1000;
                    execute("insert into y (k, d) values (" + (100 + execution) + ", " + value + ")");
                    Function.init(bindVars, null, sqlExecutionContext, null);
                    Assert.assertEquals("link first, execution " + execution, value, link.getDouble(null), 0);
                    filter.init(null, sqlExecutionContext);
                    Assert.assertEquals("link first, execution " + execution, value, source.getScalarDouble(), 0);
                    Assert.assertEquals(value, link.getDouble(null), 0);
                }
                // the Java filter first, as every consumer does today
                for (int execution = 0; execution < 3; execution++) {
                    value += 1000;
                    execute("insert into y (k, d) values (" + (200 + execution) + ", " + value + ")");
                    filter.init(null, sqlExecutionContext);
                    Assert.assertEquals("filter first, execution " + execution, value, source.getScalarDouble(), 0);
                    Function.init(bindVars, null, sqlExecutionContext, null);
                    Assert.assertEquals("filter first, execution " + execution, value, link.getDouble(null), 0);
                }
                // the link initialised again within an execution reads afresh, and the next
                // execution's Java filter does not reuse that run
                value += 1000;
                execute("insert into y (k, d) values (300, " + value + ")");
                Function.init(bindVars, null, sqlExecutionContext, null);
                Assert.assertEquals(value, link.getDouble(null), 0);
                value += 1000;
                execute("insert into y (k, d) values (301, " + value + ")");
                filter.init(null, sqlExecutionContext);
                Assert.assertEquals(value, source.getScalarDouble(), 0);
                Function.init(bindVars, null, sqlExecutionContext, null);
                Assert.assertEquals(value, link.getDouble(null), 0);
                // and the factory still returns the Java filter's rows
                TestUtils.assertEquals(referenceOf("select * from x where d > (select max(d) from y)"), print(factory));
            }
        });
    }

    @Test
    public void testReadParquetPushdownKeepsJitBinding() throws Exception {
        // read_parquet() parses the filter a second time for its row group pushdown: the scalar
        // sub-query must still be bound in the JIT filter
        Assume.assumeTrue(JitUtil.isJitSupported());
        assertMemoryLeak(() -> {
            createTables();
            try (
                    Path path = new Path();
                    PartitionDescriptor partitionDescriptor = new PartitionDescriptor();
                    TableReader reader = engine.getReader("x")
            ) {
                path.of(root).concat("x.parquet");
                PartitionEncoder.populateFromTableReader(reader, partitionDescriptor, 0);
                PartitionEncoder.encode(partitionDescriptor, path);
                Assert.assertTrue(Files.exists(path.$()));
            }
            final String[] filters = {
                    "d > (select avg(d) from y)",
                    "d > (select avg(d) from y) and i < (select max(i) from y where k < 5)",
                    "k > 100 and l <= (select max(l) from y)",
            };
            for (String filter : filters) {
                final String query = "select * from read_parquet('x.parquet') where " + filter;
                final String expected = referenceOf(query);
                for (Mode mode : Mode.values()) {
                    mode.apply();
                    try (RecordCursorFactory factory = select(query)) {
                        Assert.assertEquals(mode + " JIT: " + query, mode.expectsJit(), factory.usesCompiledFilter());
                        TestUtils.assertEquals(mode + ": " + query, expected, print(factory));
                    }
                }
            }
        });
    }

    @Test
    public void testSubQueryErrorsAsJavaFilter() throws Exception {
        // more than one row fails the execution, in every mode, with the same message
        assertMemoryLeak(() -> {
            createTables();
            final String query = "select * from x where d > (select d from y)";
            String reference = null;
            for (Mode mode : Mode.values()) {
                mode.apply();
                try (RecordCursorFactory factory = select(query)) {
                    Assert.assertEquals(mode + ": " + query, mode.expectsJit(), factory.usesCompiledFilter());
                    print(factory);
                    Assert.fail("expected an error: " + mode);
                } catch (SqlException e) {
                    if (reference == null) {
                        reference = e.getFlyweightMessage().toString();
                    }
                    Assert.assertEquals(mode.toString(), reference, e.getFlyweightMessage().toString());
                }
            }
        });
    }

    @Test
    public void testUserBindVariables() throws Exception {
        // user bind variables in the outer filter and inside the sub-query, on a cached factory
        // re-executed with new values and new sub-query rows
        assertMemoryLeak(() -> {
            final String[] queries = {
                    "select * from x where d > (select max(d) from y where k < $1) and i < $2",
                    "select * from x where i < $2 and d > (select max(d) from y where k < $1) and l > $3",
                    "select count() from x where d < (select avg(d) from y where k > $1) and l != $3",
            };
            for (String query : queries) {
                for (Mode mode : Mode.values()) {
                    createTables();
                    bindVariableService.clear();
                    bindVariableService.setLong(0, 10);
                    bindVariableService.setInt(1, 200);
                    bindVariableService.setLong(2, 100);
                    mode.apply();
                    try (RecordCursorFactory factory = select(query)) {
                        if (!query.startsWith("select count()")) {
                            Assert.assertEquals(mode + " JIT: " + query, mode.expectsJit(), factory.usesCompiledFilter());
                        }
                        for (int round = 0; round < 3; round++) {
                            bindVariableService.setLong(0, 3 + round * 30);
                            bindVariableService.setInt(1, 200 - round * 50);
                            bindVariableService.setLong(2, 100 + round * 200);
                            final String actual = print(factory);
                            final String expected = referenceOf(query);
                            mode.apply();
                            TestUtils.assertEquals(mode + ", round " + round + ": " + query, expected, actual);
                            execute("insert into y (k, d) select 100 + x, x * 3.0 + " + round + " from long_sequence(5)");
                        }
                    }
                    execute("drop table x");
                    execute("drop table y");
                }
            }
        });
    }

    private static AsyncJitFilteredRecordCursorFactory findJitFactory(RecordCursorFactory factory) {
        for (RecordCursorFactory f = factory; f != null; f = f.getBaseFactory()) {
            if (f instanceof AsyncJitFilteredRecordCursorFactory jitFactory) {
                return jitFactory;
            }
        }
        throw new AssertionError("no JIT filter in the plan");
    }

    private void assertAllModes(String query) throws SqlException {
        final String expected = referenceOf(query);
        for (Mode mode : Mode.values()) {
            mode.apply();
            try (RecordCursorFactory factory = select(query)) {
                if (!query.startsWith("select count()")) {
                    Assert.assertEquals(mode + " JIT: " + query, mode.expectsJit(), factory.usesCompiledFilter());
                }
                TestUtils.assertEquals(mode + ": " + query, expected, print(factory));
            }
        }
    }

    private void assertPlanContains(String explain, String fragment) throws SqlException {
        sink.clear();
        printSql(explain, sink);
        TestUtils.assertContains(sink, fragment);
    }

    private void createTables() throws SqlException {
        execute("create table x as (select " +
                "case when x % 11 = 0 then null else x * 0.75 end d, " +
                "case when x % 13 = 0 then null else (x * 0.5)::float end f, " +
                "case when x % 7 = 0 then null else (x % 300)::int end i, " +
                "case when x % 9 = 0 then null else x * 2 end l, " +
                "(x % 100)::byte b, (x % 1000)::short s, " +
                "rnd_symbol('a', 'b', 'c', null) sym, x k, timestamp_sequence(0, 1000000) ts " +
                "from long_sequence(1500)) timestamp(ts) partition by hour bypass wal");
        execute("create table y as (select x k, (x * 37 % 500)::int i, x * 41 l, x * 13.25 d, (x * 7.5)::float f " +
                "from long_sequence(10))");
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

        private final boolean enabled;
        private final int jitMode;

        Mode(int jitMode, boolean enabled) {
            this.jitMode = jitMode;
            this.enabled = enabled;
        }

        void apply() {
            setProperty(PropertyKey.CAIRO_SQL_JIT_SCALAR_SUBQUERY_ENABLED, enabled ? "true" : "false");
            sqlExecutionContext.setJitMode(jitMode);
        }

        boolean expectsJit() {
            return jitMode != SqlJitMode.JIT_MODE_DISABLED && enabled && JitUtil.isJitSupported();
        }
    }
}
