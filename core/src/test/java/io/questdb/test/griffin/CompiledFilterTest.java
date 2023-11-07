/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2023 QuestDB
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

import io.questdb.cairo.ColumnType;
import io.questdb.cairo.SqlJitMode;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.engine.functions.bind.BindVariableServiceImpl;
import io.questdb.jit.JitUtil;
import io.questdb.std.Numbers;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.Before;
import org.junit.Test;

/**
 * Tests for advanced features and scenarios, such as col tops, bind variables,
 * random access, record behavior, and so on.
 */
public class CompiledFilterTest extends AbstractCairoTest {

    @Before
    public void setUp() {
        // Disable the test suite on ARM64.
        Assume.assumeTrue(JitUtil.isJitSupported());

        super.setUp();
    }

    @Test
    public void testAllBindVariableTypes() throws Exception {
        assertMemoryLeak(() -> {

            ddl("create table x as (select" +
                    " rnd_boolean() aboolean," +
                    " rnd_byte(2,50) abyte," +
                    " rnd_geohash(4) ageobyte," +
                    " rnd_short(10,1024) ashort," +
                    " rnd_geohash(12) ageoshort," +
                    " rnd_char() achar," +
                    " rnd_int() anint," +
                    " rnd_geohash(16) ageoint," +
                    " rnd_symbol(4,4,4,2) asymbol," +
                    " rnd_float(2) afloat," +
                    " rnd_long() along," +
                    " rnd_double(2) adouble," +
                    " rnd_date(to_date('2015', 'yyyy'), to_date('2016', 'yyyy'), 2) adate," +
                    " rnd_geohash(32) ageolong," +
                    " timestamp_sequence(400000000000, 500000000) atimestamp" +
                    " from long_sequence(100)) timestamp(atimestamp)");

            bindVariableService.clear();
            bindVariableService.setBoolean("aboolean", false);
            bindVariableService.setByte("abyte", (byte) 28);
            bindVariableService.setGeoHash("ageobyte", 0, ColumnType.getGeoHashTypeWithBits(4));
            bindVariableService.setShort("ashort", (short) 243);
            bindVariableService.setGeoHash("ageoshort", 0b011011000010L, ColumnType.getGeoHashTypeWithBits(12));
            bindVariableService.setChar("achar", 'O');
            bindVariableService.setInt("anint", 2085282008);
            bindVariableService.setGeoHash("ageoint", 0b0101011010111101L, ColumnType.getGeoHashTypeWithBits(16));
            bindVariableService.setStr("asymbol", "HYRX");
            bindVariableService.setFloat("afloat", 0.48820507526397705f);
            bindVariableService.setLong("along", -4986232506486815364L);
            bindVariableService.setDouble("adouble", 0.42281342727402726);
            bindVariableService.setDate("adate", 1443479385706L);
            bindVariableService.setGeoHash("ageolong", 0b11010000001110101000110100011010L, ColumnType.getGeoHashTypeWithBits(32));
            bindVariableService.setTimestamp("atimestamp", 400500000000L);

            final String query = "select * from x where" +
                    " aboolean = :aboolean" +
                    " and abyte = :abyte" +
                    " and ageobyte = :ageobyte" +
                    " and ashort = :ashort" +
                    " and ageoshort = :ageoshort" +
                    " and achar = :achar" +
                    " and anint = :anint" +
                    " and ageoint = :ageoint" +
                    " and asymbol = :asymbol" +
                    " and afloat = :afloat" +
                    " and along = :along" +
                    " and adouble = :adouble" +
                    " and adate = :adate" +
                    " and ageolong = :ageolong" +
                    " and atimestamp = :atimestamp";
            final String expected = "aboolean\tabyte\tageobyte\tashort\tageoshort\tachar\tanint\tageoint\tasymbol\tafloat\talong\tadouble\tadate\tageolong\tatimestamp\n" +
                    "false\t28\t0000\t243\t011011000010\tO\t2085282008\t0101011010111101\tHYRX\t0.4882\t-4986232506486815364\t0.42281342727402726\t2015-09-28T22:29:45.706Z\t11010000001110101000110100011010\t1970-01-05T15:15:00.000000Z\n";

            assertSql(expected, query);
            assertSqlRunWithJit(query);
        });
    }

    @Test
    public void testBindVariableNullCheckScalar() throws Exception {
        testBindVariableNullCheck(SqlJitMode.JIT_MODE_FORCE_SCALAR);
    }

    @Test
    public void testBindVariableNullCheckVectorized() throws Exception {
        testBindVariableNullCheck(SqlJitMode.JIT_MODE_ENABLED);
    }

    @Test
    public void testBindVariablesFilterWithColTops() throws Exception {
        assertMemoryLeak(() -> {
            bindVariableService.clear();
            bindVariableService.setLong(0, 3);

            final String query = "select * from t1 where x = $1";
            final String expected = "x\tts\tj\n" +
                    "3\t1970-01-01T00:00:02.000000Z\tNaN\n" +
                    "3\t1970-01-01T00:01:42.000000Z\t7746536061816329025\n";

            testFilterWithColTops(query, expected, SqlJitMode.JIT_MODE_ENABLED, false);
        });
    }

    @Test
    public void testDeferredSymbolConstants() throws Exception {
        assertMemoryLeak(() -> {
            ddl("create table x as (select" +
                    " timestamp_sequence(400000000000, 500000000) ts," +
                    " x l," +
                    " rnd_symbol('A','B','C') sym" +
                    " from long_sequence(5)) timestamp(ts)");

            // The column order is important here, since we want
            // query and table column indexes to be different.
            final String query = "select sym, l, ts from x where sym = 'B' or sym = 'D' or sym = 'F'";
            final String expected = "sym\tl\tts\n" +
                    "B\t3\t1970-01-05T15:23:20.000000Z\n";

            assertSql(expected, query);
            assertSqlRunWithJit(query);

            ddl("insert into x select " +
                    " timestamp_sequence(500000000000, 500000000) ts," +
                    " (x+5) l," +
                    " rnd_symbol('D','E','F') sym " +
                    "from long_sequence(5)");

            final String expected2 = "sym\tl\tts\n" +
                    "B\t3\t1970-01-05T15:23:20.000000Z\n" +
                    "F\t6\t1970-01-06T18:53:20.000000Z\n" +
                    "F\t7\t1970-01-06T19:01:40.000000Z\n" +
                    "D\t9\t1970-01-06T19:18:20.000000Z\n";

            assertSql(expected2, query);
            assertSqlRunWithJit(query);
        });
    }

    @Test
    public void testIndexBindVariableReplacedContext() throws Exception {
        assertMemoryLeak(() -> {
            ddl("create table x as (select" +
                    " x l," +
                    " timestamp_sequence(400000000000, 500000000) ts" +
                    " from long_sequence(100)) timestamp(ts)");

            sqlExecutionContext.setJitMode(SqlJitMode.JIT_MODE_DISABLED);
            indexBindVariableReplacedContext(false);

            sqlExecutionContext.setJitMode(SqlJitMode.JIT_MODE_ENABLED);
            indexBindVariableReplacedContext(true);
        });
    }

    @Test
    public void testMixedSelectPreTouchEnabled() throws Exception {
        assertMemoryLeak(() -> {
            configOverrideColumnPreTouchEnabled(true);

            ddl("create table t1 as (select " +
                    " x," +
                    " timestamp_sequence(to_timestamp('1970-01-01', 'yyyy-MM-dd'), 100000L) ts " +
                    "from long_sequence(10)) timestamp(ts) partition by day");

            final String query = "select 1 as one, (4 + 2) as the_answer, ts as col_ts, x as col_x, sqrt(x) as root_x from t1 where x > 1";
            final String expected = "one\tthe_answer\tcol_ts\tcol_x\troot_x\n" +
                    "1\t6\t1970-01-01T00:00:00.100000Z\t2\t1.4142135623730951\n" +
                    "1\t6\t1970-01-01T00:00:00.200000Z\t3\t1.7320508075688772\n" +
                    "1\t6\t1970-01-01T00:00:00.300000Z\t4\t2.0\n" +
                    "1\t6\t1970-01-01T00:00:00.400000Z\t5\t2.23606797749979\n" +
                    "1\t6\t1970-01-01T00:00:00.500000Z\t6\t2.449489742783178\n" +
                    "1\t6\t1970-01-01T00:00:00.600000Z\t7\t2.6457513110645907\n" +
                    "1\t6\t1970-01-01T00:00:00.700000Z\t8\t2.8284271247461903\n" +
                    "1\t6\t1970-01-01T00:00:00.800000Z\t9\t3.0\n" +
                    "1\t6\t1970-01-01T00:00:00.900000Z\t10\t3.1622776601683795\n";

            assertSql(expected, query);
            assertSqlRunWithJit(query);
        });
    }

    @Test
    public void testMultiplePartitionsOrderBy() throws Exception {
        assertMemoryLeak(() -> {
            ddl("create table t1 as (select " +
                    " x," +
                    " timestamp_sequence(to_timestamp('1970-01-01', 'yyyy-MM-dd'), 100000L) ts " +
                    "from long_sequence(1000)) timestamp(ts) partition by day");

            ddl("insert into t1 select " +
                    " x," +
                    " timestamp_sequence(to_timestamp('1970-01-02', 'yyyy-MM-dd'), 100000L) ts " +
                    "from long_sequence(1000)");

            final String query = "select * from t1 where x < 3 order by ts desc";
            final String expected = "x\tts\n" +
                    "2\t1970-01-02T00:00:00.100000Z\n" +
                    "1\t1970-01-02T00:00:00.000000Z\n" +
                    "2\t1970-01-01T00:00:00.100000Z\n" +
                    "1\t1970-01-01T00:00:00.000000Z\n";

            assertSql(expected, query);
            assertSqlRunWithJit(query);
        });
    }

    @Test
    public void testNameBindVariableReplacedContext() throws Exception {
        assertMemoryLeak(() -> {
            ddl("create table x as (select" +
                    " x l," +
                    " timestamp_sequence(400000000000, 500000000) ts" +
                    " from long_sequence(100)) timestamp(ts)");

            sqlExecutionContext.setJitMode(SqlJitMode.JIT_MODE_DISABLED);
            namedBindVariableReplacedContext(false);

            sqlExecutionContext.setJitMode(SqlJitMode.JIT_MODE_ENABLED);
            namedBindVariableReplacedContext(true);
        });
    }

    @Test
    public void testPageFrameMaxSize() throws Exception {
        pageFrameMaxRows = 128;
        final long N = 8 * pageFrameMaxRows + 1;
        assertMemoryLeak(() -> {
            ddl("create table t1 as (select " +
                    " x," +
                    " timestamp_sequence(to_timestamp('1970-01-01', 'yyyy-MM-dd'), 100000L) ts " +
                    "from long_sequence(" + N + ")) timestamp(ts) partition by day");

            final String query = "select * from t1 where x < 3";
            final String expected = "x\tts\n" +
                    "1\t1970-01-01T00:00:00.000000Z\n" +
                    "2\t1970-01-01T00:00:00.100000Z\n";

            assertSql(expected, query);
            assertSqlRunWithJit(query);
        });
    }

    @Test
    public void testRandomAccessAfterToTop() throws Exception {
        assertMemoryLeak(() -> {
            ddl("create table x as (select" +
                    " x l," +
                    " timestamp_sequence(400000000000, 500000000) ts" +
                    " from long_sequence(5)) timestamp(ts)");

            final String query = "select * from x where l > 3";
            final String expected = "l\tts\n" +
                    "4\t1970-01-05T15:31:40.000000Z\n" +
                    "5\t1970-01-05T15:40:00.000000Z\n";

            assertSql(expected, query);
            assertSqlRunWithJit(query);

            try (RecordCursorFactory factory = select(query)) {
                try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                    final Record record = cursor.getRecord();
                    // 1. iteration
                    Assert.assertTrue(cursor.hasNext());
                    final long rowid = record.getRowId();
                    Assert.assertTrue(cursor.hasNext());
                    long l = record.getLong(factory.getMetadata().getColumnIndex("l"));
                    Assert.assertEquals(5, l);

                    // 2. reset iteration
                    cursor.toTop();

                    // 3. random access
                    cursor.recordAt(record, rowid);
                    l = record.getLong(factory.getMetadata().getColumnIndex("l"));
                    Assert.assertEquals(4, l);

                    // 4. iteration restarts
                    Assert.assertTrue(cursor.hasNext());
                    Assert.assertTrue(cursor.hasNext());
                    l = record.getLong(factory.getMetadata().getColumnIndex("l"));
                    Assert.assertEquals(5, l);
                }
            }
        });
    }

    @Test
    public void testRandomAccessWithColTops() throws Exception {
        assertMemoryLeak(() -> {
            ddl("create table x as (select" +
                    " x l," +
                    " timestamp_sequence(400000000000, 500000000) ts" +
                    " from long_sequence(5)) timestamp(ts)");

            ddl("alter table x add column j long", sqlExecutionContext);

            ddl("insert into x select " +
                    " (x+5) l," +
                    " timestamp_sequence(500000000000, 500000000) ts," +
                    " rnd_long() j " +
                    "from long_sequence(5)");

            final String query = "select * from x where l > 3 and j = null";
            final String expected = "l\tts\tj\n" +
                    "4\t1970-01-05T15:31:40.000000Z\tNaN\n" +
                    "5\t1970-01-05T15:40:00.000000Z\tNaN\n";

            assertSql(expected, query);
            assertSqlRunWithJit(query);

            try (RecordCursorFactory factory = select(query)) {
                try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                    final Record record = cursor.getRecord();
                    // 1. iteration
                    Assert.assertTrue(cursor.hasNext());
                    final long rowid = record.getRowId();
                    Assert.assertTrue(cursor.hasNext());
                    long l = record.getLong(factory.getMetadata().getColumnIndex("l"));
                    Assert.assertEquals(5, l);

                    // 2. random access
                    cursor.recordAt(record, rowid);
                    l = record.getLong(factory.getMetadata().getColumnIndex("l"));
                    Assert.assertEquals(4, l);

                    // 3. continue iteration
                    Assert.assertFalse(cursor.hasNext());
                }
            }
        });
    }

    @Test
    public void testSelectAllBothPageFramesFilterWithColTopsPreTouchEnabled() throws Exception {
        testSelectAllBothPageFramesFilterWithColTops(SqlJitMode.JIT_MODE_ENABLED, true);
    }

    @Test
    public void testSelectAllBothPageFramesFilterWithColTopsScalar() throws Exception {
        testSelectAllBothPageFramesFilterWithColTops(SqlJitMode.JIT_MODE_FORCE_SCALAR, false);
    }

    @Test
    public void testSelectAllBothPageFramesFilterWithColTopsVectorized() throws Exception {
        testSelectAllBothPageFramesFilterWithColTops(SqlJitMode.JIT_MODE_ENABLED, false);
    }

    @Test
    public void testSelectAllFilterWithColTopsPreTouchEnabled() throws Exception {
        testSelectAllFilterWithColTops(SqlJitMode.JIT_MODE_ENABLED, true);
    }

    @Test
    public void testSelectAllFilterWithColTopsScalar() throws Exception {
        testSelectAllFilterWithColTops(SqlJitMode.JIT_MODE_FORCE_SCALAR, false);
    }

    @Test
    public void testSelectAllFilterWithColTopsVectorized() throws Exception {
        testSelectAllFilterWithColTops(SqlJitMode.JIT_MODE_ENABLED, false);
    }

    @Test
    public void testSelectAllTypesFromRecordPreTouchDisabled() throws Exception {
        testSelectAllTypesFromRecord(false);
    }

    @Test
    public void testSelectAllTypesFromRecordPreTouchEnabled() throws Exception {
        testSelectAllTypesFromRecord(true);
    }

    @Test
    public void testSelectSingleColumnFilterWithColTopsPreTouchEnabled() throws Exception {
        testSelectSingleColumnFilterWithColTops(SqlJitMode.JIT_MODE_ENABLED, true);
    }

    @Test
    public void testSelectSingleColumnFilterWithColTopsScalar() throws Exception {
        testSelectSingleColumnFilterWithColTops(SqlJitMode.JIT_MODE_FORCE_SCALAR, false);
    }

    @Test
    public void testSelectSingleColumnFilterWithColTopsVectorized() throws Exception {
        testSelectSingleColumnFilterWithColTops(SqlJitMode.JIT_MODE_ENABLED, false);
    }

    @Test
    public void testSingleBindVariableScalar() throws Exception {
        testSingleBindVariable(SqlJitMode.JIT_MODE_FORCE_SCALAR);
    }

    @Test
    public void testSingleBindVariableVectorized() throws Exception {
        testSingleBindVariable(SqlJitMode.JIT_MODE_ENABLED);
    }

    @Test
    public void testSymbolBindVariable() throws Exception {
        assertMemoryLeak(() -> {
            ddl("create table x as (select" +
                    " rnd_symbol('A','B','C') sym," +
                    " timestamp_sequence(400000000000, 500000000) ts" +
                    " from long_sequence(5)) timestamp(ts)");

            bindVariableService.clear();
            bindVariableService.setStr("sym", "B");

            // The column order is important here, since we want
            // query and table column indexes to be different.
            final String query = "select ts, sym from x where sym = :sym";

            try (RecordCursorFactory factory = select(query)) {
                Assert.assertTrue("JIT was not enabled for query: " + query, factory.usesCompiledFilter());

                try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                    TestUtils.printCursor(cursor, factory.getMetadata(), true, sink, printer);
                }
                TestUtils.assertEquals("ts\tsym\n" +
                        "1970-01-05T15:23:20.000000Z\tB\n", sink);

                BindVariableServiceImpl bindService2 = new BindVariableServiceImpl(configuration);
                bindService2.setStr("sym", "C");
                try (
                        SqlExecutionContext context2 = TestUtils.createSqlExecutionCtx(engine, bindService2);
                        RecordCursor cursor = factory.getCursor(context2)
                ) {
                    TestUtils.printCursor(cursor, factory.getMetadata(), true, sink, printer);
                }
                TestUtils.assertEquals("ts\tsym\n" +
                        "1970-01-05T15:31:40.000000Z\tC\n" +
                        "1970-01-05T15:40:00.000000Z\tC\n", sink);
            }
        });
    }

    @Test
    public void testSymbolComparison() throws Exception {
        assertMemoryLeak(() -> {
            ddl("create table test (s symbol)");
            insert("insert into test values ('C'), ('B'), ('A')");

            assertSql("s\nB\nA\n", "select s from test where s <  'C'");
            assertSql("s\nC\nB\nA\n", "select s from test where s <= 'C'");
            assertSql("s\n", "select s from test where s >  'C'");
            assertSql("s\nC\n", "select s from test where s >= 'C'");

            assertSql("s\nA\n", "select s from test where s <  'B'");
            assertSql("s\nB\nA\n", "select s from test where s <= 'B'");
            assertSql("s\nC\n", "select s from test where s >  'B'");
            assertSql("s\nC\nB\n", "select s from test where s >= 'B'");

            assertSql("s\n", "select s from test where s <  'A'");
            assertSql("s\nA\n", "select s from test where s <= 'A'");
            assertSql("s\nC\nB\n", "select s from test where s >  'A'");
            assertSql("s\nC\nB\nA\n", "select s from test where s >= 'A'");

            assertSql("s\nC\nB\nA\n", "select s from test where s <  'Z'");
            assertSql("s\nC\nB\nA\n", "select s from test where s <= 'Z'");
            assertSql("s\n", "select s from test where s >  'Z'");
            assertSql("s\n", "select s from test where s >= 'Z'");

            assertSql("s\n", "select s from test where s <  null");
        });
    }

    private void indexBindVariableReplacedContext(boolean jit) throws SqlException {

        bindVariableService.clear();
        bindVariableService.setInt(0, 1);
        bindVariableService.setInt(1, 1000);

        final String query = "select $2 as a, l from x where l = $1";

        try (RecordCursorFactory factory = select(query)) {
            if (jit) {
                Assert.assertTrue("JIT was not enabled for query: " + query, factory.usesCompiledFilter());
            }

            try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                TestUtils.printCursor(cursor, factory.getMetadata(), true, sink, printer);
            }
            TestUtils.assertEquals("a\tl\n" +
                    "1000\t1\n", sink);

            BindVariableServiceImpl bindService2 = new BindVariableServiceImpl(configuration);
            bindService2.setInt(0, 2);
            bindService2.setInt(1, 1002);
            try (
                    SqlExecutionContext context2 = TestUtils.createSqlExecutionCtx(engine, bindService2);
                    RecordCursor cursor = factory.getCursor(context2)
            ) {
                TestUtils.printCursor(cursor, factory.getMetadata(), true, sink, printer);
            }
            TestUtils.assertEquals("a\tl\n" +
                    "1002\t2\n", sink);
        }
    }

    private void namedBindVariableReplacedContext(boolean jit) throws SqlException {

        bindVariableService.clear();
        bindVariableService.setInt("v1", 1);
        bindVariableService.setInt("v2", 1000);

        final String query = "select :v2 as a, l from x where l = :v1";

        try (RecordCursorFactory factory = select(query)) {
            if (jit) {
                Assert.assertTrue("JIT was not enabled for query: " + query, factory.usesCompiledFilter());
            }

            try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                TestUtils.printCursor(cursor, factory.getMetadata(), true, sink, printer);
            }
            TestUtils.assertEquals("a\tl\n" +
                    "1000\t1\n", sink);

            BindVariableServiceImpl bindService2 = new BindVariableServiceImpl(configuration);
            bindService2.setInt("v1", 2);
            bindService2.setInt("v2", 1002);

            try (
                    SqlExecutionContext context2 = TestUtils.createSqlExecutionCtx(engine, bindService2);
                    RecordCursor cursor = factory.getCursor(context2)
            ) {
                TestUtils.printCursor(cursor, factory.getMetadata(), true, sink, printer);
            }
            TestUtils.assertEquals("a\tl\n" +
                    "1002\t2\n", sink);
        }
    }

    private void testBindVariableNullCheck(int jitMode) throws Exception {
        assertMemoryLeak(() -> {
            sqlExecutionContext.setJitMode(jitMode);
            final long value = 42;
            ddl("create table x as (select" +
                    " " + value + " l," +
                    " to_timestamp('1971', 'yyyy') ts" +
                    " from long_sequence(1)) timestamp(ts)");

            bindVariableService.clear();
            bindVariableService.setLong("l", Numbers.LONG_NaN);

            // Here we expect a NULL value on the left side of the predicate,
            // so no rows should be returned
            final String query = "select * from x where l + :l = " + (Numbers.LONG_NaN + value);
            final String expected = "l\tts\n";

            assertSql(expected, query);
            assertSqlRunWithJit(query);
        });
    }

    private void testFilterWithColTops(String query, String expected, int jitMode, boolean preTouch) throws Exception {
        assertMemoryLeak(() -> {
            sqlExecutionContext.setJitMode(jitMode);
            configOverrideColumnPreTouchEnabled(preTouch);

            ddl("create table t1 as (select " +
                    " x," +
                    " timestamp_sequence(0, 1000000) ts " +
                    "from long_sequence(20)) timestamp(ts)");

            ddl("alter table t1 add column j long");

            ddl("insert into t1 select " +
                    " x," +
                    " timestamp_sequence(100000000, 1000000) ts," +
                    " rnd_long() j " +
                    "from long_sequence(20)");

            assertSql(expected, query);
            assertSqlRunWithJit(query);
        });
    }

    private void testSelectAllBothPageFramesFilterWithColTops(int jitMode, boolean preTouch) throws Exception {
        final String query = "select * from t1 where x >= 3 and x <= 4";
        final String expected = "x\tts\tj\n" +
                "3\t1970-01-01T00:00:02.000000Z\tNaN\n" +
                "4\t1970-01-01T00:00:03.000000Z\tNaN\n" +
                "3\t1970-01-01T00:01:42.000000Z\t7746536061816329025\n" +
                "4\t1970-01-01T00:01:43.000000Z\t-6945921502384501475\n";

        testFilterWithColTops(query, expected, jitMode, preTouch);
    }

    private void testSelectAllFilterWithColTops(int jitMode, boolean preTouch) throws Exception {
        final String query = "select * from t1 where j < 0";
        final String expected = "x\tts\tj\n" +
                "4\t1970-01-01T00:01:43.000000Z\t-6945921502384501475\n" +
                "7\t1970-01-01T00:01:46.000000Z\t-7611843578141082998\n" +
                "8\t1970-01-01T00:01:47.000000Z\t-5354193255228091881\n" +
                "9\t1970-01-01T00:01:48.000000Z\t-2653407051020864006\n" +
                "10\t1970-01-01T00:01:49.000000Z\t-1675638984090602536\n" +
                "14\t1970-01-01T00:01:53.000000Z\t-7489826605295361807\n" +
                "15\t1970-01-01T00:01:54.000000Z\t-4094902006239100839\n" +
                "16\t1970-01-01T00:01:55.000000Z\t-4474835130332302712\n" +
                "17\t1970-01-01T00:01:56.000000Z\t-6943924477733600060\n";

        testFilterWithColTops(query, expected, jitMode, preTouch);
    }

    private void testSelectAllTypesFromRecord(boolean preTouch) throws Exception {
        assertMemoryLeak(() -> {
            configOverrideColumnPreTouchEnabled(preTouch);

            final String query = "select * from x where b = true and kk < 10";
            final String expected = "kk\ta\tb\tc\td\te\tf\tg\ti\tj\tk\tl\tm\tn\tcc\tl2\thash1b\thash2b\thash3b\thash1c\thash2c\thash4c\thash8c\n" +
                    "2\t1637847416\ttrue\tV\t0.4900510449885239\t0.8258\t553\t2015-12-28T22:25:40.934Z\t\t-7611030538224290496\t1970-01-05T15:15:00.000000Z\t37\t00000000 3e e3 f1 f1 1e ca 9c 1d 06 ac\tKGHVUVSDOTSED\tY\t0x772c8b7f9505620ebbdfe8ff0cd60c64712fde5706d6ea2f545ded49c47eea61\t0\t10\t110\te\tsj\tfhcq\t35jvygt2\n" +
                    "3\t844704299\ttrue\t\t0.3456897991538844\t0.2401\t775\t2015-08-03T15:58:03.335Z\tVTJW\t-8910603140262731534\t1970-01-05T15:23:20.000000Z\t24\t00000000 ac a8 3b a6 dc 3b 7d 2b e3 92 fe 69 38 e1 77 9a\n" +
                    "00000010 e7 0c 89\tLJUMLGLHMLLEO\tY\t0xabbcbeeddca3d4fe4f25a88863fc0f467f24de22c77acf93e983e65f5551d073\t0\t01\t000\tf\t33\teusj\tb5z6npxr\n" +
                    "6\t-1501720177\ttrue\tP\t0.18158967304439033\t0.8197\t501\t2015-06-08T17:20:46.703Z\tPEHN\t-4229502740666959541\t1970-01-05T15:48:20.000000Z\t19\t\tTNLEGP\tU\t0x79423d4d320d2649767a4feda060d4fb6923c0c7d965969da1b1140a2be25241\t1\t01\t010\tr\tc0\twhjh\trcqfw2hw\n" +
                    "8\t526232578\ttrue\tE\t0.6379992093447574\t0.8515\t850\t2015-08-19T05:52:05.329Z\tPEHN\t-5157086556591926155\t1970-01-05T16:05:00.000000Z\t42\t00000000 6d 8c d8 ac c8 46 3b 47 3c e1 72 3b 9d\tJSMKIXEYVTUPD\tH\t0x2337f7e6b82ebc2405c5c1b231cffa455a6e970fb8b80abcc4129ae493cc6076\t0\t11\t000\t5\ttp\tx578\ttdnxkw6d\n";
            final String ddl = "create table x as (select" +
                    " cast(x as int) kk," +
                    " rnd_int() a," +
                    " rnd_boolean() b," +
                    " rnd_str(1,1,2) c," +
                    " rnd_double(2) d," +
                    " rnd_float(2) e," +
                    " rnd_short(10,1024) f," +
                    " rnd_date(to_date('2015', 'yyyy'), to_date('2016', 'yyyy'), 2) g," +
                    " rnd_symbol(4,4,4,2) i," +
                    " rnd_long() j," +
                    " timestamp_sequence(400000000000, 500000000) k," +
                    " rnd_byte(2,50) l," +
                    " rnd_bin(10, 20, 2) m," +
                    " rnd_str(5,16,2) n," +
                    " rnd_char() cc," +
                    " rnd_long256() l2," +
                    " rnd_geohash(1) hash1b," +
                    " rnd_geohash(2) hash2b," +
                    " rnd_geohash(3) hash3b," +
                    " rnd_geohash(5) hash1c," +
                    " rnd_geohash(10) hash2c," +
                    " rnd_geohash(20) hash4c," +
                    " rnd_geohash(40) hash8c" +
                    " from long_sequence(100)) timestamp(k)";

            assertQuery(expected,
                    query,
                    ddl,
                    "k",
                    true);
            assertSqlRunWithJit(query);
        });
    }

    private void testSelectSingleColumnFilterWithColTops(int jitMode, boolean preTouch) throws Exception {
        // The column order is important here, since we want
        // query and table column indexes to be different.
        final String query = "select j from t1 where j <> null and x < 3";
        final String expected = "j\n" +
                "4689592037643856\n" +
                "4729996258992366\n";

        testFilterWithColTops(query, expected, jitMode, preTouch);
    }

    private void testSingleBindVariable(int jitMode) throws Exception {
        assertMemoryLeak(() -> {
            sqlExecutionContext.setJitMode(jitMode);

            ddl("create table x as (select" +
                    " rnd_long() l," +
                    " timestamp_sequence(400000000000, 500000000) ts" +
                    " from long_sequence(100)) timestamp(ts)");

            bindVariableService.clear();
            bindVariableService.setLong("l", 3614738589890112276L);

            final String query = "select * from x where l = :l";
            final String expected = "l\tts\n" +
                    "3614738589890112276\t1970-01-05T16:38:20.000000Z\n";

            assertSql(expected, query);
            assertSqlRunWithJit(query);
        });
    }
}
