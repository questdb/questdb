/*******************************************************************************
 *    ___                  _   ____  ____
 *   / _ \ _   _  ___  ___| |_|  _ \| __ )
 *  | | | | | | |/ _ \/ __| __| | | |  _ \
 *  | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *   \__\_\\__,_|\___||___/\__|____/|____/
 *
 * Copyright (C) 2014-2019 Appsicle
 *
 * This program is free software: you can redistribute it and/or  modify
 * it under the terms of the GNU Affero General Public License, version 3,
 * as published by the Free Software Foundation.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 *
 ******************************************************************************/

package com.questdb.griffin;

import com.questdb.cairo.*;
import com.questdb.cairo.security.AllowAllCairoSecurityContext;
import com.questdb.cairo.sql.RecordCursorFactory;
import com.questdb.griffin.engine.functions.rnd.SharedRandom;
import com.questdb.griffin.engine.functions.str.TestMatchFunctionFactory;
import com.questdb.std.Chars;
import com.questdb.std.FilesFacade;
import com.questdb.std.FilesFacadeImpl;
import com.questdb.std.Rnd;
import com.questdb.std.str.LPSZ;
import com.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class SqlCodeGeneratorTest extends AbstractGriffinTest {

    @Before
    public void setUp3() {
        SharedRandom.RANDOM.set(new Rnd());
    }

    @Test
    public void testAmbiguousFunction() throws Exception {
        assertQuery("column\n" +
                        "234990000000000\n",
                "select 23499000000000*10 from long_sequence(1)",
                null, null);
    }

    @Test
    public void testBindVariableInSelect() throws Exception {
        TestUtils.assertMemoryLeak(() -> {

            final CairoConfiguration configuration = new DefaultCairoConfiguration(root);
            try (
                    CairoEngine engine = new CairoEngine(configuration);
                    SqlCompiler compiler = new SqlCompiler(engine)
            ) {
                bindVariableService.clear();
                bindVariableService.setLong(0, 10);
                try (RecordCursorFactory factory = compiler.compile("select x, ? from long_sequence(2)", sqlExecutionContext)) {
                    assertCursor("x\t?\n" +
                                    "1\t10\n" +
                                    "2\t10\n",
                            factory,
                            true
                    );
                }
            }
        });
    }

    @Test
    public void testBindVariableInSelect2() throws Exception {
        TestUtils.assertMemoryLeak(() -> {

            final CairoConfiguration configuration = new DefaultCairoConfiguration(root);
            try (
                    CairoEngine engine = new CairoEngine(configuration);
                    SqlCompiler compiler = new SqlCompiler(engine)
            ) {
                bindVariableService.clear();
                bindVariableService.setLong("y", 10);
                try (RecordCursorFactory factory = compiler.compile("select x, :y from long_sequence(2)", sqlExecutionContext)) {
                    assertCursor("x\t:y\n" +
                                    "1\t10\n" +
                                    "2\t10\n",
                            factory,
                            true
                    );
                }
            }
        });
    }

    @Test
    public void testBindVariableInSelect3() throws Exception {
        TestUtils.assertMemoryLeak(() -> {

            final CairoConfiguration configuration = new DefaultCairoConfiguration(root);
            try (
                    CairoEngine engine = new CairoEngine(configuration);
                    SqlCompiler compiler = new SqlCompiler(engine)
            ) {
                bindVariableService.clear();
                bindVariableService.setLong(0, 10);
                try (RecordCursorFactory factory = compiler.compile("select x, $1 from long_sequence(2)", sqlExecutionContext)) {
                    assertCursor("x\t$1\n" +
                                    "1\t10\n" +
                                    "2\t10\n",
                            factory,
                            true
                    );
                }
            }
        });
    }

    @Test
    public void testBindVariableInWhere() throws Exception {
        TestUtils.assertMemoryLeak(() -> {

            final CairoConfiguration configuration = new DefaultCairoConfiguration(root);
            try (
                    CairoEngine engine = new CairoEngine(configuration);
                    SqlCompiler compiler = new SqlCompiler(engine)
            ) {
                bindVariableService.clear();
                bindVariableService.setLong(0, 10);
                try (RecordCursorFactory factory = compiler.compile("select x from long_sequence(100) where x = ?", sqlExecutionContext)) {
                    assertCursor("x\n" +
                                    "10\n",
                            factory,
                            true
                    );
                }
            }
        });
    }

    @Test
    public void testCreateTableSymbolColumnViaCastCached() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            Assert.assertNull(compiler.compile("create table x (col string)", sqlExecutionContext));

            engine.releaseAllReaders();
            engine.releaseAllWriters();

            CairoConfiguration configuration = new DefaultCairoConfiguration(root) {
                @Override
                public boolean getDefaultSymbolCacheFlag() {
                    return false;
                }
            };

            try (
                    CairoEngine engine = new CairoEngine(configuration);
                    SqlCompiler compiler = new SqlCompiler(engine)
            ) {
                compiler.compile("create table y as (x), cast(col as symbol cache)", sqlExecutionContext);

                try (TableReader reader = engine.getReader(AllowAllCairoSecurityContext.INSTANCE, "y", TableUtils.ANY_TABLE_VERSION)) {
                    Assert.assertTrue(reader.getSymbolMapReader(0).isCached());
                }
            }
        });
    }

    @Test
    public void testCreateTableSymbolColumnViaCastCachedSymbolCapacityHigh() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            Assert.assertNull(compiler.compile("create table x (col string)", sqlExecutionContext));
            try {
                compiler.compile("create table y as (x), cast(col as symbol capacity 100000000)", sqlExecutionContext);
                Assert.fail();
            } catch (SqlException e) {
                Assert.assertEquals(51, e.getPosition());
                TestUtils.assertContains(e.getMessage(), "max cached symbol capacity");
            }

            engine.releaseAllWriters();
            engine.releaseAllReaders();
        });
    }

    @Test
    public void testCreateTableSymbolColumnViaCastNocache() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            Assert.assertNull(compiler.compile("create table x (col string)", sqlExecutionContext));

            compiler.compile("create table y as (x), cast(col as symbol nocache)", sqlExecutionContext);

            try (TableReader reader = engine.getReader(AllowAllCairoSecurityContext.INSTANCE, "y", TableUtils.ANY_TABLE_VERSION)) {
                Assert.assertFalse(reader.getSymbolMapReader(0).isCached());
            }

            engine.releaseAllReaders();
            engine.releaseAllWriters();
        });
    }

    @Test
    public void testFilterAPI() throws Exception {
        TestMatchFunctionFactory.clear();
        // no index
        final String expected = "a\tb\tk\n" +
                "80.432240999684\tCPSW\t1970-01-01T00:00:00.000000Z\n" +
                "93.446048573940\tPEHN\t1970-01-02T03:46:40.000000Z\n" +
                "88.992869122897\tSXUX\t1970-01-03T07:33:20.000000Z\n" +
                "42.177688419694\tGPGW\t1970-01-04T11:20:00.000000Z\n" +
                "66.938371476317\tDEYY\t1970-01-05T15:06:40.000000Z\n" +
                "0.359836721543\tHFOW\t1970-01-06T18:53:20.000000Z\n" +
                "21.583224269349\tYSBE\t1970-01-07T22:40:00.000000Z\n" +
                "12.503042190293\tSHRU\t1970-01-09T02:26:40.000000Z\n" +
                "67.004763918011\tQULO\t1970-01-10T06:13:20.000000Z\n" +
                "81.016127417126\tTJRS\t1970-01-11T10:00:00.000000Z\n" +
                "24.593452776060\tRFBV\t1970-01-12T13:46:40.000000Z\n" +
                "49.005104498852\tOOZZ\t1970-01-13T17:33:20.000000Z\n" +
                "18.769708157331\tMYIC\t1970-01-14T21:20:00.000000Z\n" +
                "22.822335965268\tUICW\t1970-01-16T01:06:40.000000Z\n" +
                "88.282283666977\t\t1970-01-17T04:53:20.000000Z\n" +
                "45.659895188240\tDOTS\t1970-01-18T08:40:00.000000Z\n" +
                "97.030608082441\tCTGQ\t1970-01-19T12:26:40.000000Z\n" +
                "12.024160875735\tWCKY\t1970-01-20T16:13:20.000000Z\n" +
                "63.591449938914\tDSWU\t1970-01-21T20:00:00.000000Z\n" +
                "50.652283361564\tLNVT\t1970-01-22T23:46:40.000000Z\n";

        assertQuery(expected,
                "select * from x where test_match()",
                "create table x as " +
                        "(" +
                        "select" +
                        " rnd_double(0)*100 a," +
                        " rnd_str(4,4,1) b," +
                        " timestamp_sequence(to_timestamp(0), 100000000000) k" +
                        " from" +
                        " long_sequence(20)" +
                        ") timestamp(k) partition by DAY",
                "k");

        // these values are assured to be correct for the scenario
        Assert.assertEquals(4, TestMatchFunctionFactory.getOpenCount());
        Assert.assertEquals(4, TestMatchFunctionFactory.getTopCount());
        Assert.assertEquals(1, TestMatchFunctionFactory.getCloseCount());
    }

    @Test
    public void testFilterFunctionOnSubQuery() throws Exception {
        // no index
        final String expected = "a\tb\tk\n" +
                "93.446048573940\tPEHN\t1970-01-02T03:46:40.000000Z\n" +
                "88.282283666977\t\t1970-01-17T04:53:20.000000Z\n";

        assertQuery(expected,
                "select * from x where to_symbol(b) in (select rnd_str('PEHN', 'HYRX', null) a from long_sequence(10))",
                "create table x as " +
                        "(" +
                        "select" +
                        " rnd_double(0)*100 a," +
                        " rnd_str(4,4,1) b," +
                        " timestamp_sequence(to_timestamp(0), 100000000000) k" +
                        " from" +
                        " long_sequence(20)" +
                        ") timestamp(k) partition by DAY",
                "k",
                "insert into x select * from (" +
                        "select" +
                        " rnd_double(0)*100," +
                        " 'HYRX'," +
                        " to_timestamp('1971', 'yyyy') t" +
                        " from long_sequence(1)" +
                        ") timestamp(t)",
                expected +
                        "97.595346366902\tHYRX\t1971-01-01T00:00:00.000000Z\n");
    }

    @Test
    public void testFilterOnConstantFalse() throws Exception {
        assertQuery(null,
                "select * from x o where 10 < 8",
                "create table x as " +
                        "(" +
                        "select * from" +
                        " random_cursor" +
                        "(20," +
                        " 'a', rnd_double(0)*100," +
                        " 'b', rnd_symbol(5,4,4,1)," +
                        " 'k', timestamp_sequence(to_timestamp(0), 1000000000)" +
                        ")" +
                        "), index(b) timestamp(k)",
                "k",
                "insert into x select * from (" +
                        "select" +
                        " rnd_double(0)*100," +
                        " rnd_symbol(5,4,4,1)," +
                        " timestamp_sequence(to_timestamp('2019', 'yyyy'), 1000000000) timestamp" +
                        " from long_sequence(50)" +
                        ") timestamp(timestamp)",
                null,
                false);
    }

    @Test
    public void testFilterOnInterval() throws Exception {
        assertQuery("a\tb\tk\n" +
                        "84.452581772111\tPEHN\t1970-01-01T03:36:40.000000Z\n" +
                        "97.501988537251\t\t1970-01-01T03:53:20.000000Z\n" +
                        "49.005104498852\tPEHN\t1970-01-01T04:10:00.000000Z\n",
                "select * from x o where k = '1970-01-01T03:36:40;45m'",
                "create table x as " +
                        "(" +
                        "select * from" +
                        " random_cursor" +
                        "(20," +
                        " 'a', rnd_double(0)*100," +
                        " 'b', rnd_symbol(5,4,4,1)," +
                        " 'k', timestamp_sequence(to_timestamp(0), 1000000000)" +
                        ")" +
                        "), index(b) timestamp(k)",
                "k");
    }

    @Test
    public void testFilterOnIntervalAndFilter() throws Exception {
        TestMatchFunctionFactory.clear();

        assertQuery("a\tb\tk\n" +
                        "84.452581772111\tPEHN\t1970-01-01T03:36:40.000000Z\n" +
                        "97.501988537251\t\t1970-01-01T03:53:20.000000Z\n",
                "select * from x o where k = '1970-01-01T03:36:40;45m' and a > 50 and test_match()",
                "create table x as " +
                        "(" +
                        "select * from" +
                        " random_cursor" +
                        "(20," +
                        " 'a', rnd_double(0)*100," +
                        " 'b', rnd_symbol(5,4,4,1)," +
                        " 'k', timestamp_sequence(to_timestamp(0), 1000000000)" +
                        ")" +
                        "), index(b) timestamp(k)",
                "k");

        // also good numbers, extra top calls are due to symbol column API check
        // tables without symbol columns will skip this check
        Assert.assertEquals(4, TestMatchFunctionFactory.getOpenCount());
        Assert.assertEquals(6, TestMatchFunctionFactory.getTopCount());
        Assert.assertEquals(1, TestMatchFunctionFactory.getCloseCount());
    }

    @Test
    public void testFilterOnIntrinsicFalse() throws Exception {
        assertQuery(null,
                "select * from x o where o.b in ('HYRX','PEHN', null) and a < a",
                "create table x as (select * from random_cursor(20, 'a', rnd_double(0)*100, 'b', rnd_symbol(5,4,4,1), 'k', timestamp_sequence(to_timestamp(0), 1000000000))), index(b)",
                null,
                false);
    }

    @Test
    public void testInsertMissingQuery() throws Exception {
        assertFailure(
                "insert into x (a,b)",
                "create table x as (select * from random_cursor(20, 'a', rnd_double(0)*100, 'b', rnd_symbol(5,4,4,1))), index(b)",
                19,
                "'select' expected"
        );
    }

    @Test
    public void testFilterOnNull() throws Exception {
        final String expected = "a\tb\n" +
                "11.427984775756\t\n" +
                "87.996347253916\t\n" +
                "32.881769076795\t\n" +
                "57.934663268622\t\n" +
                "26.922103479745\t\n" +
                "52.984059417621\t\n" +
                "97.501988537251\t\n" +
                "80.011211397392\t\n" +
                "92.050039469858\t\n" +
                "45.634456960908\t\n" +
                "40.455469747939\t\n";
        assertQuery(expected,
                "select * from x where b = null",
                "create table x as (select * from random_cursor(20, 'a', rnd_double(0)*100, 'b', rnd_symbol(5,4,4,1))), index(b)",
                null,
                "insert into x (a,b)" +
                        " select" +
                        " rnd_double(0)*100, " +
                        " rnd_symbol(5,4,4,1)" +
                        " from long_sequence(15)",
                expected +
                        "54.491550215189\t\n" +
                        "76.923818943378\t\n" +
                        "58.912164838798\t\n" +
                        "44.804689668614\t\n" +
                        "89.409171265819\t\n" +
                        "3.993124821273\t\n");
    }

    @Test
    public void testFilterOnSubQueryIndexed() throws Exception {
        final String expected = "a\tb\tk\n" +
                "11.427984775756\t\t1970-01-01T00:00:00.000000Z\n" +
                "23.905290108465\tRXGZ\t1970-01-03T07:33:20.000000Z\n" +
                "87.996347253916\t\t1970-01-05T15:06:40.000000Z\n" +
                "32.881769076795\t\t1970-01-06T18:53:20.000000Z\n" +
                "97.711031460512\tHYRX\t1970-01-07T22:40:00.000000Z\n" +
                "57.934663268622\t\t1970-01-10T06:13:20.000000Z\n" +
                "12.026122412833\tHYRX\t1970-01-11T10:00:00.000000Z\n" +
                "26.922103479745\t\t1970-01-13T17:33:20.000000Z\n" +
                "52.984059417621\t\t1970-01-14T21:20:00.000000Z\n" +
                "97.501988537251\t\t1970-01-17T04:53:20.000000Z\n" +
                "80.011211397392\t\t1970-01-19T12:26:40.000000Z\n" +
                "92.050039469858\t\t1970-01-20T16:13:20.000000Z\n" +
                "45.634456960908\t\t1970-01-21T20:00:00.000000Z\n" +
                "40.455469747939\t\t1970-01-22T23:46:40.000000Z\n";

        assertQuery(expected,
                "select * from x where b in (select rnd_symbol('RXGZ', 'HYRX', null, 'ABC') a from long_sequence(10))",
                "create table x as " +
                        "(" +
                        "select" +
                        " rnd_double(0)*100 a," +
                        " rnd_symbol(5,4,4,1) b," +
                        " timestamp_sequence(to_timestamp(0), 100000000000) k" +
                        " from" +
                        " long_sequence(20)" +
                        "),index(b) timestamp(k) partition by DAY",
                "k",
                "insert into x select * from (" +
                        "select" +
                        " rnd_double(0)*100," +
                        " 'ABC'," +
                        " to_timestamp('1971', 'yyyy') t" +
                        " from long_sequence(1)" +
                        ") timestamp(t)",
                expected +
                        "95.400690890497\tABC\t1971-01-01T00:00:00.000000Z\n");
    }

    @Test
    public void testFilterOnSubQueryIndexedDeferred() throws Exception {

        assertQuery(null,
                "select * from x where b in (select rnd_symbol('ABC') a from long_sequence(10))",
                "create table x as " +
                        "(" +
                        "select" +
                        " rnd_double(0)*100 a," +
                        " rnd_symbol(5,4,4,1) b," +
                        " timestamp_sequence(to_timestamp(0), 100000000000) k" +
                        " from" +
                        " long_sequence(20)" +
                        "),index(b) timestamp(k) partition by DAY",
                "k",
                "insert into x select * from (" +
                        "select" +
                        " rnd_double(0)*100," +
                        " 'ABC'," +
                        " to_timestamp('1971', 'yyyy') t" +
                        " from long_sequence(5)" +
                        ") timestamp(t)",
                "a\tb\tk\n" +
                        "95.400690890497\tABC\t1971-01-01T00:00:00.000000Z\n" +
                        "25.533193397031\tABC\t1971-01-01T00:00:00.000000Z\n" +
                        "89.409171265819\tABC\t1971-01-01T00:00:00.000000Z\n" +
                        "28.799739396819\tABC\t1971-01-01T00:00:00.000000Z\n" +
                        "68.068731346264\tABC\t1971-01-01T00:00:00.000000Z\n");
    }

    @Test
    public void testFilterOnSubQueryIndexedFiltered() throws Exception {

        TestMatchFunctionFactory.clear();

        final String expected = "a\tb\tk\n" +
                "11.427984775756\t\t1970-01-01T00:00:00.000000Z\n" +
                "23.905290108465\tRXGZ\t1970-01-03T07:33:20.000000Z\n" +
                "32.881769076795\t\t1970-01-06T18:53:20.000000Z\n" +
                "57.934663268622\t\t1970-01-10T06:13:20.000000Z\n" +
                "12.026122412833\tHYRX\t1970-01-11T10:00:00.000000Z\n" +
                "26.922103479745\t\t1970-01-13T17:33:20.000000Z\n" +
                "52.984059417621\t\t1970-01-14T21:20:00.000000Z\n" +
                "45.634456960908\t\t1970-01-21T20:00:00.000000Z\n" +
                "40.455469747939\t\t1970-01-22T23:46:40.000000Z\n";

        assertQuery(expected,
                "select * from x where b in (select rnd_symbol('RXGZ', 'HYRX', null, 'ABC') a from long_sequence(10)) and test_match()" +
                        "and a < 80",
                "create table x as " +
                        "(" +
                        "select" +
                        " rnd_double(0)*100 a," +
                        " rnd_symbol(5,4,4,1) b," +
                        " timestamp_sequence(to_timestamp(0), 100000000000) k" +
                        " from" +
                        " long_sequence(20)" +
                        "),index(b) timestamp(k) partition by DAY",
                "k",
                "insert into x select * from (" +
                        "select" +
                        " rnd_double(0)*100," +
                        " 'ABC'," +
                        " to_timestamp('1971', 'yyyy') t" +
                        " from long_sequence(5)" +
                        ") timestamp(t)",
                expected +
                        "25.533193397031\tABC\t1971-01-01T00:00:00.000000Z\n" +
                        "28.799739396819\tABC\t1971-01-01T00:00:00.000000Z\n" +
                        "68.068731346264\tABC\t1971-01-01T00:00:00.000000Z\n");

        // these value are also ok because ddl2 is present, there is another round of check for that
        Assert.assertEquals(6, TestMatchFunctionFactory.getOpenCount());
        Assert.assertEquals(12, TestMatchFunctionFactory.getTopCount());
        Assert.assertEquals(1, TestMatchFunctionFactory.getCloseCount());

    }

    @Test
    public void testFilterOnSubQueryIndexedStrColumn() throws Exception {

        assertQuery(null,
                "select * from x where b in (select 'ABC' a from long_sequence(10))",
                "create table x as " +
                        "(" +
                        "select" +
                        " rnd_double(0)*100 a," +
                        " rnd_symbol(5,4,4,1) b," +
                        " timestamp_sequence(to_timestamp(0), 100000000000) k" +
                        " from" +
                        " long_sequence(20)" +
                        "),index(b) timestamp(k) partition by DAY",
                "k",
                "insert into x select * from (" +
                        "select" +
                        " rnd_double(0)*100," +
                        " 'ABC'," +
                        " to_timestamp('1971', 'yyyy') t" +
                        " from long_sequence(1)" +
                        ") timestamp(t)",
                "a\tb\tk\n" +
                        "56.594291398612\tABC\t1971-01-01T00:00:00.000000Z\n");
    }

    @Test
    public void testFilterOnValues() throws Exception {
        final String expected1 = "a\tb\tk\n" +
                "11.427984775756\t\t1970-01-01T00:00:00.000000Z\n" +
                "70.943604871712\tPEHN\t1970-01-01T00:50:00.000000Z\n" +
                "87.996347253916\t\t1970-01-01T01:06:40.000000Z\n" +
                "32.881769076795\t\t1970-01-01T01:23:20.000000Z\n" +
                "97.711031460512\tHYRX\t1970-01-01T01:40:00.000000Z\n" +
                "81.468079445006\tPEHN\t1970-01-01T01:56:40.000000Z\n" +
                "57.934663268622\t\t1970-01-01T02:13:20.000000Z\n" +
                "12.026122412833\tHYRX\t1970-01-01T02:30:00.000000Z\n" +
                "26.922103479745\t\t1970-01-01T03:03:20.000000Z\n" +
                "52.984059417621\t\t1970-01-01T03:20:00.000000Z\n" +
                "84.452581772111\tPEHN\t1970-01-01T03:36:40.000000Z\n" +
                "97.501988537251\t\t1970-01-01T03:53:20.000000Z\n" +
                "49.005104498852\tPEHN\t1970-01-01T04:10:00.000000Z\n" +
                "80.011211397392\t\t1970-01-01T04:26:40.000000Z\n" +
                "92.050039469858\t\t1970-01-01T04:43:20.000000Z\n" +
                "45.634456960908\t\t1970-01-01T05:00:00.000000Z\n" +
                "40.455469747939\t\t1970-01-01T05:16:40.000000Z\n";

        assertQuery(expected1,
                "select * from x o where o.b in ('HYRX','PEHN', null, 'ABCD')",
                "create table x as (select * from random_cursor(20, 'a', rnd_double(0)*100, 'b', rnd_symbol(5,4,4,1), 'k', timestamp_sequence(to_timestamp(0), 1000000000))), index(b)",
                null,
                "insert into x (a,b)" +
                        " select" +
                        " rnd_double(0)*100," +
                        " rnd_symbol('HYRX','PEHN', null, 'ABCD')" +
                        " from" +
                        " long_sequence(10)",
                expected1 +
                        "56.594291398612\tHYRX\t\n" +
                        "68.216608610013\tPEHN\t\n" +
                        "96.441838325644\t\t\n" +
                        "11.585982949541\tABCD\t\n" +
                        "81.641825924675\tABCD\t\n" +
                        "54.491550215189\tPEHN\t\n" +
                        "76.923818943378\tABCD\t\n" +
                        "49.428905119585\tHYRX\t\n" +
                        "65.513358397963\tABCD\t\n" +
                        "28.200207166748\tABCD\t\n");
    }

    @Test
    public void testFilterOnValuesAndFilter() throws Exception {

        TestMatchFunctionFactory.clear();

        assertQuery("a\tb\tk\n" +
                        "11.427984775756\t\t1970-01-01T00:00:00.000000Z\n" +
                        "32.881769076795\t\t1970-01-01T01:23:20.000000Z\n" +
                        "12.026122412833\tHYRX\t1970-01-01T02:30:00.000000Z\n" +
                        "26.922103479745\t\t1970-01-01T03:03:20.000000Z\n" +
                        "49.005104498852\tPEHN\t1970-01-01T04:10:00.000000Z\n" +
                        "45.634456960908\t\t1970-01-01T05:00:00.000000Z\n" +
                        "40.455469747939\t\t1970-01-01T05:16:40.000000Z\n",
                "select * from x o where o.b in ('HYRX','PEHN', null) and a < 50 and test_match()",
                "create table x as (" +
                        "select" +
                        " *" +
                        " from" +
                        " random_cursor(20, 'a', rnd_double(0)*100, 'b', rnd_symbol(5,4,4,1), 'k', timestamp_sequence(to_timestamp(0), 1000000000))" +
                        ")," +
                        " index(b)",
                null,
                "insert into x (a,b)" +
                        " select" +
                        " rnd_double(0)*100," +
                        " rnd_symbol(5,4,4,1)" +
                        " from" +
                        " long_sequence(10)",
                "a\tb\tk\n" +
                        "11.427984775756\t\t1970-01-01T00:00:00.000000Z\n" +
                        "32.881769076795\t\t1970-01-01T01:23:20.000000Z\n" +
                        "12.026122412833\tHYRX\t1970-01-01T02:30:00.000000Z\n" +
                        "26.922103479745\t\t1970-01-01T03:03:20.000000Z\n" +
                        "49.005104498852\tPEHN\t1970-01-01T04:10:00.000000Z\n" +
                        "45.634456960908\t\t1970-01-01T05:00:00.000000Z\n" +
                        "40.455469747939\t\t1970-01-01T05:16:40.000000Z\n" +
                        "44.804689668614\t\t\n");

        // 5 opens is good because we check variable lengh column API sanity
        Assert.assertEquals(5, TestMatchFunctionFactory.getOpenCount());
        Assert.assertEquals(12, TestMatchFunctionFactory.getTopCount());
        Assert.assertEquals(1, TestMatchFunctionFactory.getCloseCount());
    }

    @Test
    public void testFilterOnValuesAndInterval() throws Exception {
        assertQuery("a\tb\tk\n" +
                        "84.452581772111\tPEHN\t1970-01-01T03:36:40.000000Z\n" +
                        "97.501988537251\t\t1970-01-01T03:53:20.000000Z\n" +
                        "49.005104498852\tPEHN\t1970-01-01T04:10:00.000000Z\n",
                "select * from x o where o.b in ('HYRX','PEHN', null) and k = '1970-01-01T03:36:40;45m'",
                "create table x as " +
                        "(" +
                        "select * from" +
                        " random_cursor" +
                        "(20," +
                        " 'a', rnd_double(0)*100," +
                        " 'b', rnd_symbol(5,4,4,1)," +
                        " 'k', timestamp_sequence(to_timestamp(0), 1000000000)" +
                        ")" +
                        "), index(b) timestamp(k)",
                "k");
    }

    @Test
    public void testFilterOnValuesDeferred() throws Exception {
        assertQuery(null,
                "select * from x o where o.b in ('ABCD', 'XYZ')",
                "create table x as (select * from random_cursor(20, 'a', rnd_double(0)*100, 'b', rnd_symbol(5,4,4,1), 'k', timestamp_sequence(to_timestamp(0), 1000000000))), index(b)",
                null,
                "insert into x (a,b)" +
                        " select" +
                        " rnd_double(0)*100," +
                        " rnd_symbol('HYRX','PEHN', null, 'ABCD')" +
                        " from" +
                        " long_sequence(10)",
                "a\tb\tk\n" +
                        "11.585982949541\tABCD\t\n" +
                        "81.641825924675\tABCD\t\n" +
                        "76.923818943378\tABCD\t\n" +
                        "65.513358397963\tABCD\t\n" +
                        "28.200207166748\tABCD\t\n");
    }

    @Test
    public void testFilterSingleKeyValue() throws Exception {
        final String expected = "a\tb\n" +
                "11.427984775756\tHYRX\n" +
                "52.984059417621\tHYRX\n" +
                "40.455469747939\tHYRX\n" +
                "72.300157631336\tHYRX\n";
        assertQuery(expected,
                "select * from x where b = 'HYRX'",
                "create table x as (select * from random_cursor(20, 'a', rnd_double(0)*100, 'b', rnd_symbol(5,4,4,0))), index(b)",
                null,
                "insert into x select" +
                        " rnd_double(0)*100," +
                        " 'HYRX'" +
                        " from long_sequence(2)",
                expected +
                        "75.881754034549\tHYRX\n" +
                        "57.789479151824\tHYRX\n");
    }

    @Test
    public void testFilterSingleKeyValueAndField() throws Exception {
        TestMatchFunctionFactory.clear();

        final String expected = "a\tb\n" +
                "52.984059417621\tHYRX\n" +
                "72.300157631336\tHYRX\n";
        assertQuery(expected,
                "select * from x where b = 'HYRX' and a > 41 and test_match()",
                "create table x as (select * from random_cursor(20, 'a', rnd_double(0)*100, 'b', rnd_symbol(5,4,4,0))), index(b)",
                null,
                "insert into x select" +
                        " rnd_double(0)*100," +
                        " 'HYRX'" +
                        " from long_sequence(2)",
                expected +
                        "75.881754034549\tHYRX\n" +
                        "57.789479151824\tHYRX\n");

        // 5 opens is good because we check variable lengh column API sanity
        Assert.assertEquals(5, TestMatchFunctionFactory.getOpenCount());
        Assert.assertEquals(12, TestMatchFunctionFactory.getTopCount());
        Assert.assertEquals(1, TestMatchFunctionFactory.getCloseCount());
    }

    @Test
    public void testFilterSingleNonExistingSymbol() throws Exception {
        assertQuery(null,
                "select * from x where b = 'ABC'",
                "create table x as (select * from random_cursor(20, 'a', rnd_double(0)*100, 'b', rnd_symbol(5,4,4,0))), index(b)",
                null,
                "insert into x select" +
                        " rnd_double(0)*100," +
                        " 'ABC'" +
                        " from long_sequence(2)",
                "a\tb\n" +
                        "75.881754034549\tABC\n" +
                        "57.789479151824\tABC\n");
    }

    @Test
    public void testFilterSingleNonExistingSymbolAndFilter() throws Exception {
        TestMatchFunctionFactory.clear();
        assertQuery(null,
                "select * from x where b = 'ABC' and a > 30 and test_match()",
                "create table x as (select * from random_cursor(20, 'a', rnd_double(0)*100, 'b', rnd_symbol(5,4,4,0))), index(b)",
                null,
                "insert into x select" +
                        " rnd_double(0)*100," +
                        " 'ABC'" +
                        " from long_sequence(2)",
                "a\tb\n" +
                        "75.881754034549\tABC\n" +
                        "57.789479151824\tABC\n");
        // 5 opens is good because we check variable length column API sanity
        Assert.assertEquals(5, TestMatchFunctionFactory.getOpenCount());
        Assert.assertEquals(8, TestMatchFunctionFactory.getTopCount());
        Assert.assertEquals(1, TestMatchFunctionFactory.getCloseCount());
    }

    @Test
    public void testFilterSubQuery() throws Exception {
        // no index
        final String expected = "a\tb\tk\n" +
                "11.427984775756\t\t1970-01-01T00:00:00.000000Z\n" +
                "23.905290108465\tRXGZ\t1970-01-03T07:33:20.000000Z\n" +
                "87.996347253916\t\t1970-01-05T15:06:40.000000Z\n" +
                "32.881769076795\t\t1970-01-06T18:53:20.000000Z\n" +
                "97.711031460512\tHYRX\t1970-01-07T22:40:00.000000Z\n" +
                "57.934663268622\t\t1970-01-10T06:13:20.000000Z\n" +
                "12.026122412833\tHYRX\t1970-01-11T10:00:00.000000Z\n" +
                "26.922103479745\t\t1970-01-13T17:33:20.000000Z\n" +
                "52.984059417621\t\t1970-01-14T21:20:00.000000Z\n" +
                "97.501988537251\t\t1970-01-17T04:53:20.000000Z\n" +
                "80.011211397392\t\t1970-01-19T12:26:40.000000Z\n" +
                "92.050039469858\t\t1970-01-20T16:13:20.000000Z\n" +
                "45.634456960908\t\t1970-01-21T20:00:00.000000Z\n" +
                "40.455469747939\t\t1970-01-22T23:46:40.000000Z\n";

        assertQuery(expected,
                "select * from x where b in (select rnd_symbol('RXGZ', 'HYRX', null) a from long_sequence(10))",
                "create table x as " +
                        "(" +
                        "select" +
                        " rnd_double(0)*100 a," +
                        " rnd_symbol(5,4,4,1) b," +
                        " timestamp_sequence(to_timestamp(0), 100000000000) k" +
                        " from" +
                        " long_sequence(20)" +
                        ") timestamp(k) partition by DAY",
                "k",
                "insert into x select * from (" +
                        "select" +
                        " rnd_double(0)*100," +
                        " 'RXGZ'," +
                        " to_timestamp('1971', 'yyyy') t" +
                        " from long_sequence(1)" +
                        ") timestamp(t)",
                expected +
                        "95.400690890497\tRXGZ\t1971-01-01T00:00:00.000000Z\n");
    }

    @Test
    public void testFilterSubQueryAddSymbol() throws Exception {
        // no index
        final String expected = "a\tb\tk\n" +
                "11.427984775756\t\t1970-01-01T00:00:00.000000Z\n" +
                "23.905290108465\tRXGZ\t1970-01-03T07:33:20.000000Z\n" +
                "87.996347253916\t\t1970-01-05T15:06:40.000000Z\n" +
                "32.881769076795\t\t1970-01-06T18:53:20.000000Z\n" +
                "97.711031460512\tHYRX\t1970-01-07T22:40:00.000000Z\n" +
                "57.934663268622\t\t1970-01-10T06:13:20.000000Z\n" +
                "12.026122412833\tHYRX\t1970-01-11T10:00:00.000000Z\n" +
                "26.922103479745\t\t1970-01-13T17:33:20.000000Z\n" +
                "52.984059417621\t\t1970-01-14T21:20:00.000000Z\n" +
                "97.501988537251\t\t1970-01-17T04:53:20.000000Z\n" +
                "80.011211397392\t\t1970-01-19T12:26:40.000000Z\n" +
                "92.050039469858\t\t1970-01-20T16:13:20.000000Z\n" +
                "45.634456960908\t\t1970-01-21T20:00:00.000000Z\n" +
                "40.455469747939\t\t1970-01-22T23:46:40.000000Z\n";

        assertQuery(expected,
                "select * from x where b in (select rnd_symbol('RXGZ', 'HYRX', 'ABC', null) a from long_sequence(10))",
                "create table x as " +
                        "(" +
                        "select" +
                        " rnd_double(0)*100 a," +
                        " rnd_symbol(5,4,4,1) b," +
                        " timestamp_sequence(to_timestamp(0), 100000000000) k" +
                        " from" +
                        " long_sequence(20)" +
                        ") timestamp(k) partition by DAY",
                "k",
                "insert into x select * from (" +
                        "select" +
                        " rnd_double(0)*100," +
                        " 'ABC'," +
                        " to_timestamp('1971', 'yyyy') t" +
                        " from long_sequence(1)" +
                        ") timestamp(t)",
                expected +
                        "95.400690890497\tABC\t1971-01-01T00:00:00.000000Z\n");
    }

    @Test
    public void testFilterSubQueryStrColumn() throws Exception {
        // no index
        final String expected = "a\tb\tk\n" +
                "11.427984775756\t\t1970-01-01T00:00:00.000000Z\n" +
                "23.905290108465\tRXGZ\t1970-01-03T07:33:20.000000Z\n" +
                "87.996347253916\t\t1970-01-05T15:06:40.000000Z\n" +
                "32.881769076795\t\t1970-01-06T18:53:20.000000Z\n" +
                "97.711031460512\tHYRX\t1970-01-07T22:40:00.000000Z\n" +
                "57.934663268622\t\t1970-01-10T06:13:20.000000Z\n" +
                "12.026122412833\tHYRX\t1970-01-11T10:00:00.000000Z\n" +
                "26.922103479745\t\t1970-01-13T17:33:20.000000Z\n" +
                "52.984059417621\t\t1970-01-14T21:20:00.000000Z\n" +
                "97.501988537251\t\t1970-01-17T04:53:20.000000Z\n" +
                "80.011211397392\t\t1970-01-19T12:26:40.000000Z\n" +
                "92.050039469858\t\t1970-01-20T16:13:20.000000Z\n" +
                "45.634456960908\t\t1970-01-21T20:00:00.000000Z\n" +
                "40.455469747939\t\t1970-01-22T23:46:40.000000Z\n";

        assertQuery(expected,
                "select * from x where b in (select rnd_str('RXGZ', 'HYRX', null) a from long_sequence(10))",
                "create table x as " +
                        "(" +
                        "select" +
                        " rnd_double(0)*100 a," +
                        " rnd_symbol(5,4,4,1) b," +
                        " timestamp_sequence(to_timestamp(0), 100000000000) k" +
                        " from" +
                        " long_sequence(20)" +
                        ") timestamp(k) partition by DAY",
                "k",
                "insert into x select * from (" +
                        "select" +
                        " rnd_double(0)*100," +
                        " 'RXGZ'," +
                        " to_timestamp('1971', 'yyyy') t" +
                        " from long_sequence(1)" +
                        ") timestamp(t)",
                expected +
                        "95.400690890497\tRXGZ\t1971-01-01T00:00:00.000000Z\n");
    }

    @Test
    public void testFilterSubQueryUnsupportedColType() throws Exception {
        assertFailure("select * from x where b in (select 12, rnd_str('RXGZ', 'HYRX', null) a from long_sequence(10))",
                "create table x as " +
                        "(" +
                        "select" +
                        " rnd_double(0)*100 a," +
                        " rnd_symbol(5,4,4,1) b," +
                        " timestamp_sequence(to_timestamp(0), 100000000000) k" +
                        " from" +
                        " long_sequence(20)" +
                        ") timestamp(k) partition by DAY",
                24,
                "supported column types are STRING and SYMBOL, found: INT");
    }

    @Test
    public void testFilterWrongType() throws Exception {
        assertFailure("select * from x where b - a",
                "create table x as " +
                        "(" +
                        "select" +
                        " rnd_double(0) a," +
                        " rnd_double(0) b," +
                        " rnd_symbol(5,4,4,1) c" +
                        " from long_sequence(10)" +
                        "), index(c)",
                24,
                "boolean expression expected"
        );
    }

    @Test
    public void testLatestByAll() throws Exception {
        assertQuery("a\tb\tk\n" +
                        "23.905290108465\tRXGZ\t1970-01-03T07:33:20.000000Z\n" +
                        "12.026122412833\tHYRX\t1970-01-11T10:00:00.000000Z\n" +
                        "48.820511018587\tVTJW\t1970-01-12T13:46:40.000000Z\n" +
                        "49.005104498852\tPEHN\t1970-01-18T08:40:00.000000Z\n" +
                        "40.455469747939\t\t1970-01-22T23:46:40.000000Z\n",
                "select * from x latest by b",
                "create table x as " +
                        "(" +
                        "select * from" +
                        " random_cursor" +
                        "(20," +
                        " 'a', rnd_double(0)*100," +
                        " 'b', rnd_symbol(5,4,4,1)," +
                        " 'k', timestamp_sequence(to_timestamp(0), 100000000000)" +
                        ")" +
                        ") timestamp(k) partition by DAY",
                "k",
                "insert into x select * from (" +
                        " select" +
                        " rnd_double(0)*100," +
                        " 'VTJW'," +
                        " to_timestamp('2019', 'yyyy') t" +
                        " from long_sequence(1)" +
                        ") timestamp (t)",
                "a\tb\tk\n" +
                        "23.905290108465\tRXGZ\t1970-01-03T07:33:20.000000Z\n" +
                        "12.026122412833\tHYRX\t1970-01-11T10:00:00.000000Z\n" +
                        "49.005104498852\tPEHN\t1970-01-18T08:40:00.000000Z\n" +
                        "40.455469747939\t\t1970-01-22T23:46:40.000000Z\n" +
                        "56.594291398612\tVTJW\t2019-01-01T00:00:00.000000Z\n");
    }

    @Test
    public void testLatestByAllBool() throws Exception {
        assertQuery("a\tb\tk\n" +
                        "97.552635405680\ttrue\t1970-01-20T16:13:20.000000Z\n" +
                        "37.625017094984\tfalse\t1970-01-22T23:46:40.000000Z\n",
                "select * from x latest by b",
                "create table x as " +
                        "(" +
                        "select * from" +
                        " random_cursor" +
                        "(20," +
                        " 'a', rnd_double(0)*100," +
                        " 'b', rnd_boolean()," +
                        " 'k', timestamp_sequence(to_timestamp(0), 100000000000)" +
                        ")" +
                        ") timestamp(k) partition by DAY",
                "k",
                "insert into x select * from (" +
                        " select" +
                        " rnd_double(0)*100," +
                        " false," +
                        " to_timestamp('2019', 'yyyy') t" +
                        " from long_sequence(1)" +
                        ") timestamp (t)",
                "a\tb\tk\n" +
                        "97.552635405680\ttrue\t1970-01-20T16:13:20.000000Z\n" +
                        "24.593452776060\tfalse\t2019-01-01T00:00:00.000000Z\n");
    }

    @Test
    public void testLatestByAllConstantFilter() throws Exception {
        final String expected = "a\tb\tk\n" +
                "23.905290108465\tRXGZ\t1970-01-03T07:33:20.000000Z\n" +
                "12.026122412833\tHYRX\t1970-01-11T10:00:00.000000Z\n" +
                "48.820511018587\tVTJW\t1970-01-12T13:46:40.000000Z\n" +
                "49.005104498852\tPEHN\t1970-01-18T08:40:00.000000Z\n" +
                "40.455469747939\t\t1970-01-22T23:46:40.000000Z\n";
        assertQuery(expected,
                "select * from x latest by b where 6 < 10",
                "create table x as " +
                        "(" +
                        "select * from" +
                        " random_cursor" +
                        "(20," +
                        " 'a', rnd_double(0)*100," +
                        " 'b', rnd_symbol(5,4,4,1)," +
                        " 'k', timestamp_sequence(to_timestamp(0), 100000000000)" +
                        ")" +
                        ") timestamp(k) partition by DAY",
                "k",
                "insert into x select * from (" +
                        " select" +
                        " rnd_double(0)*100," +
                        " 'VTJW'," +
                        " to_timestamp('2019', 'yyyy') t" +
                        " from long_sequence(1)" +
                        ") timestamp (t)",
                "a\tb\tk\n" +
                        "23.905290108465\tRXGZ\t1970-01-03T07:33:20.000000Z\n" +
                        "12.026122412833\tHYRX\t1970-01-11T10:00:00.000000Z\n" +
                        "49.005104498852\tPEHN\t1970-01-18T08:40:00.000000Z\n" +
                        "40.455469747939\t\t1970-01-22T23:46:40.000000Z\n" +
                        "56.594291398612\tVTJW\t2019-01-01T00:00:00.000000Z\n");
    }

    @Test
    public void testLatestByAllFilter() throws Exception {
        final String expected = "a\tb\tk\n" +
                "97.711031460512\tHYRX\t1970-01-07T22:40:00.000000Z\n" +
                "48.820511018587\tVTJW\t1970-01-12T13:46:40.000000Z\n" +
                "49.005104498852\tPEHN\t1970-01-18T08:40:00.000000Z\n" +
                "40.455469747939\t\t1970-01-22T23:46:40.000000Z\n";
        assertQuery(expected,
                "select * from x latest by b where a > 40",
                "create table x as " +
                        "(" +
                        "select * from" +
                        " random_cursor" +
                        "(20," +
                        " 'a', rnd_double(0)*100," +
                        " 'b', rnd_symbol(5,4,4,1)," +
                        " 'k', timestamp_sequence(to_timestamp(0), 100000000000)" +
                        ")" +
                        ") timestamp(k) partition by DAY",
                "k",
                "insert into x select * from (" +
                        " select" +
                        " rnd_double(0)*100," +
                        " 'VTJW'," +
                        " to_timestamp('2019', 'yyyy') t" +
                        " from long_sequence(1)" +
                        ") timestamp (t)",
                "a\tb\tk\n" +
                        "97.711031460512\tHYRX\t1970-01-07T22:40:00.000000Z\n" +
                        "49.005104498852\tPEHN\t1970-01-18T08:40:00.000000Z\n" +
                        "40.455469747939\t\t1970-01-22T23:46:40.000000Z\n" +
                        "56.594291398612\tVTJW\t2019-01-01T00:00:00.000000Z\n");
    }

    @Test
    public void testLatestByAllIndexed() throws Exception {
        final String expected = "a\tb\tk\n" +
                "23.905290108465\tRXGZ\t1970-01-03T07:33:20.000000Z\n" +
                "12.026122412833\tHYRX\t1970-01-11T10:00:00.000000Z\n" +
                "48.820511018587\tVTJW\t1970-01-12T13:46:40.000000Z\n" +
                "49.005104498852\tPEHN\t1970-01-18T08:40:00.000000Z\n" +
                "40.455469747939\t\t1970-01-22T23:46:40.000000Z\n";
        assertQuery(expected,
                "select * from x latest by b",
                "create table x as " +
                        "(" +
                        "select * from" +
                        " random_cursor" +
                        "(20," +
                        " 'a', rnd_double(0)*100," +
                        " 'b', rnd_symbol(5,4,4,1)," +
                        " 'k', timestamp_sequence(to_timestamp(0), 100000000000)" +
                        ")" +
                        "), index(b) timestamp(k) partition by DAY",
                "k",
                "insert into x select * from (" +
                        " select" +
                        " rnd_double(0)*100," +
                        " 'VTJW'," +
                        " to_timestamp('2019', 'yyyy') t" +
                        " from long_sequence(1)" +
                        ") timestamp (t)",
                "a\tb\tk\n" +
                        "23.905290108465\tRXGZ\t1970-01-03T07:33:20.000000Z\n" +
                        "12.026122412833\tHYRX\t1970-01-11T10:00:00.000000Z\n" +
                        "49.005104498852\tPEHN\t1970-01-18T08:40:00.000000Z\n" +
                        "40.455469747939\t\t1970-01-22T23:46:40.000000Z\n" +
                        "56.594291398612\tVTJW\t2019-01-01T00:00:00.000000Z\n");
    }

    @Test
    public void testLatestByAllIndexedConstantFilter() throws Exception {
        final String expected = "a\tb\tk\n" +
                "23.905290108465\tRXGZ\t1970-01-03T07:33:20.000000Z\n" +
                "12.026122412833\tHYRX\t1970-01-11T10:00:00.000000Z\n" +
                "48.820511018587\tVTJW\t1970-01-12T13:46:40.000000Z\n" +
                "49.005104498852\tPEHN\t1970-01-18T08:40:00.000000Z\n" +
                "40.455469747939\t\t1970-01-22T23:46:40.000000Z\n";
        assertQuery(expected,
                "select * from x latest by b where 5 > 2",
                "create table x as " +
                        "(" +
                        "select * from" +
                        " random_cursor" +
                        "(20," +
                        " 'a', rnd_double(0)*100," +
                        " 'b', rnd_symbol(5,4,4,1)," +
                        " 'k', timestamp_sequence(to_timestamp(0), 100000000000)" +
                        ")" +
                        "), index(b) timestamp(k) partition by DAY",
                "k",
                "insert into x select * from (" +
                        " select" +
                        " rnd_double(0)*100," +
                        " 'VTJW'," +
                        " to_timestamp('2019', 'yyyy') t" +
                        " from long_sequence(1)" +
                        ") timestamp (t)",
                "a\tb\tk\n" +
                        "23.905290108465\tRXGZ\t1970-01-03T07:33:20.000000Z\n" +
                        "12.026122412833\tHYRX\t1970-01-11T10:00:00.000000Z\n" +
                        "49.005104498852\tPEHN\t1970-01-18T08:40:00.000000Z\n" +
                        "40.455469747939\t\t1970-01-22T23:46:40.000000Z\n" +
                        "56.594291398612\tVTJW\t2019-01-01T00:00:00.000000Z\n");
    }

    @Test
    public void testLatestByAllIndexedFilter() throws Exception {
        final String expected = "a\tb\tk\n" +
                "48.820511018587\tVTJW\t1970-01-12T13:46:40.000000Z\n" +
                "49.005104498852\tPEHN\t1970-01-18T08:40:00.000000Z\n" +
                "40.455469747939\t\t1970-01-22T23:46:40.000000Z\n";
        assertQuery(expected,
                "select * from x latest by b where a > 40",
                "create table x as " +
                        "(" +
                        "select * from" +
                        " random_cursor" +
                        "(20," +
                        " 'a', rnd_double(0)*100," +
                        " 'b', rnd_symbol(5,4,4,1)," +
                        " 'k', timestamp_sequence(to_timestamp(0), 100000000000)" +
                        ")" +
                        "), index(b) timestamp(k) partition by DAY",
                "k",
                "insert into x select * from (" +
                        " select" +
                        " rnd_double(0)*100," +
                        " 'VTJW'," +
                        " to_timestamp('2019', 'yyyy') t" +
                        " from long_sequence(1)" +
                        ") timestamp (t)",
                "a\tb\tk\n" +
                        "49.005104498852\tPEHN\t1970-01-18T08:40:00.000000Z\n" +
                        "40.455469747939\t\t1970-01-22T23:46:40.000000Z\n" +
                        "56.594291398612\tVTJW\t2019-01-01T00:00:00.000000Z\n");
    }

    @Test
    public void testLatestByAllNewSymFilter() throws Exception {
        final String expected = "a\tb\tk\n" +
                "97.711031460512\tHYRX\t1970-01-07T22:40:00.000000Z\n" +
                "48.820511018587\tVTJW\t1970-01-12T13:46:40.000000Z\n" +
                "49.005104498852\tPEHN\t1970-01-18T08:40:00.000000Z\n" +
                "40.455469747939\t\t1970-01-22T23:46:40.000000Z\n";
        assertQuery(expected,
                "select * from x latest by b where a > 40",
                "create table x as " +
                        "(" +
                        "select * from" +
                        " random_cursor" +
                        "(20," +
                        " 'a', rnd_double(0)*100," +
                        " 'b', rnd_symbol(5,4,4,1)," +
                        " 'k', timestamp_sequence(to_timestamp(0), 100000000000)" +
                        ")" +
                        ") timestamp(k) partition by DAY",
                "k",
                "insert into x select * from (" +
                        " select" +
                        " rnd_double(0)*100," +
                        " 'CCKS'," +
                        " to_timestamp('2019', 'yyyy') t" +
                        " from long_sequence(1)" +
                        ") timestamp (t)",
                expected +
                        "56.594291398612\tCCKS\t2019-01-01T00:00:00.000000Z\n");
    }

    @Test
    public void testLatestByIOFailure() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            FilesFacade ff = new FilesFacadeImpl() {

                @Override
                public long openRO(LPSZ name) {
                    if (Chars.endsWith(name, "b.d")) {
                        return -1;
                    }
                    return super.openRO(name);
                }
            };
            CairoConfiguration configuration = new DefaultCairoConfiguration(root) {
                @Override
                public FilesFacade getFilesFacade() {
                    return ff;
                }
            };

            try (CairoEngine engine = new CairoEngine(configuration);
                 SqlCompiler compiler = new SqlCompiler(engine)) {
                try {
                    compiler.compile(("create table x as " +
                                    "(" +
                                    "select * from" +
                                    " random_cursor" +
                                    "(200," +
                                    " 'a', rnd_double(0)*100," +
                                    " 'b', rnd_symbol(5,4,4,1)," +
                                    " 'k', timestamp_sequence(to_timestamp(0), 100000000000)" +
                                    ")" +
                                    ") timestamp(k) partition by DAY"),
                            sqlExecutionContext);

                    try (final RecordCursorFactory factory = compiler.compile(
                            "select * from x latest by b where b = 'PEHN' and a < 22",
                            sqlExecutionContext
                    )) {
                        try {
                            assertCursor(("a\tb\tk\n" +
                                    "5.942010834028\tPEHN\t1970-08-03T02:53:20.000000Z\n"), factory, true);
                            Assert.fail();
                        } catch (CairoException e) {
                            TestUtils.assertContains(e.getMessage(), "Cannot open file");
                        }
                    }
                    Assert.assertEquals(0, engine.getBusyReaderCount());
                    Assert.assertEquals(0, engine.getBusyWriterCount());
                } finally {
                    engine.releaseAllWriters();
                    engine.releaseAllReaders();
                }
            }
        });
    }

    @Test
    public void testLatestByKeyValue() throws Exception {
        // no index
        assertQuery("a\tb\tk\n" +
                        "23.905290108465\tRXGZ\t1970-01-03T07:33:20.000000Z\n",
                "select * from x latest by b where b = 'RXGZ'",
                "create table x as " +
                        "(" +
                        "select * from" +
                        " random_cursor" +
                        "(20," +
                        " 'a', rnd_double(0)*100," +
                        " 'b', rnd_symbol(5,4,4,1)," +
                        " 'k', timestamp_sequence(to_timestamp(0), 100000000000)" +
                        ")" +
                        ") timestamp(k) partition by DAY",
                "k",
                "insert into x select * from (" +
                        "select" +
                        " rnd_double(0)*100," +
                        " 'RXGZ'," +
                        " to_timestamp('1971', 'yyyy') t" +
                        " from long_sequence(1)" +
                        ") timestamp(t)",
                "a\tb\tk\n" +
                        "56.594291398612\tRXGZ\t1971-01-01T00:00:00.000000Z\n");
    }

    @Test
    public void testLatestByKeyValueFiltered() throws Exception {
        TestMatchFunctionFactory.clear();
        assertQuery("a\tb\tk\n" +
                        "5.942010834028\tPEHN\t1970-08-03T02:53:20.000000Z\n",
                "select * from x latest by b where b = 'PEHN' and a < 22 and test_match()",
                "create table x as " +
                        "(" +
                        "select * from" +
                        " random_cursor" +
                        "(200," +
                        " 'a', rnd_double(0)*100," +
                        " 'b', rnd_symbol(5,4,4,1)," +
                        " 'k', timestamp_sequence(to_timestamp(0), 100000000000)" +
                        ")" +
                        ") timestamp(k) partition by DAY",
                "k",
                "insert into x select * from (" +
                        "select" +
                        " 11.3," +
                        " 'PEHN'," +
                        " to_timestamp('1971', 'yyyy') t" +
                        " from long_sequence(1)" +
                        ") timestamp(t)",
                "a\tb\tk\n" +
                        "11.300000000000\tPEHN\t1971-01-01T00:00:00.000000Z\n");

        // this is good
        Assert.assertEquals(6, TestMatchFunctionFactory.getOpenCount());
        Assert.assertEquals(12, TestMatchFunctionFactory.getTopCount());
        Assert.assertEquals(1, TestMatchFunctionFactory.getCloseCount());
    }

    @Test
    public void testLatestByKeyValueIndexed() throws Exception {
        assertQuery("a\tb\tk\n" +
                        "23.905290108465\tRXGZ\t1970-01-03T07:33:20.000000Z\n",
                "select * from x latest by b where b = 'RXGZ'",
                "create table x as " +
                        "(" +
                        "select * from" +
                        " random_cursor" +
                        "(20," +
                        " 'a', rnd_double(0)*100," +
                        " 'b', rnd_symbol(5,4,4,1)," +
                        " 'k', timestamp_sequence(to_timestamp(0), 100000000000)" +
                        ")" +
                        "), index(b) timestamp(k) partition by DAY",
                "k",
                "insert into x select * from (" +
                        "select" +
                        " rnd_double(0)*100," +
                        " 'RXGZ'," +
                        " to_timestamp('1971', 'yyyy') t" +
                        " from long_sequence(1)" +
                        ") timestamp(t)",
                "a\tb\tk\n" +
                        "56.594291398612\tRXGZ\t1971-01-01T00:00:00.000000Z\n");
    }

    @Test
    public void testLatestByKeyValueIndexedFiltered() throws Exception {
        TestMatchFunctionFactory.clear();
        assertQuery("a\tb\tk\n" +
                        "5.942010834028\tPEHN\t1970-08-03T02:53:20.000000Z\n",
                "select * from x latest by b where b = 'PEHN' and a < 22 and test_match()",
                "create table x as " +
                        "(" +
                        "select * from" +
                        " random_cursor" +
                        "(200," +
                        " 'a', rnd_double(0)*100," +
                        " 'b', rnd_symbol(5,4,4,1)," +
                        " 'k', timestamp_sequence(to_timestamp(0), 100000000000)" +
                        ")" +
                        "), index(b) timestamp(k) partition by DAY",
                "k",
                "insert into x select * from (" +
                        "select" +
                        " 11.3," +
                        " 'PEHN'," +
                        " to_timestamp('1971', 'yyyy') t" +
                        " from long_sequence(1)" +
                        ") timestamp(t)",
                "a\tb\tk\n" +
                        "11.300000000000\tPEHN\t1971-01-01T00:00:00.000000Z\n");

        Assert.assertEquals(6, TestMatchFunctionFactory.getOpenCount());
        Assert.assertEquals(12, TestMatchFunctionFactory.getTopCount());
        Assert.assertEquals(1, TestMatchFunctionFactory.getCloseCount());
    }

    @Test
    public void testLatestByKeyValueInterval() throws Exception {
        assertQuery("a\tb\tk\n" +
                        "84.452581772111\tPEHN\t1970-01-16T01:06:40.000000Z\n",
                "select * from x latest by b where b = 'PEHN' and k = '1970-01-06T18:53:20;11d'",
                "create table x as " +
                        "(" +
                        "select" +
                        " rnd_double(0)*100 a," +
                        " rnd_symbol(5,4,4,1) b," +
                        " timestamp_sequence(to_timestamp(0), 100000000000) k" +
                        " from" +
                        " long_sequence(20)" +
                        " random_cursor" +
                        "), index(b) timestamp(k) partition by DAY",
                "k");
    }

    @Test
    public void testLatestByKeyValues() throws Exception {
        // no index
        assertQuery("a\tb\tk\n" +
                        "23.905290108465\tRXGZ\t1970-01-03T07:33:20.000000Z\n" +
                        "12.026122412833\tHYRX\t1970-01-11T10:00:00.000000Z\n",
                "select * from x latest by b where b in ('RXGZ','HYRX')",
                "create table x as " +
                        "(" +
                        "select" +
                        " rnd_double(0)*100 a," +
                        " rnd_symbol(5,4,4,1) b," +
                        " timestamp_sequence(to_timestamp(0), 100000000000) k" +
                        " from" +
                        " long_sequence(20)" +
                        ") timestamp(k) partition by DAY",
                "k",
                "insert into x select * from (" +
                        "select" +
                        " rnd_double(0)*100," +
                        " 'RXGZ'," +
                        " to_timestamp('1971', 'yyyy') t" +
                        " from long_sequence(1)" +
                        ") timestamp(t)",
                "a\tb\tk\n" +
                        "12.026122412833\tHYRX\t1970-01-11T10:00:00.000000Z\n" +
                        "56.594291398612\tRXGZ\t1971-01-01T00:00:00.000000Z\n");
    }

    @Test
    public void testLatestByKeyValuesFiltered() throws Exception {
        TestMatchFunctionFactory.clear();
        // no index
        assertQuery("a\tb\tk\n" +
                        "23.905290108465\tRXGZ\t1970-01-03T07:33:20.000000Z\n" +
                        "12.026122412833\tHYRX\t1970-01-11T10:00:00.000000Z\n" +
                        "40.455469747939\t\t1970-01-22T23:46:40.000000Z\n",
                "select * from x latest by b where b in ('RXGZ','HYRX', null) and a > 12 and a < 50 and test_match()",
                "create table x as " +
                        "(" +
                        "select" +
                        " rnd_double(0)*100 a," +
                        " rnd_symbol(5,4,4,1) b," +
                        " timestamp_sequence(to_timestamp(0), 100000000000) k" +
                        " from" +
                        " long_sequence(20)" +
                        ") timestamp(k) partition by DAY",
                "k",
                "insert into x select * from (" +
                        "select" +
                        " rnd_double(0)*100," +
                        " 'RXGZ'," +
                        " to_timestamp('1971', 'yyyy') t" +
                        " from long_sequence(5)" +
                        ") timestamp(t)",
                "a\tb\tk\n" +
                        "12.026122412833\tHYRX\t1970-01-11T10:00:00.000000Z\n" +
                        "40.455469747939\t\t1970-01-22T23:46:40.000000Z\n" +
                        "12.105630273556\tRXGZ\t1971-01-01T00:00:00.000000Z\n");

        // this is good
        Assert.assertEquals(6, TestMatchFunctionFactory.getOpenCount());
        Assert.assertEquals(12, TestMatchFunctionFactory.getTopCount());
        Assert.assertEquals(1, TestMatchFunctionFactory.getCloseCount());
    }

    @Test
    public void testLatestByKeyValuesIndexed() throws Exception {
        assertQuery("a\tb\tk\n" +
                        "23.905290108465\tRXGZ\t1970-01-03T07:33:20.000000Z\n" +
                        "12.026122412833\tHYRX\t1970-01-11T10:00:00.000000Z\n",
                "select * from x latest by b where b in ('RXGZ','HYRX')",
                "create table x as " +
                        "(" +
                        "select" +
                        " rnd_double(0)*100 a," +
                        " rnd_symbol(5,4,4,1) b," +
                        " timestamp_sequence(to_timestamp(0), 100000000000) k" +
                        " from" +
                        " long_sequence(20)" +
                        "), index(b) timestamp(k) partition by DAY",
                "k",
                "insert into x select * from (" +
                        "select" +
                        " rnd_double(0)*100," +
                        " 'RXGZ'," +
                        " to_timestamp('1971', 'yyyy') t" +
                        " from long_sequence(1)" +
                        ") timestamp(t)",
                "a\tb\tk\n" +
                        "12.026122412833\tHYRX\t1970-01-11T10:00:00.000000Z\n" +
                        "56.594291398612\tRXGZ\t1971-01-01T00:00:00.000000Z\n");
    }

    @Test
    public void testLatestByKeyValuesIndexedFiltered() throws Exception {
        TestMatchFunctionFactory.clear();
        assertQuery("a\tb\tk\n" +
                        "23.905290108465\tRXGZ\t1970-01-03T07:33:20.000000Z\n" +
                        "97.711031460512\tHYRX\t1970-01-07T22:40:00.000000Z\n",
                "select * from x latest by b where b in ('RXGZ','HYRX') and a > 20 and test_match()",
                "create table x as " +
                        "(" +
                        "select" +
                        " rnd_double(0)*100 a," +
                        " rnd_symbol(5,4,4,1) b," +
                        " timestamp_sequence(to_timestamp(0), 100000000000) k" +
                        " from" +
                        " long_sequence(20)" +
                        "), index(b) timestamp(k) partition by DAY",
                "k",
                "insert into x select * from (" +
                        "select" +
                        " rnd_double(0)*100," +
                        " 'RXGZ'," +
                        " to_timestamp('1971', 'yyyy') t" +
                        " from long_sequence(1)" +
                        ") timestamp(t)",
                "a\tb\tk\n" +
                        "97.711031460512\tHYRX\t1970-01-07T22:40:00.000000Z\n" +
                        "56.594291398612\tRXGZ\t1971-01-01T00:00:00.000000Z\n");

        // this is good
        Assert.assertEquals(6, TestMatchFunctionFactory.getOpenCount());
        Assert.assertEquals(12, TestMatchFunctionFactory.getTopCount());
        Assert.assertEquals(1, TestMatchFunctionFactory.getCloseCount());
    }

    @Test
    public void testLatestByMissingKeyValue() throws Exception {
        assertQuery(null,
                "select * from x latest by b where b in ('XYZ')",
                "create table x as " +
                        "(" +
                        "select" +
                        " rnd_double(0)*100 a," +
                        " rnd_symbol(5,4,4,1) b," +
                        " timestamp_sequence(to_timestamp(0), 100000000000) k" +
                        " from" +
                        " long_sequence(20)" +
                        ") timestamp(k) partition by DAY",
                "k",
                "insert into x select * from (" +
                        "select" +
                        " rnd_double(0)*100," +
                        " 'XYZ'," +
                        " to_timestamp('1971', 'yyyy') t" +
                        " from long_sequence(3)" +
                        ") timestamp(t)",
                "a\tb\tk\n" +
                        "72.300157631336\tXYZ\t1971-01-01T00:00:00.000000Z\n");
    }

    @Test
    public void testLatestByMissingKeyValueFiltered() throws Exception {
        TestMatchFunctionFactory.clear();
        assertQuery(null,
                "select * from x latest by b where b in ('XYZ') and a < 60 and test_match()",
                "create table x as " +
                        "(" +
                        "select" +
                        " rnd_double(0)*100 a," +
                        " rnd_symbol(5,4,4,1) b," +
                        " timestamp_sequence(to_timestamp(0), 100000000000) k" +
                        " from" +
                        " long_sequence(20)" +
                        ") timestamp(k) partition by DAY",
                "k",
                "insert into x select * from (" +
                        "select" +
                        " rnd_double(0)*100," +
                        " 'XYZ'," +
                        " to_timestamp('1971', 'yyyy') t" +
                        " from long_sequence(3)" +
                        ") timestamp(t)",
                "a\tb\tk\n" +
                        "56.594291398612\tXYZ\t1971-01-01T00:00:00.000000Z\n");

        // this is good
        Assert.assertEquals(2, TestMatchFunctionFactory.getOpenCount());
        Assert.assertEquals(6, TestMatchFunctionFactory.getTopCount());
        Assert.assertEquals(1, TestMatchFunctionFactory.getCloseCount());
    }

    @Test
    public void testLatestByMissingKeyValueIndexed() throws Exception {
        assertQuery(null,
                "select * from x latest by b where b in ('XYZ')",
                "create table x as " +
                        "(" +
                        "select" +
                        " rnd_double(0)*100 a," +
                        " rnd_symbol(5,4,4,1) b," +
                        " timestamp_sequence(to_timestamp(0), 100000000000) k" +
                        " from" +
                        " long_sequence(20)" +
                        "), index(b) timestamp(k) partition by DAY",
                "k",
                "insert into x select * from (" +
                        "select" +
                        " rnd_double(0)*100," +
                        " rnd_symbol('XYZ', 'PEHN', 'ZZNK')," +
                        " timestamp_sequence(to_timestamp('1971', 'yyyy'), 100000000000) t" +
                        " from long_sequence(10)" +
                        ") timestamp(t)",
                "a\tb\tk\n" +
                        "81.641825924675\tXYZ\t1971-01-05T15:06:40.000000Z\n");
    }

    @Test
    public void testLatestByMissingKeyValueIndexedFiltered() throws Exception {
        TestMatchFunctionFactory.clear();
        assertQuery(null,
                "select * from x latest by b where b in ('XYZ') and a < 60 and test_match()",
                "create table x as " +
                        "(" +
                        "select" +
                        " rnd_double(0)*100 a," +
                        " rnd_symbol(5,4,4,1) b," +
                        " timestamp_sequence(to_timestamp(0), 100000000000) k" +
                        " from" +
                        " long_sequence(20)" +
                        "), index(b) timestamp(k) partition by DAY",
                "k",
                "insert into x select * from (" +
                        "select" +
                        " rnd_double(0)*100," +
                        " 'XYZ'," +
                        " to_timestamp('1971', 'yyyy') t" +
                        " from long_sequence(3)" +
                        ") timestamp(t)",
                "a\tb\tk\n" +
                        "56.594291398612\tXYZ\t1971-01-01T00:00:00.000000Z\n");
        // good
        Assert.assertEquals(2, TestMatchFunctionFactory.getOpenCount());
        Assert.assertEquals(6, TestMatchFunctionFactory.getTopCount());
        Assert.assertEquals(1, TestMatchFunctionFactory.getCloseCount());
    }

    @Test
    public void testLatestByMissingKeyValues() throws Exception {
        // no index
        assertQuery("a\tb\tk\n" +
                        "12.026122412833\tHYRX\t1970-01-11T10:00:00.000000Z\n",
                "select * from x latest by b where b in ('XYZ','HYRX')",
                "create table x as " +
                        "(" +
                        "select" +
                        " rnd_double(0)*100 a," +
                        " rnd_symbol(5,4,4,1) b," +
                        " timestamp_sequence(to_timestamp(0), 100000000000) k" +
                        " from" +
                        " long_sequence(20)" +
                        ") timestamp(k) partition by DAY",
                "k",
                "insert into x select * from (" +
                        "select" +
                        " rnd_double(0)*100," +
                        " 'XYZ'," +
                        " to_timestamp('1971', 'yyyy') t" +
                        " from long_sequence(1)" +
                        ") timestamp(t)",
                "a\tb\tk\n" +
                        "12.026122412833\tHYRX\t1970-01-11T10:00:00.000000Z\n" +
                        "56.594291398612\tXYZ\t1971-01-01T00:00:00.000000Z\n");
    }

    @Test
    public void testLatestByMissingKeyValuesFiltered() throws Exception {
        TestMatchFunctionFactory.clear();
        assertQuery("a\tb\tk\n" +
                        "97.711031460512\tHYRX\t1970-01-07T22:40:00.000000Z\n",
                "select * from x latest by b where b in ('XYZ', 'HYRX') and a > 30 and test_match()",
                "create table x as " +
                        "(" +
                        "select" +
                        " rnd_double(0)*100 a," +
                        " rnd_symbol(5,4,4,1) b," +
                        " timestamp_sequence(to_timestamp(0), 100000000000) k" +
                        " from" +
                        " long_sequence(20)" +
                        ") timestamp(k) partition by DAY",
                "k",
                "insert into x select * from (" +
                        "select" +
                        " rnd_double(0)*100," +
                        " 'XYZ'," +
                        " to_timestamp('1971', 'yyyy') t" +
                        " from long_sequence(1)" +
                        ") timestamp(t)",
                "a\tb\tk\n" +
                        "97.711031460512\tHYRX\t1970-01-07T22:40:00.000000Z\n" +
                        "56.594291398612\tXYZ\t1971-01-01T00:00:00.000000Z\n");

        // good
        Assert.assertEquals(6, TestMatchFunctionFactory.getOpenCount());
        Assert.assertEquals(12, TestMatchFunctionFactory.getTopCount());
        Assert.assertEquals(1, TestMatchFunctionFactory.getCloseCount());
    }

    @Test
    public void testLatestByMissingKeyValuesIndexed() throws Exception {
        assertQuery("a\tb\tk\n" +
                        "12.026122412833\tHYRX\t1970-01-11T10:00:00.000000Z\n",
                "select * from x latest by b where b in ('XYZ', 'HYRX')",
                "create table x as " +
                        "(" +
                        "select" +
                        " rnd_double(0)*100 a," +
                        " rnd_symbol(5,4,4,1) b," +
                        " timestamp_sequence(to_timestamp(0), 100000000000) k" +
                        " from" +
                        " long_sequence(20)" +
                        "), index(b) timestamp(k) partition by DAY",
                "k",
                "insert into x select * from (" +
                        "select" +
                        " rnd_double(0)*100," +
                        " 'XYZ'," +
                        " to_timestamp('1971', 'yyyy') t" +
                        " from long_sequence(1)" +
                        ") timestamp(t)",
                "a\tb\tk\n" +
                        "12.026122412833\tHYRX\t1970-01-11T10:00:00.000000Z\n" +
                        "56.594291398612\tXYZ\t1971-01-01T00:00:00.000000Z\n");
    }

    @Test
    public void testLatestByMissingKeyValuesIndexedFiltered() throws Exception {
        TestMatchFunctionFactory.clear();
        assertQuery("a\tb\tk\n" +
                        "54.551753247857\tHYRX\t1970-02-02T07:00:00.000000Z\n",
                "select * from x latest by b where b in ('XYZ', 'HYRX') and a > 30 and test_match()",
                "create table x as " +
                        "(" +
                        "select" +
                        " rnd_double(0)*100 a," +
                        " rnd_symbol(5,4,4,1) b," +
                        " timestamp_sequence(to_timestamp(0), 10000000000) k" +
                        " from" +
                        " long_sequence(300)" +
                        "), index(b) timestamp(k) partition by DAY",
                "k",
                "insert into x select * from (" +
                        "select" +
                        " 88.1," +
                        " 'XYZ'," +
                        " to_timestamp('1971', 'yyyy') t" +
                        " from long_sequence(1)" +
                        ") timestamp(t)",
                "a\tb\tk\n" +
                        "54.551753247857\tHYRX\t1970-02-02T07:00:00.000000Z\n" +
                        "88.100000000000\tXYZ\t1971-01-01T00:00:00.000000Z\n");

        // good
        Assert.assertEquals(6, TestMatchFunctionFactory.getOpenCount());
        Assert.assertEquals(12, TestMatchFunctionFactory.getTopCount());
        Assert.assertEquals(1, TestMatchFunctionFactory.getCloseCount());
    }

    @Test
    public void testLatestByNonExistingColumn() throws Exception {
        assertFailure(
                "select * from x latest by y",
                "create table x as " +
                        "(" +
                        "select" +
                        " rnd_double(0)*100 a," +
                        " rnd_symbol(5,4,4,1) b," +
                        " timestamp_sequence(to_timestamp(0), 100000000000) k" +
                        " from" +
                        " long_sequence(20)" +
                        "), index(b) timestamp(k) partition by DAY",
                26,
                "Invalid column");
    }

    @Test
    public void testLatestBySubQuery() throws Exception {
        // no index
        assertQuery("a\tb\tk\n" +
                        "23.905290108465\tRXGZ\t1970-01-03T07:33:20.000000Z\n" +
                        "12.026122412833\tHYRX\t1970-01-11T10:00:00.000000Z\n" +
                        "40.455469747939\t\t1970-01-22T23:46:40.000000Z\n",
                "select * from x latest by b where b in (select rnd_symbol('RXGZ', 'HYRX', null) a from long_sequence(10))",
                "create table x as " +
                        "(" +
                        "select" +
                        " rnd_double(0)*100 a," +
                        " rnd_symbol(5,4,4,1) b," +
                        " timestamp_sequence(to_timestamp(0), 100000000000) k" +
                        " from" +
                        " long_sequence(20)" +
                        ") timestamp(k) partition by DAY",
                "k",
                "insert into x select * from (" +
                        "select" +
                        " rnd_double(0)*100," +
                        " 'RXGZ'," +
                        " to_timestamp('1971', 'yyyy') t" +
                        " from long_sequence(1)" +
                        ") timestamp(t)",
                "a\tb\tk\n" +
                        "12.026122412833\tHYRX\t1970-01-11T10:00:00.000000Z\n" +
                        "40.455469747939\t\t1970-01-22T23:46:40.000000Z\n" +
                        "95.400690890497\tRXGZ\t1971-01-01T00:00:00.000000Z\n");
    }

    @Test
    public void testLatestBySubQueryDeferred() throws Exception {
        // no index
        assertQuery("a\tb\tk\n" +
                        "23.905290108465\tRXGZ\t1970-01-03T07:33:20.000000Z\n" +
                        "12.026122412833\tHYRX\t1970-01-11T10:00:00.000000Z\n" +
                        "40.455469747939\t\t1970-01-22T23:46:40.000000Z\n",
                "select * from x latest by b where b in (select rnd_symbol('RXGZ', 'HYRX', null, 'UCLA') a from long_sequence(10))",
                "create table x as " +
                        "(" +
                        "select" +
                        " rnd_double(0)*100 a," +
                        " rnd_symbol(5,4,4,1) b," +
                        " timestamp_sequence(to_timestamp(0), 100000000000) k" +
                        " from" +
                        " long_sequence(20)" +
                        ") timestamp(k) partition by DAY",
                "k",
                "insert into x select * from (" +
                        "select" +
                        " rnd_double(0)*100," +
                        " 'UCLA'," +
                        " to_timestamp('1971', 'yyyy') t" +
                        " from long_sequence(1)" +
                        ") timestamp(t)",
                "a\tb\tk\n" +
                        "23.905290108465\tRXGZ\t1970-01-03T07:33:20.000000Z\n" +
                        "12.026122412833\tHYRX\t1970-01-11T10:00:00.000000Z\n" +
                        "40.455469747939\t\t1970-01-22T23:46:40.000000Z\n" +
                        "95.400690890497\tUCLA\t1971-01-01T00:00:00.000000Z\n");
    }

    @Test
    public void testLatestBySubQueryDeferredFiltered() throws Exception {
        TestMatchFunctionFactory.clear();
        // no index
        assertQuery("a\tb\tk\n" +
                        "23.905290108465\tRXGZ\t1970-01-03T07:33:20.000000Z\n" +
                        "12.026122412833\tHYRX\t1970-01-11T10:00:00.000000Z\n" +
                        "40.455469747939\t\t1970-01-22T23:46:40.000000Z\n",
                "select * from x latest by b where b in (select rnd_symbol('RXGZ', 'HYRX', null, 'UCLA') a from long_sequence(10))" +
                        " and a > 12 and a < 50 and test_match()",
                "create table x as " +
                        "(" +
                        "select" +
                        " rnd_double(0)*100 a," +
                        " rnd_symbol(5,4,4,1) b," +
                        " timestamp_sequence(to_timestamp(0), 100000000000) k" +
                        " from" +
                        " long_sequence(20)" +
                        ") timestamp(k) partition by DAY",
                "k",
                "insert into x select * from (" +
                        "select" +
                        " 33.46," +
                        " 'UCLA'," +
                        " to_timestamp('1971', 'yyyy') t" +
                        " from long_sequence(1)" +
                        ") timestamp(t)",
                "a\tb\tk\n" +
                        "23.905290108465\tRXGZ\t1970-01-03T07:33:20.000000Z\n" +
                        "12.026122412833\tHYRX\t1970-01-11T10:00:00.000000Z\n" +
                        "40.455469747939\t\t1970-01-22T23:46:40.000000Z\n" +
                        "33.460000000000\tUCLA\t1971-01-01T00:00:00.000000Z\n");

        // good
        Assert.assertEquals(6, TestMatchFunctionFactory.getOpenCount());
        Assert.assertEquals(12, TestMatchFunctionFactory.getTopCount());
        Assert.assertEquals(1, TestMatchFunctionFactory.getCloseCount());
    }

    @Test
    public void testLatestBySubQueryDeferredIndexed() throws Exception {
        assertQuery("a\tb\tk\n" +
                        "23.905290108465\tRXGZ\t1970-01-03T07:33:20.000000Z\n" +
                        "12.026122412833\tHYRX\t1970-01-11T10:00:00.000000Z\n" +
                        "40.455469747939\t\t1970-01-22T23:46:40.000000Z\n",
                "select * from x latest by b where b in (select rnd_symbol('RXGZ', 'HYRX', null, 'UCLA') a from long_sequence(10))",
                "create table x as " +
                        "(" +
                        "select" +
                        " rnd_double(0)*100 a," +
                        " rnd_symbol(5,4,4,1) b," +
                        " timestamp_sequence(to_timestamp(0), 100000000000) k" +
                        " from" +
                        " long_sequence(20)" +
                        "), index(b) timestamp(k) partition by DAY",
                "k",
                "insert into x select * from (" +
                        "select" +
                        " rnd_double(0)*100," +
                        " 'UCLA'," +
                        " to_timestamp('1971', 'yyyy') t" +
                        " from long_sequence(1)" +
                        ") timestamp(t)",
                "a\tb\tk\n" +
                        "23.905290108465\tRXGZ\t1970-01-03T07:33:20.000000Z\n" +
                        "12.026122412833\tHYRX\t1970-01-11T10:00:00.000000Z\n" +
                        "40.455469747939\t\t1970-01-22T23:46:40.000000Z\n" +
                        "95.400690890497\tUCLA\t1971-01-01T00:00:00.000000Z\n");
    }

    @Test
    public void testLatestBySubQueryDeferredIndexedFiltered() throws Exception {
        TestMatchFunctionFactory.clear();
        assertQuery("a\tb\tk\n" +
                        "23.905290108465\tRXGZ\t1970-01-03T07:33:20.000000Z\n" +
                        "12.026122412833\tHYRX\t1970-01-11T10:00:00.000000Z\n" +
                        "40.455469747939\t\t1970-01-22T23:46:40.000000Z\n",
                "select * from x latest by b where b in (select rnd_symbol('RXGZ', 'HYRX', null, 'UCLA') a from long_sequence(10))" +
                        " and a > 12 and a < 50 and test_match()",
                "create table x as " +
                        "(" +
                        "select" +
                        " rnd_double(0)*100 a," +
                        " rnd_symbol(5,4,4,1) b," +
                        " timestamp_sequence(to_timestamp(0), 100000000000) k" +
                        " from" +
                        " long_sequence(20)" +
                        "), index(b) timestamp(k) partition by DAY",
                "k",
                "insert into x select * from (" +
                        "select" +
                        " 33.46," +
                        " 'UCLA'," +
                        " to_timestamp('1971', 'yyyy') t" +
                        " from long_sequence(1)" +
                        ") timestamp(t)",
                "a\tb\tk\n" +
                        "23.905290108465\tRXGZ\t1970-01-03T07:33:20.000000Z\n" +
                        "12.026122412833\tHYRX\t1970-01-11T10:00:00.000000Z\n" +
                        "40.455469747939\t\t1970-01-22T23:46:40.000000Z\n" +
                        "33.460000000000\tUCLA\t1971-01-01T00:00:00.000000Z\n");

        Assert.assertEquals(6, TestMatchFunctionFactory.getOpenCount());
        Assert.assertEquals(12, TestMatchFunctionFactory.getTopCount());
        Assert.assertEquals(1, TestMatchFunctionFactory.getCloseCount());
    }

    @Test
    public void testLatestBySubQueryFiltered() throws Exception {
        TestMatchFunctionFactory.clear();
        // no index
        assertQuery("a\tb\tk\n" +
                        "23.905290108465\tRXGZ\t1970-01-03T07:33:20.000000Z\n" +
                        "12.026122412833\tHYRX\t1970-01-11T10:00:00.000000Z\n" +
                        "40.455469747939\t\t1970-01-22T23:46:40.000000Z\n",
                "select * from x latest by b where b in (select rnd_symbol('RXGZ', 'HYRX', null) a from long_sequence(10))" +
                        " and a > 12 and a < 50 and test_match()",
                "create table x as " +
                        "(" +
                        "select" +
                        " rnd_double(0)*100 a," +
                        " rnd_symbol(5,4,4,1) b," +
                        " timestamp_sequence(to_timestamp(0), 100000000000) k" +
                        " from" +
                        " long_sequence(20)" +
                        ") timestamp(k) partition by DAY",
                "k",
                "insert into x select * from (" +
                        "select" +
                        " 33.46," +
                        " 'RXGZ'," +
                        " to_timestamp('1971', 'yyyy') t" +
                        " from long_sequence(1)" +
                        ") timestamp(t)",
                "a\tb\tk\n" +
                        "12.026122412833\tHYRX\t1970-01-11T10:00:00.000000Z\n" +
                        "40.455469747939\t\t1970-01-22T23:46:40.000000Z\n" +
                        "33.460000000000\tRXGZ\t1971-01-01T00:00:00.000000Z\n");

        Assert.assertEquals(6, TestMatchFunctionFactory.getOpenCount());
        Assert.assertEquals(12, TestMatchFunctionFactory.getTopCount());
        Assert.assertEquals(1, TestMatchFunctionFactory.getCloseCount());
    }

    @Test
    public void testLatestBySubQueryIndexed() throws Exception {
        assertQuery("a\tb\tk\n" +
                        "23.905290108465\tRXGZ\t1970-01-03T07:33:20.000000Z\n" +
                        "12.026122412833\tHYRX\t1970-01-11T10:00:00.000000Z\n" +
                        "40.455469747939\t\t1970-01-22T23:46:40.000000Z\n",
                "select * from x latest by b where b in (select rnd_symbol('RXGZ', 'HYRX', null) a from long_sequence(10))",
                "create table x as " +
                        "(" +
                        "select" +
                        " rnd_double(0)*100 a," +
                        " rnd_symbol(5,4,4,1) b," +
                        " timestamp_sequence(to_timestamp(0), 100000000000) k" +
                        " from" +
                        " long_sequence(20)" +
                        "), index(b) timestamp(k) partition by DAY",
                "k",
                "insert into x select * from (" +
                        "select" +
                        " rnd_double(0)*100," +
                        " 'RXGZ'," +
                        " to_timestamp('1971', 'yyyy') t" +
                        " from long_sequence(1)" +
                        ") timestamp(t)",
                "a\tb\tk\n" +
                        "12.026122412833\tHYRX\t1970-01-11T10:00:00.000000Z\n" +
                        "40.455469747939\t\t1970-01-22T23:46:40.000000Z\n" +
                        "95.400690890497\tRXGZ\t1971-01-01T00:00:00.000000Z\n");
    }

    @Test
    public void testLatestBySubQueryIndexedFiltered() throws Exception {
        TestMatchFunctionFactory.clear();
        assertQuery("a\tb\tk\n" +
                        "23.905290108465\tRXGZ\t1970-01-03T07:33:20.000000Z\n" +
                        "12.026122412833\tHYRX\t1970-01-11T10:00:00.000000Z\n" +
                        "40.455469747939\t\t1970-01-22T23:46:40.000000Z\n",
                "select * from x latest by b where b in (select rnd_symbol('RXGZ', 'HYRX', null) a from long_sequence(10))" +
                        " and a > 12 and a < 50 and test_match()",
                "create table x as " +
                        "(" +
                        "select" +
                        " rnd_double(0)*100 a," +
                        " rnd_symbol(5,4,4,1) b," +
                        " timestamp_sequence(to_timestamp(0), 100000000000) k" +
                        " from" +
                        " long_sequence(20)" +
                        "),index(b) timestamp(k) partition by DAY",
                "k",
                "insert into x select * from (" +
                        "select" +
                        " 33.46," +
                        " 'RXGZ'," +
                        " to_timestamp('1971', 'yyyy') t" +
                        " from long_sequence(1)" +
                        ") timestamp(t)",
                "a\tb\tk\n" +
                        "12.026122412833\tHYRX\t1970-01-11T10:00:00.000000Z\n" +
                        "40.455469747939\t\t1970-01-22T23:46:40.000000Z\n" +
                        "33.460000000000\tRXGZ\t1971-01-01T00:00:00.000000Z\n");

        Assert.assertEquals(6, TestMatchFunctionFactory.getOpenCount());
        Assert.assertEquals(12, TestMatchFunctionFactory.getTopCount());
        Assert.assertEquals(1, TestMatchFunctionFactory.getCloseCount());
    }

    @Test
    public void testLatestBySubQueryIndexedIntColumn() throws Exception {
        assertFailure(
                "select * from x latest by b where b in (select 1 a from long_sequence(4))",
                "create table x as " +
                        "(" +
                        "select" +
                        " rnd_double(0)*100 a," +
                        " rnd_symbol(5,4,4,1) b," +
                        " timestamp_sequence(to_timestamp(0), 100000000000) k" +
                        " from" +
                        " long_sequence(20)" +
                        "), index(b) timestamp(k) partition by DAY",
                47,
                "unsupported column type");
    }

    @Test
    public void testLatestBySubQueryIndexedStrColumn() throws Exception {
        assertQuery("a\tb\tk\n" +
                        "23.905290108465\tRXGZ\t1970-01-03T07:33:20.000000Z\n",
                "select * from x latest by b where b in (select 'RXGZ' from long_sequence(4))",
                "create table x as " +
                        "(" +
                        "select" +
                        " rnd_double(0)*100 a," +
                        " rnd_symbol(5,4,4,1) b," +
                        " timestamp_sequence(to_timestamp(0), 100000000000) k" +
                        " from" +
                        " long_sequence(20)" +
                        "), index(b) timestamp(k) partition by DAY",
                "k",
                "insert into x select * from (" +
                        "select" +
                        " rnd_double(0)*100," +
                        " 'RXGZ'," +
                        " to_timestamp('1971', 'yyyy') t" +
                        " from long_sequence(1)" +
                        ") timestamp(t)",
                "a\tb\tk\n" +
                        "56.594291398612\tRXGZ\t1971-01-01T00:00:00.000000Z\n");
    }

    @Test
    public void testLongCursor() throws Exception {
        assertQuery("x\n" +
                        "1\n" +
                        "2\n" +
                        "3\n" +
                        "4\n" +
                        "5\n" +
                        "6\n" +
                        "7\n" +
                        "8\n" +
                        "9\n" +
                        "10\n",
                "select * from long_sequence(10)",
                null,
                null);


        // test another record count

        assertQuery("x\n" +
                        "1\n" +
                        "2\n" +
                        "3\n" +
                        "4\n" +
                        "5\n" +
                        "6\n" +
                        "7\n" +
                        "8\n" +
                        "9\n" +
                        "10\n" +
                        "11\n" +
                        "12\n" +
                        "13\n" +
                        "14\n" +
                        "15\n" +
                        "16\n" +
                        "17\n" +
                        "18\n" +
                        "19\n" +
                        "20\n",
                "select * from long_sequence(20)",
                null,
                null);

        // test 0 record count

        assertQuery("x\n",
                "select * from long_sequence(0)",
                null,
                null);

        assertQuery("x\n",
                "select * from long_sequence(-2)",
                null,
                null);
    }

    @Test
    public void testNamedBindVariableInWhere() throws Exception {
        TestUtils.assertMemoryLeak(() -> {

            final CairoConfiguration configuration = new DefaultCairoConfiguration(root);
            try (
                    CairoEngine engine = new CairoEngine(configuration);
                    SqlCompiler compiler = new SqlCompiler(engine)
            ) {
                bindVariableService.clear();
                bindVariableService.setLong("var", 10);
                try (RecordCursorFactory factory = compiler.compile("select x from long_sequence(100) where x = :var", sqlExecutionContext)) {
                    assertCursor("x\n" +
                                    "10\n",
                            factory,
                            true
                    );
                }
            }
        });
    }

    @Test
    public void testOrderBy() throws Exception {
        final String expected = "a\tb\tk\n" +
                "11.427984775756\t\t1970-01-01T00:00:00.000000Z\n" +
                "26.922103479745\t\t1970-01-13T17:33:20.000000Z\n" +
                "32.881769076795\t\t1970-01-06T18:53:20.000000Z\n" +
                "40.455469747939\t\t1970-01-22T23:46:40.000000Z\n" +
                "45.634456960908\t\t1970-01-21T20:00:00.000000Z\n" +
                "52.984059417621\t\t1970-01-14T21:20:00.000000Z\n" +
                "57.934663268622\t\t1970-01-10T06:13:20.000000Z\n" +
                "80.011211397392\t\t1970-01-19T12:26:40.000000Z\n" +
                "87.996347253916\t\t1970-01-05T15:06:40.000000Z\n" +
                "92.050039469858\t\t1970-01-20T16:13:20.000000Z\n" +
                "97.501988537251\t\t1970-01-17T04:53:20.000000Z\n" +
                "12.026122412833\tHYRX\t1970-01-11T10:00:00.000000Z\n" +
                "97.711031460512\tHYRX\t1970-01-07T22:40:00.000000Z\n" +
                "23.905290108465\tRXGZ\t1970-01-03T07:33:20.000000Z\n";

        assertQuery(expected,
                "select * from x where b in (select rnd_symbol('RXGZ', 'HYRX', null) a from long_sequence(10))" +
                        " order by b,a,x.a",
                "create table x as " +
                        "(" +
                        "select" +
                        " rnd_double(0)*100 a," +
                        " rnd_symbol(5,4,4,1) b," +
                        " timestamp_sequence(to_timestamp(0), 100000000000) k" +
                        " from" +
                        " long_sequence(20)" +
                        ") timestamp(k) partition by DAY",
                null,
                "insert into x select * from (" +
                        "select" +
                        " rnd_double(0)*100," +
                        " 'RXGZ'," +
                        " to_timestamp('1971', 'yyyy') t" +
                        " from long_sequence(1)" +
                        ") timestamp(t)",
                expected +
                        "42.020442539326\tRXGZ\t1971-01-01T00:00:00.000000Z\n");
    }

    @Test
    public void testOrderByAllSupported() throws Exception {
        final String expected = "a\tb\tc\td\te\tf\tg\ti\tj\tk\tl\tm\tn\n" +
                "-2099411412\ttrue\t\tNaN\tNaN\t119\t2015-09-08T05:51:33.432Z\tPEHN\t8196152051414471878\t1970-01-01T05:16:40.000000Z\t17\t00000000 05 2b 73 51 cf c3 7e c0 1d 6c a9 65 81 ad 79 87\tYWXBBZVRLPT\n" +
                "-2088317486\tfalse\tU\t0.744600037109\tNaN\t651\t2015-07-18T10:50:24.009Z\tVTJW\t3446015290144635451\t1970-01-01T01:06:40.000000Z\t8\t00000000 92 fe 69 38 e1 77 9a e7 0c 89 14 58\tUMLGLHMLLEOY\n" +
                "-2077041000\ttrue\tM\t0.734065626073\t0.5026\t345\t2015-02-16T05:23:30.407Z\t\t-8534688874718947140\t1970-01-01T01:40:00.000000Z\t34\t00000000 1c 0b 20 a2 86 89 37 11 2c 14\tUSZMZVQE\n" +
                "-1915752164\tfalse\tI\t0.878611111254\t0.9966\t403\t2015-08-19T00:36:24.375Z\tCPSW\t-8506266080452644687\t1970-01-01T02:30:00.000000Z\t6\t00000000 9a ef 88 cb 4b a1 cf cf 41 7d a6\t\n" +
                "-1508370878\tfalse\t\tNaN\tNaN\t400\t2015-07-23T20:17:04.236Z\tHYRX\t-7146439436217962540\t1970-01-01T04:43:20.000000Z\t27\t00000000 fa 8d ac 3d 98 a0 ad 9a 5d df dc 72 d7 97 cb f6\n" +
                "00000010 2c 23\tVLOMPBETTTKRIV\n" +
                "-1271909747\ttrue\tB\tNaN\t0.1250\t524\t2015-02-23T11:11:04.998Z\t\t-8955092533521658248\t1970-01-01T00:16:40.000000Z\t3\t00000000 de e4 7c d2 35 07 42 fc 31 79\tRSZSRYRFBVTMHG\n" +
                "-1234141625\tfalse\tC\t0.063816578702\t0.7606\t397\t2015-02-14T21:43:16.924Z\tHYRX\t-8888027247206813045\t1970-01-01T01:56:40.000000Z\t10\t00000000 b3 14 33 80 c9 eb a3 67 7a 1a 79 e4 35 e4\tUIZULIGYVFZFK\n" +
                "-1172180184\tfalse\tS\t0.589121648388\t0.2820\t886\t\tPEHN\t1761725072747471430\t1970-01-01T00:50:00.000000Z\t27\t\tIQBZXIOVIKJS\n" +
                "-857795778\ttrue\t\t0.078280206815\t0.2395\t519\t2015-06-12T11:35:40.668Z\tPEHN\t5360746485515325739\t1970-01-01T02:46:40.000000Z\t43\t\tDMIGQZVK\n" +
                "-682294338\ttrue\tG\t0.915304483996\t0.7943\t646\t2015-11-20T14:44:35.439Z\t\t8432832362817764490\t1970-01-01T05:00:00.000000Z\t38\t\tBOSEPGIUQZHEISQH\n" +
                "-42049305\tfalse\tW\t0.469864814071\t0.8912\t264\t2015-04-25T07:53:52.476Z\t\t-5296023984443079410\t1970-01-01T03:20:00.000000Z\t17\t00000000 9f 13 8f bb 2a 4b af 8f 89 df 35 8f\tOQKYHQQ\n" +
                "33027131\tfalse\tS\t0.153698370855\t0.5083\t107\t2015-08-04T00:55:25.323Z\t\t-8966711730402783587\t1970-01-01T03:53:20.000000Z\t48\t00000000 00 6b dd 18 fe 71 76 bc 45 24 cd 13 00 7c fb 01\tGZJYYFLSVIHDWWL\n" +
                "131103569\ttrue\tO\tNaN\tNaN\t658\t2015-12-24T01:28:12.922Z\tVTJW\t-7745861463408011425\t1970-01-01T03:36:40.000000Z\t43\t\tKXEJCTIZKYFLU\n" +
                "161592763\ttrue\tZ\t0.187697081573\t0.1638\t137\t2015-03-12T05:14:11.462Z\t\t7522482991756933150\t1970-01-01T00:33:20.000000Z\t43\t00000000 06 ac 37 c8 cd 82 89 2b 4d 5f f6 46 90 c3 b3 59\n" +
                "00000010 8e e5 61 2f\tQOLYXWC\n" +
                "971963578\ttrue\t\t0.223478278116\t0.7347\t925\t2015-01-03T11:24:48.587Z\tPEHN\t-8851773155849999621\t1970-01-01T04:10:00.000000Z\t40\t00000000 89 a3 83 64 de d6 fd c4 5b c4 e9 19 47\tXHQUTZOD\n" +
                "976011946\ttrue\tU\t0.240014590077\t0.9292\t379\t\tVTJW\t3820631780839257855\t1970-01-01T02:13:20.000000Z\t12\t00000000 8a b3 14 cd 47 0b 0c 39 12 f7 05 10 f4\tGMXUKLGMXSLUQDYO\n" +
                "1150448121\ttrue\tC\t0.600707072504\t0.7398\t663\t2015-08-17T00:23:29.874Z\tVTJW\t8873452284097498126\t1970-01-01T04:26:40.000000Z\t48\t00000000 c5 60 b7 d1 5a 0c e9 db 51 13 4d 59 20 c9 37 a1\n" +
                "00000010 00\t\n" +
                "1194691156\tfalse\tQ\tNaN\t0.2915\t348\t\tHYRX\t9026435187365103026\t1970-01-01T03:03:20.000000Z\t13\t00000000 71 3d 20 e2 37 f2 64 43 84 55 a0 dd 44 11 e2 a3\tIWZNFKPEVMC\n" +
                "1431425139\tfalse\t\t0.307166678100\t0.4275\t181\t2015-07-26T11:59:20.003Z\t\t-8546113611224784332\t1970-01-01T01:23:20.000000Z\t11\t00000000 d8 57 91 88 28 a5 18 93 bd 0b\tJOXPKRGIIHYH\n" +
                "1569490116\tfalse\tZ\tNaN\t0.7611\t428\t2015-05-16T20:27:48.158Z\tVTJW\t-8671107786057422727\t1970-01-01T00:00:00.000000Z\t26\t00000000 68 61 26 af 19 c4 95 94 36 53 49\tFOWLPD\n";

        assertQuery(expected,
                "x order by a,b,c,d,e,f,g,i,j,k,l,n",
                "create table x as " +
                        "(" +
                        "select" +
                        " rnd_int() a," +
                        " rnd_boolean() b," +
                        " rnd_str(1,1,2) c," +
                        " rnd_double(2) d," +
                        " rnd_float(2) e," +
                        " rnd_short(10,1024) f," +
                        " rnd_date(to_date('2015', 'yyyy'), to_date('2016', 'yyyy'), 2) g," +
                        " rnd_symbol(4,4,4,2) i," +
                        " rnd_long() j," +
                        " timestamp_sequence(to_timestamp(0), 1000000000) k," +
                        " rnd_byte(2,50) l," +
                        " rnd_bin(10, 20, 2) m," +
                        " rnd_str(5,16,2) n" +
                        " from" +
                        " long_sequence(20)" +
                        ") partition by NONE",
                null,
                "insert into x(a,d,c,k) select * from (" +
                        "select" +
                        " 1194691157," +
                        " rnd_double(0)*100," +
                        " 'RXGZ'," +
                        " to_timestamp('1971', 'yyyy') t" +
                        " from long_sequence(1)" +
                        ") timestamp(t)",
                "a\tb\tc\td\te\tf\tg\ti\tj\tk\tl\tm\tn\n" +
                        "-2099411412\ttrue\t\tNaN\tNaN\t119\t2015-09-08T05:51:33.432Z\tPEHN\t8196152051414471878\t1970-01-01T05:16:40.000000Z\t17\t00000000 05 2b 73 51 cf c3 7e c0 1d 6c a9 65 81 ad 79 87\tYWXBBZVRLPT\n" +
                        "-2088317486\tfalse\tU\t0.744600037109\tNaN\t651\t2015-07-18T10:50:24.009Z\tVTJW\t3446015290144635451\t1970-01-01T01:06:40.000000Z\t8\t00000000 92 fe 69 38 e1 77 9a e7 0c 89 14 58\tUMLGLHMLLEOY\n" +
                        "-2077041000\ttrue\tM\t0.734065626073\t0.5026\t345\t2015-02-16T05:23:30.407Z\t\t-8534688874718947140\t1970-01-01T01:40:00.000000Z\t34\t00000000 1c 0b 20 a2 86 89 37 11 2c 14\tUSZMZVQE\n" +
                        "-1915752164\tfalse\tI\t0.878611111254\t0.9966\t403\t2015-08-19T00:36:24.375Z\tCPSW\t-8506266080452644687\t1970-01-01T02:30:00.000000Z\t6\t00000000 9a ef 88 cb 4b a1 cf cf 41 7d a6\t\n" +
                        "-1508370878\tfalse\t\tNaN\tNaN\t400\t2015-07-23T20:17:04.236Z\tHYRX\t-7146439436217962540\t1970-01-01T04:43:20.000000Z\t27\t00000000 fa 8d ac 3d 98 a0 ad 9a 5d df dc 72 d7 97 cb f6\n" +
                        "00000010 2c 23\tVLOMPBETTTKRIV\n" +
                        "-1271909747\ttrue\tB\tNaN\t0.1250\t524\t2015-02-23T11:11:04.998Z\t\t-8955092533521658248\t1970-01-01T00:16:40.000000Z\t3\t00000000 de e4 7c d2 35 07 42 fc 31 79\tRSZSRYRFBVTMHG\n" +
                        "-1234141625\tfalse\tC\t0.063816578702\t0.7606\t397\t2015-02-14T21:43:16.924Z\tHYRX\t-8888027247206813045\t1970-01-01T01:56:40.000000Z\t10\t00000000 b3 14 33 80 c9 eb a3 67 7a 1a 79 e4 35 e4\tUIZULIGYVFZFK\n" +
                        "-1172180184\tfalse\tS\t0.589121648388\t0.2820\t886\t\tPEHN\t1761725072747471430\t1970-01-01T00:50:00.000000Z\t27\t\tIQBZXIOVIKJS\n" +
                        "-857795778\ttrue\t\t0.078280206815\t0.2395\t519\t2015-06-12T11:35:40.668Z\tPEHN\t5360746485515325739\t1970-01-01T02:46:40.000000Z\t43\t\tDMIGQZVK\n" +
                        "-682294338\ttrue\tG\t0.915304483996\t0.7943\t646\t2015-11-20T14:44:35.439Z\t\t8432832362817764490\t1970-01-01T05:00:00.000000Z\t38\t\tBOSEPGIUQZHEISQH\n" +
                        "-42049305\tfalse\tW\t0.469864814071\t0.8912\t264\t2015-04-25T07:53:52.476Z\t\t-5296023984443079410\t1970-01-01T03:20:00.000000Z\t17\t00000000 9f 13 8f bb 2a 4b af 8f 89 df 35 8f\tOQKYHQQ\n" +
                        "33027131\tfalse\tS\t0.153698370855\t0.5083\t107\t2015-08-04T00:55:25.323Z\t\t-8966711730402783587\t1970-01-01T03:53:20.000000Z\t48\t00000000 00 6b dd 18 fe 71 76 bc 45 24 cd 13 00 7c fb 01\tGZJYYFLSVIHDWWL\n" +
                        "131103569\ttrue\tO\tNaN\tNaN\t658\t2015-12-24T01:28:12.922Z\tVTJW\t-7745861463408011425\t1970-01-01T03:36:40.000000Z\t43\t\tKXEJCTIZKYFLU\n" +
                        "161592763\ttrue\tZ\t0.187697081573\t0.1638\t137\t2015-03-12T05:14:11.462Z\t\t7522482991756933150\t1970-01-01T00:33:20.000000Z\t43\t00000000 06 ac 37 c8 cd 82 89 2b 4d 5f f6 46 90 c3 b3 59\n" +
                        "00000010 8e e5 61 2f\tQOLYXWC\n" +
                        "971963578\ttrue\t\t0.223478278116\t0.7347\t925\t2015-01-03T11:24:48.587Z\tPEHN\t-8851773155849999621\t1970-01-01T04:10:00.000000Z\t40\t00000000 89 a3 83 64 de d6 fd c4 5b c4 e9 19 47\tXHQUTZOD\n" +
                        "976011946\ttrue\tU\t0.240014590077\t0.9292\t379\t\tVTJW\t3820631780839257855\t1970-01-01T02:13:20.000000Z\t12\t00000000 8a b3 14 cd 47 0b 0c 39 12 f7 05 10 f4\tGMXUKLGMXSLUQDYO\n" +
                        "1150448121\ttrue\tC\t0.600707072504\t0.7398\t663\t2015-08-17T00:23:29.874Z\tVTJW\t8873452284097498126\t1970-01-01T04:26:40.000000Z\t48\t00000000 c5 60 b7 d1 5a 0c e9 db 51 13 4d 59 20 c9 37 a1\n" +
                        "00000010 00\t\n" +
                        "1194691156\tfalse\tQ\tNaN\t0.2915\t348\t\tHYRX\t9026435187365103026\t1970-01-01T03:03:20.000000Z\t13\t00000000 71 3d 20 e2 37 f2 64 43 84 55 a0 dd 44 11 e2 a3\tIWZNFKPEVMC\n" +
                        "1194691157\tfalse\tRXGZ\t88.693976174595\tNaN\t0\t\t\tNaN\t1971-01-01T00:00:00.000000Z\t0\t\t\n" +
                        "1431425139\tfalse\t\t0.307166678100\t0.4275\t181\t2015-07-26T11:59:20.003Z\t\t-8546113611224784332\t1970-01-01T01:23:20.000000Z\t11\t00000000 d8 57 91 88 28 a5 18 93 bd 0b\tJOXPKRGIIHYH\n" +
                        "1569490116\tfalse\tZ\tNaN\t0.7611\t428\t2015-05-16T20:27:48.158Z\tVTJW\t-8671107786057422727\t1970-01-01T00:00:00.000000Z\t26\t00000000 68 61 26 af 19 c4 95 94 36 53 49\tFOWLPD\n");
    }

    @Test
    public void testOrderByFull() throws Exception {
        assertQuery("b\tsum\tk\n" +
                        "\t19.202208853548\t1970-01-03T00:00:00.000000Z\n" +
                        "\t32.540322001542\t1970-01-03T18:00:00.000000Z\n" +
                        "BHF\t87.996347253916\t1970-01-03T06:00:00.000000Z\n" +
                        "CPS\t80.432240999684\t1970-01-03T00:00:00.000000Z\n" +
                        "DEY\t66.938371476317\t1970-01-03T03:00:00.000000Z\n" +
                        "DOT\t45.659895188240\t1970-01-03T15:00:00.000000Z\n" +
                        "DXY\t2.165181900725\t1970-01-03T06:00:00.000000Z\n" +
                        "EDR\t96.874232769402\t1970-01-03T09:00:00.000000Z\n" +
                        "FBV\t77.639046748187\t1970-01-03T12:00:00.000000Z\n" +
                        "JMY\t38.642336707856\t1970-01-03T12:00:00.000000Z\n" +
                        "KGH\t56.594291398612\t1970-01-03T15:00:00.000000Z\n" +
                        "OFJ\t34.356853329430\t1970-01-03T09:00:00.000000Z\n" +
                        "OOZ\t49.005104498852\t1970-01-03T12:00:00.000000Z\n" +
                        "PGW\t55.991618048008\t1970-01-03T03:00:00.000000Z\n" +
                        "RSZ\t41.381647482277\t1970-01-03T09:00:00.000000Z\n" +
                        "UOJ\t63.816075311785\t1970-01-03T06:00:00.000000Z\n" +
                        "UXI\t34.910703637305\t1970-01-03T03:00:00.000000Z\n" +
                        "XPE\t20.447441837878\t1970-01-03T00:00:00.000000Z\n" +
                        "YCT\t57.789479151824\t1970-01-03T18:00:00.000000Z\n" +
                        "ZOU\t65.903416076922\t1970-01-03T15:00:00.000000Z\n",
                // we have 'sample by fill(none)' because it doesn't support
                // random record access, which is what we intend on testing
                "select b, sum(a), k from x sample by 3h fill(none) order by b",
                "create table x as " +
                        "(" +
                        "select" +
                        " rnd_double(0)*100 a," +
                        " rnd_str(3,3,2) b," +
                        " timestamp_sequence(to_timestamp(172800000000), 3600000000) k" +
                        " from" +
                        " long_sequence(20)" +
                        ") timestamp(k) partition by NONE",
                null,
                "insert into x select * from (" +
                        "select" +
                        " rnd_double(0)*100 a," +
                        " rnd_str(3,3,2) b," +
                        " timestamp_sequence(to_timestamp(277200000000), 3600000000) k" +
                        " from" +
                        " long_sequence(5)" +
                        ") timestamp(k)",
                "b\tsum\tk\n" +
                        "\t19.202208853548\t1970-01-03T00:00:00.000000Z\n" +
                        "\t32.540322001542\t1970-01-03T18:00:00.000000Z\n" +
                        "\t3.831785863681\t1970-01-04T03:00:00.000000Z\n" +
                        "\t71.339102715558\t1970-01-04T06:00:00.000000Z\n" +
                        "BHF\t87.996347253916\t1970-01-03T06:00:00.000000Z\n" +
                        "CPS\t80.432240999684\t1970-01-03T00:00:00.000000Z\n" +
                        "DEY\t66.938371476317\t1970-01-03T03:00:00.000000Z\n" +
                        "DOT\t45.659895188240\t1970-01-03T15:00:00.000000Z\n" +
                        "DXY\t2.165181900725\t1970-01-03T06:00:00.000000Z\n" +
                        "EDR\t96.874232769402\t1970-01-03T09:00:00.000000Z\n" +
                        "FBV\t77.639046748187\t1970-01-03T12:00:00.000000Z\n" +
                        "JMY\t38.642336707856\t1970-01-03T12:00:00.000000Z\n" +
                        "KGH\t56.594291398612\t1970-01-03T15:00:00.000000Z\n" +
                        "NVT\t95.400690890497\t1970-01-04T06:00:00.000000Z\n" +
                        "OFJ\t34.356853329430\t1970-01-03T09:00:00.000000Z\n" +
                        "OOZ\t49.005104498852\t1970-01-03T12:00:00.000000Z\n" +
                        "PGW\t55.991618048008\t1970-01-03T03:00:00.000000Z\n" +
                        "RSZ\t41.381647482277\t1970-01-03T09:00:00.000000Z\n" +
                        "UOJ\t63.816075311785\t1970-01-03T06:00:00.000000Z\n" +
                        "UXI\t34.910703637305\t1970-01-03T03:00:00.000000Z\n" +
                        "WUG\t58.912164838798\t1970-01-04T06:00:00.000000Z\n" +
                        "XIO\t14.830552335849\t1970-01-04T09:00:00.000000Z\n" +
                        "XPE\t20.447441837878\t1970-01-03T00:00:00.000000Z\n" +
                        "YCT\t57.789479151824\t1970-01-03T18:00:00.000000Z\n" +
                        "ZOU\t65.903416076922\t1970-01-03T15:00:00.000000Z\n",
                true);
    }

    @Test
    public void testOrderByFullSymbol() throws Exception {
        assertQuery("b\tsum\tk\n" +
                        "\t144.984487170905\t1970-01-03T00:00:00.000000Z\n" +
                        "\t87.996347253916\t1970-01-03T03:00:00.000000Z\n" +
                        "\t146.379436136862\t1970-01-03T06:00:00.000000Z\n" +
                        "\t52.984059417621\t1970-01-03T12:00:00.000000Z\n" +
                        "\t177.513199934642\t1970-01-03T15:00:00.000000Z\n" +
                        "CPSW\t78.830658300550\t1970-01-03T12:00:00.000000Z\n" +
                        "HYRX\t186.000108135441\t1970-01-03T09:00:00.000000Z\n" +
                        "HYRX\t84.452581772111\t1970-01-03T12:00:00.000000Z\n" +
                        "HYRX\t157.953455546780\t1970-01-03T18:00:00.000000Z\n" +
                        "PEHN\t11.427984775756\t1970-01-03T00:00:00.000000Z\n" +
                        "PEHN\t94.848894980177\t1970-01-03T03:00:00.000000Z\n" +
                        "PEHN\t49.005104498852\t1970-01-03T15:00:00.000000Z\n" +
                        "VTJW\t40.228106267796\t1970-01-03T09:00:00.000000Z\n",
                // we have 'sample by fill(none)' because it doesn't support
                // random record access, which is what we intend on testing
                "select b, sum(a), k from x sample by 3h fill(none) order by b",
                "create table x as " +
                        "(" +
                        "select" +
                        " rnd_double(0)*100 a," +
                        " rnd_symbol(4,4,4,2) b," +
                        " timestamp_sequence(to_timestamp(172800000000), 3600000000) k" +
                        " from" +
                        " long_sequence(20)" +
                        ") timestamp(k) partition by NONE",
                null,
                "insert into x select * from (" +
                        "select" +
                        " rnd_double(0)*100 a," +
                        " rnd_symbol(4,4,4,2) b," +
                        " timestamp_sequence(to_timestamp(277200000000), 3600000000) k" +
                        " from" +
                        " long_sequence(5)" +
                        ") timestamp(k)",
                "b\tsum\tk\n" +
                        "\t144.984487170905\t1970-01-03T00:00:00.000000Z\n" +
                        "\t87.996347253916\t1970-01-03T03:00:00.000000Z\n" +
                        "\t146.379436136862\t1970-01-03T06:00:00.000000Z\n" +
                        "\t52.984059417621\t1970-01-03T12:00:00.000000Z\n" +
                        "\t177.513199934642\t1970-01-03T15:00:00.000000Z\n" +
                        "\t57.789479151824\t1970-01-04T03:00:00.000000Z\n" +
                        "\t49.428905119585\t1970-01-04T06:00:00.000000Z\n" +
                        "CPSW\t78.830658300550\t1970-01-03T12:00:00.000000Z\n" +
                        "HYRX\t186.000108135441\t1970-01-03T09:00:00.000000Z\n" +
                        "HYRX\t84.452581772111\t1970-01-03T12:00:00.000000Z\n" +
                        "HYRX\t157.953455546780\t1970-01-03T18:00:00.000000Z\n" +
                        "OUIC\t86.851543054196\t1970-01-04T06:00:00.000000Z\n" +
                        "PEHN\t11.427984775756\t1970-01-03T00:00:00.000000Z\n" +
                        "PEHN\t94.848894980177\t1970-01-03T03:00:00.000000Z\n" +
                        "PEHN\t49.005104498852\t1970-01-03T15:00:00.000000Z\n" +
                        "SDOT\t12.024160875735\t1970-01-04T06:00:00.000000Z\n" +
                        "SDOT\t65.513358397963\t1970-01-04T09:00:00.000000Z\n" +
                        "VTJW\t40.228106267796\t1970-01-03T09:00:00.000000Z\n",
                true);
    }

    @Test
    public void testOrderByFullTimestampLead() throws Exception {
        assertQuery("b\tsum\tk\n" +
                        "\t19.202208853548\t1970-01-03T00:00:00.000000Z\n" +
                        "CPS\t80.432240999684\t1970-01-03T00:00:00.000000Z\n" +
                        "XPE\t20.447441837878\t1970-01-03T00:00:00.000000Z\n" +
                        "DEY\t66.938371476317\t1970-01-03T03:00:00.000000Z\n" +
                        "PGW\t55.991618048008\t1970-01-03T03:00:00.000000Z\n" +
                        "UXI\t34.910703637305\t1970-01-03T03:00:00.000000Z\n" +
                        "BHF\t87.996347253916\t1970-01-03T06:00:00.000000Z\n" +
                        "DXY\t2.165181900725\t1970-01-03T06:00:00.000000Z\n" +
                        "UOJ\t63.816075311785\t1970-01-03T06:00:00.000000Z\n" +
                        "EDR\t96.874232769402\t1970-01-03T09:00:00.000000Z\n" +
                        "OFJ\t34.356853329430\t1970-01-03T09:00:00.000000Z\n" +
                        "RSZ\t41.381647482277\t1970-01-03T09:00:00.000000Z\n" +
                        "FBV\t77.639046748187\t1970-01-03T12:00:00.000000Z\n" +
                        "JMY\t38.642336707856\t1970-01-03T12:00:00.000000Z\n" +
                        "OOZ\t49.005104498852\t1970-01-03T12:00:00.000000Z\n" +
                        "DOT\t45.659895188240\t1970-01-03T15:00:00.000000Z\n" +
                        "KGH\t56.594291398612\t1970-01-03T15:00:00.000000Z\n" +
                        "ZOU\t65.903416076922\t1970-01-03T15:00:00.000000Z\n" +
                        "\t32.540322001542\t1970-01-03T18:00:00.000000Z\n" +
                        "YCT\t57.789479151824\t1970-01-03T18:00:00.000000Z\n",
                // we have 'sample by fill(none)' because it doesn't support
                // random record access, which is what we intend on testing
                "select b, sum(a), k from x sample by 3h fill(none) order by k,b",
                "create table x as " +
                        "(" +
                        "select" +
                        " rnd_double(0)*100 a," +
                        " rnd_str(3,3,2) b," +
                        " timestamp_sequence(to_timestamp(172800000000), 3600000000) k" +
                        " from" +
                        " long_sequence(20)" +
                        ") timestamp(k) partition by NONE",
                "k",
                "insert into x select * from (" +
                        "select" +
                        " rnd_double(0)*100 a," +
                        " rnd_str(3,3,2) b," +
                        " timestamp_sequence(to_timestamp(277200000000), 3600000000) k" +
                        " from" +
                        " long_sequence(5)" +
                        ") timestamp(k)",
                "b\tsum\tk\n" +
                        "\t19.202208853548\t1970-01-03T00:00:00.000000Z\n" +
                        "CPS\t80.432240999684\t1970-01-03T00:00:00.000000Z\n" +
                        "XPE\t20.447441837878\t1970-01-03T00:00:00.000000Z\n" +
                        "DEY\t66.938371476317\t1970-01-03T03:00:00.000000Z\n" +
                        "PGW\t55.991618048008\t1970-01-03T03:00:00.000000Z\n" +
                        "UXI\t34.910703637305\t1970-01-03T03:00:00.000000Z\n" +
                        "BHF\t87.996347253916\t1970-01-03T06:00:00.000000Z\n" +
                        "DXY\t2.165181900725\t1970-01-03T06:00:00.000000Z\n" +
                        "UOJ\t63.816075311785\t1970-01-03T06:00:00.000000Z\n" +
                        "EDR\t96.874232769402\t1970-01-03T09:00:00.000000Z\n" +
                        "OFJ\t34.356853329430\t1970-01-03T09:00:00.000000Z\n" +
                        "RSZ\t41.381647482277\t1970-01-03T09:00:00.000000Z\n" +
                        "FBV\t77.639046748187\t1970-01-03T12:00:00.000000Z\n" +
                        "JMY\t38.642336707856\t1970-01-03T12:00:00.000000Z\n" +
                        "OOZ\t49.005104498852\t1970-01-03T12:00:00.000000Z\n" +
                        "DOT\t45.659895188240\t1970-01-03T15:00:00.000000Z\n" +
                        "KGH\t56.594291398612\t1970-01-03T15:00:00.000000Z\n" +
                        "ZOU\t65.903416076922\t1970-01-03T15:00:00.000000Z\n" +
                        "\t32.540322001542\t1970-01-03T18:00:00.000000Z\n" +
                        "YCT\t57.789479151824\t1970-01-03T18:00:00.000000Z\n" +
                        "\t3.831785863681\t1970-01-04T03:00:00.000000Z\n" +
                        "\t71.339102715558\t1970-01-04T06:00:00.000000Z\n" +
                        "NVT\t95.400690890497\t1970-01-04T06:00:00.000000Z\n" +
                        "WUG\t58.912164838798\t1970-01-04T06:00:00.000000Z\n" +
                        "XIO\t14.830552335849\t1970-01-04T09:00:00.000000Z\n",
                true);
    }

    @Test
    public void testOrderChar() throws Exception {
        assertQuery("a\n" +
                        "C\n" +
                        "E\n" +
                        "G\n" +
                        "H\n" +
                        "H\n" +
                        "J\n" +
                        "N\n" +
                        "P\n" +
                        "P\n" +
                        "R\n" +
                        "R\n" +
                        "S\n" +
                        "T\n" +
                        "V\n" +
                        "W\n" +
                        "W\n" +
                        "X\n" +
                        "X\n" +
                        "Y\n" +
                        "Z\n",
                "select * from x order by a",
                "create table x as " +
                        "(" +
                        "select" +
                        " rnd_char() a" +
                        " from" +
                        " long_sequence(20)" +
                        ") partition by NONE",
                null,
                "insert into x select * from (" +
                        "select" +
                        " rnd_char()" +
                        " from" +
                        " long_sequence(5)" +
                        ")",
                "a\n" +
                        "C\n" +
                        "E\n" +
                        "G\n" +
                        "H\n" +
                        "H\n" +
                        "I\n" +
                        "J\n" +
                        "N\n" +
                        "P\n" +
                        "P\n" +
                        "R\n" +
                        "R\n" +
                        "S\n" +
                        "S\n" +
                        "T\n" +
                        "U\n" +
                        "V\n" +
                        "W\n" +
                        "W\n" +
                        "X\n" +
                        "X\n" +
                        "X\n" +
                        "X\n" +
                        "Y\n" +
                        "Z\n",
                true);
    }

    @Test
    public void testOrderByNonUnique() throws Exception {
        final String expected = "a\tc\tk\tn\n" +
                "-1182156192\t\t1970-01-01T04:43:20.000000Z\tGLUOHNZHZS\n" +
                "-1470806499\t\t1970-01-01T03:53:20.000000Z\t\n" +
                "-1966408995\t\t1970-01-01T02:30:00.000000Z\tBZXIOVIKJSMSS\n" +
                "-2105201404\tB\t1970-01-01T04:10:00.000000Z\tGHWVDKFL\n" +
                "1431775887\tC\t1970-01-01T05:16:40.000000Z\tEHNOMVELLKKHT\n" +
                "852921272\tC\t1970-01-01T02:13:20.000000Z\tLSUWDSWUGSHOLN\n" +
                "-147343840\tD\t1970-01-01T05:00:00.000000Z\tOGIFOUSZMZVQEB\n" +
                "1907911110\tE\t1970-01-01T03:36:40.000000Z\tPHRIPZIMNZ\n" +
                "-1715058769\tE\t1970-01-01T00:33:20.000000Z\tQEHBHFOWL\n" +
                "-514934130\tH\t1970-01-01T03:20:00.000000Z\t\n" +
                "116799613\tI\t1970-01-01T03:03:20.000000Z\tZEPIHVLTOVLJUML\n" +
                "-1204245663\tJ\t1970-01-01T04:26:40.000000Z\tPKRGIIHYHBOQMY\n" +
                "-1148479920\tJ\t1970-01-01T00:00:00.000000Z\tPSWHYRXPEH\n" +
                "410717394\tO\t1970-01-01T01:06:40.000000Z\tGETJR\n" +
                "1743740444\tS\t1970-01-01T02:46:40.000000Z\tTKVVSJ\n" +
                "326010667\tS\t1970-01-01T01:23:20.000000Z\tRFBVTMHGOOZZVDZ\n" +
                "1876812930\tV\t1970-01-01T01:56:40.000000Z\tSDOTSEDYYCTGQOLY\n" +
                "-938514914\tX\t1970-01-01T00:50:00.000000Z\tBEOUOJSHRUEDRQQ\n" +
                "1545253512\tX\t1970-01-01T00:16:40.000000Z\tSXUXIBBTGPGWFFY\n" +
                "-235358133\tY\t1970-01-01T01:40:00.000000Z\tCXZOUICWEK\n";

        assertQuery(expected,
                "x order by c",
                "create table x as " +
                        "(" +
                        "select" +
                        " rnd_int() a," +
                        " rnd_str(1,1,2) c," +
                        " timestamp_sequence(to_timestamp(0), 1000000000) k," +
                        " rnd_str(5,16,2) n" +
                        " from" +
                        " long_sequence(20)" +
                        ") partition by NONE",
                null,
                "insert into x select * from (" +
                        "select" +
                        " rnd_int()," +
                        " 'J'," +
                        " to_timestamp('1971', 'yyyy') t," +
                        " 'APPC'" +
                        " from long_sequence(1)" +
                        ") timestamp(t)",
                "a\tc\tk\tn\n" +
                        "-1182156192\t\t1970-01-01T04:43:20.000000Z\tGLUOHNZHZS\n" +
                        "-1470806499\t\t1970-01-01T03:53:20.000000Z\t\n" +
                        "-1966408995\t\t1970-01-01T02:30:00.000000Z\tBZXIOVIKJSMSS\n" +
                        "-2105201404\tB\t1970-01-01T04:10:00.000000Z\tGHWVDKFL\n" +
                        "1431775887\tC\t1970-01-01T05:16:40.000000Z\tEHNOMVELLKKHT\n" +
                        "852921272\tC\t1970-01-01T02:13:20.000000Z\tLSUWDSWUGSHOLN\n" +
                        "-147343840\tD\t1970-01-01T05:00:00.000000Z\tOGIFOUSZMZVQEB\n" +
                        "1907911110\tE\t1970-01-01T03:36:40.000000Z\tPHRIPZIMNZ\n" +
                        "-1715058769\tE\t1970-01-01T00:33:20.000000Z\tQEHBHFOWL\n" +
                        "-514934130\tH\t1970-01-01T03:20:00.000000Z\t\n" +
                        "116799613\tI\t1970-01-01T03:03:20.000000Z\tZEPIHVLTOVLJUML\n" +
                        "1570930196\tJ\t1971-01-01T00:00:00.000000Z\tAPPC\n" +
                        "-1204245663\tJ\t1970-01-01T04:26:40.000000Z\tPKRGIIHYHBOQMY\n" +
                        "-1148479920\tJ\t1970-01-01T00:00:00.000000Z\tPSWHYRXPEH\n" +
                        "410717394\tO\t1970-01-01T01:06:40.000000Z\tGETJR\n" +
                        "1743740444\tS\t1970-01-01T02:46:40.000000Z\tTKVVSJ\n" +
                        "326010667\tS\t1970-01-01T01:23:20.000000Z\tRFBVTMHGOOZZVDZ\n" +
                        "1876812930\tV\t1970-01-01T01:56:40.000000Z\tSDOTSEDYYCTGQOLY\n" +
                        "-938514914\tX\t1970-01-01T00:50:00.000000Z\tBEOUOJSHRUEDRQQ\n" +
                        "1545253512\tX\t1970-01-01T00:16:40.000000Z\tSXUXIBBTGPGWFFY\n" +
                        "-235358133\tY\t1970-01-01T01:40:00.000000Z\tCXZOUICWEK\n");
    }

    @Test
    public void testOrderByTimestamp() throws Exception {
        final String expected = "a\tb\tk\n" +
                "11.427984775756\t\t1970-01-01T00:00:00.000000Z\n" +
                "23.905290108465\tRXGZ\t1970-01-03T07:33:20.000000Z\n" +
                "87.996347253916\t\t1970-01-05T15:06:40.000000Z\n" +
                "32.881769076795\t\t1970-01-06T18:53:20.000000Z\n" +
                "97.711031460512\tHYRX\t1970-01-07T22:40:00.000000Z\n" +
                "57.934663268622\t\t1970-01-10T06:13:20.000000Z\n" +
                "12.026122412833\tHYRX\t1970-01-11T10:00:00.000000Z\n" +
                "26.922103479745\t\t1970-01-13T17:33:20.000000Z\n" +
                "52.984059417621\t\t1970-01-14T21:20:00.000000Z\n" +
                "97.501988537251\t\t1970-01-17T04:53:20.000000Z\n" +
                "80.011211397392\t\t1970-01-19T12:26:40.000000Z\n" +
                "92.050039469858\t\t1970-01-20T16:13:20.000000Z\n" +
                "45.634456960908\t\t1970-01-21T20:00:00.000000Z\n" +
                "40.455469747939\t\t1970-01-22T23:46:40.000000Z\n";

        assertQuery(expected,
                "select * from x where b in (select rnd_symbol('RXGZ', 'HYRX', null) a from long_sequence(10))" +
                        " order by k",
                "create table x as " +
                        "(" +
                        "select" +
                        " rnd_double(0)*100 a," +
                        " rnd_symbol(5,4,4,1) b," +
                        " timestamp_sequence(to_timestamp(0), 100000000000) k" +
                        " from" +
                        " long_sequence(20)" +
                        ") timestamp(k) partition by DAY",
                "k",
                "insert into x select * from (" +
                        "select" +
                        " rnd_double(0)*100," +
                        " 'RXGZ'," +
                        " to_timestamp('1971', 'yyyy') t" +
                        " from long_sequence(1)" +
                        ") timestamp(t)",
                expected +
                        "95.400690890497\tRXGZ\t1971-01-01T00:00:00.000000Z\n");
    }

    @Test
    public void testOrderByTimestampLead() throws Exception {
        final String expected = "a\tc\tk\tn\n" +
                "-2105201404\tB\t1970-01-01T00:00:01.000000Z\tGHWVDKFL\n" +
                "-1966408995\t\t1970-01-01T00:00:01.000000Z\tBZXIOVIKJSMSS\n" +
                "-1715058769\tE\t1970-01-01T00:00:01.000000Z\tQEHBHFOWL\n" +
                "-1470806499\t\t1970-01-01T00:00:01.000000Z\t\n" +
                "-1204245663\tJ\t1970-01-01T00:00:01.000000Z\tPKRGIIHYHBOQMY\n" +
                "-1182156192\t\t1970-01-01T00:00:01.000000Z\tGLUOHNZHZS\n" +
                "-1148479920\tJ\t1970-01-01T00:00:01.000000Z\tPSWHYRXPEH\n" +
                "-938514914\tX\t1970-01-01T00:00:01.000000Z\tBEOUOJSHRUEDRQQ\n" +
                "-514934130\tH\t1970-01-01T00:00:01.000000Z\t\n" +
                "-235358133\tY\t1970-01-01T00:00:01.000000Z\tCXZOUICWEK\n" +
                "-147343840\tD\t1970-01-01T00:00:01.000000Z\tOGIFOUSZMZVQEB\n" +
                "116799613\tI\t1970-01-01T00:00:01.000000Z\tZEPIHVLTOVLJUML\n" +
                "326010667\tS\t1970-01-01T00:00:01.000000Z\tRFBVTMHGOOZZVDZ\n" +
                "410717394\tO\t1970-01-01T00:00:01.000000Z\tGETJR\n" +
                "852921272\tC\t1970-01-01T00:00:01.000000Z\tLSUWDSWUGSHOLN\n" +
                "1431775887\tC\t1970-01-01T00:00:01.000000Z\tEHNOMVELLKKHT\n" +
                "1545253512\tX\t1970-01-01T00:00:01.000000Z\tSXUXIBBTGPGWFFY\n" +
                "1743740444\tS\t1970-01-01T00:00:01.000000Z\tTKVVSJ\n" +
                "1876812930\tV\t1970-01-01T00:00:01.000000Z\tSDOTSEDYYCTGQOLY\n" +
                "1907911110\tE\t1970-01-01T00:00:01.000000Z\tPHRIPZIMNZ\n";

        assertQuery(expected,
                "x order by k,a",
                "create table x as " +
                        "(" +
                        "select" +
                        " rnd_int() a," +
                        " rnd_str(1,1,2) c," +
                        " to_timestamp(1000000) k," +
                        " rnd_str(5,16,2) n" +
                        " from" +
                        " long_sequence(20)" +
                        ") timestamp(k) partition by NONE",
                "k",
                "insert into x select * from (" +
                        "select" +
                        " 852921272," +
                        " 'J'," +
                        " to_timestamp(1000000) t," +
                        " 'APPC'" +
                        " from long_sequence(1)" +
                        ") timestamp(t)",
                "a\tc\tk\tn\n" +
                        "-2105201404\tB\t1970-01-01T00:00:01.000000Z\tGHWVDKFL\n" +
                        "-1966408995\t\t1970-01-01T00:00:01.000000Z\tBZXIOVIKJSMSS\n" +
                        "-1715058769\tE\t1970-01-01T00:00:01.000000Z\tQEHBHFOWL\n" +
                        "-1470806499\t\t1970-01-01T00:00:01.000000Z\t\n" +
                        "-1204245663\tJ\t1970-01-01T00:00:01.000000Z\tPKRGIIHYHBOQMY\n" +
                        "-1182156192\t\t1970-01-01T00:00:01.000000Z\tGLUOHNZHZS\n" +
                        "-1148479920\tJ\t1970-01-01T00:00:01.000000Z\tPSWHYRXPEH\n" +
                        "-938514914\tX\t1970-01-01T00:00:01.000000Z\tBEOUOJSHRUEDRQQ\n" +
                        "-514934130\tH\t1970-01-01T00:00:01.000000Z\t\n" +
                        "-235358133\tY\t1970-01-01T00:00:01.000000Z\tCXZOUICWEK\n" +
                        "-147343840\tD\t1970-01-01T00:00:01.000000Z\tOGIFOUSZMZVQEB\n" +
                        "116799613\tI\t1970-01-01T00:00:01.000000Z\tZEPIHVLTOVLJUML\n" +
                        "326010667\tS\t1970-01-01T00:00:01.000000Z\tRFBVTMHGOOZZVDZ\n" +
                        "410717394\tO\t1970-01-01T00:00:01.000000Z\tGETJR\n" +
                        "852921272\tJ\t1970-01-01T00:00:01.000000Z\tAPPC\n" +
                        "852921272\tC\t1970-01-01T00:00:01.000000Z\tLSUWDSWUGSHOLN\n" +
                        "1431775887\tC\t1970-01-01T00:00:01.000000Z\tEHNOMVELLKKHT\n" +
                        "1545253512\tX\t1970-01-01T00:00:01.000000Z\tSXUXIBBTGPGWFFY\n" +
                        "1743740444\tS\t1970-01-01T00:00:01.000000Z\tTKVVSJ\n" +
                        "1876812930\tV\t1970-01-01T00:00:01.000000Z\tSDOTSEDYYCTGQOLY\n" +
                        "1907911110\tE\t1970-01-01T00:00:01.000000Z\tPHRIPZIMNZ\n");
    }

    @Test
    public void testOrderByTwoStrings() throws Exception {
        final String expected = "a\tc\tk\tn\n" +
                "-1182156192\t\t1970-01-01T04:43:20.000000Z\tGLUOHNZHZS\n" +
                "-1966408995\t\t1970-01-01T02:30:00.000000Z\tBZXIOVIKJSMSS\n" +
                "-1470806499\t\t1970-01-01T03:53:20.000000Z\t\n" +
                "-2105201404\tB\t1970-01-01T04:10:00.000000Z\tGHWVDKFL\n" +
                "852921272\tC\t1970-01-01T02:13:20.000000Z\tLSUWDSWUGSHOLN\n" +
                "1431775887\tC\t1970-01-01T05:16:40.000000Z\tEHNOMVELLKKHT\n" +
                "-147343840\tD\t1970-01-01T05:00:00.000000Z\tOGIFOUSZMZVQEB\n" +
                "-1715058769\tE\t1970-01-01T00:33:20.000000Z\tQEHBHFOWL\n" +
                "1907911110\tE\t1970-01-01T03:36:40.000000Z\tPHRIPZIMNZ\n" +
                "-514934130\tH\t1970-01-01T03:20:00.000000Z\t\n" +
                "116799613\tI\t1970-01-01T03:03:20.000000Z\tZEPIHVLTOVLJUML\n" +
                "-1148479920\tJ\t1970-01-01T00:00:00.000000Z\tPSWHYRXPEH\n" +
                "-1204245663\tJ\t1970-01-01T04:26:40.000000Z\tPKRGIIHYHBOQMY\n" +
                "410717394\tO\t1970-01-01T01:06:40.000000Z\tGETJR\n" +
                "1743740444\tS\t1970-01-01T02:46:40.000000Z\tTKVVSJ\n" +
                "326010667\tS\t1970-01-01T01:23:20.000000Z\tRFBVTMHGOOZZVDZ\n" +
                "1876812930\tV\t1970-01-01T01:56:40.000000Z\tSDOTSEDYYCTGQOLY\n" +
                "1545253512\tX\t1970-01-01T00:16:40.000000Z\tSXUXIBBTGPGWFFY\n" +
                "-938514914\tX\t1970-01-01T00:50:00.000000Z\tBEOUOJSHRUEDRQQ\n" +
                "-235358133\tY\t1970-01-01T01:40:00.000000Z\tCXZOUICWEK\n";

        assertQuery(expected,
                "x order by c,n desc",
                "create table x as " +
                        "(" +
                        "select" +
                        " rnd_int() a," +
                        " rnd_str(1,1,2) c," +
                        " timestamp_sequence(to_timestamp(0), 1000000000) k," +
                        " rnd_str(5,16,2) n" +
                        " from" +
                        " long_sequence(20)" +
                        ") partition by NONE",
                null,
                "insert into x select * from (" +
                        "select" +
                        " rnd_int()," +
                        " 'J'," +
                        " to_timestamp('1971', 'yyyy') t," +
                        " 'ZZCC'" +
                        " from long_sequence(1)" +
                        ") timestamp(t)",
                "a\tc\tk\tn\n" +
                        "-1182156192\t\t1970-01-01T04:43:20.000000Z\tGLUOHNZHZS\n" +
                        "-1966408995\t\t1970-01-01T02:30:00.000000Z\tBZXIOVIKJSMSS\n" +
                        "-1470806499\t\t1970-01-01T03:53:20.000000Z\t\n" +
                        "-2105201404\tB\t1970-01-01T04:10:00.000000Z\tGHWVDKFL\n" +
                        "852921272\tC\t1970-01-01T02:13:20.000000Z\tLSUWDSWUGSHOLN\n" +
                        "1431775887\tC\t1970-01-01T05:16:40.000000Z\tEHNOMVELLKKHT\n" +
                        "-147343840\tD\t1970-01-01T05:00:00.000000Z\tOGIFOUSZMZVQEB\n" +
                        "-1715058769\tE\t1970-01-01T00:33:20.000000Z\tQEHBHFOWL\n" +
                        "1907911110\tE\t1970-01-01T03:36:40.000000Z\tPHRIPZIMNZ\n" +
                        "-514934130\tH\t1970-01-01T03:20:00.000000Z\t\n" +
                        "116799613\tI\t1970-01-01T03:03:20.000000Z\tZEPIHVLTOVLJUML\n" +
                        "1570930196\tJ\t1971-01-01T00:00:00.000000Z\tZZCC\n" +
                        "-1148479920\tJ\t1970-01-01T00:00:00.000000Z\tPSWHYRXPEH\n" +
                        "-1204245663\tJ\t1970-01-01T04:26:40.000000Z\tPKRGIIHYHBOQMY\n" +
                        "410717394\tO\t1970-01-01T01:06:40.000000Z\tGETJR\n" +
                        "1743740444\tS\t1970-01-01T02:46:40.000000Z\tTKVVSJ\n" +
                        "326010667\tS\t1970-01-01T01:23:20.000000Z\tRFBVTMHGOOZZVDZ\n" +
                        "1876812930\tV\t1970-01-01T01:56:40.000000Z\tSDOTSEDYYCTGQOLY\n" +
                        "1545253512\tX\t1970-01-01T00:16:40.000000Z\tSXUXIBBTGPGWFFY\n" +
                        "-938514914\tX\t1970-01-01T00:50:00.000000Z\tBEOUOJSHRUEDRQQ\n" +
                        "-235358133\tY\t1970-01-01T01:40:00.000000Z\tCXZOUICWEK\n");
    }

    @Test
    public void testOrderByUnsupportedType() throws Exception {
        assertFailure(
                "x order by a,m,n",
                "create table x as " +
                        "(" +
                        "select" +
                        " rnd_int() a," +
                        " rnd_boolean() b," +
                        " rnd_str(1,1,2) c," +
                        " rnd_double(2) d," +
                        " rnd_float(2) e," +
                        " rnd_short(10,1024) f," +
                        " rnd_date(to_date('2015', 'yyyy'), to_date('2016', 'yyyy'), 2) g," +
                        " rnd_symbol(4,4,4,2) i," +
                        " rnd_long() j," +
                        " timestamp_sequence(to_timestamp(0), 1000000000) k," +
                        " rnd_byte(2,50) l," +
                        " rnd_bin(10, 20, 2) m," +
                        " rnd_str(5,16,2) n" +
                        " from" +
                        " long_sequence(20)" +
                        ") partition by NONE",
                13, "unsupported column type: BINARY"
        );

    }

    @Test
    public void testSelectColumns() throws Exception {
        assertQuery("a\ta1\tb\tc\td\te\tf1\tf\tg\th\ti\tj\tj1\tk\tl\tm\n" +
                        "NaN\t1569490116\tfalse\t\tNaN\t0.7611\t-1593\t428\t2015-04-04T16:34:47.226Z\t\t\t185\t7039584373105579285\t1970-01-01T00:00:00.000000Z\t4\t00000000 af 19 c4 95 94 36 53 49 b4 59 7e\n" +
                        "10\t1253890363\tfalse\tXYS\t0.191123461757\t0.5793\t-1379\t881\t\t2015-03-04T23:08:35.722465Z\tHYRX\t188\t-4986232506486815364\t1970-01-01T00:16:40.000000Z\t50\t00000000 42 fc 31 79 5f 8b 81 2b 93 4d 1a 8e 78 b5\n" +
                        "27\t-1819240775\ttrue\tGOO\t0.041428124702\t0.9205\t-9039\t97\t2015-08-25T03:15:07.653Z\t2015-12-06T09:41:30.297134Z\tHYRX\t109\t571924429013198086\t1970-01-01T00:33:20.000000Z\t21\t\n" +
                        "18\t-1201923128\ttrue\tUVS\t0.758817540345\t0.5779\t-4379\t480\t2015-12-16T09:15:02.086Z\t2015-05-31T18:12:45.686366Z\tCPSW\tNaN\t-6161552193869048721\t1970-01-01T00:50:00.000000Z\t27\t00000000 28 c7 84 47 dc d2 85 7f a5 b8 7b 4a 9d 46\n" +
                        "NaN\t865832060\ttrue\t\t0.148305523358\t0.9442\t2508\t95\t\t2015-10-20T09:33:20.502524Z\t\tNaN\t-3289070757475856942\t1970-01-01T01:06:40.000000Z\t40\t00000000 f2 3c ed 39 ac a8 3b a6 dc 3b 7d 2b e3 92 fe 69\n" +
                        "00000010 38 e1\n" +
                        "22\t1100812407\tfalse\tOVL\tNaN\t0.7633\t-17778\t698\t2015-09-13T09:55:17.815Z\t\tCPSW\t182\t-8757007522346766135\t1970-01-01T01:23:20.000000Z\t23\t\n" +
                        "18\t1677463366\tfalse\tMNZ\t0.337470756550\t0.1179\t18904\t533\t2015-05-13T23:13:05.262Z\t2015-05-10T00:20:17.926993Z\t\t175\t6351664568801157821\t1970-01-01T01:40:00.000000Z\t29\t00000000 5d d0 eb 67 44 a7 6a 71 34 e0 b0 e9 98 f7 67 62\n" +
                        "00000010 28 60\n" +
                        "4\t39497392\tfalse\tUOH\t0.029227696943\t0.1718\t14242\t652\t\t2015-05-24T22:09:55.175991Z\tVTJW\t141\t3527911398466283309\t1970-01-01T01:56:40.000000Z\t9\t00000000 d9 6f 04 ab 27 47 8f 23 3f ae 7c 9f 77 04 e9 0c\n" +
                        "00000010 ea 4e ea 8b\n" +
                        "10\t1545963509\tfalse\tNWI\t0.113718418361\t0.0620\t-29980\t356\t2015-09-12T14:33:11.105Z\t2015-08-06T04:51:01.526782Z\t\t168\t6380499796471875623\t1970-01-01T02:13:20.000000Z\t13\t00000000 54 52 d0 29 26 c5 aa da 18 ce 5f b2 8b 5c 54 90\n" +
                        "4\t53462821\tfalse\tGOO\t0.055149337562\t0.1195\t-6087\t115\t2015-08-09T19:28:14.249Z\t2015-09-20T01:50:37.694867Z\tCPSW\t145\t-7212878484370155026\t1970-01-01T02:30:00.000000Z\t46\t\n" +
                        "30\t-2139296159\tfalse\t\t0.185864355816\t0.5638\t21020\t299\t2015-12-30T22:10:50.759Z\t2015-01-19T15:54:44.696040Z\tHYRX\t105\t-3463832009795858033\t1970-01-01T02:46:40.000000Z\t38\t00000000 b8 07 b1 32 57 ff 9a ef 88 cb 4b\n" +
                        "21\t-406528351\tfalse\tNLE\tNaN\tNaN\t21057\t968\t2015-10-17T07:20:26.881Z\t2015-06-02T13:00:45.180827Z\tPEHN\t102\t5360746485515325739\t1970-01-01T03:03:20.000000Z\t43\t\n" +
                        "17\t415709351\tfalse\tGQZ\t0.491990017163\t0.6292\t18605\t581\t2015-03-04T06:48:42.194Z\t2015-08-14T15:51:23.307152Z\tHYRX\t185\t-5611837907908424613\t1970-01-01T03:20:00.000000Z\t19\t00000000 20 e2 37 f2 64 43 84 55 a0 dd 44 11 e2 a3 24 4e\n" +
                        "00000010 44 a8 0d fe\n" +
                        "19\t-1387693529\ttrue\tMCG\t0.848083900630\t0.4699\t24206\t119\t2015-03-01T23:54:10.204Z\t2015-10-01T12:02:08.698373Z\t\t175\t3669882909701240516\t1970-01-01T03:36:40.000000Z\t12\t00000000 8f bb 2a 4b af 8f 89 df 35 8f da fe 33 98 80 85\n" +
                        "00000010 20 53 3b 51\n" +
                        "21\t346891421\tfalse\t\t0.933609514583\t0.6380\t15084\t405\t2015-10-12T05:36:54.066Z\t2015-11-16T05:48:57.958190Z\tPEHN\t196\t-9200716729349404576\t1970-01-01T03:53:20.000000Z\t43\t\n" +
                        "27\t263487884\ttrue\tHZQ\t0.703978540803\t0.8461\t31562\t834\t2015-08-04T00:55:25.323Z\t2015-07-25T18:26:42.499255Z\tHYRX\t128\t8196544381931602027\t1970-01-01T04:10:00.000000Z\t15\t00000000 71 76 bc 45 24 cd 13 00 7c fb 01 19 ca f2\n" +
                        "9\t-1034870849\tfalse\tLSV\t0.650660460171\t0.7020\t-838\t110\t2015-08-17T23:50:39.534Z\t2015-03-17T03:23:26.126568Z\tHYRX\tNaN\t-6929866925584807039\t1970-01-01T04:26:40.000000Z\t4\t00000000 4b fb 2d 16 f3 89 a3 83 64 de\n" +
                        "26\t1848218326\ttrue\tSUW\t0.803404910559\t0.0440\t-3502\t854\t2015-04-04T20:55:02.116Z\t2015-11-23T07:46:10.570856Z\t\t145\t4290477379978201771\t1970-01-01T04:43:20.000000Z\t35\t00000000 6d 54 75 10 b3 4c 0e 8f f1 0c c5 60 b7 d1 5a\n" +
                        "5\t-1496904948\ttrue\tDBZ\t0.286271736488\tNaN\t5698\t764\t2015-02-06T02:49:54.147Z\t\t\tNaN\t-3058745577013275321\t1970-01-01T05:00:00.000000Z\t19\t00000000 d4 ab be 30 fa 8d ac 3d 98 a0 ad 9a 5d\n" +
                        "20\t856634079\ttrue\tRJU\t0.108206023861\t0.4565\t13505\t669\t2015-11-14T15:19:19.390Z\t\tVTJW\t134\t-3700177025310488849\t1970-01-01T05:16:40.000000Z\t3\t00000000 f8 a1 46 87 28 92 a3 9b e3 cb c2 64 8a b0 35 d8\n" +
                        "00000010 ab 3f a1 f5\n",
                "select a,a1,b,c,d,e,f1,f,g,h,i,j,j1,k,l,m from x",
                "create table x as (" +
                        "select * from random_cursor(" +
                        " 20," + // record count
                        " 'a1', rnd_int()," +
                        " 'a', rnd_int(0, 30, 2)," +
                        " 'b', rnd_boolean()," +
                        " 'c', rnd_str(3,3,2)," +
                        " 'd', rnd_double(2)," +
                        " 'e', rnd_float(2)," +
                        " 'f', rnd_short(10,1024)," +
                        " 'f1', rnd_short()," +
                        " 'g', rnd_date(to_date('2015', 'yyyy'), to_date('2016', 'yyyy'), 2)," +
                        " 'h', rnd_timestamp(to_timestamp('2015', 'yyyy'), to_timestamp('2016', 'yyyy'), 2)," +
                        " 'i', rnd_symbol(4,4,4,2)," +
                        " 'j', rnd_long(100,200,2)," +
                        " 'j1', rnd_long()," +
                        " 'k', timestamp_sequence(to_timestamp(0), 1000000000)," +
                        " 'l', rnd_byte(2,50)," +
                        " 'm', rnd_bin(10, 20, 2)" +
                        "))  timestamp(k) partition by DAY",
                "k");
    }

    @Test
    public void testSelectColumnsSansTimestamp() throws Exception {
        assertQuery("a\ta1\tb\tc\td\te\tf1\tf\tg\th\ti\tj\tj1\n" +
                        "NaN\t1569490116\tfalse\t\tNaN\t0.7611\t-1593\t428\t2015-04-04T16:34:47.226Z\t\t\t185\t7039584373105579285\n" +
                        "10\t1253890363\tfalse\tXYS\t0.191123461757\t0.5793\t-1379\t881\t\t2015-03-04T23:08:35.722465Z\tHYRX\t188\t-4986232506486815364\n" +
                        "27\t-1819240775\ttrue\tGOO\t0.041428124702\t0.9205\t-9039\t97\t2015-08-25T03:15:07.653Z\t2015-12-06T09:41:30.297134Z\tHYRX\t109\t571924429013198086\n" +
                        "18\t-1201923128\ttrue\tUVS\t0.758817540345\t0.5779\t-4379\t480\t2015-12-16T09:15:02.086Z\t2015-05-31T18:12:45.686366Z\tCPSW\tNaN\t-6161552193869048721\n" +
                        "NaN\t865832060\ttrue\t\t0.148305523358\t0.9442\t2508\t95\t\t2015-10-20T09:33:20.502524Z\t\tNaN\t-3289070757475856942\n" +
                        "22\t1100812407\tfalse\tOVL\tNaN\t0.7633\t-17778\t698\t2015-09-13T09:55:17.815Z\t\tCPSW\t182\t-8757007522346766135\n" +
                        "18\t1677463366\tfalse\tMNZ\t0.337470756550\t0.1179\t18904\t533\t2015-05-13T23:13:05.262Z\t2015-05-10T00:20:17.926993Z\t\t175\t6351664568801157821\n" +
                        "4\t39497392\tfalse\tUOH\t0.029227696943\t0.1718\t14242\t652\t\t2015-05-24T22:09:55.175991Z\tVTJW\t141\t3527911398466283309\n" +
                        "10\t1545963509\tfalse\tNWI\t0.113718418361\t0.0620\t-29980\t356\t2015-09-12T14:33:11.105Z\t2015-08-06T04:51:01.526782Z\t\t168\t6380499796471875623\n" +
                        "4\t53462821\tfalse\tGOO\t0.055149337562\t0.1195\t-6087\t115\t2015-08-09T19:28:14.249Z\t2015-09-20T01:50:37.694867Z\tCPSW\t145\t-7212878484370155026\n" +
                        "30\t-2139296159\tfalse\t\t0.185864355816\t0.5638\t21020\t299\t2015-12-30T22:10:50.759Z\t2015-01-19T15:54:44.696040Z\tHYRX\t105\t-3463832009795858033\n" +
                        "21\t-406528351\tfalse\tNLE\tNaN\tNaN\t21057\t968\t2015-10-17T07:20:26.881Z\t2015-06-02T13:00:45.180827Z\tPEHN\t102\t5360746485515325739\n" +
                        "17\t415709351\tfalse\tGQZ\t0.491990017163\t0.6292\t18605\t581\t2015-03-04T06:48:42.194Z\t2015-08-14T15:51:23.307152Z\tHYRX\t185\t-5611837907908424613\n" +
                        "19\t-1387693529\ttrue\tMCG\t0.848083900630\t0.4699\t24206\t119\t2015-03-01T23:54:10.204Z\t2015-10-01T12:02:08.698373Z\t\t175\t3669882909701240516\n" +
                        "21\t346891421\tfalse\t\t0.933609514583\t0.6380\t15084\t405\t2015-10-12T05:36:54.066Z\t2015-11-16T05:48:57.958190Z\tPEHN\t196\t-9200716729349404576\n" +
                        "27\t263487884\ttrue\tHZQ\t0.703978540803\t0.8461\t31562\t834\t2015-08-04T00:55:25.323Z\t2015-07-25T18:26:42.499255Z\tHYRX\t128\t8196544381931602027\n" +
                        "9\t-1034870849\tfalse\tLSV\t0.650660460171\t0.7020\t-838\t110\t2015-08-17T23:50:39.534Z\t2015-03-17T03:23:26.126568Z\tHYRX\tNaN\t-6929866925584807039\n" +
                        "26\t1848218326\ttrue\tSUW\t0.803404910559\t0.0440\t-3502\t854\t2015-04-04T20:55:02.116Z\t2015-11-23T07:46:10.570856Z\t\t145\t4290477379978201771\n" +
                        "5\t-1496904948\ttrue\tDBZ\t0.286271736488\tNaN\t5698\t764\t2015-02-06T02:49:54.147Z\t\t\tNaN\t-3058745577013275321\n" +
                        "20\t856634079\ttrue\tRJU\t0.108206023861\t0.4565\t13505\t669\t2015-11-14T15:19:19.390Z\t\tVTJW\t134\t-3700177025310488849\n",
                "select a,a1,b,c,d,e,f1,f,g,h,i,j,j1 from x",
                "create table x as (" +
                        "select * from random_cursor(" +
                        " 20," + // record count
                        " 'a1', rnd_int()," +
                        " 'a', rnd_int(0, 30, 2)," +
                        " 'b', rnd_boolean()," +
                        " 'c', rnd_str(3,3,2)," +
                        " 'd', rnd_double(2)," +
                        " 'e', rnd_float(2)," +
                        " 'f', rnd_short(10,1024)," +
                        " 'f1', rnd_short()," +
                        " 'g', rnd_date(to_date('2015', 'yyyy'), to_date('2016', 'yyyy'), 2)," +
                        " 'h', rnd_timestamp(to_timestamp('2015', 'yyyy'), to_timestamp('2016', 'yyyy'), 2)," +
                        " 'i', rnd_symbol(4,4,4,2)," +
                        " 'j', rnd_long(100,200,2)," +
                        " 'j1', rnd_long()," +
                        " 'k', timestamp_sequence(to_timestamp(0), 1000000000)," +
                        " 'l', rnd_byte(2,50)," +
                        " 'm', rnd_bin(10, 20, 2)" +
                        "))  timestamp(k) partition by DAY",
                null);
    }

    @Test
    public void testSelectFromAliasedTable() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            Assert.assertNull(compiler.compile("create table my_table (sym int, id long)", sqlExecutionContext));

            try {
                try (RecordCursorFactory factory = compiler.compile("select sum(a.sym) yo, a.id from my_table a", sqlExecutionContext)) {
                    Assert.assertNotNull(factory);
                }
            } finally {
                engine.releaseAllWriters();
                engine.releaseAllReaders();
            }
        });
    }

    @Test
    public void testVirtualColumns() throws Exception {
        assertQuery("a\ta1\tb\tc\tcolumn\tf1\tf\tg\th\ti\tj\tj1\tk\tl\tm\n" +
                        "NaN\t1569490116\tfalse\t\tNaN\t-1593\t428\t2015-04-04T16:34:47.226Z\t\t\t185\t7039584373105579285\t1970-01-01T00:00:00.000000Z\t4\t00000000 af 19 c4 95 94 36 53 49 b4 59 7e\n" +
                        "10\t1253890363\tfalse\tXYS\t0.770470058952\t-1379\t881\t\t2015-03-04T23:08:35.722465Z\tHYRX\t188\t-4986232506486815364\t1970-01-01T00:16:40.000000Z\t50\t00000000 42 fc 31 79 5f 8b 81 2b 93 4d 1a 8e 78 b5\n" +
                        "27\t-1819240775\ttrue\tGOO\t0.961928462780\t-9039\t97\t2015-08-25T03:15:07.653Z\t2015-12-06T09:41:30.297134Z\tHYRX\t109\t571924429013198086\t1970-01-01T00:33:20.000000Z\t21\t\n" +
                        "18\t-1201923128\ttrue\tUVS\t1.336712287603\t-4379\t480\t2015-12-16T09:15:02.086Z\t2015-05-31T18:12:45.686366Z\tCPSW\tNaN\t-6161552193869048721\t1970-01-01T00:50:00.000000Z\t27\t00000000 28 c7 84 47 dc d2 85 7f a5 b8 7b 4a 9d 46\n" +
                        "NaN\t865832060\ttrue\t\t1.092471408807\t2508\t95\t\t2015-10-20T09:33:20.502524Z\t\tNaN\t-3289070757475856942\t1970-01-01T01:06:40.000000Z\t40\t00000000 f2 3c ed 39 ac a8 3b a6 dc 3b 7d 2b e3 92 fe 69\n" +
                        "00000010 38 e1\n" +
                        "22\t1100812407\tfalse\tOVL\tNaN\t-17778\t698\t2015-09-13T09:55:17.815Z\t\tCPSW\t182\t-8757007522346766135\t1970-01-01T01:23:20.000000Z\t23\t\n" +
                        "18\t1677463366\tfalse\tMNZ\t0.455323861618\t18904\t533\t2015-05-13T23:13:05.262Z\t2015-05-10T00:20:17.926993Z\t\t175\t6351664568801157821\t1970-01-01T01:40:00.000000Z\t29\t00000000 5d d0 eb 67 44 a7 6a 71 34 e0 b0 e9 98 f7 67 62\n" +
                        "00000010 28 60\n" +
                        "4\t39497392\tfalse\tUOH\t0.201030575323\t14242\t652\t\t2015-05-24T22:09:55.175991Z\tVTJW\t141\t3527911398466283309\t1970-01-01T01:56:40.000000Z\t9\t00000000 d9 6f 04 ab 27 47 8f 23 3f ae 7c 9f 77 04 e9 0c\n" +
                        "00000010 ea 4e ea 8b\n" +
                        "10\t1545963509\tfalse\tNWI\t0.175745872737\t-29980\t356\t2015-09-12T14:33:11.105Z\t2015-08-06T04:51:01.526782Z\t\t168\t6380499796471875623\t1970-01-01T02:13:20.000000Z\t13\t00000000 54 52 d0 29 26 c5 aa da 18 ce 5f b2 8b 5c 54 90\n" +
                        "4\t53462821\tfalse\tGOO\t0.174661478313\t-6087\t115\t2015-08-09T19:28:14.249Z\t2015-09-20T01:50:37.694867Z\tCPSW\t145\t-7212878484370155026\t1970-01-01T02:30:00.000000Z\t46\t\n" +
                        "30\t-2139296159\tfalse\t\t0.749638583912\t21020\t299\t2015-12-30T22:10:50.759Z\t2015-01-19T15:54:44.696040Z\tHYRX\t105\t-3463832009795858033\t1970-01-01T02:46:40.000000Z\t38\t00000000 b8 07 b1 32 57 ff 9a ef 88 cb 4b\n" +
                        "21\t-406528351\tfalse\tNLE\tNaN\t21057\t968\t2015-10-17T07:20:26.881Z\t2015-06-02T13:00:45.180827Z\tPEHN\t102\t5360746485515325739\t1970-01-01T03:03:20.000000Z\t43\t\n" +
                        "17\t415709351\tfalse\tGQZ\t1.121198641526\t18605\t581\t2015-03-04T06:48:42.194Z\t2015-08-14T15:51:23.307152Z\tHYRX\t185\t-5611837907908424613\t1970-01-01T03:20:00.000000Z\t19\t00000000 20 e2 37 f2 64 43 84 55 a0 dd 44 11 e2 a3 24 4e\n" +
                        "00000010 44 a8 0d fe\n" +
                        "19\t-1387693529\ttrue\tMCG\t1.317948686301\t24206\t119\t2015-03-01T23:54:10.204Z\t2015-10-01T12:02:08.698373Z\t\t175\t3669882909701240516\t1970-01-01T03:36:40.000000Z\t12\t00000000 8f bb 2a 4b af 8f 89 df 35 8f da fe 33 98 80 85\n" +
                        "00000010 20 53 3b 51\n" +
                        "21\t346891421\tfalse\t\t1.571608691562\t15084\t405\t2015-10-12T05:36:54.066Z\t2015-11-16T05:48:57.958190Z\tPEHN\t196\t-9200716729349404576\t1970-01-01T03:53:20.000000Z\t43\t\n" +
                        "27\t263487884\ttrue\tHZQ\t1.550099673177\t31562\t834\t2015-08-04T00:55:25.323Z\t2015-07-25T18:26:42.499255Z\tHYRX\t128\t8196544381931602027\t1970-01-01T04:10:00.000000Z\t15\t00000000 71 76 bc 45 24 cd 13 00 7c fb 01 19 ca f2\n" +
                        "9\t-1034870849\tfalse\tLSV\t1.352704947170\t-838\t110\t2015-08-17T23:50:39.534Z\t2015-03-17T03:23:26.126568Z\tHYRX\tNaN\t-6929866925584807039\t1970-01-01T04:26:40.000000Z\t4\t00000000 4b fb 2d 16 f3 89 a3 83 64 de\n" +
                        "26\t1848218326\ttrue\tSUW\t0.847444875235\t-3502\t854\t2015-04-04T20:55:02.116Z\t2015-11-23T07:46:10.570856Z\t\t145\t4290477379978201771\t1970-01-01T04:43:20.000000Z\t35\t00000000 6d 54 75 10 b3 4c 0e 8f f1 0c c5 60 b7 d1 5a\n" +
                        "5\t-1496904948\ttrue\tDBZ\tNaN\t5698\t764\t2015-02-06T02:49:54.147Z\t\t\tNaN\t-3058745577013275321\t1970-01-01T05:00:00.000000Z\t19\t00000000 d4 ab be 30 fa 8d ac 3d 98 a0 ad 9a 5d\n" +
                        "20\t856634079\ttrue\tRJU\t0.564672758270\t13505\t669\t2015-11-14T15:19:19.390Z\t\tVTJW\t134\t-3700177025310488849\t1970-01-01T05:16:40.000000Z\t3\t00000000 f8 a1 46 87 28 92 a3 9b e3 cb c2 64 8a b0 35 d8\n" +
                        "00000010 ab 3f a1 f5\n",
                "select a,a1,b,c,d+e,f1,f,g,h,i,j,j1,k,l,m from x",
                "create table x as (" +
                        "select * from random_cursor(" +
                        " 20," + // record count
                        " 'a1', rnd_int()," +
                        " 'a', rnd_int(0, 30, 2)," +
                        " 'b', rnd_boolean()," +
                        " 'c', rnd_str(3,3,2)," +
                        " 'd', rnd_double(2)," +
                        " 'e', rnd_float(2)," +
                        " 'f', rnd_short(10,1024)," +
                        " 'f1', rnd_short()," +
                        " 'g', rnd_date(to_date('2015', 'yyyy'), to_date('2016', 'yyyy'), 2)," +
                        " 'h', rnd_timestamp(to_timestamp('2015', 'yyyy'), to_timestamp('2016', 'yyyy'), 2)," +
                        " 'i', rnd_symbol(4,4,4,2)," +
                        " 'j', rnd_long(100,200,2)," +
                        " 'j1', rnd_long()," +
                        " 'k', timestamp_sequence(to_timestamp(0), 1000000000)," +
                        " 'l', rnd_byte(2,50)," +
                        " 'm', rnd_bin(10, 20, 2)" +
                        "))  timestamp(k) partition by DAY",
                "k");
    }

    @Test
    public void testVirtualColumnsSansTimestamp() throws Exception {
        assertQuery("a\ta1\tb\tc\tcolumn\n" +
                        "NaN\t1569490116\tfalse\t\tNaN\n" +
                        "10\t1253890363\tfalse\tXYS\t0.770470058952\n" +
                        "27\t-1819240775\ttrue\tGOO\t0.961928462780\n" +
                        "18\t-1201923128\ttrue\tUVS\t1.336712287603\n" +
                        "NaN\t865832060\ttrue\t\t1.092471408807\n" +
                        "22\t1100812407\tfalse\tOVL\tNaN\n" +
                        "18\t1677463366\tfalse\tMNZ\t0.455323861618\n" +
                        "4\t39497392\tfalse\tUOH\t0.201030575323\n" +
                        "10\t1545963509\tfalse\tNWI\t0.175745872737\n" +
                        "4\t53462821\tfalse\tGOO\t0.174661478313\n" +
                        "30\t-2139296159\tfalse\t\t0.749638583912\n" +
                        "21\t-406528351\tfalse\tNLE\tNaN\n" +
                        "17\t415709351\tfalse\tGQZ\t1.121198641526\n" +
                        "19\t-1387693529\ttrue\tMCG\t1.317948686301\n" +
                        "21\t346891421\tfalse\t\t1.571608691562\n" +
                        "27\t263487884\ttrue\tHZQ\t1.550099673177\n" +
                        "9\t-1034870849\tfalse\tLSV\t1.352704947170\n" +
                        "26\t1848218326\ttrue\tSUW\t0.847444875235\n" +
                        "5\t-1496904948\ttrue\tDBZ\tNaN\n" +
                        "20\t856634079\ttrue\tRJU\t0.564672758270\n",
                "select a,a1,b,c,d+e from x",
                "create table x as (" +
                        "select * from random_cursor(" +
                        " 20," + // record count
                        " 'a1', rnd_int()," +
                        " 'a', rnd_int(0, 30, 2)," +
                        " 'b', rnd_boolean()," +
                        " 'c', rnd_str(3,3,2)," +
                        " 'd', rnd_double(2)," +
                        " 'e', rnd_float(2)," +
                        " 'f', rnd_short(10,1024)," +
                        " 'f1', rnd_short()," +
                        " 'g', rnd_date(to_date('2015', 'yyyy'), to_date('2016', 'yyyy'), 2)," +
                        " 'h', rnd_timestamp(to_timestamp('2015', 'yyyy'), to_timestamp('2016', 'yyyy'), 2)," +
                        " 'i', rnd_symbol(4,4,4,2)," +
                        " 'j', rnd_long(100,200,2)," +
                        " 'j1', rnd_long()," +
                        " 'k', timestamp_sequence(to_timestamp(0), 1000000000)," +
                        " 'l', rnd_byte(2,50)," +
                        " 'm', rnd_bin(10, 20, 2)" +
                        "))  timestamp(k) partition by DAY",
                null);
    }

}