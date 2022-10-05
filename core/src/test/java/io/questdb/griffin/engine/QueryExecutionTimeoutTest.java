/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2022 QuestDB
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

package io.questdb.griffin.engine;

import io.questdb.cairo.CairoException;
import io.questdb.cairo.sql.*;
import io.questdb.griffin.AbstractGriffinTest;
import io.questdb.griffin.CompiledQuery;
import io.questdb.griffin.DefaultSqlExecutionCircuitBreakerConfiguration;
import io.questdb.griffin.SqlException;
import io.questdb.std.MemoryTag;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import static org.junit.Assert.fail;
import static org.hamcrest.MatcherAssert.*;
import static org.hamcrest.Matchers.*;

/**
 * This test verifies that various factories use circuit breaker and thus can time out or recognize broken connection.
 */
public class QueryExecutionTimeoutTest extends AbstractGriffinTest {

    @BeforeClass
    public static void setUpStatic() {
        SqlExecutionCircuitBreakerConfiguration config = new DefaultSqlExecutionCircuitBreakerConfiguration() {
            @Override
            public int getCircuitBreakerThrottle() {
                return 0;
            }
        };

        circuitBreaker = new NetworkSqlExecutionCircuitBreaker(config, MemoryTag.NATIVE_CB5) {
            {
                setTimeout(-100);//trigger timeout on first check 
            }

            @Override
            protected boolean testConnection(long fd) {
                return false;
            }

        };
        AbstractGriffinTest.setUpStatic();
    }

    @Test
    public void testTimeoutInVectorizedKeyedGroupBy() throws Exception {
        assertTimeout("create table grouptest as (select cast(x%1000000 as int) as i, x as l from long_sequence(10000) );",
                "select i, avg(l), max(l) \n" +
                        "from grouptest \n" +
                        "group by i");
    }

    @Test
    public void testTimeoutInVectorizedNonKeyedGroupBy() throws Exception {
        assertTimeout("create table grouptest as (select cast(x%1000000 as int) as i, x as l from long_sequence(10000) );",
                "select avg(l), max(l) \n" +
                        "from grouptest \n");
    }

    @Test
    public void testTimeoutInNonVectorizedKeyedGroupBy() throws Exception {
        assertTimeout("create table grouptest as (select x as i, x as l from long_sequence(10000) );",
                "select i, avg(l), max(l) \n" +
                        "from grouptest \n" +
                        "group by i");
    }

    @Test
    public void testTimeoutInNonVectorizedNonKeyedGroupBy() throws Exception {
        assertTimeout("create table grouptest as (select x as i, x as l from long_sequence(10000) );",
                "select avg(cast(l as int)), max(l) \n" +
                        "from grouptest \n");
    }

    @Test
    public void testTimeoutInRowNumber() throws Exception {
        assertTimeout("create table rntest as (select x as key from long_sequence(1000));\n",
                "select row_number() over (partition by key%1000 ), key  \n" +
                        "from rntest");
    }

    @Test
    public void testTimeoutInOrderedRowNumber() throws Exception {
        assertTimeout("create table rntest as (select x as key from long_sequence(1000));\n",
                "select row_number() over (partition by key%1000 order by key ), key  \n" +
                        "from rntest");
    }

    @Test
    public void testTimeoutInLatestByAllFiltered() throws Exception {
        assertTimeout("create table x as " +
                        "(select  rnd_double(0)*100 a, " +
                        "rnd_str(2,4,4) b, " +
                        "timestamp_sequence(0, 100000000000) k " +
                        "from long_sequence(20)) " +
                        "timestamp(k) partition by DAY",
                "select * from x latest by b where b = 'HNR'");
    }

    @Test
    public void testTimeoutInLatestByAllIndexed() throws Exception {
        assertTimeout("create table x as " +
                        "(" +
                        "select" +
                        " timestamp_sequence(0, 100000000000) k," +
                        " rnd_double(0)*100 a1," +
                        " rnd_double(0)*100 a2," +
                        " rnd_double(0)*100 a3," +
                        " rnd_double(0)*100 a," +
                        " rnd_symbol(5,4,4,1) b" +
                        " from long_sequence(20)" +
                        "), index(b) timestamp(k) partition by DAY",
                "select * from (select a,k,b from x latest on k partition by b) where a > 40");
    }

    @Test
    public void testTimeoutInLatestByAll() throws Exception {
        assertTimeout("create table xx(value long256, ts timestamp) timestamp(ts)",
                "insert into xx values(null, 0)",
                "select * from xx latest on ts partition by value");
    }

    @Test
    public void testTimeoutInLatestByValueFiltered() throws Exception {
        assertTimeout("CREATE table trades(symbol symbol, side symbol, ts timestamp) timestamp(ts)",
                "insert into trades " +
                        "select 'BTC' || x, 'buy' || x, dateadd( 's', x::int, now() ) " +
                        "from long_sequence(10000)",
                "SELECT * FROM trades " +
                        "WHERE symbol in ('BTC1') " +
                        "AND side in 'buy1' " +
                        "LATEST ON ts " +
                        "PARTITION BY symbol;");
    }

    @Test
    public void testTimeoutInLatestByValueIndexed() throws Exception {
        assertTimeout("create table x as " +
                        "(select rnd_double(0)*100 a, " +
                        "rnd_symbol(5,4,4,1) b, " +
                        "timestamp_sequence(0, 100000000000) k " +
                        "from long_sequence(200)), " +
                        "index(b) timestamp(k) partition by DAY",
                "select * from x where b = 'PEHN' and a < 22 and test_match() latest on k partition by b");
    }

    @Test
    public void testTimeoutInLatestByValue() throws Exception {
        assertTimeout("create table x as " +
                        "(select  rnd_double(0)*100 a, " +
                        "rnd_symbol(5,4,4,1) b, " +
                        "timestamp_sequence(0, 100000000000) k " +
                        "from long_sequence(20)) " +
                        "timestamp(k) partition by DAY",
                "select * from x where b = 'RXGZ' latest on k partition by b");
    }

    @Test
    public void testTimeoutInLatestByValueList() throws Exception {
        assertTimeout("create table t as (" +
                        "select " +
                        "x, " +
                        "rnd_symbol('a', 'b', 'c', 'd', 'e', 'f') s, " +
                        "timestamp_sequence(0, 60*60*1000*1000L) ts " +
                        "from long_sequence(49)" +
                        ") timestamp(ts) Partition by DAY",
                "select ts, x, s from t latest on ts partition by s");
    }

    @Test
    public void testTimeoutInLatestByValueListWithFindSelectedSymbolsAndFilter() throws Exception {
        assertTimeout("create table t as (" +
                        "select " +
                        "x, " +
                        "rnd_symbol('a', 'b', null) s, " +
                        "timestamp_sequence(0, 60*60*1000*1000L) ts " +
                        "from long_sequence(49)" +
                        ") timestamp(ts) Partition by DAY",
                "select * from t " +
                        "where s in ('a', 'b') and x%2 = 0 " +
                        "latest on ts partition by s");
    }

    @Test
    public void testTimeoutInLatestByValueListWithFindSelectedSymbolsAndNoFilter() throws Exception {
        assertTimeout("create table t as (" +
                        "select " +
                        "x, " +
                        "rnd_symbol('a', 'b', null) s, " +
                        "timestamp_sequence(0, 60*60*1000*1000L) ts " +
                        "from long_sequence(49)" +
                        ") timestamp(ts) Partition by DAY",
                "select * from t where s in ('a', null) latest on ts partition by s");
    }

    @Test
    public void testTimeoutInLatestByValueListWithFindAllDistinctSymbolsAndNoFilter() throws Exception {
        assertTimeout("create table t as (" +
                        "select " +
                        "x, " +
                        "rnd_symbol('a', 'b', 'c', 'd', 'e', 'f') s, " +
                        "timestamp_sequence(0, 60*60*1000*1000L) ts " +
                        "from long_sequence(49)" +
                        ") timestamp(ts) Partition by DAY",
                "select ts, x, s from t latest on ts partition by s");
    }

    @Test
    public void testTimeoutInLatestByValueListWithFindAllDistinctSymbolsAndFilter() throws Exception {
        assertTimeout("create table t as (" +
                        "select " +
                        "x, " +
                        "rnd_symbol('a', 'b', null) s, " +
                        "timestamp_sequence(0, 60*60*1000*1000L) ts " +
                        "from long_sequence(49)" +
                        ") timestamp(ts) Partition by DAY",
                "selecT * from t where x%2 = 1 latest on ts partition by s");
    }

    @Test
    public void testTimeoutInLatestByValuesFiltered() throws Exception {
        assertTimeout("create table x as " +
                        "(select rnd_double(0)*100 a, " +
                        "rnd_symbol(5,4,4,1) b, " +
                        "timestamp_sequence(0, 100000000000) k " +
                        "from long_sequence(20)) " +
                        "timestamp(k) partition by DAY",
                "select * from x where b in (select rnd_symbol('RXGZ', 'HYRX', null, 'UCLA') a from long_sequence(10)) and a > 12 and a < 50 and test_match() latest on k partition by b");
    }

    @Test
    public void testTimeoutInLatestByValuesIndexed() throws Exception {
        assertTimeout("create table x as " +
                        "(select rnd_double(0)*100 a, " +
                        "rnd_symbol(5,4,4,1) b, " +
                        "timestamp_sequence(0, 10000000000) k " +
                        "from long_sequence(300)), " +
                        "index(b) timestamp(k) partition by DAY",
                "select * from x where b in ('XYZ', 'HYRX') and a > 30 and test_match() latest on k partition by b");
    }

    @Test
    public void testTimeoutInLatestByValues() throws Exception {
        assertTimeout("create table x as " +
                        "(select rnd_double(0)*100 a, " +
                        "rnd_symbol(5,4,4,1) b, " +
                        "timestamp_sequence(0, 100000000000) k " +
                        "from long_sequence(20)) " +
                        "timestamp(k) partition by DAY",
                "select * from x where b in (select list('RXGZ', 'HYRX', null, 'UCLA') a from long_sequence(10)) latest on k partition by b");
    }

    @Test
    public void testTimeoutInMultiHashJoin() throws Exception {
        ((NetworkSqlExecutionCircuitBreaker) circuitBreaker).setTimeout(1);
        try {
            assertTimeout("create table grouptest as " +
                            "(select cast(x%1000000 as int) as i, x as l from long_sequence(100000) );\n",
                    "select * from \n" +
                            "(\n" +
                            "  select * \n" +
                            "  from grouptest gt1\n" +
                            "  join grouptest gt2 on i\n" +
                            ")\n" +
                            "join grouptest gt3 on i");
        } finally {
            resetTimeout();
        }
    }

    @Test
    public void testTimeoutInInsertAsSelect() throws Exception {
        assertTimeout("create table instest ( i int, l long, d double ) ",
                "insert into instest select rnd_int(), rnd_long(), rnd_double() from long_sequence(10000000)",
                "select count(*) from instest");
    }

    @Test
    public void testTimeoutInInsertAsSelectBatchedAndOrderedByTs() throws Exception {
        assertTimeout("create table instest ( i int, l long, d double, ts timestamp ) timestamp(ts) ",
                "insert batch 100 into instest select rnd_int(), rnd_long(), rnd_double(), cast(x as timestamp) from long_sequence(10000000)",
                "select count(*) from instest");
    }

    @Test
    public void testTimeoutInInsertAsSelectBatchedAndOrderedByTsAsString() throws Exception {
        assertTimeout("create table instest ( i int, l long, d double, ts timestamp ) timestamp(ts) ",
                "insert batch 100 into instest select rnd_int(), rnd_long(), rnd_double(), cast(cast(x as timestamp) as string) from long_sequence(10000000)",
                "select count(*) from instest");
    }

    @Test
    public void testTimeoutInInsertAsSelectOrderedByTs() throws Exception {
        assertTimeout("create table instest ( i int, l long, d double, ts timestamp ) timestamp(ts) ",
                "insert into instest select rnd_int(), rnd_long(), rnd_double(), cast(x as timestamp) from long_sequence(10000000)",
                "select count(*) from instest");
    }

    @Test
    public void testTimeoutInCreateTableAsSelectFromVirtualTable() throws Exception {
        assertTimeout("create table instest as (select rnd_int(), rnd_long(), rnd_double() from long_sequence(10000000))",
                "",
                "select count(*) from instest");

        try {
            compile("select * from instest");
            fail();
        } catch (SqlException e) {
            assertThat(e.getMessage(), containsString("table does not exist"));
        }
    }

    @Test
    public void testTimeoutInCreateTableAsSelectFromRealTable() throws Exception {
        unsetTimeout();
        try {
            compiler.compile("create table instest as (select rnd_int(), rnd_long(), rnd_double() from long_sequence(10000))", sqlExecutionContext);
            resetTimeout();
            assertTimeout("create table instest2 as (select * from instest);", "");
        } finally {
            resetTimeout();
        }

        try {
            compile("select * from instest2");
            fail();
        } catch (SqlException e) {
            assertThat(e.getMessage(), containsString("table does not exist"));
        }
    }

    @Test
    public void testTimeoutInUpdateTable() throws Exception {
        unsetTimeout();
        try {
            compiler.compile("create table updtest as (select rnd_int() i, rnd_long() l, rnd_double() d from long_sequence(10000))", sqlExecutionContext);
            resetTimeout();
            assertTimeout("update updtest  set i = rnd_int(), l = i * l, d = d/7 *31",
                    "");
        } finally {
            resetTimeout();
        }
    }

    private void unsetTimeout() {
        ((NetworkSqlExecutionCircuitBreaker) circuitBreaker).setTimeout(Long.MAX_VALUE);
    }

    private void resetTimeout() {
        ((NetworkSqlExecutionCircuitBreaker) circuitBreaker).setTimeout(-100);
    }

    private void assertTimeout(String ddl, String query) throws Exception {
        assertTimeout(ddl, null, query);
    }

    private void assertTimeout(String ddl, String dml, String query) throws Exception {
        try {
            assertMemoryLeak(() -> {
                compile(ddl, sqlExecutionContext);
                if (dml != null) {
                    compile(dml, sqlExecutionContext).execute(null);
                }

                snapshotMemoryUsage();
                CompiledQuery cc = compiler.compile(query, sqlExecutionContext);
                try (RecordCursorFactory factory = cc.getRecordCursorFactory();
                     RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                    cursor.hasNext();
                }
                assertFactoryMemoryUsage();
            });

            fail("Cairo timeout exception expected!");
        } catch (CairoException ce) {
            Assert.assertTrue("Exception should be interrupted! " + ce, ce.isInterruption());
        }
    }
}
