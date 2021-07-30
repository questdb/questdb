/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2020 QuestDB
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

package io.questdb.griffin;

import io.questdb.cairo.TableWriter;
import io.questdb.test.tools.TestUtils;
import org.junit.Test;

public class TransactionLogTest extends AbstractGriffinTest {

    @Test
    public void testOrderedAcrossPartition() throws Exception {
        assertMemoryLeak(() -> {
            compiler.compile(
                    "create table x as  " +
                            "(select" +
                            " cast(x + 10 as int) i," +
                            " rnd_symbol('msft','ibm', 'googl') sym," +
                            " round(rnd_double(0)*100, 3) amt," +
                            " to_timestamp('2018-01', 'yyyy-MM') + (x + 10) * 720000000 timestamp," +
                            " rnd_boolean() b," +
                            " rnd_str(1,1,2) c," +
                            " rnd_double(2) d," +
                            " rnd_float(2) e," +
                            " rnd_short(10,1024) f," +
                            " rnd_date(to_date('2015', 'yyyy'), to_date('2016', 'yyyy'), 2) g," +
                            " rnd_symbol(4,4,4,2) ik," +
                            " rnd_long() j," +
                            " timestamp_sequence(to_timestamp('2018-01-10', 'yyyy-MM-dd'), 3000000) k," +
                            " rnd_byte(2,50) l," +
                            " rnd_bin(10, 20, 2) m," +
                            " rnd_str(5,16,2) n," +
                            " rnd_long256() o" +
                            " from long_sequence(100000)" +
                            ") timestamp(k) partition by DAY",
                    sqlExecutionContext
            );

            compiler.compile("create table y as (select * from x limit 80000) timestamp(k) partition by DAY", sqlExecutionContext);

            try (
                    TableWriter w1 = engine.getWriter(sqlExecutionContext.getCairoSecurityContext(), "x", "log test");
                    TableWriter w2 = engine.getWriter(sqlExecutionContext.getCairoSecurityContext(), "y", "log test")
            ) {
                w1.enableTransactionLog();
                sink.clear();
                sink.put(w1.reconcileSlaveState(w2.getRawTxnMemory()));
            }

            TestUtils.assertEquals(
                    "keep dataVersion=0\n" +
                            "append ts=2018-01-12T00:00:00.000000Z startRow=22400 rowCount=6400\n" +
                            "whole ts=2018-01-13T00:00:00.000000Z startRow=0 rowCount=13600\n",
                    sink
            );
        });
    }

    @Test
    public void testOrderedCleanPartitionBreakOnLast() throws Exception {
        assertMemoryLeak(() -> {
            compiler.compile(
                    "create table x as  " +
                            "(select" +
                            " cast(x + 10 as int) i," +
                            " rnd_symbol('msft','ibm', 'googl') sym," +
                            " round(rnd_double(0)*100, 3) amt," +
                            " to_timestamp('2018-01', 'yyyy-MM') + (x + 10) * 720000000 timestamp," +
                            " rnd_boolean() b," +
                            " rnd_str(1,1,2) c," +
                            " rnd_double(2) d," +
                            " rnd_float(2) e," +
                            " rnd_short(10,1024) f," +
                            " rnd_date(to_date('2015', 'yyyy'), to_date('2016', 'yyyy'), 2) g," +
                            " rnd_symbol(4,4,4,2) ik," +
                            " rnd_long() j," +
                            " timestamp_sequence(to_timestamp('2018-01-10', 'yyyy-MM-dd'), 3000000) k," +
                            " rnd_byte(2,50) l," +
                            " rnd_bin(10, 20, 2) m," +
                            " rnd_str(5,16,2) n," +
                            " rnd_long256() o" +
                            " from long_sequence(100000)" +
                            ") timestamp(k) partition by DAY",
                    sqlExecutionContext
            );

            compiler.compile("create table y as (select * from x where k < '2018-01-13') timestamp(k) partition by DAY", sqlExecutionContext);

            try (
                    TableWriter w1 = engine.getWriter(sqlExecutionContext.getCairoSecurityContext(), "x", "log test");
                    TableWriter w2 = engine.getWriter(sqlExecutionContext.getCairoSecurityContext(), "y", "log test")
            ) {
                w1.enableTransactionLog();
                sink.clear();
                sink.put(w1.reconcileSlaveState(w2.getRawTxnMemory()));
            }

/*
            compiler.compile(
                    "insert into x select * from " +
                            "(select" +
                            " cast(x + 10 as int) i," +
                            " rnd_symbol('msft','ibm', 'googl') sym," +
                            " round(rnd_double(0)*100, 3) amt," +
                            " to_timestamp('2018-01', 'yyyy-MM') + (x + 10) * 720000000 timestamp," +
                            " rnd_boolean() b," +
                            " rnd_str(1,1,2) c," +
                            " rnd_double(2) d," +
                            " rnd_float(2) e," +
                            " rnd_short(10,1024) f," +
                            " rnd_date(to_date('2015', 'yyyy'), to_date('2016', 'yyyy'), 2) g," +
                            " rnd_symbol(4,4,4,2) ik," +
                            " rnd_long() j," +
                            " timestamp_sequence(to_timestamp('2018-01-15', 'yyyy-MM-dd'), 3000000) k," +
                            " rnd_byte(2,50) l," +
                            " rnd_bin(10, 20, 2) m," +
                            " rnd_str(5,16,2) n," +
                            " rnd_long256() o" +
                            " from long_sequence(100000)" +
                            ") timestamp(k)",
                    sqlExecutionContext
            );
*/

            TestUtils.assertEquals(
                    "keep dataVersion=0\n" +
                            "whole ts=2018-01-13T00:00:00.000000Z startRow=0 rowCount=13600\n",
                    sink
            );
        });

    }

    @Test
    public void testOrderedTruncate() throws Exception {
        assertMemoryLeak(() -> {
            compiler.compile(
                    "create table x as  " +
                            "(select" +
                            " cast(x + 10 as int) i," +
                            " rnd_symbol('msft','ibm', 'googl') sym," +
                            " round(rnd_double(0)*100, 3) amt," +
                            " to_timestamp('2018-01', 'yyyy-MM') + (x + 10) * 720000000 timestamp," +
                            " rnd_boolean() b," +
                            " rnd_str(1,1,2) c," +
                            " rnd_double(2) d," +
                            " rnd_float(2) e," +
                            " rnd_short(10,1024) f," +
                            " rnd_date(to_date('2015', 'yyyy'), to_date('2016', 'yyyy'), 2) g," +
                            " rnd_symbol(4,4,4,2) ik," +
                            " rnd_long() j," +
                            " timestamp_sequence(to_timestamp('2018-01-10', 'yyyy-MM-dd'), 3000000) k," +
                            " rnd_byte(2,50) l," +
                            " rnd_bin(10, 20, 2) m," +
                            " rnd_str(5,16,2) n," +
                            " rnd_long256() o" +
                            " from long_sequence(100000)" +
                            ") timestamp(k) partition by DAY",
                    sqlExecutionContext
            );

            compiler.compile("create table y as (select * from x) timestamp(k) partition by DAY", sqlExecutionContext);

            compiler.compile("truncate table x", sqlExecutionContext);

            compiler.compile("insert into x " +
                            "select" +
                            " cast(x + 10 as int) i," +
                            " rnd_symbol('msft','ibm', 'googl') sym," +
                            " round(rnd_double(0)*100, 3) amt," +
                            " to_timestamp('2018-01', 'yyyy-MM') + (x + 10) * 720000000 timestamp," +
                            " rnd_boolean() b," +
                            " rnd_str(1,1,2) c," +
                            " rnd_double(2) d," +
                            " rnd_float(2) e," +
                            " rnd_short(10,1024) f," +
                            " rnd_date(to_date('2015', 'yyyy'), to_date('2016', 'yyyy'), 2) g," +
                            " rnd_symbol(4,4,4,2) ik," +
                            " rnd_long() j," +
                            " timestamp_sequence(to_timestamp('2018-01-10', 'yyyy-MM-dd'), 3000000) k," +
                            " rnd_byte(2,50) l," +
                            " rnd_bin(10, 20, 2) m," +
                            " rnd_str(5,16,2) n," +
                            " rnd_long256() o" +
                            " from long_sequence(100000)",
                    sqlExecutionContext
            );

            try (
                    TableWriter w1 = engine.getWriter(sqlExecutionContext.getCairoSecurityContext(), "x", "log test");
                    TableWriter w2 = engine.getWriter(sqlExecutionContext.getCairoSecurityContext(), "y", "log test")
            ) {
                w1.enableTransactionLog();
                sink.clear();
                sink.put(w1.reconcileSlaveState(w2.getRawTxnMemory()));
            }

            TestUtils.assertEquals(
                    "truncate dataVersion=1\n" +
                            "whole ts=2018-01-10T00:00:00.000000Z startRow=0 rowCount=28800\n" +
                            "whole ts=2018-01-11T00:00:00.000000Z startRow=0 rowCount=28800\n" +
                            "whole ts=2018-01-12T00:00:00.000000Z startRow=0 rowCount=28800\n" +
                            "whole ts=2018-01-13T00:00:00.000000Z startRow=0 rowCount=13600\n",
                    sink
            );
        });
    }
}
