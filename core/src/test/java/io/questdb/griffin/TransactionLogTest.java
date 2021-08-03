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
import org.junit.Assert;
import org.junit.Ignore;
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
                sink.clear();
                sink.put(w1.reconcileSlaveState(w2.getRawTxnMemory(), w2.getRawMetaMemory(), w2.getRawMetaMemorySize()));
                Assert.assertTrue(w1.isTransactionLogEnabled());
            }

            TestUtils.assertEquals(
                    "{\"table\":{\"action\":\"keep\",\"dataVersion\":0},\"partitions\":[{\"action\":\"append\",\"ts\":\"2018-01-12T00:00:00.000000Z\",\"startRow\":22400,\"rowCount\":6400,\"nameTxn\":-1,\"dataTxn\":0,},{\"action\":\"whole\",\"ts\":\"2018-01-13T00:00:00.000000Z\",\"startRow\":0,\"rowCount\":13600,\"nameTxn\":-1,\"dataTxn\":0,}],\"columnMetaData\":[],\"columnMetaIndex\":[]}",
                    sink
            );
        });
    }

    @Test
    public void testOrderedAddColumn() throws Exception {
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

            compiler.compile("alter table x add column z double", sqlExecutionContext);

            try (
                    TableWriter w1 = engine.getWriter(sqlExecutionContext.getCairoSecurityContext(), "x", "log test");
                    TableWriter w2 = engine.getWriter(sqlExecutionContext.getCairoSecurityContext(), "y", "log test")
            ) {
                sink.clear();
                sink.put(w1.reconcileSlaveState(w2.getRawTxnMemory(), w2.getRawMetaMemory(), w2.getRawMetaMemorySize()));
                Assert.assertTrue(w1.isTransactionLogEnabled());
            }

            TestUtils.assertEquals(
                    "{\"table\":{\"action\":\"keep\",\"dataVersion\":0},\"partitions\":[{\"action\":\"append\",\"ts\":\"2018-01-12T00:00:00.000000Z\",\"startRow\":22400,\"rowCount\":6400,\"nameTxn\":-1,\"dataTxn\":0,},{\"action\":\"whole\",\"ts\":\"2018-01-13T00:00:00.000000Z\",\"startRow\":0,\"rowCount\":13600,\"nameTxn\":-1,\"dataTxn\":0,}],\"columnMetaData\":[{\"name\":\"z\",\"type\":\"DOUBLE\",\"index\":false,\"indexCapacity\":256}],\"columnMetaIndex\":[{\"action\":\"add\",\"fromIndex\":0,\"toIndex\":17}]}",
                    sink
            );
        });
    }

    @Test
    @Ignore
    // this test fails because master and slave cannot reconcile the situation
    // where column by the same name was first removed and then re-added. We need more information on
    // column metadata to help us reconcile this. Until this time the test exists but fails. When
    // new information is added on column metadata - the test can be uncommented and completed
    public void testOrderedRemoveAndReAddColumnSameType() throws Exception {
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

            compiler.compile("alter table x drop column o", sqlExecutionContext);

            engine.releaseAllWriters();
            engine.releaseAllReaders();

            compiler.compile("alter table x add column o long256", sqlExecutionContext);

            try (
                    TableWriter w1 = engine.getWriter(sqlExecutionContext.getCairoSecurityContext(), "x", "log test");
                    TableWriter w2 = engine.getWriter(sqlExecutionContext.getCairoSecurityContext(), "y", "log test")
            ) {
                sink.clear();
                sink.put(w1.reconcileSlaveState(w2.getRawTxnMemory(), w2.getRawMetaMemory(), w2.getRawMetaMemorySize()));
                Assert.assertTrue(w1.isTransactionLogEnabled());
            }

            TestUtils.assertEquals(
                    "{\"table\":{\"action\":\"keep\",\"dataVersion\":0},\"partitions\":[{\"action\":\"append\",\"ts\":\"2018-01-12T00:00:00.000000Z\",\"startRow\":22400,\"rowCount\":6400,\"nameTxn\":-1,\"dataTxn\":0,},{\"action\":\"whole\",\"ts\":\"2018-01-13T00:00:00.000000Z\",\"startRow\":0,\"rowCount\":13600,\"nameTxn\":-1,\"dataTxn\":0,}],\"columnMetaData\":[{\"name\":\"z\",\"type\":\"DOUBLE\",\"index\":false,\"indexCapacity\":256}],\"columnMetaIndex\":[{\"action\":\"add\",\"fromIndex\":0,\"toIndex\":17}]}",
                    sink
            );
        });
    }

    @Test
    public void testOrderedRemoveColumn() throws Exception {
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

            compiler.compile("alter table x drop column j", sqlExecutionContext);

            try (
                    TableWriter w1 = engine.getWriter(sqlExecutionContext.getCairoSecurityContext(), "x", "log test");
                    TableWriter w2 = engine.getWriter(sqlExecutionContext.getCairoSecurityContext(), "y", "log test")
            ) {
                sink.clear();
                sink.put(w1.reconcileSlaveState(w2.getRawTxnMemory(), w2.getRawMetaMemory(), w2.getRawMetaMemorySize()));
                Assert.assertTrue(w1.isTransactionLogEnabled());
            }

            TestUtils.assertEquals(
                    "{\"table\":{\"action\":\"keep\",\"dataVersion\":0},\"partitions\":[{\"action\":\"append\",\"ts\":\"2018-01-12T00:00:00.000000Z\",\"startRow\":22400,\"rowCount\":6400,\"nameTxn\":-1,\"dataTxn\":0,},{\"action\":\"whole\",\"ts\":\"2018-01-13T00:00:00.000000Z\",\"startRow\":0,\"rowCount\":13600,\"nameTxn\":-1,\"dataTxn\":0,}],\"columnMetaData\":[],\"columnMetaIndex\":[{\"action\":\"remove\",\"fromIndex\":11,\"toIndex\":-1},{\"action\":\"move\",\"fromIndex\":12,\"toIndex\":11},{\"action\":\"move\",\"fromIndex\":13,\"toIndex\":12},{\"action\":\"move\",\"fromIndex\":14,\"toIndex\":13},{\"action\":\"move\",\"fromIndex\":15,\"toIndex\":14},{\"action\":\"move\",\"fromIndex\":16,\"toIndex\":15}]}",
                    sink
            );
        });
    }

    @Test
    public void testO3PartitionInsertedBefore() throws Exception {
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

            compiler.compile("create table y as (x) timestamp(k) partition by DAY", sqlExecutionContext);

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
                            " timestamp_sequence(to_timestamp('2018-01-09', 'yyyy-MM-dd'), 30000) k," +
                            " rnd_byte(2,50) l," +
                            " rnd_bin(10, 20, 2) m," +
                            " rnd_str(5,16,2) n," +
                            " rnd_long256() o" +
                            " from long_sequence(10000)",
                    sqlExecutionContext
            );

            try (
                    TableWriter w1 = engine.getWriter(sqlExecutionContext.getCairoSecurityContext(), "x", "log test");
                    TableWriter w2 = engine.getWriter(sqlExecutionContext.getCairoSecurityContext(), "y", "log test")
            ) {
                sink.clear();
                sink.put(w1.reconcileSlaveState(w2.getRawTxnMemory(), w2.getRawMetaMemory(), w2.getRawMetaMemorySize()));
                Assert.assertTrue(w1.isTransactionLogEnabled());
            }

            TestUtils.assertEquals(
                    "{\"table\":{\"action\":\"keep\",\"dataVersion\":0},\"partitions\":[{\"action\":\"whole\",\"ts\":\"2018-01-09T00:00:00.000000Z\",\"startRow\":0,\"rowCount\":10000,\"nameTxn\":-1,\"dataTxn\":1,}],\"columnMetaData\":[],\"columnMetaIndex\":[]}",
                    sink
            );
        });
    }

    @Test
    public void testColumnNameCaseSensitivity() throws Exception {
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

            compiler.compile("create table y as (" +
                    "select i, sym, amt, timestamp, b, c, d, e, f, g G, ik IK, j, k K, l, m, n, o from x" +
                    ") timestamp(k) partition by DAY", sqlExecutionContext);

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
                            " timestamp_sequence(to_timestamp('2018-01-09', 'yyyy-MM-dd'), 30000) k," +
                            " rnd_byte(2,50) l," +
                            " rnd_bin(10, 20, 2) m," +
                            " rnd_str(5,16,2) n," +
                            " rnd_long256() o" +
                            " from long_sequence(10000)",
                    sqlExecutionContext
            );

            try (
                    TableWriter w1 = engine.getWriter(sqlExecutionContext.getCairoSecurityContext(), "x", "log test");
                    TableWriter w2 = engine.getWriter(sqlExecutionContext.getCairoSecurityContext(), "y", "log test")
            ) {
                sink.clear();
                sink.put(w1.reconcileSlaveState(w2.getRawTxnMemory(), w2.getRawMetaMemory(), w2.getRawMetaMemorySize()));
                Assert.assertTrue(w1.isTransactionLogEnabled());
            }

            TestUtils.assertEquals(
                    "{\"table\":{\"action\":\"keep\",\"dataVersion\":0},\"partitions\":[{\"action\":\"whole\",\"ts\":\"2018-01-09T00:00:00.000000Z\",\"startRow\":0,\"rowCount\":10000,\"nameTxn\":-1,\"dataTxn\":1,}],\"columnMetaData\":[],\"columnMetaIndex\":[]}",
                    sink
            );
        });
    }

    @Test
    public void testO3PartitionMutates() throws Exception {
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

            compiler.compile("create table y as (x) timestamp(k) partition by DAY", sqlExecutionContext);

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
                            " timestamp_sequence(to_timestamp('2018-01-11', 'yyyy-MM-dd'), 30000) k," +
                            " rnd_byte(2,50) l," +
                            " rnd_bin(10, 20, 2) m," +
                            " rnd_str(5,16,2) n," +
                            " rnd_long256() o" +
                            " from long_sequence(10000)",
                    sqlExecutionContext
            );

            try (
                    TableWriter w1 = engine.getWriter(sqlExecutionContext.getCairoSecurityContext(), "x", "log test");
                    TableWriter w2 = engine.getWriter(sqlExecutionContext.getCairoSecurityContext(), "y", "log test")
            ) {
                sink.clear();
                sink.put(w1.reconcileSlaveState(w2.getRawTxnMemory(), w2.getRawMetaMemory(), w2.getRawMetaMemorySize()));
                Assert.assertTrue(w1.isTransactionLogEnabled());
            }

            TestUtils.assertEquals(
                    "{\"table\":{\"action\":\"keep\",\"dataVersion\":0},\"partitions\":[{\"action\":\"whole\",\"ts\":\"2018-01-11T00:00:00.000000Z\",\"startRow\":0,\"rowCount\":38800,\"nameTxn\":1,\"dataTxn\":0,}],\"columnMetaData\":[],\"columnMetaIndex\":[]}",
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
                sink.clear();
                sink.put(w1.reconcileSlaveState(w2.getRawTxnMemory(), w2.getRawMetaMemory(), w2.getRawMetaMemorySize()));
                Assert.assertTrue(w1.isTransactionLogEnabled());
            }

            TestUtils.assertEquals(
                    "{\"table\":{\"action\":\"keep\",\"dataVersion\":0},\"partitions\":[{\"action\":\"whole\",\"ts\":\"2018-01-13T00:00:00.000000Z\",\"startRow\":0,\"rowCount\":13600,\"nameTxn\":-1,\"dataTxn\":0,}],\"columnMetaData\":[],\"columnMetaIndex\":[]}",
                    sink
            );
        });

    }

    @Test
    public void testOrderedNothingToDo() throws Exception {
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

            compiler.compile("create table y as (x) timestamp(k) partition by DAY", sqlExecutionContext);

            try (
                    TableWriter w1 = engine.getWriter(sqlExecutionContext.getCairoSecurityContext(), "x", "log test");
                    TableWriter w2 = engine.getWriter(sqlExecutionContext.getCairoSecurityContext(), "y", "log test")
            ) {
                sink.clear();
                sink.put(w1.reconcileSlaveState(w2.getRawTxnMemory(), w2.getRawMetaMemory(), w2.getRawMetaMemorySize()));
                Assert.assertFalse(w1.isTransactionLogEnabled());
            }

            TestUtils.assertEquals(
                    "{\"table\":{\"action\":\"keep\",\"dataVersion\":0},\"partitions\":[],\"columnMetaData\":[],\"columnMetaIndex\":[]}",
                    sink
            );
        });
    }

    @Test
    public void testOrderedTruncateAndReAdd() throws Exception {
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
                sink.clear();
                sink.put(w1.reconcileSlaveState(w2.getRawTxnMemory(), w2.getRawMetaMemory(), w2.getRawMetaMemorySize()));
                Assert.assertTrue(w1.isTransactionLogEnabled());
            }

            TestUtils.assertEquals(
                    "{\"table\":{\"action\":\"truncate\",\"dataVersion\":1},\"partitions\":[{\"action\":\"whole\",\"ts\":\"2018-01-10T00:00:00.000000Z\",\"startRow\":0,\"rowCount\":28800,\"nameTxn\":-1,\"dataTxn\":2,},{\"action\":\"whole\",\"ts\":\"2018-01-11T00:00:00.000000Z\",\"startRow\":0,\"rowCount\":28800,\"nameTxn\":-1,\"dataTxn\":2,},{\"action\":\"whole\",\"ts\":\"2018-01-12T00:00:00.000000Z\",\"startRow\":0,\"rowCount\":28800,\"nameTxn\":-1,\"dataTxn\":2,},{\"action\":\"whole\",\"ts\":\"2018-01-13T00:00:00.000000Z\",\"startRow\":0,\"rowCount\":13600,\"nameTxn\":-1,\"dataTxn\":2,}],\"columnMetaData\":[],\"columnMetaIndex\":[]}",
                    sink
            );
        });
    }

    @Test
    public void testOrderedTruncateToEmpty() throws Exception {
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

            try (
                    TableWriter w1 = engine.getWriter(sqlExecutionContext.getCairoSecurityContext(), "x", "log test");
                    TableWriter w2 = engine.getWriter(sqlExecutionContext.getCairoSecurityContext(), "y", "log test")
            ) {
                sink.clear();
                sink.put(w1.reconcileSlaveState(w2.getRawTxnMemory(), w2.getRawMetaMemory(), w2.getRawMetaMemorySize()));
                Assert.assertTrue(w1.isTransactionLogEnabled());
            }

            TestUtils.assertEquals(
                    "{\"table\":{\"action\":\"truncate\",\"dataVersion\":1},\"partitions\":[],\"columnMetaData\":[],\"columnMetaIndex\":[]}",
                    sink
            );
        });
    }
}
