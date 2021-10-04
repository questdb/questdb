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

import io.questdb.cairo.CairoEngine;
import io.questdb.cairo.TableSyncModel;
import io.questdb.cairo.TableUtils;
import io.questdb.cairo.TableWriter;
import io.questdb.griffin.engine.functions.rnd.SharedRandom;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.mp.FanOut;
import io.questdb.mp.RingQueue;
import io.questdb.mp.SCSequence;
import io.questdb.mp.Sequence;
import io.questdb.network.Net;
import io.questdb.std.Rnd;
import io.questdb.tasks.TableWriterTask;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

import java.io.Closeable;

public class ReplModelReconTest extends AbstractGriffinTest {

    private static final Log LOG = LogFactory.getLog(ReplModelReconTest.class);

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
                sink.put(w1.replCreateTableSyncModel(w2.getRawTxnMemory(), w2.getRawMetaMemory(), w2.getRawMetaMemorySize()));
                Assert.assertTrue(w1.isTransactionLogPending());
            }

            TestUtils.assertEquals(
                    "{\"table\":{\"action\":\"keep\",\"dataVersion\":0},\"partitions\":[{\"action\":\"whole\",\"ts\":\"2018-01-09T00:00:00.000000Z\",\"startRow\":0,\"rowCount\":10000,\"nameTxn\":-1,\"dataTxn\":1}]}",
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
                sink.put(w1.replCreateTableSyncModel(w2.getRawTxnMemory(), w2.getRawMetaMemory(), w2.getRawMetaMemorySize()));
                Assert.assertTrue(w1.isTransactionLogPending());
            }

            TestUtils.assertEquals(
                    "{\"table\":{\"action\":\"keep\",\"dataVersion\":0},\"partitions\":[{\"action\":\"whole\",\"ts\":\"2018-01-09T00:00:00.000000Z\",\"startRow\":0,\"rowCount\":10000,\"nameTxn\":-1,\"dataTxn\":1}]}",
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
                sink.put(w1.replCreateTableSyncModel(w2.getRawTxnMemory(), w2.getRawMetaMemory(), w2.getRawMetaMemorySize()));
                Assert.assertTrue(w1.isTransactionLogPending());
            }

            TestUtils.assertEquals(
                    "{\"table\":{\"action\":\"keep\",\"dataVersion\":0},\"partitions\":[{\"action\":\"whole\",\"ts\":\"2018-01-11T00:00:00.000000Z\",\"startRow\":0,\"rowCount\":38800,\"nameTxn\":1,\"dataTxn\":0}]}",
                    sink
            );
        });
    }

    @Test
    public void testO3TriggerTransactionLog() throws Exception {
        assertMemoryLeak(() -> {

            SharedRandom.RANDOM.set(new Rnd());

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

            compiler.compile("create table chunk1 as (" +
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
                    " timestamp_sequence(to_timestamp('2018-01-10', 'yyyy-MM-dd') + (3000000+100000)*100000L, 30000) k," +
                    " rnd_byte(2,50) l," +
                    " rnd_bin(10, 20, 2) m," +
                    " rnd_str(5,16,2) n," +
                    " rnd_long256() o" +
                    " from long_sequence(10000)" +
                    ")", sqlExecutionContext
            );

            compiler.compile("insert into x select * from chunk1", sqlExecutionContext);

            long masterId;
            try (TableWriter w1 = engine.getWriter(sqlExecutionContext.getCairoSecurityContext(), "x", "log test")) {
                masterId = w1.getMetadata().getId();
            }

            try (
                    TableWriter w2 = engine.getWriter(sqlExecutionContext.getCairoSecurityContext(), "y", "log test");
                    SimpleLocalClient client = new SimpleLocalClient(engine)
            ) {
                sendAndAssertSync(masterId, w2, client, Net.parseIPv4("192.168.1.11"), 0);
                // send a couple of sync requests to check that master does not freak out
                sendAndAssertSync(masterId, w2, client, Net.parseIPv4("192.168.1.10"), 1);
            }

            // O3 chunk (chunk2)
            compiler.compile(
                    "create table chunk2 as (" +
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
                            " cast(to_timestamp('2018-01-09', 'yyyy-MM-dd') + 30000*10000L + ((x-1)/4) * 1000000L + (4-(x-1)%4)*80009L  as timestamp) k," +
                            " rnd_byte(2,50) l," +
                            " rnd_bin(10, 20, 2) m," +
                            " rnd_str(5,16,2) n," +
                            " rnd_long256() o" +
                            " from long_sequence(100)" +
                            ")",
                    sqlExecutionContext
            );

            compiler.compile(
                    "create table chunk3 as (" +
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
                            " timestamp_sequence(to_timestamp('2018-01-13T14:11:40', 'yyyy-MM-ddTHH:mm:ss'), 30000) k," +
                            " rnd_byte(2,50) l," +
                            " rnd_bin(10, 20, 2) m," +
                            " rnd_str(5,16,2) n," +
                            " rnd_long256() o" +
                            " from long_sequence(100)" +
                            ")",
                    sqlExecutionContext
            );

            compiler.compile("insert batch 20000 commitLag 3d into x select * from chunk2", sqlExecutionContext);
            compiler.compile("insert into x select * from chunk3", sqlExecutionContext);

            // we should now have transaction log, which can be retrieved via function call
            // if we copy new chunk, which was outside of transaction log and transaction log itself into 'y'
            // we would expect to end up with the same 'x' and 'y'

            compiler.compile("insert into y select * from chunk1", sqlExecutionContext);
            compiler.compile("insert into y select * from transaction_log('x')", sqlExecutionContext);

            // tables should be the same
            TestUtils.assertSqlCursors(
                    compiler,
                    sqlExecutionContext,
                    "x",
                    "y",
                    LOG
            );
        });
    }

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
                sink.put(w1.replCreateTableSyncModel(w2.getRawTxnMemory(), w2.getRawMetaMemory(), w2.getRawMetaMemorySize()));
                Assert.assertTrue(w1.isTransactionLogPending());
            }

            TestUtils.assertEquals(
                    "{\"table\":{\"action\":\"keep\",\"dataVersion\":0},\"partitions\":[{\"action\":\"append\",\"ts\":\"2018-01-12T00:00:00.000000Z\",\"startRow\":22400,\"rowCount\":6400,\"nameTxn\":-1,\"dataTxn\":0},{\"action\":\"whole\",\"ts\":\"2018-01-13T00:00:00.000000Z\",\"startRow\":0,\"rowCount\":13600,\"nameTxn\":-1,\"dataTxn\":1}]}",
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

            compile("alter table x add column z double", sqlExecutionContext);

            try (
                    TableWriter w1 = engine.getWriter(sqlExecutionContext.getCairoSecurityContext(), "x", "log test");
                    TableWriter w2 = engine.getWriter(sqlExecutionContext.getCairoSecurityContext(), "y", "log test")
            ) {
                sink.clear();
                sink.put(w1.replCreateTableSyncModel(w2.getRawTxnMemory(), w2.getRawMetaMemory(), w2.getRawMetaMemorySize()));
                Assert.assertTrue(w1.isTransactionLogPending());
            }

            TestUtils.assertEquals(
                    "{\"table\":{\"action\":\"keep\",\"dataVersion\":0},\"columnTops\":[{\"ts\":\"2018-01-13T00:00:00.000000Z\",\"index\":17,\"top\":13600}],\"partitions\":[{\"action\":\"append\",\"ts\":\"2018-01-12T00:00:00.000000Z\",\"startRow\":22400,\"rowCount\":6400,\"nameTxn\":-1,\"dataTxn\":0},{\"action\":\"whole\",\"ts\":\"2018-01-13T00:00:00.000000Z\",\"startRow\":0,\"rowCount\":13600,\"nameTxn\":-1,\"dataTxn\":2}],\"columnMetaData\":[{\"name\":\"z\",\"type\":\"DOUBLE\",\"index\":false,\"indexCapacity\":256}],\"columnMetaIndex\":[{\"action\":\"add\",\"fromIndex\":0,\"toIndex\":17}]}",
                    sink
            );
        });
    }

    @Test
    public void testOrderedAddColumnTop() throws Exception {
        assertMemoryLeak(() -> {
            createDataOrderedAddColumnTop();
            try (
                    TableWriter w1 = engine.getWriter(sqlExecutionContext.getCairoSecurityContext(), "x", "log test");
                    TableWriter w2 = engine.getWriter(sqlExecutionContext.getCairoSecurityContext(), "y", "log test")
            ) {
                sink.clear();
                sink.put(w1.replCreateTableSyncModel(w2.getRawTxnMemory(), w2.getRawMetaMemory(), w2.getRawMetaMemorySize()));
                Assert.assertTrue(w1.isTransactionLogPending());
            }

            TestUtils.assertEquals(
                    "{\"table\":{\"action\":\"keep\",\"dataVersion\":0},\"columnTops\":[{\"ts\":\"2018-01-13T00:00:00.000000Z\",\"index\":17,\"top\":13600}],\"partitions\":[{\"action\":\"append\",\"ts\":\"2018-01-12T00:00:00.000000Z\",\"startRow\":22400,\"rowCount\":6400,\"nameTxn\":-1,\"dataTxn\":0},{\"action\":\"whole\",\"ts\":\"2018-01-13T00:00:00.000000Z\",\"startRow\":0,\"rowCount\":23600,\"nameTxn\":-1,\"dataTxn\":3}],\"columnMetaData\":[{\"name\":\"z\",\"type\":\"DOUBLE\",\"index\":false,\"indexCapacity\":256}],\"columnMetaIndex\":[{\"action\":\"add\",\"fromIndex\":0,\"toIndex\":17}]}",
                    sink
            );
        });
    }

    @Test
    public void testOrderedAddColumnTopViaCmd() throws Exception {
        assertMemoryLeak(() -> {

            createDataOrderedAddColumnTop();

            try (
                    TableWriter w1 = engine.getWriter(sqlExecutionContext.getCairoSecurityContext(), "x", "log test");
                    TableWriter w2 = engine.getWriter(sqlExecutionContext.getCairoSecurityContext(), "y", "log test");
                    SimpleLocalClient client = new SimpleLocalClient(engine)
            ) {
                Assert.assertTrue(
                        client.publishSyncCmd(
                                w1.getTableName(),
                                w1.getMetadata().getId(),
                                w2,
                                Net.parseIPv4("192.168.1.14"),
                                0
                        )
                );

                w1.tick(true);
                engine.tick();

                final TableSyncModel model = client.consumeSyncModel();

                Assert.assertNotNull(model);
                sink.clear();
                sink.put(model);
                Assert.assertTrue(w1.isTransactionLogPending());
            }

            TestUtils.assertEquals(
                    "{\"table\":{\"action\":\"keep\",\"dataVersion\":0},\"columnTops\":[{\"ts\":\"2018-01-13T00:00:00.000000Z\",\"index\":17,\"top\":13600}],\"partitions\":[{\"action\":\"append\",\"ts\":\"2018-01-12T00:00:00.000000Z\",\"startRow\":22400,\"rowCount\":6400,\"nameTxn\":-1,\"dataTxn\":0},{\"action\":\"whole\",\"ts\":\"2018-01-13T00:00:00.000000Z\",\"startRow\":0,\"rowCount\":23600,\"nameTxn\":-1,\"dataTxn\":3}],\"columnMetaData\":[{\"name\":\"z\",\"type\":\"DOUBLE\",\"index\":false,\"indexCapacity\":256}],\"columnMetaIndex\":[{\"action\":\"add\",\"fromIndex\":0,\"toIndex\":17}]}",
                    sink
            );
        });
    }

    @Test
    public void testOrderedAddColumnTopViaEngineCmd() throws Exception {
        assertMemoryLeak(() -> {
            createDataOrderedAddColumnTop();

            // We are going to be triggering sync event via engine. For this we will need to know our master
            // table id

            long masterId;
            try (TableWriter w1 = engine.getWriter(sqlExecutionContext.getCairoSecurityContext(), "x", "log test")) {
                masterId = w1.getMetadata().getId();
            }

            try (
                    TableWriter w2 = engine.getWriter(sqlExecutionContext.getCairoSecurityContext(), "y", "log test");
                    SimpleLocalClient client = new SimpleLocalClient(engine)
            ) {
                Assert.assertTrue(
                        client.publishSyncCmd(
                                "x",
                                masterId,
                                w2,
                                Net.parseIPv4("192.168.1.12"),
                                0
                        )
                );

                engine.tick();

                final TableSyncModel model = client.consumeSyncModel();
                Assert.assertNotNull(model);
                sink.clear();
                sink.put(model);
            }

            TestUtils.assertEquals(
                    "{\"table\":{\"action\":\"keep\",\"dataVersion\":0},\"columnTops\":[{\"ts\":\"2018-01-13T00:00:00.000000Z\",\"index\":17,\"top\":13600}],\"partitions\":[{\"action\":\"append\",\"ts\":\"2018-01-12T00:00:00.000000Z\",\"startRow\":22400,\"rowCount\":6400,\"nameTxn\":-1,\"dataTxn\":0},{\"action\":\"whole\",\"ts\":\"2018-01-13T00:00:00.000000Z\",\"startRow\":0,\"rowCount\":23600,\"nameTxn\":-1,\"dataTxn\":3}],\"columnMetaData\":[{\"name\":\"z\",\"type\":\"DOUBLE\",\"index\":false,\"indexCapacity\":256}],\"columnMetaIndex\":[{\"action\":\"add\",\"fromIndex\":0,\"toIndex\":17}]}",
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
                sink.put(w1.replCreateTableSyncModel(w2.getRawTxnMemory(), w2.getRawMetaMemory(), w2.getRawMetaMemorySize()));
                Assert.assertTrue(w1.isTransactionLogPending());
            }

            TestUtils.assertEquals(
                    "{\"table\":{\"action\":\"keep\",\"dataVersion\":0},\"partitions\":[{\"action\":\"whole\",\"ts\":\"2018-01-13T00:00:00.000000Z\",\"startRow\":0,\"rowCount\":13600,\"nameTxn\":-1,\"dataTxn\":1}]}",
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
                sink.put(w1.replCreateTableSyncModel(w2.getRawTxnMemory(), w2.getRawMetaMemory(), w2.getRawMetaMemorySize()));
                Assert.assertFalse(w1.isTransactionLogPending());
            }

            TestUtils.assertEquals(
                    "{\"table\":{\"action\":\"keep\",\"dataVersion\":0}}",
                    sink
            );
        });
    }

    @Test
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

            compile("alter table x drop column o", sqlExecutionContext);

            engine.releaseAllWriters();
            engine.releaseAllReaders();

            compile("alter table x add column o long256", sqlExecutionContext);

            try (
                    TableWriter w1 = engine.getWriter(sqlExecutionContext.getCairoSecurityContext(), "x", "log test");
                    TableWriter w2 = engine.getWriter(sqlExecutionContext.getCairoSecurityContext(), "y", "log test")
            ) {
                sink.clear();
                sink.put(w1.replCreateTableSyncModel(w2.getRawTxnMemory(), w2.getRawMetaMemory(), w2.getRawMetaMemorySize()));
                Assert.assertTrue(w1.isTransactionLogPending());
            }

            TestUtils.assertEquals(
                    "{\"table\":{\"action\":\"keep\",\"dataVersion\":0},\"partitions\":[{\"action\":\"append\",\"ts\":\"2018-01-12T00:00:00.000000Z\",\"startRow\":22400,\"rowCount\":6400,\"nameTxn\":-1,\"dataTxn\":0},{\"action\":\"whole\",\"ts\":\"2018-01-13T00:00:00.000000Z\",\"startRow\":0,\"rowCount\":13600,\"nameTxn\":-1,\"dataTxn\":0}],\"columnMetaData\":[{\"name\":\"z\",\"type\":\"DOUBLE\",\"index\":false,\"indexCapacity\":256}],\"columnMetaIndex\":[{\"action\":\"add\",\"fromIndex\":0,\"toIndex\":17}]}",
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

            compile("alter table x drop column j", sqlExecutionContext);

            try (
                    TableWriter w1 = engine.getWriter(sqlExecutionContext.getCairoSecurityContext(), "x", "log test");
                    TableWriter w2 = engine.getWriter(sqlExecutionContext.getCairoSecurityContext(), "y", "log test")
            ) {
                sink.clear();
                sink.put(w1.replCreateTableSyncModel(w2.getRawTxnMemory(), w2.getRawMetaMemory(), w2.getRawMetaMemorySize()));
                Assert.assertTrue(w1.isTransactionLogPending());
            }

            TestUtils.assertEquals(
                    "{\"table\":{\"action\":\"keep\",\"dataVersion\":0},\"partitions\":[{\"action\":\"append\",\"ts\":\"2018-01-12T00:00:00.000000Z\",\"startRow\":22400,\"rowCount\":6400,\"nameTxn\":-1,\"dataTxn\":0},{\"action\":\"whole\",\"ts\":\"2018-01-13T00:00:00.000000Z\",\"startRow\":0,\"rowCount\":13600,\"nameTxn\":-1,\"dataTxn\":2}],\"columnMetaIndex\":[{\"action\":\"remove\",\"fromIndex\":11,\"toIndex\":-1},{\"action\":\"move\",\"fromIndex\":12,\"toIndex\":11},{\"action\":\"move\",\"fromIndex\":13,\"toIndex\":12},{\"action\":\"move\",\"fromIndex\":14,\"toIndex\":13},{\"action\":\"move\",\"fromIndex\":15,\"toIndex\":14},{\"action\":\"move\",\"fromIndex\":16,\"toIndex\":15}]}",
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
                sink.put(w1.replCreateTableSyncModel(w2.getRawTxnMemory(), w2.getRawMetaMemory(), w2.getRawMetaMemorySize()));
                Assert.assertTrue(w1.isTransactionLogPending());
            }

            TestUtils.assertEquals(
                    "{\"table\":{\"action\":\"truncate\",\"dataVersion\":1},\"partitions\":[{\"action\":\"whole\",\"ts\":\"2018-01-10T00:00:00.000000Z\",\"startRow\":0,\"rowCount\":28800,\"nameTxn\":-1,\"dataTxn\":2},{\"action\":\"whole\",\"ts\":\"2018-01-11T00:00:00.000000Z\",\"startRow\":0,\"rowCount\":28800,\"nameTxn\":-1,\"dataTxn\":2},{\"action\":\"whole\",\"ts\":\"2018-01-12T00:00:00.000000Z\",\"startRow\":0,\"rowCount\":28800,\"nameTxn\":-1,\"dataTxn\":2},{\"action\":\"whole\",\"ts\":\"2018-01-13T00:00:00.000000Z\",\"startRow\":0,\"rowCount\":13600,\"nameTxn\":-1,\"dataTxn\":3}]}",
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
                sink.put(w1.replCreateTableSyncModel(w2.getRawTxnMemory(), w2.getRawMetaMemory(), w2.getRawMetaMemorySize()));
                Assert.assertTrue(w1.isTransactionLogPending());
            }

            TestUtils.assertEquals(
                    "{\"table\":{\"action\":\"truncate\",\"dataVersion\":1}}",
                    sink
            );
        });
    }

    private void createDataOrderedAddColumnTop() throws SqlException {
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

        compile("alter table x add column z double", sqlExecutionContext);

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
                        " timestamp_sequence(to_timestamp('2018-01-13T23:00:00', 'yyyy-MM-ddTHH:mm:ss'), 30000) k," +
                        " rnd_byte(2,50) l," +
                        " rnd_bin(10, 20, 2) m," +
                        " rnd_str(5,16,2) n," +
                        " rnd_long256() o," +
                        " rnd_double() z" +
                        " from long_sequence(10000)",
                sqlExecutionContext
        );
    }

    private void sendAndAssertSync(
            long masterId,
            TableWriter w2,
            SimpleLocalClient client,
            long slaveIP,
            long sequence
    ) {
        client.publishSyncCmd(
                "x",
                masterId,
                w2,
                slaveIP,
                sequence
        );

        engine.tick();

        sink.clear();
        TableSyncModel model = client.consumeSyncModel();
        Assert.assertNotNull(model);
        sink.put(model);

        TestUtils.assertEquals(
                "{\"table\":{\"action\":\"keep\",\"dataVersion\":0},\"partitions\":[{\"action\":\"append\",\"ts\":\"2018-01-13T00:00:00.000000Z\",\"startRow\":13600,\"rowCount\":10000,\"nameTxn\":-1,\"dataTxn\":2}]}",
                sink
        );
    }

    private static class SimpleLocalClient implements Closeable {
        private final Sequence cmdPubSeq;
        private final RingQueue<TableWriterTask> cmdQueue;
        private final RingQueue<TableWriterTask> evtQueue;
        private final Sequence evtSubSeq;
        private final FanOut evtFanOut;

        public SimpleLocalClient(CairoEngine engine) {
            this.cmdPubSeq = engine.getMessageBus().getTableWriterCommandPubSeq();
            this.cmdQueue = engine.getMessageBus().getTableWriterCommandQueue();
            this.evtQueue = engine.getMessageBus().getTableWriterEventQueue();

            // consume event from bus and make sure it is what we expect
            evtFanOut = engine.getMessageBus().getTableWriterEventFanOut();
            // our sequence
            evtSubSeq = new SCSequence(evtFanOut.current(), null);
            evtFanOut.and(evtSubSeq);
        }

        @Override
        public void close() {
            evtFanOut.remove(evtSubSeq);
        }

        public TableSyncModel consumeSyncModel() {
            long cursor = evtSubSeq.next();
            if (cursor > -1) {
                try {
                    TableWriterTask event = evtQueue.get(cursor);
                    TableSyncModel model = new TableSyncModel();
                    model.fromBinary(event.getData());
                    return model;
                } finally {
                    evtSubSeq.done(cursor);
                }
            }
            return null;
        }

        public boolean publishSyncCmd(String tableName, long tableId, TableWriter slave, long slaveIP, long sequence) {
            long cursor = cmdPubSeq.next();
            if (cursor > -1) {
                TableWriterTask task = cmdQueue.get(cursor);
                final long txMem = slave.getRawTxnMemory();
                task.fromSlaveSyncRequest(
                        // we need to know master table ID from master's writer because
                        // we are simulating replication from table X to table Y on the same database
                        // In real world slave will have the same ID as master
                        tableId,
                        tableName,
                        txMem,
                        TableUtils.getTxMemorySize(txMem),
                        slave.getRawMetaMemory(),
                        slave.getRawMetaMemorySize(),
                        slaveIP,
                        sequence
                );

                cmdPubSeq.done(cursor);
                return true;
            }
            return false;
        }
    }
}
