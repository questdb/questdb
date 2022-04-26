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

package io.questdb.griffin;

import io.questdb.cairo.*;
import io.questdb.cairo.security.AllowAllCairoSecurityContext;
import io.questdb.cairo.vm.Vm;
import io.questdb.cairo.vm.api.MemoryMARW;
import io.questdb.mp.FanOut;
import io.questdb.mp.RingQueue;
import io.questdb.mp.SCSequence;
import io.questdb.mp.Sequence;
import io.questdb.network.Net;
import io.questdb.std.Os;
import io.questdb.std.Rnd;
import io.questdb.std.str.Path;
import io.questdb.tasks.TableWriterTask;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

import java.io.Closeable;

import static org.junit.Assert.assertEquals;

public class ReplModelReconTest extends AbstractGriffinTest {

    private final SCSequence eventSubSequence = new SCSequence();
    @Override
    public void setUp() {
        super.setUp();
        CairoConfiguration.RANDOM.set(new Rnd());
    }

    @Test
    public void testInitialModelToTable() throws Exception {
        assertMemoryLeak(() -> {
            compile(
                    "create table x as  " +
                            "(select" +
                            " timestamp_sequence(to_timestamp('2018-01-10', 'yyyy-MM-dd'), 3000000) k" +
                            " from long_sequence(100000)" +
                            ") timestamp(k) partition by DAY",
                    sqlExecutionContext
            );
            try (

                    TableWriter w1 = engine.getWriter(sqlExecutionContext.getCairoSecurityContext(), "x", "log test");
                    MemoryMARW mem = Vm.getMARWInstance();
                    Path path = new Path();
            ) {
                TableSyncModel model = w1.createInitialSyncModel();

                //rename the table to avoid conflict
                model.setTableName("y");

                engine.createTable(sqlExecutionContext.getCairoSecurityContext(), mem, path, model);
                int status = engine.getStatus(sqlExecutionContext.getCairoSecurityContext(), path, model.getTableName());
                assertEquals(TableUtils.TABLE_EXISTS, status);
            }
        });
    }

    @Test
    public void testInitialModelContainsTableNameAndColumns() throws Exception {
        assertMemoryLeak(() -> {
            compile(
                    "create table x as  " +
                            "(select" +
                            " timestamp_sequence(to_timestamp('2018-01-10', 'yyyy-MM-dd'), 3000000) k" +
                            " from long_sequence(100000)" +
                            ") timestamp(k) partition by DAY",
                    sqlExecutionContext
            );

            try (
                    TableWriter w1 = engine.getWriter(sqlExecutionContext.getCairoSecurityContext(), "x", "log test");
            ) {
                TableSyncModel model = w1.createInitialSyncModel();
                assertEquals("x", model.getTableName());
                sink.clear();
                sink.put(model);
                TestUtils.assertEquals(
                        "{\"table\":" +
                                "{\"action\":\"keep\",\"dataVersion\":0,maxTimestamp:\"1970-01-01T00:00:00.000000Z\"}," +
                                "\"columnMetaData\":[{\"name\":\"k\",\"type\":\"TIMESTAMP\",\"hash\":4729996258992366,\"index\":false,\"indexCapacity\":0}]," +
                                "\"columnMetaIndex\":[{\"action\":\"add\",\"fromIndex\":0,\"toIndex\":0}]}",
                        sink
                );

            }
        });
    }

    @Test
    public void testColumnNameCaseSensitivity() throws Exception {
        assertMemoryLeak(() -> {
            compile(
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

            compile("create table y as (" +
                    "select i, sym, amt, timestamp, b, c, d, e, f, g G, ik IK, j, k K, l, m, n, o from x" +
                    ") timestamp(k) partition by DAY", sqlExecutionContext);

            compile("insert into x " +
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
                sink.put(w1.replCreateTableSyncModel(w2.getRawTxnMemory(), w2.getRawTxnMemorySize(), w2.getRawMetaMemory(), w2.getRawMetaMemorySize(), w2.getRawColumnVersionsMemory(), w2.getRawColumnVersionsMemorySize()));
            }

            TestUtils.assertEquals(
                    "{\"table\":{\"action\":\"keep\",\"dataVersion\":0,maxTimestamp:\"2018-01-13T11:19:57.000000Z\"},\"varColumns\":[{\"ts\":\"2018-01-09T00:00:00.000000Z\",\"index\":5,\"size\":56526},{\"ts\":\"2018-01-09T00:00:00.000000Z\",\"index\":14,\"size\":181399},{\"ts\":\"2018-01-09T00:00:00.000000Z\",\"index\":15,\"size\":215132}],\"partitions\":[{\"action\":\"whole\",\"ts\":\"2018-01-09T00:00:00.000000Z\",\"startRow\":0,\"rowCount\":10000,\"nameTxn\":-1,\"columnVersion\":-1}]}",
                    sink
            );
        });
    }

    @Test
    public void testO3PartitionInsertedBefore() throws Exception {
        assertMemoryLeak(() -> {
            compile(
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

            compile("create table y as (x) timestamp(k) partition by DAY", sqlExecutionContext);

            compile("insert into x " +
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
                sink.put(w1.replCreateTableSyncModel(w2.getRawTxnMemory(), w2.getRawTxnMemorySize(), w2.getRawMetaMemory(), w2.getRawMetaMemorySize(), w2.getRawColumnVersionsMemory(), w2.getRawColumnVersionsMemorySize()));
            }

            TestUtils.assertEquals(
                    "{\"table\":{\"action\":\"keep\",\"dataVersion\":0,maxTimestamp:\"2018-01-13T11:19:57.000000Z\"},\"varColumns\":[{\"ts\":\"2018-01-09T00:00:00.000000Z\",\"index\":5,\"size\":56526},{\"ts\":\"2018-01-09T00:00:00.000000Z\",\"index\":14,\"size\":181399},{\"ts\":\"2018-01-09T00:00:00.000000Z\",\"index\":15,\"size\":215132}],\"partitions\":[{\"action\":\"whole\",\"ts\":\"2018-01-09T00:00:00.000000Z\",\"startRow\":0,\"rowCount\":10000,\"nameTxn\":-1,\"columnVersion\":-1}]}",
                    sink
            );
        });
    }

    @Test
    public void testO3PartitionMutates() throws Exception {
        assertMemoryLeak(() -> {
            compile(
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

            compile("create table y as (x) timestamp(k) partition by DAY", sqlExecutionContext);

            compile("insert into x " +
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
                sink.put(w1.replCreateTableSyncModel(w2.getRawTxnMemory(), w2.getRawTxnMemorySize(), w2.getRawMetaMemory(), w2.getRawMetaMemorySize(), w2.getRawColumnVersionsMemory(), w2.getRawColumnVersionsMemorySize()));
            }

            TestUtils.assertEquals(
                    "{\"table\":{\"action\":\"keep\",\"dataVersion\":0,maxTimestamp:\"2018-01-13T11:19:57.000000Z\"},\"varColumns\":[{\"ts\":\"2018-01-11T00:00:00.000000Z\",\"index\":5,\"size\":219590},{\"ts\":\"2018-01-11T00:00:00.000000Z\",\"index\":14,\"size\":699021},{\"ts\":\"2018-01-11T00:00:00.000000Z\",\"index\":15,\"size\":835104}],\"partitions\":[{\"action\":\"whole\",\"ts\":\"2018-01-11T00:00:00.000000Z\",\"startRow\":0,\"rowCount\":38800,\"nameTxn\":1,\"columnVersion\":-1}]}",
                    sink
            );
        });
    }

    @Test
    public void testO3TriggerTransactionLog() throws Exception {
        assertMemoryLeak(() -> {
            compile(
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

            compile("create table y as (x) timestamp(k) partition by DAY", sqlExecutionContext);

            compile("create table chunk1 as (" +
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

            compile("insert into x select * from chunk1", sqlExecutionContext);

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
            compile(
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

            compile(
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

            compile("insert batch 20000 commitLag 3d into x select * from chunk2", sqlExecutionContext);
            compile("insert into x select * from chunk3", sqlExecutionContext);

            // we should now have transaction log, which can be retrieved via function call
            // if we copy new chunk, which was outside of transaction log and transaction log itself into 'y'
            // we would expect to end up with the same 'x' and 'y'

            // todo: here we should assert that "x" content has been delivered to slave table
            //     we do not yet have this facility
        });
    }

    @Test
    public void testOrderedAcrossPartition() throws Exception {
        assertMemoryLeak(() -> {
            compile(
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

            compile("create table y as (select * from x limit 80000) timestamp(k) partition by DAY", sqlExecutionContext);

            try (
                    TableWriter w1 = engine.getWriter(sqlExecutionContext.getCairoSecurityContext(), "x", "log test");
                    TableWriter w2 = engine.getWriter(sqlExecutionContext.getCairoSecurityContext(), "y", "log test")
            ) {
                sink.clear();
                sink.put(w1.replCreateTableSyncModel(w2.getRawTxnMemory(), w2.getRawTxnMemorySize(), w2.getRawMetaMemory(), w2.getRawMetaMemorySize(), w2.getRawColumnVersionsMemory(), w2.getRawColumnVersionsMemorySize()));
            }

            TestUtils.assertEquals(
                    "{\"table\":{\"action\":\"keep\",\"dataVersion\":0,maxTimestamp:\"2018-01-13T11:19:57.000000Z\"},\"varColumns\":[{\"ts\":\"2018-01-12T00:00:00.000000Z\",\"index\":5,\"size\":163194},{\"ts\":\"2018-01-12T00:00:00.000000Z\",\"index\":14,\"size\":518900},{\"ts\":\"2018-01-12T00:00:00.000000Z\",\"index\":15,\"size\":618176},{\"ts\":\"2018-01-13T00:00:00.000000Z\",\"index\":5,\"size\":77070},{\"ts\":\"2018-01-13T00:00:00.000000Z\",\"index\":14,\"size\":244928},{\"ts\":\"2018-01-13T00:00:00.000000Z\",\"index\":15,\"size\":292504}],\"partitions\":[{\"action\":\"append\",\"ts\":\"2018-01-12T00:00:00.000000Z\",\"startRow\":22400,\"rowCount\":6400,\"nameTxn\":-1,\"columnVersion\":-1},{\"action\":\"whole\",\"ts\":\"2018-01-13T00:00:00.000000Z\",\"startRow\":0,\"rowCount\":13600,\"nameTxn\":-1,\"columnVersion\":0}]}",
                    sink
            );
        });
    }

    @Test
    public void testOrderedAddColumn() throws Exception {
        assertMemoryLeak(() -> {
            compile(
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

            compile("create table y as (select * from x limit 80000) timestamp(k) partition by DAY", sqlExecutionContext);

            compile("alter table x add column z double", sqlExecutionContext);
            for (int i = 0; i < 10; i++) {
                compile("alter table x drop column z", sqlExecutionContext);
                compile("alter table x add column z double", sqlExecutionContext);
            }

            try (
                    TableWriter w1 = engine.getWriter(sqlExecutionContext.getCairoSecurityContext(), "x", "log test");
                    TableWriter w2 = engine.getWriter(sqlExecutionContext.getCairoSecurityContext(), "y", "log test")
            ) {
                sink.clear();
                sink.put(w1.replCreateTableSyncModel(w2.getRawTxnMemory(), w2.getRawTxnMemorySize(), w2.getRawMetaMemory(), w2.getRawMetaMemorySize(), w2.getRawColumnVersionsMemory(), w2.getRawColumnVersionsMemorySize()));
            }

            TestUtils.assertEquals(
                    "{\"table\":{\"action\":\"keep\",\"dataVersion\":0,maxTimestamp:\"2018-01-13T11:19:57.000000Z\"},\"columnVersions\":[{\"ts\":\"2018-01-13T00:00:00.000000Z\",\"index\":27,\"nameTxn\":21,\"top\":13600}],\"varColumns\":[{\"ts\":\"2018-01-12T00:00:00.000000Z\",\"index\":5,\"size\":163194},{\"ts\":\"2018-01-12T00:00:00.000000Z\",\"index\":14,\"size\":518900},{\"ts\":\"2018-01-12T00:00:00.000000Z\",\"index\":15,\"size\":618176},{\"ts\":\"2018-01-13T00:00:00.000000Z\",\"index\":5,\"size\":77070},{\"ts\":\"2018-01-13T00:00:00.000000Z\",\"index\":14,\"size\":244928},{\"ts\":\"2018-01-13T00:00:00.000000Z\",\"index\":15,\"size\":292504}],\"partitions\":[{\"action\":\"append\",\"ts\":\"2018-01-12T00:00:00.000000Z\",\"startRow\":22400,\"rowCount\":6400,\"nameTxn\":-1,\"columnVersion\":-1},{\"action\":\"whole\",\"ts\":\"2018-01-13T00:00:00.000000Z\",\"startRow\":0,\"rowCount\":13600,\"nameTxn\":-1,\"columnVersion\":11}],\"columnMetaData\":[{\"name\":\"z\",\"type\":\"DOUBLE\",\"hash\":6820495939660535106,\"index\":false,\"indexCapacity\":256}],\"columnMetaIndex\":[{\"action\":\"add\",\"fromIndex\":0,\"toIndex\":27}]}",
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
                sink.put(w1.replCreateTableSyncModel(w2.getRawTxnMemory(), w2.getRawTxnMemorySize(), w2.getRawMetaMemory(), w2.getRawMetaMemorySize(), w2.getRawColumnVersionsMemory(), w2.getRawColumnVersionsMemorySize()));
            }

            TestUtils.assertEquals(
                    "{\"table\":{\"action\":\"keep\",\"dataVersion\":0,maxTimestamp:\"2018-01-13T23:04:59.970000Z\"},\"columnVersions\":[{\"ts\":\"2018-01-13T00:00:00.000000Z\",\"index\":17,\"nameTxn\":1,\"top\":13600}],\"varColumns\":[{\"ts\":\"2018-01-12T00:00:00.000000Z\",\"index\":5,\"size\":163194},{\"ts\":\"2018-01-12T00:00:00.000000Z\",\"index\":14,\"size\":518900},{\"ts\":\"2018-01-12T00:00:00.000000Z\",\"index\":15,\"size\":618176},{\"ts\":\"2018-01-13T00:00:00.000000Z\",\"index\":5,\"size\":133802},{\"ts\":\"2018-01-13T00:00:00.000000Z\",\"index\":14,\"size\":424521},{\"ts\":\"2018-01-13T00:00:00.000000Z\",\"index\":15,\"size\":507396}],\"partitions\":[{\"action\":\"append\",\"ts\":\"2018-01-12T00:00:00.000000Z\",\"startRow\":22400,\"rowCount\":6400,\"nameTxn\":-1,\"columnVersion\":-1},{\"action\":\"whole\",\"ts\":\"2018-01-13T00:00:00.000000Z\",\"startRow\":0,\"rowCount\":23600,\"nameTxn\":-1,\"columnVersion\":1}],\"columnMetaData\":[{\"name\":\"z\",\"type\":\"DOUBLE\",\"hash\":-3546540271125917157,\"index\":false,\"indexCapacity\":256}],\"columnMetaIndex\":[{\"action\":\"add\",\"fromIndex\":0,\"toIndex\":17}]}",
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

                w1.tick();

                final TableSyncModel model = client.consumeSyncModel();

                Assert.assertNotNull(model);
                sink.clear();
                sink.put(model);
            }

            TestUtils.assertEquals(
                    "{\"table\":{\"action\":\"keep\",\"dataVersion\":0,maxTimestamp:\"2018-01-13T23:04:59.970000Z\"},\"columnVersions\":[{\"ts\":\"2018-01-13T00:00:00.000000Z\",\"index\":17,\"nameTxn\":1,\"top\":13600}],\"varColumns\":[{\"ts\":\"2018-01-12T00:00:00.000000Z\",\"index\":5,\"size\":163194},{\"ts\":\"2018-01-12T00:00:00.000000Z\",\"index\":14,\"size\":518900},{\"ts\":\"2018-01-12T00:00:00.000000Z\",\"index\":15,\"size\":618176},{\"ts\":\"2018-01-13T00:00:00.000000Z\",\"index\":5,\"size\":133802},{\"ts\":\"2018-01-13T00:00:00.000000Z\",\"index\":14,\"size\":424521},{\"ts\":\"2018-01-13T00:00:00.000000Z\",\"index\":15,\"size\":507396}],\"partitions\":[{\"action\":\"append\",\"ts\":\"2018-01-12T00:00:00.000000Z\",\"startRow\":22400,\"rowCount\":6400,\"nameTxn\":-1,\"columnVersion\":-1},{\"action\":\"whole\",\"ts\":\"2018-01-13T00:00:00.000000Z\",\"startRow\":0,\"rowCount\":23600,\"nameTxn\":-1,\"columnVersion\":1}],\"columnMetaData\":[{\"name\":\"z\",\"type\":\"DOUBLE\",\"hash\":-3546540271125917157,\"index\":false,\"indexCapacity\":256}],\"columnMetaIndex\":[{\"action\":\"add\",\"fromIndex\":0,\"toIndex\":17}]}",
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

                final TableSyncModel model = client.consumeSyncModel();
                Assert.assertNotNull(model);
                sink.clear();
                sink.put(model);
            }

            TestUtils.assertEquals(
                    "{\"table\":{\"action\":\"keep\",\"dataVersion\":0,maxTimestamp:\"2018-01-13T23:04:59.970000Z\"},\"columnVersions\":[{\"ts\":\"2018-01-13T00:00:00.000000Z\",\"index\":17,\"nameTxn\":1,\"top\":13600}],\"varColumns\":[{\"ts\":\"2018-01-12T00:00:00.000000Z\",\"index\":5,\"size\":163194},{\"ts\":\"2018-01-12T00:00:00.000000Z\",\"index\":14,\"size\":518900},{\"ts\":\"2018-01-12T00:00:00.000000Z\",\"index\":15,\"size\":618176},{\"ts\":\"2018-01-13T00:00:00.000000Z\",\"index\":5,\"size\":133802},{\"ts\":\"2018-01-13T00:00:00.000000Z\",\"index\":14,\"size\":424521},{\"ts\":\"2018-01-13T00:00:00.000000Z\",\"index\":15,\"size\":507396}],\"partitions\":[{\"action\":\"append\",\"ts\":\"2018-01-12T00:00:00.000000Z\",\"startRow\":22400,\"rowCount\":6400,\"nameTxn\":-1,\"columnVersion\":-1},{\"action\":\"whole\",\"ts\":\"2018-01-13T00:00:00.000000Z\",\"startRow\":0,\"rowCount\":23600,\"nameTxn\":-1,\"columnVersion\":1}],\"columnMetaData\":[{\"name\":\"z\",\"type\":\"DOUBLE\",\"hash\":-3546540271125917157,\"index\":false,\"indexCapacity\":256}],\"columnMetaIndex\":[{\"action\":\"add\",\"fromIndex\":0,\"toIndex\":17}]}",
                    sink
            );
        });
    }

    @Test
    public void testOrderedCleanPartitionBreakOnLast() throws Exception {
        assertMemoryLeak(() -> {
            compile(
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

            compile("create table y as (select * from x where k < '2018-01-13') timestamp(k) partition by DAY", sqlExecutionContext);

            try (
                    TableWriter w1 = engine.getWriter(sqlExecutionContext.getCairoSecurityContext(), "x", "log test");
                    TableWriter w2 = engine.getWriter(sqlExecutionContext.getCairoSecurityContext(), "y", "log test")
            ) {
                sink.clear();
                sink.put(w1.replCreateTableSyncModel(w2.getRawTxnMemory(), w2.getRawTxnMemorySize(), w2.getRawMetaMemory(), w2.getRawMetaMemorySize(), w2.getRawColumnVersionsMemory(), w2.getRawColumnVersionsMemorySize()));
            }

            TestUtils.assertEquals(
                    "{\"table\":{\"action\":\"keep\",\"dataVersion\":0,maxTimestamp:\"2018-01-13T11:19:57.000000Z\"},\"varColumns\":[{\"ts\":\"2018-01-13T00:00:00.000000Z\",\"index\":5,\"size\":77070},{\"ts\":\"2018-01-13T00:00:00.000000Z\",\"index\":14,\"size\":244928},{\"ts\":\"2018-01-13T00:00:00.000000Z\",\"index\":15,\"size\":292504}],\"partitions\":[{\"action\":\"whole\",\"ts\":\"2018-01-13T00:00:00.000000Z\",\"startRow\":0,\"rowCount\":13600,\"nameTxn\":-1,\"columnVersion\":0}]}",
                    sink
            );
        });
    }

    @Test
    public void testOrderedNothingToDo() throws Exception {
        assertMemoryLeak(() -> {
            compile(
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

            compile("create table y as (x) timestamp(k) partition by DAY", sqlExecutionContext);

            try (
                    TableWriter w1 = engine.getWriter(sqlExecutionContext.getCairoSecurityContext(), "x", "log test");
                    TableWriter w2 = engine.getWriter(sqlExecutionContext.getCairoSecurityContext(), "y", "log test")
            ) {
                sink.clear();
                sink.put(w1.replCreateTableSyncModel(w2.getRawTxnMemory(), w2.getRawTxnMemorySize(), w2.getRawMetaMemory(), w2.getRawMetaMemorySize(), w2.getRawColumnVersionsMemory(), w2.getRawColumnVersionsMemorySize()));
            }

            TestUtils.assertEquals(
                    "{\"table\":{\"action\":\"keep\",\"dataVersion\":0,maxTimestamp:\"2018-01-13T11:19:57.000000Z\"}}",
                    sink
            );
        });
    }

    @Test
    public void testOrderedRemoveAndReAddColumnSameNameLast() throws Exception {
        assertMemoryLeak(() -> {
            compile(
                    "create table x as  " +
                            "(select" +
                            " cast(x + 10 as int) i," +
                            " rnd_symbol('msft','ibm', 'googl') sym," +
                            " round(rnd_double(0)*100, 3) amt," +
                            " to_timestamp" +
                            "('2018-01', 'yyyy-MM') + (x + 10) * 720000000 timestamp," +
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

            compile("create table y as (select * from x limit 80000) timestamp(k) partition by DAY", sqlExecutionContext);

            compile("alter table x drop column o", sqlExecutionContext);

            engine.releaseAllWriters();
            engine.releaseAllReaders();

            compile("alter table x add column o long256", sqlExecutionContext);

            try (
                    TableWriter w1 = engine.getWriter(sqlExecutionContext.getCairoSecurityContext(), "x", "log test");
                    TableWriter w2 = engine.getWriter(sqlExecutionContext.getCairoSecurityContext(), "y", "log test")
            ) {
                sink.clear();
                sink.put(w1.replCreateTableSyncModel(w2.getRawTxnMemory(), w2.getRawTxnMemorySize(), w2.getRawMetaMemory(), w2.getRawMetaMemorySize(), w2.getRawColumnVersionsMemory(), w2.getRawColumnVersionsMemorySize()));
            }

            TestUtils.assertEquals(
                    "{\"table\":{\"action\":\"keep\",\"dataVersion\":0,maxTimestamp:\"2018-01-13T11:19:57.000000Z\"},\"columnVersions\":[{\"ts\":\"2018-01-13T00:00:00.000000Z\",\"index\":17,\"nameTxn\":2,\"top\":13600}],\"varColumns\":[{\"ts\":\"2018-01-12T00:00:00.000000Z\",\"index\":5,\"size\":163194},{\"ts\":\"2018-01-12T00:00:00.000000Z\",\"index\":14,\"size\":518900},{\"ts\":\"2018-01-12T00:00:00.000000Z\",\"index\":15,\"size\":618176},{\"ts\":\"2018-01-13T00:00:00.000000Z\",\"index\":5,\"size\":77070},{\"ts\":\"2018-01-13T00:00:00.000000Z\",\"index\":14,\"size\":244928},{\"ts\":\"2018-01-13T00:00:00.000000Z\",\"index\":15,\"size\":292504}],\"partitions\":[{\"action\":\"append\",\"ts\":\"2018-01-12T00:00:00.000000Z\",\"startRow\":22400,\"rowCount\":6400,\"nameTxn\":-1,\"columnVersion\":-1},{\"action\":\"whole\",\"ts\":\"2018-01-13T00:00:00.000000Z\",\"startRow\":0,\"rowCount\":13600,\"nameTxn\":-1,\"columnVersion\":1}],\"columnMetaData\":[{\"name\":\"o\",\"type\":\"LONG256\",\"hash\":-3546540271125917157,\"index\":false,\"indexCapacity\":256}],\"columnMetaIndex\":[{\"action\":\"remove\",\"fromIndex\":16,\"toIndex\":16},{\"action\":\"add\",\"fromIndex\":0,\"toIndex\":17}]}",
                    sink
            );
        });
    }

    @Test
    public void testOrderedRemoveAndReAddColumnSameNameNotLast() throws Exception {
        assertMemoryLeak(() -> {
            compile(
                    "create table x as  " +
                            "(select" +
                            " cast(x + 10 as int) i," +
                            " rnd_symbol('msft','ibm', 'googl') sym," +
                            " round(rnd_double(0)*100, 3) amt," +
                            " to_timestamp" +
                            "('2018-01', 'yyyy-MM') + (x + 10) * 720000000 timestamp," +
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

            compile("create table y as (select * from x limit 80000) timestamp(k) partition by DAY", sqlExecutionContext);

            compile("alter table x drop column n", sqlExecutionContext);

            engine.releaseAllWriters();
            engine.releaseAllReaders();

            compile("alter table x add column n long256", sqlExecutionContext);

            try (
                    TableWriter w1 = engine.getWriter(sqlExecutionContext.getCairoSecurityContext(), "x", "log test");
                    TableWriter w2 = engine.getWriter(sqlExecutionContext.getCairoSecurityContext(), "y", "log test")
            ) {
                sink.clear();
                sink.put(w1.replCreateTableSyncModel(w2.getRawTxnMemory(), w2.getRawTxnMemorySize(), w2.getRawMetaMemory(), w2.getRawMetaMemorySize(), w2.getRawColumnVersionsMemory(), w2.getRawColumnVersionsMemorySize()));
            }

            TestUtils.assertEquals(
                    "{\"table\":{\"action\":\"keep\",\"dataVersion\":0,maxTimestamp:\"2018-01-13T11:19:57.000000Z\"},\"columnVersions\":[{\"ts\":\"2018-01-13T00:00:00.000000Z\",\"index\":17,\"nameTxn\":2,\"top\":13600}],\"varColumns\":[{\"ts\":\"2018-01-12T00:00:00.000000Z\",\"index\":5,\"size\":163194},{\"ts\":\"2018-01-12T00:00:00.000000Z\",\"index\":14,\"size\":518900},{\"ts\":\"2018-01-13T00:00:00.000000Z\",\"index\":5,\"size\":77070},{\"ts\":\"2018-01-13T00:00:00.000000Z\",\"index\":14,\"size\":244928}],\"partitions\":[{\"action\":\"append\",\"ts\":\"2018-01-12T00:00:00.000000Z\",\"startRow\":22400,\"rowCount\":6400,\"nameTxn\":-1,\"columnVersion\":-1},{\"action\":\"whole\",\"ts\":\"2018-01-13T00:00:00.000000Z\",\"startRow\":0,\"rowCount\":13600,\"nameTxn\":-1,\"columnVersion\":1}],\"columnMetaData\":[{\"name\":\"n\",\"type\":\"LONG256\",\"hash\":-3546540271125917157,\"index\":false,\"indexCapacity\":256}],\"columnMetaIndex\":[{\"action\":\"remove\",\"fromIndex\":15,\"toIndex\":15},{\"action\":\"add\",\"fromIndex\":0,\"toIndex\":17}]}",
                    sink
            );
        });
    }

    @Test
    public void testOrderedRemoveAndReAddColumnSameNameLastTableReader() throws Exception {
        // todo: when we remove column X and then add column X to table the current algo is to
        //    attempt to replace the existing file. On windows OS it is only possible to do
        //    when there is no active reader holding old file. This in effect precludes
        //    testing TableReader reloading its columns on Windows. To deal with this situation we
        //    must implement versioning of the metadata. E.g. if we cannot replace column X
        //    we would create column X.[txn] and have reader let go of X and open X.[txn] on reload.
        //    For the time being this test is only possible on non-Windows
        if (Os.type == Os.WINDOWS) {
            return;
        }

        assertMemoryLeak(() -> {
            compile(
                    "create table x as  " +
                            "(select" +
                            " cast(x + 10 as int) i," +
                            " rnd_symbol('msft','ibm', 'googl') sym," +
                            " round(rnd_double(0)*100, 3) amt," +
                            " to_timestamp" +
                            "('2018-01', 'yyyy-MM') + (x + 10) * 720000000 timestamp," +
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
                            " from long_sequence(30)" +
                            ") timestamp(k) partition by DAY",
                    sqlExecutionContext
            );

            TestUtils.assertSql(
                    compiler,
                    sqlExecutionContext,
                    "x",
                    sink,
                    "i\tsym\tamt\ttimestamp\tb\tc\td\te\tf\tg\tik\tj\tk\tl\tm\tn\to\n" +
                            "11\tmsft\t50.938\t2018-01-01T02:12:00.000000Z\tfalse\tU\t0.5243722859289777\t0.8072\t365\t2015-05-02T19:30:57.935Z\t\t-4485747798769957016\t2018-01-10T00:00:00.000000Z\t19\t00000000 19 c4 95 94 36 53 49 b4 59 7e 3b 08 a1 1e\tYSBEOUOJSHRUEDRQ\t0xa937c9ce75e81607a1b56c3d802c47359290f9bc187b0cd2bacd57f41b59057c\n" +
                            "12\tibm\t76.643\t2018-01-01T02:24:00.000000Z\tfalse\tR\tNaN\t0.2459\t993\t\tPEHN\t-5228148654835984711\t2018-01-10T00:00:03.000000Z\t41\t00000000 fb 64 bb 1a d4 f0 2d 40 e2 4b b1 3e e3\tXZOUIC\t0xc009aea26fdde482ba37e200ad5b17cdada00dc8b85c1bc8a5f80be4b45bf437\n" +
                            "13\tmsft\t45.660000000000004\t2018-01-01T02:36:00.000000Z\tfalse\tO\t0.5406709846540508\t0.8164\t470\t2015-01-19T11:40:30.568Z\tVTJW\t-8906871108655466881\t2018-01-10T00:00:06.000000Z\t22\t\tCKYLSUWDSWUGSHOL\t0xde9be4e331fe36e67dc859770af204938151081b8acafaddb0c1415d6f1c1b8d\n" +
                            "14\tgoogl\t28.8\t2018-01-01T02:48:00.000000Z\ttrue\tV\t0.625966045857722\tNaN\t676\t2015-08-02T18:19:40.414Z\tHYRX\t7035958104135945276\t2018-01-10T00:00:09.000000Z\t40\t\tVVSJO\t0x9502128cda0887fe3cdb8640c107a6927eb6d80649d1dfe38e4a7f661df6c32b\n" +
                            "15\tibm\t48.927\t2018-01-01T03:00:00.000000Z\ttrue\tL\t0.053594208204197136\t0.2637\t840\t2015-04-18T11:35:01.041Z\tCPSW\t6108846371653428062\t2018-01-10T00:00:12.000000Z\t9\t00000000 ea c3 c9 73 93 46 fe c2 d3 68 79 8b 43 1d 57 34\tBEZGHWVDKF\t0x3c84de8f7bd9235d6baf9eca859915f55b9e7c38b838ab612accfc7ab9ae2e0b\n" +
                            "16\tibm\t81.897\t2018-01-01T03:12:00.000000Z\tfalse\tI\t0.20585069039325443\t0.2966\t765\t2015-09-29T19:09:06.992Z\tVTJW\t-8857660828600848720\t2018-01-10T00:00:15.000000Z\t40\t00000000 58 7d 24 bc 2e 60 6a 1c 0b 20 a2 86 89 37 11 2c\n" +
                            "00000010 14\tUSZMZVQE\t0xa4cdac45d508383f9c0a1370d099b7237e25b91255572a8f86fd0ebdb6707e47\n" +
                            "17\tgoogl\t10.105\t2018-01-01T03:24:00.000000Z\tfalse\tM\tNaN\t0.3081\t554\t2015-01-20T04:50:34.098Z\tCPSW\t6184401532241477140\t2018-01-10T00:00:18.000000Z\t34\t00000000 eb a3 67 7a 1a 79 e4 35 e4 3a\tIZULIGYVF\t0x695f8ae33a2cc2aa529d71aba0f6fec5172a489c48c269265971448a21302a29\n" +
                            "18\tmsft\t9.619\t2018-01-01T03:36:00.000000Z\ttrue\tH\t0.868788610834602\t0.4915\t285\t2015-03-26T16:53:09.312Z\tHYRX\t6174532314769579955\t2018-01-10T00:00:21.000000Z\t48\t00000000 0b 0c 39 12 f7 05 10 f4 6d f1 e3 ee 58\tLGMXSLUQDYOPHNIM\t0xcfedff23a67d918fb49b3c24e456ad6e6d5992f2da279bf54f1ae0eca85e79fa\n" +
                            "19\tgoogl\t63.35\t2018-01-01T03:48:00.000000Z\tfalse\tP\t0.7707249647497968\tNaN\t162\t2015-02-05T10:14:02.889Z\t\t7046578844650327247\t2018-01-10T00:00:24.000000Z\t47\t\tLEGPUHHIUGGLNYR\t0x36be4fe79117ebd53756b77218c738a7737b1dacd6be597192384aabd888ecb3\n" +
                            "20\tgoogl\t30.049\t2018-01-01T04:00:00.000000Z\tfalse\tV\t0.1511578096923386\t0.1875\t345\t2015-03-04T06:48:42.194Z\tCPSW\t-7069883773042994098\t2018-01-10T00:00:27.000000Z\t47\t00000000 5b e3 71 3d 20 e2 37 f2 64 43\tIZJSVTNP\t0x21dc4adc870c62fe19b2faa4e8255a0d5a9657993732b7a8a68ac79165936544\n" +
                            "21\tgoogl\t75.279\t2018-01-01T04:12:00.000000Z\ttrue\tC\t0.4834201611292943\t0.7943\t28\t2015-06-16T21:00:55.459Z\tHYRX\t6785355388782691241\t2018-01-10T00:00:30.000000Z\t39\t\tRVNGSTEQOD\t0xbabcd0482f05618f926cdd99e63abb35650d1fb462d014df59070392ef6aa389\n" +
                            "22\tmsft\t97.057\t2018-01-01T04:24:00.000000Z\tfalse\tH\t0.11624252077059061\t0.9206\t387\t\t\t-2317221228709139922\t2018-01-10T00:00:33.000000Z\t29\t00000000 94 5f ec d3 dc f8 43 b2 e3 75 62 60 af 6d 8c d8\n" +
                            "00000010 ac c8\tZQSNPXMKJSMKIXEY\t0x49fe08def9f9b91823a6518ae0269fdd71bff9b02ec1c06b908e72465eb96500\n" +
                            "23\tgoogl\t60.505\t2018-01-01T04:36:00.000000Z\tfalse\tW\t0.3549235578142891\t0.0421\t619\t\tCPSW\t-4792110563143960444\t2018-01-10T00:00:36.000000Z\t32\t\tSVIHDWWL\t0xbe37b6a4aa024b2c9ed0238f63d235e0b12591dcaa29570890668cd439eefcba\n" +
                            "24\tgoogl\t15.992\t2018-01-01T04:48:00.000000Z\ttrue\tL\t0.8531407145325477\t0.7299\t280\t2015-11-27T16:24:41.118Z\t\t8231256356538221412\t2018-01-10T00:00:39.000000Z\t13\t\tXFSUWPNXH\t0x30ec2498d018efdd67bf677cfe82f2528ebaf26ca19a89b985e70b46349799fe\n" +
                            "25\tibm\t3.315\t2018-01-01T05:00:00.000000Z\ttrue\t\t0.600707072503926\t0.7398\t663\t2015-08-17T00:23:29.874Z\tVTJW\t8873452284097498126\t2018-01-10T00:00:42.000000Z\t48\t00000000 c5 60 b7 d1 5a 0c e9 db 51 13 4d 59 20 c9 37 a1\n" +
                            "00000010 00\t\t0xcff85f9258847e03a6f2e2a772cd2f3751d822a67dff3d2375166223a6181642\n" +
                            "26\tibm\t15.275\t2018-01-01T05:12:00.000000Z\ttrue\tK\t0.23567419576658333\t0.5714\t919\t\tVTJW\t-6951299253573823571\t2018-01-10T00:00:45.000000Z\t6\t\tUHNRJ\t0x9b1c56d8f84c12a38ca81bb363d7ac4585afb517c8aee023d107b1affff76a2c\n" +
                            "27\tgoogl\t30.062\t2018-01-01T05:24:00.000000Z\ttrue\tE\t0.7783351753890267\t0.3305\t725\t2015-12-22T01:44:08.182Z\t\t8809114770260886433\t2018-01-10T00:00:48.000000Z\t43\t00000000 92 a3 9b e3 cb c2 64 8a b0 35\tBOSEPGIUQZHEISQH\t0x9022a75a739ee488eefa2920026dba88f422dd383e36d838d28649c382dd862c\n" +
                            "28\tibm\t9.82\t2018-01-01T05:36:00.000000Z\ttrue\tQ\tNaN\t0.8616\t248\t\tPEHN\t-5994003889528876849\t2018-01-10T00:00:51.000000Z\t32\t00000000 1d 6c a9 65 81 ad 79 87 fc 92 83\t\t0x5f3f358f3f41ca279c2038f76df9e8329d29ecb3258c00f32fa707df3da8b188\n" +
                            "29\tmsft\t68.161\t2018-01-01T05:48:00.000000Z\tfalse\tX\t0.08109202364673884\t0.2363\t941\t2015-06-28T22:57:49.499Z\tCPSW\t5040581546737547170\t2018-01-10T00:00:54.000000Z\t10\t00000000 f8 f6 78 09 1c 5d 88 f5 52 fd 36 02 50\t\t0x8f5549cf32a4976c5dc373cbc950a49041457ebc5a02a2b542cbd49414e022a0\n" +
                            "30\tibm\t76.363\t2018-01-01T06:00:00.000000Z\tfalse\tL\t0.0966240354078981\tNaN\t925\t2015-11-01T15:54:50.666Z\t\t-1413857400568569799\t2018-01-10T00:00:57.000000Z\t16\t00000000 97 99 d8 77 33 3f b2 67 da 98 47 47 bf 4f\t\t0x600745737c0bd2f67aba89828673d2ed924859b85fa2e348a25fdac663e4945f\n" +
                            "31\tmsft\t45.517\t2018-01-01T06:12:00.000000Z\tfalse\t\t0.6852762111021103\tNaN\t224\t2015-05-19T00:47:18.698Z\tCPSW\t-5553307518930307222\t2018-01-10T00:01:00.000000Z\t37\t\tDNZZBB\t0x691f7c5915477f1d96020f5967ffd19c3dab9feb91ad6a1c1976dd1db6fd213b\n" +
                            "32\tgoogl\t26.161\t2018-01-01T06:24:00.000000Z\ttrue\tO\t0.7020508159399581\tNaN\t906\t2015-12-14T22:20:49.590Z\tVTJW\t-6675915451669544850\t2018-01-10T00:01:03.000000Z\t35\t00000000 2d 04 86 e7 ca 29 98 07 69 ca 5b d6 cf 09 69 01\n" +
                            "00000010 b1 55 38\tCRZUPVQFULMERTP\t0x37f159b0aee619ee437ba368944e80fc63ca88355338f7d63d7be6282dc5fa14\n" +
                            "33\tibm\t73.898\t2018-01-01T06:36:00.000000Z\ttrue\tS\t0.019529452719755813\t0.5331\t912\t2015-02-10T20:59:38.110Z\tCPSW\t-6299635170681612469\t2018-01-10T00:01:06.000000Z\t17\t00000000 f9 17 9e cf 6a 34 2c 37 a3 6f 2a 12 61 3a 9a ad\tLTPKBBQFNPOYNNC\t0xd5bed884b3b4944dc74f56dfb421704074d8eae98ec42624488d5fcab0cbd7a4\n" +
                            "34\tmsft\t69.514\t2018-01-01T06:48:00.000000Z\ttrue\tL\t0.9109198044456538\t0.7573\t231\t2015-05-11T19:35:19.143Z\t\t5490214592036715795\t2018-01-10T00:01:09.000000Z\t35\t\tIGENFEL\t0x6360c99923d254f38f22547ae9661423637e1aaf86ebdc1a96869a9a55de723c\n" +
                            "35\tibm\t63.208\t2018-01-01T07:00:00.000000Z\tfalse\tJ\tNaN\t0.7417\t577\t2015-09-18T08:06:36.078Z\tPEHN\t-5908279002634647771\t2018-01-10T00:01:12.000000Z\t7\t00000000 44 33 6e 00 8e 93 bd 27 42 f8 25 2a 42 71\tKCDNZNL\t0x55d1df30de9b9e118d2ea069ca3c854c8824c1a4d6b282ac4f2f4daeda0e7e7a\n" +
                            "36\tgoogl\t84.964\t2018-01-01T07:12:00.000000Z\tfalse\t\t0.8413721135371649\t0.7361\t1018\t2015-09-22T19:59:24.544Z\tHYRX\t-6053685374103404229\t2018-01-10T00:01:15.000000Z\t20\t00000000 ab ab ac 21 61 99 be 2d f5 30 78 6d 5a 3b\t\t0x5b8def4e7a017e884a3c2c504403708b49fb8d5fe0ff283cbac6499e71ce5b30\n" +
                            "37\tgoogl\t49.608000000000004\t2018-01-01T07:24:00.000000Z\ttrue\tV\t0.9154548873622441\t0.8471\t992\t2015-06-29T08:20:50.572Z\tVTJW\t7270114241818967501\t2018-01-10T00:01:18.000000Z\t25\t00000000 a4 a3 c8 66 0c 40 71 ea 20 7e 43 97 27\tZNVZHCNXZ\t0x52be6ffb7954571d2eef6233c9c8df576e213d545c2611306e6ed811e2548695\n" +
                            "38\tibm\t81.519\t2018-01-01T07:36:00.000000Z\tfalse\tL\t0.8740701330165472\t0.4177\t926\t2015-11-19T04:41:43.768Z\tVTJW\t2010158947254808963\t2018-01-10T00:01:21.000000Z\t45\t00000000 58 3b 4b b7 e2 7f ab 6e 23 03 dd c7 d6\tDRHFBCZI\t0x022f1bcf3743bcd705cd7bfcc309dec560cef557a4907ec5776d02c90b364248\n" +
                            "39\tmsft\t12.934000000000001\t2018-01-01T07:48:00.000000Z\ttrue\tQ\t0.8940422626709261\tNaN\t162\t2015-12-20T18:44:44.021Z\tPEHN\t-7175695171900374773\t2018-01-10T00:01:24.000000Z\t42\t\t\t0xc707cb8de0171211785433a900073b7c220f49362d43ba85361b34a8bfd0d8be\n" +
                            "40\tgoogl\t16.638\t2018-01-01T08:00:00.000000Z\ttrue\t\t0.1810605823104886\t0.9209\t917\t2015-08-05T02:39:14.093Z\tHYRX\t7768501691006807692\t2018-01-10T00:01:27.000000Z\t3\t00000000 c0 55 12 44 dc 4b c0 d9 1c 71 cf 5a 8f 21 06\tMQEHDHQH\t0x1283140ab775531c12aeb931ef2626592440b62ba8918cce31a10a23dc2e0f60\n"
            );

            compile("alter table x drop column o", sqlExecutionContext);
            compile("alter table x add column o long256", sqlExecutionContext);

            TestUtils.assertSql(
                    compiler,
                    sqlExecutionContext,
                    "x",
                    sink,
                    "i\tsym\tamt\ttimestamp\tb\tc\td\te\tf\tg\tik\tj\tk\tl\tm\tn\to\n" +
                            "11\tmsft\t50.938\t2018-01-01T02:12:00.000000Z\tfalse\tU\t0.5243722859289777\t0.8072\t365\t2015-05-02T19:30:57.935Z\t\t-4485747798769957016\t2018-01-10T00:00:00.000000Z\t19\t00000000 19 c4 95 94 36 53 49 b4 59 7e 3b 08 a1 1e\tYSBEOUOJSHRUEDRQ\t\n" +
                            "12\tibm\t76.643\t2018-01-01T02:24:00.000000Z\tfalse\tR\tNaN\t0.2459\t993\t\tPEHN\t-5228148654835984711\t2018-01-10T00:00:03.000000Z\t41\t00000000 fb 64 bb 1a d4 f0 2d 40 e2 4b b1 3e e3\tXZOUIC\t\n" +
                            "13\tmsft\t45.660000000000004\t2018-01-01T02:36:00.000000Z\tfalse\tO\t0.5406709846540508\t0.8164\t470\t2015-01-19T11:40:30.568Z\tVTJW\t-8906871108655466881\t2018-01-10T00:00:06.000000Z\t22\t\tCKYLSUWDSWUGSHOL\t\n" +
                            "14\tgoogl\t28.8\t2018-01-01T02:48:00.000000Z\ttrue\tV\t0.625966045857722\tNaN\t676\t2015-08-02T18:19:40.414Z\tHYRX\t7035958104135945276\t2018-01-10T00:00:09.000000Z\t40\t\tVVSJO\t\n" +
                            "15\tibm\t48.927\t2018-01-01T03:00:00.000000Z\ttrue\tL\t0.053594208204197136\t0.2637\t840\t2015-04-18T11:35:01.041Z\tCPSW\t6108846371653428062\t2018-01-10T00:00:12.000000Z\t9\t00000000 ea c3 c9 73 93 46 fe c2 d3 68 79 8b 43 1d 57 34\tBEZGHWVDKF\t\n" +
                            "16\tibm\t81.897\t2018-01-01T03:12:00.000000Z\tfalse\tI\t0.20585069039325443\t0.2966\t765\t2015-09-29T19:09:06.992Z\tVTJW\t-8857660828600848720\t2018-01-10T00:00:15.000000Z\t40\t00000000 58 7d 24 bc 2e 60 6a 1c 0b 20 a2 86 89 37 11 2c\n" +
                            "00000010 14\tUSZMZVQE\t\n" +
                            "17\tgoogl\t10.105\t2018-01-01T03:24:00.000000Z\tfalse\tM\tNaN\t0.3081\t554\t2015-01-20T04:50:34.098Z\tCPSW\t6184401532241477140\t2018-01-10T00:00:18.000000Z\t34\t00000000 eb a3 67 7a 1a 79 e4 35 e4 3a\tIZULIGYVF\t\n" +
                            "18\tmsft\t9.619\t2018-01-01T03:36:00.000000Z\ttrue\tH\t0.868788610834602\t0.4915\t285\t2015-03-26T16:53:09.312Z\tHYRX\t6174532314769579955\t2018-01-10T00:00:21.000000Z\t48\t00000000 0b 0c 39 12 f7 05 10 f4 6d f1 e3 ee 58\tLGMXSLUQDYOPHNIM\t\n" +
                            "19\tgoogl\t63.35\t2018-01-01T03:48:00.000000Z\tfalse\tP\t0.7707249647497968\tNaN\t162\t2015-02-05T10:14:02.889Z\t\t7046578844650327247\t2018-01-10T00:00:24.000000Z\t47\t\tLEGPUHHIUGGLNYR\t\n" +
                            "20\tgoogl\t30.049\t2018-01-01T04:00:00.000000Z\tfalse\tV\t0.1511578096923386\t0.1875\t345\t2015-03-04T06:48:42.194Z\tCPSW\t-7069883773042994098\t2018-01-10T00:00:27.000000Z\t47\t00000000 5b e3 71 3d 20 e2 37 f2 64 43\tIZJSVTNP\t\n" +
                            "21\tgoogl\t75.279\t2018-01-01T04:12:00.000000Z\ttrue\tC\t0.4834201611292943\t0.7943\t28\t2015-06-16T21:00:55.459Z\tHYRX\t6785355388782691241\t2018-01-10T00:00:30.000000Z\t39\t\tRVNGSTEQOD\t\n" +
                            "22\tmsft\t97.057\t2018-01-01T04:24:00.000000Z\tfalse\tH\t0.11624252077059061\t0.9206\t387\t\t\t-2317221228709139922\t2018-01-10T00:00:33.000000Z\t29\t00000000 94 5f ec d3 dc f8 43 b2 e3 75 62 60 af 6d 8c d8\n" +
                            "00000010 ac c8\tZQSNPXMKJSMKIXEY\t\n" +
                            "23\tgoogl\t60.505\t2018-01-01T04:36:00.000000Z\tfalse\tW\t0.3549235578142891\t0.0421\t619\t\tCPSW\t-4792110563143960444\t2018-01-10T00:00:36.000000Z\t32\t\tSVIHDWWL\t\n" +
                            "24\tgoogl\t15.992\t2018-01-01T04:48:00.000000Z\ttrue\tL\t0.8531407145325477\t0.7299\t280\t2015-11-27T16:24:41.118Z\t\t8231256356538221412\t2018-01-10T00:00:39.000000Z\t13\t\tXFSUWPNXH\t\n" +
                            "25\tibm\t3.315\t2018-01-01T05:00:00.000000Z\ttrue\t\t0.600707072503926\t0.7398\t663\t2015-08-17T00:23:29.874Z\tVTJW\t8873452284097498126\t2018-01-10T00:00:42.000000Z\t48\t00000000 c5 60 b7 d1 5a 0c e9 db 51 13 4d 59 20 c9 37 a1\n" +
                            "00000010 00\t\t\n" +
                            "26\tibm\t15.275\t2018-01-01T05:12:00.000000Z\ttrue\tK\t0.23567419576658333\t0.5714\t919\t\tVTJW\t-6951299253573823571\t2018-01-10T00:00:45.000000Z\t6\t\tUHNRJ\t\n" +
                            "27\tgoogl\t30.062\t2018-01-01T05:24:00.000000Z\ttrue\tE\t0.7783351753890267\t0.3305\t725\t2015-12-22T01:44:08.182Z\t\t8809114770260886433\t2018-01-10T00:00:48.000000Z\t43\t00000000 92 a3 9b e3 cb c2 64 8a b0 35\tBOSEPGIUQZHEISQH\t\n" +
                            "28\tibm\t9.82\t2018-01-01T05:36:00.000000Z\ttrue\tQ\tNaN\t0.8616\t248\t\tPEHN\t-5994003889528876849\t2018-01-10T00:00:51.000000Z\t32\t00000000 1d 6c a9 65 81 ad 79 87 fc 92 83\t\t\n" +
                            "29\tmsft\t68.161\t2018-01-01T05:48:00.000000Z\tfalse\tX\t0.08109202364673884\t0.2363\t941\t2015-06-28T22:57:49.499Z\tCPSW\t5040581546737547170\t2018-01-10T00:00:54.000000Z\t10\t00000000 f8 f6 78 09 1c 5d 88 f5 52 fd 36 02 50\t\t\n" +
                            "30\tibm\t76.363\t2018-01-01T06:00:00.000000Z\tfalse\tL\t0.0966240354078981\tNaN\t925\t2015-11-01T15:54:50.666Z\t\t-1413857400568569799\t2018-01-10T00:00:57.000000Z\t16\t00000000 97 99 d8 77 33 3f b2 67 da 98 47 47 bf 4f\t\t\n" +
                            "31\tmsft\t45.517\t2018-01-01T06:12:00.000000Z\tfalse\t\t0.6852762111021103\tNaN\t224\t2015-05-19T00:47:18.698Z\tCPSW\t-5553307518930307222\t2018-01-10T00:01:00.000000Z\t37\t\tDNZZBB\t\n" +
                            "32\tgoogl\t26.161\t2018-01-01T06:24:00.000000Z\ttrue\tO\t0.7020508159399581\tNaN\t906\t2015-12-14T22:20:49.590Z\tVTJW\t-6675915451669544850\t2018-01-10T00:01:03.000000Z\t35\t00000000 2d 04 86 e7 ca 29 98 07 69 ca 5b d6 cf 09 69 01\n" +
                            "00000010 b1 55 38\tCRZUPVQFULMERTP\t\n" +
                            "33\tibm\t73.898\t2018-01-01T06:36:00.000000Z\ttrue\tS\t0.019529452719755813\t0.5331\t912\t2015-02-10T20:59:38.110Z\tCPSW\t-6299635170681612469\t2018-01-10T00:01:06.000000Z\t17\t00000000 f9 17 9e cf 6a 34 2c 37 a3 6f 2a 12 61 3a 9a ad\tLTPKBBQFNPOYNNC\t\n" +
                            "34\tmsft\t69.514\t2018-01-01T06:48:00.000000Z\ttrue\tL\t0.9109198044456538\t0.7573\t231\t2015-05-11T19:35:19.143Z\t\t5490214592036715795\t2018-01-10T00:01:09.000000Z\t35\t\tIGENFEL\t\n" +
                            "35\tibm\t63.208\t2018-01-01T07:00:00.000000Z\tfalse\tJ\tNaN\t0.7417\t577\t2015-09-18T08:06:36.078Z\tPEHN\t-5908279002634647771\t2018-01-10T00:01:12.000000Z\t7\t00000000 44 33 6e 00 8e 93 bd 27 42 f8 25 2a 42 71\tKCDNZNL\t\n" +
                            "36\tgoogl\t84.964\t2018-01-01T07:12:00.000000Z\tfalse\t\t0.8413721135371649\t0.7361\t1018\t2015-09-22T19:59:24.544Z\tHYRX\t-6053685374103404229\t2018-01-10T00:01:15.000000Z\t20\t00000000 ab ab ac 21 61 99 be 2d f5 30 78 6d 5a 3b\t\t\n" +
                            "37\tgoogl\t49.608000000000004\t2018-01-01T07:24:00.000000Z\ttrue\tV\t0.9154548873622441\t0.8471\t992\t2015-06-29T08:20:50.572Z\tVTJW\t7270114241818967501\t2018-01-10T00:01:18.000000Z\t25\t00000000 a4 a3 c8 66 0c 40 71 ea 20 7e 43 97 27\tZNVZHCNXZ\t\n" +
                            "38\tibm\t81.519\t2018-01-01T07:36:00.000000Z\tfalse\tL\t0.8740701330165472\t0.4177\t926\t2015-11-19T04:41:43.768Z\tVTJW\t2010158947254808963\t2018-01-10T00:01:21.000000Z\t45\t00000000 58 3b 4b b7 e2 7f ab 6e 23 03 dd c7 d6\tDRHFBCZI\t\n" +
                            "39\tmsft\t12.934000000000001\t2018-01-01T07:48:00.000000Z\ttrue\tQ\t0.8940422626709261\tNaN\t162\t2015-12-20T18:44:44.021Z\tPEHN\t-7175695171900374773\t2018-01-10T00:01:24.000000Z\t42\t\t\t\n" +
                            "40\tgoogl\t16.638\t2018-01-01T08:00:00.000000Z\ttrue\t\t0.1810605823104886\t0.9209\t917\t2015-08-05T02:39:14.093Z\tHYRX\t7768501691006807692\t2018-01-10T00:01:27.000000Z\t3\t00000000 c0 55 12 44 dc 4b c0 d9 1c 71 cf 5a 8f 21 06\tMQEHDHQH\t\n"
            );
        });
    }

    @Test
    public void testOrderedRemoveAndReAddColumnSameNameLastTableReaderDiffType() throws Exception {
        // todo: when we remove column X and then add column X to table the current algo is to
        //    attempt to replace the existing file. On windows OS it is only possible to do
        //    when there is no active reader holding old file. This in effect precludes
        //    testing TableReader reloading its columns on Windows. To deal with this situation we
        //    must implement versioning of the metadata. E.g. if we cannot replace column X
        //    we would create column X.[txn] and have reader let go of X and open X.[txn] on reload.
        //    For the time being this test is only possible on non-Windows
        if (Os.type == Os.WINDOWS) {
            return;
        }

        assertMemoryLeak(() -> {
            compile(
                    "create table x as  " +
                            "(select" +
                            " cast(x + 10 as int) i," +
                            " rnd_symbol('msft','ibm', 'googl') sym," +
                            " round(rnd_double(0)*100, 3) amt," +
                            " to_timestamp" +
                            "('2018-01', 'yyyy-MM') + (x + 10) * 720000000 timestamp," +
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
                            " from long_sequence(30)" +
                            ") timestamp(k) partition by DAY",
                    sqlExecutionContext
            );

            TestUtils.assertSql(
                    compiler,
                    sqlExecutionContext,
                    "x",
                    sink,
                    "i\tsym\tamt\ttimestamp\tb\tc\td\te\tf\tg\tik\tj\tk\tl\tm\tn\to\n" +
                            "11\tmsft\t50.938\t2018-01-01T02:12:00.000000Z\tfalse\tU\t0.5243722859289777\t0.8072\t365\t2015-05-02T19:30:57.935Z\t\t-4485747798769957016\t2018-01-10T00:00:00.000000Z\t19\t00000000 19 c4 95 94 36 53 49 b4 59 7e 3b 08 a1 1e\tYSBEOUOJSHRUEDRQ\t0xa937c9ce75e81607a1b56c3d802c47359290f9bc187b0cd2bacd57f41b59057c\n" +
                            "12\tibm\t76.643\t2018-01-01T02:24:00.000000Z\tfalse\tR\tNaN\t0.2459\t993\t\tPEHN\t-5228148654835984711\t2018-01-10T00:00:03.000000Z\t41\t00000000 fb 64 bb 1a d4 f0 2d 40 e2 4b b1 3e e3\tXZOUIC\t0xc009aea26fdde482ba37e200ad5b17cdada00dc8b85c1bc8a5f80be4b45bf437\n" +
                            "13\tmsft\t45.660000000000004\t2018-01-01T02:36:00.000000Z\tfalse\tO\t0.5406709846540508\t0.8164\t470\t2015-01-19T11:40:30.568Z\tVTJW\t-8906871108655466881\t2018-01-10T00:00:06.000000Z\t22\t\tCKYLSUWDSWUGSHOL\t0xde9be4e331fe36e67dc859770af204938151081b8acafaddb0c1415d6f1c1b8d\n" +
                            "14\tgoogl\t28.8\t2018-01-01T02:48:00.000000Z\ttrue\tV\t0.625966045857722\tNaN\t676\t2015-08-02T18:19:40.414Z\tHYRX\t7035958104135945276\t2018-01-10T00:00:09.000000Z\t40\t\tVVSJO\t0x9502128cda0887fe3cdb8640c107a6927eb6d80649d1dfe38e4a7f661df6c32b\n" +
                            "15\tibm\t48.927\t2018-01-01T03:00:00.000000Z\ttrue\tL\t0.053594208204197136\t0.2637\t840\t2015-04-18T11:35:01.041Z\tCPSW\t6108846371653428062\t2018-01-10T00:00:12.000000Z\t9\t00000000 ea c3 c9 73 93 46 fe c2 d3 68 79 8b 43 1d 57 34\tBEZGHWVDKF\t0x3c84de8f7bd9235d6baf9eca859915f55b9e7c38b838ab612accfc7ab9ae2e0b\n" +
                            "16\tibm\t81.897\t2018-01-01T03:12:00.000000Z\tfalse\tI\t0.20585069039325443\t0.2966\t765\t2015-09-29T19:09:06.992Z\tVTJW\t-8857660828600848720\t2018-01-10T00:00:15.000000Z\t40\t00000000 58 7d 24 bc 2e 60 6a 1c 0b 20 a2 86 89 37 11 2c\n" +
                            "00000010 14\tUSZMZVQE\t0xa4cdac45d508383f9c0a1370d099b7237e25b91255572a8f86fd0ebdb6707e47\n" +
                            "17\tgoogl\t10.105\t2018-01-01T03:24:00.000000Z\tfalse\tM\tNaN\t0.3081\t554\t2015-01-20T04:50:34.098Z\tCPSW\t6184401532241477140\t2018-01-10T00:00:18.000000Z\t34\t00000000 eb a3 67 7a 1a 79 e4 35 e4 3a\tIZULIGYVF\t0x695f8ae33a2cc2aa529d71aba0f6fec5172a489c48c269265971448a21302a29\n" +
                            "18\tmsft\t9.619\t2018-01-01T03:36:00.000000Z\ttrue\tH\t0.868788610834602\t0.4915\t285\t2015-03-26T16:53:09.312Z\tHYRX\t6174532314769579955\t2018-01-10T00:00:21.000000Z\t48\t00000000 0b 0c 39 12 f7 05 10 f4 6d f1 e3 ee 58\tLGMXSLUQDYOPHNIM\t0xcfedff23a67d918fb49b3c24e456ad6e6d5992f2da279bf54f1ae0eca85e79fa\n" +
                            "19\tgoogl\t63.35\t2018-01-01T03:48:00.000000Z\tfalse\tP\t0.7707249647497968\tNaN\t162\t2015-02-05T10:14:02.889Z\t\t7046578844650327247\t2018-01-10T00:00:24.000000Z\t47\t\tLEGPUHHIUGGLNYR\t0x36be4fe79117ebd53756b77218c738a7737b1dacd6be597192384aabd888ecb3\n" +
                            "20\tgoogl\t30.049\t2018-01-01T04:00:00.000000Z\tfalse\tV\t0.1511578096923386\t0.1875\t345\t2015-03-04T06:48:42.194Z\tCPSW\t-7069883773042994098\t2018-01-10T00:00:27.000000Z\t47\t00000000 5b e3 71 3d 20 e2 37 f2 64 43\tIZJSVTNP\t0x21dc4adc870c62fe19b2faa4e8255a0d5a9657993732b7a8a68ac79165936544\n" +
                            "21\tgoogl\t75.279\t2018-01-01T04:12:00.000000Z\ttrue\tC\t0.4834201611292943\t0.7943\t28\t2015-06-16T21:00:55.459Z\tHYRX\t6785355388782691241\t2018-01-10T00:00:30.000000Z\t39\t\tRVNGSTEQOD\t0xbabcd0482f05618f926cdd99e63abb35650d1fb462d014df59070392ef6aa389\n" +
                            "22\tmsft\t97.057\t2018-01-01T04:24:00.000000Z\tfalse\tH\t0.11624252077059061\t0.9206\t387\t\t\t-2317221228709139922\t2018-01-10T00:00:33.000000Z\t29\t00000000 94 5f ec d3 dc f8 43 b2 e3 75 62 60 af 6d 8c d8\n" +
                            "00000010 ac c8\tZQSNPXMKJSMKIXEY\t0x49fe08def9f9b91823a6518ae0269fdd71bff9b02ec1c06b908e72465eb96500\n" +
                            "23\tgoogl\t60.505\t2018-01-01T04:36:00.000000Z\tfalse\tW\t0.3549235578142891\t0.0421\t619\t\tCPSW\t-4792110563143960444\t2018-01-10T00:00:36.000000Z\t32\t\tSVIHDWWL\t0xbe37b6a4aa024b2c9ed0238f63d235e0b12591dcaa29570890668cd439eefcba\n" +
                            "24\tgoogl\t15.992\t2018-01-01T04:48:00.000000Z\ttrue\tL\t0.8531407145325477\t0.7299\t280\t2015-11-27T16:24:41.118Z\t\t8231256356538221412\t2018-01-10T00:00:39.000000Z\t13\t\tXFSUWPNXH\t0x30ec2498d018efdd67bf677cfe82f2528ebaf26ca19a89b985e70b46349799fe\n" +
                            "25\tibm\t3.315\t2018-01-01T05:00:00.000000Z\ttrue\t\t0.600707072503926\t0.7398\t663\t2015-08-17T00:23:29.874Z\tVTJW\t8873452284097498126\t2018-01-10T00:00:42.000000Z\t48\t00000000 c5 60 b7 d1 5a 0c e9 db 51 13 4d 59 20 c9 37 a1\n" +
                            "00000010 00\t\t0xcff85f9258847e03a6f2e2a772cd2f3751d822a67dff3d2375166223a6181642\n" +
                            "26\tibm\t15.275\t2018-01-01T05:12:00.000000Z\ttrue\tK\t0.23567419576658333\t0.5714\t919\t\tVTJW\t-6951299253573823571\t2018-01-10T00:00:45.000000Z\t6\t\tUHNRJ\t0x9b1c56d8f84c12a38ca81bb363d7ac4585afb517c8aee023d107b1affff76a2c\n" +
                            "27\tgoogl\t30.062\t2018-01-01T05:24:00.000000Z\ttrue\tE\t0.7783351753890267\t0.3305\t725\t2015-12-22T01:44:08.182Z\t\t8809114770260886433\t2018-01-10T00:00:48.000000Z\t43\t00000000 92 a3 9b e3 cb c2 64 8a b0 35\tBOSEPGIUQZHEISQH\t0x9022a75a739ee488eefa2920026dba88f422dd383e36d838d28649c382dd862c\n" +
                            "28\tibm\t9.82\t2018-01-01T05:36:00.000000Z\ttrue\tQ\tNaN\t0.8616\t248\t\tPEHN\t-5994003889528876849\t2018-01-10T00:00:51.000000Z\t32\t00000000 1d 6c a9 65 81 ad 79 87 fc 92 83\t\t0x5f3f358f3f41ca279c2038f76df9e8329d29ecb3258c00f32fa707df3da8b188\n" +
                            "29\tmsft\t68.161\t2018-01-01T05:48:00.000000Z\tfalse\tX\t0.08109202364673884\t0.2363\t941\t2015-06-28T22:57:49.499Z\tCPSW\t5040581546737547170\t2018-01-10T00:00:54.000000Z\t10\t00000000 f8 f6 78 09 1c 5d 88 f5 52 fd 36 02 50\t\t0x8f5549cf32a4976c5dc373cbc950a49041457ebc5a02a2b542cbd49414e022a0\n" +
                            "30\tibm\t76.363\t2018-01-01T06:00:00.000000Z\tfalse\tL\t0.0966240354078981\tNaN\t925\t2015-11-01T15:54:50.666Z\t\t-1413857400568569799\t2018-01-10T00:00:57.000000Z\t16\t00000000 97 99 d8 77 33 3f b2 67 da 98 47 47 bf 4f\t\t0x600745737c0bd2f67aba89828673d2ed924859b85fa2e348a25fdac663e4945f\n" +
                            "31\tmsft\t45.517\t2018-01-01T06:12:00.000000Z\tfalse\t\t0.6852762111021103\tNaN\t224\t2015-05-19T00:47:18.698Z\tCPSW\t-5553307518930307222\t2018-01-10T00:01:00.000000Z\t37\t\tDNZZBB\t0x691f7c5915477f1d96020f5967ffd19c3dab9feb91ad6a1c1976dd1db6fd213b\n" +
                            "32\tgoogl\t26.161\t2018-01-01T06:24:00.000000Z\ttrue\tO\t0.7020508159399581\tNaN\t906\t2015-12-14T22:20:49.590Z\tVTJW\t-6675915451669544850\t2018-01-10T00:01:03.000000Z\t35\t00000000 2d 04 86 e7 ca 29 98 07 69 ca 5b d6 cf 09 69 01\n" +
                            "00000010 b1 55 38\tCRZUPVQFULMERTP\t0x37f159b0aee619ee437ba368944e80fc63ca88355338f7d63d7be6282dc5fa14\n" +
                            "33\tibm\t73.898\t2018-01-01T06:36:00.000000Z\ttrue\tS\t0.019529452719755813\t0.5331\t912\t2015-02-10T20:59:38.110Z\tCPSW\t-6299635170681612469\t2018-01-10T00:01:06.000000Z\t17\t00000000 f9 17 9e cf 6a 34 2c 37 a3 6f 2a 12 61 3a 9a ad\tLTPKBBQFNPOYNNC\t0xd5bed884b3b4944dc74f56dfb421704074d8eae98ec42624488d5fcab0cbd7a4\n" +
                            "34\tmsft\t69.514\t2018-01-01T06:48:00.000000Z\ttrue\tL\t0.9109198044456538\t0.7573\t231\t2015-05-11T19:35:19.143Z\t\t5490214592036715795\t2018-01-10T00:01:09.000000Z\t35\t\tIGENFEL\t0x6360c99923d254f38f22547ae9661423637e1aaf86ebdc1a96869a9a55de723c\n" +
                            "35\tibm\t63.208\t2018-01-01T07:00:00.000000Z\tfalse\tJ\tNaN\t0.7417\t577\t2015-09-18T08:06:36.078Z\tPEHN\t-5908279002634647771\t2018-01-10T00:01:12.000000Z\t7\t00000000 44 33 6e 00 8e 93 bd 27 42 f8 25 2a 42 71\tKCDNZNL\t0x55d1df30de9b9e118d2ea069ca3c854c8824c1a4d6b282ac4f2f4daeda0e7e7a\n" +
                            "36\tgoogl\t84.964\t2018-01-01T07:12:00.000000Z\tfalse\t\t0.8413721135371649\t0.7361\t1018\t2015-09-22T19:59:24.544Z\tHYRX\t-6053685374103404229\t2018-01-10T00:01:15.000000Z\t20\t00000000 ab ab ac 21 61 99 be 2d f5 30 78 6d 5a 3b\t\t0x5b8def4e7a017e884a3c2c504403708b49fb8d5fe0ff283cbac6499e71ce5b30\n" +
                            "37\tgoogl\t49.608000000000004\t2018-01-01T07:24:00.000000Z\ttrue\tV\t0.9154548873622441\t0.8471\t992\t2015-06-29T08:20:50.572Z\tVTJW\t7270114241818967501\t2018-01-10T00:01:18.000000Z\t25\t00000000 a4 a3 c8 66 0c 40 71 ea 20 7e 43 97 27\tZNVZHCNXZ\t0x52be6ffb7954571d2eef6233c9c8df576e213d545c2611306e6ed811e2548695\n" +
                            "38\tibm\t81.519\t2018-01-01T07:36:00.000000Z\tfalse\tL\t0.8740701330165472\t0.4177\t926\t2015-11-19T04:41:43.768Z\tVTJW\t2010158947254808963\t2018-01-10T00:01:21.000000Z\t45\t00000000 58 3b 4b b7 e2 7f ab 6e 23 03 dd c7 d6\tDRHFBCZI\t0x022f1bcf3743bcd705cd7bfcc309dec560cef557a4907ec5776d02c90b364248\n" +
                            "39\tmsft\t12.934000000000001\t2018-01-01T07:48:00.000000Z\ttrue\tQ\t0.8940422626709261\tNaN\t162\t2015-12-20T18:44:44.021Z\tPEHN\t-7175695171900374773\t2018-01-10T00:01:24.000000Z\t42\t\t\t0xc707cb8de0171211785433a900073b7c220f49362d43ba85361b34a8bfd0d8be\n" +
                            "40\tgoogl\t16.638\t2018-01-01T08:00:00.000000Z\ttrue\t\t0.1810605823104886\t0.9209\t917\t2015-08-05T02:39:14.093Z\tHYRX\t7768501691006807692\t2018-01-10T00:01:27.000000Z\t3\t00000000 c0 55 12 44 dc 4b c0 d9 1c 71 cf 5a 8f 21 06\tMQEHDHQH\t0x1283140ab775531c12aeb931ef2626592440b62ba8918cce31a10a23dc2e0f60\n"
            );

            compile("alter table x drop column o", sqlExecutionContext);
            compile("alter table x add column o int", sqlExecutionContext);

            TestUtils.assertSql(
                    compiler,
                    sqlExecutionContext,
                    "x",
                    sink,
                    "i\tsym\tamt\ttimestamp\tb\tc\td\te\tf\tg\tik\tj\tk\tl\tm\tn\to\n" +
                            "11\tmsft\t50.938\t2018-01-01T02:12:00.000000Z\tfalse\tU\t0.5243722859289777\t0.8072\t365\t2015-05-02T19:30:57.935Z\t\t-4485747798769957016\t2018-01-10T00:00:00.000000Z\t19\t00000000 19 c4 95 94 36 53 49 b4 59 7e 3b 08 a1 1e\tYSBEOUOJSHRUEDRQ\tNaN\n" +
                            "12\tibm\t76.643\t2018-01-01T02:24:00.000000Z\tfalse\tR\tNaN\t0.2459\t993\t\tPEHN\t-5228148654835984711\t2018-01-10T00:00:03.000000Z\t41\t00000000 fb 64 bb 1a d4 f0 2d 40 e2 4b b1 3e e3\tXZOUIC\tNaN\n" +
                            "13\tmsft\t45.660000000000004\t2018-01-01T02:36:00.000000Z\tfalse\tO\t0.5406709846540508\t0.8164\t470\t2015-01-19T11:40:30.568Z\tVTJW\t-8906871108655466881\t2018-01-10T00:00:06.000000Z\t22\t\tCKYLSUWDSWUGSHOL\tNaN\n" +
                            "14\tgoogl\t28.8\t2018-01-01T02:48:00.000000Z\ttrue\tV\t0.625966045857722\tNaN\t676\t2015-08-02T18:19:40.414Z\tHYRX\t7035958104135945276\t2018-01-10T00:00:09.000000Z\t40\t\tVVSJO\tNaN\n" +
                            "15\tibm\t48.927\t2018-01-01T03:00:00.000000Z\ttrue\tL\t0.053594208204197136\t0.2637\t840\t2015-04-18T11:35:01.041Z\tCPSW\t6108846371653428062\t2018-01-10T00:00:12.000000Z\t9\t00000000 ea c3 c9 73 93 46 fe c2 d3 68 79 8b 43 1d 57 34\tBEZGHWVDKF\tNaN\n" +
                            "16\tibm\t81.897\t2018-01-01T03:12:00.000000Z\tfalse\tI\t0.20585069039325443\t0.2966\t765\t2015-09-29T19:09:06.992Z\tVTJW\t-8857660828600848720\t2018-01-10T00:00:15.000000Z\t40\t00000000 58 7d 24 bc 2e 60 6a 1c 0b 20 a2 86 89 37 11 2c\n" +
                            "00000010 14\tUSZMZVQE\tNaN\n" +
                            "17\tgoogl\t10.105\t2018-01-01T03:24:00.000000Z\tfalse\tM\tNaN\t0.3081\t554\t2015-01-20T04:50:34.098Z\tCPSW\t6184401532241477140\t2018-01-10T00:00:18.000000Z\t34\t00000000 eb a3 67 7a 1a 79 e4 35 e4 3a\tIZULIGYVF\tNaN\n" +
                            "18\tmsft\t9.619\t2018-01-01T03:36:00.000000Z\ttrue\tH\t0.868788610834602\t0.4915\t285\t2015-03-26T16:53:09.312Z\tHYRX\t6174532314769579955\t2018-01-10T00:00:21.000000Z\t48\t00000000 0b 0c 39 12 f7 05 10 f4 6d f1 e3 ee 58\tLGMXSLUQDYOPHNIM\tNaN\n" +
                            "19\tgoogl\t63.35\t2018-01-01T03:48:00.000000Z\tfalse\tP\t0.7707249647497968\tNaN\t162\t2015-02-05T10:14:02.889Z\t\t7046578844650327247\t2018-01-10T00:00:24.000000Z\t47\t\tLEGPUHHIUGGLNYR\tNaN\n" +
                            "20\tgoogl\t30.049\t2018-01-01T04:00:00.000000Z\tfalse\tV\t0.1511578096923386\t0.1875\t345\t2015-03-04T06:48:42.194Z\tCPSW\t-7069883773042994098\t2018-01-10T00:00:27.000000Z\t47\t00000000 5b e3 71 3d 20 e2 37 f2 64 43\tIZJSVTNP\tNaN\n" +
                            "21\tgoogl\t75.279\t2018-01-01T04:12:00.000000Z\ttrue\tC\t0.4834201611292943\t0.7943\t28\t2015-06-16T21:00:55.459Z\tHYRX\t6785355388782691241\t2018-01-10T00:00:30.000000Z\t39\t\tRVNGSTEQOD\tNaN\n" +
                            "22\tmsft\t97.057\t2018-01-01T04:24:00.000000Z\tfalse\tH\t0.11624252077059061\t0.9206\t387\t\t\t-2317221228709139922\t2018-01-10T00:00:33.000000Z\t29\t00000000 94 5f ec d3 dc f8 43 b2 e3 75 62 60 af 6d 8c d8\n" +
                            "00000010 ac c8\tZQSNPXMKJSMKIXEY\tNaN\n" +
                            "23\tgoogl\t60.505\t2018-01-01T04:36:00.000000Z\tfalse\tW\t0.3549235578142891\t0.0421\t619\t\tCPSW\t-4792110563143960444\t2018-01-10T00:00:36.000000Z\t32\t\tSVIHDWWL\tNaN\n" +
                            "24\tgoogl\t15.992\t2018-01-01T04:48:00.000000Z\ttrue\tL\t0.8531407145325477\t0.7299\t280\t2015-11-27T16:24:41.118Z\t\t8231256356538221412\t2018-01-10T00:00:39.000000Z\t13\t\tXFSUWPNXH\tNaN\n" +
                            "25\tibm\t3.315\t2018-01-01T05:00:00.000000Z\ttrue\t\t0.600707072503926\t0.7398\t663\t2015-08-17T00:23:29.874Z\tVTJW\t8873452284097498126\t2018-01-10T00:00:42.000000Z\t48\t00000000 c5 60 b7 d1 5a 0c e9 db 51 13 4d 59 20 c9 37 a1\n" +
                            "00000010 00\t\tNaN\n" +
                            "26\tibm\t15.275\t2018-01-01T05:12:00.000000Z\ttrue\tK\t0.23567419576658333\t0.5714\t919\t\tVTJW\t-6951299253573823571\t2018-01-10T00:00:45.000000Z\t6\t\tUHNRJ\tNaN\n" +
                            "27\tgoogl\t30.062\t2018-01-01T05:24:00.000000Z\ttrue\tE\t0.7783351753890267\t0.3305\t725\t2015-12-22T01:44:08.182Z\t\t8809114770260886433\t2018-01-10T00:00:48.000000Z\t43\t00000000 92 a3 9b e3 cb c2 64 8a b0 35\tBOSEPGIUQZHEISQH\tNaN\n" +
                            "28\tibm\t9.82\t2018-01-01T05:36:00.000000Z\ttrue\tQ\tNaN\t0.8616\t248\t\tPEHN\t-5994003889528876849\t2018-01-10T00:00:51.000000Z\t32\t00000000 1d 6c a9 65 81 ad 79 87 fc 92 83\t\tNaN\n" +
                            "29\tmsft\t68.161\t2018-01-01T05:48:00.000000Z\tfalse\tX\t0.08109202364673884\t0.2363\t941\t2015-06-28T22:57:49.499Z\tCPSW\t5040581546737547170\t2018-01-10T00:00:54.000000Z\t10\t00000000 f8 f6 78 09 1c 5d 88 f5 52 fd 36 02 50\t\tNaN\n" +
                            "30\tibm\t76.363\t2018-01-01T06:00:00.000000Z\tfalse\tL\t0.0966240354078981\tNaN\t925\t2015-11-01T15:54:50.666Z\t\t-1413857400568569799\t2018-01-10T00:00:57.000000Z\t16\t00000000 97 99 d8 77 33 3f b2 67 da 98 47 47 bf 4f\t\tNaN\n" +
                            "31\tmsft\t45.517\t2018-01-01T06:12:00.000000Z\tfalse\t\t0.6852762111021103\tNaN\t224\t2015-05-19T00:47:18.698Z\tCPSW\t-5553307518930307222\t2018-01-10T00:01:00.000000Z\t37\t\tDNZZBB\tNaN\n" +
                            "32\tgoogl\t26.161\t2018-01-01T06:24:00.000000Z\ttrue\tO\t0.7020508159399581\tNaN\t906\t2015-12-14T22:20:49.590Z\tVTJW\t-6675915451669544850\t2018-01-10T00:01:03.000000Z\t35\t00000000 2d 04 86 e7 ca 29 98 07 69 ca 5b d6 cf 09 69 01\n" +
                            "00000010 b1 55 38\tCRZUPVQFULMERTP\tNaN\n" +
                            "33\tibm\t73.898\t2018-01-01T06:36:00.000000Z\ttrue\tS\t0.019529452719755813\t0.5331\t912\t2015-02-10T20:59:38.110Z\tCPSW\t-6299635170681612469\t2018-01-10T00:01:06.000000Z\t17\t00000000 f9 17 9e cf 6a 34 2c 37 a3 6f 2a 12 61 3a 9a ad\tLTPKBBQFNPOYNNC\tNaN\n" +
                            "34\tmsft\t69.514\t2018-01-01T06:48:00.000000Z\ttrue\tL\t0.9109198044456538\t0.7573\t231\t2015-05-11T19:35:19.143Z\t\t5490214592036715795\t2018-01-10T00:01:09.000000Z\t35\t\tIGENFEL\tNaN\n" +
                            "35\tibm\t63.208\t2018-01-01T07:00:00.000000Z\tfalse\tJ\tNaN\t0.7417\t577\t2015-09-18T08:06:36.078Z\tPEHN\t-5908279002634647771\t2018-01-10T00:01:12.000000Z\t7\t00000000 44 33 6e 00 8e 93 bd 27 42 f8 25 2a 42 71\tKCDNZNL\tNaN\n" +
                            "36\tgoogl\t84.964\t2018-01-01T07:12:00.000000Z\tfalse\t\t0.8413721135371649\t0.7361\t1018\t2015-09-22T19:59:24.544Z\tHYRX\t-6053685374103404229\t2018-01-10T00:01:15.000000Z\t20\t00000000 ab ab ac 21 61 99 be 2d f5 30 78 6d 5a 3b\t\tNaN\n" +
                            "37\tgoogl\t49.608000000000004\t2018-01-01T07:24:00.000000Z\ttrue\tV\t0.9154548873622441\t0.8471\t992\t2015-06-29T08:20:50.572Z\tVTJW\t7270114241818967501\t2018-01-10T00:01:18.000000Z\t25\t00000000 a4 a3 c8 66 0c 40 71 ea 20 7e 43 97 27\tZNVZHCNXZ\tNaN\n" +
                            "38\tibm\t81.519\t2018-01-01T07:36:00.000000Z\tfalse\tL\t0.8740701330165472\t0.4177\t926\t2015-11-19T04:41:43.768Z\tVTJW\t2010158947254808963\t2018-01-10T00:01:21.000000Z\t45\t00000000 58 3b 4b b7 e2 7f ab 6e 23 03 dd c7 d6\tDRHFBCZI\tNaN\n" +
                            "39\tmsft\t12.934000000000001\t2018-01-01T07:48:00.000000Z\ttrue\tQ\t0.8940422626709261\tNaN\t162\t2015-12-20T18:44:44.021Z\tPEHN\t-7175695171900374773\t2018-01-10T00:01:24.000000Z\t42\t\t\tNaN\n" +
                            "40\tgoogl\t16.638\t2018-01-01T08:00:00.000000Z\ttrue\t\t0.1810605823104886\t0.9209\t917\t2015-08-05T02:39:14.093Z\tHYRX\t7768501691006807692\t2018-01-10T00:01:27.000000Z\t3\t00000000 c0 55 12 44 dc 4b c0 d9 1c 71 cf 5a 8f 21 06\tMQEHDHQH\tNaN\n"
            );
        });
    }

    @Test
    public void testOrderedRemoveAndReAddColumnSameNameLastTableReaderMovePos() throws Exception {
        // todo: when we remove column X and then add column X to table the current algo is to
        //    attempt to replace the existing file. On windows OS it is only possible to do
        //    when there is no active reader holding old file. This in effect precludes
        //    testing TableReader reloading its columns on Windows. To deal with this situation we
        //    must implement versioning of the metadata. E.g. if we cannot replace column X
        //    we would create column X.[txn] and have reader let go of X and open X.[txn] on reload.
        //    For the time being this test is only possible on non-Windows
        if (Os.type == Os.WINDOWS) {
            return;
        }

        assertMemoryLeak(() -> {
            compile(
                    "create table x as  " +
                            "(select" +
                            " cast(x + 10 as int) i," +
                            " rnd_symbol('msft','ibm', 'googl') sym," +
                            " round(rnd_double(0)*100, 3) amt," +
                            " to_timestamp" +
                            "('2018-01', 'yyyy-MM') + (x + 10) * 720000000 timestamp," +
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
                            " from long_sequence(30)" +
                            ") timestamp(k) partition by DAY",
                    sqlExecutionContext
            );

            TestUtils.assertSql(
                    compiler,
                    sqlExecutionContext,
                    "x",
                    sink,
                    "i\tsym\tamt\ttimestamp\tb\tc\td\te\tf\tg\tik\tj\tk\tl\tm\tn\to\n" +
                            "11\tmsft\t50.938\t2018-01-01T02:12:00.000000Z\tfalse\tU\t0.5243722859289777\t0.8072\t365\t2015-05-02T19:30:57.935Z\t\t-4485747798769957016\t2018-01-10T00:00:00.000000Z\t19\t00000000 19 c4 95 94 36 53 49 b4 59 7e 3b 08 a1 1e\tYSBEOUOJSHRUEDRQ\t0xa937c9ce75e81607a1b56c3d802c47359290f9bc187b0cd2bacd57f41b59057c\n" +
                            "12\tibm\t76.643\t2018-01-01T02:24:00.000000Z\tfalse\tR\tNaN\t0.2459\t993\t\tPEHN\t-5228148654835984711\t2018-01-10T00:00:03.000000Z\t41\t00000000 fb 64 bb 1a d4 f0 2d 40 e2 4b b1 3e e3\tXZOUIC\t0xc009aea26fdde482ba37e200ad5b17cdada00dc8b85c1bc8a5f80be4b45bf437\n" +
                            "13\tmsft\t45.660000000000004\t2018-01-01T02:36:00.000000Z\tfalse\tO\t0.5406709846540508\t0.8164\t470\t2015-01-19T11:40:30.568Z\tVTJW\t-8906871108655466881\t2018-01-10T00:00:06.000000Z\t22\t\tCKYLSUWDSWUGSHOL\t0xde9be4e331fe36e67dc859770af204938151081b8acafaddb0c1415d6f1c1b8d\n" +
                            "14\tgoogl\t28.8\t2018-01-01T02:48:00.000000Z\ttrue\tV\t0.625966045857722\tNaN\t676\t2015-08-02T18:19:40.414Z\tHYRX\t7035958104135945276\t2018-01-10T00:00:09.000000Z\t40\t\tVVSJO\t0x9502128cda0887fe3cdb8640c107a6927eb6d80649d1dfe38e4a7f661df6c32b\n" +
                            "15\tibm\t48.927\t2018-01-01T03:00:00.000000Z\ttrue\tL\t0.053594208204197136\t0.2637\t840\t2015-04-18T11:35:01.041Z\tCPSW\t6108846371653428062\t2018-01-10T00:00:12.000000Z\t9\t00000000 ea c3 c9 73 93 46 fe c2 d3 68 79 8b 43 1d 57 34\tBEZGHWVDKF\t0x3c84de8f7bd9235d6baf9eca859915f55b9e7c38b838ab612accfc7ab9ae2e0b\n" +
                            "16\tibm\t81.897\t2018-01-01T03:12:00.000000Z\tfalse\tI\t0.20585069039325443\t0.2966\t765\t2015-09-29T19:09:06.992Z\tVTJW\t-8857660828600848720\t2018-01-10T00:00:15.000000Z\t40\t00000000 58 7d 24 bc 2e 60 6a 1c 0b 20 a2 86 89 37 11 2c\n" +
                            "00000010 14\tUSZMZVQE\t0xa4cdac45d508383f9c0a1370d099b7237e25b91255572a8f86fd0ebdb6707e47\n" +
                            "17\tgoogl\t10.105\t2018-01-01T03:24:00.000000Z\tfalse\tM\tNaN\t0.3081\t554\t2015-01-20T04:50:34.098Z\tCPSW\t6184401532241477140\t2018-01-10T00:00:18.000000Z\t34\t00000000 eb a3 67 7a 1a 79 e4 35 e4 3a\tIZULIGYVF\t0x695f8ae33a2cc2aa529d71aba0f6fec5172a489c48c269265971448a21302a29\n" +
                            "18\tmsft\t9.619\t2018-01-01T03:36:00.000000Z\ttrue\tH\t0.868788610834602\t0.4915\t285\t2015-03-26T16:53:09.312Z\tHYRX\t6174532314769579955\t2018-01-10T00:00:21.000000Z\t48\t00000000 0b 0c 39 12 f7 05 10 f4 6d f1 e3 ee 58\tLGMXSLUQDYOPHNIM\t0xcfedff23a67d918fb49b3c24e456ad6e6d5992f2da279bf54f1ae0eca85e79fa\n" +
                            "19\tgoogl\t63.35\t2018-01-01T03:48:00.000000Z\tfalse\tP\t0.7707249647497968\tNaN\t162\t2015-02-05T10:14:02.889Z\t\t7046578844650327247\t2018-01-10T00:00:24.000000Z\t47\t\tLEGPUHHIUGGLNYR\t0x36be4fe79117ebd53756b77218c738a7737b1dacd6be597192384aabd888ecb3\n" +
                            "20\tgoogl\t30.049\t2018-01-01T04:00:00.000000Z\tfalse\tV\t0.1511578096923386\t0.1875\t345\t2015-03-04T06:48:42.194Z\tCPSW\t-7069883773042994098\t2018-01-10T00:00:27.000000Z\t47\t00000000 5b e3 71 3d 20 e2 37 f2 64 43\tIZJSVTNP\t0x21dc4adc870c62fe19b2faa4e8255a0d5a9657993732b7a8a68ac79165936544\n" +
                            "21\tgoogl\t75.279\t2018-01-01T04:12:00.000000Z\ttrue\tC\t0.4834201611292943\t0.7943\t28\t2015-06-16T21:00:55.459Z\tHYRX\t6785355388782691241\t2018-01-10T00:00:30.000000Z\t39\t\tRVNGSTEQOD\t0xbabcd0482f05618f926cdd99e63abb35650d1fb462d014df59070392ef6aa389\n" +
                            "22\tmsft\t97.057\t2018-01-01T04:24:00.000000Z\tfalse\tH\t0.11624252077059061\t0.9206\t387\t\t\t-2317221228709139922\t2018-01-10T00:00:33.000000Z\t29\t00000000 94 5f ec d3 dc f8 43 b2 e3 75 62 60 af 6d 8c d8\n" +
                            "00000010 ac c8\tZQSNPXMKJSMKIXEY\t0x49fe08def9f9b91823a6518ae0269fdd71bff9b02ec1c06b908e72465eb96500\n" +
                            "23\tgoogl\t60.505\t2018-01-01T04:36:00.000000Z\tfalse\tW\t0.3549235578142891\t0.0421\t619\t\tCPSW\t-4792110563143960444\t2018-01-10T00:00:36.000000Z\t32\t\tSVIHDWWL\t0xbe37b6a4aa024b2c9ed0238f63d235e0b12591dcaa29570890668cd439eefcba\n" +
                            "24\tgoogl\t15.992\t2018-01-01T04:48:00.000000Z\ttrue\tL\t0.8531407145325477\t0.7299\t280\t2015-11-27T16:24:41.118Z\t\t8231256356538221412\t2018-01-10T00:00:39.000000Z\t13\t\tXFSUWPNXH\t0x30ec2498d018efdd67bf677cfe82f2528ebaf26ca19a89b985e70b46349799fe\n" +
                            "25\tibm\t3.315\t2018-01-01T05:00:00.000000Z\ttrue\t\t0.600707072503926\t0.7398\t663\t2015-08-17T00:23:29.874Z\tVTJW\t8873452284097498126\t2018-01-10T00:00:42.000000Z\t48\t00000000 c5 60 b7 d1 5a 0c e9 db 51 13 4d 59 20 c9 37 a1\n" +
                            "00000010 00\t\t0xcff85f9258847e03a6f2e2a772cd2f3751d822a67dff3d2375166223a6181642\n" +
                            "26\tibm\t15.275\t2018-01-01T05:12:00.000000Z\ttrue\tK\t0.23567419576658333\t0.5714\t919\t\tVTJW\t-6951299253573823571\t2018-01-10T00:00:45.000000Z\t6\t\tUHNRJ\t0x9b1c56d8f84c12a38ca81bb363d7ac4585afb517c8aee023d107b1affff76a2c\n" +
                            "27\tgoogl\t30.062\t2018-01-01T05:24:00.000000Z\ttrue\tE\t0.7783351753890267\t0.3305\t725\t2015-12-22T01:44:08.182Z\t\t8809114770260886433\t2018-01-10T00:00:48.000000Z\t43\t00000000 92 a3 9b e3 cb c2 64 8a b0 35\tBOSEPGIUQZHEISQH\t0x9022a75a739ee488eefa2920026dba88f422dd383e36d838d28649c382dd862c\n" +
                            "28\tibm\t9.82\t2018-01-01T05:36:00.000000Z\ttrue\tQ\tNaN\t0.8616\t248\t\tPEHN\t-5994003889528876849\t2018-01-10T00:00:51.000000Z\t32\t00000000 1d 6c a9 65 81 ad 79 87 fc 92 83\t\t0x5f3f358f3f41ca279c2038f76df9e8329d29ecb3258c00f32fa707df3da8b188\n" +
                            "29\tmsft\t68.161\t2018-01-01T05:48:00.000000Z\tfalse\tX\t0.08109202364673884\t0.2363\t941\t2015-06-28T22:57:49.499Z\tCPSW\t5040581546737547170\t2018-01-10T00:00:54.000000Z\t10\t00000000 f8 f6 78 09 1c 5d 88 f5 52 fd 36 02 50\t\t0x8f5549cf32a4976c5dc373cbc950a49041457ebc5a02a2b542cbd49414e022a0\n" +
                            "30\tibm\t76.363\t2018-01-01T06:00:00.000000Z\tfalse\tL\t0.0966240354078981\tNaN\t925\t2015-11-01T15:54:50.666Z\t\t-1413857400568569799\t2018-01-10T00:00:57.000000Z\t16\t00000000 97 99 d8 77 33 3f b2 67 da 98 47 47 bf 4f\t\t0x600745737c0bd2f67aba89828673d2ed924859b85fa2e348a25fdac663e4945f\n" +
                            "31\tmsft\t45.517\t2018-01-01T06:12:00.000000Z\tfalse\t\t0.6852762111021103\tNaN\t224\t2015-05-19T00:47:18.698Z\tCPSW\t-5553307518930307222\t2018-01-10T00:01:00.000000Z\t37\t\tDNZZBB\t0x691f7c5915477f1d96020f5967ffd19c3dab9feb91ad6a1c1976dd1db6fd213b\n" +
                            "32\tgoogl\t26.161\t2018-01-01T06:24:00.000000Z\ttrue\tO\t0.7020508159399581\tNaN\t906\t2015-12-14T22:20:49.590Z\tVTJW\t-6675915451669544850\t2018-01-10T00:01:03.000000Z\t35\t00000000 2d 04 86 e7 ca 29 98 07 69 ca 5b d6 cf 09 69 01\n" +
                            "00000010 b1 55 38\tCRZUPVQFULMERTP\t0x37f159b0aee619ee437ba368944e80fc63ca88355338f7d63d7be6282dc5fa14\n" +
                            "33\tibm\t73.898\t2018-01-01T06:36:00.000000Z\ttrue\tS\t0.019529452719755813\t0.5331\t912\t2015-02-10T20:59:38.110Z\tCPSW\t-6299635170681612469\t2018-01-10T00:01:06.000000Z\t17\t00000000 f9 17 9e cf 6a 34 2c 37 a3 6f 2a 12 61 3a 9a ad\tLTPKBBQFNPOYNNC\t0xd5bed884b3b4944dc74f56dfb421704074d8eae98ec42624488d5fcab0cbd7a4\n" +
                            "34\tmsft\t69.514\t2018-01-01T06:48:00.000000Z\ttrue\tL\t0.9109198044456538\t0.7573\t231\t2015-05-11T19:35:19.143Z\t\t5490214592036715795\t2018-01-10T00:01:09.000000Z\t35\t\tIGENFEL\t0x6360c99923d254f38f22547ae9661423637e1aaf86ebdc1a96869a9a55de723c\n" +
                            "35\tibm\t63.208\t2018-01-01T07:00:00.000000Z\tfalse\tJ\tNaN\t0.7417\t577\t2015-09-18T08:06:36.078Z\tPEHN\t-5908279002634647771\t2018-01-10T00:01:12.000000Z\t7\t00000000 44 33 6e 00 8e 93 bd 27 42 f8 25 2a 42 71\tKCDNZNL\t0x55d1df30de9b9e118d2ea069ca3c854c8824c1a4d6b282ac4f2f4daeda0e7e7a\n" +
                            "36\tgoogl\t84.964\t2018-01-01T07:12:00.000000Z\tfalse\t\t0.8413721135371649\t0.7361\t1018\t2015-09-22T19:59:24.544Z\tHYRX\t-6053685374103404229\t2018-01-10T00:01:15.000000Z\t20\t00000000 ab ab ac 21 61 99 be 2d f5 30 78 6d 5a 3b\t\t0x5b8def4e7a017e884a3c2c504403708b49fb8d5fe0ff283cbac6499e71ce5b30\n" +
                            "37\tgoogl\t49.608000000000004\t2018-01-01T07:24:00.000000Z\ttrue\tV\t0.9154548873622441\t0.8471\t992\t2015-06-29T08:20:50.572Z\tVTJW\t7270114241818967501\t2018-01-10T00:01:18.000000Z\t25\t00000000 a4 a3 c8 66 0c 40 71 ea 20 7e 43 97 27\tZNVZHCNXZ\t0x52be6ffb7954571d2eef6233c9c8df576e213d545c2611306e6ed811e2548695\n" +
                            "38\tibm\t81.519\t2018-01-01T07:36:00.000000Z\tfalse\tL\t0.8740701330165472\t0.4177\t926\t2015-11-19T04:41:43.768Z\tVTJW\t2010158947254808963\t2018-01-10T00:01:21.000000Z\t45\t00000000 58 3b 4b b7 e2 7f ab 6e 23 03 dd c7 d6\tDRHFBCZI\t0x022f1bcf3743bcd705cd7bfcc309dec560cef557a4907ec5776d02c90b364248\n" +
                            "39\tmsft\t12.934000000000001\t2018-01-01T07:48:00.000000Z\ttrue\tQ\t0.8940422626709261\tNaN\t162\t2015-12-20T18:44:44.021Z\tPEHN\t-7175695171900374773\t2018-01-10T00:01:24.000000Z\t42\t\t\t0xc707cb8de0171211785433a900073b7c220f49362d43ba85361b34a8bfd0d8be\n" +
                            "40\tgoogl\t16.638\t2018-01-01T08:00:00.000000Z\ttrue\t\t0.1810605823104886\t0.9209\t917\t2015-08-05T02:39:14.093Z\tHYRX\t7768501691006807692\t2018-01-10T00:01:27.000000Z\t3\t00000000 c0 55 12 44 dc 4b c0 d9 1c 71 cf 5a 8f 21 06\tMQEHDHQH\t0x1283140ab775531c12aeb931ef2626592440b62ba8918cce31a10a23dc2e0f60\n"
            );

            compile("alter table x drop column n", sqlExecutionContext);
            compile("alter table x add column n string", sqlExecutionContext);

            TestUtils.assertSql(
                    compiler,
                    sqlExecutionContext,
                    "x",
                    sink,
                    "i\tsym\tamt\ttimestamp\tb\tc\td\te\tf\tg\tik\tj\tk\tl\tm\to\tn\n" +
                            "11\tmsft\t50.938\t2018-01-01T02:12:00.000000Z\tfalse\tU\t0.5243722859289777\t0.8072\t365\t2015-05-02T19:30:57.935Z\t\t-4485747798769957016\t2018-01-10T00:00:00.000000Z\t19\t00000000 19 c4 95 94 36 53 49 b4 59 7e 3b 08 a1 1e\t0xa937c9ce75e81607a1b56c3d802c47359290f9bc187b0cd2bacd57f41b59057c\t\n" +
                            "12\tibm\t76.643\t2018-01-01T02:24:00.000000Z\tfalse\tR\tNaN\t0.2459\t993\t\tPEHN\t-5228148654835984711\t2018-01-10T00:00:03.000000Z\t41\t00000000 fb 64 bb 1a d4 f0 2d 40 e2 4b b1 3e e3\t0xc009aea26fdde482ba37e200ad5b17cdada00dc8b85c1bc8a5f80be4b45bf437\t\n" +
                            "13\tmsft\t45.660000000000004\t2018-01-01T02:36:00.000000Z\tfalse\tO\t0.5406709846540508\t0.8164\t470\t2015-01-19T11:40:30.568Z\tVTJW\t-8906871108655466881\t2018-01-10T00:00:06.000000Z\t22\t\t0xde9be4e331fe36e67dc859770af204938151081b8acafaddb0c1415d6f1c1b8d\t\n" +
                            "14\tgoogl\t28.8\t2018-01-01T02:48:00.000000Z\ttrue\tV\t0.625966045857722\tNaN\t676\t2015-08-02T18:19:40.414Z\tHYRX\t7035958104135945276\t2018-01-10T00:00:09.000000Z\t40\t\t0x9502128cda0887fe3cdb8640c107a6927eb6d80649d1dfe38e4a7f661df6c32b\t\n" +
                            "15\tibm\t48.927\t2018-01-01T03:00:00.000000Z\ttrue\tL\t0.053594208204197136\t0.2637\t840\t2015-04-18T11:35:01.041Z\tCPSW\t6108846371653428062\t2018-01-10T00:00:12.000000Z\t9\t00000000 ea c3 c9 73 93 46 fe c2 d3 68 79 8b 43 1d 57 34\t0x3c84de8f7bd9235d6baf9eca859915f55b9e7c38b838ab612accfc7ab9ae2e0b\t\n" +
                            "16\tibm\t81.897\t2018-01-01T03:12:00.000000Z\tfalse\tI\t0.20585069039325443\t0.2966\t765\t2015-09-29T19:09:06.992Z\tVTJW\t-8857660828600848720\t2018-01-10T00:00:15.000000Z\t40\t00000000 58 7d 24 bc 2e 60 6a 1c 0b 20 a2 86 89 37 11 2c\n" +
                            "00000010 14\t0xa4cdac45d508383f9c0a1370d099b7237e25b91255572a8f86fd0ebdb6707e47\t\n" +
                            "17\tgoogl\t10.105\t2018-01-01T03:24:00.000000Z\tfalse\tM\tNaN\t0.3081\t554\t2015-01-20T04:50:34.098Z\tCPSW\t6184401532241477140\t2018-01-10T00:00:18.000000Z\t34\t00000000 eb a3 67 7a 1a 79 e4 35 e4 3a\t0x695f8ae33a2cc2aa529d71aba0f6fec5172a489c48c269265971448a21302a29\t\n" +
                            "18\tmsft\t9.619\t2018-01-01T03:36:00.000000Z\ttrue\tH\t0.868788610834602\t0.4915\t285\t2015-03-26T16:53:09.312Z\tHYRX\t6174532314769579955\t2018-01-10T00:00:21.000000Z\t48\t00000000 0b 0c 39 12 f7 05 10 f4 6d f1 e3 ee 58\t0xcfedff23a67d918fb49b3c24e456ad6e6d5992f2da279bf54f1ae0eca85e79fa\t\n" +
                            "19\tgoogl\t63.35\t2018-01-01T03:48:00.000000Z\tfalse\tP\t0.7707249647497968\tNaN\t162\t2015-02-05T10:14:02.889Z\t\t7046578844650327247\t2018-01-10T00:00:24.000000Z\t47\t\t0x36be4fe79117ebd53756b77218c738a7737b1dacd6be597192384aabd888ecb3\t\n" +
                            "20\tgoogl\t30.049\t2018-01-01T04:00:00.000000Z\tfalse\tV\t0.1511578096923386\t0.1875\t345\t2015-03-04T06:48:42.194Z\tCPSW\t-7069883773042994098\t2018-01-10T00:00:27.000000Z\t47\t00000000 5b e3 71 3d 20 e2 37 f2 64 43\t0x21dc4adc870c62fe19b2faa4e8255a0d5a9657993732b7a8a68ac79165936544\t\n" +
                            "21\tgoogl\t75.279\t2018-01-01T04:12:00.000000Z\ttrue\tC\t0.4834201611292943\t0.7943\t28\t2015-06-16T21:00:55.459Z\tHYRX\t6785355388782691241\t2018-01-10T00:00:30.000000Z\t39\t\t0xbabcd0482f05618f926cdd99e63abb35650d1fb462d014df59070392ef6aa389\t\n" +
                            "22\tmsft\t97.057\t2018-01-01T04:24:00.000000Z\tfalse\tH\t0.11624252077059061\t0.9206\t387\t\t\t-2317221228709139922\t2018-01-10T00:00:33.000000Z\t29\t00000000 94 5f ec d3 dc f8 43 b2 e3 75 62 60 af 6d 8c d8\n" +
                            "00000010 ac c8\t0x49fe08def9f9b91823a6518ae0269fdd71bff9b02ec1c06b908e72465eb96500\t\n" +
                            "23\tgoogl\t60.505\t2018-01-01T04:36:00.000000Z\tfalse\tW\t0.3549235578142891\t0.0421\t619\t\tCPSW\t-4792110563143960444\t2018-01-10T00:00:36.000000Z\t32\t\t0xbe37b6a4aa024b2c9ed0238f63d235e0b12591dcaa29570890668cd439eefcba\t\n" +
                            "24\tgoogl\t15.992\t2018-01-01T04:48:00.000000Z\ttrue\tL\t0.8531407145325477\t0.7299\t280\t2015-11-27T16:24:41.118Z\t\t8231256356538221412\t2018-01-10T00:00:39.000000Z\t13\t\t0x30ec2498d018efdd67bf677cfe82f2528ebaf26ca19a89b985e70b46349799fe\t\n" +
                            "25\tibm\t3.315\t2018-01-01T05:00:00.000000Z\ttrue\t\t0.600707072503926\t0.7398\t663\t2015-08-17T00:23:29.874Z\tVTJW\t8873452284097498126\t2018-01-10T00:00:42.000000Z\t48\t00000000 c5 60 b7 d1 5a 0c e9 db 51 13 4d 59 20 c9 37 a1\n" +
                            "00000010 00\t0xcff85f9258847e03a6f2e2a772cd2f3751d822a67dff3d2375166223a6181642\t\n" +
                            "26\tibm\t15.275\t2018-01-01T05:12:00.000000Z\ttrue\tK\t0.23567419576658333\t0.5714\t919\t\tVTJW\t-6951299253573823571\t2018-01-10T00:00:45.000000Z\t6\t\t0x9b1c56d8f84c12a38ca81bb363d7ac4585afb517c8aee023d107b1affff76a2c\t\n" +
                            "27\tgoogl\t30.062\t2018-01-01T05:24:00.000000Z\ttrue\tE\t0.7783351753890267\t0.3305\t725\t2015-12-22T01:44:08.182Z\t\t8809114770260886433\t2018-01-10T00:00:48.000000Z\t43\t00000000 92 a3 9b e3 cb c2 64 8a b0 35\t0x9022a75a739ee488eefa2920026dba88f422dd383e36d838d28649c382dd862c\t\n" +
                            "28\tibm\t9.82\t2018-01-01T05:36:00.000000Z\ttrue\tQ\tNaN\t0.8616\t248\t\tPEHN\t-5994003889528876849\t2018-01-10T00:00:51.000000Z\t32\t00000000 1d 6c a9 65 81 ad 79 87 fc 92 83\t0x5f3f358f3f41ca279c2038f76df9e8329d29ecb3258c00f32fa707df3da8b188\t\n" +
                            "29\tmsft\t68.161\t2018-01-01T05:48:00.000000Z\tfalse\tX\t0.08109202364673884\t0.2363\t941\t2015-06-28T22:57:49.499Z\tCPSW\t5040581546737547170\t2018-01-10T00:00:54.000000Z\t10\t00000000 f8 f6 78 09 1c 5d 88 f5 52 fd 36 02 50\t0x8f5549cf32a4976c5dc373cbc950a49041457ebc5a02a2b542cbd49414e022a0\t\n" +
                            "30\tibm\t76.363\t2018-01-01T06:00:00.000000Z\tfalse\tL\t0.0966240354078981\tNaN\t925\t2015-11-01T15:54:50.666Z\t\t-1413857400568569799\t2018-01-10T00:00:57.000000Z\t16\t00000000 97 99 d8 77 33 3f b2 67 da 98 47 47 bf 4f\t0x600745737c0bd2f67aba89828673d2ed924859b85fa2e348a25fdac663e4945f\t\n" +
                            "31\tmsft\t45.517\t2018-01-01T06:12:00.000000Z\tfalse\t\t0.6852762111021103\tNaN\t224\t2015-05-19T00:47:18.698Z\tCPSW\t-5553307518930307222\t2018-01-10T00:01:00.000000Z\t37\t\t0x691f7c5915477f1d96020f5967ffd19c3dab9feb91ad6a1c1976dd1db6fd213b\t\n" +
                            "32\tgoogl\t26.161\t2018-01-01T06:24:00.000000Z\ttrue\tO\t0.7020508159399581\tNaN\t906\t2015-12-14T22:20:49.590Z\tVTJW\t-6675915451669544850\t2018-01-10T00:01:03.000000Z\t35\t00000000 2d 04 86 e7 ca 29 98 07 69 ca 5b d6 cf 09 69 01\n" +
                            "00000010 b1 55 38\t0x37f159b0aee619ee437ba368944e80fc63ca88355338f7d63d7be6282dc5fa14\t\n" +
                            "33\tibm\t73.898\t2018-01-01T06:36:00.000000Z\ttrue\tS\t0.019529452719755813\t0.5331\t912\t2015-02-10T20:59:38.110Z\tCPSW\t-6299635170681612469\t2018-01-10T00:01:06.000000Z\t17\t00000000 f9 17 9e cf 6a 34 2c 37 a3 6f 2a 12 61 3a 9a ad\t0xd5bed884b3b4944dc74f56dfb421704074d8eae98ec42624488d5fcab0cbd7a4\t\n" +
                            "34\tmsft\t69.514\t2018-01-01T06:48:00.000000Z\ttrue\tL\t0.9109198044456538\t0.7573\t231\t2015-05-11T19:35:19.143Z\t\t5490214592036715795\t2018-01-10T00:01:09.000000Z\t35\t\t0x6360c99923d254f38f22547ae9661423637e1aaf86ebdc1a96869a9a55de723c\t\n" +
                            "35\tibm\t63.208\t2018-01-01T07:00:00.000000Z\tfalse\tJ\tNaN\t0.7417\t577\t2015-09-18T08:06:36.078Z\tPEHN\t-5908279002634647771\t2018-01-10T00:01:12.000000Z\t7\t00000000 44 33 6e 00 8e 93 bd 27 42 f8 25 2a 42 71\t0x55d1df30de9b9e118d2ea069ca3c854c8824c1a4d6b282ac4f2f4daeda0e7e7a\t\n" +
                            "36\tgoogl\t84.964\t2018-01-01T07:12:00.000000Z\tfalse\t\t0.8413721135371649\t0.7361\t1018\t2015-09-22T19:59:24.544Z\tHYRX\t-6053685374103404229\t2018-01-10T00:01:15.000000Z\t20\t00000000 ab ab ac 21 61 99 be 2d f5 30 78 6d 5a 3b\t0x5b8def4e7a017e884a3c2c504403708b49fb8d5fe0ff283cbac6499e71ce5b30\t\n" +
                            "37\tgoogl\t49.608000000000004\t2018-01-01T07:24:00.000000Z\ttrue\tV\t0.9154548873622441\t0.8471\t992\t2015-06-29T08:20:50.572Z\tVTJW\t7270114241818967501\t2018-01-10T00:01:18.000000Z\t25\t00000000 a4 a3 c8 66 0c 40 71 ea 20 7e 43 97 27\t0x52be6ffb7954571d2eef6233c9c8df576e213d545c2611306e6ed811e2548695\t\n" +
                            "38\tibm\t81.519\t2018-01-01T07:36:00.000000Z\tfalse\tL\t0.8740701330165472\t0.4177\t926\t2015-11-19T04:41:43.768Z\tVTJW\t2010158947254808963\t2018-01-10T00:01:21.000000Z\t45\t00000000 58 3b 4b b7 e2 7f ab 6e 23 03 dd c7 d6\t0x022f1bcf3743bcd705cd7bfcc309dec560cef557a4907ec5776d02c90b364248\t\n" +
                            "39\tmsft\t12.934000000000001\t2018-01-01T07:48:00.000000Z\ttrue\tQ\t0.8940422626709261\tNaN\t162\t2015-12-20T18:44:44.021Z\tPEHN\t-7175695171900374773\t2018-01-10T00:01:24.000000Z\t42\t\t0xc707cb8de0171211785433a900073b7c220f49362d43ba85361b34a8bfd0d8be\t\n" +
                            "40\tgoogl\t16.638\t2018-01-01T08:00:00.000000Z\ttrue\t\t0.1810605823104886\t0.9209\t917\t2015-08-05T02:39:14.093Z\tHYRX\t7768501691006807692\t2018-01-10T00:01:27.000000Z\t3\t00000000 c0 55 12 44 dc 4b c0 d9 1c 71 cf 5a 8f 21 06\t0x1283140ab775531c12aeb931ef2626592440b62ba8918cce31a10a23dc2e0f60\t\n"
            );
        });
    }

    @Test
    public void testOrderedRemoveColumn() throws Exception {
        assertMemoryLeak(() -> {
            compile(
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

            compile("create table y as (select * from x limit 80000) timestamp(k) partition by DAY", sqlExecutionContext);

            compile("alter table x drop column j", sqlExecutionContext);

            try (
                    TableWriter w1 = engine.getWriter(sqlExecutionContext.getCairoSecurityContext(), "x", "log test");
                    TableWriter w2 = engine.getWriter(sqlExecutionContext.getCairoSecurityContext(), "y", "log test")
            ) {
                sink.clear();
                sink.put(w1.replCreateTableSyncModel(w2.getRawTxnMemory(), w2.getRawTxnMemorySize(), w2.getRawMetaMemory(), w2.getRawMetaMemorySize(), w2.getRawColumnVersionsMemory(), w2.getRawColumnVersionsMemorySize()));
            }

            TestUtils.assertEquals(
                    "{\"table\":{\"action\":\"keep\",\"dataVersion\":0,maxTimestamp:\"2018-01-13T11:19:57.000000Z\"},\"varColumns\":[{\"ts\":\"2018-01-12T00:00:00.000000Z\",\"index\":5,\"size\":163194},{\"ts\":\"2018-01-12T00:00:00.000000Z\",\"index\":14,\"size\":518900},{\"ts\":\"2018-01-12T00:00:00.000000Z\",\"index\":15,\"size\":618176},{\"ts\":\"2018-01-13T00:00:00.000000Z\",\"index\":5,\"size\":77070},{\"ts\":\"2018-01-13T00:00:00.000000Z\",\"index\":14,\"size\":244928},{\"ts\":\"2018-01-13T00:00:00.000000Z\",\"index\":15,\"size\":292504}],\"partitions\":[{\"action\":\"append\",\"ts\":\"2018-01-12T00:00:00.000000Z\",\"startRow\":22400,\"rowCount\":6400,\"nameTxn\":-1,\"columnVersion\":-1},{\"action\":\"whole\",\"ts\":\"2018-01-13T00:00:00.000000Z\",\"startRow\":0,\"rowCount\":13600,\"nameTxn\":-1,\"columnVersion\":0}],\"columnMetaIndex\":[{\"action\":\"remove\",\"fromIndex\":11,\"toIndex\":11}]}",
                    sink
            );
        });
    }

    @Test
    public void testOrderedTruncateAndReAdd() throws Exception {
        assertMemoryLeak(() -> {
            compile(
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

            compile("create table y as (select * from x) timestamp(k) partition by DAY", sqlExecutionContext);

            compile("truncate table x", sqlExecutionContext);

            compile("insert into x " +
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
                sink.put(w1.replCreateTableSyncModel(w2.getRawTxnMemory(), w2.getRawTxnMemorySize(), w2.getRawMetaMemory(), w2.getRawMetaMemorySize(), w2.getRawColumnVersionsMemory(), w2.getRawColumnVersionsMemorySize()));
            }

            TestUtils.assertEquals(
                    "{\"table\":{\"action\":\"truncate\",\"dataVersion\":1,maxTimestamp:\"2018-01-13T11:19:57.000000Z\"},\"varColumns\":[{\"ts\":\"2018-01-10T00:00:00.000000Z\",\"index\":5,\"size\":163088},{\"ts\":\"2018-01-10T00:00:00.000000Z\",\"index\":14,\"size\":522390},{\"ts\":\"2018-01-10T00:00:00.000000Z\",\"index\":15,\"size\":617994},{\"ts\":\"2018-01-11T00:00:00.000000Z\",\"index\":5,\"size\":163228},{\"ts\":\"2018-01-11T00:00:00.000000Z\",\"index\":14,\"size\":518071},{\"ts\":\"2018-01-11T00:00:00.000000Z\",\"index\":15,\"size\":617786},{\"ts\":\"2018-01-12T00:00:00.000000Z\",\"index\":5,\"size\":163310},{\"ts\":\"2018-01-12T00:00:00.000000Z\",\"index\":14,\"size\":516080},{\"ts\":\"2018-01-12T00:00:00.000000Z\",\"index\":15,\"size\":619090},{\"ts\":\"2018-01-13T00:00:00.000000Z\",\"index\":5,\"size\":77182},{\"ts\":\"2018-01-13T00:00:00.000000Z\",\"index\":14,\"size\":243619},{\"ts\":\"2018-01-13T00:00:00.000000Z\",\"index\":15,\"size\":293052}],\"partitions\":[{\"action\":\"whole\",\"ts\":\"2018-01-10T00:00:00.000000Z\",\"startRow\":0,\"rowCount\":28800,\"nameTxn\":-1,\"columnVersion\":-1},{\"action\":\"whole\",\"ts\":\"2018-01-11T00:00:00.000000Z\",\"startRow\":0,\"rowCount\":28800,\"nameTxn\":-1,\"columnVersion\":-1},{\"action\":\"whole\",\"ts\":\"2018-01-12T00:00:00.000000Z\",\"startRow\":0,\"rowCount\":28800,\"nameTxn\":-1,\"columnVersion\":-1},{\"action\":\"whole\",\"ts\":\"2018-01-13T00:00:00.000000Z\",\"startRow\":0,\"rowCount\":13600,\"nameTxn\":-1,\"columnVersion\":0}]}",
                    sink
            );
        });
    }

    @Test
    public void testOrderedTruncateToEmpty() throws Exception {
        assertMemoryLeak(() -> {
            compile(
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

            compile("create table y as (select * from x) timestamp(k) partition by DAY", sqlExecutionContext);

            compile("truncate table x", sqlExecutionContext);

            try (
                    TableWriter w1 = engine.getWriter(sqlExecutionContext.getCairoSecurityContext(), "x", "log test");
                    TableWriter w2 = engine.getWriter(sqlExecutionContext.getCairoSecurityContext(), "y", "log test")
            ) {
                sink.clear();
                sink.put(w1.replCreateTableSyncModel(w2.getRawTxnMemory(), w2.getRawTxnMemorySize(), w2.getRawMetaMemory(), w2.getRawMetaMemorySize(), w2.getRawColumnVersionsMemory(), w2.getRawColumnVersionsMemorySize()));
            }

            TestUtils.assertEquals(
                    "{\"table\":{\"action\":\"truncate\",\"dataVersion\":1,maxTimestamp:\"\"}}",
                    sink
            );
        });
    }

    @Test
    public void testUpdatePartitionAddColumnAndMutates() throws Exception {
        assertMemoryLeak(() -> {
            compile(
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

            compile("create table y as (x) timestamp(k) partition by DAY", sqlExecutionContext);
            compile("alter table x add column z double", sqlExecutionContext);
            executeUpdate("UPDATE x SET z = -42.999 where k > to_timestamp('2018-01-11', 'yyyy-MM-dd') and k < to_timestamp('2018-01-12', 'yyyy-MM-dd')");

            try (
                    TableWriter w1 = engine.getWriter(sqlExecutionContext.getCairoSecurityContext(), "x", "log test");
                    TableWriter w2 = engine.getWriter(sqlExecutionContext.getCairoSecurityContext(), "y", "log test")
            ) {
                sink.clear();
                sink.put(w1.replCreateTableSyncModel(w2.getRawTxnMemory(), w2.getRawTxnMemorySize(), w2.getRawMetaMemory(), w2.getRawMetaMemorySize(), w2.getRawColumnVersionsMemory(), w2.getRawColumnVersionsMemorySize()));
            }

            TestUtils.assertEquals(
                    "{\"table\":{\"action\":\"keep\",\"dataVersion\":0,maxTimestamp:\"2018-01-13T11:19:57.000000Z\"},\"columnVersions\":[{\"ts\":\"2018-01-11T00:00:00.000000Z\",\"index\":17,\"nameTxn\":2,\"top\":1},{\"ts\":\"2018-01-13T00:00:00.000000Z\",\"index\":17,\"nameTxn\":1,\"top\":13600}],\"partitions\":[{\"action\":\"columns\",\"ts\":\"2018-01-11T00:00:00.000000Z\",\"startRow\":0,\"rowCount\":28800,\"nameTxn\":-1,\"columnVersion\":1},{\"action\":\"columns\",\"ts\":\"2018-01-13T00:00:00.000000Z\",\"startRow\":0,\"rowCount\":13600,\"nameTxn\":-1,\"columnVersion\":2}],\"columnMetaData\":[{\"name\":\"z\",\"type\":\"DOUBLE\",\"hash\":-3546540271125917157,\"index\":false,\"indexCapacity\":256}],\"columnMetaIndex\":[{\"action\":\"add\",\"fromIndex\":0,\"toIndex\":17}]}",
                    sink
            );
        });
    }

    @Test
    public void testUpdatePartitionMutates() throws Exception {
        assertMemoryLeak(() -> {
            compile(
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

            compile("create table y as (x) timestamp(k) partition by DAY", sqlExecutionContext);

            compile("insert into x " +
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

            executeUpdate("UPDATE x SET i = -42, n = 'xyz' where k > to_timestamp('2018-01-12', 'yyyy-MM-dd') and k < to_timestamp('2018-01-13', 'yyyy-MM-dd')");

            compile("insert into x " +
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
                            " timestamp_sequence(to_timestamp('2018-01-13', 'yyyy-MM-dd'), 30000) k," +
                            " rnd_byte(2,50) l," +
                            " rnd_bin(10, 20, 2) m," +
                            " rnd_str(5,16,2) n," +
                            " rnd_long256() o" +
                            " from long_sequence(10000)",
                    sqlExecutionContext
            );

            executeUpdate("UPDATE x SET i = -42, n = 'xyz' where k > to_timestamp('2018-01-13', 'yyyy-MM-dd') and k < to_timestamp('2018-01-14', 'yyyy-MM-dd')");

            try (
                    TableWriter w1 = engine.getWriter(sqlExecutionContext.getCairoSecurityContext(), "x", "log test");
                    TableWriter w2 = engine.getWriter(sqlExecutionContext.getCairoSecurityContext(), "y", "log test")
            ) {
                sink.clear();
                sink.put(w1.replCreateTableSyncModel(w2.getRawTxnMemory(), w2.getRawTxnMemorySize(), w2.getRawMetaMemory(), w2.getRawMetaMemorySize(), w2.getRawColumnVersionsMemory(), w2.getRawColumnVersionsMemorySize()));
            }

            TestUtils.assertEquals(
                    "{\"table\":{\"action\":\"keep\",\"dataVersion\":0,maxTimestamp:\"2018-01-13T11:19:57.000000Z\"},\"columnVersions\":[{\"ts\":\"2018-01-12T00:00:00.000000Z\",\"index\":0,\"nameTxn\":2,\"top\":0},{\"ts\":\"2018-01-12T00:00:00.000000Z\",\"index\":15,\"nameTxn\":2,\"top\":0},{\"ts\":\"2018-01-13T00:00:00.000000Z\",\"index\":0,\"nameTxn\":4,\"top\":0},{\"ts\":\"2018-01-13T00:00:00.000000Z\",\"index\":15,\"nameTxn\":4,\"top\":0}],\"varColumns\":[{\"ts\":\"2018-01-11T00:00:00.000000Z\",\"index\":5,\"size\":219590},{\"ts\":\"2018-01-11T00:00:00.000000Z\",\"index\":14,\"size\":699021},{\"ts\":\"2018-01-11T00:00:00.000000Z\",\"index\":15,\"size\":835104},{\"ts\":\"2018-01-12T00:00:00.000000Z\",\"index\":15,\"size\":288018},{\"ts\":\"2018-01-13T00:00:00.000000Z\",\"index\":5,\"size\":133846},{\"ts\":\"2018-01-13T00:00:00.000000Z\",\"index\":14,\"size\":426045},{\"ts\":\"2018-01-13T00:00:00.000000Z\",\"index\":15,\"size\":236026}],\"partitions\":[{\"action\":\"whole\",\"ts\":\"2018-01-11T00:00:00.000000Z\",\"startRow\":0,\"rowCount\":38800,\"nameTxn\":1,\"columnVersion\":-1},{\"action\":\"columns\",\"ts\":\"2018-01-12T00:00:00.000000Z\",\"startRow\":0,\"rowCount\":28800,\"nameTxn\":-1,\"columnVersion\":0},{\"action\":\"whole\",\"ts\":\"2018-01-13T00:00:00.000000Z\",\"startRow\":0,\"rowCount\":23600,\"nameTxn\":3,\"columnVersion\":2}]}",
                    sink
            );
        });
    }

    @Test
    public void testUpdatePartitionMutatesSameTwice() throws Exception {
        assertMemoryLeak(() -> {
            compile(
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

            compile("create table y as (x) timestamp(k) partition by DAY", sqlExecutionContext);

            executeUpdate("UPDATE x SET i = -42, n = 'xyz' where k > to_timestamp('2018-01-11', 'yyyy-MM-dd') and k < to_timestamp('2018-01-12', 'yyyy-MM-dd')");

            executeUpdate("UPDATE x SET i = -42, n = 'xyz' where k > to_timestamp('2018-01-12', 'yyyy-MM-dd') and k < to_timestamp('2018-01-13', 'yyyy-MM-dd')");
            executeUpdate("UPDATE x SET i = -43, n = 'xyz' where k > to_timestamp('2018-01-12', 'yyyy-MM-dd') and k < to_timestamp('2018-01-13', 'yyyy-MM-dd')");

            try (
                    TableWriter w1 = engine.getWriter(sqlExecutionContext.getCairoSecurityContext(), "x", "log test");
                    TableWriter w2 = engine.getWriter(sqlExecutionContext.getCairoSecurityContext(), "y", "log test")
            ) {
                sink.clear();
                sink.put(w1.replCreateTableSyncModel(w2.getRawTxnMemory(), w2.getRawTxnMemorySize(), w2.getRawMetaMemory(), w2.getRawMetaMemorySize(), w2.getRawColumnVersionsMemory(), w2.getRawColumnVersionsMemorySize()));
            }

            TestUtils.assertEquals(
                    "{\"table\":{\"action\":\"keep\",\"dataVersion\":0,maxTimestamp:\"2018-01-13T11:19:57.000000Z\"},\"columnVersions\":[{\"ts\":\"2018-01-11T00:00:00.000000Z\",\"index\":0,\"nameTxn\":1,\"top\":0},{\"ts\":\"2018-01-11T00:00:00.000000Z\",\"index\":15,\"nameTxn\":1,\"top\":0},{\"ts\":\"2018-01-12T00:00:00.000000Z\",\"index\":0,\"nameTxn\":3,\"top\":0},{\"ts\":\"2018-01-12T00:00:00.000000Z\",\"index\":15,\"nameTxn\":3,\"top\":0}],\"varColumns\":[{\"ts\":\"2018-01-11T00:00:00.000000Z\",\"index\":15,\"size\":288012},{\"ts\":\"2018-01-12T00:00:00.000000Z\",\"index\":15,\"size\":288018}],\"partitions\":[{\"action\":\"columns\",\"ts\":\"2018-01-11T00:00:00.000000Z\",\"startRow\":0,\"rowCount\":28800,\"nameTxn\":-1,\"columnVersion\":0},{\"action\":\"columns\",\"ts\":\"2018-01-12T00:00:00.000000Z\",\"startRow\":0,\"rowCount\":28800,\"nameTxn\":-1,\"columnVersion\":2}]}",
                    sink
            );
        });
    }

    private void createDataOrderedAddColumnTop() throws SqlException {
        compile(
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

        compile("create table y as (select * from x limit 80000) timestamp(k) partition by DAY", sqlExecutionContext);

        compile("alter table x add column z double", sqlExecutionContext);

        compile("insert into x " +
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

    private void executeUpdate(String query) throws SqlException {
        CompiledQuery cc = compiler.compile(query, sqlExecutionContext);
        Assert.assertEquals(CompiledQuery.UPDATE, cc.getType());
        try (QueryFuture qf = cc.execute(eventSubSequence)) {
            qf.await();
        }
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
        w2.tick();

        sink.clear();
        TableSyncModel model = client.consumeSyncModel();
        Assert.assertNotNull(model);
        sink.put(model);

        TestUtils.assertEquals(
                "{\"table\":{\"action\":\"keep\",\"dataVersion\":0,maxTimestamp:\"2018-01-13T14:11:39.970000Z\"},\"varColumns\":[{\"ts\":\"2018-01-13T00:00:00.000000Z\",\"index\":5,\"size\":133596},{\"ts\":\"2018-01-13T00:00:00.000000Z\",\"index\":14,\"size\":426327},{\"ts\":\"2018-01-13T00:00:00.000000Z\",\"index\":15,\"size\":507636}],\"partitions\":[{\"action\":\"append\",\"ts\":\"2018-01-13T00:00:00.000000Z\",\"startRow\":13600,\"rowCount\":10000,\"nameTxn\":-1,\"columnVersion\":0}]}",
                sink
        );
    }

    private static class SimpleLocalClient implements Closeable {
        private final RingQueue<TableWriterTask> evtQueue;
        private final Sequence evtSubSeq;
        private final FanOut evtFanOut;

        public SimpleLocalClient(CairoEngine engine) {
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
            AsyncWriterCommand cmd = new AsyncWriterCommandBase() {
                @Override
                public long apply(TableWriter tableWriter, boolean acceptStructureChange) {
                    throw new RuntimeException("Should not be called for SYNC command");
                }

                @Override
                public void serialize(TableWriterTask task) {
                    task.fromSlaveSyncRequest(
                            // we need to know master table ID from master's writer because
                            // we are simulating replication from table X to table Y on the same database
                            // In real world slave will have the same ID as master
                            tableId,
                            tableName,
                            slave.getRawTxnMemory(),
                            slave.getRawTxnMemorySize(),
                            slave.getRawMetaMemory(),
                            slave.getRawMetaMemorySize(),
                            slave.getRawColumnVersionsMemory(),
                            slave.getRawColumnVersionsMemorySize(),
                            slaveIP,
                            sequence
                    );
                }

                @Override
                public AsyncWriterCommand deserialize(TableWriterTask task) {
                    return this;
                }

                @Override
                public String getCommandName() {
                    return "SYNC";
                }
            };

            try (TableWriter writer = engine.getWriterOrPublishCommand(AllowAllCairoSecurityContext.INSTANCE, tableName, cmd)) {
                if (writer != null) {
                    writer.publishAsyncWriterCommand(cmd);
                }
            }
            return true;
        }
    }
}
