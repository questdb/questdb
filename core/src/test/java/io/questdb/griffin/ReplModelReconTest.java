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
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.griffin.engine.functions.rnd.SharedRandom;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.mp.FanOut;
import io.questdb.mp.RingQueue;
import io.questdb.mp.SCSequence;
import io.questdb.mp.Sequence;
import io.questdb.std.Rnd;
import io.questdb.tasks.TableWriterTask;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Ignore;
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
    @Ignore
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

            try (
                    TableWriter w1 = engine.getWriter(sqlExecutionContext.getCairoSecurityContext(), "x", "log test");
                    TableWriter w2 = engine.getWriter(sqlExecutionContext.getCairoSecurityContext(), "y", "log test")
            ) {
                sink.clear();
                sink.put(w1.replCreateTableSyncModel(w2.getRawTxnMemory(), w2.getRawMetaMemory(), w2.getRawMetaMemorySize()));
                Assert.assertTrue(w1.isTransactionLogPending());
            }

            TestUtils.assertEquals(
                    "{\"table\":{\"action\":\"keep\",\"dataVersion\":0},\"partitions\":[{\"action\":\"append\",\"ts\":\"2018-01-13T00:00:00.000000Z\",\"startRow\":13600,\"rowCount\":10000,\"nameTxn\":-1,\"dataTxn\":2}]}",
                    sink
            );

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
                            " cast(to_timestamp('2018-01-09', 'yyyy-MM-dd') + 30000*10000L + ((x-1)/4) * 1000000L + (4-(x-1)%4)*80009L  as timestamp) ts," +
                            " rnd_byte(2,50) l," +
                            " rnd_bin(10, 20, 2) m," +
                            " rnd_str(5,16,2) n," +
                            " rnd_long256() o" +
                            " from long_sequence(100)" +
                            ")",
                    sqlExecutionContext
            );

            sink.clear();
            TestUtils.printSql(
                    compiler,
                    sqlExecutionContext,
                    "transaction_log('x')",
                    sink
            );

            System.out.println(sink);

//            compiler.compile("insert batch 20000 commitLag 3d into x select * from chunk2", sqlExecutionContext);

            // we should now have transaction log, which can be retrieved via function call
            // if we copy new chunk, which was outside of transaction log and transaction log itself into 'y'
            // we would expect to end up with the same 'x' and 'y'

            compiler.compile("insert into y select * from chunk1", sqlExecutionContext);
//            compiler.compile("insert into y select * from transaction_log('x')", sqlExecutionContext);
//
            TestUtils.assertSqlCursors(
                    compiler,
                    sqlExecutionContext,
                    "x",
                    "y",
                    LOG
            );

            // have table reader recover the transaction log
            try (RecordCursorFactory factory = compiler.compile("transaction_log('x')", sqlExecutionContext).getRecordCursorFactory()) {
                String expected = "i\tsym\tamt\ttimestamp\tb\tc\td\te\tf\tg\tik\tj\tk\tl\tm\tn\to\n" +
                        "11\tmsft\t2.592\t2018-01-01T02:12:00.000000Z\ttrue\tX\tNaN\tNaN\t824\t2015-07-04T15:37:31.425Z\t\t-7846705642525506251\t2018-01-09T00:05:00.240027Z\t43\t00000000 b5 87 19 1f 19 a9 83 4f 55 cd a4 8d 11\tVTTOVYBW\t0x582117b525c52aeb2e4268a0619e593a94aaafeae72c7701eb55c3062ebcf762\n" +
                        "12\tmsft\t46.428000000000004\t2018-01-01T02:24:00.000000Z\tfalse\tT\tNaN\t0.0897\t990\t2015-04-04T04:38:18.678Z\tPGCX\t-4014978642156892922\t1970-01-01T00:00:00.000001Z\t48\t\tVTGEKQQFGUFOKN\t0x36c8854948d5a63502fc7ba2f09ef1b4584f54e7d4ec39bfa8dcb338ed9a4b29\n" +
                        "13\tgoogl\t50.388\t2018-01-01T02:36:00.000000Z\ttrue\tX\t0.4321481882398057\t0.2001\t705\t2015-04-17T03:32:45.457Z\tEIGM\t8426533532295992624\t2018-01-09T00:05:00.160018Z\t24\t00000000 e8 ad 36 5c cb b8 5d b4 53 28 19 c2 01 0e 8f 66\n" +
                        "00000010 97\tWQLHCJXVCGNTQ\t0x926944041335a99ce55a5eef7ed7c026b0c7f93d95f591d535eaf6526c598515\n" +
                        "14\tgoogl\t44.732\t2018-01-01T02:48:00.000000Z\ttrue\tV\tNaN\tNaN\t203\t2015-12-08T17:18:27.478Z\tPGCX\t5380477662961061825\t1970-01-01T00:00:00.000002Z\t10\t00000000 90 8d 93 db 1f 62 a1 94 79 2f 42\t\t0x861dcb18681a98961dfe4c0f274ef859293d5ed21833e5bb718e3c893864a683\n" +
                        "15\tibm\t56.004\t2018-01-01T03:00:00.000000Z\tfalse\tG\t0.11029911861859187\t0.5488\t944\t2015-02-09T23:37:17.398Z\tEIGM\t-1119752973795476401\t2018-01-09T00:05:00.080009Z\t14\t\t\t0xdae79c787f442fca8e1a3effc0c4ecca4e9b9ab68da2c72246a980f20f7822f4\n" +
                        "16\tgoogl\t23.851\t2018-01-01T03:12:00.000000Z\ttrue\tT\t0.8687593575113316\t0.0848\t949\t2015-08-27T12:50:47.116Z\tPGCX\t3419109867899316800\t1970-01-01T00:00:00.000003Z\t8\t\tGWCGCRZBENJBFY\t0x4bf1012412873a95947292c0eddc32ea963a2444ba60e8e357c6f20d09de81cb\n" +
                        "17\tibm\t4.473\t2018-01-01T03:24:00.000000Z\ttrue\t\t0.12277210490085044\tNaN\t844\t2015-09-10T21:51:27.889Z\tPGCX\t-7330281417393706681\t2018-01-09T00:05:01.320036Z\t47\t\tOIZGXXTLLHOBPZ\t0x2cb22c191beae3a542f622953672274b66387ca028e175769e41912793089909\n" +
                        "18\tibm\t65.755\t2018-01-01T03:36:00.000000Z\tfalse\tN\t0.7547582686966736\t0.4079\t715\t2015-01-30T11:12:28.646Z\tPGCX\t5210528511280885759\t1970-01-01T00:00:00.000004Z\t28\t00000000 fb 8c d5 bc d0 91 83 4a 63 c0 f6 14 84 e5 71 5d\n" +
                        "00000010 7e 34 20\t\t0x7ac145aebab595f48ce15348ff006d2e494d2cb1d5b3f70c8bcbf2a2c00945fe\n" +
                        "19\tibm\t67.833\t2018-01-01T03:48:00.000000Z\ttrue\tQ\t0.9158943125756168\t0.2221\t1009\t\t\t2746515355045090231\t2018-01-09T00:05:01.240027Z\t42\t00000000 f3 64 67 e2 7f e2 f5 4d c7 a4 8d 3b e0\t\t0x2979567d7db6314d232940f51d05a5f234c74c362e4265a34f186c88ec302bf1\n" +
                        "20\tibm\t76.855\t2018-01-01T04:00:00.000000Z\ttrue\tD\t0.393808944708379\tNaN\t349\t2015-10-09T17:39:07.004Z\tEIGM\t-9202703447224120734\t1970-01-01T00:00:00.000005Z\t18\t00000000 d8 9b 54 f8 98 b0 a3 67 e0 c0 9d ae 7e 26 f2 74\n" +
                        "00000010 49 2a 06\tVSKTWJEZSZDLMYLQ\t0x1531a2abc7fc705471387ba12301211f79905419b688761e1e67af2c204b2f5e\n" +
                        "21\tibm\t7.513\t2018-01-01T04:12:00.000000Z\tfalse\tD\tNaN\t0.0832\t135\t2015-05-31T00:41:18.042Z\tGDUO\t-8804954999656744085\t2018-01-09T00:05:01.160018Z\t21\t00000000 2c 3e 75 9b 62 12 e6 ad c9 c1 0a a0 e7 f3 09 0e\n" +
                        "00000010 c7\tYVOBCQVEVTS\t0x3c70e77deeb0a61042415439ba4c44494bfd2bb4144bfe217302d6a279f6dacc\n" +
                        "22\tmsft\t99.161\t2018-01-01T04:24:00.000000Z\ttrue\tS\t0.5448049525249924\t0.9921\t660\t2015-11-27T02:59:12.624Z\t\t-7089209359684810221\t1970-01-01T00:00:00.000006Z\t11\t00000000 c3 09 20 52 81 d4 cb 27 3c 32 24 46 30 0b a7\tVXRMEJOBQRUI\t0x73442faca59d8bcb7df4d54d861094e88e41ab0cd6104af459c721dafb3b1ea4\n" +
                        "23\tmsft\t52.113\t2018-01-01T04:36:00.000000Z\tfalse\tM\t0.9964150787030598\t0.9432\t300\t2015-01-18T18:37:41.049Z\t\t4332672151434608519\t2018-01-09T00:05:01.080009Z\t22\t\tVIGIETLBEZPWLSQ\t0xa61e0544db22abd43aa6914f35f0d18c09bacd1464126b050b33c18841054e49\n" +
                        "24\tibm\t65.32600000000001\t2018-01-01T04:48:00.000000Z\tfalse\tT\t0.1966890757410883\t0.5546\t39\t\tPGCX\t4471071070414219829\t1970-01-01T00:00:00.000007Z\t24\t00000000 c3 a4 63 79 38 f6 f7 09 48 df\tXDVIQOXJUNNRG\t0xb4cc44b8afbab285cf828df8951674c58a943a1def342cdf3a5e25051944e30f\n" +
                        "25\tmsft\t98.383\t2018-01-01T05:00:00.000000Z\tfalse\tF\t0.7638567572615929\tNaN\t773\t\t\t1739611607907942795\t2018-01-09T00:05:02.320036Z\t11\t00000000 08 03 ee 7a 52 a5 7e 8c 99 70 91 57 50 30\tWLGYGBPIGIFVT\t0xeb0f654e655b0ffce50230673e32d69fbf5f923d1ecaa34671abc3239d1cc32b\n" +
                        "26\tmsft\t19.466\t2018-01-01T05:12:00.000000Z\ttrue\tP\t0.0971940665379305\t0.1551\t460\t2015-08-04T17:40:09.946Z\tPGCX\t-1698999598759904076\t1970-01-01T00:00:00.000008Z\t25\t00000000 5b 90 5f 87 f9 12 c6 b7 33 5c 66 61 48 e9\tRKQFGWHFZBL\t0x34fce1104d81ef233593e8a6cc8c4b796b5215401281c1e69dd83cb03bd8fbfd\n" +
                        "27\tmsft\t2.515\t2018-01-01T05:24:00.000000Z\ttrue\tS\t0.9949535551744708\t0.4539\t455\t2015-08-03T15:40:31.152Z\tGDUO\t-5112727921935267717\t2018-01-09T00:05:02.240027Z\t40\t\t\t0x77eef5cccef791dd860b86c9e85a824b403cf5d6fa369344799ecd80e51aa9eb\n" +
                        "28\tgoogl\t5.596\t2018-01-01T05:36:00.000000Z\tfalse\tS\t0.33242758930266914\t0.1250\t628\t2015-07-12T21:09:54.633Z\tGDUO\t6051676324231803895\t1970-01-01T00:00:00.000009Z\t11\t\tOVRJUTKZGQ\t0xe184d68d59839c38c9c6f2e39687f081a2225d7db9deddefa9fbc60e3c6fe595\n" +
                        "29\tmsft\t11.207\t2018-01-01T05:48:00.000000Z\tfalse\tV\t0.24485779917562034\tNaN\t174\t2015-12-15T14:32:05.953Z\tQCFV\t5883489345136750945\t2018-01-09T00:05:02.160018Z\t37\t00000000 7c a9 a6 98 0e 23 e4 ef d9 26 d2 f7 94 40\t\t0x3fb46fe6a0fc1b9f87b78577eb7ead0bab9d6b5576d7537e91862729e7fa2130\n" +
                        "30\tmsft\t85.62\t2018-01-01T06:00:00.000000Z\ttrue\t\t0.42488439332809314\tNaN\t320\t2015-11-21T09:18:04.418Z\tPGCX\t-2825227152418595114\t1970-01-01T00:00:00.000010Z\t31\t00000000 a9 47 0c 12 5f 60 01 91 54 15 52 48 e1 3d\tZWSGGUGD\t0x921f3ec417d2ef9385e599e46ea87ed76d68650543e829fa644ae799200be9ba\n" +
                        "31\tmsft\t0.723\t2018-01-01T06:12:00.000000Z\ttrue\tP\tNaN\t0.5278\t576\t2015-01-25T22:52:43.733Z\tPGCX\t-4733640001127818408\t2018-01-09T00:05:02.080009Z\t17\t\tVRDUCWMLN\t0x83e5bc89e7dba7f4bd167e0bea9ce399960cd0b5e7e6851e59c8d969a72856da\n" +
                        "32\tibm\t32.898\t2018-01-01T06:24:00.000000Z\tfalse\tU\tNaN\t0.7246\t439\t2015-09-10T23:00:29.735Z\t\t-6345141271315877369\t1970-01-01T00:00:00.000011Z\t6\t\tGWOICHMR\t0x68d36b6288f5d818c1e4d3fd1ba6e496da38f3e0f360596facfa49cd8f53fc62\n" +
                        "33\tgoogl\t73.667\t2018-01-01T06:36:00.000000Z\ttrue\tL\t0.03770012637600928\t0.6414\t414\t\tPGCX\t3373400371124787293\t2018-01-09T00:05:03.320036Z\t40\t\tHWSUHSBNV\t0x6c7c337b62312325842ef0f31849b0a970311eaefc1afc3c5088e8f6f6753cef\n" +
                        "34\tibm\t63.453\t2018-01-01T06:48:00.000000Z\tfalse\tI\t0.26239369794840217\t0.4078\t252\t2015-11-23T08:05:18.738Z\t\t2485383496847621906\t1970-01-01T00:00:00.000012Z\t32\t\tJBPGNIYPBZZL\t0x98e114741634333272e6b4f2484bbd3f9e07c377fb2c0b0ed1aa37d4f138755e\n" +
                        "35\tgoogl\t77.639\t2018-01-01T07:00:00.000000Z\tfalse\tY\t0.36102882769970035\tNaN\t244\t2015-02-03T06:25:23.167Z\tQCFV\t-8703284362906492849\t2018-01-09T00:05:03.240027Z\t17\t00000000 e5 4e 30 a9 90 aa ac 43 b8 10 99 6a 99\tHHQBCPCU\t0x442031d7201f18c853cd9e95ae849dba4be30a31a4dc0d2243dcf5676f036ac2\n" +
                        "36\tibm\t51.615\t2018-01-01T07:12:00.000000Z\ttrue\tR\t0.1918586696266893\t0.1192\t950\t\t\t-7037054039034036225\t1970-01-01T00:00:00.000013Z\t10\t\tBNWHNJE\t0x5a04ecd6caa48db2851afcf82b206a4e77d4658eee764d88bcf29054bb9d076a\n" +
                        "37\tgoogl\t87.46300000000001\t2018-01-01T07:24:00.000000Z\ttrue\t\t0.9962089083944984\tNaN\t140\t2015-12-06T14:28:47.318Z\tQCFV\t-6585978673053378748\t2018-01-09T00:05:03.160018Z\t32\t00000000 d8 98 66 ed d6 a0 1e 3c d0 2d 27 36 f1 00 8a c7\n" +
                        "00000010 77 e6\tXHLYIHDLFLL\t0x86b2b60d9fee90b5a64e18359a655b2e9c42e2fecf1e3000a1ff8f9efee5b37f\n" +
                        "38\tgoogl\t70.976\t2018-01-01T07:36:00.000000Z\ttrue\tT\t0.47359884921144746\t0.8457\t200\t2015-02-21T16:46:49.541Z\t\t6386938178359029322\t1970-01-01T00:00:00.000014Z\t40\t00000000 a1 4a 63 94 83 90 b2 06 40 e7 39 53 48 ab 76 bc\n" +
                        "00000010 9b b7 52\tTTTEZHTIQQQS\t0x7f6411aaedbc25cc5bef45f390ea905313c36e8f7088071965562022879b4b8e\n" +
                        "39\tmsft\t16.915\t2018-01-01T07:48:00.000000Z\ttrue\tT\t0.12660971332563564\t0.0112\t697\t2015-04-03T21:04:03.481Z\tPGCX\t2253519439647557731\t2018-01-09T00:05:03.080009Z\t30\t00000000 20 66 b6 c0 9b ec 67 9e 99 d4 a3 da 3b 92\tGYSVE\t0xae25da70ea1b822a797fbb6207c305ee811dc9ebdd54d9286e2fe993f0cdd1d2\n" +
                        "40\tibm\t93.36200000000001\t2018-01-01T08:00:00.000000Z\ttrue\tQ\t0.35498982451113825\tNaN\t356\t\t\t-3389129830005724044\t1970-01-01T00:00:00.000015Z\t40\t00000000 05 ec 6d 11 1b 04 11 f6 02 03 b8 25 d1 86 c3\t\t0x84177c0df124da60d92cf3212db6a9c1ac52247fecb4e32e50452f063ff1e843\n" +
                        "41\tgoogl\t87.936\t2018-01-01T08:12:00.000000Z\tfalse\tH\t0.4648750288932547\t0.0299\t307\t2015-09-17T22:12:35.265Z\t\t-5315941299151788007\t2018-01-09T00:05:04.320036Z\t18\t00000000 67 43 0e e9 d0 6a 3d 4f f8 0b fc f6 08 e9\tCGBBMPQSHTCPTOPF\t0xc01968ff912b7231d1c74fc71a6fd084d59bd537242b6382c546d782aae00109\n" +
                        "42\tibm\t42.381\t2018-01-01T08:24:00.000000Z\ttrue\tW\t0.1091669937399774\t0.8835\t987\t2015-09-06T14:33:46.426Z\tQCFV\t8069315697119463922\t1970-01-01T00:00:00.000016Z\t32\t00000000 8a 2a 24 38 9f 9e 8c e4 ed 3c 99 f3 34 12 bf\tHIVWU\t0x1fb0682eaee7799c239f92d6ad9e133e2325a6400d03670e577e2acff050953a\n" +
                        "43\tgoogl\t27.129\t2018-01-01T08:36:00.000000Z\tfalse\tO\t0.40420690050595487\t0.9743\t422\t2015-12-20T08:03:26.500Z\tPGCX\t6758438264799333170\t2018-01-09T00:05:04.240027Z\t8\t00000000 66 17 74 a7 13 7f 10 b0 da df\tNRLBMESDSOOIF\t0x8e39d4f0d40a0e5a2a94824a0bcbbdbf2437e7ef61f7e438700dd3eb0aa6f699\n" +
                        "44\tgoogl\t82.127\t2018-01-01T08:48:00.000000Z\ttrue\tO\t0.2943352898982291\t0.2614\t586\t2015-02-13T07:32:47.562Z\tEIGM\t-8370425851535977342\t1970-01-01T00:00:00.000017Z\t20\t00000000 49 5a a3 2b bf 17 ff e1 46 49 ce 67 8c\tZRETLWBNXR\t0x3c2937cd08ed7412336bcc89e9ceba20330012785503d47566d7654d79c260c3\n" +
                        "45\tibm\t20.281\t2018-01-01T09:00:00.000000Z\ttrue\tI\t0.8574875349240728\t0.1454\t69\t2015-12-06T14:00:27.412Z\tEIGM\t4533281377304275206\t2018-01-09T00:05:04.160018Z\t16\t\tTNMRGWOOJD\t0x69fa9b1b7cd9d2f66c5e97b4de540ef36ca3ff2580038bf39f1002bb2537ab04\n" +
                        "46\tibm\t84.487\t2018-01-01T09:12:00.000000Z\ttrue\tV\t0.3053809393560172\t0.9555\t799\t2015-05-21T03:01:51.730Z\tPGCX\t7009906852879083972\t1970-01-01T00:00:00.000018Z\t14\t00000000 ec 30 fe e8 db 87 fb 7e 33 cb 24 7f 73 1d\tFOJXKDCKX\t0x69436e94356e0b3f7b00f2d8542e272d4295a0519ea9fb0427b3182fac12c754\n" +
                        "47\tmsft\t21.374\t2018-01-01T09:24:00.000000Z\tfalse\tB\t0.1931647758040418\t0.8422\t861\t2015-01-08T21:53:44.098Z\tQCFV\t3933412126145430432\t2018-01-09T00:05:04.080009Z\t44\t00000000 41 f4 d0 e6 dd ee 4e 12 ff f0\t\t0x8748647eed9fcd6c895774e4771151ca238c45c2e98f716b11be09308c6ee121\n" +
                        "48\tgoogl\t17.691\t2018-01-01T09:36:00.000000Z\tfalse\tS\tNaN\tNaN\t1019\t2015-01-11T12:14:40.063Z\tQCFV\t2932257551581936013\t1970-01-01T00:00:00.000019Z\t33\t00000000 de 32 eb cb f2 a3 b1 b0 00 1d 6a 41 93 5f 15 b2\n" +
                        "00000010 47 72 06 a8\tLUSKIZUQ\t0xa0f5caa86dd94f3ab5d7ef76d6912a31bbcf2ca3c117bb69b614476005f24777\n" +
                        "49\tgoogl\t59.199\t2018-01-01T09:48:00.000000Z\tfalse\tT\t0.11522485886918832\t0.1423\t292\t\t\t-5386911804896199867\t2018-01-09T00:05:05.320036Z\t8\t\tUNUQGENY\t0x5994d39b8229ba98886da011cbe018e8831d51a3e4bc7fc46dffbf74698c2a73\n" +
                        "50\tmsft\t71.815\t2018-01-01T10:00:00.000000Z\tfalse\t\t0.9480493252774558\t0.4886\t164\t2015-04-25T07:41:54.728Z\t\t-4152007073616347952\t1970-01-01T00:00:00.000020Z\t23\t00000000 39 52 32 d9 83 6b fa 26 67 0e 60 21 f5\tIOZPDYIWTD\t0x4d2e65a7786091139ed63a60e92e153398f5f9b0285afc13ad49638ff1bd4630\n" +
                        "51\tibm\t66.82600000000001\t2018-01-01T10:12:00.000000Z\ttrue\tE\tNaN\t0.8492\t32\t2015-10-11T06:39:34.756Z\tEIGM\t-8083289474968658601\t2018-01-09T00:05:05.240027Z\t2\t00000000 b7 0a c6 ec fa 64 f8 1c 63 ce db 12 b1 9f 55 b3\n" +
                        "00000010 e5 5e 2e\t\t0x3975d60cd45d205d50d65cbb65216f264d4d6e5bfdb6c4ba48ff6a5b10ad063c\n" +
                        "52\tmsft\t37.112\t2018-01-01T10:24:00.000000Z\ttrue\t\tNaN\t0.2620\t877\t\tPGCX\t2603605003689778175\t1970-01-01T00:00:00.000021Z\t29\t00000000 e1 ce b8 b9 83 ec 34 4a eb 4b 4f f0 ab 0b 93 23\n" +
                        "00000010 fc 8d c3\tZWMFME\t0x9a2965e9ee8ae2ec445b16408f3c5e1e9112bfe1729f1bb6ee354e4a392d885e\n" +
                        "53\tibm\t99.66\t2018-01-01T10:36:00.000000Z\ttrue\tD\t0.22813860308839085\t0.3827\t497\t2015-11-20T09:22:32.462Z\tPGCX\t8774570944193648180\t2018-01-09T00:05:05.160018Z\t25\t00000000 3e 58 e8 6a 02 41 47 6b 39 bf\tNWWSTGZW\t0x9bbd38b82a7cf71c7b5d5f20f9a4d155853473bd3a22e85e93a6e8e3601e33f1\n" +
                        "54\tibm\t87.921\t2018-01-01T10:48:00.000000Z\tfalse\tR\tNaN\t0.9098\t39\t2015-05-20T20:48:24.658Z\t\t-5566971365073829070\t1970-01-01T00:00:00.000022Z\t18\t00000000 91 cf 73 93 10 e8 a5 50 33 13 ee 12 06\tFYXJWDRYYLG\t0xae1c396be649995156e25cdcf1cd2bc147e5f73d4081029d80c813e9b8488fe8\n" +
                        "55\tmsft\t68.889\t2018-01-01T11:00:00.000000Z\ttrue\tI\t0.2944570701757583\t0.2651\t409\t2015-06-13T23:02:23.019Z\tEIGM\t-8876341890782144334\t2018-01-09T00:05:05.080009Z\t12\t00000000 44 5d 4e 6d 8c a4 43 91 d8 ea e6 5b b4 f5 01 71\n" +
                        "00000010 f2 29 d8 be\tEPUBL\t0x6539679536341fdd4257224054ddb074824e60853994438f970b17e1b329f19f\n" +
                        "56\tmsft\t72.345\t2018-01-01T11:12:00.000000Z\tfalse\tJ\t0.13972954047695807\t0.2171\t699\t2015-08-30T10:20:37.712Z\t\t2063551415621954787\t1970-01-01T00:00:00.000023Z\t22\t00000000 e1 0f 78 65 1b 84 59 e7 47 7c b8 22 15 f5 eb 53\n" +
                        "00000010 64 31\tRLHLKXNG\t0x8088c562f01abe8f9ab0c5f8c0613cd0c3282666003e79f8deb61ca3833decdf\n" +
                        "57\tmsft\t63.785000000000004\t2018-01-01T11:24:00.000000Z\ttrue\tW\tNaN\t0.9820\t684\t2015-10-23T00:20:32.552Z\t\t-6020829238723221929\t2018-01-09T00:05:06.320036Z\t38\t00000000 a9 43 ba 65 f1 9d 2c a1 87 e3 70 1c c9 8c 06 e4\n" +
                        "00000010 64\tTBHFXPMPU\t0xb5edc365e0ad0e99b5204420901ca73c9280de5c04d8b81a766863d1f09073de\n" +
                        "58\tibm\t5.33\t2018-01-01T11:36:00.000000Z\tfalse\tX\t0.47586514392213686\t0.3896\t534\t2015-05-06T11:54:45.996Z\t\t-8988054948870237547\t1970-01-01T00:00:00.000024Z\t26\t\tBRPNXODLW\t0x9f697d4d21077bb4c3f2a0c5e2b180f0eb3af9e5d05e634adb95adb2caed5ead\n" +
                        "59\tibm\t74.188\t2018-01-01T11:48:00.000000Z\ttrue\tM\tNaN\t0.2865\t436\t2015-10-01T05:40:47.415Z\tPGCX\t-6214304135212844343\t2018-01-09T00:05:06.240027Z\t31\t00000000 52 70 95 b0 55 d5 75 04 35 d5 91 02 0f 82 1b 41\n" +
                        "00000010 6e 24\tPBOSVK\t0x89485fe0e0c224585579a82888d9c3b396037b84646d5b7dd5e8e277bc300ec7\n" +
                        "60\tgoogl\t43.78\t2018-01-01T12:00:00.000000Z\tfalse\tU\t0.8235530910063924\t0.1112\t229\t\tGDUO\t-6270776997167519352\t1970-01-01T00:00:00.000025Z\t29\t\t\t0xc7d3a98e638c4d958f691be3704cd4107a1e202ecdfcaea79b90df5a47b09732\n" +
                        "61\tibm\t30.083000000000002\t2018-01-01T12:12:00.000000Z\tfalse\tH\t0.3567991846159069\t0.9776\t25\t2015-10-24T01:20:08.562Z\t\t-7650197424704303491\t2018-01-09T00:05:06.160018Z\t45\t00000000 67 ef d8 41 f7 b5 2c 24 8f bc 0f 89 11 22 c1 a1\n" +
                        "00000010 68 5c 65\tUHJBJNBYLJYGGG\t0xce85ae976215b8a9c105925fd8c3584dc17615a93f415dde669c62cc283b73ec\n" +
                        "62\tibm\t97.691\t2018-01-01T12:24:00.000000Z\tfalse\tN\t0.022505431934031983\t0.1174\t116\t2015-05-29T00:09:29.984Z\t\t2616690236313880770\t1970-01-01T00:00:00.000026Z\t36\t\tSZBEICJOYP\t0x8d4f633b82ce23d6e5bc5c988986a1f19f09743698941acb4916bce0931637e3\n" +
                        "63\tmsft\t58.735\t2018-01-01T12:36:00.000000Z\tfalse\t\t0.418656279417825\t0.5389\t674\t2015-03-18T01:57:57.453Z\t\t5745457059787414347\t2018-01-09T00:05:06.080009Z\t30\t00000000 43 9c 1a 98 73 25 ff 87 b0 f0 98 58 2e a0 2f 3a\n" +
                        "00000010 af ef 2d b9\tCNOGU\t0x7d96e7ab6102d19b5173816ce51d70d356ec3a1e6d8a7df60f85500875c39969\n" +
                        "64\tgoogl\t58.977000000000004\t2018-01-01T12:48:00.000000Z\ttrue\t\t0.10629988094021403\t0.2105\t327\t2015-11-09T15:52:50.030Z\t\t2383367538558683605\t1970-01-01T00:00:00.000027Z\t39\t\tKIHFKWQLXWF\t0x9d38ab6da6194811b965464b17e4e0e778b04dc95405e01f9f2305c09e58b3ba\n" +
                        "65\tgoogl\t85.917\t2018-01-01T13:00:00.000000Z\ttrue\tJ\t0.7232706215119478\t0.9366\t500\t2015-08-06T22:16:28.906Z\tGDUO\t-4229152126636991074\t2018-01-09T00:05:07.320036Z\t11\t00000000 fb dc 86 09 b0 b7 53 23 bf 7d b2 04\tTJKDDGUUDCFM\t0x4cfadae1b6d46ab73c42d1f593462978973183af3af8946660dde8005b150473\n" +
                        "66\tgoogl\t73.393\t2018-01-01T13:12:00.000000Z\ttrue\tF\t0.9625837014976498\t0.8263\t922\t\tGDUO\t2203477829156054952\t1970-01-01T00:00:00.000028Z\t12\t00000000 a9 fa 5e b3 d5 d6 97 f0 b3 03 78 be 49 dc 83 9d\n" +
                        "00000010 7e cf 3e 49\tEROZNJOSKQWKFZ\t0xb4a3849a99ea23e19ed6f6731dc9658d84bd1280b1fe918e62499ee5c694bc63\n" +
                        "67\tmsft\t60.766\t2018-01-01T13:24:00.000000Z\ttrue\tK\t0.7530001856942596\tNaN\t647\t2015-02-28T17:11:21.683Z\tQCFV\t-6611656939083900931\t2018-01-09T00:05:07.240027Z\t2\t00000000 ac 68 13 1b 58 e9 76 4f 4b 45 6e 20 db e2 92 4e\n" +
                        "00000010 e0 21 f6 74\tBRKFLNJECWFSSWT\t0x526f77bd2e5aabcd4391793bfda2516321cc4ea0d335a3e176c2d34eefdbaf31\n" +
                        "68\tgoogl\t5.5\t2018-01-01T13:36:00.000000Z\tfalse\t\t0.09998995057385829\t0.8703\t296\t2015-10-28T07:16:34.793Z\tEIGM\t-7756422336222456172\t1970-01-01T00:00:00.000029Z\t33\t00000000 2c 73 05 dc 02 47 cc 76 fb c0 8a b2 bd\tYFYFQUJPVOPECTU\t0xd2e0304f0a5a84e57113413df60cb941588ad65bcffe59acb946e0398d1db785\n" +
                        "69\tibm\t59.868\t2018-01-01T13:48:00.000000Z\ttrue\tJ\t0.7317147968514648\t0.2816\t408\t2015-04-27T14:31:56.120Z\tQCFV\t5142656445929621631\t2018-01-09T00:05:07.160018Z\t27\t00000000 95 22 7c fd c5 06 3f 27 ea d5 ed 71 56 7d 17 a9\n" +
                        "00000010 ba 50\tOYUGQTOIUJXM\t0x5aaecb28b7767340309394afb8f21b5b831ed1fcb56994b7643c43f7ab730bbe\n" +
                        "70\tmsft\t26.239\t2018-01-01T14:00:00.000000Z\ttrue\tY\t0.31818545931978015\t0.5250\t749\t2015-08-16T04:34:23.329Z\tQCFV\t-6676032593747360919\t1970-01-01T00:00:00.000030Z\t28\t00000000 5f 87 42 7a a5 df d0 16 d6 26 27 c7 2b d9 bc d0\n" +
                        "00000010 7e be fc b4\tZKUGOQJDME\t0x25cea99a2520f7908d18777f6d1bbd339dec256e3b3a6c7343ce6f2209672875\n" +
                        "71\tmsft\t93.018\t2018-01-01T14:12:00.000000Z\tfalse\tH\t0.5830026786376984\t0.0240\t461\t2015-03-20T16:38:52.844Z\tGDUO\t-4266192262533897502\t2018-01-09T00:05:07.080009Z\t48\t\tPHWEIJTEJSPKPHU\t0xa0e0112f0b06b81c625973611894045055e90d4cb8c84eb62231a3d7142e83c2\n" +
                        "72\tmsft\t9.235\t2018-01-01T14:24:00.000000Z\tfalse\tU\tNaN\t0.3906\t684\t2015-08-10T18:59:44.365Z\t\t8487014185891984162\t1970-01-01T00:00:00.000031Z\t41\t00000000 6f fc aa b1 74 db bf e3 e9 60 36 2a 73\tNQSYV\t0x63b88416907568d68f394c1374fdc5cf44e347cd3ef2113b3d816310ce85424e\n" +
                        "73\tgoogl\t77.368\t2018-01-01T14:36:00.000000Z\tfalse\tK\t0.6432953558477122\t0.7458\t1006\t2015-01-10T07:09:28.654Z\tQCFV\t5536651481888826318\t2018-01-09T00:05:08.320036Z\t24\t00000000 05 7d ac 7b b5 dc b7 48 9d 1a 2d\tREISOHQEPPSEDID\t0x4927b908a0f4f1beaf87157ba2e4cd1979951e959cd1f9657b26806213585e44\n" +
                        "74\tmsft\t40.348\t2018-01-01T14:48:00.000000Z\ttrue\tF\t0.009479819692946867\tNaN\t377\t2015-04-05T18:40:41.412Z\tEIGM\t6019074372627566050\t1970-01-01T00:00:00.000032Z\t25\t00000000 12 25 06 90 3a 47 96 61 82 12 d3 a4 8c\tNKQGZRDCOMV\t0xbad7706cbb6f82e3a61178bc701858ba7c5f4e257ab7aaada5950c5d6e355ca6\n" +
                        "75\tgoogl\t53.281\t2018-01-01T15:00:00.000000Z\ttrue\t\t0.18787963424629506\t0.6411\t956\t\tQCFV\t-7026763540774681452\t2018-01-09T00:05:08.240027Z\t5\t\tXOWBPIEKKGTCPH\t0x411212c62a006bf775af6a3294624f08498dffdb0c99ff8181008b363c0f0fc8\n" +
                        "76\tgoogl\t36.374\t2018-01-01T15:12:00.000000Z\tfalse\tY\t0.03892462180566281\t0.5538\t733\t2015-02-09T00:08:51.489Z\tEIGM\t6710137344790837965\t1970-01-01T00:00:00.000033Z\t11\t\tMGGGSTKBXFHZN\t0xaf9b14aaf1648da1a075c269888c65838ba0c1b39442b3afb1345f7c0f646005\n" +
                        "77\tibm\t62.971000000000004\t2018-01-01T15:24:00.000000Z\ttrue\tP\t0.44404071464963535\tNaN\t77\t2015-04-13T22:07:57.429Z\t\t-8605348191342762751\t2018-01-09T00:05:08.160018Z\t20\t00000000 19 b4 33 10 27 33 ab 6c 02 c9 c1 ce e3 bc 07 3c\tHDXIBGP\t0x71e4a82441e001517fb2a34b2accf798bd5494aeef230fe08a91ac99e04406af\n" +
                        "78\tgoogl\t43.252\t2018-01-01T15:36:00.000000Z\ttrue\tD\t0.15688587035938162\t0.5996\t276\t2015-12-23T09:53:59.740Z\tEIGM\t-5699130780961187659\t1970-01-01T00:00:00.000034Z\t43\t00000000 df 8b df 82 d7 e6 ae d4 d7 7e 96\t\t0x730641c12910557961f41f49b51d791738286fb7337ade1b4d197c31e9bab8c2\n" +
                        "79\tgoogl\t46.57\t2018-01-01T15:48:00.000000Z\ttrue\t\t0.5013034528933313\t0.9289\t120\t\tQCFV\t-7166583305703344592\t2018-01-09T00:05:08.080009Z\t37\t00000000 0c 98 e4 e1 41 a4 0f 17 a1 51 8f ee bb 23 9a 0a\n" +
                        "00000010 04 b5 dd 03\tUXKQMCYKLNB\t0xa9a8ebc41315b11f64737b76152885e527f2607768aa59598be173cdd35be3f1\n" +
                        "80\tgoogl\t39.819\t2018-01-01T16:00:00.000000Z\tfalse\tO\t0.25731580439798774\t0.7262\t809\t2015-04-21T12:47:39.697Z\tPGCX\t-1226520739388834892\t1970-01-01T00:00:00.000035Z\t26\t00000000 f0 e8 d3 52 ce 1b 8c 94 5c 3c ea 57 99 ff\tHGHLLXXUBTYLE\t0xc925a43c5b0bb3378e5611ea697d86ef92c593655515e87dd4682360d247b511\n" +
                        "81\tgoogl\t94.459\t2018-01-01T16:12:00.000000Z\ttrue\tX\t0.5500751040959276\tNaN\t160\t2015-08-29T02:42:27.756Z\t\t-2923631812842293781\t2018-01-09T00:05:09.320036Z\t2\t00000000 b2 46 1e db d9 90 af ab 59 90 52 61 08 38 ce a1\n" +
                        "00000010 27 6e 5f 18\tZHQNOHJKMEPFPEH\t0xc3521cd0a649fa18d5f88da243713dff97509894afaa7dc998129f5174884636\n" +
                        "82\tmsft\t17.695\t2018-01-01T16:24:00.000000Z\ttrue\tQ\t0.6421085975969587\t0.0930\t98\t2015-10-11T07:25:56.359Z\t\t-3159389959820229071\t1970-01-01T00:00:00.000036Z\t18\t00000000 5d 7c 81 95 b8 ab de 8a 45 aa b1 08 2c eb 35 30\tSHXJSKZESO\t0x5c24439cdcb2d91d89efaa6ef66f715e647cb4097809000274bc34c8b7a91c8d\n" +
                        "83\tgoogl\t4.102\t2018-01-01T16:36:00.000000Z\ttrue\t\t0.6300776370745916\t0.7788\t386\t2015-03-08T16:17:57.782Z\tPGCX\t-3173419019361443829\t2018-01-09T00:05:09.240027Z\t46\t00000000 13 e2 8b 8d ee 0a 41 f3 c7 a0\tOSBPI\t0xb5f1770ca19dc00d5e2e2d69adc9b86f645a72883c28f145620f8ab7be8fbc37\n" +
                        "84\tmsft\t62.106\t2018-01-01T16:48:00.000000Z\ttrue\tO\t0.873087456601778\t0.7179\t541\t2015-06-25T04:28:01.300Z\tQCFV\t4713089647395388796\t1970-01-01T00:00:00.000037Z\t14\t00000000 e2 8c c4 3e 67 c7 73 9f 8e 24\t\t0x3d9b311c5692ad1827da397f360f9e157043aaf4186d5f34a9dd9743176bdf61\n" +
                        "85\tibm\t55.126\t2018-01-01T17:00:00.000000Z\ttrue\tG\t0.7226870494951839\t0.4980\t766\t2015-01-16T07:17:48.736Z\tEIGM\t-3260826870346627617\t2018-01-09T00:05:09.160018Z\t41\t00000000 f4 e8 d2 2f ce c5 cb 34 1f 37 b3 e3 6b b2\t\t0x90b7651d4340d36d77b1c5dcccce081610bbc5b6421340ec5b2461de58570d96\n" +
                        "86\tmsft\t73.241\t2018-01-01T17:12:00.000000Z\tfalse\tG\t0.4306088190787237\t0.8962\t136\t2015-06-10T15:47:20.982Z\tPGCX\t2149834489838280113\t1970-01-01T00:00:00.000038Z\t42\t\tOTPDZZVXF\t0x9c0cc691c104a8f9bd14c3deb716f98fb71373ac18f236288b4d24dda5101b03\n" +
                        "87\tibm\t6.094\t2018-01-01T17:24:00.000000Z\tfalse\tS\t0.6871665292966334\t0.9885\t299\t2015-12-21T04:43:47.089Z\tPGCX\t4953499231907070958\t2018-01-09T00:05:09.080009Z\t25\t00000000 e5 79 02 6a 2f 13 55 84 90 f4 80 0a 65\tKTUYZTMOWPG\t0x7a0d2880e5e515abc50c349c42a6c2c7d6c40732ed81728e6600c382d610ba2d\n" +
                        "88\tibm\t21.424\t2018-01-01T17:36:00.000000Z\tfalse\tS\tNaN\tNaN\t153\t2015-02-03T00:23:13.331Z\tPGCX\t-6855861594464824744\t1970-01-01T00:00:00.000039Z\t37\t00000000 e4 eb 97 49 09 3a 24 b7 17 c5 8c f8 e8 96 9e 45\n" +
                        "00000010 89 2a 93 2d\tXSDSTBFURPUZOGQI\t0x8b2e05fe18cd00ac8507ecf080bdd3da8ff5d57815ed51233a2a029e870d2301\n" +
                        "89\tgoogl\t20.371\t2018-01-01T17:48:00.000000Z\tfalse\t\t0.18863629790847292\tNaN\t210\t2015-06-17T12:01:25.281Z\tEIGM\t4302354550746869703\t2018-01-09T00:05:10.320036Z\t33\t00000000 5e a8 43 84 e2 c0 22 20 db 14 c6 ec cb 51\tHHIHWYY\t0x8ce05206ac5d169c9d5284a5bf5609b18cf9746ec2a049c1b16063f8441d2fdc\n" +
                        "90\tgoogl\t9.183\t2018-01-01T18:00:00.000000Z\tfalse\tW\t0.820833461416011\t0.4504\t659\t2015-07-12T21:32:10.532Z\tEIGM\t7495881932006433603\t1970-01-01T00:00:00.000040Z\t8\t00000000 8e a5 37 9c 40 f3 28 61 6e 9f b0 8c da\tMBBVPYUWN\t0xcd8188f78156c238e109a28e69e0c47cac1c98cae8f0d905881df815656d9106\n" +
                        "91\tgoogl\t79.45\t2018-01-01T18:12:00.000000Z\ttrue\tF\t0.08717347129583586\t0.5398\t511\t2015-09-29T15:53:42.998Z\tEIGM\t4755079211960750651\t2018-01-09T00:05:10.240027Z\t2\t00000000 a8 be 05 0c a6 0f b5 1e 05 b2 cb 76 1d 2b 2c df\tUGJDRGQHBLGZEV\t0x5769a8fa98eb2fd767a72ad61482db535823485b87e4717d99343b9be896daa1\n" +
                        "92\tibm\t77.047\t2018-01-01T18:24:00.000000Z\ttrue\tR\t0.8954354974806623\t0.4605\t694\t2015-11-16T20:58:40.383Z\tEIGM\t5886512273229003366\t1970-01-01T00:00:00.000041Z\t49\t\tEYYDOMOFQSVQH\t0xb1ae7a14b690ee33606137964d2ddd212580eaf2637dc27b4e9c23b1c7783124\n" +
                        "93\tibm\t84.812\t2018-01-01T18:36:00.000000Z\tfalse\tZ\t0.42222482929945826\t0.8414\t311\t2015-11-14T20:25:02.775Z\tQCFV\t-9011351407992663693\t2018-01-09T00:05:10.160018Z\t28\t00000000 c9 f8 83 e9 a8 be 97 f3 f5 f5 83 04 96 15 87 96\t\t0x65ab5024c8770fb2832921ee0ae3029d9e8c5a0a25e0492c6e3c0d15b200ab55\n" +
                        "94\tmsft\t6.07\t2018-01-01T18:48:00.000000Z\tfalse\t\tNaN\t0.2933\t554\t2015-05-17T00:24:04.786Z\tPGCX\t4514003953996392511\t1970-01-01T00:00:00.000042Z\t15\t\tKLHGKVSVEEOSYITN\t0x43f6a1850cbf0035889b56b385115d52508d0027b02544b32b8b5d3c50f519c4\n" +
                        "95\tibm\t98.886\t2018-01-01T19:00:00.000000Z\ttrue\t\tNaN\t0.3052\t303\t2015-03-08T15:12:10.805Z\t\t3130566027472097601\t2018-01-09T00:05:10.080009Z\t49\t00000000 0c 7b 36 9c 2c cc aa 0a f5 e7 3c 12 ee\tLUXTPXPCPORTFEQQ\t0x81d863639cb4e3b04d627da797da57f959d700df347ae8ca7ec452c73f2f4ed2\n" +
                        "96\tibm\t10.121\t2018-01-01T19:12:00.000000Z\tfalse\tB\tNaN\t0.7680\t275\t\tPGCX\t8516632215392603266\t1970-01-01T00:00:00.000043Z\t18\t00000000 01 2b 5e b0 54 70 a9 af 72 b8 75 40 8b 58 14\tZNSHQM\t0x9107b453473a2e72538cc4b1386952d11524388f28fd6f2252fd0b1ec5abaaab\n" +
                        "97\tmsft\t60.234\t2018-01-01T19:24:00.000000Z\tfalse\tZ\t0.4594877440175763\tNaN\t787\t2015-03-07T07:31:11.461Z\tEIGM\t-6109267864053821421\t2018-01-09T00:05:11.320036Z\t28\t\tIJGSNBXBXFHXOOWD\t0xb1e71a3b348ff18a767c215bcd3cc2930abad34d93d2dad3765c7dc4c4a0e732\n" +
                        "98\tibm\t5.8580000000000005\t2018-01-01T19:36:00.000000Z\ttrue\tB\t0.18281598758002304\t0.5190\t487\t2015-08-09T22:21:39.378Z\t\t7406189963105479623\t1970-01-01T00:00:00.000044Z\t28\t00000000 3c 35 fd b7 5a e7 0b e4 6d 8d 6b af 08 89 70 73\n" +
                        "00000010 53 1d\tRZUVPQRUL\t0x0a757d51db8fca55439a0e4c9a1972d4a8105311d86e5f57b66200dbb70d76b7\n" +
                        "99\tibm\t42.682\t2018-01-01T19:48:00.000000Z\ttrue\tU\t0.4545491948336532\t0.4856\t110\t2015-07-04T09:10:17.706Z\tEIGM\t1579063475818792888\t2018-01-09T00:05:11.240027Z\t50\t00000000 38 9b a9 8c 7c fa 2b 4c 41 59 87 2b 18 7b 64 79\n" +
                        "00000010 e8 7f cf\tVLEURPUDCMFCMXYR\t0xabc37f5512aed80add19c7ebf7afdab6a216d6073bac8826804a3ff09923da3d\n" +
                        "100\tmsft\t54.922000000000004\t2018-01-01T20:00:00.000000Z\ttrue\tE\t0.20313630662522963\t0.7363\t209\t\tGDUO\t-6195223512023904553\t1970-01-01T00:00:00.000045Z\t5\t00000000 f0 93 8e 21 50 86 c3 82 b6 94 74 1b 01 84\t\t0xa2ca33a32d1b96a2a17acff5ac104e5b62b7b4b80e36884f0c16a97b86fa8119\n" +
                        "101\tgoogl\t82.956\t2018-01-01T20:12:00.000000Z\ttrue\t\t0.7176813243697292\t0.1860\t345\t2015-07-08T13:43:15.333Z\tGDUO\t-5814518159788583083\t2018-01-09T00:05:11.160018Z\t5\t\tXWWURQHEWJVMBVCM\t0x5c4b46b9ba13c678575d3e4ee640ed05536d9fd24b6a9b0c41359f8cb67fd697\n" +
                        "102\tgoogl\t62.824\t2018-01-01T20:24:00.000000Z\ttrue\tL\t0.2166743842940404\t0.7083\t912\t2015-07-19T22:51:11.746Z\t\t-4165375866282629335\t1970-01-01T00:00:00.000046Z\t24\t\t\t0xd5a5bcaf36055acccc06bd99b1afca80b492d3739f6948cf649df4ca93efb777\n" +
                        "103\tmsft\t21.451\t2018-01-01T20:36:00.000000Z\ttrue\tR\t0.7741404486334744\t0.6291\t738\t2015-02-26T12:33:46.542Z\tGDUO\t-4003083513462717585\t2018-01-09T00:05:11.080009Z\t13\t\t\t0x7dfeadb18f51ccfa4dc9a404d88181f8a3a08e18209dcf4fc696f3d96dbe647e\n" +
                        "104\tmsft\t27.489\t2018-01-01T20:48:00.000000Z\ttrue\tG\t0.3061431876107684\t0.3384\t1022\t2015-03-24T22:14:26.863Z\t\t-4081772434580927150\t1970-01-01T00:00:00.000047Z\t28\t00000000 67 2e b7 2c 51 0d 71 c2 1f 33 45 a3 13\tTNHCCCCJFNWM\t0xbcb25b47c75410589f7fcb0462bef2fb4479bf3d58b98cf52bf4390ddb933c8f\n" +
                        "105\tgoogl\t57.126\t2018-01-01T21:00:00.000000Z\ttrue\tG\t0.04128074839201168\t0.8127\t342\t2015-03-13T13:17:33.795Z\t\t-8164350634629323810\t2018-01-09T00:05:12.320036Z\t28\t00000000 5d 7d f7 ac e8 a3 9d 85 99 1e 70\tIKHZYNCWTYUFSGNC\t0xe3223b501baffd69a67255299571ae654b5df34ff36e5ab34cc2ef2530d5238f\n" +
                        "106\tibm\t73.892\t2018-01-01T21:12:00.000000Z\tfalse\tY\t0.8257251025407386\tNaN\t271\t2015-12-16T03:43:46.886Z\tPGCX\t-8388698483687936051\t1970-01-01T00:00:00.000048Z\t33\t00000000 b9 ae 63 a6 94 d5 f8 87 dd 0a 8f 30 72 03 b7\tOKPNRIGPPQSOJYNN\t0x95634bb9ceb8d6c653b74eb700b0dad644413aeb9db6c08351c1201dbb8581c7\n" +
                        "107\tibm\t7.140000000000001\t2018-01-01T21:24:00.000000Z\ttrue\tD\t0.8420355325690225\t0.5887\t139\t2015-10-18T01:18:43.150Z\tPGCX\t-6038781983929956494\t2018-01-09T00:05:12.240027Z\t17\t00000000 16 c4 fe 8a 91 ed 7c 05 7a 28 4a c9 37 60 9a bd\n" +
                        "00000010 fe 57\tVEXLSMMZCFF\t0x90a3dc96ee345ea2e44c012d13baa5a7b51da34cfb2503c197d1ecadff42139e\n" +
                        "108\tgoogl\t2.963\t2018-01-01T21:36:00.000000Z\tfalse\tN\t0.3842050915110937\t0.3310\t868\t2015-01-02T07:35:22.731Z\t\t4548053808202109689\t1970-01-01T00:00:00.000049Z\t22\t00000000 49 cd 9e 11 8c cc ca bd 7f cb e5 2d c8 0e 10 6d\n" +
                        "00000010 a3 22 13\tRELHQT\t0x59357e364dbe19a2a1c85d3d46985f678ce795887b179b23431c896f0ac52f12\n" +
                        "109\tmsft\t77.389\t2018-01-01T21:48:00.000000Z\tfalse\tW\t0.799469393902194\t0.1755\t277\t2015-06-18T18:05:19.804Z\t\t5046726778735149700\t2018-01-09T00:05:12.160018Z\t26\t00000000 f4 72 4e ce 42 a7 a6 00 e5 95 bd 33 a5 58 66 53\n" +
                        "00000010 21 7e 15 d4\tYZIEMQYYH\t0x4952818f6e7e5c1b17d5addf741ece5378711ad3a4504de7753f9c4ac8c0a9cf\n" +
                        "110\tmsft\t37.04\t2018-01-01T22:00:00.000000Z\tfalse\tM\tNaN\t0.4415\t669\t2015-11-08T09:20:22.648Z\t\t-6124052434628000760\t1970-01-01T00:00:00.000050Z\t39\t00000000 1a 16 03 9d 87 ac 5e 22 75 95\tZHJPS\t0x56cf62fe076558f494d0463c32814a457a042f77fa258b464547b82b7bccaf3c\n";

                assertCursor(
                        expected,
                        factory,
                        true,
                        true,
                        true,
                        true
                );
            }
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

            compiler.compile("alter table x add column z double", sqlExecutionContext);

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
                                w2
                        )
                );

                w1.tick();

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
                                w2
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

            compiler.compile("alter table x drop column j", sqlExecutionContext);

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

        compiler.compile("alter table x add column z double", sqlExecutionContext);

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

        public boolean publishSyncCmd(String tableName, long tableId, TableWriter slave) {
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
                        slave.getRawMetaMemorySize()
                );

                cmdPubSeq.done(cursor);
                return true;
            }
            return false;
        }
    }
}
