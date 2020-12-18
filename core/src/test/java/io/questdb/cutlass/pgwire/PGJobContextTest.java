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

package io.questdb.cutlass.pgwire;

import io.questdb.cutlass.NetUtils;
import io.questdb.griffin.AbstractGriffinTest;
import io.questdb.griffin.engine.functions.rnd.SharedRandom;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.network.*;
import io.questdb.std.Chars;
import io.questdb.std.Numbers;
import io.questdb.std.Rnd;
import io.questdb.std.microtime.TimestampFormatUtils;
import io.questdb.std.str.CharSink;
import io.questdb.std.str.StringSink;
import io.questdb.test.tools.TestUtils;
import org.jetbrains.annotations.NotNull;
import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;
import org.postgresql.PGResultSetMetaData;
import org.postgresql.copy.CopyIn;
import org.postgresql.copy.CopyManager;
import org.postgresql.core.BaseConnection;
import org.postgresql.util.PGTimestamp;
import org.postgresql.util.PSQLException;

import java.io.IOException;
import java.io.InputStream;
import java.sql.*;
import java.util.GregorianCalendar;
import java.util.Properties;
import java.util.TimeZone;
import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.atomic.AtomicBoolean;

import static io.questdb.std.Numbers.hexDigits;
import static org.junit.Assert.assertTrue;

public class PGJobContextTest extends AbstractGriffinTest {

    private static final Log LOG = LogFactory.getLog(PGJobContextTest.class);

    @Test
    public void largeBatchInsertMethod() throws Exception {

        assertMemoryLeak(() -> {

            final PGWireConfiguration conf = new DefaultPGWireConfiguration() {
                @Override
                public int[] getWorkerAffinity() {
                    return new int[]{-1, -1, -1, -1};
                }

                @Override
                public int getWorkerCount() {
                    return 4;
                }
            };

            try (final PGWireServer ignored = PGWireServer.create(
                    conf,
                    null,
                    LOG,
                    engine,
                    compiler.getFunctionFactoryCache()
            )) {
                try (Connection connection = DriverManager.getConnection("jdbc:postgresql://localhost:8812/", "admin", "quest")) {
                    try (Statement statement = connection.createStatement()) {
                        statement.executeUpdate("create table test_large_batch(id long,val int)");
                    }
                    connection.setAutoCommit(false);
                    try (PreparedStatement batchInsert = connection.prepareStatement("insert into test_large_batch(id,val) values(?,?)")) {
                        for (int i = 0; i < 10_000; i++) {
                            batchInsert.setLong(1, 0L);
                            batchInsert.setInt(2, 1);
                            batchInsert.addBatch();
                            batchInsert.setLong(1, 1L);
                            batchInsert.setInt(2, 2);
                            batchInsert.addBatch();
                            batchInsert.setLong(1, 2L);
                            batchInsert.setInt(2, 3);
                            batchInsert.addBatch();
                            batchInsert.clearParameters();
                            batchInsert.executeLargeBatch();
                        }
                        connection.commit();
                    }

                    StringSink sink = new StringSink();
                    String expected = "count[BIGINT]\n" +
                            "30000\n";
                    Statement statement = connection.createStatement();
                    ResultSet rs = statement.executeQuery("select count(*) from test_large_batch");
                    assertResultSet(expected, sink, rs);
                }
            }
        });
    }

    @Test
    public void regularBatchInsertMethod() throws Exception {

        assertMemoryLeak(() -> {

            final CountDownLatch haltLatch = new CountDownLatch(1);
            final AtomicBoolean running = new AtomicBoolean(true);

            try {
                startBasicServer(NetworkFacadeImpl.INSTANCE,
                        new DefaultPGWireConfiguration(),
                        haltLatch,
                        running
                );


                try (Connection connection = DriverManager.getConnection("jdbc:postgresql://localhost:9120/", "admin", "quest")) {
                    try (Statement statement = connection.createStatement()) {
                        statement.executeUpdate("create table test_batch(id long,val int)");
                    }
                    try (PreparedStatement batchInsert = connection.prepareStatement("insert into test_batch(id,val) values(?,?)")) {
                        batchInsert.setLong(1, 0L);
                        batchInsert.setInt(2, 1);
                        batchInsert.addBatch();
                        batchInsert.setLong(1, 1L);
                        batchInsert.setInt(2, 2);
                        batchInsert.addBatch();
                        batchInsert.setLong(1, 2L);
                        batchInsert.setInt(2, 3);
                        batchInsert.addBatch();
                        batchInsert.clearParameters();
                        batchInsert.executeBatch();
                    }

                    StringSink sink = new StringSink();
                    String expected = "id[BIGINT],val[INTEGER]\n" +
                            "0,1\n" +
                            "1,2\n" +
                            "2,3\n";
                    Statement statement = connection.createStatement();
                    ResultSet rs = statement.executeQuery("select * from test_batch");
                    assertResultSet(expected, sink, rs);
                }
            } finally {
                running.set(false);
                haltLatch.await();
            }
        });
    }

    @Test
//this looks like the same script as the preparedStatementHex()
    public void testAllParamsHex() throws Exception {
        final String script = ">0000006e00030000757365720078797a0064617461626173650071646200636c69656e745f656e636f64696e67005554463800446174655374796c650049534f0054696d655a6f6e65004575726f70652f4c6f6e646f6e0065787472615f666c6f61745f64696769747300320000\n" +
                "<520000000800000003\n" +
                ">70000000076f6800\n" +
                "<520000000800000000530000001154696d655a6f6e6500474d5400530000001d6170706c69636174696f6e5f6e616d6500517565737444420053000000187365727665725f76657273696f6e0031312e33005300000019696e74656765725f6461746574696d6573006f6e005300000019636c69656e745f656e636f64696e670055544638005a0000000549\n" +
                ">5000000022005345542065787472615f666c6f61745f646967697473203d2033000000420000000c0000000000000000450000000900000000015300000004\n" +
                "<310000000432000000044300000008534554005a0000000549\n" +
                ">500000003700534554206170706c69636174696f6e5f6e616d65203d2027506f737467726553514c204a4442432044726976657227000000420000000c0000000000000000450000000900000000015300000004\n" +
                "<310000000432000000044300000008534554005a0000000549\n" +
                ">500000002a0073656c65637420312c322c332066726f6d206c6f6e675f73657175656e6365283129000000420000000c000000000000000044000000065000450000000900000000005300000004\n" +
                "<31000000043200000004540000004200033100000040010001000000170004ffffffff00003200000040010002000000170004ffffffff00003300000040010003000000170004ffffffff000044000000150003000000013100000001320000000133430000000d53454c4543542031005a0000000549\n" +
                ">50000000260073656c65637420312066726f6d206c6f6e675f73657175656e6365283229000000420000000c000000000000000044000000065000450000000900000000005300000004\n" +
                "<31000000043200000004540000001a00013100000040010001000000170004ffffffff0000440000000b00010000000131440000000b00010000000131430000000d53454c4543542032005a0000000549\n" +
                ">500000002a0073656c65637420312c322c332066726f6d206c6f6e675f73657175656e6365283129000000420000000c000000000000000044000000065000450000000900000000005300000004\n" +
                "<31000000043200000004540000004200033100000040010001000000170004ffffffff00003200000040010002000000170004ffffffff00003300000040010003000000170004ffffffff000044000000150003000000013100000001320000000133430000000d53454c4543542031005a0000000549\n" +
                ">50000000260073656c65637420312066726f6d206c6f6e675f73657175656e6365283229000000420000000c000000000000000044000000065000450000000900000000005300000004\n" +
                "<31000000043200000004540000001a00013100000040010001000000170004ffffffff0000440000000b00010000000131440000000b00010000000131430000000d53454c4543542032005a0000000549\n" +
                ">500000002a0073656c65637420312c322c332066726f6d206c6f6e675f73657175656e6365283129000000420000000c000000000000000044000000065000450000000900000000005300000004\n" +
                "<31000000043200000004540000004200033100000040010001000000170004ffffffff00003200000040010002000000170004ffffffff00003300000040010003000000170004ffffffff000044000000150003000000013100000001320000000133430000000d53454c4543542031005a0000000549\n" +
                ">50000000260073656c65637420312066726f6d206c6f6e675f73657175656e6365283229000000420000000c000000000000000044000000065000450000000900000000005300000004\n" +
                "<31000000043200000004540000001a00013100000040010001000000170004ffffffff0000440000000b00010000000131440000000b00010000000131430000000d53454c4543542032005a0000000549\n" +
                ">500000002a0073656c65637420312c322c332066726f6d206c6f6e675f73657175656e6365283129000000420000000c000000000000000044000000065000450000000900000000005300000004\n" +
                "<31000000043200000004540000004200033100000040010001000000170004ffffffff00003200000040010002000000170004ffffffff00003300000040010003000000170004ffffffff000044000000150003000000013100000001320000000133430000000d53454c4543542031005a0000000549\n" +
                ">50000000260073656c65637420312066726f6d206c6f6e675f73657175656e6365283229000000420000000c000000000000000044000000065000450000000900000000005300000004\n" +
                "<31000000043200000004540000001a00013100000040010001000000170004ffffffff0000440000000b00010000000131440000000b00010000000131430000000d53454c4543542032005a0000000549\n" +
                ">500000002d535f310073656c65637420312c322c332066726f6d206c6f6e675f73657175656e6365283129000000420000000f00535f310000000000000044000000065000450000000900000000005300000004\n" +
                "<31000000043200000004540000004200033100000040010001000000170004ffffffff00003200000040010002000000170004ffffffff00003300000040010003000000170004ffffffff000044000000150003000000013100000001320000000133430000000d53454c4543542031005a0000000549\n" +
                ">50000000260073656c65637420312066726f6d206c6f6e675f73657175656e6365283229000000420000000c000000000000000044000000065000450000000900000000005300000004\n" +
                "<31000000043200000004540000001a00013100000040010001000000170004ffffffff0000440000000b00010000000131440000000b00010000000131430000000d53454c4543542032005a0000000549\n" +
                ">420000000f00535f3100000000000000450000000900000000005300000004\n" +
                "<320000000444000000150003000000013100000001320000000133430000000d53454c4543542031005a0000000549\n" +
                ">50000000260073656c65637420312066726f6d206c6f6e675f73657175656e6365283229000000420000000c000000000000000044000000065000450000000900000000005300000004\n" +
                "<31000000043200000004540000001a00013100000040010001000000170004ffffffff0000440000000b00010000000131440000000b00010000000131430000000d53454c4543542032005a0000000549\n" +
                ">420000000f00535f3100000000000000450000000900000000005300000004\n" +
                "<320000000444000000150003000000013100000001320000000133430000000d53454c4543542031005a0000000549\n" +
                ">50000000260073656c65637420312066726f6d206c6f6e675f73657175656e6365283229000000420000000c000000000000000044000000065000450000000900000000005300000004\n" +
                "<31000000043200000004540000001a00013100000040010001000000170004ffffffff0000440000000b00010000000131440000000b00010000000131430000000d53454c4543542032005a0000000549\n" +
                ">420000000f00535f3100000000000000450000000900000000005300000004\n" +
                "<320000000444000000150003000000013100000001320000000133430000000d53454c4543542031005a0000000549\n" +
                ">50000000260073656c65637420312066726f6d206c6f6e675f73657175656e6365283229000000420000000c000000000000000044000000065000450000000900000000005300000004\n" +
                "<31000000043200000004540000001a00013100000040010001000000170004ffffffff0000440000000b00010000000131440000000b00010000000131430000000d53454c4543542032005a0000000549\n" +
                ">420000000f00535f3100000000000000450000000900000000005300000004\n" +
                "<320000000444000000150003000000013100000001320000000133430000000d53454c4543542031005a0000000549\n" +
                ">50000000260073656c65637420312066726f6d206c6f6e675f73657175656e6365283229000000420000000c000000000000000044000000065000450000000900000000005300000004\n" +
                "<31000000043200000004540000001a00013100000040010001000000170004ffffffff0000440000000b00010000000131440000000b00010000000131430000000d53454c4543542032005a0000000549\n" +
                ">420000000f00535f3100000000000000450000000900000000005300000004\n" +
                "<320000000444000000150003000000013100000001320000000133430000000d53454c4543542031005a0000000549\n" +
                ">50000000260073656c65637420312066726f6d206c6f6e675f73657175656e6365283229000000420000000c000000000000000044000000065000450000000900000000005300000004\n" +
                "<31000000043200000004540000001a00013100000040010001000000170004ffffffff0000440000000b00010000000131440000000b00010000000131430000000d53454c4543542032005a0000000549\n" +
                ">5800000004";
        assertHexScript(
                getFragmentedSendFacade(),
                NetworkFacadeImpl.INSTANCE,
                script,
                getHexPgWireConfig()
        );
    }

    @Test
    public void testBadMessageLength() throws Exception {
        final String script =
                ">0000006900030000757365720078797a006461746162617365006e6162755f61707000636c69656e745f656e636f64696e67005554463800446174655374796c650049534f0054696d655a6f6e6500474d540065787472615f666c6f61745f64696769747300320000\n" +
                        "<520000000800000003\n" +
                        ">70000000006f6800\n" +
                        "<!!";
        assertHexScript(
                getFragmentedSendFacade(),
                NetworkFacadeImpl.INSTANCE,
                script,
                new DefaultPGWireConfiguration()
        );
    }

    @Test
    public void testBadPasswordLength() throws Exception {
        assertHexScript(
                NetworkFacadeImpl.INSTANCE,
                NetworkFacadeImpl.INSTANCE,
                ">0000000804d2162f\n" +
                        "<4e\n" +
                        ">0000007500030000757365720061646d696e006461746162617365006e6162755f61707000636c69656e745f656e636f64696e67005554463800446174655374796c650049534f0054696d655a6f6e65004575726f70652f4c6f6e646f6e0065787472615f666c6f61745f64696769747300320000\n" +
                        "<520000000800000003\n" +
                        ">700000000464756e6e6f00\n" +
                        "<!!",
                new DefaultPGWireConfiguration()
        );
    }

    @Test
    public void testBatchInsertWithTransaction() throws Exception {
        assertMemoryLeak(() -> {

            final CountDownLatch haltLatch = new CountDownLatch(1);
            final AtomicBoolean running = new AtomicBoolean(true);

            try {
                startBasicServer(NetworkFacadeImpl.INSTANCE,
                        new DefaultPGWireConfiguration(),
                        haltLatch,
                        running
                );

                Properties properties = new Properties();
                properties.setProperty("user", "admin");
                properties.setProperty("password", "quest");
                properties.setProperty("sslmode", "disable");
                final Connection connection = DriverManager.getConnection("jdbc:postgresql://127.0.0.1:9120/qdb", properties);
                try (Statement statement = connection.createStatement()) {
                    statement.executeUpdate("create table test (id long,val int)");
                    statement.executeUpdate("create table test2(id long,val int)");
                }

                connection.setAutoCommit(false);
                try (PreparedStatement batchInsert = connection.prepareStatement("insert into test(id,val) values(?,?)")) {
                    batchInsert.setLong(1, 0L);
                    batchInsert.setInt(2, 1);
                    batchInsert.addBatch();
                    batchInsert.setLong(1, 1L);
                    batchInsert.setInt(2, 2);
                    batchInsert.addBatch();
                    batchInsert.setLong(1, 2L);
                    batchInsert.setInt(2, 3);
                    batchInsert.addBatch();
                    batchInsert.clearParameters();
                    batchInsert.executeLargeBatch();
                }

                try (PreparedStatement batchInsert = connection.prepareStatement("insert into test2(id,val) values(?,?)")) {
                    batchInsert.setLong(1, 0L);
                    batchInsert.setInt(2, 1);
                    batchInsert.addBatch();
                    batchInsert.setLong(1, 1L);
                    batchInsert.setInt(2, 2);
                    batchInsert.addBatch();
                    batchInsert.setLong(1, 2L);
                    batchInsert.setInt(2, 3);
                    batchInsert.addBatch();
                    batchInsert.clearParameters();
                    batchInsert.executeLargeBatch();
                }

                connection.commit();

                connection.setAutoCommit(true);
                StringSink sink = new StringSink();
                String expected = "id[BIGINT],val[INTEGER]\n" +
                        "0,1\n" +
                        "1,2\n" +
                        "2,3\n";
                Statement statement = connection.createStatement();
                ResultSet rs = statement.executeQuery("select * from test");
                assertResultSet(expected, sink, rs);

                sink.clear();
                Statement statement2 = connection.createStatement();
                ResultSet rs2 = statement2.executeQuery("select * from test2");
                assertResultSet(expected, sink, rs2);

                //now switch on autocommit and check that data is inserted without explicitly calling commit()
                connection.setAutoCommit(true);
                try (PreparedStatement batchInsert = connection.prepareStatement("insert into test(id,val) values(?,?)")) {
                    batchInsert.setLong(1, 3L);
                    batchInsert.setInt(2, 4);
                    batchInsert.addBatch();
                    batchInsert.setLong(1, 4L);
                    batchInsert.setInt(2, 5);
                    batchInsert.addBatch();
                    batchInsert.setLong(1, 5L);
                    batchInsert.setInt(2, 6);
                    batchInsert.addBatch();
                    batchInsert.clearParameters();
                    batchInsert.executeLargeBatch();
                }

                sink.clear();
                expected = "id[BIGINT],val[INTEGER]\n" +
                        "0,1\n" +
                        "1,2\n" +
                        "2,3\n" +
                        "3,4\n" +
                        "4,5\n" +
                        "5,6\n";
                Statement statement3 = connection.createStatement();
                ResultSet rs3 = statement3.executeQuery("select * from test");
                assertResultSet(expected, sink, rs3);

                //now fail insertion during transaction
                try (Statement statement4 = connection.createStatement()) {
                    statement4.executeUpdate("create table anothertab(id long, val int, k timestamp) timestamp(k) ");
                }
                connection.setAutoCommit(false);
                try (PreparedStatement batchInsert = connection.prepareStatement("insert into anothertab(id, val, k) values(?,?,?)")) {
                    batchInsert.setLong(1, 3L);
                    batchInsert.setInt(2, 4);
                    batchInsert.setLong(3, 1_000L);
                    batchInsert.addBatch();
                    batchInsert.setLong(1, 4L);
                    batchInsert.setInt(2, 5);
                    batchInsert.setLong(3, 0L);
                    batchInsert.addBatch();
                    batchInsert.setLong(1, 5L);
                    batchInsert.setInt(2, 6);
                    batchInsert.setLong(3, 2_000L);
                    batchInsert.addBatch();
                    batchInsert.clearParameters();
                    batchInsert.executeLargeBatch();
                    Assert.fail();
                } catch (Exception e) {
                    LOG.error().$(e).$();
                }
                //now transaction fail, we should rollback transaction
                connection.rollback();
                connection.setAutoCommit(true);
                sink.clear();
                expected = "id[BIGINT],val[INTEGER],k[TIMESTAMP]\n";
                Statement statement4 = connection.createStatement();
                ResultSet rs4 = statement4.executeQuery("select * from anothertab");
                assertResultSet(expected, sink, rs4);

                //
                connection.close();
            } finally {
                running.set(false);
                haltLatch.await();
            }
        });
    }

    @Test
    public void testBlobOverLimit() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            final CountDownLatch haltLatch = new CountDownLatch(1);
            final AtomicBoolean running = new AtomicBoolean(true);
            try {
                startBasicServer(NetworkFacadeImpl.INSTANCE,
                        new DefaultPGWireConfiguration() {
                            @Override
                            public int getMaxBlobSizeOnQuery() {
                                return 150;
                            }
                        },
                        haltLatch,
                        running
                );

                Properties properties = new Properties();
                properties.setProperty("user", "admin");
                properties.setProperty("password", "quest");

                final Connection connection = DriverManager.getConnection("jdbc:postgresql://127.0.0.1:9120/qdb", properties);
                Statement statement = connection.createStatement();

                try {
                    statement.executeQuery(
                            "select " +
                                    "rnd_str(4,4,4) s, " +
                                    "rnd_int(0, 256, 4) i, " +
                                    "rnd_double(4) d, " +
                                    "timestamp_sequence(0,10000) t, " +
                                    "rnd_float(4) f, " +
                                    "rnd_short() _short, " +
                                    "rnd_long(0, 10000000, 5) l, " +
                                    "rnd_timestamp(to_timestamp('2015','yyyy'),to_timestamp('2016','yyyy'),2) ts2, " +
                                    "rnd_byte(0,127) bb, " +
                                    "rnd_boolean() b, " +
                                    "rnd_symbol(4,4,4,2), " +
                                    "rnd_date(to_date('2015', 'yyyy'), to_date('2016', 'yyyy'), 2)," +
                                    "rnd_bin(1024,2048,2) " +
                                    "from long_sequence(50)");
                    Assert.fail();
                } catch (PSQLException e) {
                    Assert.assertEquals("blob is too large [blobSize=1903, max=150, columnIndex=12]", e.getServerErrorMessage().getMessage());
                }

                connection.close();
            } finally {
                running.set(false);
                haltLatch.await();
            }
        });
    }

    @Test
    public void testBrokenUtf8QueryInParseMessage() throws Exception {
        assertHexScript(
                NetworkFacadeImpl.INSTANCE,
                NetworkFacadeImpl.INSTANCE,
                ">0000000804d2162f\n" +
                        "<4e\n" +
                        ">0000007500030000757365720061646d696e006461746162617365006e6162755f61707000636c69656e745f656e636f64696e67005554463800446174655374796c650049534f0054696d655a6f6e65004575726f70652f4c6f6e646f6e0065787472615f666c6f61745f64696769747300320000\n" +
                        "<520000000800000003\n" +
                        ">700000000a717565737400\n" +
                        "<520000000800000000530000001154696d655a6f6e6500474d5400530000001d6170706c69636174696f6e5f6e616d6500517565737444420053000000187365727665725f76657273696f6e0031312e33005300000019696e74656765725f6461746574696d6573006f6e005300000019636c69656e745f656e636f64696e670055544638005a0000000549\n" +
                        ">50000000220053ac542065787472615f666c6f61745f646967697473203d2033000000420000000c0000000000000000450000000900000000015300000004\n" +
                        "<!!"
                , new DefaultPGWireConfiguration()
        );
    }

    @Test
    public void testCharIntLongDoubleBooleanParametersWithoutExplicitParameterTypeHex() throws Exception {
        String script = ">0000006e00030000757365720078797a0064617461626173650071646200636c69656e745f656e636f64696e67005554463800446174655374796c650049534f0054696d655a6f6e65004575726f70652f4c6f6e646f6e0065787472615f666c6f61745f64696769747300320000\n" +
                "<520000000800000003\n" +
                ">70000000076f6800\n" +
                "<520000000800000000530000001154696d655a6f6e6500474d5400530000001d6170706c69636174696f6e5f6e616d6500517565737444420053000000187365727665725f76657273696f6e0031312e33005300000019696e74656765725f6461746574696d6573006f6e005300000019636c69656e745f656e636f64696e670055544638005a0000000549\n" +
                ">50000000300073656c65637420782c202024312c2024322066726f6d206c6f6e675f73657175656e63652832293b000000\n" +
                ">4200000021000000010000000200000001330000000a353030303030303030300000\n" +
                ">44000000065000\n" +
                ">45000000090000000000\n" +
                ">4800000004\n" +
                "<31000000043200000004540000004400037800000040010001000000140004ffffffff0000243100000040010002000000170004ffffffff0000243200000040010003000000140004ffffffff0000440000001e0003000000013100000001330000000a35303030303030303030440000001e0003000000013200000001330000000a35303030303030303030430000000d53454c454354203200\n";

        assertHexScript(NetworkFacadeImpl.INSTANCE,
                NetworkFacadeImpl.INSTANCE,
                script,
                getHexPgWireConfig());
    }

    @Test
    public void testCloseMessageFollowedByNewQueryHex() throws Exception {
        String script = ">0000006e00030000757365720078797a0064617461626173650071646200636c69656e745f656e636f64696e67005554463800446174655374796c650049534f0054696d655a6f6e65004575726f70652f4c6f6e646f6e0065787472615f666c6f61745f64696769747300320000\n" +
                "<520000000800000003\n" +
                ">70000000076f6800\n" +
                "<520000000800000000530000001154696d655a6f6e6500474d5400530000001d6170706c69636174696f6e5f6e616d6500517565737444420053000000187365727665725f76657273696f6e0031312e33005300000019696e74656765725f6461746574696d6573006f6e005300000019636c69656e745f656e636f64696e670055544638005a0000000549\n" +
                ">5000000022005345542065787472615f666c6f61745f646967697473203d2033000000420000000c0000000000000000450000000900000000015300000004\n" +
                "<310000000432000000044300000008534554005a0000000549\n" +
                ">500000003700534554206170706c69636174696f6e5f6e616d65203d2027506f737467726553514c204a4442432044726976657227000000420000000c0000000000000000450000000900000000015300000004\n" +
                "<310000000432000000044300000008534554005a0000000549\n" +
                ">500000002a0073656c65637420312c322c332066726f6d206c6f6e675f73657175656e6365283129000000420000000c000000000000000044000000065000450000000900000000005300000004\n" +
                "<31000000043200000004540000004200033100000040010001000000170004ffffffff00003200000040010002000000170004ffffffff00003300000040010003000000170004ffffffff000044000000150003000000013100000001320000000133430000000d53454c4543542031005a0000000549\n" +
                ">500000002d535f310073656c65637420312c322c332066726f6d206c6f6e675f73657175656e6365283129000000420000000f00535f310000000000000044000000065000450000000900000000005300000004\n" +
                "<31000000043200000004540000004200033100000040010001000000170004ffffffff00003200000040010002000000170004ffffffff00003300000040010003000000170004ffffffff000044000000150003000000013100000001320000000133430000000d53454c4543542031005a0000000549\n" +
                ">420000000f00535f3100000000000000450000000900000000005300000004\n" +
                "<320000000444000000150003000000013100000001320000000133430000000d53454c4543542031005a0000000549\n" +
                ">420000000f00535f3100000000000000450000000900000000005300000004\n" +
                "<320000000444000000150003000000013100000001320000000133430000000d53454c4543542031005a0000000549\n" +
                ">430000000953535f310050000000260073656c65637420312066726f6d206c6f6e675f73657175656e6365283229000000420000000c000000000000000044000000065000450000000900000000005300000004\n" +
                "<330000000431000000043200000004540000001a00013100000040010001000000170004ffffffff0000440000000b00010000000131440000000b00010000000131430000000d53454c4543542032005a0000000549\n" +
                ">5800000004";
        assertHexScript(NetworkFacadeImpl.INSTANCE,
                NetworkFacadeImpl.INSTANCE,
                script,
                getHexPgWireConfig());
    }

    @Test
    public void testCloseMessageForPortalHex() throws Exception {
        String script = ">0000006e00030000757365720078797a0064617461626173650071646200636c69656e745f656e636f64696e67005554463800446174655374796c650049534f0054696d655a6f6e65004575726f70652f4c6f6e646f6e0065787472615f666c6f61745f64696769747300320000\n" +
                "<520000000800000003\n" +
                ">70000000076f6800\n" +
                "<520000000800000000530000001154696d655a6f6e6500474d5400530000001d6170706c69636174696f6e5f6e616d6500517565737444420053000000187365727665725f76657273696f6e0031312e33005300000019696e74656765725f6461746574696d6573006f6e005300000019636c69656e745f656e636f64696e670055544638005a0000000549\n" +
                ">5000000022005345542065787472615f666c6f61745f646967697473203d2033000000420000000c0000000000000000450000000900000000015300000004\n" +
                "<310000000432000000044300000008534554005a0000000549\n" +
                ">500000003700534554206170706c69636174696f6e5f6e616d65203d2027506f737467726553514c204a4442432044726976657227000000420000000c0000000000000000450000000900000000015300000004\n" +
                "<310000000432000000044300000008534554005a0000000549\n" +
                ">500000002a0073656c65637420312c322c332066726f6d206c6f6e675f73657175656e6365283129000000420000000c000000000000000044000000065000450000000900000000005300000004\n" +
                "<31000000043200000004540000004200033100000040010001000000170004ffffffff00003200000040010002000000170004ffffffff00003300000040010003000000170004ffffffff000044000000150003000000013100000001320000000133430000000d53454c4543542031005a0000000549\n" +
                ">500000002d535f310073656c65637420312c322c332066726f6d206c6f6e675f73657175656e6365283129000000420000000f00535f310000000000000044000000065000450000000900000000005300000004\n" +
                "<31000000043200000004540000004200033100000040010001000000170004ffffffff00003200000040010002000000170004ffffffff00003300000040010003000000170004ffffffff000044000000150003000000013100000001320000000133430000000d53454c4543542031005a0000000549\n" +
                ">420000000f00535f3100000000000000450000000900000000005300000004\n" +
                "<320000000444000000150003000000013100000001320000000133430000000d53454c4543542031005a0000000549\n" +
                ">420000000f00535f3100000000000000450000000900000000005300000004\n" +
                "<320000000444000000150003000000013100000001320000000133430000000d53454c4543542031005a0000000549\n" +
                ">430000000950535f31005300000004\n" +
                "<33000000045a0000000549\n" +
                ">5800000004";
        assertHexScript(NetworkFacadeImpl.INSTANCE,
                NetworkFacadeImpl.INSTANCE,
                script,
                getHexPgWireConfig());
    }

    @Test
    public void testCloseMessageForSelectWithParamsHex() throws Exception {
        //hex for close message 43 00000009 53 535f31 00
        String script = ">0000006e00030000757365720078797a0064617461626173650071646200636c69656e745f656e636f64696e67005554463800446174655374796c650049534f0054696d655a6f6e65004575726f70652f4c6f6e646f6e0065787472615f666c6f61745f64696769747300320000\n" +
                "<520000000800000003\n" +
                ">70000000076f6800\n" +
                "<520000000800000000530000001154696d655a6f6e6500474d5400530000001d6170706c69636174696f6e5f6e616d6500517565737444420053000000187365727665725f76657273696f6e0031312e33005300000019696e74656765725f6461746574696d6573006f6e005300000019636c69656e745f656e636f64696e670055544638005a0000000549\n" +
                ">5000000022005345542065787472615f666c6f61745f646967697473203d2033000000420000000c0000000000000000450000000900000000015300000004\n" +
                "<310000000432000000044300000008534554005a0000000549\n" +
                ">500000003700534554206170706c69636174696f6e5f6e616d65203d2027506f737467726553514c204a4442432044726976657227000000420000000c0000000000000000450000000900000000015300000004\n" +
                "<310000000432000000044300000008534554005a0000000549\n" +
                ">500000003b0073656c65637420782c24312c24322c24332066726f6d206c6f6e675f73657175656e63652832290000030000001700000014000002bd420000002600000003000000000000000300000001340000000331323300000004352e3433000044000000065000450000000900000000005300000004\n" +
                "<31000000043200000004540000005900047800000040010001000000140004ffffffff0000243100000040010002000000170004ffffffff0000243200000040010003000000140004ffffffff0000243300000040010004000002bd0004ffffffff0000440000001f0004000000013100000001340000000331323300000004352e3433440000001f0004000000013200000001340000000331323300000004352e3433430000000d53454c4543542032005a0000000549\n" +
                ">500000003b0073656c65637420782c24312c24322c24332066726f6d206c6f6e675f73657175656e63652832290000030000001700000014000002bd420000002600000003000000000000000300000001340000000331323300000004352e3433000044000000065000450000000900000000005300000004\n" +
                "<31000000043200000004540000005900047800000040010001000000140004ffffffff0000243100000040010002000000170004ffffffff0000243200000040010003000000140004ffffffff0000243300000040010004000002bd0004ffffffff0000440000001f0004000000013100000001340000000331323300000004352e3433440000001f0004000000013200000001340000000331323300000004352e3433430000000d53454c4543542032005a0000000549\n" +
                ">500000003b0073656c65637420782c24312c24322c24332066726f6d206c6f6e675f73657175656e63652832290000030000001700000014000002bd420000002600000003000000000000000300000001340000000331323300000004352e3433000044000000065000450000000900000000005300000004\n" +
                "<31000000043200000004540000005900047800000040010001000000140004ffffffff0000243100000040010002000000170004ffffffff0000243200000040010003000000140004ffffffff0000243300000040010004000002bd0004ffffffff0000440000001f0004000000013100000001340000000331323300000004352e3433440000001f0004000000013200000001340000000331323300000004352e3433430000000d53454c4543542032005a0000000549\n" +
                ">500000003b0073656c65637420782c24312c24322c24332066726f6d206c6f6e675f73657175656e63652832290000030000001700000014000002bd420000002600000003000000000000000300000001340000000331323300000004352e3433000044000000065000450000000900000000005300000004\n" +
                "<31000000043200000004540000005900047800000040010001000000140004ffffffff0000243100000040010002000000170004ffffffff0000243200000040010003000000140004ffffffff0000243300000040010004000002bd0004ffffffff0000440000001f0004000000013100000001340000000331323300000004352e3433440000001f0004000000013200000001340000000331323300000004352e3433430000000d53454c4543542032005a0000000549\n" +
                ">500000003e535f310073656c65637420782c24312c24322c24332066726f6d206c6f6e675f73657175656e63652832290000030000001700000014000002bd420000002900535f31000003000000000000000300000001340000000331323300000004352e3433000044000000065000450000000900000000005300000004\n" +
                "<31000000043200000004540000005900047800000040010001000000140004ffffffff0000243100000040010002000000170004ffffffff0000243200000040010003000000140004ffffffff0000243300000040010004000002bd0004ffffffff0000440000001f0004000000013100000001340000000331323300000004352e3433440000001f0004000000013200000001340000000331323300000004352e3433430000000d53454c4543542032005a0000000549\n" +
                ">420000002900535f31000003000000000000000300000001340000000331323300000004352e34330000450000000900000000005300000004\n" +
                "<3200000004440000001f0004000000013100000001340000000331323300000004352e3433440000001f0004000000013200000001340000000331323300000004352e3433430000000d53454c4543542032005a0000000549\n" +
                ">430000000953535f31005300000004\n" +
                "<33000000045a0000000549\n" +
                ">5800000004";
        assertHexScript(NetworkFacadeImpl.INSTANCE,
                NetworkFacadeImpl.INSTANCE,
                script,
                getHexPgWireConfig());
    }

    @Test
    public void testCloseMessageHex() throws Exception {
        //hex for close message 43 00000009 53 535f31 00
        String script = ">0000006e00030000757365720078797a0064617461626173650071646200636c69656e745f656e636f64696e67005554463800446174655374796c650049534f0054696d655a6f6e65004575726f70652f4c6f6e646f6e0065787472615f666c6f61745f64696769747300320000\n" +
                "<520000000800000003\n" +
                ">70000000076f6800\n" +
                "<520000000800000000530000001154696d655a6f6e6500474d5400530000001d6170706c69636174696f6e5f6e616d6500517565737444420053000000187365727665725f76657273696f6e0031312e33005300000019696e74656765725f6461746574696d6573006f6e005300000019636c69656e745f656e636f64696e670055544638005a0000000549\n" +
                ">5000000022005345542065787472615f666c6f61745f646967697473203d2033000000420000000c0000000000000000450000000900000000015300000004\n" +
                "<310000000432000000044300000008534554005a0000000549\n" +
                ">500000003700534554206170706c69636174696f6e5f6e616d65203d2027506f737467726553514c204a4442432044726976657227000000420000000c0000000000000000450000000900000000015300000004\n" +
                "<310000000432000000044300000008534554005a0000000549\n" +
                ">500000002a0073656c65637420312c322c332066726f6d206c6f6e675f73657175656e6365283129000000420000000c000000000000000044000000065000450000000900000000005300000004\n" +
                "<31000000043200000004540000004200033100000040010001000000170004ffffffff00003200000040010002000000170004ffffffff00003300000040010003000000170004ffffffff000044000000150003000000013100000001320000000133430000000d53454c4543542031005a0000000549\n" +
                ">500000002d535f310073656c65637420312c322c332066726f6d206c6f6e675f73657175656e6365283129000000420000000f00535f310000000000000044000000065000450000000900000000005300000004\n" +
                "<31000000043200000004540000004200033100000040010001000000170004ffffffff00003200000040010002000000170004ffffffff00003300000040010003000000170004ffffffff000044000000150003000000013100000001320000000133430000000d53454c4543542031005a0000000549\n" +
                ">420000000f00535f3100000000000000450000000900000000005300000004\n" +
                "<320000000444000000150003000000013100000001320000000133430000000d53454c4543542031005a0000000549\n" +
                ">420000000f00535f3100000000000000450000000900000000005300000004\n" +
                "<320000000444000000150003000000013100000001320000000133430000000d53454c4543542031005a0000000549\n" +
                ">430000000953535f31005300000004\n" +
                "<33000000045a0000000549\n" +
                ">5800000004";
        assertHexScript(NetworkFacadeImpl.INSTANCE,
                NetworkFacadeImpl.INSTANCE,
                script,
                getHexPgWireConfig());
    }

    @Test
    public void testCloseMessageWithBadUtf8InStatementNameHex() throws Exception {
        String script = ">0000006e00030000757365720078797a0064617461626173650071646200636c69656e745f656e636f64696e67005554463800446174655374796c650049534f0054696d655a6f6e65004575726f70652f4c6f6e646f6e0065787472615f666c6f61745f64696769747300320000\n" +
                "<520000000800000003\n" +
                ">70000000076f6800\n" +
                "<520000000800000000530000001154696d655a6f6e6500474d5400530000001d6170706c69636174696f6e5f6e616d6500517565737444420053000000187365727665725f76657273696f6e0031312e33005300000019696e74656765725f6461746574696d6573006f6e005300000019636c69656e745f656e636f64696e670055544638005a0000000549\n" +
                ">5000000022005345542065787472615f666c6f61745f646967697473203d2033000000420000000c0000000000000000450000000900000000015300000004\n" +
                "<310000000432000000044300000008534554005a0000000549\n" +
                ">500000003700534554206170706c69636174696f6e5f6e616d65203d2027506f737467726553514c204a4442432044726976657227000000420000000c0000000000000000450000000900000000015300000004\n" +
                "<310000000432000000044300000008534554005a0000000549\n" +
                ">500000002a0073656c65637420312c322c332066726f6d206c6f6e675f73657175656e6365283129000000420000000c000000000000000044000000065000450000000900000000005300000004\n" +
                "<31000000043200000004540000004200033100000040010001000000170004ffffffff00003200000040010002000000170004ffffffff00003300000040010003000000170004ffffffff000044000000150003000000013100000001320000000133430000000d53454c4543542031005a0000000549\n" +
                ">500000002d535f310073656c65637420312c322c332066726f6d206c6f6e675f73657175656e6365283129000000420000000f00535f310000000000000044000000065000450000000900000000005300000004\n" +
                "<31000000043200000004540000004200033100000040010001000000170004ffffffff00003200000040010002000000170004ffffffff00003300000040010003000000170004ffffffff000044000000150003000000013100000001320000000133430000000d53454c4543542031005a0000000549\n" +
                ">420000000f00535f3100000000000000450000000900000000005300000004\n" +
                "<320000000444000000150003000000013100000001320000000133430000000d53454c4543542031005a0000000549\n" +
                ">420000000f00535f3100000000000000450000000900000000005300000004\n" +
                "<320000000444000000150003000000013100000001320000000133430000000d53454c4543542031005a0000000549\n" +
                ">430000000953535fac005300000004\n" +
                "<!!";
        assertHexScript(NetworkFacadeImpl.INSTANCE,
                NetworkFacadeImpl.INSTANCE,
                script,
                getHexPgWireConfig());
    }

    @Test
    public void testCloseMessageWithInvalidStatementNameHex() throws Exception {
        String script = ">0000006e00030000757365720078797a0064617461626173650071646200636c69656e745f656e636f64696e67005554463800446174655374796c650049534f0054696d655a6f6e65004575726f70652f4c6f6e646f6e0065787472615f666c6f61745f64696769747300320000\n" +
                "<520000000800000003\n" +
                ">70000000076f6800\n" +
                "<520000000800000000530000001154696d655a6f6e6500474d5400530000001d6170706c69636174696f6e5f6e616d6500517565737444420053000000187365727665725f76657273696f6e0031312e33005300000019696e74656765725f6461746574696d6573006f6e005300000019636c69656e745f656e636f64696e670055544638005a0000000549\n" +
                ">5000000022005345542065787472615f666c6f61745f646967697473203d2033000000420000000c0000000000000000450000000900000000015300000004\n" +
                "<310000000432000000044300000008534554005a0000000549\n" +
                ">500000003700534554206170706c69636174696f6e5f6e616d65203d2027506f737467726553514c204a4442432044726976657227000000420000000c0000000000000000450000000900000000015300000004\n" +
                "<310000000432000000044300000008534554005a0000000549\n" +
                ">500000002a0073656c65637420312c322c332066726f6d206c6f6e675f73657175656e6365283129000000420000000c000000000000000044000000065000450000000900000000005300000004\n" +
                "<31000000043200000004540000004200033100000040010001000000170004ffffffff00003200000040010002000000170004ffffffff00003300000040010003000000170004ffffffff000044000000150003000000013100000001320000000133430000000d53454c4543542031005a0000000549\n" +
                ">500000002d535f310073656c65637420312c322c332066726f6d206c6f6e675f73657175656e6365283129000000420000000f00535f310000000000000044000000065000450000000900000000005300000004\n" +
                "<31000000043200000004540000004200033100000040010001000000170004ffffffff00003200000040010002000000170004ffffffff00003300000040010003000000170004ffffffff000044000000150003000000013100000001320000000133430000000d53454c4543542031005a0000000549\n" +
                ">420000000f00535f3100000000000000450000000900000000005300000004\n" +
                "<320000000444000000150003000000013100000001320000000133430000000d53454c4543542031005a0000000549\n" +
                ">420000000f00535f3100000000000000450000000900000000005300000004\n" +
                "<320000000444000000150003000000013100000001320000000133430000000d53454c4543542031005a0000000549\n" +
                ">430000000953535f32005300000004\n" +
                "<!!";
        assertHexScript(NetworkFacadeImpl.INSTANCE,
                NetworkFacadeImpl.INSTANCE,
                script,
                getHexPgWireConfig());
    }

    @Test
    public void testCloseMessageWithInvalidTypeHex() throws Exception {
        String script = ">0000006e00030000757365720078797a0064617461626173650071646200636c69656e745f656e636f64696e67005554463800446174655374796c650049534f0054696d655a6f6e65004575726f70652f4c6f6e646f6e0065787472615f666c6f61745f64696769747300320000\n" +
                "<520000000800000003\n" +
                ">70000000076f6800\n" +
                "<520000000800000000530000001154696d655a6f6e6500474d5400530000001d6170706c69636174696f6e5f6e616d6500517565737444420053000000187365727665725f76657273696f6e0031312e33005300000019696e74656765725f6461746574696d6573006f6e005300000019636c69656e745f656e636f64696e670055544638005a0000000549\n" +
                ">5000000022005345542065787472615f666c6f61745f646967697473203d2033000000420000000c0000000000000000450000000900000000015300000004\n" +
                "<310000000432000000044300000008534554005a0000000549\n" +
                ">500000003700534554206170706c69636174696f6e5f6e616d65203d2027506f737467726553514c204a4442432044726976657227000000420000000c0000000000000000450000000900000000015300000004\n" +
                "<310000000432000000044300000008534554005a0000000549\n" +
                ">500000002a0073656c65637420312c322c332066726f6d206c6f6e675f73657175656e6365283129000000420000000c000000000000000044000000065000450000000900000000005300000004\n" +
                "<31000000043200000004540000004200033100000040010001000000170004ffffffff00003200000040010002000000170004ffffffff00003300000040010003000000170004ffffffff000044000000150003000000013100000001320000000133430000000d53454c4543542031005a0000000549\n" +
                ">500000002d535f310073656c65637420312c322c332066726f6d206c6f6e675f73657175656e6365283129000000420000000f00535f310000000000000044000000065000450000000900000000005300000004\n" +
                "<31000000043200000004540000004200033100000040010001000000170004ffffffff00003200000040010002000000170004ffffffff00003300000040010003000000170004ffffffff000044000000150003000000013100000001320000000133430000000d53454c4543542031005a0000000549\n" +
                ">420000000f00535f3100000000000000450000000900000000005300000004\n" +
                "<320000000444000000150003000000013100000001320000000133430000000d53454c4543542031005a0000000549\n" +
                ">420000000f00535f3100000000000000450000000900000000005300000004\n" +
                "<320000000444000000150003000000013100000001320000000133430000000d53454c4543542031005a0000000549\n" +
                ">430000000951535f31005300000004\n" +
                "<!!";
        assertHexScript(NetworkFacadeImpl.INSTANCE,
                NetworkFacadeImpl.INSTANCE,
                script,
                getHexPgWireConfig());
    }

    @Test
    @Ignore
    public void testCopyIn() throws SQLException, BrokenBarrierException, InterruptedException {
        final CountDownLatch haltLatch = new CountDownLatch(1);
        final AtomicBoolean running = new AtomicBoolean(true);
        try {
            startBasicServer(
                    NetworkFacadeImpl.INSTANCE,
                    new DefaultPGWireConfiguration(),
                    haltLatch,
                    running
            );

            Properties properties = new Properties();
            properties.setProperty("user", "admin");
            properties.setProperty("password", "quest");

            final Connection connection = DriverManager.getConnection("jdbc:postgresql://127.0.0.1:9120/qdb", properties);

            PreparedStatement stmt = connection.prepareStatement("create table tab (a int, b int)");
            stmt.execute();

            CopyManager copyManager = new CopyManager((BaseConnection) connection);

            CopyIn copyIn = copyManager.copyIn("copy tab from STDIN");

            String text = "a,b\r\n" +
                    "10,20";

            byte[] bytes = text.getBytes();
            copyIn.writeToCopy(bytes, 0, bytes.length);
            copyIn.endCopy();
        } finally {
            running.set(false);
            haltLatch.await();
        }
    }

    @Test
    public void testDDL() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            final CountDownLatch haltLatch = new CountDownLatch(1);
            final AtomicBoolean running = new AtomicBoolean(true);
            try {
                startBasicServer(
                        NetworkFacadeImpl.INSTANCE,
                        new DefaultPGWireConfiguration(),
                        haltLatch,
                        running
                );

                Properties properties = new Properties();
                properties.setProperty("user", "admin");
                properties.setProperty("password", "quest");
                properties.setProperty("sslmode", "disable");

                final Connection connection = DriverManager.getConnection("jdbc:postgresql://localhost:9120/qdb", properties);
                PreparedStatement statement = connection.prepareStatement("create table x (a int)");
                statement.execute();
                connection.close();
            } finally {
                running.set(false);
                haltLatch.await();
            }
        });
    }

    @Test
    public void testEmptySql() throws Exception {
        assertMemoryLeak(() -> {

            final CountDownLatch haltLatch = new CountDownLatch(1);
            final AtomicBoolean running = new AtomicBoolean(true);

            try {
                startBasicServer(NetworkFacadeImpl.INSTANCE,
                        new DefaultPGWireConfiguration(),
                        haltLatch,
                        running
                );

                sink.clear();

                Properties properties = new Properties();
                properties.setProperty("user", "admin");
                properties.setProperty("password", "quest");
                properties.setProperty("sslmode", "disable");
                final Connection connection = DriverManager.getConnection("jdbc:postgresql://127.0.0.1:9120/qdb", properties);
                try (Statement statement = connection.createStatement()) {
                    statement.execute("");
                }
                connection.close();
            } finally {
                running.set(false);
                haltLatch.await();
            }
        });
    }

    @Test
    public void testGORMConnect() throws Exception {
        // GORM is a Golang ORM tool
        assertHexScript(
                ">0000005e0003000064617461626173650071646200646174657374796c650049534f2c204d44590065787472615f666c6f61745f646967697473003200757365720061646d696e00636c69656e745f656e636f64696e6700555446380000\n" +
                        "<520000000800000003\n" +
                        ">700000000a717565737400\n" +
                        "<520000000800000000530000001154696d655a6f6e6500474d5400530000001d6170706c69636174696f6e5f6e616d6500517565737444420053000000187365727665725f76657273696f6e0031312e33005300000019696e74656765725f6461746574696d6573006f6e005300000019636c69656e745f656e636f64696e670055544638005a0000000549\n" +
                        ">51000000063b00\n"
        );
    }

    @Test
    public void testHappyPathForIntParameterWithoutExplicitParameterTypeHex() throws Exception {
        String script = ">0000006e00030000757365720078797a0064617461626173650071646200636c69656e745f656e636f64696e67005554463800446174655374796c650049534f0054696d655a6f6e65004575726f70652f4c6f6e646f6e0065787472615f666c6f61745f64696769747300320000\n" +
                "<520000000800000003\n" +
                ">70000000076f6800\n" +
                "<520000000800000000530000001154696d655a6f6e6500474d5400530000001d6170706c69636174696f6e5f6e616d6500517565737444420053000000187365727665725f76657273696f6e0031312e33005300000019696e74656765725f6461746574696d6573006f6e005300000019636c69656e745f656e636f64696e670055544638005a0000000549\n" +
                ">500000002c0073656c65637420782c202024312066726f6d206c6f6e675f73657175656e63652832293b000000\n" +
                ">420000001100000000000100000001330000\n" +
                ">44000000065000\n" +
                ">45000000090000000000\n" +
                ">4800000004\n" +
                "<31000000043200000004540000002f00027800000040010001000000140004ffffffff0000243100000040010002000000170004ffffffff000044000000100002000000013100000001334400000010000200000001320000000133430000000d53454c454354203200\n";

        assertHexScript(NetworkFacadeImpl.INSTANCE,
                NetworkFacadeImpl.INSTANCE,
                script,
                getHexPgWireConfig());
    }

    @Test
    public void testHexFragmentedSend() throws Exception {
        // this is a HEX encoded bytes of the same script as 'testSimple' sends using postgres jdbc driver
        String script = ">0000000804d2162f\n" +
                "<4e\n" +
                ">00" +
                "00" +
                "00" +
                "70" +
                "00" +
                "03" +
                "00" +
                "00" +
                "75" +
                "73" +
                "65" +
                "720061646d696e0064617461626173650071646200636c69656e745f656e636f64696e67005554463800446174655374796c650049534f0054696d655a6f6e65004575726f70652f4c6f6e646f6e0065787472615f666c6f61745f64696769747300320000\n" +
                "<520000000800000003\n" +
                ">70000000" +
                "0a717565737400\n" +
                "<520000000800000000530000001154696d655a6f6e6500474d5400530000001d6170706c69636174696f6e5f6e616d6500517565737444420053000000187365727665725f76657273696f6e0031312e33005300000019696e74656765725f6461746574696d6573006f6e005300000019636c69656e745f656e636f64696e670055544638005a0000000549\n" +
                ">5000000022005345542065787472615f666c6f61745f646967697473203d2033000000420000000c0000000000000000450000000900000000015300000004\n" +
                "<310000000432000000044300000008534554005a0000000549\n" +
                ">5000000037005345" +
                "54206170706c69636" +
                "174696f6e5f6e616d" +
                "65203d2027506f737" +
                "467726553514c204a" +
                "4442432044726976657227000000420000000c0000000000000000450000000900000000015300000004\n" +
                "<310000000432000000044300000008534554005a0000000549\n" +
                ">50000001940073656c65637420726e645f73747228342c342c342920732c20726e645f696e7428302c203235362c20342920692c20726e645f646f75626c6528342920642c2074696d657374616d705f73657175656e636528302c31303030302920742c20726e645f666c6f617428342920662c20726e645f73686f72742829205f73686f72742c20726e645f6c6f6e6728302c2031303030303030302c203529206c2c20726e645f74696d657374616d7028746f5f74696d657374616d70282732303135272c277979797927292c746f5f74696d657374616d70282732303136272c277979797927292c3229207473322c20726e645f6279746528302c313237292062622c20726e645f626f6f6c65616e282920622c20726e645f73796d626f6c28342c342c342c32292c20726e645f6461746528746f5f64617465282732303135272c20277979797927292c20746f5f64617465282732303136272c20277979797927292c2032292c726e645f62696e2831302c32302c32292066726f6d206c6f6e675f73657175656e636528353029000000420000000c000000000000000044000000065000450000000900000000005300000004\n" +
                "<310000000432000000045400000128000d730000004001000100000413ffff0000000000006900000040010002000000170004ffffffff00006400000040010003000002bd0004ffffffff000074000000400100040000045a0004ffffffff00006600000040010005000002bc0004ffffffff00005f73686f727400000040010006000000150004ffffffff00006c00000040010007000000140004ffffffff0000747332000000400100080000045a0004ffffffff0000626200000040010009000000150004ffffffff0000620000004001000a000000100004ffffffff0000726e645f73796d626f6c0000004001000b00000413ffff000000000000726e645f646174650000004001000c0000045a0004ffffffff0000726e645f62696e0000004001000d00000011ffff00000000000144000000a6000dffffffff00000002353700000012302e363235343032313534323431323031380000001a313937302d30312d30312030303a30303a30302e30303030303000000005302e343632000000052d313539330000000733343235323332ffffffff000000033132310000000166000000045045484e00000017323031352d30332d31372030343a32353a35322e3736350000000e19c49594365349b4597e3b08a11e44000000c8000d00000004585953420000000331343200000012302e353739333436363332363836323231310000001a313937302d30312d30312030303a30303a30302e30313030303000000005302e39363900000005323030383800000007313531373439300000001a323031352d30312d31372032303a34313a31392e343830363835000000033130300000000174000000045045484e00000017323031352d30362d32302030313a31303a35382e35393900000011795f8b812b934d1a8e78b5b91153d0fb6444000000c2000d000000044f5a5a560000000332313900000013302e31363338313337343737333734383531340000001a313937302d30312d30312030303a30303a30302e30323030303000000005302e363539000000062d313233303300000007393438393530380000001a323031352d30382d31332031373a31303a31392e37353235323100000001360000000166ffffffff00000017323031352d30352d32302030313a34383a33372e3431380000000f2b4d5ff64690c3b3598ee5612f640e44000000b1000d000000044f4c595800000002333000000012302e373133333931303237313535353834330000001a313937302d30312d30312030303a30303a30302e30333030303000000005302e363535000000043636313000000007363530343432380000001a323031352d30382d30382030303a34323a32342e353435363339000000033132330000000166ffffffff00000017323031352d30312d30332031333a35333a30332e313635ffffffff44000000ac000d000000045449514200000002343200000012302e363830363837333133343632363431380000001a313937302d30312d30312030303a30303a30302e30343030303000000005302e363236000000052d3136303500000007383831343038360000001a323031352d30372d32382031353a30383a35332e34363234393500000002323800000001740000000443505357ffffffff0000000e3ba6dc3b7d2be392fe6938e1779a44000000af000d000000044c544f560000000331333700000012302e373633323631353030343332343530330000001a313937302d30312d30312030303a30303a30302e30353030303000000005302e3838320000000439303534ffffffff0000001a323031352d30342d32302030353a30393a30332e353830353734000000033130360000000166000000045045484e00000017323031352d30312d30392030363a35373a31372e353132ffffffff44000000a0000d000000045a494d4e00000003313235ffffffff0000001a313937302d30312d30312030303a30303a30302e303630303030ffffffff00000005313135323400000007383333353236310000001a323031352d31302d32362030323a31303a35302e363838333934000000033131310000000174000000045045484e00000017323031352d30382d32312031353a34363a33322e363234ffffffff44000000a1000d000000044f504a4f0000000331363800000013302e31303435393335323331323333313138330000001a313937302d30312d30312030303a30303a30302e30373030303000000005302e353335000000052d3539323000000007373038303730340000001a323031352d30372d31312030393a31353a33382e3334323731370000000331303300000001660000000456544a57ffffffffffffffff44000000b6000d00000004474c554f0000000331343500000012302e353339313632363632313739343637330000001a313937302d30312d30312030303a30303a30302e30383030303000000005302e37363700000005313432343200000007323439393932320000001a323031352d31312d30322030393a30313a33312e3331323830340000000238340000000166000000045045484e00000017323031352d31312d31342031373a33373a33362e303433ffffffff44000000c3000d000000045a5651450000000331303300000012302e363732393430353539303737333633380000001a313937302d30312d30312030303a30303a30302e303930303030ffffffff00000005313337323700000007373837353834360000001a323031352d31322d31322031333a31363a32362e3133343536320000000232320000000174000000045045484e00000017323031352d30312d32302030343a35303a33342e30393800000012143380c9eba3677a1a79e435e43adc5c65ff44000000a7000d000000044c4947590000000331393900000012302e323833363334373133393438313436390000001a313937302d30312d30312030303a30303a30302e313030303030ffffffff00000005333034323600000007333231353536320000001a323031352d30382d32312031343a35353a30372e30353537323200000002313100000001660000000456544a57ffffffff0000000dff703ac78ab314cd470b0c391244000000a7000d000000044d514e5400000002343300000012302e353835393333323338383539393633380000001a313937302d30312d30312030303a30303a30302e31313030303000000005302e333335000000053237303139ffffffffffffffff0000000232370000000174000000045045484e00000017323031352d30372d31322031323a35393a34372e3636350000001326fb2e42faf56e8f80e354b807b13257ff9aef44000000c8000d00000004575743430000000332313300000012302e373636353032393931343337363935320000001a313937302d30312d30312030303a30303a30302e31323030303000000005302e35383000000005313336343000000007343132313932330000001a323031352d30382d30362030323a32373a33302e3436393736320000000237330000000166000000045045484e00000017323031352d30342d33302030383a31383a31302e3435330000001271a7d5af11963708dd98ef54882aa2ade7d444000000af000d00000004564647500000000331323000000012302e383430323936343730383132393534360000001a313937302d30312d30312030303a30303a30302e31333030303000000005302e373733000000043732323300000007373234313432330000001a323031352d31322d31382030373a33323a31382e34353630323500000002343300000001660000000456544a57ffffffff00000011244e44a80dfe27ec53135db215e7b8356744000000b7000d00000004524d44470000000331333400000013302e31313034373331353231343739333639360000001a313937302d30312d30312030303a30303a30302e31343030303000000005302e30343300000005323132323700000007373135353730380000001a323031352d30372d30332030343a31323a34352e3737343238310000000234320000000174000000044350535700000017323031352d30322d32342031323a31303a34332e313939ffffffff44000000a5000d0000000457464f5100000003323535ffffffff0000001a313937302d30312d30312030303a30303a30302e31353030303000000005302e31313600000005333135363900000007363638383237370000001a323031352d30352d31392030333a33303a34352e373739393939000000033132360000000174000000045045484e00000017323031352d31322d30392030393a35373a31372e303738ffffffff4400000098000d000000044d58444b00000002353600000012302e393939373739373233343033313638380000001a313937302d30312d30312030303a30303a30302e31363030303000000005302e353233000000062d33323337320000000736383834313332ffffffff0000000235380000000166ffffffff00000017323031352d30312d32302030363a31383a31382e353833ffffffff44000000bb000d00000004584d4b4a0000000331333900000012302e383430353831353439333536373431370000001a313937302d30312d30312030303a30303a30302e31373030303000000005302e333036000000053235383536ffffffff0000001a323031352d30352d31382030333a35303a32322e373331343337000000013200000001740000000456544a5700000017323031352d30362d32352031303a34353a30312e3031340000000d007cfb0119caf2bf845a6f383544000000af000d0000000456494844ffffffffffffffff0000001a313937302d30312d30312030303a30303a30302e31383030303000000005302e35353000000005323232383000000007393130393834320000001a323031352d30312d32352031333a35313a33382e3237303538330000000239340000000166000000044350535700000017323031352d31302d32372030323a35323a31392e3933350000000e2d16f389a38364ded6fdc45bc4e944000000bd000d0000000457504e58ffffffff00000012302e393436393730303831333932363930370000001a313937302d30312d30312030303a30303a30302e31393030303000000005302e343135000000062d3137393333000000063637343236310000001a323031352d30332d30342031353a34333a31352e3231333638360000000234330000000174000000044859525800000017323031352d31322d31382032313a32383a32352e3332350000000ab34c0e8ff10cc560b7d144000000bd000d0000000459504f5600000002333600000012302e363734313234383434383732383832340000001a313937302d30312d30312030303a30303a30302e32303030303000000005302e303331000000052d3538383800000007313337353432330000001a323031352d31322d31302032303a35303a33352e38363636313400000001330000000174ffffffff00000017323031352d30372d32332032303a31373a30342e3233360000000dd4abbe30fa8dac3d98a0ad9a5d44000000c6000d000000044e55484effffffff00000012302e363934303931373932353134383333320000001a313937302d30312d30312030303a30303a30302e32313030303000000005302e333339000000062d323532323600000007333532343734380000001a323031352d30352d30372030343a30373a31382e31353239363800000002333900000001740000000456544a5700000017323031352d30342d30342031353a32333a33342e31333000000012b8bef8a146872892a39be3cbc2648ab035d8440000009c000d00000004424f53450000000332343000000013302e30363030313832373732313535363031390000001a313937302d30312d30312030303a30303a30302e32323030303000000005302e33373900000005323339303400000007393036393333390000001a323031352d30332d32312030333a34323a34322e3634333138360000000238340000000174ffffffffffffffffffffffff44000000c5000d00000004494e4b470000000331323400000012302e383631353834313632373730323735330000001a313937302d30312d30312030303a30303a30302e32333030303000000005302e343034000000062d333033383300000007373233333534320000001a323031352d30372d32312031363a34323a34372e3031323134380000000239390000000166ffffffff00000017323031352d30382d32372031373a32353a33352e3330380000001287fc9283fc88f3322770c801b0dcc93a5b7e44000000b1000d000000044655584300000002353200000012302e373433303130313939343531313531370000001a313937302d30312d30312030303a30303a30302e323430303030ffffffff000000062d313437323900000007313034323036340000001a323031352d30382d32312030323a31303a35382e3934393637340000000232380000000174000000044350535700000017323031352d30382d32392032303a31353a35312e383335ffffffff44000000bd000d00000004554e595100000002373100000011302e3434323039353431303238313933380000001a313937302d30312d30312030303a30303a30302e32353030303000000005302e353339000000062d3232363131ffffffff0000001a323031352d31322d32332031383a34313a34322e3331393835390000000239380000000174000000045045484e00000017323031352d30312d32362030303a35353a35302e3230320000000f28ed9799d877333fb267da984747bf44000000b1000d000000044b424d51ffffffff00000013302e32383031393231383832353035313339350000001a313937302d30312d30312030303a30303a30302e323630303030ffffffff000000053132323430ffffffff0000001a323031352d30382d31362030313a30323a35352e3736363632320000000232310000000166ffffffff00000017323031352d30352d31392030303a34373a31382e3639380000000d6ade4604d381e7a21622353b1c4400000091000d000000044a534f4c00000003323433ffffffff0000001a313937302d30312d30312030303a30303a30302e32373030303000000005302e303638000000062d3137343638ffffffffffffffff0000000232300000000174ffffffff00000017323031352d30362d31392031303a33383a35342e343833000000113de02d0486e7ca29980769ca5bd6cf0969440000007f000d00000004484e535300000003313530ffffffff0000001a313937302d30312d30312030303a30303a30302e32383030303000000005302e3134380000000531343834310000000735393932343433ffffffff0000000232350000000166000000045045484effffffff0000000c14d6fcee032281b806c406af44000000c3000d00000004505a50420000000331303100000014302e3036313634363731373738363135383034350000001a313937302d30312d30312030303a30303a30302e323930303030ffffffff00000005313232333700000007393837383137390000001a323031352d30392d30332032323a31333a31382e38353234363500000002373900000001660000000456544a5700000017323031352d31322d31372031353a31323a35342e3935380000001012613a9aad982e7552ad62878845b99d44000000c3000d000000044f594e4e00000002323500000012302e333339333530393531343030303234370000001a313937302d30312d30312030303a30303a30302e33303030303000000005302e36323800000005323234313200000007343733363337380000001a323031352d31302d31302031323a31393a34322e353238323234000000033130360000000174000000044350535700000017323031352d30372d30312030303a32333a34392e3738390000000d54133fffb67ecd0427669489db4400000083000dffffffff0000000331313700000012302e353633383430343737353636333136310000001a313937302d30312d30312030303a30303a30302e333130303030ffffffff000000052d353630340000000736333533303138ffffffff0000000238340000000166ffffffffffffffff0000000b2bad2507db6244336e008e4400000099000d00000004485652490000000332333300000013302e32323430373636353739303730353737370000001a313937302d30312d30312030303a30303a30302e33323030303000000005302e3432350000000531303436390000000731373135323133ffffffff0000000238360000000166ffffffff00000017323031352d30322d30322030353a34383a31372e333733ffffffff44000000b6000d000000044f59544f00000002393600000012302e373430373538313631363931363336340000001a313937302d30312d30312030303a30303a30302e33333030303000000005302e353238000000062d313232333900000007333439393632300000001a323031352d30322d30372032323a33353a30332e3231323236380000000231370000000166000000045045484e00000017323031352d30332d32392031323a35353a31312e363832ffffffff44000000a5000d000000044c46435900000002363300000012302e373231373331353732393739303732320000001a313937302d30312d30312030303a30303a30302e333430303030ffffffff0000000532333334340000000739353233393832ffffffff000000033132330000000166000000044350535700000017323031352d30352d31382030343a33353a32372e3232380000000e05e5c04eccd6e37b34cd1535bba444000000c1000d0000000447484c580000000331343800000012302e333035373933373730343936343237320000001a313937302d30312d30312030303a30303a30302e33353030303000000005302e363336000000062d333134353700000007323332323333370000001a323031352d31302d32322031323a30363a30352e3534343730310000000239310000000174000000044859525800000017323031352d30352d32312030393a33333a31382e3135380000000a571d91723004b702cb0344000000a4000d000000045954535a00000003313233ffffffff0000001a313937302d30312d30312030303a30303a30302e33363030303000000005302e35313900000005323235333400000007343434363233360000001a323031352d30372d32372030373a32333a33372e3233333731310000000235330000000166000000044350535700000017323031352d30312d31332030343a33373a31302e303336ffffffff44000000a3000d0000000453574c5500000003323531ffffffff0000001a313937302d30312d30312030303a30303a30302e33373030303000000005302e313739000000043737333400000007343038323437350000001a323031352d31302d32312031383a32343a33342e3430303334350000000236390000000166000000045045484e00000017323031352d30342d30312031343a33333a34322e303035ffffffff44000000b1000d0000000454514a4c00000003323435ffffffff0000001a313937302d30312d30312030303a30303a30302e33383030303000000005302e3836350000000439353136000000063932393334300000001a323031352d30352d32382030343a31383a31382e36343035363700000002363900000001660000000456544a5700000017323031352d30362d31322032303a31323a32382e3838310000000f6c3e51d7ebb10771321faf404e8c47440000009e000d000000045245494a000000023934ffffffff0000001a313937302d30312d30312030303a30303a30302e33393030303000000005302e313330000000062d3239393234ffffffff0000001a323031352d30332d32302032323a31343a34362e323034373138000000033131330000000174000000044859525800000017323031352d31322d31392031333a35383a34312e383139ffffffff44000000c2000d000000044844485100000002393400000012302e373233343138313737333430373533360000001a313937302d30312d30312030303a30303a30302e34303030303000000005302e373330000000053139393730000000063635343133310000001a323031352d30312d31302032323a35363a30382e3438303435300000000238340000000174ffffffff00000017323031352d30332d30352031373a31343a34382e323735000000124f566b65a45338e9cdc1a7ee8675ada52d4944000000b8000d00000004554d455500000002343000000014302e3030383434343033333233303538303733390000001a313937302d30312d30312030303a30303a30302e34313030303000000005302e383035000000062d313136323300000007343539393836320000001a323031352d31312d32302030343a30323a34342e3333353934370000000237360000000166000000045045484e00000017323031352d30352d31372031373a33333a32302e393232ffffffff44000000ad000d00000004594a494800000003313834ffffffff0000001a313937302d30312d30312030303a30303a30302e34323030303000000005302e33383300000005313736313400000007333130313637310000001a323031352d30312d32382031323a30353a34362e363833303031000000033130350000000174ffffffff00000017323031352d31322d30372031393a32343a33362e3833380000000cec69cd73bb9bc595db6191ce44000000a3000d000000044359584700000002323700000012302e323931373739363035333034353734370000001a313937302d30312d30312030303a30303a30302e34333030303000000005302e393533000000043339343400000006323439313635ffffffff0000000236370000000174ffffffff00000017323031352d30332d30322030383a31393a34342e3536360000000e0148153e0c7f3f8fe4b5ab34212944000000b4000d000000044d5254470000000331343300000013302e30323633323533313336313439393131330000001a313937302d30312d30312030303a30303a30302e34343030303000000005302e393433000000062d323733323000000007313636373834320000001a323031352d30312d32342031393a35363a31352e3937333130390000000231310000000166ffffffff00000017323031352d30312d32342030373a31353a30322e373732ffffffff44000000c3000d00000004444f4e500000000332343600000011302e3635343232363234383734303434370000001a313937302d30312d30312030303a30303a30302e34353030303000000005302e35353600000005323734373700000007343136303031380000001a323031352d31322d31342030333a34303a30352e3931313833390000000232300000000174000000045045484e00000017323031352d31302d32392031343a33353a31302e3136370000000e079201f56aa131cdcbc2a2b48e9944000000c4000d00000004495158530000000332333200000013302e32333037353730303231383033383835330000001a313937302d30312d30312030303a30303a30302e34363030303000000005302e303439000000062d313831313300000007343030353232380000001a323031352d30362d31312031333a30303a30372e32343831383800000001380000000174000000044350535700000017323031352d30382d31362031313a30393a32342e3331310000000dfa1f9224b1b8676508b7f8410044000000b1000dffffffff00000003313738ffffffff0000001a313937302d30312d30312030303a30303a30302e34373030303000000005302e393033000000062d313436323600000007323933343537300000001a323031352d30342d30342030383a35313a35342e3036383135340000000238380000000174ffffffff00000017323031352d30372d30312030343a33323a32332e30383300000014843625632b6361431c477db646babb98ca08bea444000000b0000d000000044855575a00000002393400000011302e3131303430313337343937393631330000001a313937302d30312d30312030303a30303a30302e34383030303000000005302e343230000000052d3337333600000007353638373531340000001a323031352d30312d30322031373a31383a30352e3632373633330000000237340000000166ffffffff00000017323031352d30332d32392030363a33393a31312e363432ffffffff44000000ab000d000000045352454400000002363600000013302e31313237343636373134303931353932380000001a313937302d30312d30312030303a30303a30302e34393030303000000005302e303630000000062d313035343300000007333636393337370000001a323031352d31302d32322030323a35333a30322e3338313335310000000237370000000174000000045045484effffffff0000000b7c3fd6883a93ef24a5e2bc430000000e53454c454354203530005a0000000549";

        assertHexScript(
                getFragmentedSendFacade(),
                NetworkFacadeImpl.INSTANCE,
                script,
                new DefaultPGWireConfiguration()
        );
    }

    @Test
    public void testInsert() throws Exception {
        testInsert0(false);
    }

    /*
    nodejs code:
    ------------------
            const { Client } = require("pg")

            const start = async () => {
              try {
                const client = new Client({
                  database: "qdb",
                  host: "127.0.0.1",
                  password: "quest",
                  port: 8812,
                  user: "admin",
                })
                await client.connect()

                const res = await client.query("INSERT INTO test VALUES($1, $2);", [
                  "abc",
                  "123"
                ])

                console.log(res)

                await client.end()
              } catch (e) {
                console.log(e)
              }
            }

            start()
    ------------------
     */
    @Test
    public void testInsertFomNodeJsWith2Parameters_TableDoesNotExist() throws Exception {
        final String script = ">0000006900030000757365720078797a006461746162617365006e6162755f61707000636c69656e745f656e636f64696e67005554463800446174655374796c650049534f0054696d655a6f6e6500474d540065787472615f666c6f61745f64696769747300320000\n" +
                "<520000000800000003\n" +
                ">70000000076f6800\n" +
                "<520000000800000000530000001154696d655a6f6e6500474d5400530000001d6170706c69636174696f6e5f6e616d6500517565737444420053000000187365727665725f76657273696f6e0031312e33005300000019696e74656765725f6461746574696d6573006f6e005300000019636c69656e745f656e636f64696e670055544638005a0000000549\n" +
                ">500000002800494e5345525420494e544f20746573742056414c5545532824312c202432293b000000\n" +
                ">420000001a00000000000200000003616263000000033132330000\n" +
                ">44000000065000\n" +
                ">45000000090000000000\n" +
                ">4800000004\n" +
                "<310000000432000000046e00000004430000000b494e5345525400\n" +
                ">4800000004\n" +
                ">5300000004\n" +
                "<5a0000000549\n" +
                ">5800000004\n";
        assertHexScript(
                NetworkFacadeImpl.INSTANCE,
                NetworkFacadeImpl.INSTANCE,
                script,
                getHexPgWireConfig()
        );
    }

    /*
nodejs code:
------------------
        const { Client } = require("pg")

        const start = async () => {
          try {
            const client = new Client({
              database: "qdb",
              host: "127.0.0.1",
              password: "quest",
              port: 8812,
              user: "admin",
            })
            await client.connect()

            const res = await client.query("CREATE TABLE test (id string, number int);")

            const res = await client.query("INSERT INTO test VALUES($1, $2);", [
              "abc",
              "123"
            ])

            console.log(res)

            await client.end()
          } catch (e) {
            console.log(e)
          }
        }

        start()
------------------
 */
    @Test
    public void testInsertFomNodeJsWith2Parameters_WithTableCreation() throws Exception {
        final String script = ">0000006900030000757365720078797a006461746162617365006e6162755f61707000636c69656e745f656e636f64696e67005554463800446174655374796c650049534f0054696d655a6f6e6500474d540065787472615f666c6f61745f64696769747300320000\n" +
                "<520000000800000003\n" +
                ">70000000076f6800\n" +
                "<520000000800000000530000001154696d655a6f6e6500474d5400530000001d6170706c69636174696f6e5f6e616d6500517565737444420053000000187365727665725f76657273696f6e0031312e33005300000019696e74656765725f6461746574696d6573006f6e005300000019636c69656e745f656e636f64696e670055544638005a0000000549\n" +
                ">510000002f435245415445205441424c4520746573742028696420737472696e672c206e756d62657220696e74293b00\n" +
                "<43000000074f4b005a0000000549\n" +
                ">500000002800494e5345525420494e544f20746573742056414c5545532824312c202432293b000000\n" +
                ">420000001a00000000000200000003616263000000033132330000\n" +
                ">44000000065000\n" +
                ">45000000090000000000\n" +
                ">4800000004\n" +
                "<310000000432000000046e00000004430000000b494e5345525400\n" +
                ">4800000004\n" +
                ">5300000004\n" +
                "<5a0000000549\n" +
                ">5800000004\n";
        assertHexScript(
                NetworkFacadeImpl.INSTANCE,
                NetworkFacadeImpl.INSTANCE,
                script,
                getHexPgWireConfig()
        );
    }

    @Test
    public void testInsertSimpleQueryMode() throws Exception {
        testInsert0(true);
    }

    @Test
    public void testInsertTableDoesNotExistPrepared() throws Exception {
        testInsertTableDoesNotExist(false, "Cannot append. File does not exist");
    }

    @Test
    public void testInsertTableDoesNotExistSimple() throws Exception {
        testInsertTableDoesNotExist(true, "table 'x' does not exist");
    }

    @Test
    public void testIntAndLongParametersWithFormatCountGreaterThanValueCount() throws Exception {
        String script = ">0000006e00030000757365720078797a0064617461626173650071646200636c69656e745f656e636f64696e67005554463800446174655374796c650049534f0054696d655a6f6e65004575726f70652f4c6f6e646f6e0065787472615f666c6f61745f64696769747300320000\n" +
                "<520000000800000003\n" +
                ">70000000076f6800\n" +
                "<520000000800000000530000001154696d655a6f6e6500474d5400530000001d6170706c69636174696f6e5f6e616d6500517565737444420053000000187365727665725f76657273696f6e0031312e33005300000019696e74656765725f6461746574696d6573006f6e005300000019636c69656e745f656e636f64696e670055544638005a0000000549\n" +
                ">50000000300073656c65637420782c202024312c2024322066726f6d206c6f6e675f73657175656e63652832293b000000\n" +
                ">420000002500000003000000000000000200000001330000000a353030303030303030300000\n" +
                "<!!";

        assertHexScript(NetworkFacadeImpl.INSTANCE,
                NetworkFacadeImpl.INSTANCE,
                script,
                getHexPgWireConfig());
    }

    @Test
    public void testIntAndLongParametersWithFormatCountSmallerThanValueCount() throws Exception {
        String script = ">0000006e00030000757365720078797a0064617461626173650071646200636c69656e745f656e636f64696e67005554463800446174655374796c650049534f0054696d655a6f6e65004575726f70652f4c6f6e646f6e0065787472615f666c6f61745f64696769747300320000\n" +
                "<520000000800000003\n" +
                ">70000000076f6800\n" +
                "<520000000800000000530000001154696d655a6f6e6500474d5400530000001d6170706c69636174696f6e5f6e616d6500517565737444420053000000187365727665725f76657273696f6e0031312e33005300000019696e74656765725f6461746574696d6573006f6e005300000019636c69656e745f656e636f64696e670055544638005a0000000549\n" +
                ">50000000300073656c65637420782c202024312c2024322066726f6d206c6f6e675f73657175656e63652832293b000000\n" +
                ">42000000230000000200000000000300000001330000000a353030303030303030300000\n" +
                "<!!";

        assertHexScript(NetworkFacadeImpl.INSTANCE,
                NetworkFacadeImpl.INSTANCE,
                script,
                getHexPgWireConfig());
    }

    @Test
    public void testIntAndLongParametersWithoutExplicitParameterTypeButOneExplicitTextFormatHex() throws Exception {
        String script = ">0000006e00030000757365720078797a0064617461626173650071646200636c69656e745f656e636f64696e67005554463800446174655374796c650049534f0054696d655a6f6e65004575726f70652f4c6f6e646f6e0065787472615f666c6f61745f64696769747300320000\n" +
                "<520000000800000003\n" +
                ">70000000076f6800\n" +
                "<520000000800000000530000001154696d655a6f6e6500474d5400530000001d6170706c69636174696f6e5f6e616d6500517565737444420053000000187365727665725f76657273696f6e0031312e33005300000019696e74656765725f6461746574696d6573006f6e005300000019636c69656e745f656e636f64696e670055544638005a0000000549\n" +
                ">50000000300073656c65637420782c202024312c2024322066726f6d206c6f6e675f73657175656e63652832293b000000\n" +
                ">4200000021000000010000000200000001330000000a353030303030303030300000\n" +
                ">44000000065000\n" +
                ">45000000090000000000\n" +
                ">4800000004\n" +
                "<31000000043200000004540000004400037800000040010001000000140004ffffffff0000243100000040010002000000170004ffffffff0000243200000040010003000000140004ffffffff0000440000001e0003000000013100000001330000000a35303030303030303030440000001e0003000000013200000001330000000a35303030303030303030430000000d53454c454354203200\n";

        assertHexScript(NetworkFacadeImpl.INSTANCE,
                NetworkFacadeImpl.INSTANCE,
                script,
                getHexPgWireConfig());
    }

    @Test
    public void testIntParameterWithoutExplicitParameterTypeButExplicitTextFormatHex() throws Exception {
        String script = ">0000006e00030000757365720078797a0064617461626173650071646200636c69656e745f656e636f64696e67005554463800446174655374796c650049534f0054696d655a6f6e65004575726f70652f4c6f6e646f6e0065787472615f666c6f61745f64696769747300320000\n" +
                "<520000000800000003\n" +
                ">70000000076f6800\n" +
                "<520000000800000000530000001154696d655a6f6e6500474d5400530000001d6170706c69636174696f6e5f6e616d6500517565737444420053000000187365727665725f76657273696f6e0031312e33005300000019696e74656765725f6461746574696d6573006f6e005300000019636c69656e745f656e636f64696e670055544638005a0000000549\n" +
                ">500000002c0073656c65637420782c202024312066726f6d206c6f6e675f73657175656e63652832293b000000\n" +
                ">4200000013000000010000000100000001330000\n" +
                ">44000000065000\n" +
                ">45000000090000000000\n" +
                ">4800000004\n" +
                "<31000000043200000004540000002f00027800000040010001000000140004ffffffff0000243100000040010002000000170004ffffffff000044000000100002000000013100000001334400000010000200000001320000000133430000000d53454c454354203200\n";

        assertHexScript(NetworkFacadeImpl.INSTANCE,
                NetworkFacadeImpl.INSTANCE,
                script,
                getHexPgWireConfig());
    }

    @Test
    public void testLargeOutput() throws Exception {
        TestUtils.assertMemoryLeak(() -> {

            final String expected = "1[INTEGER],2[INTEGER],3[INTEGER]\n" +
                    "1,2,3\n" +
                    "1,2,3\n" +
                    "1,2,3\n" +
                    "1,2,3\n" +
                    "1,2,3\n" +
                    "1,2,3\n" +
                    "1,2,3\n" +
                    "1,2,3\n" +
                    "1,2,3\n" +
                    "1,2,3\n" +
                    "1,2,3\n" +
                    "1,2,3\n" +
                    "1,2,3\n" +
                    "1,2,3\n" +
                    "1,2,3\n" +
                    "1,2,3\n" +
                    "1,2,3\n" +
                    "1,2,3\n" +
                    "1,2,3\n" +
                    "1,2,3\n" +
                    "1,2,3\n" +
                    "1,2,3\n" +
                    "1,2,3\n" +
                    "1,2,3\n" +
                    "1,2,3\n" +
                    "1,2,3\n" +
                    "1,2,3\n" +
                    "1,2,3\n" +
                    "1,2,3\n" +
                    "1,2,3\n" +
                    "1,2,3\n" +
                    "1,2,3\n" +
                    "1,2,3\n" +
                    "1,2,3\n" +
                    "1,2,3\n" +
                    "1,2,3\n" +
                    "1,2,3\n" +
                    "1,2,3\n" +
                    "1,2,3\n" +
                    "1,2,3\n" +
                    "1,2,3\n" +
                    "1,2,3\n" +
                    "1,2,3\n" +
                    "1,2,3\n" +
                    "1,2,3\n" +
                    "1,2,3\n" +
                    "1,2,3\n" +
                    "1,2,3\n" +
                    "1,2,3\n" +
                    "1,2,3\n";

            final CountDownLatch haltLatch = new CountDownLatch(1);
            final AtomicBoolean running = new AtomicBoolean(true);
            try {
                startBasicServer(
                        NetworkFacadeImpl.INSTANCE,
                        new DefaultPGWireConfiguration() {
                            @Override
                            public int getSendBufferSize() {
                                return 512;
                            }
                        },
                        haltLatch,
                        running
                );

                Properties properties = new Properties();
                properties.setProperty("user", "admin");
                properties.setProperty("password", "quest");
                properties.setProperty("sslmode", "disable");
                properties.setProperty("binaryTransfer", "false");

                final Connection connection = DriverManager.getConnection("jdbc:postgresql://127.0.0.1:9120/qdb", properties);
                PreparedStatement statement = connection.prepareStatement("select 1,2,3 from long_sequence(50)");
                Statement statement1 = connection.createStatement();

                StringSink sink = new StringSink();
                for (int i = 0; i < 10; i++) {
                    sink.clear();
                    ResultSet rs = statement.executeQuery();

                    statement1.executeQuery("select 1 from long_sequence(2)");
                    assertResultSet(expected, sink, rs);
                    rs.close();
                }
                connection.close();
            } finally {
                running.set(false);
                haltLatch.await();
            }
        });
    }

    @Test
    public void testLargeOutputHex() throws Exception {
        String script = ">0000007300030000757365720078797a006461746162617365006e6162755f61707000636c69656e745f656e636f64696e67005554463800446174655374796c650049534f0054696d655a6f6e65004575726f70652f4c6f6e646f6e0065787472615f666c6f61745f64696769747300320000\n" +
                "<520000000800000003\n" +
                ">70000000076f6800\n" +
                "<520000000800000000530000001154696d655a6f6e6500474d5400530000001d6170706c69636174696f6e5f6e616d6500517565737444420053000000187365727665725f76657273696f6e0031312e33005300000019696e74656765725f6461746574696d6573006f6e005300000019636c69656e745f656e636f64696e670055544638005a0000000549\n" +
                ">5000000022005345542065787472615f666c6f61745f646967697473203d2033000000420000000c0000000000000000450000000900000000015300000004\n" +
                "<310000000432000000044300000008534554005a0000000549\n" +
                ">500000003700534554206170706c69636174696f6e5f6e616d65203d2027506f737467726553514c204a4442432044726976657227000000420000000c0000000000000000450000000900000000015300000004\n" +
                "<310000000432000000044300000008534554005a0000000549\n" +
                ">500000002b0073656c65637420312c322c332066726f6d206c6f6e675f73657175656e636528353029000000420000000c000000000000000044000000065000450000000900000000005300000004\n" +
                "<31000000043200000004540000004200033100000040010001000000170004ffffffff00003200000040010002000000170004ffffffff00003300000040010003000000170004ffffffff000044000000150003000000013100000001320000000133440000001500030000000131000000013200000001334400000015000300000001310000000132000000013344000000150003000000013100000001320000000133440000001500030000000131000000013200000001334400000015000300000001310000000132000000013344000000150003000000013100000001320000000133440000001500030000000131000000013200000001334400000015000300000001310000000132000000013344000000150003000000013100000001320000000133440000001500030000000131000000013200000001334400000015000300000001310000000132000000013344000000150003000000013100000001320000000133440000001500030000000131000000013200000001334400000015000300000001310000000132000000013344000000150003000000013100000001320000000133440000001500030000000131000000013200000001334400000015000300000001310000000132000000013344000000150003000000013100000001320000000133\n" +
                "<4400000015000300000001310000000132000000013344000000150003000000013100000001320000000133440000001500030000000131000000013200000001334400000015000300000001310000000132000000013344000000150003000000013100000001320000000133440000001500030000000131000000013200000001334400000015000300000001310000000132000000013344000000150003000000013100000001320000000133440000001500030000000131000000013200000001334400000015000300000001310000000132000000013344000000150003000000013100000001320000000133440000001500030000000131000000013200000001334400000015000300000001310000000132000000013344000000150003000000013100000001320000000133440000001500030000000131000000013200000001334400000015000300000001310000000132000000013344000000150003000000013100000001320000000133440000001500030000000131000000013200000001334400000015000300000001310000000132000000013344000000150003000000013100000001320000000133440000001500030000000131000000013200000001334400000015000300000001310000000132000000013344000000150003000000013100000001320000000133\n" +
                "<4400000015000300000001310000000132000000013344000000150003000000013100000001320000000133440000001500030000000131000000013200000001334400000015000300000001310000000132000000013344000000150003000000013100000001320000000133440000001500030000000131000000013200000001334400000015000300000001310000000132000000013344000000150003000000013100000001320000000133430000000e53454c454354203438005a0000000549\n" +
                ">50000000260073656c65637420312066726f6d206c6f6e675f73657175656e6365283229000000420000000c000000000000000044000000065000450000000900000000005300000004\n" +
                "<31000000043200000004540000001a00013100000040010001000000170004ffffffff0000440000000b00010000000131440000000b00010000000131430000000d53454c4543542032005a0000000549\n" +
                ">500000002b0073656c65637420312c322c332066726f6d206c6f6e675f73657175656e636528353029000000420000000c000000000000000044000000065000450000000900000000005300000004\n" +
                "<31000000043200000004540000004200033100000040010001000000170004ffffffff00003200000040010002000000170004ffffffff00003300000040010003000000170004ffffffff000044000000150003000000013100000001320000000133440000001500030000000131000000013200000001334400000015000300000001310000000132000000013344000000150003000000013100000001320000000133440000001500030000000131000000013200000001334400000015000300000001310000000132000000013344000000150003000000013100000001320000000133440000001500030000000131000000013200000001334400000015000300000001310000000132000000013344000000150003000000013100000001320000000133440000001500030000000131000000013200000001334400000015000300000001310000000132000000013344000000150003000000013100000001320000000133440000001500030000000131000000013200000001334400000015000300000001310000000132000000013344000000150003000000013100000001320000000133440000001500030000000131000000013200000001334400000015000300000001310000000132000000013344000000150003000000013100000001320000000133\n" +
                "<4400000015000300000001310000000132000000013344000000150003000000013100000001320000000133440000001500030000000131000000013200000001334400000015000300000001310000000132000000013344000000150003000000013100000001320000000133440000001500030000000131000000013200000001334400000015000300000001310000000132000000013344000000150003000000013100000001320000000133440000001500030000000131000000013200000001334400000015000300000001310000000132000000013344000000150003000000013100000001320000000133440000001500030000000131000000013200000001334400000015000300000001310000000132000000013344000000150003000000013100000001320000000133440000001500030000000131000000013200000001334400000015000300000001310000000132000000013344000000150003000000013100000001320000000133440000001500030000000131000000013200000001334400000015000300000001310000000132000000013344000000150003000000013100000001320000000133440000001500030000000131000000013200000001334400000015000300000001310000000132000000013344000000150003000000013100000001320000000133\n" +
                "<4400000015000300000001310000000132000000013344000000150003000000013100000001320000000133440000001500030000000131000000013200000001334400000015000300000001310000000132000000013344000000150003000000013100000001320000000133440000001500030000000131000000013200000001334400000015000300000001310000000132000000013344000000150003000000013100000001320000000133430000000e53454c454354203438005a0000000549\n" +
                ">50000000260073656c65637420312066726f6d206c6f6e675f73657175656e6365283229000000420000000c000000000000000044000000065000450000000900000000005300000004\n" +
                "<31000000043200000004540000001a00013100000040010001000000170004ffffffff0000440000000b00010000000131440000000b00010000000131430000000d53454c4543542032005a0000000549\n" +
                ">500000002b0073656c65637420312c322c332066726f6d206c6f6e675f73657175656e636528353029000000420000000c000000000000000044000000065000450000000900000000005300000004\n" +
                "<31000000043200000004540000004200033100000040010001000000170004ffffffff00003200000040010002000000170004ffffffff00003300000040010003000000170004ffffffff000044000000150003000000013100000001320000000133440000001500030000000131000000013200000001334400000015000300000001310000000132000000013344000000150003000000013100000001320000000133440000001500030000000131000000013200000001334400000015000300000001310000000132000000013344000000150003000000013100000001320000000133440000001500030000000131000000013200000001334400000015000300000001310000000132000000013344000000150003000000013100000001320000000133440000001500030000000131000000013200000001334400000015000300000001310000000132000000013344000000150003000000013100000001320000000133440000001500030000000131000000013200000001334400000015000300000001310000000132000000013344000000150003000000013100000001320000000133440000001500030000000131000000013200000001334400000015000300000001310000000132000000013344000000150003000000013100000001320000000133\n" +
                "<4400000015000300000001310000000132000000013344000000150003000000013100000001320000000133440000001500030000000131000000013200000001334400000015000300000001310000000132000000013344000000150003000000013100000001320000000133440000001500030000000131000000013200000001334400000015000300000001310000000132000000013344000000150003000000013100000001320000000133440000001500030000000131000000013200000001334400000015000300000001310000000132000000013344000000150003000000013100000001320000000133440000001500030000000131000000013200000001334400000015000300000001310000000132000000013344000000150003000000013100000001320000000133440000001500030000000131000000013200000001334400000015000300000001310000000132000000013344000000150003000000013100000001320000000133440000001500030000000131000000013200000001334400000015000300000001310000000132000000013344000000150003000000013100000001320000000133440000001500030000000131000000013200000001334400000015000300000001310000000132000000013344000000150003000000013100000001320000000133\n" +
                "<4400000015000300000001310000000132000000013344000000150003000000013100000001320000000133440000001500030000000131000000013200000001334400000015000300000001310000000132000000013344000000150003000000013100000001320000000133440000001500030000000131000000013200000001334400000015000300000001310000000132000000013344000000150003000000013100000001320000000133430000000e53454c454354203438005a0000000549\n" +
                ">50000000260073656c65637420312066726f6d206c6f6e675f73657175656e6365283229000000420000000c000000000000000044000000065000450000000900000000005300000004\n" +
                "<31000000043200000004540000001a00013100000040010001000000170004ffffffff0000440000000b00010000000131440000000b00010000000131430000000d53454c4543542032005a0000000549\n" +
                ">500000002b0073656c65637420312c322c332066726f6d206c6f6e675f73657175656e636528353029000000420000000c000000000000000044000000065000450000000900000000005300000004\n" +
                "<31000000043200000004540000004200033100000040010001000000170004ffffffff00003200000040010002000000170004ffffffff00003300000040010003000000170004ffffffff000044000000150003000000013100000001320000000133440000001500030000000131000000013200000001334400000015000300000001310000000132000000013344000000150003000000013100000001320000000133440000001500030000000131000000013200000001334400000015000300000001310000000132000000013344000000150003000000013100000001320000000133440000001500030000000131000000013200000001334400000015000300000001310000000132000000013344000000150003000000013100000001320000000133440000001500030000000131000000013200000001334400000015000300000001310000000132000000013344000000150003000000013100000001320000000133440000001500030000000131000000013200000001334400000015000300000001310000000132000000013344000000150003000000013100000001320000000133440000001500030000000131000000013200000001334400000015000300000001310000000132000000013344000000150003000000013100000001320000000133\n" +
                "<4400000015000300000001310000000132000000013344000000150003000000013100000001320000000133440000001500030000000131000000013200000001334400000015000300000001310000000132000000013344000000150003000000013100000001320000000133440000001500030000000131000000013200000001334400000015000300000001310000000132000000013344000000150003000000013100000001320000000133440000001500030000000131000000013200000001334400000015000300000001310000000132000000013344000000150003000000013100000001320000000133440000001500030000000131000000013200000001334400000015000300000001310000000132000000013344000000150003000000013100000001320000000133440000001500030000000131000000013200000001334400000015000300000001310000000132000000013344000000150003000000013100000001320000000133440000001500030000000131000000013200000001334400000015000300000001310000000132000000013344000000150003000000013100000001320000000133440000001500030000000131000000013200000001334400000015000300000001310000000132000000013344000000150003000000013100000001320000000133\n" +
                "<4400000015000300000001310000000132000000013344000000150003000000013100000001320000000133440000001500030000000131000000013200000001334400000015000300000001310000000132000000013344000000150003000000013100000001320000000133440000001500030000000131000000013200000001334400000015000300000001310000000132000000013344000000150003000000013100000001320000000133430000000e53454c454354203438005a0000000549\n" +
                ">50000000260073656c65637420312066726f6d206c6f6e675f73657175656e6365283229000000420000000c000000000000000044000000065000450000000900000000005300000004\n" +
                "<31000000043200000004540000001a00013100000040010001000000170004ffffffff0000440000000b00010000000131440000000b00010000000131430000000d53454c4543542032005a0000000549\n" +
                ">500000002e535f310073656c65637420312c322c332066726f6d206c6f6e675f73657175656e636528353029000000420000000f00535f310000000000000044000000065000450000000900000000005300000004\n" +
                "<31000000043200000004540000004200033100000040010001000000170004ffffffff00003200000040010002000000170004ffffffff00003300000040010003000000170004ffffffff000044000000150003000000013100000001320000000133440000001500030000000131000000013200000001334400000015000300000001310000000132000000013344000000150003000000013100000001320000000133440000001500030000000131000000013200000001334400000015000300000001310000000132000000013344000000150003000000013100000001320000000133440000001500030000000131000000013200000001334400000015000300000001310000000132000000013344000000150003000000013100000001320000000133440000001500030000000131000000013200000001334400000015000300000001310000000132000000013344000000150003000000013100000001320000000133440000001500030000000131000000013200000001334400000015000300000001310000000132000000013344000000150003000000013100000001320000000133440000001500030000000131000000013200000001334400000015000300000001310000000132000000013344000000150003000000013100000001320000000133\n" +
                "<4400000015000300000001310000000132000000013344000000150003000000013100000001320000000133440000001500030000000131000000013200000001334400000015000300000001310000000132000000013344000000150003000000013100000001320000000133440000001500030000000131000000013200000001334400000015000300000001310000000132000000013344000000150003000000013100000001320000000133440000001500030000000131000000013200000001334400000015000300000001310000000132000000013344000000150003000000013100000001320000000133440000001500030000000131000000013200000001334400000015000300000001310000000132000000013344000000150003000000013100000001320000000133440000001500030000000131000000013200000001334400000015000300000001310000000132000000013344000000150003000000013100000001320000000133440000001500030000000131000000013200000001334400000015000300000001310000000132000000013344000000150003000000013100000001320000000133440000001500030000000131000000013200000001334400000015000300000001310000000132000000013344000000150003000000013100000001320000000133\n" +
                "<4400000015000300000001310000000132000000013344000000150003000000013100000001320000000133440000001500030000000131000000013200000001334400000015000300000001310000000132000000013344000000150003000000013100000001320000000133440000001500030000000131000000013200000001334400000015000300000001310000000132000000013344000000150003000000013100000001320000000133430000000e53454c454354203438005a0000000549\n" +
                ">50000000260073656c65637420312066726f6d206c6f6e675f73657175656e6365283229000000420000000c000000000000000044000000065000450000000900000000005300000004\n" +
                "<31000000043200000004540000001a00013100000040010001000000170004ffffffff0000440000000b00010000000131440000000b00010000000131430000000d53454c4543542032005a0000000549\n" +
                ">420000000f00535f3100000000000000450000000900000000005300000004\n" +
                "<32000000044400000015000300000001310000000132000000013344000000150003000000013100000001320000000133440000001500030000000131000000013200000001334400000015000300000001310000000132000000013344000000150003000000013100000001320000000133440000001500030000000131000000013200000001334400000015000300000001310000000132000000013344000000150003000000013100000001320000000133440000001500030000000131000000013200000001334400000015000300000001310000000132000000013344000000150003000000013100000001320000000133440000001500030000000131000000013200000001334400000015000300000001310000000132000000013344000000150003000000013100000001320000000133440000001500030000000131000000013200000001334400000015000300000001310000000132000000013344000000150003000000013100000001320000000133440000001500030000000131000000013200000001334400000015000300000001310000000132000000013344000000150003000000013100000001320000000133440000001500030000000131000000013200000001334400000015000300000001310000000132000000013344000000150003000000013100000001320000000133\n" +
                "<4400000015000300000001310000000132000000013344000000150003000000013100000001320000000133440000001500030000000131000000013200000001334400000015000300000001310000000132000000013344000000150003000000013100000001320000000133440000001500030000000131000000013200000001334400000015000300000001310000000132000000013344000000150003000000013100000001320000000133440000001500030000000131000000013200000001334400000015000300000001310000000132000000013344000000150003000000013100000001320000000133440000001500030000000131000000013200000001334400000015000300000001310000000132000000013344000000150003000000013100000001320000000133440000001500030000000131000000013200000001334400000015000300000001310000000132000000013344000000150003000000013100000001320000000133440000001500030000000131000000013200000001334400000015000300000001310000000132000000013344000000150003000000013100000001320000000133440000001500030000000131000000013200000001334400000015000300000001310000000132000000013344000000150003000000013100000001320000000133\n" +
                "<44000000150003000000013100000001320000000133440000001500030000000131000000013200000001334400000015000300000001310000000132000000013344000000150003000000013100000001320000000133430000000e53454c454354203438005a0000000549\n" +
                ">50000000260073656c65637420312066726f6d206c6f6e675f73657175656e6365283229000000420000000c000000000000000044000000065000450000000900000000005300000004\n" +
                "<31000000043200000004540000001a00013100000040010001000000170004ffffffff0000440000000b00010000000131440000000b00010000000131430000000d53454c4543542032005a0000000549\n" +
                ">420000000f00535f3100000000000000450000000900000000005300000004\n" +
                "<32000000044400000015000300000001310000000132000000013344000000150003000000013100000001320000000133440000001500030000000131000000013200000001334400000015000300000001310000000132000000013344000000150003000000013100000001320000000133440000001500030000000131000000013200000001334400000015000300000001310000000132000000013344000000150003000000013100000001320000000133440000001500030000000131000000013200000001334400000015000300000001310000000132000000013344000000150003000000013100000001320000000133440000001500030000000131000000013200000001334400000015000300000001310000000132000000013344000000150003000000013100000001320000000133440000001500030000000131000000013200000001334400000015000300000001310000000132000000013344000000150003000000013100000001320000000133440000001500030000000131000000013200000001334400000015000300000001310000000132000000013344000000150003000000013100000001320000000133440000001500030000000131000000013200000001334400000015000300000001310000000132000000013344000000150003000000013100000001320000000133\n" +
                "<4400000015000300000001310000000132000000013344000000150003000000013100000001320000000133440000001500030000000131000000013200000001334400000015000300000001310000000132000000013344000000150003000000013100000001320000000133440000001500030000000131000000013200000001334400000015000300000001310000000132000000013344000000150003000000013100000001320000000133440000001500030000000131000000013200000001334400000015000300000001310000000132000000013344000000150003000000013100000001320000000133440000001500030000000131000000013200000001334400000015000300000001310000000132000000013344000000150003000000013100000001320000000133440000001500030000000131000000013200000001334400000015000300000001310000000132000000013344000000150003000000013100000001320000000133440000001500030000000131000000013200000001334400000015000300000001310000000132000000013344000000150003000000013100000001320000000133440000001500030000000131000000013200000001334400000015000300000001310000000132000000013344000000150003000000013100000001320000000133\n" +
                "<44000000150003000000013100000001320000000133440000001500030000000131000000013200000001334400000015000300000001310000000132000000013344000000150003000000013100000001320000000133430000000e53454c454354203438005a0000000549\n" +
                ">50000000260073656c65637420312066726f6d206c6f6e675f73657175656e6365283229000000420000000c000000000000000044000000065000450000000900000000005300000004\n" +
                "<31000000043200000004540000001a00013100000040010001000000170004ffffffff0000440000000b00010000000131440000000b00010000000131430000000d53454c4543542032005a0000000549\n" +
                ">420000000f00535f3100000000000000450000000900000000005300000004\n" +
                "<32000000044400000015000300000001310000000132000000013344000000150003000000013100000001320000000133440000001500030000000131000000013200000001334400000015000300000001310000000132000000013344000000150003000000013100000001320000000133440000001500030000000131000000013200000001334400000015000300000001310000000132000000013344000000150003000000013100000001320000000133440000001500030000000131000000013200000001334400000015000300000001310000000132000000013344000000150003000000013100000001320000000133440000001500030000000131000000013200000001334400000015000300000001310000000132000000013344000000150003000000013100000001320000000133440000001500030000000131000000013200000001334400000015000300000001310000000132000000013344000000150003000000013100000001320000000133440000001500030000000131000000013200000001334400000015000300000001310000000132000000013344000000150003000000013100000001320000000133440000001500030000000131000000013200000001334400000015000300000001310000000132000000013344000000150003000000013100000001320000000133\n" +
                "<4400000015000300000001310000000132000000013344000000150003000000013100000001320000000133440000001500030000000131000000013200000001334400000015000300000001310000000132000000013344000000150003000000013100000001320000000133440000001500030000000131000000013200000001334400000015000300000001310000000132000000013344000000150003000000013100000001320000000133440000001500030000000131000000013200000001334400000015000300000001310000000132000000013344000000150003000000013100000001320000000133440000001500030000000131000000013200000001334400000015000300000001310000000132000000013344000000150003000000013100000001320000000133440000001500030000000131000000013200000001334400000015000300000001310000000132000000013344000000150003000000013100000001320000000133440000001500030000000131000000013200000001334400000015000300000001310000000132000000013344000000150003000000013100000001320000000133440000001500030000000131000000013200000001334400000015000300000001310000000132000000013344000000150003000000013100000001320000000133\n" +
                "<44000000150003000000013100000001320000000133440000001500030000000131000000013200000001334400000015000300000001310000000132000000013344000000150003000000013100000001320000000133430000000e53454c454354203438005a0000000549\n" +
                ">50000000260073656c65637420312066726f6d206c6f6e675f73657175656e6365283229000000420000000c000000000000000044000000065000450000000900000000005300000004\n" +
                "<31000000043200000004540000001a00013100000040010001000000170004ffffffff0000440000000b00010000000131440000000b00010000000131430000000d53454c4543542032005a0000000549\n" +
                ">420000000f00535f3100000000000000450000000900000000005300000004\n" +
                "<32000000044400000015000300000001310000000132000000013344000000150003000000013100000001320000000133440000001500030000000131000000013200000001334400000015000300000001310000000132000000013344000000150003000000013100000001320000000133440000001500030000000131000000013200000001334400000015000300000001310000000132000000013344000000150003000000013100000001320000000133440000001500030000000131000000013200000001334400000015000300000001310000000132000000013344000000150003000000013100000001320000000133440000001500030000000131000000013200000001334400000015000300000001310000000132000000013344000000150003000000013100000001320000000133440000001500030000000131000000013200000001334400000015000300000001310000000132000000013344000000150003000000013100000001320000000133440000001500030000000131000000013200000001334400000015000300000001310000000132000000013344000000150003000000013100000001320000000133440000001500030000000131000000013200000001334400000015000300000001310000000132000000013344000000150003000000013100000001320000000133\n" +
                "<4400000015000300000001310000000132000000013344000000150003000000013100000001320000000133440000001500030000000131000000013200000001334400000015000300000001310000000132000000013344000000150003000000013100000001320000000133440000001500030000000131000000013200000001334400000015000300000001310000000132000000013344000000150003000000013100000001320000000133440000001500030000000131000000013200000001334400000015000300000001310000000132000000013344000000150003000000013100000001320000000133440000001500030000000131000000013200000001334400000015000300000001310000000132000000013344000000150003000000013100000001320000000133440000001500030000000131000000013200000001334400000015000300000001310000000132000000013344000000150003000000013100000001320000000133440000001500030000000131000000013200000001334400000015000300000001310000000132000000013344000000150003000000013100000001320000000133440000001500030000000131000000013200000001334400000015000300000001310000000132000000013344000000150003000000013100000001320000000133\n" +
                "<44000000150003000000013100000001320000000133440000001500030000000131000000013200000001334400000015000300000001310000000132000000013344000000150003000000013100000001320000000133430000000e53454c454354203438005a0000000549\n" +
                ">50000000260073656c65637420312066726f6d206c6f6e675f73657175656e6365283229000000420000000c000000000000000044000000065000450000000900000000005300000004\n" +
                "<31000000043200000004540000001a00013100000040010001000000170004ffffffff0000440000000b00010000000131440000000b00010000000131430000000d53454c4543542032005a0000000549\n" +
                ">420000000f00535f3100000000000000450000000900000000005300000004\n" +
                "<32000000044400000015000300000001310000000132000000013344000000150003000000013100000001320000000133440000001500030000000131000000013200000001334400000015000300000001310000000132000000013344000000150003000000013100000001320000000133440000001500030000000131000000013200000001334400000015000300000001310000000132000000013344000000150003000000013100000001320000000133440000001500030000000131000000013200000001334400000015000300000001310000000132000000013344000000150003000000013100000001320000000133440000001500030000000131000000013200000001334400000015000300000001310000000132000000013344000000150003000000013100000001320000000133440000001500030000000131000000013200000001334400000015000300000001310000000132000000013344000000150003000000013100000001320000000133440000001500030000000131000000013200000001334400000015000300000001310000000132000000013344000000150003000000013100000001320000000133440000001500030000000131000000013200000001334400000015000300000001310000000132000000013344000000150003000000013100000001320000000133\n" +
                "<4400000015000300000001310000000132000000013344000000150003000000013100000001320000000133440000001500030000000131000000013200000001334400000015000300000001310000000132000000013344000000150003000000013100000001320000000133440000001500030000000131000000013200000001334400000015000300000001310000000132000000013344000000150003000000013100000001320000000133440000001500030000000131000000013200000001334400000015000300000001310000000132000000013344000000150003000000013100000001320000000133440000001500030000000131000000013200000001334400000015000300000001310000000132000000013344000000150003000000013100000001320000000133440000001500030000000131000000013200000001334400000015000300000001310000000132000000013344000000150003000000013100000001320000000133440000001500030000000131000000013200000001334400000015000300000001310000000132000000013344000000150003000000013100000001320000000133440000001500030000000131000000013200000001334400000015000300000001310000000132000000013344000000150003000000013100000001320000000133\n" +
                "<44000000150003000000013100000001320000000133440000001500030000000131000000013200000001334400000015000300000001310000000132000000013344000000150003000000013100000001320000000133430000000e53454c454354203438005a0000000549\n" +
                ">50000000260073656c65637420312066726f6d206c6f6e675f73657175656e6365283229000000420000000c000000000000000044000000065000450000000900000000005300000004\n" +
                "<31000000043200000004540000001a00013100000040010001000000170004ffffffff0000440000000b00010000000131440000000b00010000000131430000000d53454c4543542032005a0000000549\n" +
                ">5800000004\n";
        NetworkFacade nf = new NetworkFacadeImpl() {
            boolean good = false;

            @Override
            public int send(long fd, long buffer, int bufferLen) {
                if (good) {
                    good = false;
                    return super.send(fd, buffer, bufferLen);
                }

                good = true;
                return 0;
            }
        };

        assertHexScript(NetworkFacadeImpl.INSTANCE, nf, script, new DefaultPGWireConfiguration() {
            @Override
            public int getSendBufferSize() {
                return 512;
            }

            @Override
            public String getDefaultPassword() {
                return "oh";
            }

            @Override
            public String getDefaultUsername() {
                return "xyz";
            }
        });
    }

    @Test
    public void testLoginBadPassword() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            final CountDownLatch haltLatch = new CountDownLatch(1);
            final AtomicBoolean running = new AtomicBoolean(true);
            try {
                startBasicServer(NetworkFacadeImpl.INSTANCE,
                        new DefaultPGWireConfiguration(),
                        haltLatch,
                        running
                );

                Properties properties = new Properties();
                properties.setProperty("user", "admin");
                properties.setProperty("password", "dunno");
                try {
                    DriverManager.getConnection("jdbc:postgresql://127.0.0.1:9120/qdb", properties);
                    Assert.fail();
                } catch (SQLException e) {
                    TestUtils.assertContains(e.getMessage(), "invalid username/password");
                }
            } finally {
                running.set(false);
                haltLatch.await();
            }
        });
    }

    @Test
    public void testLoginBadUsername() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            final CountDownLatch haltLatch = new CountDownLatch(1);
            final AtomicBoolean running = new AtomicBoolean(true);
            try {
                startBasicServer(NetworkFacadeImpl.INSTANCE,
                        new DefaultPGWireConfiguration(),
                        haltLatch,
                        running
                );

                Properties properties = new Properties();
                properties.setProperty("user", "joe");
                properties.setProperty("password", "quest");
                try {
                    DriverManager.getConnection("jdbc:postgresql://127.0.0.1:9120/qdb", properties);
                    Assert.fail();
                } catch (SQLException e) {
                    TestUtils.assertContains(e.getMessage(), "invalid username/password");
                }
            } finally {
                running.set(false);
                haltLatch.await();
            }
        });
    }

    @Test
    public void testMalformedInitPropertyName() throws Exception {
        assertHexScript(
                NetworkFacadeImpl.INSTANCE,
                NetworkFacadeImpl.INSTANCE,
                ">0000004c00030000757365720061646d696e006461746162617365006e6162755f61707000636c69656e745f656e636f64696e67005554463800446174655374796c650049534f0054696d655a6f6e6500474d540065787472615f666c6f61745f64696769747300320000\n" +
                        "<!!",
                new DefaultPGWireConfiguration()
        );
    }

    @Test
    public void testMalformedInitPropertyValue() throws Exception {
        assertHexScript(
                NetworkFacadeImpl.INSTANCE,
                NetworkFacadeImpl.INSTANCE,
                ">0000001e00030000757365720061646d696e006461746162617365006e6162755f61707000636c69656e745f656e636f64696e67005554463800446174655374796c650049534f0054696d655a6f6e6500474d540065787472615f666c6f61745f64696769747300320000\n" +
                        "<!!",
                new DefaultPGWireConfiguration()
        );
    }

    @Test
    public void testMicroTimestamp() throws Exception {
        assertMemoryLeak(() -> {
            final PGWireConfiguration conf = new DefaultPGWireConfiguration() {
                @Override
                public int[] getWorkerAffinity() {
                    return new int[]{-1, -1, -1, -1};
                }

                @Override
                public int getWorkerCount() {
                    return 4;
                }
            };

            try (final PGWireServer ignored = PGWireServer.create(
                    conf,
                    null,
                    LOG,
                    engine,
                    compiler.getFunctionFactoryCache()
            )) {
                Properties properties = new Properties();
                properties.setProperty("user", "admin");
                properties.setProperty("password", "quest");
                properties.setProperty("sslmode", "disable");
                TimeZone.setDefault(TimeZone.getTimeZone("EDT"));
                final Connection connection = DriverManager.getConnection("jdbc:postgresql://127.0.0.1:8812/qdb", properties);
                connection.prepareCall("create table x(t timestamp)").execute();

                PreparedStatement statement = connection.prepareStatement("insert into x values (?)");

                final String expected = "t[TIMESTAMP]\n" +
                        "2019-02-11 13:48:11.123998\n" +
                        "2019-02-11 13:48:11.123999\n" +
                        "2019-02-11 13:48:11.124\n" +
                        "2019-02-11 13:48:11.124001\n" +
                        "2019-02-11 13:48:11.124002\n" +
                        "2019-02-11 13:48:11.124003\n" +
                        "2019-02-11 13:48:11.124004\n" +
                        "2019-02-11 13:48:11.124005\n" +
                        "2019-02-11 13:48:11.124006\n" +
                        "2019-02-11 13:48:11.124007\n" +
                        "2019-02-11 13:48:11.124008\n" +
                        "2019-02-11 13:48:11.124009\n" +
                        "2019-02-11 13:48:11.12401\n" +
                        "2019-02-11 13:48:11.124011\n" +
                        "2019-02-11 13:48:11.124012\n" +
                        "2019-02-11 13:48:11.124013\n" +
                        "2019-02-11 13:48:11.124014\n" +
                        "2019-02-11 13:48:11.124015\n" +
                        "2019-02-11 13:48:11.124016\n" +
                        "2019-02-11 13:48:11.124017\n";

                long ts = TimestampFormatUtils.parseTimestamp("2019-02-11T13:48:11.123998Z");
                for (int i = 0; i < 20; i++) {
                    statement.setLong(1, ts + i);
                    statement.execute();
                }
                StringSink sink = new StringSink();
                PreparedStatement sel = connection.prepareStatement("x");
                ResultSet res = sel.executeQuery();
                assertResultSet(expected, sink, res);
                connection.close();
            }
        });
    }

    @Test
    public void testMultiplePreparedStatements() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            final CountDownLatch haltLatch = new CountDownLatch(1);
            final AtomicBoolean running = new AtomicBoolean(true);
            try {
                startBasicServer(
                        NetworkFacadeImpl.INSTANCE,
                        new DefaultPGWireConfiguration(),
                        haltLatch,
                        running
                );

                Properties properties = new Properties();
                properties.setProperty("user", "admin");
                properties.setProperty("password", "quest");
                properties.setProperty("sslmode", "disable");
                properties.setProperty("binaryTransfer", "false");
                final Connection connection = DriverManager.getConnection("jdbc:postgresql://127.0.0.1:9120/qdb", properties);
                PreparedStatement ps1 = connection.prepareStatement("select 1,2,3 from long_sequence(1)");
                PreparedStatement ps2 = connection.prepareStatement("select 4,5,6 from long_sequence(1)");
                PreparedStatement ps3 = connection.prepareStatement("select 7,8,9 from long_sequence(2)");

                final String expected = "1[INTEGER],2[INTEGER],3[INTEGER]\n" +
                        "1,2,3\n";

                StringSink sink = new StringSink();
                for (int i = 0; i < 10; i++) {
                    sink.clear();
                    ResultSet rs1 = ps1.executeQuery();
                    ResultSet rs2 = ps2.executeQuery();
                    ResultSet rs3 = ps3.executeQuery();

                    assertResultSet(expected, sink, rs1);
                    rs1.close();
                    rs2.close();
                    rs3.close();
                }

                Statement statement1 = connection.createStatement();
                for (int i = 0; i < 10; i++) {
                    PreparedStatement s = connection.prepareStatement("select 2,2,2,2 from long_sequence(1)");
                    s.executeQuery();
                    statement1.executeQuery("select 1 from long_sequence(2)");
                }
                connection.close();
            } finally {
                running.set(false);
                haltLatch.await();
            }
        });
    }

    @Test
    public void testNamedStatementWithoutParameterTypeHex() throws Exception {
        String script = ">0000006e00030000757365720078797a0064617461626173650071646200636c69656e745f656e636f64696e67005554463800446174655374796c650049534f0054696d655a6f6e65004575726f70652f4c6f6e646f6e0065787472615f666c6f61745f64696769747300320000\n" +
                "<520000000800000003\n" +
                ">70000000076f6800\n" +
                "<520000000800000000530000001154696d655a6f6e6500474d5400530000001d6170706c69636174696f6e5f6e616d6500517565737444420053000000187365727665725f76657273696f6e0031312e33005300000019696e74656765725f6461746574696d6573006f6e005300000019636c69656e745f656e636f64696e670055544638005a0000000549\n" +
                ">5000000022005345542065787472615f666c6f61745f646967697473203d2033000000420000000c0000000000000000450000000900000000015300000004\n" +
                "<310000000432000000044300000008534554005a0000000549\n" +
                ">500000003700534554206170706c69636174696f6e5f6e616d65203d2027506f737467726553514c204a4442432044726976657227000000420000000c0000000000000000450000000900000000015300000004\n" +
                "<310000000432000000044300000008534554005a0000000549\n" +
                ">5000000032535f310073656c65637420782c24312c24322c24332066726f6d206c6f6e675f73657175656e6365283229000000420000002900535f31000003000000000000000300000001340000000331323300000004352e3433000044000000065000450000000900000000005300000004\n" +
                "<31000000043200000004540000005900047800000040010001000000140004ffffffff0000243100000040010002000000170004ffffffff0000243200000040010003000000170004ffffffff0000243300000040010004000002bd0004ffffffff0000440000001f0004000000013100000001340000000331323300000004352e3433440000001f0004000000013200000001340000000331323300000004352e3433430000000d53454c4543542032005a0000000549\n" +
                ">420000002900535f31000003000000000000000300000001340000000331323300000004352e34330000450000000900000000005300000004\n" +
                "<3200000004440000001f0004000000013100000001340000000331323300000004352e3433440000001f0004000000013200000001340000000331323300000004352e3433430000000d53454c4543542032005a0000000549\n" +
                ">430000000953535f31005300000004\n" +
                "<33000000045a0000000549\n" +
                ">5800000004";
        assertHexScript(NetworkFacadeImpl.INSTANCE,
                NetworkFacadeImpl.INSTANCE,
                script,
                getHexPgWireConfig());
    }

    @Test
    public void testNoDataAndEmptyQueryResponsesHex() throws Exception {
        String script = ">0000006e00030000757365720078797a0064617461626173650071646200636c69656e745f656e636f64696e67005554463800446174655374796c650049534f0054696d655a6f6e65004575726f70652f4c6f6e646f6e0065787472615f666c6f61745f64696769747300320000\n" +
                "<520000000800000003\n" +
                ">70000000076f6800\n" +
                "<520000000800000000530000001154696d655a6f6e6500474d5400530000001d6170706c69636174696f6e5f6e616d6500517565737444420053000000187365727665725f76657273696f6e0031312e33005300000019696e74656765725f6461746574696d6573006f6e005300000019636c69656e745f656e636f64696e670055544638005a0000000549\n" +
                ">5000000022005345542065787472615f666c6f61745f646967697473203d2033000000420000000c0000000000000000450000000900000000015300000004\n" +
                "<310000000432000000044300000008534554005a0000000549\n" +
                ">500000003700534554206170706c69636174696f6e5f6e616d65203d2027506f737467726553514c204a4442432044726976657227000000420000000c0000000000000000450000000900000000015300000004\n" +
                "<310000000432000000044300000008534554005a0000000549\n" +
                ">500000000800000000420000000c000000000000000044000000065000450000000900000000005300000004\n" +
                "<310000000432000000046e000000046e0000000449000000045a0000000549";
        assertHexScript(NetworkFacadeImpl.INSTANCE,
                NetworkFacadeImpl.INSTANCE,
                script,
                getHexPgWireConfig());
    }

    @Test
    public void testParameterTypeCountGreaterThanParameterValueCount() throws Exception {
        String script = ">0000006e00030000757365720078797a0064617461626173650071646200636c69656e745f656e636f64696e67005554463800446174655374796c650049534f0054696d655a6f6e65004575726f70652f4c6f6e646f6e0065787472615f666c6f61745f64696769747300320000\n" +
                "<520000000800000003\n" +
                ">70000000076f6800\n" +
                "<520000000800000000530000001154696d655a6f6e6500474d5400530000001d6170706c69636174696f6e5f6e616d6500517565737444420053000000187365727665725f76657273696f6e0031312e33005300000019696e74656765725f6461746574696d6573006f6e005300000019636c69656e745f656e636f64696e670055544638005a0000000549\n" +
                ">5000000022005345542065787472615f666c6f61745f646967697473203d2033000000420000000c0000000000000000450000000900000000015300000004\n" +
                "<310000000432000000044300000008534554005a0000000549\n" +
                ">500000003700534554206170706c69636174696f6e5f6e616d65203d2027506f737467726553514c204a4442432044726976657227000000420000000c0000000000000000450000000900000000015300000004\n" +
                "<310000000432000000044300000008534554005a0000000549\n" +
                ">500000003b0073656c65637420782c24312c24322c24332066726f6d206c6f6e675f73657175656e63652832290000030000001700000014000002bd420000002600000003000000000000000200000001340000000331323300000004352e3433000044000000065000450000000900000000005300000004\n" +
                "<!!";
        assertHexScript(NetworkFacadeImpl.INSTANCE,
                NetworkFacadeImpl.INSTANCE,
                script,
                getHexPgWireConfig());
    }

    @Test
    public void testParseMessageBadQueryTerminator() throws Exception {
        final String script = ">0000006900030000757365720078797a006461746162617365006e6162755f61707000636c69656e745f656e636f64696e67005554463800446174655374796c650049534f0054696d655a6f6e6500474d540065787472615f666c6f61745f64696769747300320000\n" +
                "<520000000800000003\n" +
                ">70000000076f6800\n" +
                "<520000000800000000530000001154696d655a6f6e6500474d5400530000001d6170706c69636174696f6e5f6e616d6500517565737444420053000000187365727665725f76657273696f6e0031312e33005300000019696e74656765725f6461746574696d6573006f6e005300000019636c69656e745f656e636f64696e670055544638005a0000000549\n" +
                ">5000000022005345542065787472615f666c6f61745f646967697473203d203308899889988998\n" +
                "<!!";
        assertHexScript(
                NetworkFacadeImpl.INSTANCE,
                NetworkFacadeImpl.INSTANCE,
                script,
                getHexPgWireConfig()
        );
    }

    @Test
    public void testParseMessageBadStatementTerminator() throws Exception {
        final String script = ">0000006900030000757365720078797a006461746162617365006e6162755f61707000636c69656e745f656e636f64696e67005554463800446174655374796c650049534f0054696d655a6f6e6500474d540065787472615f666c6f61745f64696769747300320000\n" +
                "<520000000800000003\n" +
                ">70000000076f6800\n" +
                "<520000000800000000530000001154696d655a6f6e6500474d5400530000001d6170706c69636174696f6e5f6e616d6500517565737444420053000000187365727665725f76657273696f6e0031312e33005300000019696e74656765725f6461746574696d6573006f6e005300000019636c69656e745f656e636f64696e670055544638005a0000000549\n" +
                ">5000000022555345542065787472615f666c6f61745f646967697473203d2033555555425555550c5555555555555555455555550955555555015355555504\n" +
                "<!!";
        assertHexScript(
                NetworkFacadeImpl.INSTANCE,
                NetworkFacadeImpl.INSTANCE,
                script,
                getHexPgWireConfig()
        );
    }

    @Test
    public void testParseMessageNegativeParameterCount() throws Exception {
        final String script = ">0000006900030000757365720078797a006461746162617365006e6162755f61707000636c69656e745f656e636f64696e67005554463800446174655374796c650049534f0054696d655a6f6e6500474d540065787472615f666c6f61745f64696769747300320000\n" +
                "<520000000800000003\n" +
                ">70000000076f6800\n" +
                "<520000000800000000530000001154696d655a6f6e6500474d5400530000001d6170706c69636174696f6e5f6e616d6500517565737444420053000000187365727665725f76657273696f6e0031312e33005300000019696e74656765725f6461746574696d6573006f6e005300000019636c69656e745f656e636f64696e670055544638005a0000000549\n" +
                ">5000000022005345542065787472615f666c6f61745f646967697473203d2033000000420000000c0000000000000000450000000900000000015300000004\n" +
                "<310000000432000000044300000008534554005a0000000549\n" +
                ">500000003700534554206170706c69636174696f6e5f6e616d65203d2027506f737467726553514c204a4442432044726976657227000000420000000c0000000000000000450000000900000000015300000004\n" +
                "<310000000432000000044300000008534554005a0000000549\n" +
                ">50000000cd0073656c65637420782c24312c24322c24332c24342c24352c24362c24372c24382c24392c2431302c2431312c2431322c2431332c2431342c2431352c2431362c2431372c2431382c2431392c2432302c2432312c2432322066726f6d206c6f6e675f73657175656e636528352900fefe0000001700000014000002bc000002bd0000001500000010000004130000041300000000000000000000001700000014000002bc000002bd000000150000001000000413000004130000043a000000000000045a000004a0420000012c0000001600010001000100010001000000000000000000000001000100010001000100000000000000010000000000000016000000040000000400000008000000000000007b0000000440adc28f000000083fe22c27a63736ce00000002005b00000004545255450000000568656c6c6f0000001dd0b3d180d183d0bfd0bfd0b020d182d183d180d0b8d181d182d0bed0b20000000e313937302d30312d3031202b30300000001a313937302d30382d32302031313a33333a32302e3033332b3030ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff0000001a313937302d30312d30312030303a30353a30302e3031312b30300000001a313937302d30312d30312030303a30383a32302e3032332b3030000044000000065000450000000900000000005300000004\n" +
                "<!!";
        assertHexScript(
                NetworkFacadeImpl.INSTANCE,
                NetworkFacadeImpl.INSTANCE,
                script,
                getHexPgWireConfig()
        );
    }

    @Test
    public void testParseMessageTruncatedAtParameter() throws Exception {
        final String script = ">0000006900030000757365720078797a006461746162617365006e6162755f61707000636c69656e745f656e636f64696e67005554463800446174655374796c650049534f0054696d655a6f6e6500474d540065787472615f666c6f61745f64696769747300320000\n" +
                "<520000000800000003\n" +
                ">70000000076f6800\n" +
                "<520000000800000000530000001154696d655a6f6e6500474d5400530000001d6170706c69636174696f6e5f6e616d6500517565737444420053000000187365727665725f76657273696f6e0031312e33005300000019696e74656765725f6461746574696d6573006f6e005300000019636c69656e745f656e636f64696e670055544638005a0000000549\n" +
                ">5000000022005345542065787472615f666c6f61745f646967697473203d2033000000420000000c0000000000000000450000000900000000015300000004\n" +
                "<310000000432000000044300000008534554005a0000000549\n" +
                ">500000003700534554206170706c69636174696f6e5f6e616d65203d2027506f737467726553514c204a4442432044726976657227000000420000000c0000000000000000450000000900000000015300000004\n" +
                "<310000000432000000044300000008534554005a0000000549\n" +
                ">50000000cd0073656c65637420782c24312c24322c24332c24342c24352c24362c24372c24382c24392c2431302c2431312c2431322c2431332c2431342c2431352c2431362c2431372c2431382c2431392c2432302c2432312c2432322066726f6d206c6f6e675f73657175656e63652835290000260000001700000014000002bc000002bd0000001500000010000004130000041300000000000000000000001700000014000002bc000002bd000000150000001000000413000004130000043a000000000000045a000004a0420000012c0000001600010001000100010001000000000000000000000001000100010001000100000000000000010000000000000016000000040000000400000008000000000000007b0000000440adc28f000000083fe22c27a63736ce00000002005b00000004545255450000000568656c6c6f0000001dd0b3d180d183d0bfd0bfd0b020d182d183d180d0b8d181d182d0bed0b20000000e313937302d30312d3031202b30300000001a313937302d30382d32302031313a33333a32302e3033332b3030ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff0000001a313937302d30312d30312030303a30353a30302e3031312b30300000001a313937302d30312d30312030303a30383a32302e3032332b3030000044000000065000450000000900000000005300000004\n" +
                "<!!";
        assertHexScript(
                NetworkFacadeImpl.INSTANCE,
                NetworkFacadeImpl.INSTANCE,
                script,
                getHexPgWireConfig()
        );
    }

    @Test
    public void testParseMessageTruncatedAtParameterCount() throws Exception {
        final String script = ">0000006900030000757365720078797a006461746162617365006e6162755f61707000636c69656e745f656e636f64696e67005554463800446174655374796c650049534f0054696d655a6f6e6500474d540065787472615f666c6f61745f64696769747300320000\n" +
                "<520000000800000003\n" +
                ">70000000076f6800\n" +
                "<520000000800000000530000001154696d655a6f6e6500474d5400530000001d6170706c69636174696f6e5f6e616d6500517565737444420053000000187365727665725f76657273696f6e0031312e33005300000019696e74656765725f6461746574696d6573006f6e005300000019636c69656e745f656e636f64696e670055544638005a0000000549\n" +
                ">5000000022005345542065787472615f666c6f61745f646967697473203d2033000000420000000c0000000000000000450000000900000000015300000004\n" +
                "<310000000432000000044300000008534554005a0000000549\n" +
                ">500000003700534554206170706c69636174696f6e5f6e616d65203d2027506f737467726553514c204a4442432044726976657227000000420000000c0000000000000000450000000900000000015300000004\n" +
                "<310000000432000000044300000008534554005a0000000549\n" +
                // II
                ">50000000740073656c65637420782c24312c24322c24332c24342c24352c24362c24372c24382c24392c2431302c2431312c2431322c2431332c2431342c2431352c2431362c2431372c2431382c2431392c2432302c2432312c2432322066726f6d206c6f6e675f73657175656e63652835290000160000001700000014000002bc000002bd0000001500000010000004130000041300000000000000000000001700000014000002bc000002bd000000150000001000000413000004130000043a000000000000045a000004a0420000012c0000001600010001000100010001000000000000000000000001000100010001000100000000000000010000000000000016000000040000000400000008000000000000007b0000000440adc28f000000083fe22c27a63736ce00000002005b00000004545255450000000568656c6c6f0000001dd0b3d180d183d0bfd0bfd0b020d182d183d180d0b8d181d182d0bed0b20000000e313937302d30312d3031202b30300000001a313937302d30382d32302031313a33333a32302e3033332b3030ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff0000001a313937302d30312d30312030303a30353a30302e3031312b30300000001a313937302d30312d30312030303a30383a32302e3032332b3030000044000000065000450000000900000000005300000004\n" +
                "<!!";
        assertHexScript(
                NetworkFacadeImpl.INSTANCE,
                NetworkFacadeImpl.INSTANCE,
                script,
                getHexPgWireConfig()
        );
    }

    @Test
    public void testPreparedStatement() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            final CountDownLatch haltLatch = new CountDownLatch(1);
            final AtomicBoolean running = new AtomicBoolean(true);
            try {
                startBasicServer(
                        NetworkFacadeImpl.INSTANCE,
                        new DefaultPGWireConfiguration(),
                        haltLatch,
                        running
                );

                Properties properties = new Properties();
                properties.setProperty("user", "admin");
                properties.setProperty("password", "quest");
                properties.setProperty("sslmode", "disable");
                properties.setProperty("binaryTransfer", "false");
                final Connection connection = DriverManager.getConnection("jdbc:postgresql://127.0.0.1:9120/qdb", properties);
                PreparedStatement statement = connection.prepareStatement("select 1,2,3 from long_sequence(1)");
                Statement statement1 = connection.createStatement();

                final String expected = "1[INTEGER],2[INTEGER],3[INTEGER]\n" +
                        "1,2,3\n";

                StringSink sink = new StringSink();
                for (int i = 0; i < 10; i++) {
                    sink.clear();
                    ResultSet rs = statement.executeQuery();

                    statement1.executeQuery("select 1 from long_sequence(2)");
                    assertResultSet(expected, sink, rs);
                    rs.close();
                }
                connection.close();
            } finally {
                running.set(false);
                haltLatch.await();
            }
        });
    }

    @Test
    public void testPreparedStatementHex() throws Exception {
        assertPreparedStatementHex(NetworkFacadeImpl.INSTANCE, getHexPgWireConfig());
    }

    @Test
    public void testPreparedStatementHexSendFlowControl() throws Exception {

        NetworkFacade nf = new NetworkFacadeImpl() {
            boolean good = false;

            @Override
            public int send(long fd, long buffer, int bufferLen) {
                if (good) {
                    good = false;
                    return super.send(fd, buffer, bufferLen);
                }

                good = true;
                return 0;
            }
        };

        PGWireConfiguration configuration = new DefaultPGWireConfiguration() {
            @Override
            public NetworkFacade getNetworkFacade() {
                return nf;
            }

            @Override
            public int getIdleSendCountBeforeGivingUp() {
                return 0;
            }

            @Override
            public String getDefaultPassword() {
                return "oh";
            }

            @Override
            public String getDefaultUsername() {
                return "xyz";
            }
        };
        assertPreparedStatementHex(nf, configuration);
    }

    @Test
    public void testPreparedStatementHexSendFlowControl2() throws Exception {

        // the objective of this setup is to exercise send retry behaviour

        NetworkFacade nf = new NetworkFacadeImpl() {
            final int counterBreak = 2;
            int counter = 0;

            @Override
            public int send(long fd, long buffer, int bufferLen) {
                if (counter++ % counterBreak == 0) {
                    return super.send(fd, buffer, bufferLen);
                }
                return 0;
            }
        };

        PGWireConfiguration configuration = new DefaultPGWireConfiguration() {
            @Override
            public NetworkFacade getNetworkFacade() {
                return nf;
            }

            @Override
            public int getIdleSendCountBeforeGivingUp() {
                return 1;
            }

            @Override
            public String getDefaultPassword() {
                return "oh";
            }

            @Override
            public String getDefaultUsername() {
                return "xyz";
            }
        };

        assertPreparedStatementHex(nf, configuration);
    }

    @Test
    public void testPreparedStatementHexSendFlowControl3() throws Exception {

        // the objective of this setup is to exercise send retry behaviour

        NetworkFacade nf = new NetworkFacadeImpl() {
            final int counterBreak = 3; // another branch of retry logic
            int counter = 0;

            @Override
            public int send(long fd, long buffer, int bufferLen) {
                if (counter++ % counterBreak == 0) {
                    return super.send(fd, buffer, bufferLen);
                }
                return 0;
            }
        };

        PGWireConfiguration configuration = new DefaultPGWireConfiguration() {
            @Override
            public NetworkFacade getNetworkFacade() {
                return nf;
            }

            @Override
            public int getIdleSendCountBeforeGivingUp() {
                return 1;
            }

            @Override
            public String getDefaultPassword() {
                return "oh";
            }

            @Override
            public String getDefaultUsername() {
                return "xyz";
            }
        };

        assertPreparedStatementHex(nf, configuration);
    }

    @Test
    public void testPreparedStatementParamBadByte() throws Exception {
        assertHexScript(
                NetworkFacadeImpl.INSTANCE,
                NetworkFacadeImpl.INSTANCE,
                ">0000006b00030000757365720061646d696e006461746162617365006e6162755f61707000636c69656e745f656e636f64696e67005554463800446174655374796c650049534f0054696d655a6f6e6500474d540065787472615f666c6f61745f64696769747300320000\n" +
                        "<520000000800000003\n" +
                        ">700000000a717565737400\n" +
                        "<520000000800000000530000001154696d655a6f6e6500474d5400530000001d6170706c69636174696f6e5f6e616d6500517565737444420053000000187365727665725f76657273696f6e0031312e33005300000019696e74656765725f6461746574696d6573006f6e005300000019636c69656e745f656e636f64696e670055544638005a0000000549\n" +
                        ">5000000022005345542065787472615f666c6f61745f646967697473203d2033000000420000000c0000000000000000450000000900000000015300000004\n" +
                        "<310000000432000000044300000008534554005a0000000549\n" +
                        ">500000003700534554206170706c69636174696f6e5f6e616d65203d2027506f737467726553514c204a4442432044726976657227000000420000000c0000000000000000450000000900000000015300000004\n" +
                        "<310000000432000000044300000008534554005a0000000549\n" +
                        ">50000000cd0073656c65637420782c24312c24322c24332c24342c24352c24362c24372c24382c24392c2431302c2431312c2431322c2431332c2431342c2431352c2431362c2431372c2431382c2431392c2432302c2432312c2432322066726f6d206c6f6e675f73657175656e63652835290000160000001700000014000002bd000002bd0000001500000010000004130000041300000000000000000000001700000014000002bc000002bd000000150000001000000413000004130000043a000000000000045a000004a04200000123000000160000000000000000000000000000000000000000000000000000000000000000000000000000000000000000001600000001340000000331323300000004352e343300000007302e353637383900000002993100000004545255450000000568656c6c6f0000001dd0b3d180d183d0bfd0bfd0b020d182d183d180d0b8d181d182d0bed0b20000000e313937302d30312d3031202b30300000001a313937302d30382d32302031313a33333a32302e3033332b3030ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff0000001a313937302d30312d30312030303a30353a30302e3031312b30300000001a313937302d30312d30312030303a30383a32302e3032332b3030000044000000065000450000000900000000005300000004\n" +
                        "<!!",
                new DefaultPGWireConfiguration()
        );
    }

    @Test
    public void testPreparedStatementParamBadDouble() throws Exception {
        assertHexScript(
                NetworkFacadeImpl.INSTANCE,
                NetworkFacadeImpl.INSTANCE,
                ">0000006b00030000757365720061646d696e006461746162617365006e6162755f61707000636c69656e745f656e636f64696e67005554463800446174655374796c650049534f0054696d655a6f6e6500474d540065787472615f666c6f61745f64696769747300320000\n" +
                        "<520000000800000003\n" +
                        ">700000000a717565737400\n" +
                        "<520000000800000000530000001154696d655a6f6e6500474d5400530000001d6170706c69636174696f6e5f6e616d6500517565737444420053000000187365727665725f76657273696f6e0031312e33005300000019696e74656765725f6461746574696d6573006f6e005300000019636c69656e745f656e636f64696e670055544638005a0000000549\n" +
                        ">5000000022005345542065787472615f666c6f61745f646967697473203d2033000000420000000c0000000000000000450000000900000000015300000004\n" +
                        "<310000000432000000044300000008534554005a0000000549\n" +
                        ">500000003700534554206170706c69636174696f6e5f6e616d65203d2027506f737467726553514c204a4442432044726976657227000000420000000c0000000000000000450000000900000000015300000004\n" +
                        "<310000000432000000044300000008534554005a0000000549\n" +
                        ">50000000cd0073656c65637420782c24312c24322c24332c24342c24352c24362c24372c24382c24392c2431302c2431312c2431322c2431332c2431342c2431352c2431362c2431372c2431382c2431392c2432302c2432312c2432322066726f6d206c6f6e675f73657175656e63652835290000160000001700000014000002bd000002bd0000001500000010000004130000041300000000000000000000001700000014000002bc000002bd000000150000001000000413000004130000043a000000000000045a000004a04200000123000000160000000000000000000000000000000000000000000000000000000000000000000000000000000000000000001600000001340000000331323300000004352f343300000007302e353637383900000002393100000004545255450000000568656c6c6f0000001dd0b3d180d183d0bfd0bfd0b020d182d183d180d0b8d181d182d0bed0b20000000e313937302d30312d3031202b30300000001a313937302d30382d32302031313a33333a32302e3033332b3030ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff0000001a313937302d30312d30312030303a30353a30302e3031312b30300000001a313937302d30312d30312030303a30383a32302e3032332b3030000044000000065000450000000900000000005300000004\n" +
                        "<!!",
                new DefaultPGWireConfiguration()
        );
    }

    @Test
    public void testPreparedStatementParamBadInt() throws Exception {
        assertHexScript(
                NetworkFacadeImpl.INSTANCE,
                NetworkFacadeImpl.INSTANCE,
                ">0000006b00030000757365720061646d696e006461746162617365006e6162755f61707000636c69656e745f656e636f64696e67005554463800446174655374796c650049534f0054696d655a6f6e6500474d540065787472615f666c6f61745f64696769747300320000\n" +
                        "<520000000800000003\n" +
                        ">700000000a717565737400\n" +
                        "<520000000800000000530000001154696d655a6f6e6500474d5400530000001d6170706c69636174696f6e5f6e616d6500517565737444420053000000187365727665725f76657273696f6e0031312e33005300000019696e74656765725f6461746574696d6573006f6e005300000019636c69656e745f656e636f64696e670055544638005a0000000549\n" +
                        ">5000000022005345542065787472615f666c6f61745f646967697473203d2033000000420000000c0000000000000000450000000900000000015300000004\n" +
                        "<310000000432000000044300000008534554005a0000000549\n" +
                        ">500000003700534554206170706c69636174696f6e5f6e616d65203d2027506f737467726553514c204a4442432044726976657227000000420000000c0000000000000000450000000900000000015300000004\n" +
                        "<310000000432000000044300000008534554005a0000000549\n" +
                        ">50000000cd0073656c65637420782c24312c24322c24332c24342c24352c24362c24372c24382c24392c2431302c2431312c2431322c2431332c2431342c2431352c2431362c2431372c2431382c2431392c2432302c2432312c2432322066726f6d206c6f6e675f73657175656e63652835290000160000001700000014000002bd000002bd0000001500000010000004130000041300000000000000000000001700000014000002bc000002bd000000150000001000000413000004130000043a000000000000045a000004a04200000123000000160000000000000000000000000000000000000000000000000000000000000000000000000000000000000000001600000001fc0000000331323300000004352e343300000007302e353637383900000002393100000004545255450000000568656c6c6f0000001dd0b3d180d183d0bfd0bfd0b020d182d183d180d0b8d181d182d0bed0b20000000e313937302d30312d3031202b30300000001a313937302d30382d32302031313a33333a32302e3033332b3030ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff0000001a313937302d30312d30312030303a30353a30302e3031312b30300000001a313937302d30312d30312030303a30383a32302e3032332b3030000044000000065000450000000900000000005300000004\n" +
                        "<!!",
                new DefaultPGWireConfiguration()
        );
    }

    @Test
    public void testPreparedStatementParamBadLong() throws Exception {
        assertHexScript(
                NetworkFacadeImpl.INSTANCE,
                NetworkFacadeImpl.INSTANCE,
                ">0000006b00030000757365720061646d696e006461746162617365006e6162755f61707000636c69656e745f656e636f64696e67005554463800446174655374796c650049534f0054696d655a6f6e6500474d540065787472615f666c6f61745f64696769747300320000\n" +
                        "<520000000800000003\n" +
                        ">700000000a717565737400\n" +
                        "<520000000800000000530000001154696d655a6f6e6500474d5400530000001d6170706c69636174696f6e5f6e616d6500517565737444420053000000187365727665725f76657273696f6e0031312e33005300000019696e74656765725f6461746574696d6573006f6e005300000019636c69656e745f656e636f64696e670055544638005a0000000549\n" +
                        ">5000000022005345542065787472615f666c6f61745f646967697473203d2033000000420000000c0000000000000000450000000900000000015300000004\n" +
                        "<310000000432000000044300000008534554005a0000000549\n" +
                        ">500000003700534554206170706c69636174696f6e5f6e616d65203d2027506f737467726553514c204a4442432044726976657227000000420000000c0000000000000000450000000900000000015300000004\n" +
                        "<310000000432000000044300000008534554005a0000000549\n" +
                        ">50000000cd0073656c65637420782c24312c24322c24332c24342c24352c24362c24372c24382c24392c2431302c2431312c2431322c2431332c2431342c2431352c2431362c2431372c2431382c2431392c2432302c2432312c2432322066726f6d206c6f6e675f73657175656e63652835290000160000001700000014000002bd000002bd0000001500000010000004130000041300000000000000000000001700000014000002bc000002bd000000150000001000000413000004130000043a000000000000045a000004a04200000123000000160000000000000000000000000000000000000000000000000000000000000000000000000000000000000000001600000001340000000331b23300000004352e343300000007302e353637383900000002393100000004545255450000000568656c6c6f0000001dd0b3d180d183d0bfd0bfd0b020d182d183d180d0b8d181d182d0bed0b20000000e313937302d30312d3031202b30300000001a313937302d30382d32302031313a33333a32302e3033332b3030ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff0000001a313937302d30312d30312030303a30353a30302e3031312b30300000001a313937302d30312d30312030303a30383a32302e3032332b3030000044000000065000450000000900000000005300000004\n" +
                        "<!!",
                new DefaultPGWireConfiguration()
        );
    }

    @Test
    public void testPreparedStatementParamValueLengthOverflow() throws Exception {
        assertHexScript(
                NetworkFacadeImpl.INSTANCE,
                NetworkFacadeImpl.INSTANCE,
                ">0000006b00030000757365720061646d696e006461746162617365006e6162755f61707000636c69656e745f656e636f64696e67005554463800446174655374796c650049534f0054696d655a6f6e6500474d540065787472615f666c6f61745f64696769747300320000\n" +
                        "<520000000800000003\n" +
                        ">700000000a717565737400\n" +
                        "<520000000800000000530000001154696d655a6f6e6500474d5400530000001d6170706c69636174696f6e5f6e616d6500517565737444420053000000187365727665725f76657273696f6e0031312e33005300000019696e74656765725f6461746574696d6573006f6e005300000019636c69656e745f656e636f64696e670055544638005a0000000549\n" +
                        ">5000000022005345542065787472615f666c6f61745f646967697473203d2033000000420000000c0000000000000000450000000900000000015300000004\n" +
                        "<310000000432000000044300000008534554005a0000000549\n" +
                        ">500000003700534554206170706c69636174696f6e5f6e616d65203d2027506f737467726553514c204a4442432044726976657227000000420000000c0000000000000000450000000900000000015300000004\n" +
                        "<310000000432000000044300000008534554005a0000000549\n" +
                        ">50000000cd0073656c65637420782c24312c24322c24332c24342c24352c24362c24372c24382c24392c2431302c2431312c2431322c2431332c2431342c2431352c2431362c2431372c2431382c2431392c2432302c2432312c2432322066726f6d206c6f6e675f73657175656e63652835290000160000001700000014000002bd000002bd0000001500000010000004130000041300000000000000000000001700000014000002bc000002bd000000150000001000000413000004130000043a000000000000045a000004a04200000123000000160000000000000000000000000000000000000000000000000000000000000000000000000000000000000000001600000001340000333331b23300000004352e343300000007302e353637383900000002393100000004545255450000000568656c6c6f0000001dd0b3d180d183d0bfd0bfd0b020d182d183d180d0b8d181d182d0bed0b20000000e313937302d30312d3031202b30300000001a313937302d30382d32302031313a33333a32302e3033332b3030ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff0000001a313937302d30312d30312030303a30353a30302e3031312b30300000001a313937302d30312d30312030303a30383a32302e3032332b3030000044000000065000450000000900000000005300000004\n" +
                        "<!!",
                new DefaultPGWireConfiguration()
        );
    }

    @Test
    public void testPreparedStatementParams() throws Exception {
        assertMemoryLeak(() -> {
            final PGWireConfiguration conf = new DefaultPGWireConfiguration() {
                @Override
                public int[] getWorkerAffinity() {
                    return new int[]{-1, -1, -1, -1};
                }

                @Override
                public int getWorkerCount() {
                    return 4;
                }
            };

            try (final PGWireServer ignored = PGWireServer.create(
                    conf,
                    null,
                    LOG,
                    engine,
                    compiler.getFunctionFactoryCache()
            )) {
                Properties properties = new Properties();
                properties.setProperty("user", "admin");
                properties.setProperty("password", "quest");
                properties.setProperty("sslmode", "disable");
                properties.setProperty("binaryTransfer", "true");
                TimeZone.setDefault(TimeZone.getTimeZone("EDT"));
                final Connection connection = DriverManager.getConnection("jdbc:postgresql://127.0.0.1:8812/qdb", properties);
                PreparedStatement statement = connection.prepareStatement("select x,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,? from long_sequence(5)");
                statement.setInt(1, 4);
                statement.setLong(2, 123L);
                statement.setFloat(3, 5.43f);
                statement.setDouble(4, 0.56789);
                statement.setByte(5, (byte) 91);
                statement.setBoolean(6, true);
                statement.setString(7, "hello");
                // this is to test UTF8 behaviour
                statement.setString(8, "группа туристов");
                statement.setDate(9, new Date(100L));
                statement.setTimestamp(10, new Timestamp(20000000033L));

                // nulls
                statement.setNull(11, Types.INTEGER);
                statement.setNull(12, Types.BIGINT);
                statement.setNull(13, Types.REAL);
                statement.setNull(14, Types.DOUBLE);
                statement.setNull(15, Types.SMALLINT);
                statement.setNull(16, Types.BOOLEAN);
                statement.setNull(17, Types.VARCHAR);
                statement.setString(18, null);
                statement.setNull(19, Types.DATE);
                // bizarrely this NULL will go out to server as type UNSPECIFIED, despite JDBC API
                // actually specifying the type as you can see
                // todo: null like this needs to be derived from table field where it goes to
                //    this is not yet supported
                // statement.setNull(20, Types.TIMESTAMP);
                // and so is this - UNSPECIFIED apparently
                // statement.setTimestamp(20, null);

                // when someone uses PostgreSQL's type extensions, which alter driver behaviour
                // we should handle this gracefully

                statement.setTimestamp(20, new PGTimestamp(300011));
                statement.setTimestamp(21, new PGTimestamp(500023, new GregorianCalendar()));

                final String expected = "x[BIGINT],$1[INTEGER],$2[BIGINT],$3[REAL],$4[DOUBLE],$5[SMALLINT],$6[BIT],$7[VARCHAR],$8[VARCHAR],$9[DATE],$10[TIMESTAMP],$11[INTEGER],$12[BIGINT],$13[REAL],$14[DOUBLE],$15[SMALLINT],$16[BIT],$17[VARCHAR],$18[VARCHAR],$19[DATE],$20[TIMESTAMP],$21[TIMESTAMP]\n" +
                        "1,4,123,5.429999828338623,0.56789,91,true,hello,группа туристов,1970-01-01 00:00:00.0,1970-08-20 11:33:20.033,null,null,null,null,0,false,null,null,null,1970-01-01 00:05:00.011,1970-01-01 00:08:20.023\n" +
                        "2,4,123,5.429999828338623,0.56789,91,true,hello,группа туристов,1970-01-01 00:00:00.0,1970-08-20 11:33:20.033,null,null,null,null,0,false,null,null,null,1970-01-01 00:05:00.011,1970-01-01 00:08:20.023\n" +
                        "3,4,123,5.429999828338623,0.56789,91,true,hello,группа туристов,1970-01-01 00:00:00.0,1970-08-20 11:33:20.033,null,null,null,null,0,false,null,null,null,1970-01-01 00:05:00.011,1970-01-01 00:08:20.023\n" +
                        "4,4,123,5.429999828338623,0.56789,91,true,hello,группа туристов,1970-01-01 00:00:00.0,1970-08-20 11:33:20.033,null,null,null,null,0,false,null,null,null,1970-01-01 00:05:00.011,1970-01-01 00:08:20.023\n" +
                        "5,4,123,5.429999828338623,0.56789,91,true,hello,группа туристов,1970-01-01 00:00:00.0,1970-08-20 11:33:20.033,null,null,null,null,0,false,null,null,null,1970-01-01 00:05:00.011,1970-01-01 00:08:20.023\n";

                StringSink sink = new StringSink();
                for (int i = 0; i < 10000; i++) {
                    sink.clear();
                    ResultSet rs = statement.executeQuery();
                    assertResultSet(expected, sink, rs);
                    rs.close();
                }
                connection.close();
            }
        });
    }

    @Test
    public void testPreparedStatementTextParams() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            final CountDownLatch haltLatch = new CountDownLatch(1);
            final AtomicBoolean running = new AtomicBoolean(true);
            try {
                startBasicServer(
                        NetworkFacadeImpl.INSTANCE,
                        new DefaultPGWireConfiguration(),
                        haltLatch,
                        running
                );

                Properties properties = new Properties();
                properties.setProperty("user", "admin");
                properties.setProperty("password", "quest");
                properties.setProperty("sslmode", "disable");
                properties.setProperty("binaryTransfer", "false");
                TimeZone.setDefault(TimeZone.getTimeZone("EDT"));
                final Connection connection = DriverManager.getConnection("jdbc:postgresql://127.0.0.1:9120/qdb", properties);
                PreparedStatement statement = connection.prepareStatement("select x,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,? from long_sequence(5)");
                statement.setInt(1, 4);
                statement.setLong(2, 123L);
                statement.setFloat(3, 5.43f);
                statement.setDouble(4, 0.56789);
                statement.setByte(5, (byte) 91);
                statement.setBoolean(6, true);
                statement.setString(7, "hello");
                // this is to test UTF8 behaviour
                statement.setString(8, "группа туристов");
                statement.setDate(9, new Date(100L));
                statement.setTimestamp(10, new Timestamp(20000000033L));

                // nulls
                statement.setNull(11, Types.INTEGER);
                statement.setNull(12, Types.BIGINT);
                statement.setNull(13, Types.REAL);
                statement.setNull(14, Types.DOUBLE);
                statement.setNull(15, Types.SMALLINT);
                statement.setNull(16, Types.BOOLEAN);
                statement.setNull(17, Types.VARCHAR);
                statement.setString(18, null);
                statement.setNull(19, Types.DATE);
//                statement.setNull(20, Types.TIMESTAMP);

                // when someone uses PostgreSQL's type extensions, which alter driver behaviour
                // we should handle this gracefully

                statement.setTimestamp(20, new PGTimestamp(300011));
                statement.setTimestamp(21, new PGTimestamp(500023, new GregorianCalendar()));

                // Bind variables are out of context here, hence they are all STRING/VARCHAR
                // this is the reason why we show PG wire Dates verbatim. Even though PG wire does eventually tell us
                // that this data is typed (sometimes), their requirement to describe SQL statement before
                // they send us bind variable types and values forces us to stick with STRING.
                final String expected = "x[BIGINT],$1[VARCHAR],$2[VARCHAR],$3[VARCHAR],$4[VARCHAR],$5[VARCHAR],$6[VARCHAR],$7[VARCHAR],$8[VARCHAR],$9[VARCHAR],$10[VARCHAR],$11[VARCHAR],$12[VARCHAR],$13[VARCHAR],$14[VARCHAR],$15[VARCHAR],$16[VARCHAR],$17[VARCHAR],$18[VARCHAR],$19[VARCHAR],$20[VARCHAR],$21[VARCHAR]\n" +
                        "1,4,123,5.43,0.56789,91,TRUE,hello,группа туристов,1970-01-01 +00,1970-08-20 11:33:20.033+00,null,null,null,null,null,null,null,null,null,1970-01-01 00:05:00.011+00,1970-01-01 00:08:20.023+00\n" +
                        "2,4,123,5.43,0.56789,91,TRUE,hello,группа туристов,1970-01-01 +00,1970-08-20 11:33:20.033+00,null,null,null,null,null,null,null,null,null,1970-01-01 00:05:00.011+00,1970-01-01 00:08:20.023+00\n" +
                        "3,4,123,5.43,0.56789,91,TRUE,hello,группа туристов,1970-01-01 +00,1970-08-20 11:33:20.033+00,null,null,null,null,null,null,null,null,null,1970-01-01 00:05:00.011+00,1970-01-01 00:08:20.023+00\n" +
                        "4,4,123,5.43,0.56789,91,TRUE,hello,группа туристов,1970-01-01 +00,1970-08-20 11:33:20.033+00,null,null,null,null,null,null,null,null,null,1970-01-01 00:05:00.011+00,1970-01-01 00:08:20.023+00\n" +
                        "5,4,123,5.43,0.56789,91,TRUE,hello,группа туристов,1970-01-01 +00,1970-08-20 11:33:20.033+00,null,null,null,null,null,null,null,null,null,1970-01-01 00:05:00.011+00,1970-01-01 00:08:20.023+00\n";

                StringSink sink = new StringSink();
                for (int i = 0; i < 10; i++) {
                    sink.clear();
                    ResultSet rs = statement.executeQuery();
                    assertResultSet(expected, sink, rs);
                    rs.close();
                }
                connection.close();
            } finally {
                running.set(false);
                haltLatch.await();
            }
        });
    }

    @Test
    public void testSchemasCall() throws Exception {
        assertMemoryLeak(() -> {

            final CountDownLatch haltLatch = new CountDownLatch(1);
            final AtomicBoolean running = new AtomicBoolean(true);

            try {
                startBasicServer(NetworkFacadeImpl.INSTANCE,
                        new DefaultPGWireConfiguration(),
                        haltLatch,
                        running
                );

                sink.clear();

                Properties properties = new Properties();
                properties.setProperty("user", "admin");
                properties.setProperty("password", "quest");
                properties.setProperty("sslmode", "disable");
                final Connection connection = DriverManager.getConnection("jdbc:postgresql://127.0.0.1:9120/qdb", properties);
                try (Statement statement = connection.createStatement()) {
                    statement.executeUpdate("create table test (id long,val int)");
                    statement.executeUpdate("create table test2(id long,val int)");
                }

                final DatabaseMetaData metaData = connection.getMetaData();
                try (ResultSet rs = metaData.getCatalogs()) {
                    assertResultSet(
                            "TABLE_CAT[VARCHAR]\n" +
                                    "qdb\n",
                            sink,
                            rs);
                }

                sink.clear();

                try (ResultSet rs = metaData.getSchemas()) {
                    assertResultSet(
                            "TABLE_SCHEM[VARCHAR],TABLE_CATALOG[VARCHAR]\n" +
                                    "pg_catalog,null\n" +
                                    "public,null\n",
                            sink,
                            rs
                    );
                }

                sink.clear();

                try (ResultSet rs = metaData.getTables(
                        "qdb", null, null, null
                )) {
                    assertResultSet(
                            "TABLE_CAT[VARCHAR],TABLE_SCHEM[VARCHAR],TABLE_NAME[VARCHAR],TABLE_TYPE[VARCHAR],REMARKS[VARCHAR],TYPE_CAT[CHAR],TYPE_SCHEM[CHAR],TYPE_NAME[CHAR],SELF_REFERENCING_COL_NAME[CHAR],REF_GENERATION[CHAR]\n" +
                                    "null,public,test,TABLE,table,\0,\0,\0,\0,\0\n" +
                                    "null,public,test2,TABLE,table,\0,\0,\0,\0,\0\n",
                            sink,
                            rs
                    );
                }


                sink.clear();
                try (ResultSet rs = metaData.getColumns("qdb", null, "test", null)) {
                    assertResultSet(
                            "TABLE_CAT[VARCHAR],TABLE_SCHEM[VARCHAR],TABLE_NAME[VARCHAR],COLUMN_NAME[VARCHAR],DATA_TYPE[SMALLINT],TYPE_NAME[VARCHAR],COLUMN_SIZE[INTEGER],BUFFER_LENGTH[VARCHAR],DECIMAL_DIGITS[INTEGER],NUM_PREC_RADIX[INTEGER],NULLABLE[INTEGER],REMARKS[VARCHAR],COLUMN_DEF[VARCHAR],SQL_DATA_TYPE[INTEGER],SQL_DATETIME_SUB[INTEGER],CHAR_OCTET_LENGTH[VARCHAR],ORDINAL_POSITION[INTEGER],IS_NULLABLE[VARCHAR],SCOPE_CATALOG[VARCHAR],SCOPE_SCHEMA[VARCHAR],SCOPE_TABLE[VARCHAR],SOURCE_DATA_TYPE[SMALLINT],IS_AUTOINCREMENT[VARCHAR],IS_GENERATEDCOLUMN[VARCHAR]\n" +
                                    "null,public,test,id,-5,int8,19,null,0,10,1,column,,null,null,19,0,YES,null,null,null,0,YES,\n" +
                                    "null,public,test,val,4,int4,10,null,0,10,1,column,,null,null,10,1,YES,null,null,null,0,YES,\n",
                            sink,
                            rs
                    );
                }

                // todo:  does not work
                //    trim() function syntax is not supported (https://w3resource.com/PostgreSQL/trim-function.php)
                /*
                sink.clear();
                try (ResultSet rs = metaData.getIndexInfo("qdb", "public", "test", true, false)) {
                    assertResultSet(
                            "",
                            sink,
                            rs
                    );
                }
                */
                connection.close();
            } finally {
                running.set(false);
                haltLatch.await();
            }
        });
    }

    @Test
    public void testSendingBufferWhenFlushMessageReceivedeHex() throws Exception {
        String script = ">0000006e00030000757365720078797a0064617461626173650071646200636c69656e745f656e636f64696e67005554463800446174655374796c650049534f0054696d655a6f6e65004575726f70652f4c6f6e646f6e0065787472615f666c6f61745f64696769747300320000\n" +
                "<520000000800000003\n" +
                ">70000000076f6800\n" +
                "<520000000800000000530000001154696d655a6f6e6500474d5400530000001d6170706c69636174696f6e5f6e616d6500517565737444420053000000187365727665725f76657273696f6e0031312e33005300000019696e74656765725f6461746574696d6573006f6e005300000019636c69656e745f656e636f64696e670055544638005a0000000549\n" +
                ">500000002c0073656c65637420782c202024312066726f6d206c6f6e675f73657175656e63652832293b000000\n" +
                ">420000001100000000000100000001330000\n" +
                ">44000000065000\n" +
                ">45000000090000000000\n" +
                ">4800000004\n" +
                "<31000000043200000004540000002f00027800000040010001000000140004ffffffff0000243100000040010002000000170004ffffffff000044000000100002000000013100000001334400000010000200000001320000000133430000000d53454c454354203200\n" +
                ">4800000004\n" +
                ">5300000004\n" +
                "<5a0000000549\n" +
                ">5800000004";

        assertHexScript(NetworkFacadeImpl.INSTANCE,
                NetworkFacadeImpl.INSTANCE,
                script,
                getHexPgWireConfig());
    }

    @Test
    public void testSimple() throws Exception {
        testQuery("rnd_double(4) d, ", "s[VARCHAR],i[INTEGER],d[DOUBLE],t[TIMESTAMP],f[REAL],_short[SMALLINT],l[BIGINT],ts2[TIMESTAMP],bb[SMALLINT],b[BIT],rnd_symbol[VARCHAR],rnd_date[TIMESTAMP],rnd_bin[BINARY],rnd_char[CHAR],rnd_long256[NUMERIC]\n");
    }

    @Test
    public void testSimpleHex() throws Exception {
        // this is a HEX encoded bytes of the same script as 'testSimple' sends using postgres jdbc driver
        String script = ">0000000804d2162f\n" +
                "<4e\n" +
                ">0000007000030000757365720061646d696e0064617461626173650071646200636c69656e745f656e636f64696e67005554463800446174655374796c650049534f0054696d655a6f6e65004575726f70652f4c6f6e646f6e0065787472615f666c6f61745f64696769747300320000\n" +
                "<520000000800000003\n" +
                ">700000000a717565737400\n" +
                "<520000000800000000530000001154696d655a6f6e6500474d5400530000001d6170706c69636174696f6e5f6e616d6500517565737444420053000000187365727665725f76657273696f6e0031312e33005300000019696e74656765725f6461746574696d6573006f6e005300000019636c69656e745f656e636f64696e670055544638005a0000000549\n" +
                ">5000000022005345542065787472615f666c6f61745f646967697473203d2033000000420000000c0000000000000000450000000900000000015300000004\n" +
                "<310000000432000000044300000008534554005a0000000549\n" +
                ">500000003700534554206170706c69636174696f6e5f6e616d65203d2027506f737467726553514c204a4442432044726976657227000000420000000c0000000000000000450000000900000000015300000004\n" +
                "<310000000432000000044300000008534554005a0000000549\n" +
                ">50000001940073656c65637420726e645f73747228342c342c342920732c20726e645f696e7428302c203235362c20342920692c20726e645f646f75626c6528342920642c2074696d657374616d705f73657175656e636528302c31303030302920742c20726e645f666c6f617428342920662c20726e645f73686f72742829205f73686f72742c20726e645f6c6f6e6728302c2031303030303030302c203529206c2c20726e645f74696d657374616d7028746f5f74696d657374616d70282732303135272c277979797927292c746f5f74696d657374616d70282732303136272c277979797927292c3229207473322c20726e645f6279746528302c313237292062622c20726e645f626f6f6c65616e282920622c20726e645f73796d626f6c28342c342c342c32292c20726e645f6461746528746f5f64617465282732303135272c20277979797927292c20746f5f64617465282732303136272c20277979797927292c2032292c726e645f62696e2831302c32302c32292066726f6d206c6f6e675f73657175656e636528353029000000420000000c000000000000000044000000065000450000000900000000005300000004\n" +
                "<310000000432000000045400000128000d730000004001000100000413ffff0000000000006900000040010002000000170004ffffffff00006400000040010003000002bd0004ffffffff000074000000400100040000045a0004ffffffff00006600000040010005000002bc0004ffffffff00005f73686f727400000040010006000000150004ffffffff00006c00000040010007000000140004ffffffff0000747332000000400100080000045a0004ffffffff0000626200000040010009000000150004ffffffff0000620000004001000a000000100004ffffffff0000726e645f73796d626f6c0000004001000b00000413ffff000000000000726e645f646174650000004001000c0000045a0004ffffffff0000726e645f62696e0000004001000d00000011ffff00000000000144000000a6000dffffffff00000002353700000012302e363235343032313534323431323031380000001a313937302d30312d30312030303a30303a30302e30303030303000000005302e343632000000052d313539330000000733343235323332ffffffff000000033132310000000166000000045045484e00000017323031352d30332d31372030343a32353a35322e3736350000000e19c49594365349b4597e3b08a11e44000000c8000d00000004585953420000000331343200000012302e353739333436363332363836323231310000001a313937302d30312d30312030303a30303a30302e30313030303000000005302e39363900000005323030383800000007313531373439300000001a323031352d30312d31372032303a34313a31392e343830363835000000033130300000000174000000045045484e00000017323031352d30362d32302030313a31303a35382e35393900000011795f8b812b934d1a8e78b5b91153d0fb6444000000c2000d000000044f5a5a560000000332313900000013302e31363338313337343737333734383531340000001a313937302d30312d30312030303a30303a30302e30323030303000000005302e363539000000062d313233303300000007393438393530380000001a323031352d30382d31332031373a31303a31392e37353235323100000001360000000166ffffffff00000017323031352d30352d32302030313a34383a33372e3431380000000f2b4d5ff64690c3b3598ee5612f640e44000000b1000d000000044f4c595800000002333000000012302e373133333931303237313535353834330000001a313937302d30312d30312030303a30303a30302e30333030303000000005302e363535000000043636313000000007363530343432380000001a323031352d30382d30382030303a34323a32342e353435363339000000033132330000000166ffffffff00000017323031352d30312d30332031333a35333a30332e313635ffffffff44000000ac000d000000045449514200000002343200000012302e363830363837333133343632363431380000001a313937302d30312d30312030303a30303a30302e30343030303000000005302e363236000000052d3136303500000007383831343038360000001a323031352d30372d32382031353a30383a35332e34363234393500000002323800000001740000000443505357ffffffff0000000e3ba6dc3b7d2be392fe6938e1779a44000000af000d000000044c544f560000000331333700000012302e373633323631353030343332343530330000001a313937302d30312d30312030303a30303a30302e30353030303000000005302e3838320000000439303534ffffffff0000001a323031352d30342d32302030353a30393a30332e353830353734000000033130360000000166000000045045484e00000017323031352d30312d30392030363a35373a31372e353132ffffffff44000000a0000d000000045a494d4e00000003313235ffffffff0000001a313937302d30312d30312030303a30303a30302e303630303030ffffffff00000005313135323400000007383333353236310000001a323031352d31302d32362030323a31303a35302e363838333934000000033131310000000174000000045045484e00000017323031352d30382d32312031353a34363a33322e363234ffffffff44000000a1000d000000044f504a4f0000000331363800000013302e31303435393335323331323333313138330000001a313937302d30312d30312030303a30303a30302e30373030303000000005302e353335000000052d3539323000000007373038303730340000001a323031352d30372d31312030393a31353a33382e3334323731370000000331303300000001660000000456544a57ffffffffffffffff44000000b6000d00000004474c554f0000000331343500000012302e353339313632363632313739343637330000001a313937302d30312d30312030303a30303a30302e30383030303000000005302e37363700000005313432343200000007323439393932320000001a323031352d31312d30322030393a30313a33312e3331323830340000000238340000000166000000045045484e00000017323031352d31312d31342031373a33373a33362e303433ffffffff44000000c3000d000000045a5651450000000331303300000012302e363732393430353539303737333633380000001a313937302d30312d30312030303a30303a30302e303930303030ffffffff00000005313337323700000007373837353834360000001a323031352d31322d31322031333a31363a32362e3133343536320000000232320000000174000000045045484e00000017323031352d30312d32302030343a35303a33342e30393800000012143380c9eba3677a1a79e435e43adc5c65ff44000000a7000d000000044c4947590000000331393900000012302e323833363334373133393438313436390000001a313937302d30312d30312030303a30303a30302e313030303030ffffffff00000005333034323600000007333231353536320000001a323031352d30382d32312031343a35353a30372e30353537323200000002313100000001660000000456544a57ffffffff0000000dff703ac78ab314cd470b0c391244000000a7000d000000044d514e5400000002343300000012302e353835393333323338383539393633380000001a313937302d30312d30312030303a30303a30302e31313030303000000005302e333335000000053237303139ffffffffffffffff0000000232370000000174000000045045484e00000017323031352d30372d31322031323a35393a34372e3636350000001326fb2e42faf56e8f80e354b807b13257ff9aef44000000c8000d00000004575743430000000332313300000012302e373636353032393931343337363935320000001a313937302d30312d30312030303a30303a30302e31323030303000000005302e35383000000005313336343000000007343132313932330000001a323031352d30382d30362030323a32373a33302e3436393736320000000237330000000166000000045045484e00000017323031352d30342d33302030383a31383a31302e3435330000001271a7d5af11963708dd98ef54882aa2ade7d444000000af000d00000004564647500000000331323000000012302e383430323936343730383132393534360000001a313937302d30312d30312030303a30303a30302e31333030303000000005302e373733000000043732323300000007373234313432330000001a323031352d31322d31382030373a33323a31382e34353630323500000002343300000001660000000456544a57ffffffff00000011244e44a80dfe27ec53135db215e7b8356744000000b7000d00000004524d44470000000331333400000013302e31313034373331353231343739333639360000001a313937302d30312d30312030303a30303a30302e31343030303000000005302e30343300000005323132323700000007373135353730380000001a323031352d30372d30332030343a31323a34352e3737343238310000000234320000000174000000044350535700000017323031352d30322d32342031323a31303a34332e313939ffffffff44000000a5000d0000000457464f5100000003323535ffffffff0000001a313937302d30312d30312030303a30303a30302e31353030303000000005302e31313600000005333135363900000007363638383237370000001a323031352d30352d31392030333a33303a34352e373739393939000000033132360000000174000000045045484e00000017323031352d31322d30392030393a35373a31372e303738ffffffff4400000098000d000000044d58444b00000002353600000012302e393939373739373233343033313638380000001a313937302d30312d30312030303a30303a30302e31363030303000000005302e353233000000062d33323337320000000736383834313332ffffffff0000000235380000000166ffffffff00000017323031352d30312d32302030363a31383a31382e353833ffffffff44000000bb000d00000004584d4b4a0000000331333900000012302e383430353831353439333536373431370000001a313937302d30312d30312030303a30303a30302e31373030303000000005302e333036000000053235383536ffffffff0000001a323031352d30352d31382030333a35303a32322e373331343337000000013200000001740000000456544a5700000017323031352d30362d32352031303a34353a30312e3031340000000d007cfb0119caf2bf845a6f383544000000af000d0000000456494844ffffffffffffffff0000001a313937302d30312d30312030303a30303a30302e31383030303000000005302e35353000000005323232383000000007393130393834320000001a323031352d30312d32352031333a35313a33382e3237303538330000000239340000000166000000044350535700000017323031352d31302d32372030323a35323a31392e3933350000000e2d16f389a38364ded6fdc45bc4e944000000bd000d0000000457504e58ffffffff00000012302e393436393730303831333932363930370000001a313937302d30312d30312030303a30303a30302e31393030303000000005302e343135000000062d3137393333000000063637343236310000001a323031352d30332d30342031353a34333a31352e3231333638360000000234330000000174000000044859525800000017323031352d31322d31382032313a32383a32352e3332350000000ab34c0e8ff10cc560b7d144000000bd000d0000000459504f5600000002333600000012302e363734313234383434383732383832340000001a313937302d30312d30312030303a30303a30302e32303030303000000005302e303331000000052d3538383800000007313337353432330000001a323031352d31322d31302032303a35303a33352e38363636313400000001330000000174ffffffff00000017323031352d30372d32332032303a31373a30342e3233360000000dd4abbe30fa8dac3d98a0ad9a5d44000000c6000d000000044e55484effffffff00000012302e363934303931373932353134383333320000001a313937302d30312d30312030303a30303a30302e32313030303000000005302e333339000000062d323532323600000007333532343734380000001a323031352d30352d30372030343a30373a31382e31353239363800000002333900000001740000000456544a5700000017323031352d30342d30342031353a32333a33342e31333000000012b8bef8a146872892a39be3cbc2648ab035d8440000009c000d00000004424f53450000000332343000000013302e30363030313832373732313535363031390000001a313937302d30312d30312030303a30303a30302e32323030303000000005302e33373900000005323339303400000007393036393333390000001a323031352d30332d32312030333a34323a34322e3634333138360000000238340000000174ffffffffffffffffffffffff44000000c5000d00000004494e4b470000000331323400000012302e383631353834313632373730323735330000001a313937302d30312d30312030303a30303a30302e32333030303000000005302e343034000000062d333033383300000007373233333534320000001a323031352d30372d32312031363a34323a34372e3031323134380000000239390000000166ffffffff00000017323031352d30382d32372031373a32353a33352e3330380000001287fc9283fc88f3322770c801b0dcc93a5b7e44000000b1000d000000044655584300000002353200000012302e373433303130313939343531313531370000001a313937302d30312d30312030303a30303a30302e323430303030ffffffff000000062d313437323900000007313034323036340000001a323031352d30382d32312030323a31303a35382e3934393637340000000232380000000174000000044350535700000017323031352d30382d32392032303a31353a35312e383335ffffffff44000000bd000d00000004554e595100000002373100000011302e3434323039353431303238313933380000001a313937302d30312d30312030303a30303a30302e32353030303000000005302e353339000000062d3232363131ffffffff0000001a323031352d31322d32332031383a34313a34322e3331393835390000000239380000000174000000045045484e00000017323031352d30312d32362030303a35353a35302e3230320000000f28ed9799d877333fb267da984747bf44000000b1000d000000044b424d51ffffffff00000013302e32383031393231383832353035313339350000001a313937302d30312d30312030303a30303a30302e323630303030ffffffff000000053132323430ffffffff0000001a323031352d30382d31362030313a30323a35352e3736363632320000000232310000000166ffffffff00000017323031352d30352d31392030303a34373a31382e3639380000000d6ade4604d381e7a21622353b1c4400000091000d000000044a534f4c00000003323433ffffffff0000001a313937302d30312d30312030303a30303a30302e32373030303000000005302e303638000000062d3137343638ffffffffffffffff0000000232300000000174ffffffff00000017323031352d30362d31392031303a33383a35342e343833000000113de02d0486e7ca29980769ca5bd6cf0969440000007f000d00000004484e535300000003313530ffffffff0000001a313937302d30312d30312030303a30303a30302e32383030303000000005302e3134380000000531343834310000000735393932343433ffffffff0000000232350000000166000000045045484effffffff0000000c14d6fcee032281b806c406af44000000c3000d00000004505a50420000000331303100000014302e3036313634363731373738363135383034350000001a313937302d30312d30312030303a30303a30302e323930303030ffffffff00000005313232333700000007393837383137390000001a323031352d30392d30332032323a31333a31382e38353234363500000002373900000001660000000456544a5700000017323031352d31322d31372031353a31323a35342e3935380000001012613a9aad982e7552ad62878845b99d44000000c3000d000000044f594e4e00000002323500000012302e333339333530393531343030303234370000001a313937302d30312d30312030303a30303a30302e33303030303000000005302e36323800000005323234313200000007343733363337380000001a323031352d31302d31302031323a31393a34322e353238323234000000033130360000000174000000044350535700000017323031352d30372d30312030303a32333a34392e3738390000000d54133fffb67ecd0427669489db4400000083000dffffffff0000000331313700000012302e353633383430343737353636333136310000001a313937302d30312d30312030303a30303a30302e333130303030ffffffff000000052d353630340000000736333533303138ffffffff0000000238340000000166ffffffffffffffff0000000b2bad2507db6244336e008e4400000099000d00000004485652490000000332333300000013302e32323430373636353739303730353737370000001a313937302d30312d30312030303a30303a30302e33323030303000000005302e3432350000000531303436390000000731373135323133ffffffff0000000238360000000166ffffffff00000017323031352d30322d30322030353a34383a31372e333733ffffffff44000000b6000d000000044f59544f00000002393600000012302e373430373538313631363931363336340000001a313937302d30312d30312030303a30303a30302e33333030303000000005302e353238000000062d313232333900000007333439393632300000001a323031352d30322d30372032323a33353a30332e3231323236380000000231370000000166000000045045484e00000017323031352d30332d32392031323a35353a31312e363832ffffffff44000000a5000d000000044c46435900000002363300000012302e373231373331353732393739303732320000001a313937302d30312d30312030303a30303a30302e333430303030ffffffff0000000532333334340000000739353233393832ffffffff000000033132330000000166000000044350535700000017323031352d30352d31382030343a33353a32372e3232380000000e05e5c04eccd6e37b34cd1535bba444000000c1000d0000000447484c580000000331343800000012302e333035373933373730343936343237320000001a313937302d30312d30312030303a30303a30302e33353030303000000005302e363336000000062d333134353700000007323332323333370000001a323031352d31302d32322031323a30363a30352e3534343730310000000239310000000174000000044859525800000017323031352d30352d32312030393a33333a31382e3135380000000a571d91723004b702cb0344000000a4000d000000045954535a00000003313233ffffffff0000001a313937302d30312d30312030303a30303a30302e33363030303000000005302e35313900000005323235333400000007343434363233360000001a323031352d30372d32372030373a32333a33372e3233333731310000000235330000000166000000044350535700000017323031352d30312d31332030343a33373a31302e303336ffffffff44000000a3000d0000000453574c5500000003323531ffffffff0000001a313937302d30312d30312030303a30303a30302e33373030303000000005302e313739000000043737333400000007343038323437350000001a323031352d31302d32312031383a32343a33342e3430303334350000000236390000000166000000045045484e00000017323031352d30342d30312031343a33333a34322e303035ffffffff44000000b1000d0000000454514a4c00000003323435ffffffff0000001a313937302d30312d30312030303a30303a30302e33383030303000000005302e3836350000000439353136000000063932393334300000001a323031352d30352d32382030343a31383a31382e36343035363700000002363900000001660000000456544a5700000017323031352d30362d31322032303a31323a32382e3838310000000f6c3e51d7ebb10771321faf404e8c47440000009e000d000000045245494a000000023934ffffffff0000001a313937302d30312d30312030303a30303a30302e33393030303000000005302e313330000000062d3239393234ffffffff0000001a323031352d30332d32302032323a31343a34362e323034373138000000033131330000000174000000044859525800000017323031352d31322d31392031333a35383a34312e383139ffffffff44000000c2000d000000044844485100000002393400000012302e373233343138313737333430373533360000001a313937302d30312d30312030303a30303a30302e34303030303000000005302e373330000000053139393730000000063635343133310000001a323031352d30312d31302032323a35363a30382e3438303435300000000238340000000174ffffffff00000017323031352d30332d30352031373a31343a34382e323735000000124f566b65a45338e9cdc1a7ee8675ada52d4944000000b8000d00000004554d455500000002343000000014302e3030383434343033333233303538303733390000001a313937302d30312d30312030303a30303a30302e34313030303000000005302e383035000000062d313136323300000007343539393836320000001a323031352d31312d32302030343a30323a34342e3333353934370000000237360000000166000000045045484e00000017323031352d30352d31372031373a33333a32302e393232ffffffff44000000ad000d00000004594a494800000003313834ffffffff0000001a313937302d30312d30312030303a30303a30302e34323030303000000005302e33383300000005313736313400000007333130313637310000001a323031352d30312d32382031323a30353a34362e363833303031000000033130350000000174ffffffff00000017323031352d31322d30372031393a32343a33362e3833380000000cec69cd73bb9bc595db6191ce44000000a3000d000000044359584700000002323700000012302e323931373739363035333034353734370000001a313937302d30312d30312030303a30303a30302e34333030303000000005302e393533000000043339343400000006323439313635ffffffff0000000236370000000174ffffffff00000017323031352d30332d30322030383a31393a34342e3536360000000e0148153e0c7f3f8fe4b5ab34212944000000b4000d000000044d5254470000000331343300000013302e30323633323533313336313439393131330000001a313937302d30312d30312030303a30303a30302e34343030303000000005302e393433000000062d323733323000000007313636373834320000001a323031352d30312d32342031393a35363a31352e3937333130390000000231310000000166ffffffff00000017323031352d30312d32342030373a31353a30322e373732ffffffff44000000c3000d00000004444f4e500000000332343600000011302e3635343232363234383734303434370000001a313937302d30312d30312030303a30303a30302e34353030303000000005302e35353600000005323734373700000007343136303031380000001a323031352d31322d31342030333a34303a30352e3931313833390000000232300000000174000000045045484e00000017323031352d31302d32392031343a33353a31302e3136370000000e079201f56aa131cdcbc2a2b48e9944000000c4000d00000004495158530000000332333200000013302e32333037353730303231383033383835330000001a313937302d30312d30312030303a30303a30302e34363030303000000005302e303439000000062d313831313300000007343030353232380000001a323031352d30362d31312031333a30303a30372e32343831383800000001380000000174000000044350535700000017323031352d30382d31362031313a30393a32342e3331310000000dfa1f9224b1b8676508b7f8410044000000b1000dffffffff00000003313738ffffffff0000001a313937302d30312d30312030303a30303a30302e34373030303000000005302e393033000000062d313436323600000007323933343537300000001a323031352d30342d30342030383a35313a35342e3036383135340000000238380000000174ffffffff00000017323031352d30372d30312030343a33323a32332e30383300000014843625632b6361431c477db646babb98ca08bea444000000b0000d000000044855575a00000002393400000011302e3131303430313337343937393631330000001a313937302d30312d30312030303a30303a30302e34383030303000000005302e343230000000052d3337333600000007353638373531340000001a323031352d30312d30322031373a31383a30352e3632373633330000000237340000000166ffffffff00000017323031352d30332d32392030363a33393a31312e363432ffffffff44000000ab000d000000045352454400000002363600000013302e31313237343636373134303931353932380000001a313937302d30312d30312030303a30303a30302e34393030303000000005302e303630000000062d313035343300000007333636393337370000001a323031352d31302d32322030323a35333a30322e3338313335310000000237370000000174000000045045484effffffff0000000b7c3fd6883a93ef24a5e2bc430000000e53454c454354203530005a0000000549";
        assertHexScript(script);
    }

    @Test
    public void testSimpleSimpleQuery() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            final CountDownLatch haltLatch = new CountDownLatch(1);
            final AtomicBoolean running = new AtomicBoolean(true);
            try {
                startBasicServer(NetworkFacadeImpl.INSTANCE,
                        new DefaultPGWireConfiguration(),
                        haltLatch,
                        running
                );

                Properties properties = new Properties();
                properties.setProperty("user", "admin");
                properties.setProperty("password", "quest");
                properties.setProperty("preferQueryMode", "simple");

                final Connection connection = DriverManager.getConnection("jdbc:postgresql://127.0.0.1:9120/qdb", properties);
                Statement statement = connection.createStatement();
                ResultSet rs = statement.executeQuery(
                        "select " +
                                "rnd_str(4,4,4) s, " +
                                "rnd_int(0, 256, 4) i, " +
                                "rnd_double(4) d, " +
                                "timestamp_sequence(0,10000) t, " +
                                "rnd_float(4) f, " +
                                "rnd_short() _short, " +
                                "rnd_long(0, 10000000, 5) l, " +
                                "rnd_timestamp(to_timestamp('2015','yyyy'),to_timestamp('2016','yyyy'),2) ts2, " +
                                "rnd_byte(0,127) bb, " +
                                "rnd_boolean() b, " +
                                "rnd_symbol(4,4,4,2), " +
                                "rnd_date(to_date('2015', 'yyyy'), to_date('2016', 'yyyy'), 2)," +
                                "rnd_bin(10,20,2) " +
                                "from long_sequence(50)");

                final String expected = "s[VARCHAR],i[INTEGER],d[DOUBLE],t[TIMESTAMP],f[REAL],_short[SMALLINT],l[BIGINT],ts2[TIMESTAMP],bb[SMALLINT],b[BIT],rnd_symbol[VARCHAR],rnd_date[TIMESTAMP],rnd_bin[BINARY]\n" +
                        "null,57,0.6254021542412018,1970-01-01 00:00:00.0,0.4620000123977661,-1593,3425232,null,121,false,PEHN,2015-03-17 04:25:52.765,00000000 19 c4 95 94 36 53 49 b4 59 7e 3b 08 a1 1e\n" +
                        "XYSB,142,0.5793466326862211,1970-01-01 00:00:00.01,0.968999981880188,20088,1517490,2015-01-17 20:41:19.480685,100,true,PEHN,2015-06-20 01:10:58.599,00000000 79 5f 8b 81 2b 93 4d 1a 8e 78 b5 b9 11 53 d0 fb\n" +
                        "00000010 64\n" +
                        "OZZV,219,0.16381374773748514,1970-01-01 00:00:00.02,0.6589999794960022,-12303,9489508,2015-08-13 17:10:19.752521,6,false,null,2015-05-20 01:48:37.418,00000000 2b 4d 5f f6 46 90 c3 b3 59 8e e5 61 2f 64 0e\n" +
                        "OLYX,30,0.7133910271555843,1970-01-01 00:00:00.03,0.6549999713897705,6610,6504428,2015-08-08 00:42:24.545639,123,false,null,2015-01-03 13:53:03.165,null\n" +
                        "TIQB,42,0.6806873134626418,1970-01-01 00:00:00.04,0.6259999871253967,-1605,8814086,2015-07-28 15:08:53.462495,28,true,CPSW,null,00000000 3b a6 dc 3b 7d 2b e3 92 fe 69 38 e1 77 9a\n" +
                        "LTOV,137,0.7632615004324503,1970-01-01 00:00:00.05,0.8820000290870667,9054,null,2015-04-20 05:09:03.580574,106,false,PEHN,2015-01-09 06:57:17.512,null\n" +
                        "ZIMN,125,null,1970-01-01 00:00:00.06,null,11524,8335261,2015-10-26 02:10:50.688394,111,true,PEHN,2015-08-21 15:46:32.624,null\n" +
                        "OPJO,168,0.10459352312331183,1970-01-01 00:00:00.07,0.5350000262260437,-5920,7080704,2015-07-11 09:15:38.342717,103,false,VTJW,null,null\n" +
                        "GLUO,145,0.5391626621794673,1970-01-01 00:00:00.08,0.7670000195503235,14242,2499922,2015-11-02 09:01:31.312804,84,false,PEHN,2015-11-14 17:37:36.043,null\n" +
                        "ZVQE,103,0.6729405590773638,1970-01-01 00:00:00.09,null,13727,7875846,2015-12-12 13:16:26.134562,22,true,PEHN,2015-01-20 04:50:34.098,00000000 14 33 80 c9 eb a3 67 7a 1a 79 e4 35 e4 3a dc 5c\n" +
                        "00000010 65 ff\n" +
                        "LIGY,199,0.2836347139481469,1970-01-01 00:00:00.1,null,30426,3215562,2015-08-21 14:55:07.055722,11,false,VTJW,null,00000000 ff 70 3a c7 8a b3 14 cd 47 0b 0c 39 12\n" +
                        "MQNT,43,0.5859332388599638,1970-01-01 00:00:00.11,0.33500000834465027,27019,null,null,27,true,PEHN,2015-07-12 12:59:47.665,00000000 26 fb 2e 42 fa f5 6e 8f 80 e3 54 b8 07 b1 32 57\n" +
                        "00000010 ff 9a ef\n" +
                        "WWCC,213,0.7665029914376952,1970-01-01 00:00:00.12,0.5799999833106995,13640,4121923,2015-08-06 02:27:30.469762,73,false,PEHN,2015-04-30 08:18:10.453,00000000 71 a7 d5 af 11 96 37 08 dd 98 ef 54 88 2a a2 ad\n" +
                        "00000010 e7 d4\n" +
                        "VFGP,120,0.8402964708129546,1970-01-01 00:00:00.13,0.7730000019073486,7223,7241423,2015-12-18 07:32:18.456025,43,false,VTJW,null,00000000 24 4e 44 a8 0d fe 27 ec 53 13 5d b2 15 e7 b8 35\n" +
                        "00000010 67\n" +
                        "RMDG,134,0.11047315214793696,1970-01-01 00:00:00.14,0.0430000014603138,21227,7155708,2015-07-03 04:12:45.774281,42,true,CPSW,2015-02-24 12:10:43.199,null\n" +
                        "WFOQ,255,null,1970-01-01 00:00:00.15,0.11599999666213989,31569,6688277,2015-05-19 03:30:45.779999,126,true,PEHN,2015-12-09 09:57:17.078,null\n" +
                        "MXDK,56,0.9997797234031688,1970-01-01 00:00:00.16,0.5230000019073486,-32372,6884132,null,58,false,null,2015-01-20 06:18:18.583,null\n" +
                        "XMKJ,139,0.8405815493567417,1970-01-01 00:00:00.17,0.3059999942779541,25856,null,2015-05-18 03:50:22.731437,2,true,VTJW,2015-06-25 10:45:01.014,00000000 00 7c fb 01 19 ca f2 bf 84 5a 6f 38 35\n" +
                        "VIHD,null,null,1970-01-01 00:00:00.18,0.550000011920929,22280,9109842,2015-01-25 13:51:38.270583,94,false,CPSW,2015-10-27 02:52:19.935,00000000 2d 16 f3 89 a3 83 64 de d6 fd c4 5b c4 e9\n" +
                        "WPNX,null,0.9469700813926907,1970-01-01 00:00:00.19,0.41499999165534973,-17933,674261,2015-03-04 15:43:15.213686,43,true,HYRX,2015-12-18 21:28:25.325,00000000 b3 4c 0e 8f f1 0c c5 60 b7 d1\n" +
                        "YPOV,36,0.6741248448728824,1970-01-01 00:00:00.2,0.03099999949336052,-5888,1375423,2015-12-10 20:50:35.866614,3,true,null,2015-07-23 20:17:04.236,00000000 d4 ab be 30 fa 8d ac 3d 98 a0 ad 9a 5d\n" +
                        "NUHN,null,0.6940917925148332,1970-01-01 00:00:00.21,0.33899998664855957,-25226,3524748,2015-05-07 04:07:18.152968,39,true,VTJW,2015-04-04 15:23:34.13,00000000 b8 be f8 a1 46 87 28 92 a3 9b e3 cb c2 64 8a b0\n" +
                        "00000010 35 d8\n" +
                        "BOSE,240,0.06001827721556019,1970-01-01 00:00:00.22,0.3790000081062317,23904,9069339,2015-03-21 03:42:42.643186,84,true,null,null,null\n" +
                        "INKG,124,0.8615841627702753,1970-01-01 00:00:00.23,0.40400001406669617,-30383,7233542,2015-07-21 16:42:47.012148,99,false,null,2015-08-27 17:25:35.308,00000000 87 fc 92 83 fc 88 f3 32 27 70 c8 01 b0 dc c9 3a\n" +
                        "00000010 5b 7e\n" +
                        "FUXC,52,0.7430101994511517,1970-01-01 00:00:00.24,null,-14729,1042064,2015-08-21 02:10:58.949674,28,true,CPSW,2015-08-29 20:15:51.835,null\n" +
                        "UNYQ,71,0.442095410281938,1970-01-01 00:00:00.25,0.5389999747276306,-22611,null,2015-12-23 18:41:42.319859,98,true,PEHN,2015-01-26 00:55:50.202,00000000 28 ed 97 99 d8 77 33 3f b2 67 da 98 47 47 bf\n" +
                        "KBMQ,null,0.28019218825051395,1970-01-01 00:00:00.26,null,12240,null,2015-08-16 01:02:55.766622,21,false,null,2015-05-19 00:47:18.698,00000000 6a de 46 04 d3 81 e7 a2 16 22 35 3b 1c\n" +
                        "JSOL,243,null,1970-01-01 00:00:00.27,0.06800000369548798,-17468,null,null,20,true,null,2015-06-19 10:38:54.483,00000000 3d e0 2d 04 86 e7 ca 29 98 07 69 ca 5b d6 cf 09\n" +
                        "00000010 69\n" +
                        "HNSS,150,null,1970-01-01 00:00:00.28,0.14800000190734863,14841,5992443,null,25,false,PEHN,null,00000000 14 d6 fc ee 03 22 81 b8 06 c4 06 af\n" +
                        "PZPB,101,0.061646717786158045,1970-01-01 00:00:00.29,null,12237,9878179,2015-09-03 22:13:18.852465,79,false,VTJW,2015-12-17 15:12:54.958,00000000 12 61 3a 9a ad 98 2e 75 52 ad 62 87 88 45 b9 9d\n" +
                        "OYNN,25,0.3393509514000247,1970-01-01 00:00:00.3,0.628000020980835,22412,4736378,2015-10-10 12:19:42.528224,106,true,CPSW,2015-07-01 00:23:49.789,00000000 54 13 3f ff b6 7e cd 04 27 66 94 89 db\n" +
                        "null,117,0.5638404775663161,1970-01-01 00:00:00.31,null,-5604,6353018,null,84,false,null,null,00000000 2b ad 25 07 db 62 44 33 6e 00 8e\n" +
                        "HVRI,233,0.22407665790705777,1970-01-01 00:00:00.32,0.42500001192092896,10469,1715213,null,86,false,null,2015-02-02 05:48:17.373,null\n" +
                        "OYTO,96,0.7407581616916364,1970-01-01 00:00:00.33,0.527999997138977,-12239,3499620,2015-02-07 22:35:03.212268,17,false,PEHN,2015-03-29 12:55:11.682,null\n" +
                        "LFCY,63,0.7217315729790722,1970-01-01 00:00:00.34,null,23344,9523982,null,123,false,CPSW,2015-05-18 04:35:27.228,00000000 05 e5 c0 4e cc d6 e3 7b 34 cd 15 35 bb a4\n" +
                        "GHLX,148,0.3057937704964272,1970-01-01 00:00:00.35,0.6359999775886536,-31457,2322337,2015-10-22 12:06:05.544701,91,true,HYRX,2015-05-21 09:33:18.158,00000000 57 1d 91 72 30 04 b7 02 cb 03\n" +
                        "YTSZ,123,null,1970-01-01 00:00:00.36,0.5189999938011169,22534,4446236,2015-07-27 07:23:37.233711,53,false,CPSW,2015-01-13 04:37:10.036,null\n" +
                        "SWLU,251,null,1970-01-01 00:00:00.37,0.17900000512599945,7734,4082475,2015-10-21 18:24:34.400345,69,false,PEHN,2015-04-01 14:33:42.005,null\n" +
                        "TQJL,245,null,1970-01-01 00:00:00.38,0.8650000095367432,9516,929340,2015-05-28 04:18:18.640567,69,false,VTJW,2015-06-12 20:12:28.881,00000000 6c 3e 51 d7 eb b1 07 71 32 1f af 40 4e 8c 47\n" +
                        "REIJ,94,null,1970-01-01 00:00:00.39,0.12999999523162842,-29924,null,2015-03-20 22:14:46.204718,113,true,HYRX,2015-12-19 13:58:41.819,null\n" +
                        "HDHQ,94,0.7234181773407536,1970-01-01 00:00:00.4,0.7300000190734863,19970,654131,2015-01-10 22:56:08.48045,84,true,null,2015-03-05 17:14:48.275,00000000 4f 56 6b 65 a4 53 38 e9 cd c1 a7 ee 86 75 ad a5\n" +
                        "00000010 2d 49\n" +
                        "UMEU,40,0.008444033230580739,1970-01-01 00:00:00.41,0.8050000071525574,-11623,4599862,2015-11-20 04:02:44.335947,76,false,PEHN,2015-05-17 17:33:20.922,null\n" +
                        "YJIH,184,null,1970-01-01 00:00:00.42,0.382999986410141,17614,3101671,2015-01-28 12:05:46.683001,105,true,null,2015-12-07 19:24:36.838,00000000 ec 69 cd 73 bb 9b c5 95 db 61 91 ce\n" +
                        "CYXG,27,0.2917796053045747,1970-01-01 00:00:00.43,0.953000009059906,3944,249165,null,67,true,null,2015-03-02 08:19:44.566,00000000 01 48 15 3e 0c 7f 3f 8f e4 b5 ab 34 21 29\n" +
                        "MRTG,143,0.02632531361499113,1970-01-01 00:00:00.44,0.9430000185966492,-27320,1667842,2015-01-24 19:56:15.973109,11,false,null,2015-01-24 07:15:02.772,null\n" +
                        "DONP,246,0.654226248740447,1970-01-01 00:00:00.45,0.5559999942779541,27477,4160018,2015-12-14 03:40:05.911839,20,true,PEHN,2015-10-29 14:35:10.167,00000000 07 92 01 f5 6a a1 31 cd cb c2 a2 b4 8e 99\n" +
                        "IQXS,232,0.23075700218038853,1970-01-01 00:00:00.46,0.04899999871850014,-18113,4005228,2015-06-11 13:00:07.248188,8,true,CPSW,2015-08-16 11:09:24.311,00000000 fa 1f 92 24 b1 b8 67 65 08 b7 f8 41 00\n" +
                        "null,178,null,1970-01-01 00:00:00.47,0.902999997138977,-14626,2934570,2015-04-04 08:51:54.068154,88,true,null,2015-07-01 04:32:23.083,00000000 84 36 25 63 2b 63 61 43 1c 47 7d b6 46 ba bb 98\n" +
                        "00000010 ca 08 be a4\n" +
                        "HUWZ,94,0.110401374979613,1970-01-01 00:00:00.48,0.41999998688697815,-3736,5687514,2015-01-02 17:18:05.627633,74,false,null,2015-03-29 06:39:11.642,null\n" +
                        "SRED,66,0.11274667140915928,1970-01-01 00:00:00.49,0.05999999865889549,-10543,3669377,2015-10-22 02:53:02.381351,77,true,PEHN,null,00000000 7c 3f d6 88 3a 93 ef 24 a5 e2 bc\n";

                StringSink sink = new StringSink();

                // dump metadata
                assertResultSet(expected, sink, rs);
                connection.close();
            } finally {
                running.set(false);
                haltLatch.await();
            }
        });
    }

    @Test
    public void testSqlSyntaxError() throws SQLException, InterruptedException, BrokenBarrierException {

        final CountDownLatch haltLatch = new CountDownLatch(1);
        final AtomicBoolean running = new AtomicBoolean(true);
        try {
            startBasicServer(
                    NetworkFacadeImpl.INSTANCE,
                    new DefaultPGWireConfiguration(),
                    haltLatch,
                    running
            );

            Properties properties = new Properties();
            properties.setProperty("user", "admin");
            properties.setProperty("password", "quest");
            properties.setProperty("sslmode", "disable");

            final Connection connection = DriverManager.getConnection("jdbc:postgresql://127.0.0.1:9120/qdb", properties);
            PreparedStatement statement = connection.prepareStatement("create table x");
            try {
                statement.execute();
                Assert.fail();
            } catch (SQLException e) {
                assertTrue(e instanceof PSQLException);
                PSQLException pe = (PSQLException) e;
                Assert.assertEquals(15, pe.getServerErrorMessage().getPosition());
                Assert.assertEquals("'(' or 'as' expected", pe.getServerErrorMessage().getMessage());
            }
            statement.close();
            connection.close();
        } finally {
            running.set(false);
            haltLatch.await();
        }
    }

    /*
    We want to ensure that tableoid is set to zero, otherwise squirrelSql will not display the result set.
     */
    @Test
    public void testThatTableOidIsSetToZero() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            final CountDownLatch haltLatch = new CountDownLatch(1);
            final AtomicBoolean running = new AtomicBoolean(true);
            try {
                startBasicServer(
                        NetworkFacadeImpl.INSTANCE,
                        new DefaultPGWireConfiguration(),
                        haltLatch,
                        running
                );

                Properties properties = new Properties();
                properties.setProperty("user", "admin");
                properties.setProperty("password", "quest");
                properties.setProperty("sslmode", "disable");
                properties.setProperty("binaryTransfer", "false");
                final Connection connection = DriverManager.getConnection("jdbc:postgresql://127.0.0.1:9120/qdb", properties);
                PreparedStatement statement = connection.prepareStatement("select 1,2,3 from long_sequence(1)");

                ResultSet rs = statement.executeQuery();
                assertTrue(((PGResultSetMetaData) rs.getMetaData()).getBaseColumnName(1).isEmpty()); // getBaseColumnName returns "" if tableOid is zero
                rs.close();
                connection.close();
            } finally {
                running.set(false);
                haltLatch.await();
            }
        });
    }

    @Test
    public void testUnsupportedParameterType() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            final CountDownLatch haltLatch = new CountDownLatch(1);
            final AtomicBoolean running = new AtomicBoolean(true);
            try {
                startBasicServer(
                        NetworkFacadeImpl.INSTANCE,
                        new DefaultPGWireConfiguration(),
                        haltLatch,
                        running
                );

                Properties properties = new Properties();
                properties.setProperty("user", "admin");
                properties.setProperty("password", "quest");
                properties.setProperty("sslmode", "disable");
                properties.setProperty("binaryTransfer", "false");

                final Connection connection = DriverManager.getConnection("jdbc:postgresql://127.0.0.1:9120/qdb", properties);
                PreparedStatement statement = connection.prepareStatement("select x, ? from long_sequence(5)");
                // TIME is passed over protocol as UNSPECIFIED type
                // it will rely on date parser to work out what it is
                // for now date parser does not parse just time, it could i guess if required.
                statement.setTime(1, new Time(100L));

                try {
                    statement.executeQuery();
                    Assert.fail();
                } catch (SQLException e) {
                    assertTrue(Chars.startsWith(e.getMessage(), "ERROR: bad parameter value"));
                }
                connection.close();
            } finally {
                running.set(false);
                haltLatch.await();
            }
        });
    }

    @Test
    public void testUtf8QueryText() throws Exception {
        testQuery(
                "rnd_double(4) расход, ",
                "s[VARCHAR],i[INTEGER],расход[DOUBLE],t[TIMESTAMP],f[REAL],_short[SMALLINT],l[BIGINT],ts2[TIMESTAMP],bb[SMALLINT],b[BIT],rnd_symbol[VARCHAR],rnd_date[TIMESTAMP],rnd_bin[BINARY],rnd_char[CHAR],rnd_long256[NUMERIC]\n"
        );
    }

    private static void toSink(InputStream is, CharSink sink) throws IOException {
        // limit what we print
        byte[] bb = new byte[1];
        int i = 0;
        while (is.read(bb) > 0) {
            byte b = bb[0];
            if (i > 0) {
                if ((i % 16) == 0) {
                    sink.put('\n');
                    Numbers.appendHexPadded(sink, i);
                }
            } else {
                Numbers.appendHexPadded(sink, i);
            }
            sink.put(' ');

            final int v;
            if (b < 0) {
                v = 256 + b;
            } else {
                v = b;
            }

            if (v < 0x10) {
                sink.put('0');
                sink.put(hexDigits[b]);
            } else {
                sink.put(hexDigits[v / 0x10]);
                sink.put(hexDigits[v % 0x10]);
            }

            i++;
        }
    }

    private void assertHexScript(String script) throws Exception {
        assertHexScript(NetworkFacadeImpl.INSTANCE, NetworkFacadeImpl.INSTANCE, script, new DefaultPGWireConfiguration());
    }

    private void assertHexScript(
            NetworkFacade clientNf,
            NetworkFacade serverNf,
            String script,
            PGWireConfiguration PGWireConfiguration
    ) throws Exception {
        assertMemoryLeak(() -> {
            final CountDownLatch haltLatch = new CountDownLatch(1);
            final AtomicBoolean running = new AtomicBoolean(true);
            try {
                startBasicServer(
                        serverNf,
                        PGWireConfiguration,
                        haltLatch,
                        running
                );

                NetUtils.playScript(clientNf, script, "127.0.0.1", 9120);
            } finally {
                running.set(false);
                haltLatch.await();
            }
        });
    }

    private void assertPreparedStatementHex(NetworkFacade nf, PGWireConfiguration configuration) throws Exception {
        assertHexScript(NetworkFacadeImpl.INSTANCE, nf, ">0000006e00030000757365720078797a0064617461626173650071646200636c69656e745f656e636f64696e67005554463800446174655374796c650049534f0054696d655a6f6e65004575726f70652f4c6f6e646f6e0065787472615f666c6f61745f64696769747300320000\n" +
                "<520000000800000003\n" +
                ">70000000076f6800\n" +
                "<520000000800000000530000001154696d655a6f6e6500474d5400530000001d6170706c69636174696f6e5f6e616d6500517565737444420053000000187365727665725f76657273696f6e0031312e33005300000019696e74656765725f6461746574696d6573006f6e005300000019636c69656e745f656e636f64696e670055544638005a0000000549\n" +
                ">5000000022005345542065787472615f666c6f61745f646967697473203d2033000000420000000c0000000000000000450000000900000000015300000004\n" +
                "<310000000432000000044300000008534554005a0000000549\n" +
                ">500000003700534554206170706c69636174696f6e5f6e616d65203d2027506f737467726553514c204a4442432044726976657227000000420000000c0000000000000000450000000900000000015300000004\n" +
                "<310000000432000000044300000008534554005a0000000549\n" +
                ">500000002a0073656c65637420312c322c332066726f6d206c6f6e675f73657175656e6365283129000000420000000c000000000000000044000000065000450000000900000000005300000004\n" +
                "<31000000043200000004540000004200033100000040010001000000170004ffffffff00003200000040010002000000170004ffffffff00003300000040010003000000170004ffffffff000044000000150003000000013100000001320000000133430000000d53454c4543542031005a0000000549\n" +
                ">50000000260073656c65637420312066726f6d206c6f6e675f73657175656e6365283229000000420000000c000000000000000044000000065000450000000900000000005300000004\n" +
                "<31000000043200000004540000001a00013100000040010001000000170004ffffffff0000440000000b00010000000131440000000b00010000000131430000000d53454c4543542032005a0000000549\n" +
                ">500000002a0073656c65637420312c322c332066726f6d206c6f6e675f73657175656e6365283129000000420000000c000000000000000044000000065000450000000900000000005300000004\n" +
                "<31000000043200000004540000004200033100000040010001000000170004ffffffff00003200000040010002000000170004ffffffff00003300000040010003000000170004ffffffff000044000000150003000000013100000001320000000133430000000d53454c4543542031005a0000000549\n" +
                ">50000000260073656c65637420312066726f6d206c6f6e675f73657175656e6365283229000000420000000c000000000000000044000000065000450000000900000000005300000004\n" +
                "<31000000043200000004540000001a00013100000040010001000000170004ffffffff0000440000000b00010000000131440000000b00010000000131430000000d53454c4543542032005a0000000549\n" +
                ">500000002a0073656c65637420312c322c332066726f6d206c6f6e675f73657175656e6365283129000000420000000c000000000000000044000000065000450000000900000000005300000004\n" +
                "<31000000043200000004540000004200033100000040010001000000170004ffffffff00003200000040010002000000170004ffffffff00003300000040010003000000170004ffffffff000044000000150003000000013100000001320000000133430000000d53454c4543542031005a0000000549\n" +
                ">50000000260073656c65637420312066726f6d206c6f6e675f73657175656e6365283229000000420000000c000000000000000044000000065000450000000900000000005300000004\n" +
                "<31000000043200000004540000001a00013100000040010001000000170004ffffffff0000440000000b00010000000131440000000b00010000000131430000000d53454c4543542032005a0000000549\n" +
                ">500000002a0073656c65637420312c322c332066726f6d206c6f6e675f73657175656e6365283129000000420000000c000000000000000044000000065000450000000900000000005300000004\n" +
                "<31000000043200000004540000004200033100000040010001000000170004ffffffff00003200000040010002000000170004ffffffff00003300000040010003000000170004ffffffff000044000000150003000000013100000001320000000133430000000d53454c4543542031005a0000000549\n" +
                ">50000000260073656c65637420312066726f6d206c6f6e675f73657175656e6365283229000000420000000c000000000000000044000000065000450000000900000000005300000004\n" +
                "<31000000043200000004540000001a00013100000040010001000000170004ffffffff0000440000000b00010000000131440000000b00010000000131430000000d53454c4543542032005a0000000549\n" +
                ">500000002d535f310073656c65637420312c322c332066726f6d206c6f6e675f73657175656e6365283129000000420000000f00535f310000000000000044000000065000450000000900000000005300000004\n" +
                "<31000000043200000004540000004200033100000040010001000000170004ffffffff00003200000040010002000000170004ffffffff00003300000040010003000000170004ffffffff000044000000150003000000013100000001320000000133430000000d53454c4543542031005a0000000549\n" +
                ">50000000260073656c65637420312066726f6d206c6f6e675f73657175656e6365283229000000420000000c000000000000000044000000065000450000000900000000005300000004\n" +
                "<31000000043200000004540000001a00013100000040010001000000170004ffffffff0000440000000b00010000000131440000000b00010000000131430000000d53454c4543542032005a0000000549\n" +
                ">420000000f00535f3100000000000000450000000900000000005300000004\n" +
                "<320000000444000000150003000000013100000001320000000133430000000d53454c4543542031005a0000000549\n" +
                ">50000000260073656c65637420312066726f6d206c6f6e675f73657175656e6365283229000000420000000c000000000000000044000000065000450000000900000000005300000004\n" +
                "<31000000043200000004540000001a00013100000040010001000000170004ffffffff0000440000000b00010000000131440000000b00010000000131430000000d53454c4543542032005a0000000549\n" +
                ">420000000f00535f3100000000000000450000000900000000005300000004\n" +
                "<320000000444000000150003000000013100000001320000000133430000000d53454c4543542031005a0000000549\n" +
                ">50000000260073656c65637420312066726f6d206c6f6e675f73657175656e6365283229000000420000000c000000000000000044000000065000450000000900000000005300000004\n" +
                "<31000000043200000004540000001a00013100000040010001000000170004ffffffff0000440000000b00010000000131440000000b00010000000131430000000d53454c4543542032005a0000000549\n" +
                ">420000000f00535f3100000000000000450000000900000000005300000004\n" +
                "<320000000444000000150003000000013100000001320000000133430000000d53454c4543542031005a0000000549\n" +
                ">50000000260073656c65637420312066726f6d206c6f6e675f73657175656e6365283229000000420000000c000000000000000044000000065000450000000900000000005300000004\n" +
                "<31000000043200000004540000001a00013100000040010001000000170004ffffffff0000440000000b00010000000131440000000b00010000000131430000000d53454c4543542032005a0000000549\n" +
                ">420000000f00535f3100000000000000450000000900000000005300000004\n" +
                "<320000000444000000150003000000013100000001320000000133430000000d53454c4543542031005a0000000549\n" +
                ">50000000260073656c65637420312066726f6d206c6f6e675f73657175656e6365283229000000420000000c000000000000000044000000065000450000000900000000005300000004\n" +
                "<31000000043200000004540000001a00013100000040010001000000170004ffffffff0000440000000b00010000000131440000000b00010000000131430000000d53454c4543542032005a0000000549\n" +
                ">420000000f00535f3100000000000000450000000900000000005300000004\n" +
                "<320000000444000000150003000000013100000001320000000133430000000d53454c4543542031005a0000000549\n" +
                ">50000000260073656c65637420312066726f6d206c6f6e675f73657175656e6365283229000000420000000c000000000000000044000000065000450000000900000000005300000004\n" +
                "<31000000043200000004540000001a00013100000040010001000000170004ffffffff0000440000000b00010000000131440000000b00010000000131430000000d53454c4543542032005a0000000549\n" +
                ">5800000004", configuration);
    }

    private void assertResultSet(String expected, StringSink sink, ResultSet rs) throws SQLException, IOException {
        // dump metadata
        ResultSetMetaData metaData = rs.getMetaData();
        final int columnCount = metaData.getColumnCount();
        for (int i = 0; i < columnCount; i++) {
            if (i > 0) {
                sink.put(',');
            }

            sink.put(metaData.getColumnName(i + 1));
            sink.put('[').put(JDBCType.valueOf(metaData.getColumnType(i + 1)).name()).put(']');
        }
        sink.put('\n');

        while (rs.next()) {
            for (int i = 1; i <= columnCount; i++) {
                if (i > 1) {
                    sink.put(',');
                }
                switch (JDBCType.valueOf(metaData.getColumnType(i))) {
                    case VARCHAR:
                    case NUMERIC:
                        String stringValue = rs.getString(i);
                        if (rs.wasNull()) {
                            sink.put("null");
                        } else {
                            sink.put(stringValue);
                        }
                        break;
                    case INTEGER:
                        int intValue = rs.getInt(i);
                        if (rs.wasNull()) {
                            sink.put("null");
                        } else {
                            sink.put(intValue);
                        }
                        break;
                    case DOUBLE:
                        double doubleValue = rs.getDouble(i);
                        if (rs.wasNull()) {
                            sink.put("null");
                        } else {
                            sink.put(doubleValue);
                        }
                        break;
                    case TIMESTAMP:
                        Timestamp timestamp = rs.getTimestamp(i);
                        if (timestamp == null) {
                            sink.put("null");
                        } else {
                            sink.put(timestamp.toString());
                        }
                        break;
                    case REAL:
                        double floatValue = rs.getFloat(i);
                        if (rs.wasNull()) {
                            sink.put("null");
                        } else {
                            sink.put(floatValue);
                        }
                        break;
                    case SMALLINT:
                        sink.put(rs.getShort(i));
                        break;
                    case BIGINT:
                        long longValue = rs.getLong(i);
                        if (rs.wasNull()) {
                            sink.put("null");
                        } else {
                            sink.put(longValue);
                        }
                        break;
                    case CHAR:
                        char charValue = rs.getString(i).charAt(0);
                        if (rs.wasNull()) {
                            sink.put("null");
                        } else {
                            sink.put(charValue);
                        }
                        break;
                    case BIT:
                        sink.put(rs.getBoolean(i));
                        break;
                    case TIME:
                    case DATE:
                        timestamp = rs.getTimestamp(i);
                        if (timestamp == null) {
                            sink.put("null");
                        } else {
                            sink.put(timestamp.toString());
                        }
                        break;
                    case BINARY:
                        InputStream stream = rs.getBinaryStream(i);
                        if (stream == null) {
                            sink.put("null");
                        } else {
                            toSink(stream, sink);
                        }
                        break;
                }
            }
            sink.put('\n');
        }

        TestUtils.assertEquals(expected, sink);
    }

    private void execSelectWithParam(PreparedStatement select, int value) throws SQLException {
        sink.clear();
        select.setInt(1, value);
        try (ResultSet resultSet = select.executeQuery()) {
            sink.clear();
            while (resultSet.next()) {
                sink.put(resultSet.getInt(1));
                sink.put('\n');
            }
        }
    }

    @NotNull
    private NetworkFacade getFragmentedSendFacade() {
        return new NetworkFacadeImpl() {
            @Override
            public int send(long fd, long buffer, int bufferLen) {
                int total = 0;
                for (int i = 0; i < bufferLen; i++) {
                    int n = super.send(fd, buffer + i, 1);
                    if (n < 0) {
                        return n;
                    }
                    total += n;
                }
                return total;
            }
        };
    }

    @NotNull
    private DefaultPGWireConfiguration getHexPgWireConfig() {
        return new DefaultPGWireConfiguration() {
            @Override
            public String getDefaultPassword() {
                return "oh";
            }

            @Override
            public String getDefaultUsername() {
                return "xyz";
            }
        };
    }

    private void startBasicServer(
            NetworkFacade nf,
            PGWireConfiguration configuration,
            CountDownLatch haltLatch,
            AtomicBoolean running
    ) throws BrokenBarrierException, InterruptedException {
        final CyclicBarrier barrier = new CyclicBarrier(2);
        new Thread(() -> {
            long fd = Net.socketTcp(true);
            try {
                Assert.assertEquals(0, nf.setReusePort(fd));
                nf.configureNoLinger(fd);

                assertTrue(nf.bindTcp(fd, 0, 9120));
                nf.listen(fd, 128);

                LOG.info().$("listening [fd=").$(fd).$(']').$();

                try (PGJobContext PGJobContext = new PGJobContext(configuration, engine, engine.getMessageBus(), null)) {
                    SharedRandom.RANDOM.set(new Rnd());
                    try {
                        barrier.await();
                    } catch (InterruptedException | BrokenBarrierException e) {
                        e.printStackTrace();
                    }
                    final long clientFd = Net.accept(fd);
                    nf.configureNonBlocking(clientFd);
                    try (PGConnectionContext context = new PGConnectionContext(engine, configuration, null, 1)) {
                        context.of(clientFd, null);
                        LOG.info().$("connected [clientFd=").$(clientFd).$(']').$();
                        while (running.get()) {
                            try {
                                PGJobContext.handleClientOperation(context);
                            } catch (PeerDisconnectedException | BadProtocolException e) {
                                break;
                            } catch (PeerIsSlowToReadException | PeerIsSlowToWriteException ignored) {
                            }
                        }
                        nf.close(clientFd);
                    }
                }
            } finally {
                nf.close(fd);
                haltLatch.countDown();
                LOG.info().$("done").$();
            }
        }).start();
        barrier.await();
    }

    private void testInsert0(boolean simpleQueryMode) throws Exception {
        assertMemoryLeak(() -> {

            String expectedAll = "0\n" +
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
                    "20\n" +
                    "21\n" +
                    "22\n" +
                    "23\n" +
                    "24\n" +
                    "25\n" +
                    "26\n" +
                    "27\n" +
                    "28\n" +
                    "29\n";


            final CountDownLatch haltLatch = new CountDownLatch(1);
            final AtomicBoolean running = new AtomicBoolean(true);
            try {
                startBasicServer(
                        NetworkFacadeImpl.INSTANCE,
                        new DefaultPGWireConfiguration(),
                        haltLatch,
                        running
                );

                Properties properties = new Properties();
                properties.setProperty("user", "admin");
                properties.setProperty("password", "quest");
                properties.setProperty("sslmode", "disable");
                if (simpleQueryMode) {
                    properties.setProperty("preferQueryMode", "simple");
                }

                try (final Connection connection = DriverManager.getConnection("jdbc:postgresql://localhost:9120/qdb", properties)) {
                    PreparedStatement statement = connection.prepareStatement("create table x (a int)");
                    statement.execute();

                    // exercise parameters on select statement
                    PreparedStatement select = connection.prepareStatement("x where a = ?");
                    execSelectWithParam(select, 9);

                    PreparedStatement insert = connection.prepareStatement("insert into x (a) values (?)");
                    for (int i = 0; i < 30; i++) {
                        insert.setInt(1, i);
                        insert.execute();
                    }

                    try (ResultSet resultSet = connection.prepareStatement("x").executeQuery()) {
                        sink.clear();
                        while (resultSet.next()) {
                            sink.put(resultSet.getInt(1));
                            sink.put('\n');
                        }
                    }

                    TestUtils.assertEquals(expectedAll, sink);

                    // exercise parameters on select statement
                    execSelectWithParam(select, 9);
                    TestUtils.assertEquals("9\n", sink);

                    execSelectWithParam(select, 11);
                    TestUtils.assertEquals("11\n", sink);

                }
            } finally {
                running.set(false);
                haltLatch.await();
            }
        });
    }

    private void testInsertTableDoesNotExist(boolean simple, String expectedError) throws Exception {
        // we are going to:
        // 1. create a table
        // 2. insert a record
        // 3. drop table
        // 4. attempt to insert a record (should fail)
        assertMemoryLeak(() -> {
            final CountDownLatch haltLatch = new CountDownLatch(1);
            final AtomicBoolean running = new AtomicBoolean(true);
            try {
                startBasicServer(
                        NetworkFacadeImpl.INSTANCE,
                        new DefaultPGWireConfiguration(),
                        haltLatch,
                        running
                );

                Properties properties = new Properties();
                properties.setProperty("user", "admin");
                properties.setProperty("password", "quest");
                properties.setProperty("sslmode", "disable");

                if (simple) {
                    properties.setProperty("preferQueryMode", "simple");
                }

                try (final Connection connection = DriverManager.getConnection("jdbc:postgresql://localhost:9120/qdb", properties)) {
                    PreparedStatement statement = connection.prepareStatement("create table x (a int)");
                    statement.execute();

                    // exercise parameters on select statement
                    PreparedStatement select = connection.prepareStatement("x where a = ?");
                    execSelectWithParam(select, 9);

                    PreparedStatement insert = connection.prepareStatement("insert into x (a) values (?)");
                    insert.setInt(1, 1);
                    insert.execute();

                    PreparedStatement drop = connection.prepareStatement("drop table x");
                    drop.execute();

                    try {
                        insert.setInt(1, 10);
                        insert.execute();
                        Assert.fail();
                    } catch (SQLException e) {
                        TestUtils.assertContains(e.getMessage(), expectedError);
                    }
                }
            } finally {
                running.set(false);
                haltLatch.await();
            }
        });
    }

    private void testQuery(String s, String s2) throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            final CountDownLatch haltLatch = new CountDownLatch(1);
            final AtomicBoolean running = new AtomicBoolean(true);
            try {
                startBasicServer(NetworkFacadeImpl.INSTANCE,
                        new DefaultPGWireConfiguration(),
                        haltLatch,
                        running
                );

                Properties properties = new Properties();
                properties.setProperty("user", "admin");
                properties.setProperty("password", "quest");

                final Connection connection = DriverManager.getConnection("jdbc:postgresql://127.0.0.1:9120/qdb", properties);
                Statement statement = connection.createStatement();
                ResultSet rs = statement.executeQuery(
                        "select " +
                                "rnd_str(4,4,4) s, " +
                                "rnd_int(0, 256, 4) i, " +
                                s +
                                "timestamp_sequence(0,10000) t, " +
                                "rnd_float(4) f, " +
                                "rnd_short() _short, " +
                                "rnd_long(0, 10000000, 5) l, " +
                                "rnd_timestamp(to_timestamp('2015','yyyy'),to_timestamp('2016','yyyy'),2) ts2, " +
                                "rnd_byte(0,127) bb, " +
                                "rnd_boolean() b, " +
                                "rnd_symbol(4,4,4,2), " +
                                "rnd_date(to_date('2015', 'yyyy'), to_date('2016', 'yyyy'), 2)," +
                                "rnd_bin(10,20,2), " +
                                "rnd_char(), " +
                                "rnd_long256() " +
                                "from long_sequence(50)");

                final String expected = s2 +
                        "null,57,0.6254021542412018,1970-01-01 00:00:00.0,0.4620000123977661,-1593,3425232,null,121,false,PEHN,2015-03-17 04:25:52.765,00000000 19 c4 95 94 36 53 49 b4 59 7e 3b 08 a1 1e,D,0x5f20a35e80e154f458dfd08eeb9cc39ecec82869edec121bc2593f82b430328d\n" +
                        "OUOJ,77,null,1970-01-01 00:00:00.01,0.6759999990463257,-7374,7777791,2015-06-19 08:47:45.603182,53,true,null,2015-11-10 09:50:33.215,00000000 8b 81 2b 93 4d 1a 8e 78 b5 b9 11 53 d0 fb 64 bb\n" +
                        "00000010 1a d4 f0,V,0xbedf29efb28cdcb1b75dccbdf1f8b84b9b27eba5e9cfa1e29660300cea7db540\n" +
                        "ICCX,205,0.8837421918800907,1970-01-01 00:00:00.02,0.05400000140070915,6093,4552960,2015-07-17 00:50:59.787742,33,false,VTJW,2015-07-15 01:06:11.226,00000000 e5 61 2f 64 0e 2c 7f d7 6f b8 c9 ae 28 c7 84 47,U,0x8b4e4831499fc2a526567f4430b46b7f78c594c496995885aa1896d0ad3419d2\n" +
                        "GSHO,31,0.34947269997137365,1970-01-01 00:00:00.03,0.1979999989271164,10795,6406207,2015-05-22 14:59:41.673422,56,false,null,null,00000000 49 1c f2 3c ed 39 ac a8 3b a6,S,0x7eb6d80649d1dfe38e4a7f661df6c32b2f171b3f06f6387d2fd2b4a60ba2ba3b\n" +
                        "HZEP,180,0.06944480046327317,1970-01-01 00:00:00.04,0.4300000071525574,21347,null,2015-02-07 10:02:13.600956,41,false,HYRX,null,00000000 ea c3 c9 73 93 46 fe c2 d3 68 79 8b 43 1d 57 34,F,0x38e4be9e19321b57832dd27952d949d8691dd4412a2d398d4fc01e2b9fd11623\n" +
                        "HWVD,38,0.48524046868499715,1970-01-01 00:00:00.05,0.6800000071525574,25579,5575751,2015-10-19 12:38:49.360294,15,false,VTJW,2015-02-06 22:58:50.333,null,Q,0x85134468025aaeb0a2f8bbebb989ba609bb0f21ac9e427283eef3f158e084362\n" +
                        "PGLU,97,0.029227696942726644,1970-01-01 00:00:00.06,0.1720000058412552,-18912,8340272,2015-05-24 22:09:55.175991,111,false,VTJW,2015-11-08 21:57:22.812,00000000 d9 6f 04 ab 27 47 8f 23 3f ae 7c 9f 77 04 e9 0c\n" +
                        "00000010 ea 4e ea 8b,K,0x55d3686d5da27e14255a91b0e28abeb36c3493fcb2d0272d6046e5d137dd8f0f\n" +
                        "WIFF,104,0.892454783921197,1970-01-01 00:00:00.07,0.09300000220537186,28218,4009057,2015-02-18 07:26:10.141055,89,false,HYRX,null,00000000 29 26 c5 aa da 18 ce 5f b2 8b 5c 54 90 25 c2 20\n" +
                        "00000010 ff,R,0x55b0586d1c02dfb399904624c49b6d8a7d85ee2916b209c779406ab1f85e333a\n" +
                        "CLTJ,115,0.2093569947644236,1970-01-01 00:00:00.08,0.5460000038146973,-8207,2378718,2015-04-21 12:25:43.291916,31,false,PEHN,null,00000000 a5 db a1 76 1c 1c 26 fb 2e 42 fa,F,0x483c83d88ac674e3894499a1a1680580cfedff23a67d918fb49b3c24e456ad6e\n" +
                        "HFLP,79,0.9130151105125102,1970-01-01 00:00:00.09,null,14667,2513248,2015-08-31 13:16:12.318782,3,false,null,2015-02-08 12:28:36.066,null,U,0x79423d4d320d2649767a4feda060d4fb6923c0c7d965969da1b1140a2be25241\n" +
                        "GLNY,138,0.7165847318191405,1970-01-01 00:00:00.1,0.753000020980835,-2666,9337379,2015-03-25 09:21:52.776576,111,false,HYRX,2015-01-24 15:23:13.092,00000000 62 e1 4e d6 b2 57 5b e3 71 3d 20 e2 37 f2 64 43,Y,0xaac42ccbc493cf44aa6a0a1d4cdf40dd6ae4fd257e4412a07f19777ec1368055\n" +
                        "VTNP,237,0.29242748475227853,1970-01-01 00:00:00.11,0.753000020980835,-26861,2354132,2015-02-10 18:27:11.140675,56,true,null,2015-02-25 00:45:15.363,00000000 28 b6 a9 17 ec 0e 01 c4 eb 9f 13 8f bb 2a 4b,O,0x926cdd99e63abb35650d1fb462d014df59070392ef6aa389932e4b508e35428f\n" +
                        "WFOQ,255,null,1970-01-01 00:00:00.12,0.11599999666213989,31569,6688277,2015-05-19 03:30:45.779999,126,true,PEHN,2015-12-09 09:57:17.078,null,E,0x4f38804270a4a64349b5760a687d8cf838cbb9ae96e9ecdc745ed9faeb513ad3\n" +
                        "EJCT,195,0.13312214396754163,1970-01-01 00:00:00.13,0.9440000057220459,-3013,null,2015-11-03 14:54:47.524015,114,true,PEHN,2015-08-28 07:41:29.952,00000000 fb 9d 63 ca 94 00 6b dd 18 fe 71 76 bc 45 24 cd\n" +
                        "00000010 13 00 7c,R,0x3cfe50b9cabaf1f29e0dcffb7520ebcac48ad6b8f6962219b27b0ac7fbdee201\n" +
                        "JYYF,249,0.2000682450929353,1970-01-01 00:00:00.14,0.6019999980926514,5869,2079217,2015-07-10 18:16:38.882991,44,true,HYRX,null,00000000 b7 6c 4b fb 2d 16 f3 89 a3 83 64 de d6 fd c4 5b\n" +
                        "00000010 c4 e9 19 47,P,0x85e70b46349799fe49f783d5343dd7bc3d3fe1302cd3371137fccdabf181b5ad\n" +
                        "TZOD,null,0.36078878996232167,1970-01-01 00:00:00.15,0.6010000109672546,-23125,5083310,null,11,false,VTJW,2015-09-19 18:14:57.59,00000000 c5 60 b7 d1 5a 0c e9 db 51 13 4d 59 20 c9 37 a1\n" +
                        "00000010 00,E,0xcff85f9258847e03a6f2e2a772cd2f3751d822a67dff3d2375166223a6181642\n" +
                        "PBMB,76,0.23567419576658333,1970-01-01 00:00:00.16,0.5709999799728394,26284,null,2015-05-21 13:14:56.349036,45,true,null,2015-09-11 09:34:39.05,00000000 97 cb f6 2c 23 45 a3 76 60 15,M,0x3c3a3b7947ce8369926cbcb16e9a2f11cfab70f2d175d0d9aeb989be79cd2b8c\n" +
                        "TKRI,201,0.2625424312419562,1970-01-01 00:00:00.17,0.9150000214576721,-5486,9917162,2015-05-03 03:59:04.256719,66,false,VTJW,2015-01-15 03:22:01.033,00000000 a1 f5 4b ea 01 c9 63 b4 fc 92 60 1f df 41 ec 2c,O,0x4e3e15ad49e0a859312981a73c9dfce79022a75a739ee488eefa2920026dba88\n" +
                        "NKGQ,174,0.4039042639581232,1970-01-01 00:00:00.18,0.43799999356269836,20687,7315329,2015-07-25 04:52:27.724869,20,false,PEHN,2015-06-10 22:28:57.01,00000000 92 83 fc 88 f3 32 27 70 c8 01 b0,T,0x579b14c2725d7a7e5dfbd8e23498715b8d9ee30e7bcbf83a6d1b1c80f012a4c9\n" +
                        "FUXC,52,0.7430101994511517,1970-01-01 00:00:00.19,null,-14729,1042064,2015-08-21 02:10:58.949674,28,true,CPSW,2015-08-29 20:15:51.835,null,X,0x41457ebc5a02a2b542cbd49414e022a06f4aa2dc48a9a4d99288224be334b250\n" +
                        "TGNJ,159,0.9562577128401444,1970-01-01 00:00:00.2,0.25099998712539673,795,5069730,2015-07-01 01:36:57.101749,71,true,PEHN,2015-09-12 05:41:59.999,00000000 33 3f b2 67 da 98 47 47 bf 4f ea 5f 48 ed,M,0x4ba20a8e0cf7c53c9f527485c4aac4a2826f47baacd58b28700a67f6119c63bb\n" +
                        "HCNP,173,0.18684267640195917,1970-01-01 00:00:00.21,0.6880000233650208,-14882,8416858,2015-06-16 19:31:59.812848,25,false,HYRX,2015-09-30 17:28:24.113,00000000 1d 5c c1 5d 2d 44 ea 00 81 c4 19 a1 ec 74 f8 10\n" +
                        "00000010 fc 6e 23,D,0x3d64559865f84c86488be951819f43042f036147c78e0b2d127ca5db2f41c5e0\n" +
                        "EZBR,243,0.8203418140538824,1970-01-01 00:00:00.22,0.22100000083446503,-8447,4677168,2015-03-24 03:32:39.832378,78,false,CPSW,2015-02-16 04:04:19.082,00000000 42 67 78 47 b3 80 69 b9 14 d6 fc ee 03 22 81 b8,Q,0x721304ffe1c934386466208d506905af40c7e3bce4b28406783a3945ab682cc4\n" +
                        "ZPBH,131,0.1999576586778039,1970-01-01 00:00:00.23,0.4790000021457672,-18951,874555,2015-12-22 19:13:55.404123,52,false,null,2015-10-03 05:16:17.891,null,Z,0xa944baa809a3f2addd4121c47cb1139add4f1a5641c91e3ab81f4f0ca152ec61\n" +
                        "VLTP,196,0.4104855595304533,1970-01-01 00:00:00.24,0.9179999828338623,-12269,142107,2015-10-10 18:27:43.423774,92,false,PEHN,2015-02-06 18:42:24.631,null,H,0x5293ce3394424e6a5ae63bdf09a84e32bac4484bdeec40e887ec84d015101766\n" +
                        "RUMM,185,null,1970-01-01 00:00:00.25,0.8379999995231628,-27649,3639049,2015-05-06 00:51:57.375784,89,true,PEHN,null,null,W,0x3166ed3bbffb858312f19057d95341886360c99923d254f38f22547ae9661423\n" +
                        "null,71,0.7409092302023607,1970-01-01 00:00:00.26,0.7419999837875366,-18837,4161180,2015-04-22 10:19:19.162814,37,true,HYRX,2015-09-23 03:14:56.664,00000000 8e 93 bd 27 42 f8 25 2a 42 71 a3 7a 58 e5,D,0x689a15d8906770fcaefe0266b9f63bd6698c574248e9011c6cc84d9a6d41e0b8\n" +
                        "NGZT,214,0.18170646835643245,1970-01-01 00:00:00.27,0.8410000205039978,21764,3231872,null,79,false,HYRX,2015-05-20 07:51:29.675,00000000 ab ab ac 21 61 99 be 2d f5 30 78 6d 5a 3b,H,0x5b8def4e7a017e884a3c2c504403708b49fb8d5fe0ff283cbac6499e71ce5b30\n" +
                        "EYYP,13,null,1970-01-01 00:00:00.28,0.5339999794960022,19136,4658108,2015-08-20 05:26:04.061614,5,false,CPSW,2015-03-23 23:43:37.634,00000000 c8 66 0c 40 71 ea 20 7e 43 97 27 1f 5c d9 ee 04\n" +
                        "00000010 5b 9c,C,0x6e6ed811e25486953f35987a50016bbf481e9f55c33ac48c6a22b0bd6f7b0bf2\n" +
                        "GMPL,50,0.7902682918274309,1970-01-01 00:00:00.29,0.8740000128746033,-27807,5693029,2015-07-14 21:06:07.975747,37,true,CPSW,2015-09-01 04:00:29.049,00000000 3b 4b b7 e2 7f ab 6e 23 03 dd c7 d6,U,0x72c607b1992ff2f8802e839b77a4a2d34b8b967c412e7c895b509b55d1c38d29\n" +
                        "BCZI,207,0.10863061577000221,1970-01-01 00:00:00.3,0.1289999932050705,3999,121232,null,88,true,CPSW,2015-05-10 21:10:20.041,00000000 97 0b f5 ef 3b be 85 7c 11 f7 34,K,0x33be4c04695f74d776ac6df71a221f518f3c64248fb5943ea55ab4e6916f3f6c\n" +
                        "DXUU,139,null,1970-01-01 00:00:00.31,0.2619999945163727,-15289,341060,2015-01-06 07:48:24.624773,110,false,null,2015-07-08 18:37:16.872,00000000 71 cf 5a 8f 21 06 b2 3f 0e 41 93 89 27 ca 10 2f\n" +
                        "00000010 60 ce,N,0x1c05d81633694e02795ebacfceb0c7dd7ec9b7e9c634bc791283140ab775531c\n" +
                        "FMDV,197,0.2522102209201954,1970-01-01 00:00:00.32,0.9929999709129333,-26026,5396438,null,83,true,CPSW,null,00000000 86 75 ad a5 2d 49 48 68 36 f0 35,K,0x308a7a4966e65a0160b00229634848957fa67d6a419e1721b1520f66caa74945\n" +
                        "SQCN,62,0.11500943478849246,1970-01-01 00:00:00.33,0.5950000286102295,1011,4631412,null,56,false,VTJW,null,null,W,0x66906dc1f1adbc206a8bf627c859714a6b841d6c6c8e44ce147261f8689d9250\n" +
                        "QSCM,130,0.8671405978559277,1970-01-01 00:00:00.34,0.42800000309944153,22899,403193,null,21,true,PEHN,2015-11-30 21:04:32.865,00000000 a0 ba a5 d1 63 ca 32 e5 0d 68 52 c6 94 c3 18 c9\n" +
                        "00000010 7c,I,0x3dcc3621f3734c485bb81c28ec2ddb0163def06fb4e695dc2bfa47b82318ff9f\n" +
                        "UUZI,196,0.9277429447320458,1970-01-01 00:00:00.35,0.625,24355,5761736,null,116,false,null,2015-02-04 07:15:26.997,null,B,0xb0a5224248b093a067eee4529cce26c37429f999bffc9548aa3df14bfed42969\n" +
                        "DEQN,41,0.9028381160965113,1970-01-01 00:00:00.36,0.11999999731779099,29066,2545404,2015-04-07 21:58:14.714791,125,false,PEHN,2015-02-06 23:29:49.836,00000000 ec 4b 97 27 df cd 7a 14 07 92 01,I,0x55016acb254b58cd3ce05caab6551831683728ff2f725aa1ba623366c2d08e6a\n" +
                        "null,164,0.7652775387729266,1970-01-01 00:00:00.37,0.31200000643730164,-8563,7684501,2015-02-01 12:38:28.322282,0,true,HYRX,2015-07-16 20:11:51.34,null,F,0x97af9db84b80545ecdee65143cbc92f89efea4d0456d90f29dd9339572281042\n" +
                        "QJPL,160,0.1740035812230043,1970-01-01 00:00:00.38,0.7630000114440918,5991,2099269,2015-02-25 15:49:06.472674,65,true,VTJW,2015-04-23 11:15:13.065,00000000 de 58 45 d0 1b 58 be 33 92 cd 5c 9d,E,0xa85a5fc20776e82b36c1cdbfe34eb2636eec4ffc0b44f925b09ac4f09cb27f36\n" +
                        "BKUN,208,0.4452148524967028,1970-01-01 00:00:00.39,0.5820000171661377,17928,6383721,2015-10-23 07:12:20.730424,7,false,null,2015-01-02 17:04:58.959,00000000 5e 37 e4 68 2a 96 06 46 b6 aa,F,0xe1d2020be2cb7be9c5b68f9ea1bd30c789e6d0729d44b64390678b574ed0f592\n" +
                        "REDS,4,0.03804995327454719,1970-01-01 00:00:00.4,0.10300000011920929,2358,1897491,2015-07-21 16:34:14.571565,75,false,CPSW,2015-07-30 16:04:46.726,00000000 d6 88 3a 93 ef 24 a5 e2 bc 86,P,0x892458b34e8769928647166465305ef1dd668040845a10a38ea5fba6cf9bfc92\n" +
                        "MPVR,null,null,1970-01-01 00:00:00.41,0.5920000076293945,8754,5828044,2015-10-05 21:11:10.600851,116,false,CPSW,null,null,H,0x9d1e67c6be2f24b2a4e2cc6a628c94395924dadabaed7ee459b2a61b0fcb74c5\n" +
                        "KKNZ,186,0.8223388398922372,1970-01-01 00:00:00.42,0.7200000286102295,-6179,8728907,null,80,true,VTJW,2015-09-11 03:49:12.244,00000000 16 b2 d8 83 f5 95 7c 95 fd 52 bb 50 c9,B,0x55724661cfcc811f4482e1a2ba8efaef6e4aef0394801c40941d89f24081f64d\n" +
                        "BICL,182,0.7215695095610233,1970-01-01 00:00:00.43,0.22699999809265137,-22899,6401660,2015-08-23 18:31:29.931618,78,true,null,null,null,T,0xbbb751ee10f060d1c2fbeb73044504aea55a8e283bcf857b539d8cd889fa9c91\n" +
                        "SWPF,null,0.48770772310128674,1970-01-01 00:00:00.44,0.9139999747276306,-17929,8377336,2015-12-13 23:04:20.465454,28,false,HYRX,2015-10-31 13:37:01.327,00000000 b2 31 9c 69 be 74 9a ad cc cf b8 e4 d1 7a 4f,I,0xbe91d734443388a2a631d716b575c819c9224a25e3f6e6fa6cd78093d5e7ea16\n" +
                        "BHEV,80,0.8917678500174907,1970-01-01 00:00:00.45,0.2370000034570694,29284,9577513,2015-10-20 07:38:23.889249,27,false,HYRX,2015-12-15 13:32:56.797,00000000 92 83 24 53 60 4d 04 c2 f0 7a 07 d4 a3 d1 5f 0d\n" +
                        "00000010 fe 63 10 0d,V,0x225fddd0f4325a9d8634e1cb317338a0d3cb7f61737f167dc902b6f6d779c753\n" +
                        "DPCH,62,0.6684502332750604,1970-01-01 00:00:00.46,0.8790000081062317,-22600,9266553,null,89,true,VTJW,2015-05-25 19:42:17.955,00000000 35 1b b9 0f 97 f5 77 7e a3 2d ce fe eb cd 47 06\n" +
                        "00000010 53 61 97,S,0x89d6a43b23f83695b236ae5ffab54622ce1f4dac846490a8b88f0468c0cbfa33\n" +
                        "MKNJ,61,0.2682009935575007,1970-01-01 00:00:00.47,0.8130000233650208,-1322,null,2015-11-04 08:11:39.996132,4,false,CPSW,2015-07-29 22:51:03.349,00000000 82 08 fb e7 94 3a 32 5d 8a 66 0b e4 85 f1 13 06\n" +
                        "00000010 f2 27,V,0x9890d4aea149f0498bdef1c6ba16dd8cbd01cf83632884ae8b7083f888554b0c\n" +
                        "GSQI,158,0.8047954890194065,1970-01-01 00:00:00.48,0.34700000286102295,23139,1252385,2015-04-22 00:10:12.067311,32,true,null,2015-01-09 06:06:32.213,00000000 38 a7 85 46 1a 27 5b 4d 0f 33 f4 70,V,0xc0e6e110b909e13a812425a38162be0bb65e29ed529d4dba868a7075f3b34357\n" +
                        "BPTU,205,0.430214712409255,1970-01-01 00:00:00.49,0.9049999713897705,31266,8271557,2015-01-07 05:53:03.838005,14,true,VTJW,2015-10-30 05:33:15.819,00000000 24 0b c5 1a 5a 8d 85 50 39 42 9e 8a 86 17 89 6b,S,0x4e272e9dfde7bb12618178f7feba5021382a8c47a28fefa475d743cf0c2c4bcd\n";

                StringSink sink = new StringSink();

                // dump metadata
                assertResultSet(expected, sink, rs);
                connection.close();
            } finally {
                running.set(false);
                haltLatch.await();
            }
        });
    }
}
