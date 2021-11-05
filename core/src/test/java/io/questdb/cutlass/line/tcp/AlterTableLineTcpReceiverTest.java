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

package io.questdb.cutlass.line.tcp;

import io.questdb.cairo.TableReader;
import io.questdb.cairo.TableReaderMetadata;
import io.questdb.cairo.pool.PoolListener;
import io.questdb.cairo.security.AllowAllCairoSecurityContext;
import io.questdb.cairo.sql.AlterStatement;
import io.questdb.griffin.*;
import io.questdb.griffin.engine.functions.bind.BindVariableServiceImpl;
import io.questdb.griffin.model.IntervalUtils;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.mp.SCSequence;
import io.questdb.mp.SOCountDownLatch;
import io.questdb.network.Net;
import io.questdb.std.Chars;
import io.questdb.std.MemoryTag;
import io.questdb.std.Unsafe;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

import java.nio.charset.StandardCharsets;
import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.CyclicBarrier;

public class AlterTableLineTcpReceiverTest extends AbstractLineTcpReceiverTest {
    private final static Log LOG = LogFactory.getLog(AlterTableLineTcpReceiverTest.class);
    private SqlException sqlException;
    public static final int WAIT_NO_WAIT = 0x0;
    public static final int WAIT_ENGINE_TABLE_RELEASE = 0x1;
    public static final int WAIT_ILP_TABLE_RELEASE = 0x2;
    public static final int WAIT_ALTER_TABLE_RELEASE = 0x4;
    private volatile long alterCommandId;
    private final SCSequence scSequence  = new SCSequence();

    @Test
    public void testAlterTableAddIndex() throws Exception {
        runInContext((server) -> {
            String lineData = "plug,label=Power,room=6A watts=\"1\" 2631819999000\n" +
                    "plug,label=Power,room=6B watts=\"22\" 1631817902842\n" +
                    "plug,label=Line,room=6C watts=\"333\" 1531817902842\n";
            SqlException ex = sendWithAlterStatement(server, lineData, "plug", WAIT_ALTER_TABLE_RELEASE | WAIT_ENGINE_TABLE_RELEASE, false,
                    "ALTER TABLE plug ALTER COLUMN label ADD INDEX");
            Assert.assertNull(ex);

            String expected = "label\troom\twatts\ttimestamp\n" +
                    "Line\t6C\t333\t1970-01-01T00:25:31.817902Z\n" +
                    "Power\t6B\t22\t1970-01-01T00:27:11.817902Z\n" +
                    "Power\t6A\t1\t1970-01-01T00:43:51.819999Z\n";
            assertTable(expected, "plug");
            try(TableReader rdr = engine.getReader(AllowAllCairoSecurityContext.INSTANCE, "plug")) {
                TableReaderMetadata metadata = rdr.getMetadata();
                Assert.assertTrue("Alter makes column indexed",
                        metadata.isColumnIndexed(
                                metadata.getColumnIndex("label")
                        ));
            }
        });
    }

    @Test
    public void testAlterCommandDropsColumn() throws Exception {
        runInContext((server) -> {
            String lineData = "plug,label=Power,room=6A watts=\"1\" 2631819999000\n" +
                    "plug,label=Power,room=6B watts=\"22\" 1631817902842\n" +
                    "plug,label=Line,room=6C watts=\"333\" 1531817902842\n";

            SqlException exception = sendWithAlterStatement(server, lineData,
                    "plug",
                    WAIT_ALTER_TABLE_RELEASE | WAIT_ENGINE_TABLE_RELEASE,
                    false,
                    "ALTER TABLE plug DROP COLUMN label");

            Assert.assertNotNull(exception);
            TestUtils.assertEquals("ALTER TABLE cannot change table structure while Writer is busy", exception.getFlyweightMessage());
            exception = sendWithAlterStatement(
                    server,
                    lineData,
                    "plug",
                    WAIT_ALTER_TABLE_RELEASE | WAIT_ENGINE_TABLE_RELEASE,
                    false,
                    "ALTER TABLE plug RENAME COLUMN label TO label2"
            );

            Assert.assertNotNull(exception);
            TestUtils.assertEquals(
                    "ALTER TABLE cannot change table structure while Writer is busy",
                    exception.getFlyweightMessage()
            );
            lineData = "plug,label=Power,room=6A watts=\"4\" 2631819999001\n" +
                    "plug,label=Power,room=6B watts=\"55\" 1631817902843\n" +
                    "plug,label=Line,room=6C watts=\"666\" 1531817902843\n";

            // re-send, this should re-add column label
            send(server, lineData, "plug");

            String expected = "label\troom\twatts\ttimestamp\n" +
                    "Line\t6C\t666\t1970-01-01T00:25:31.817902Z\n" +
                    "Line\t6C\t333\t1970-01-01T00:25:31.817902Z\n" +
                    "Line\t6C\t333\t1970-01-01T00:25:31.817902Z\n" +
                    "Power\t6B\t55\t1970-01-01T00:27:11.817902Z\n" +
                    "Power\t6B\t22\t1970-01-01T00:27:11.817902Z\n" +
                    "Power\t6B\t22\t1970-01-01T00:27:11.817902Z\n" +
                    "Power\t6A\t4\t1970-01-01T00:43:51.819999Z\n" +
                    "Power\t6A\t1\t1970-01-01T00:43:51.819999Z\n" +
                    "Power\t6A\t1\t1970-01-01T00:43:51.819999Z\n";
            assertTable(expected, "plug");
        });
    }

    @Test
    public void testAlterCommandAddColumn() throws Exception {
        runInContext((server) -> {
            String lineData = "plug,room=6A watts=\"1\" 2631819999000\n" +
                    "plug,room=6B watts=\"22\" 1631817902842\n" +
                    "plug,room=6C watts=\"333\" 1531817902842\n";

            SqlException exception = sendWithAlterStatement(
                    server,
                    lineData,
                    "plug",
                    WAIT_ALTER_TABLE_RELEASE | WAIT_ENGINE_TABLE_RELEASE,
                    false,
                    "ALTER TABLE plug Add COLUMN label2 INT");

            Assert.assertNull(exception);

            lineData = "plug,label=Power,room=6A watts=\"4\" 2631819999000\n" +
                    "plug,label=Power,room=6B watts=\"55\" 1631817902842\n" +
                    "plug,label=Line,room=6C watts=\"666\" 1531817902842\n";

            send(
                    server,
                    lineData,
                    "plug"
            );

            String expected = "room\twatts\ttimestamp\tlabel2\tlabel\n" +
                    "6C\t666\t1970-01-01T00:25:31.817902Z\tNaN\tLine\n" +
                    "6C\t333\t1970-01-01T00:25:31.817902Z\tNaN\t\n" +
                    "6B\t55\t1970-01-01T00:27:11.817902Z\tNaN\tPower\n" +
                    "6B\t22\t1970-01-01T00:27:11.817902Z\tNaN\t\n" +
                    "6A\t4\t1970-01-01T00:43:51.819999Z\tNaN\tPower\n" +
                    "6A\t1\t1970-01-01T00:43:51.819999Z\tNaN\t\n";
            assertTable(expected, "plug");
        });
    }

    @Test
    public void testAlterCommandDropPartition() throws Exception {
        long day1 = 0;
        long day2 = IntervalUtils.parseFloorPartialDate("1970-02-02") * 1000;
        long day3 = IntervalUtils.parseFloorPartialDate("1970-03-03") * 1000;
        runInContext((server) -> {
            String lineData = "plug,room=6A watts=\"1\" " + day1 + "\n";
            send(server, lineData, "plug");
            lineData = "plug,room=6B watts=\"22\" " + day2 + "\n" +
                    "plug,room=6C watts=\"333\" " + day3 + "\n";

            SqlException exception = sendWithAlterStatement(
                    server,
                    lineData,
                    "plug",
                    WAIT_ALTER_TABLE_RELEASE | WAIT_ENGINE_TABLE_RELEASE,
                    false,
                    "ALTER TABLE plug DROP PARTITION LIST '1970-01-01'");

            Assert.assertNull(exception);
            String expected = "room\twatts\ttimestamp\n" +
                    "6B\t22\t1970-02-02T00:00:00.000000Z\n" +
                    "6C\t333\t1970-03-03T00:00:00.000000Z\n";
            assertTable(expected, "plug");
        });
    }

    @Test
    public void testAlterCommandSequenceReleased() throws Exception {
        long day1 = 0;
        long day2 = IntervalUtils.parseFloorPartialDate("1970-02-02") * 1000;
        runInContext((server) -> {
            String lineData = "plug,room=6A watts=\"1\" " + day1 + "\n";
            send(server, lineData, "plug");
            lineData = "plug,room=6B watts=\"22\" " + day2 + "\n";

            for(int i = 0; i < 10; i++) {
                SqlException exception = sendWithAlterStatement(
                        server,
                        lineData,
                        "plug",
                        WAIT_ALTER_TABLE_RELEASE | WAIT_ENGINE_TABLE_RELEASE,
                        false,
                        "ALTER TABLE plug add column col" + i + " int");
                Assert.assertNull(exception);
            }
            String expected = "room\twatts\ttimestamp\tcol0\tcol1\tcol2\tcol3\tcol4\tcol5\tcol6\tcol7\tcol8\tcol9\n" +
                    "6A\t1\t1970-01-01T00:00:00.000000Z\tNaN\tNaN\tNaN\tNaN\tNaN\tNaN\tNaN\tNaN\tNaN\tNaN\n" +
                    "6B\t22\t1970-02-02T00:00:00.000000Z\tNaN\tNaN\tNaN\tNaN\tNaN\tNaN\tNaN\tNaN\tNaN\tNaN\n" +
                    "6B\t22\t1970-02-02T00:00:00.000000Z\tNaN\tNaN\tNaN\tNaN\tNaN\tNaN\tNaN\tNaN\tNaN\tNaN\n" +
                    "6B\t22\t1970-02-02T00:00:00.000000Z\tNaN\tNaN\tNaN\tNaN\tNaN\tNaN\tNaN\tNaN\tNaN\tNaN\n" +
                    "6B\t22\t1970-02-02T00:00:00.000000Z\tNaN\tNaN\tNaN\tNaN\tNaN\tNaN\tNaN\tNaN\tNaN\tNaN\n" +
                    "6B\t22\t1970-02-02T00:00:00.000000Z\tNaN\tNaN\tNaN\tNaN\tNaN\tNaN\tNaN\tNaN\tNaN\tNaN\n" +
                    "6B\t22\t1970-02-02T00:00:00.000000Z\tNaN\tNaN\tNaN\tNaN\tNaN\tNaN\tNaN\tNaN\tNaN\tNaN\n" +
                    "6B\t22\t1970-02-02T00:00:00.000000Z\tNaN\tNaN\tNaN\tNaN\tNaN\tNaN\tNaN\tNaN\tNaN\tNaN\n" +
                    "6B\t22\t1970-02-02T00:00:00.000000Z\tNaN\tNaN\tNaN\tNaN\tNaN\tNaN\tNaN\tNaN\tNaN\tNaN\n" +
                    "6B\t22\t1970-02-02T00:00:00.000000Z\tNaN\tNaN\tNaN\tNaN\tNaN\tNaN\tNaN\tNaN\tNaN\tNaN\n" +
                    "6B\t22\t1970-02-02T00:00:00.000000Z\tNaN\tNaN\tNaN\tNaN\tNaN\tNaN\tNaN\tNaN\tNaN\tNaN\n";
            assertTable(expected, "plug");
        });
    }

    @Test
    public void testAlterCommandTableMetaModifications() throws Exception {
        runInContext((server) -> {
            String lineData = "plug,label=Power,room=6A watts=\"1\" 2631819999000\n" +
                    "plug,label=Power,room=6B watts=\"22\" 1631817902842\n" +
                    "plug,label=Line,room=6C watts=\"333\" 1531817902842\n";

            SqlException exception = sendWithAlterStatement(
                    server,
                    lineData,
                    "plug",
                    WAIT_ALTER_TABLE_RELEASE | WAIT_ENGINE_TABLE_RELEASE,
                    false,
                    "ALTER TABLE plug SET PARAM commitLag = 20s;");
            Assert.assertNull(exception);

            exception = sendWithAlterStatement(
                    server,
                    lineData,
                    "plug",
                    WAIT_ALTER_TABLE_RELEASE | WAIT_ENGINE_TABLE_RELEASE,
                    false,
                    "ALTER TABLE plug SET PARAM maxUncommittedRows = 1;");
            Assert.assertNull(exception);

            SqlException exception3 = sendWithAlterStatement(
                    server,
                    lineData,
                    "plug",
                    WAIT_ALTER_TABLE_RELEASE | WAIT_ENGINE_TABLE_RELEASE,
                    false,
                    "alter table plug alter column label nocache;");
            Assert.assertNull(exception3);

            assertTable("label\troom\twatts\ttimestamp\n" +
                            "Line\t6C\t333\t1970-01-01T00:25:31.817902Z\n" +
                            "Line\t6C\t333\t1970-01-01T00:25:31.817902Z\n" +
                            "Line\t6C\t333\t1970-01-01T00:25:31.817902Z\n" +
                            "Power\t6B\t22\t1970-01-01T00:27:11.817902Z\n" +
                            "Power\t6B\t22\t1970-01-01T00:27:11.817902Z\n" +
                            "Power\t6B\t22\t1970-01-01T00:27:11.817902Z\n" +
                            "Power\t6A\t1\t1970-01-01T00:43:51.819999Z\n" +
                            "Power\t6A\t1\t1970-01-01T00:43:51.819999Z\n" +
                            "Power\t6A\t1\t1970-01-01T00:43:51.819999Z\n",
                    "plug");

            engine.releaseAllReaders();
            try (TableReader reader = engine.getReader(AllowAllCairoSecurityContext.INSTANCE, "plug")) {
                TableReaderMetadata meta = reader.getMetadata();
                Assert.assertEquals(1, meta.getMaxUncommittedRows());
                Assert.assertEquals(20 * 1_000_000L, meta.getCommitLag());
                Assert.assertFalse(reader.getSymbolMapReader(meta.getColumnIndex("label")).isCached());
            }
        });
    }

    private void assertTable(CharSequence expected, CharSequence tableName) {
        try (TableReader reader = engine.getReader(AllowAllCairoSecurityContext.INSTANCE, tableName)) {
            assertCursorTwoPass(expected, reader.getCursor(), reader.getMetadata());
        }
    }

    @FunctionalInterface
    public interface LineTcpServerAwareContext {
        void run(LineTcpReceiver receiver);
    }

    private SqlException sendWithAlterStatement(LineTcpReceiver server, String lineData, String tableName, int wait, boolean noLinger, String alterTableCommand) {
        sqlException = null;
        int countDownCount = 1;
        if ((wait & WAIT_ENGINE_TABLE_RELEASE) != 0 && (wait & WAIT_ALTER_TABLE_RELEASE) != 0) {
            countDownCount++;
        }
        SOCountDownLatch releaseLatch = new SOCountDownLatch(countDownCount);
        CyclicBarrier startBarrier = new CyclicBarrier(2);

        if (alterTableCommand != null && (wait & WAIT_ALTER_TABLE_RELEASE) != 0) {
            new Thread(() -> {
                boolean waited = false;
                // Wait in parallel thead
                try {
                    startBarrier.await();
                    waited = true;
                    LOG.info().$("Busy waiting for writer ASYNC event ").$(alterCommandId).$();
                    sqlException = AlterCommandExecution.waitWriterEvent(
                            engine,
                            alterCommandId,
                            scSequence,
                            10_000_000,  // 1s
                            -1
                    );
                } catch (Throwable e) {
                    LOG.error().$(e).$();
                } finally {
                    // exit this method if alter executed
                    releaseLatch.countDown();
                    if (waited) {
                        LOG.info().$("Stopped waiting for writer ASYNC event").$();
                        // If subscribed to global writer event queue, unsubscribe here
                        AlterCommandExecution.stopEngineAsyncWriterEventWait(engine, scSequence);
                    }
                }
            }).start();
        }

        if (wait != WAIT_NO_WAIT) {
            engine.setPoolListener((factoryType, thread, name, event, segment, position) -> {
                if (Chars.equalsNc(tableName, name)) {
                    if ((wait & WAIT_ENGINE_TABLE_RELEASE) != 0 || (wait & WAIT_ALTER_TABLE_RELEASE) != 0) {
                        if (factoryType == PoolListener.SRC_WRITER) {
                            switch (event) {
                                case PoolListener.EV_RETURN:
                                    LOG.info().$("EV_RETURN ").$(name).$();
                                    releaseLatch.countDown();
                                    break;
                                case PoolListener.EV_CREATE:
                                case PoolListener.EV_GET:
                                    LOG.info().$("EV_GET ").$(name).$();
                                    if (alterTableCommand != null) {
                                        try {
                                            // Execute ALTER in parallel thread
                                            alterCommandId = executeAlterSql(alterTableCommand);
                                            startBarrier.await();
                                        } catch (BrokenBarrierException | InterruptedException e) {
                                            e.printStackTrace();
                                            releaseLatch.countDown();
                                        } catch (SqlException e) {
                                            sqlException = e;
                                            releaseLatch.countDown();
                                        }
                                    }
                                    break;
                            }
                        }
                    } else {
                        releaseLatch.countDown();
                    }
                }
            });
        }

        try {
            int ipv4address = Net.parseIPv4("127.0.0.1");
            long sockaddr = Net.sockaddr(ipv4address, bindPort);
            long fd = Net.socketTcp(true);
            try {
                TestUtils.assertConnect(fd, sockaddr, noLinger);
                byte[] lineDataBytes = lineData.getBytes(StandardCharsets.UTF_8);
                long bufaddr = Unsafe.malloc(lineDataBytes.length, MemoryTag.NATIVE_DEFAULT);
                try {
                    for (int n = 0; n < lineDataBytes.length; n++) {
                        Unsafe.getUnsafe().putByte(bufaddr + n, lineDataBytes[n]);
                    }
                    int rc = Net.send(fd, bufaddr, lineDataBytes.length);
                    Assert.assertEquals(lineDataBytes.length, rc);
                } finally {
                    Unsafe.free(bufaddr, lineDataBytes.length, MemoryTag.NATIVE_DEFAULT);
                }
            } finally {
                Net.close(fd);
                Net.freeSockAddr(sockaddr);
            }
            if (wait != WAIT_NO_WAIT) {
                releaseLatch.await();
                return sqlException;
            }
        } finally {
            switch (wait) {
                case WAIT_ENGINE_TABLE_RELEASE:
                case WAIT_ALTER_TABLE_RELEASE:
                    engine.setPoolListener(null);
                    break;
                case WAIT_ILP_TABLE_RELEASE:
                    server.setSchedulerListener(null);
                    break;
            }
            AlterCommandExecution.stopEngineAsyncWriterEventWait(engine, scSequence);
        }
        return null;
    }

    private long executeAlterSql(String sql) throws SqlException {
        // Subscribe local writer even queue to the global engine writer response queue
        AlterCommandExecution.setUpEngineAsyncWriterEventWait(engine, scSequence);
        LOG.info().$("Started waiting for writer ASYNC event").$();
        try (SqlCompiler compiler = new SqlCompiler(engine);
             SqlExecutionContext sqlExecutionContext = new SqlExecutionContextImpl(engine, 1)
                     .with(
                             AllowAllCairoSecurityContext.INSTANCE,
                             new BindVariableServiceImpl(configuration),
                             null,
                             -1,
                             null
                     )
        ) {
            CompiledQuery cc = compiler.compile(sql, sqlExecutionContext);
            AlterStatement alterStatement = cc.getAlterStatement();
            assert alterStatement != null;

            return cc.executeAsyncNoWait();
        }
    }

    private void send(LineTcpReceiver receiver, String lineData, String tableName) throws Exception {
        SqlException ex = sendWithAlterStatement(receiver, lineData, tableName, WAIT_ENGINE_TABLE_RELEASE, true, null);
        if (ex != null) {
            throw ex;
        }
    }
}
