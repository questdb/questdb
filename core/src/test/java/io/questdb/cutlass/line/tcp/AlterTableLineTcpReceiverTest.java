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

package io.questdb.cutlass.line.tcp;

import io.questdb.cairo.*;
import io.questdb.cairo.pool.PoolListener;
import io.questdb.cairo.security.AllowAllCairoSecurityContext;
import io.questdb.cairo.sql.OperationFuture;
import io.questdb.griffin.*;
import io.questdb.griffin.engine.functions.bind.BindVariableServiceImpl;
import io.questdb.griffin.engine.ops.AlterOperation;
import io.questdb.griffin.model.IntervalUtils;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.mp.SCSequence;
import io.questdb.mp.SOCountDownLatch;
import io.questdb.network.Net;
import io.questdb.std.*;
import io.questdb.std.datetime.microtime.Timestamps;
import io.questdb.std.str.Path;
import io.questdb.std.str.StringSink;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

import java.nio.charset.StandardCharsets;
import java.util.LinkedList;
import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

public class AlterTableLineTcpReceiverTest extends AbstractLineTcpReceiverTest {
    private final static Log LOG = LogFactory.getLog(AlterTableLineTcpReceiverTest.class);

    private final SCSequence scSequence = new SCSequence();
    private volatile OperationFuture alterOperationFuture;
    private SqlException sqlException;

    @Test
    public void testAlterCommandAddColumn() throws Exception {
        runInContext((server) -> {
            String lineData = "plug,room=6A watts=\"1\" 2631819998000\n" +
                    "plug,room=6B watts=\"22\" 1631817901842\n" +
                    "plug,room=6C watts=\"333\" 1531817901842\n";

            SqlException exception = sendWithAlterStatement(
                    lineData,
                    WAIT_ALTER_TABLE_RELEASE | WAIT_ENGINE_TABLE_RELEASE,
                    "ALTER TABLE plug ADD COLUMN label2 INT");
            Assert.assertNull(exception);

            lineData = "plug,label=Power,room=6A watts=\"4\" 2631819999000\n" +
                    "plug,label=Power,room=6B watts=\"55\" 1631817902842\n" +
                    "plug,label=Line,room=6C watts=\"666\" 1531817902842\n";
            send(lineData);

            String expected = "room\twatts\ttimestamp\tlabel2\tlabel\n" +
                    "6C\t333\t1970-01-01T00:25:31.817901Z\tNaN\t\n" +
                    "6C\t666\t1970-01-01T00:25:31.817902Z\tNaN\tLine\n" +
                    "6B\t22\t1970-01-01T00:27:11.817901Z\tNaN\t\n" +
                    "6B\t55\t1970-01-01T00:27:11.817902Z\tNaN\tPower\n" +
                    "6A\t1\t1970-01-01T00:43:51.819998Z\tNaN\t\n" +
                    "6A\t4\t1970-01-01T00:43:51.819999Z\tNaN\tPower\n";
            assertTable(expected);
        });
    }

    @Test
    public void testAlterCommandDropAllPartitions() throws Exception {
        long day1 = IntervalUtils.parseFloorPartialTimestamp("2023-02-27") * 1000;
        long day2 = IntervalUtils.parseFloorPartialTimestamp("2023-02-28") * 1000;
        runInContext((server) -> {
            final AtomicLong ilpProducerWatts = new AtomicLong(0L);
            final AtomicBoolean keepSending = new AtomicBoolean(true);
            final AtomicReference<Throwable> ilpProducerProblem = new AtomicReference<>();
            final SOCountDownLatch ilpProducerHalted = new SOCountDownLatch(1);
            final SOCountDownLatch partitionDropperHalted = new SOCountDownLatch(1);

            final Thread ilpProducer = new Thread(() -> {
                String lineTpt = "plug,room=6A watts=\"%di\" %d%n";
                try {
                    while (keepSending.get()) {
                        try {
                            long watts = ilpProducerWatts.getAndIncrement();
                            long day = (watts + 1) % 4 == 0 ? day1 : day2;
                            String lineData = String.format(lineTpt, watts, day);
                            send(lineData);
                            LOG.info().$("sent: ").$(lineData).$();
                        } catch (Throwable unexpected) {
                            ilpProducerProblem.set(unexpected);
                            keepSending.set(false);
                            break;
                        }
                    }
                } finally {
                    LOG.info().$("sender has finished").$();
                    ilpProducerHalted.countDown();
                }
            }, "ilp-producer");
            ilpProducer.start();

            final AtomicReference<SqlException> partitionDropperProblem = new AtomicReference<>();

            final Thread partitionDropper = new Thread(() -> {
                while (ilpProducerWatts.get() < 20) {
                    Os.pause();
                }
                LOG.info().$("ABOUT TO DROP PARTITIONS").$();
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
                    CompiledQuery cc = compiler.compile("ALTER TABLE plug DROP PARTITION WHERE timestamp > 0", sqlExecutionContext);
                    try (OperationFuture result = cc.execute(scSequence)) {
                        result.await();
                        Assert.assertEquals(OperationFuture.QUERY_COMPLETE, result.getStatus());
                    }
                } catch (SqlException e) {
                    partitionDropperProblem.set(e);
                } finally {
                    // a few rows may have made it into the active partition,
                    // as dropping it is concurrent with inserting
                    keepSending.set(false);
                    Path.clearThreadLocals();
                    partitionDropperHalted.countDown();
                }
            }, "partition-dropper");
            partitionDropper.start();

            ilpProducerHalted.await();
            Assert.assertNull(ilpProducerProblem.get());
            Assert.assertNull(partitionDropperProblem.get());
            partitionDropperHalted.await();
        }, true, 50L);
    }

    @Test
    public void testAlterCommandDropLastPartition() throws Exception {
        runInContext((server) -> {
            long day1 = IntervalUtils.parseFloorPartialTimestamp("2023-02-27") * 1000; // <-- last partition

            try (TableModel tm = new TableModel(configuration, "plug", PartitionBy.DAY)) {
                tm.col("room", ColumnType.SYMBOL);
                tm.col("watts", ColumnType.LONG);
                tm.timestamp();

                CairoTestUtils.create(tm);
            }

            try (TableWriterAPI writer = getTableWriterAPI("plug")) {
                TableWriter.Row row = writer.newRow(day1 / 1000);
                row.putSym(0, "6A");
                row.putLong(1, 100L);
                row.append();
                writer.commit();
            }

            SqlException exception = sendWithAlterStatement(
                    "plug,room=6A watts=1i " + day1 + "\n" +
                            "plug,room=6B watts=37i " + day1 + "\n" +
                            "plug,room=7G watts=21i " + day1 + "\n" +
                            "plug,room=1C watts=11i " + day1 + "\n",
                    WAIT_ALTER_TABLE_RELEASE | WAIT_ENGINE_TABLE_RELEASE,
                    "ALTER TABLE plug DROP PARTITION LIST '2023-02-27'"
            );
            Assert.assertNull(exception);
            assertTable("room\twatts\ttimestamp\n");

            send("plug,room=6A watts=125i " + day1 + "\n");

            assertTable("room\twatts\ttimestamp\n" +
                    "6A\t125\t2023-02-27T00:00:00.000000Z\n");
        }, true, 50L);
    }

    @Test
    public void testAlterCommandDropPartition() throws Exception {
        long day1 = 0;
        long day2 = IntervalUtils.parseFloorPartialTimestamp("1970-02-02") * 1000;
        long day3 = IntervalUtils.parseFloorPartialTimestamp("1970-03-03") * 1000;
        runInContext((server) -> {
            String lineData = "plug,room=6A watts=\"1\" " + day1 + "\n";
            send(lineData);

            lineData = "plug,room=6B watts=\"22\" " + day2 + "\n" +
                    "plug,room=6C watts=\"333\" " + day3 + "\n";
            SqlException exception = sendWithAlterStatement(
                    lineData,
                    WAIT_ALTER_TABLE_RELEASE | WAIT_ENGINE_TABLE_RELEASE,
                    "ALTER TABLE plug DROP PARTITION LIST '1970-01-01'"
            );
            Assert.assertNull(exception);

            String expected = "room\twatts\ttimestamp\n" +
                    "6B\t22\t1970-02-02T00:00:00.000000Z\n" +
                    "6C\t333\t1970-03-03T00:00:00.000000Z\n";
            assertTable(expected);
        }, true, 250);
    }

    @Test
    public void testAlterCommandDropsColumn() throws Exception {
        runInContext((server) -> {
            String lineData = "plug,label=Power,room=6A watts=\"1\" 2631819994000\n" +
                    "plug,label=Power,room=6B watts=\"22\" 1631817905842\n" +
                    "plug,label=Line,room=6C watts=\"333\" 1531817906844\n";

            SqlException exception = sendWithAlterStatement(lineData,
                    WAIT_ALTER_TABLE_RELEASE | WAIT_ENGINE_TABLE_RELEASE,
                    "ALTER TABLE plug DROP COLUMN label");

            Assert.assertNotNull(exception);
            TestUtils.assertEquals("async cmd cannot change table structure while writer is busy", exception.getFlyweightMessage());
            exception = sendWithAlterStatement(
                    lineData,
                    WAIT_ALTER_TABLE_RELEASE | WAIT_ENGINE_TABLE_RELEASE,
                    "ALTER TABLE plug RENAME COLUMN label TO label2"
            );

            Assert.assertNotNull(exception);
            TestUtils.assertEquals("async cmd cannot change table structure while writer is busy", exception.getFlyweightMessage());
            lineData = "plug,label=Power,room=6A watts=\"4\" 2631819995001\n" +
                    "plug,label=Power,room=6B watts=\"55\" 1631817902845\n" +
                    "plug,label=Line,room=6C watts=\"666\" 1531817903846\n";

            // re-send, this should re-add column label
            send(lineData);

            String expected = "label\troom\twatts\ttimestamp\n" +
                    "Line\t6C\t666\t1970-01-01T00:25:31.817903Z\n" +
                    "Line\t6C\t333\t1970-01-01T00:25:31.817906Z\n" +
                    "Line\t6C\t333\t1970-01-01T00:25:31.817906Z\n" +
                    "Power\t6B\t55\t1970-01-01T00:27:11.817902Z\n" +
                    "Power\t6B\t22\t1970-01-01T00:27:11.817905Z\n" +
                    "Power\t6B\t22\t1970-01-01T00:27:11.817905Z\n" +
                    "Power\t6A\t1\t1970-01-01T00:43:51.819994Z\n" +
                    "Power\t6A\t1\t1970-01-01T00:43:51.819994Z\n" +
                    "Power\t6A\t4\t1970-01-01T00:43:51.819995Z\n";
            assertTable(expected);
        }, false, 1000);
    }

    @Test
    public void testAlterCommandSequenceReleased() throws Exception {
        long day1 = 0;
        long day2 = IntervalUtils.parseFloorPartialTimestamp("1970-02-02") * 1000;
        runInContext((server) -> {
            String lineData = "plug,room=6A watts=\"1\" " + day1 + "\n";
            send(lineData);
            lineData = "plug,room=6B watts=\"22\" " + day2 + "\n";

            for (int i = 0; i < 10; i++) {
                SqlException exception = sendWithAlterStatement(
                        lineData,
                        WAIT_ALTER_TABLE_RELEASE | WAIT_ENGINE_TABLE_RELEASE,
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
            assertTable(expected);
        }, true, 250);
    }

    @Test
    public void testAlterCommandTableMetaModifications() throws Exception {
        runInContext((server) -> {
            String lineData = "plug,label=Power,room=6A watts=\"1\" 2631819999000\n" +
                    "plug,label=Power,room=6B watts=\"22\" 1631817902842\n" +
                    "plug,label=Line,room=6C watts=\"333\" 1531817902842\n";

            SqlException exception = sendWithAlterStatement(
                    lineData,
                    WAIT_ALTER_TABLE_RELEASE | WAIT_ENGINE_TABLE_RELEASE,
                    "ALTER TABLE plug SET PARAM o3MaxLag = 20s;");
            Assert.assertNull(exception);

            exception = sendWithAlterStatement(
                    lineData,
                    WAIT_ALTER_TABLE_RELEASE | WAIT_ENGINE_TABLE_RELEASE,
                    "ALTER TABLE plug SET PARAM maxUncommittedRows = 1;");
            Assert.assertNull(exception);

            SqlException exception3 = sendWithAlterStatement(
                    lineData,
                    WAIT_ALTER_TABLE_RELEASE | WAIT_ENGINE_TABLE_RELEASE,
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
                    "Power\t6A\t1\t1970-01-01T00:43:51.819999Z\n"
            );

            engine.releaseAllReaders();
            try (TableReader reader = getReader("plug")) {
                TableReaderMetadata meta = reader.getMetadata();
                Assert.assertEquals(1, meta.getMaxUncommittedRows());
                Assert.assertEquals(20 * 1_000_000L, meta.getO3MaxLag());
                Assert.assertFalse(reader.getSymbolMapReader(meta.getColumnIndex("label")).isCached());
            }
        });
    }

    @Test
    public void testAlterTableAddIndex() throws Exception {
        runInContext((server) -> {
            String lineData = "plug,label=Power,room=6A watts=\"1\" 2631819999000\n" +
                    "plug,label=Power,room=6B watts=\"22\" 1631817902842\n" +
                    "plug,label=Line,room=6C watts=\"333\" 1531817902842\n";
            SqlException ex = sendWithAlterStatement(lineData, WAIT_ALTER_TABLE_RELEASE | WAIT_ENGINE_TABLE_RELEASE,
                    "ALTER TABLE plug ALTER COLUMN label ADD INDEX");
            Assert.assertNull(ex);

            String expected = "label\troom\twatts\ttimestamp\n" +
                    "Line\t6C\t333\t1970-01-01T00:25:31.817902Z\n" +
                    "Power\t6B\t22\t1970-01-01T00:27:11.817902Z\n" +
                    "Power\t6A\t1\t1970-01-01T00:43:51.819999Z\n";
            assertTable(expected);
            try (TableReader rdr = getReader("plug")) {
                TableReaderMetadata metadata = rdr.getMetadata();
                Assert.assertTrue("Alter makes column indexed",
                        metadata.isColumnIndexed(
                                metadata.getColumnIndex("label")
                        ));
            }
        });
    }

    @Test
    public void testDropColumnAddDuplicate() throws Exception {
        runInContext((server) -> {
            send(
                    "plug,room=6A watts=\"1\",power=220 2631819999000\n" +
                            "plug,room=6B watts=\"22\" 1631817902842\n" +
                            "plug,room=6C watts=\"333\",power=220 1531817902842\n"
            );

            try (TableWriter tableWriter = getWriter("plug")) {
                tableWriter.removeColumn("watts");
            }

            send(
                    "plug,room=6A watts=\"1\",watts=2,power=220 2631819999000\n"
            );

            String expected = "room\tpower\ttimestamp\twatts\n" +
                    "6C\t220.0\t1970-01-01T00:25:31.817902Z\t\n" +
                    "6B\tNaN\t1970-01-01T00:27:11.817902Z\t\n" +
                    "6A\t220.0\t1970-01-01T00:43:51.819999Z\t\n" +
                    "6A\t220.0\t1970-01-01T00:43:51.819999Z\t1\n";
            assertTable(expected);
        });
    }

    @Test
    public void testDropColumnInTheMiddle() throws Exception {
        runInContext((server) -> {
            String lineData = "plug,room=6A watts=\"1\",power=220 2631819999000\n" +
                    "plug,room=6B watts=\"22\" 1631817902842\n" +
                    "plug,room=6C watts=\"333\",power=220 1531817902842\n";

            send(
                    lineData
            );

            try (TableWriter tableWriter = getWriter("plug")) {
                tableWriter.removeColumn("watts");
            }

            // Send same data again
            send(
                    lineData
            );

            String expected = "room\tpower\ttimestamp\twatts\n" +
                    "6C\t220.0\t1970-01-01T00:25:31.817902Z\t333\n" +
                    "6C\t220.0\t1970-01-01T00:25:31.817902Z\t\n" +
                    "6B\tNaN\t1970-01-01T00:27:11.817902Z\t22\n" +
                    "6B\tNaN\t1970-01-01T00:27:11.817902Z\t\n" +
                    "6A\t220.0\t1970-01-01T00:43:51.819999Z\t1\n" +
                    "6A\t220.0\t1970-01-01T00:43:51.819999Z\t\n";
            assertTable(expected);
        });
    }

    @Test
    public void testRandomColumnAddedDeleted() throws Exception {
        runInContext((server) -> {
            LinkedList<Integer> columnsAdded = new LinkedList<>();

            Rnd rnd = new Rnd();
            StringSink symbols = new StringSink();
            StringSink fields = new StringSink();

            for (int i = 1; i < 30; i++) {
                if (columnsAdded.size() == 0 || rnd.nextPositiveInt() % 3 != 1) {
                    // add column
                    boolean isSymbol = rnd.nextBoolean();

                    symbols.clear();
                    for (int col : columnsAdded) {
                        if (col > 0) {
                            symbols.put(",column_").put(col).put("=").put(col);
                        }
                    }

                    fields.clear();
                    int added = 0;
                    for (int col : columnsAdded) {
                        if (col < 0) {
                            col = Math.abs(col);
                            if (!isSymbol || added++ > 0) {
                                fields.put(',');
                            }
                            fields.put("column_").put(col).put("=\"").put(col).put('\"');
                        }
                    }

                    String lineData = isSymbol
                            ? String.format("plug,column_%d=%d,iteration=%d%s %s %d\n", i, i, i % 5, symbols, fields, i * Timestamps.MINUTE_MICROS * 20 * 1000)
                            : String.format("plug,iteration=%d%s column_%d=\"%d\"%s %d\n", i % 5, symbols, i, i, fields, i * Timestamps.MINUTE_MICROS * 20 * 1000);

                    send(lineData);
                    columnsAdded.add(isSymbol ? i : -i);
                } else {
                    try (TableWriter tableWriter = getWriter("plug")) {
                        int dropCol = columnsAdded.get(rnd.nextPositiveInt() % columnsAdded.size());
                        tableWriter.removeColumn("column_" + Math.abs(dropCol));
                        columnsAdded.remove((Object) dropCol);
                    }
                }
            }

            String expected = "iteration\ttimestamp\tcolumn_24\tcolumn_25\tcolumn_26\n" +
                    "1\t1970-01-01T00:20:00.000000Z\t\t\t\n" +
                    "2\t1970-01-01T00:40:00.000000Z\t\t\t\n" +
                    "3\t1970-01-01T01:00:00.000000Z\t\t\t\n" +
                    "4\t1970-01-01T01:20:00.000000Z\t\t\t\n" +
                    "2\t1970-01-01T02:20:00.000000Z\t\t\t\n" +
                    "4\t1970-01-01T03:00:00.000000Z\t\t\t\n" +
                    "2\t1970-01-01T04:00:00.000000Z\t\t\t\n" +
                    "3\t1970-01-01T04:20:00.000000Z\t\t\t\n" +
                    "1\t1970-01-01T05:20:00.000000Z\t\t\t\n" +
                    "3\t1970-01-01T06:00:00.000000Z\t\t\t\n" +
                    "0\t1970-01-01T06:40:00.000000Z\t\t\t\n" +
                    "2\t1970-01-01T07:20:00.000000Z\t\t\t\n" +
                    "4\t1970-01-01T08:00:00.000000Z\t24\t\t\n" +
                    "0\t1970-01-01T08:20:00.000000Z\t24\t25\t\n" +
                    "1\t1970-01-01T08:40:00.000000Z\t24\t25\t26\n" +
                    "2\t1970-01-01T09:00:00.000000Z\t24\t25\t26\n";
            assertTable(expected);
        });
    }

    @Test
    public void testSymbolColumnDeletedAndAdded() throws Exception {
        runInContext((server) -> {

            send(
                    "plug,room=6A watts=\"1\",power=220 2631819999000\n" +
                            "plug,room=6B watts=\"22\" 1631817902842\n" +
                            "plug,room=6C watts=\"333\",power=220 1531817902842\n"
            );

            try (TableWriter tableWriter = getWriter("plug")) {
                tableWriter.removeColumn("room");
            }

            // Send same data again
            send(
                    "plug watts=\"1\",power=220 2631819999000\n" +
                            "plug,room=6BB watts=\"22\" 1631817902842\n" +
                            "plug,room=6C watts=\"333\",power=220 1531817902842\n"
            );

            String expected = "watts\tpower\ttimestamp\troom\n" +
                    "333\t220.0\t1970-01-01T00:25:31.817902Z\t6C\n" +
                    "333\t220.0\t1970-01-01T00:25:31.817902Z\t\n" +
                    "22\tNaN\t1970-01-01T00:27:11.817902Z\t6BB\n" +
                    "22\tNaN\t1970-01-01T00:27:11.817902Z\t\n" +
                    "1\t220.0\t1970-01-01T00:43:51.819999Z\t\n" +
                    "1\t220.0\t1970-01-01T00:43:51.819999Z\t\n";
            assertTable(expected);
        });
    }

    private void assertTable(CharSequence expected) {
        assertTable(expected, "plug");
    }

    private OperationFuture executeAlterSql(String sql) throws SqlException {
        // Subscribe local writer even queue to the global engine writer response queue
        LOG.info().$("Started waiting for writer ASYNC event").$();
        try (
                SqlCompiler compiler = new SqlCompiler(engine);
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
            AlterOperation alterOp = cc.getAlterOperation();
            assert alterOp != null;

            return cc.execute(scSequence);
        }
    }

    private void send(String lineData) throws Exception {
        SqlException ex = sendWithAlterStatement(lineData, WAIT_ENGINE_TABLE_RELEASE, null);
        if (ex != null) {
            throw ex;
        }
    }

    private SqlException sendWithAlterStatement(String lineData, int wait, String alterTableCommand) {
        sqlException = null;
        int countDownCount = 1;
        if ((wait & WAIT_ENGINE_TABLE_RELEASE) != 0 && (wait & WAIT_ALTER_TABLE_RELEASE) != 0) {
            countDownCount++;
        }
        SOCountDownLatch releaseLatch = new SOCountDownLatch(countDownCount);
        CyclicBarrier startBarrier = new CyclicBarrier(2);

        if (alterTableCommand != null && (wait & WAIT_ALTER_TABLE_RELEASE) != 0) {
            new Thread(() -> {
                // Wait in parallel thread
                try {
                    startBarrier.await();
                    LOG.info().$("Busy waiting for writer ASYNC event ").$(alterOperationFuture).$();
                    alterOperationFuture.await(25 * Timestamps.SECOND_MILLIS);
                } catch (SqlException exception) {
                    sqlException = exception;
                } catch (Throwable e) {
                    LOG.error().$(e).$();
                } finally {
                    // exit this method if alter executed
                    LOG.info().$("Stopped waiting for writer ASYNC event").$();
                    // If subscribed to global writer event queue, unsubscribe here
                    alterOperationFuture.close();
                    alterOperationFuture = null;
                    releaseLatch.countDown();
                }
            }).start();
        }

        if (wait != WAIT_NO_WAIT) {
            engine.setPoolListener((factoryType, thread, name, event, segment, position) -> {
                if (name != null && Chars.equalsNc(name.getTableName(), "plug")) {
                    if ((wait & WAIT_ENGINE_TABLE_RELEASE) != 0 || (wait & WAIT_ALTER_TABLE_RELEASE) != 0) {
                        if (PoolListener.isWalOrWriter(factoryType)) {
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
                                            alterOperationFuture = executeAlterSql(alterTableCommand);
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
            int fd = Net.socketTcp(true);
            try {
                TestUtils.assertConnect(fd, sockaddr);
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
                assert alterOperationFuture == null;
                return sqlException;
            }
        } finally {
            if (wait != WAIT_NO_WAIT) {
                engine.setPoolListener(null);
            }
        }
        return null;
    }
}
