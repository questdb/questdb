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

package io.questdb.test.cutlass.line.tcp;

import io.questdb.cairo.*;
import io.questdb.cairo.pool.PoolListener;
import io.questdb.cairo.sql.OperationFuture;
import io.questdb.griffin.CompiledQuery;
import io.questdb.griffin.SqlCompiler;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.engine.ops.AlterOperation;
import io.questdb.griffin.model.IntervalUtils;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.mp.SCSequence;
import io.questdb.mp.SOCountDownLatch;
import io.questdb.mp.Sequence;
import io.questdb.network.Net;
import io.questdb.std.*;
import io.questdb.std.datetime.microtime.Timestamps;
import io.questdb.std.str.Path;
import io.questdb.std.str.StringSink;
import io.questdb.test.cairo.TableModel;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.nio.charset.StandardCharsets;
import java.util.LinkedList;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

public class AlterWalTableLineTcpReceiverTest extends AbstractLineTcpReceiverTest {
    private final static Log LOG = LogFactory.getLog(AlterWalTableLineTcpReceiverTest.class);

    private final SCSequence scSequence = new SCSequence();
    private SqlException sqlException;

    @Before
    public void setUp() {
        configOverrideDefaultTableWriteMode(SqlWalMode.WAL_ENABLED);
        super.setUp();
    }

    @Test
    public void testAlterCommandAddColumn() throws Exception {
        runInContext((server) -> {
            String lineData = "plug,room=6A watts=\"1\" 2631819999000\n" +
                    "plug,room=6B watts=\"22\" 1631817902842\n" +
                    "plug,room=6C watts=\"333\" 1531817902842\n";

            SqlException exception = sendWithAlterStatement(lineData,
                    "ALTER TABLE plug ADD COLUMN label2 INT",
                    false, 1);
            Assert.assertNull(exception);

            lineData = "plug,label=Power,room=6A watts=\"4\" 2631819999000\n" +
                    "plug,label=Power,room=6B watts=\"55\" 1631817902842\n" +
                    "plug,label=Line,room=6C watts=\"666\" 1531817902842\n";
            send(lineData);

            drainWalQueue();

            String expected = "room\twatts\ttimestamp\tlabel2\tlabel\n" +
                    "6C\t666\t1970-01-01T00:25:31.817902Z\tNaN\tLine\n" +
                    "6C\t333\t1970-01-01T00:25:31.817902Z\tNaN\t\n" +
                    "6B\t55\t1970-01-01T00:27:11.817902Z\tNaN\tPower\n" +
                    "6B\t22\t1970-01-01T00:27:11.817902Z\tNaN\t\n" +
                    "6A\t4\t1970-01-01T00:43:51.819999Z\tNaN\tPower\n" +
                    "6A\t1\t1970-01-01T00:43:51.819999Z\tNaN\t\n";
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
                    LOG.info().$("sender finished").$();
                    Path.clearThreadLocals();
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
                try (
                        SqlCompiler compiler = new SqlCompiler(engine);
                        SqlExecutionContext sqlExecutionContext = TestUtils.createSqlExecutionCtx(engine)
                ) {
                    CompiledQuery cc = compiler.compile("ALTER TABLE plug DROP PARTITION WHERE timestamp > 0", sqlExecutionContext);
                    try (OperationFuture result = cc.execute(scSequence)) {
                        result.await();
                        Assert.assertEquals(OperationFuture.QUERY_COMPLETE, result.getStatus());
                    }
                } catch (SqlException e) {
                    partitionDropperProblem.set(e);
                } finally {
                    Path.clearThreadLocals();
                    // a few rows may have made it into the active partition,
                    // as dropping it is concurrent with inserting
                    keepSending.set(false);
                }

            }, "partition-dropper");
            partitionDropper.start();

            ilpProducerHalted.await();

            drainWalQueue();

            Assert.assertNull(ilpProducerProblem.get());
            Assert.assertNull(partitionDropperProblem.get());
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
                tm.wal();
                TableToken ignored = TestUtils.create(tm, engine);
            }

            try (TableWriterAPI writer = getTableWriterAPI("plug")) {
                TableWriter.Row row = writer.newRow(day1 / 1000);
                row.putSym(0, "6A");
                row.putLong(1, 100L);
                row.append();
                writer.commit();
            }
            drainWalQueue();

            String lineData = "plug,room=6A watts=1i " + day1 + "\n" +
                    "plug,room=6B watts=37i " + day1 + "\n" +
                    "plug,room=7G watts=21i " + day1 + "\n" +
                    "plug,room=1C watts=11i " + day1 + "\n";
            SqlException exception = sendWithAlterStatement(lineData,
                    "ALTER TABLE plug DROP PARTITION LIST '2023-02-27'",
                    false, 1);
            Assert.assertNull(exception);
            drainWalQueue();

            assertTable("room\twatts\ttimestamp\n");

            lineData = "plug,room=6A watts=125i " + day1 + "\n";
            send(lineData);
            drainWalQueue();

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
            SqlException exception = sendWithAlterStatement(lineData,
                    "ALTER TABLE plug DROP PARTITION LIST '1970-01-01'",
                    false, 1);
            Assert.assertNull(exception);

            drainWalQueue();

            String expected = "room\twatts\ttimestamp\n" +
                    "6B\t22\t1970-02-02T00:00:00.000000Z\n" +
                    "6C\t333\t1970-03-03T00:00:00.000000Z\n";
            assertTable(expected);
        }, true, 250);
    }

    @Test
    public void testAlterCommandRenameAndReAddColumnDifferentType() throws Exception {
        runInContext((server) -> {
            String lineData = "plug,label=Power,room=6A watts=\"1\" 2631819999000\n" +
                    "plug,label=Power,room=6B watts=\"22\" 1631817902842\n" +
                    "plug,label=Line,room=6C watts=\"333\" 1531817902842\n";

            SqlException exception = sendWithAlterStatement(lineData,
                    "ALTER TABLE plug RENAME COLUMN label TO label2",
                    false, 1);
            Assert.assertNull(exception);

            lineData = "plug,room=6A watts=\"4\",label=0i 2631819999001\n" +
                    "plug,room=6B watts=\"55\",label=0i 1631817902843\n" +
                    "plug,room=6C watts=\"666\",label=1i 1531817902843\n";

            // re-send, this should re-add column label, but with integer type
            send(lineData);
            drainWalQueue();

            String expected = "label2\troom\twatts\ttimestamp\tlabel\n" +
                    "\t6C\t666\t1970-01-01T00:25:31.817902Z\t1\n" +
                    "Line\t6C\t333\t1970-01-01T00:25:31.817902Z\tNaN\n" +
                    "\t6B\t55\t1970-01-01T00:27:11.817902Z\t0\n" +
                    "Power\t6B\t22\t1970-01-01T00:27:11.817902Z\tNaN\n" +
                    "\t6A\t4\t1970-01-01T00:43:51.819999Z\t0\n" +
                    "Power\t6A\t1\t1970-01-01T00:43:51.819999Z\tNaN\n";
            assertTable(expected);
        }, false, 1000);
    }

    @Test
    public void testAlterCommandRenameAndReAddColumnSameType() throws Exception {
        runInContext((server) -> {
            String lineData = "plug,label=Power,room=6A watts=\"1\" 2631819999000\n" +
                    "plug,label=Power,room=6B watts=\"22\" 1631817902842\n" +
                    "plug,label=Line,room=6C watts=\"333\" 1531817902842\n";

            SqlException exception = sendWithAlterStatement(lineData,
                    "ALTER TABLE plug RENAME COLUMN label TO label2",
                    false, 1);
            Assert.assertNull(exception);

            lineData = "plug,label=Power,room=6A watts=\"4\" 2631819999001\n" +
                    "plug,label=Power,room=6B watts=\"55\" 1631817902843\n" +
                    "plug,label=Line,room=6C watts=\"666\" 1531817902843\n";

            // re-send, this should re-add column label
            send(lineData);
            drainWalQueue();

            String expected = "label2\troom\twatts\ttimestamp\tlabel\n" +
                    "\t6C\t666\t1970-01-01T00:25:31.817902Z\tLine\n" +
                    "Line\t6C\t333\t1970-01-01T00:25:31.817902Z\t\n" +
                    "\t6B\t55\t1970-01-01T00:27:11.817902Z\tPower\n" +
                    "Power\t6B\t22\t1970-01-01T00:27:11.817902Z\t\n" +
                    "\t6A\t4\t1970-01-01T00:43:51.819999Z\tPower\n" +
                    "Power\t6A\t1\t1970-01-01T00:43:51.819999Z\t\n";
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
                SqlException exception = sendWithAlterStatement(lineData,
                        "ALTER TABLE plug add column col" + i + " int",
                        false, 1);
                Assert.assertNull(exception);
            }
            drainWalQueue();

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

            SqlException exception = sendWithAlterStatement(lineData,
                    "ALTER TABLE plug SET PARAM o3MaxLag = 20s;",
                    false, 1);
            Assert.assertNull(exception);

            exception = sendWithAlterStatement(lineData,
                    "ALTER TABLE plug SET PARAM maxUncommittedRows = 1;",
                    false, 1);
            Assert.assertNull(exception);

            SqlException exception3 = sendWithAlterStatement(lineData,
                    "alter table plug alter column label nocache;",
                    false, 1);
            Assert.assertNull(exception3);

            drainWalQueue();

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
    public void testAlterCommandTruncateTable() throws Exception {
        long day1 = IntervalUtils.parseFloorPartialTimestamp("2023-02-27") * 1000;
        long day2 = IntervalUtils.parseFloorPartialTimestamp("2023-02-28") * 1000;
        runInContext((server) -> {
            final AtomicLong ilpProducerWatts = new AtomicLong(0L);
            final AtomicBoolean keepSending = new AtomicBoolean(true);
            final AtomicReference<Throwable> ilpProducerProblem = new AtomicReference<>();
            final SOCountDownLatch ilpProducerHalted = new SOCountDownLatch(1);
            final AtomicReference<SqlException> partitionDropperProblem = new AtomicReference<>();
            try (
                    SqlCompiler compiler = new SqlCompiler(engine);
                    SqlExecutionContext sqlExecutionContext = TestUtils.createSqlExecutionCtx(engine)
            ) {

                compiler.compile("CREATE TABLE plug as " +
                        " (select cast(x as symbol) room, rnd_long() as watts, timestamp_sequence('2023-02-27', 1000) timestamp from long_sequence(100)) " +
                        "timestamp(timestamp) partition by DAY WAL ", sqlExecutionContext);

                drainWalQueue();
                engine.releaseInactive();

                final Thread ilpProducer = new Thread(() -> {
                    String lineTpt = "plug,room=%d watts=%di %d%n";
                    try {
                        while (keepSending.get()) {
                            try {
                                long watts = ilpProducerWatts.getAndIncrement();
                                long day = (watts + 1) % 4 == 0 ? day1 : day2;
                                long room = watts % 20;
                                String lineData = String.format(lineTpt, room, watts, day);
                                send(lineData);
                                LOG.info().$("sent: ").$(lineData).$();
                            } catch (Throwable unexpected) {
                                ilpProducerProblem.set(unexpected);
                                keepSending.set(false);
                                break;
                            }
                        }
                    } finally {
                        LOG.info().$("sender finished").$();
                        Path.clearThreadLocals();
                        ilpProducerHalted.countDown();
                    }
                }, "ilp-producer");
                ilpProducer.start();


                final Thread partitionDropper = new Thread(() -> {
                    while (ilpProducerWatts.get() < 20) {
                        Os.pause();
                    }
                    LOG.info().$("ABOUT TO TRUNCATE TABLE").$();
                    try {
                        CompiledQuery cc = compiler.compile("TRUNCATE TABLE plug", sqlExecutionContext);
                        try (OperationFuture result = cc.execute(scSequence)) {
                            result.await();
                            Assert.assertEquals(OperationFuture.QUERY_COMPLETE, result.getStatus());
                        }
                        Os.sleep(100);
                    } catch (SqlException e) {
                        partitionDropperProblem.set(e);
                    } finally {
                        Path.clearThreadLocals();
                        // a few rows may have made it into the active partition,
                        // as dropping it is concurrent with inserting
                        keepSending.set(false);
                    }

                }, "partition-dropper");
                partitionDropper.start();

                ilpProducerHalted.await();
                drainWalQueue();

                if (ilpProducerProblem.get() != null) {
                    throw new RuntimeException(ilpProducerProblem.get());
                }
                if (partitionDropperProblem.get() != null) {
                    throw new RuntimeException(partitionDropperProblem.get());
                }

                // Check can read data without exceptions.
                // Data can be random, no invariant to check.
                TestUtils.printSql(compiler, sqlExecutionContext, "select * from plug", sink);
            }
        }, true, 50L);
    }

    @Test
    public void testAlterTableAddIndex() throws Exception {
        runInContext((server) -> {
            String lineData = "plug,label=Power,room=6A watts=\"1\" 2631819999000\n" +
                    "plug,label=Power,room=6B watts=\"22\" 1631817902842\n" +
                    "plug,label=Line,room=6C watts=\"333\" 1531817902842\n";
            SqlException ex = sendWithAlterStatement(lineData,
                    "ALTER TABLE plug ALTER COLUMN label ADD INDEX",
                    false, 1);
            Assert.assertNull(ex);

            drainWalQueue();

            String expected = "label\troom\twatts\ttimestamp\n" +
                    "Line\t6C\t333\t1970-01-01T00:25:31.817902Z\n" +
                    "Power\t6B\t22\t1970-01-01T00:27:11.817902Z\n" +
                    "Power\t6A\t1\t1970-01-01T00:43:51.819999Z\n";
            assertTable(expected);
            try (TableReader rdr = getReader("plug")) {
                TableReaderMetadata metadata = rdr.getMetadata();
                Assert.assertTrue(
                        "Alter makes column indexed",
                        metadata.isColumnIndexed(metadata.getColumnIndex("label"))
                );
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

            try (OperationFuture alterOperationFuture = executeAlterSql("ALTER TABLE plug DROP COLUMN watts")) {
                alterOperationFuture.await(10 * Timestamps.SECOND_MILLIS);
                Assert.assertEquals(OperationFuture.QUERY_COMPLETE, alterOperationFuture.getStatus());
                // We don't drain WAL queue after this call to verify that TableUpdateDetails uses
                // sequencer metadata, not table metadata.
            }

            send("plug,room=6A watts=\"1\",watts=2,power=220 2631819999000\n");
            drainWalQueue();

            String expected = "room\tpower\ttimestamp\twatts\n" +
                    "6C\t220.0\t1970-01-01T00:25:31.817902Z\t\n" +
                    "6B\tNaN\t1970-01-01T00:27:11.817902Z\t\n" +
                    "6A\t220.0\t1970-01-01T00:43:51.819999Z\t\n" +
                    "6A\t220.0\t1970-01-01T00:43:51.819999Z\t1\n";
            assertTable(expected);
        });
    }

    @Test
    public void testDropColumnConcurrently() throws Exception {
        final int rows = 10_000;
        runInContext((server) -> {
            String lineData = "plug,room=0i watts=\"1\",power=220 2631819999000\n";
            // pre-create the table
            send(lineData);

            StringBuilder sb = new StringBuilder();
            for (int i = 0; i < rows; i++) {
                sb.append("plug,room=")
                        .append(i % 100)
                        .append("i watts=\"2\",power=220 ")
                        .append(2631819999000L + 1000 * i)
                        .append('\n');
            }
            lineData = sb.toString();
            SqlException exception = sendWithAlterStatement(lineData,
                    "ALTER TABLE plug DROP COLUMN room",
                    true, 1);
            Assert.assertNull(exception);
            drainWalQueue();

            // The outcome of this test is non-deterministic, i.e. watts column may or may not
            // be present in the table. But in any case we expect all rows to be inserted.
            assertTableSize(10001);
        });
    }

    @Test
    public void testDropColumnConcurrentlyManyAttempts() throws Exception {
        final int rows = 10_000;
        runInContext((server) -> {
            String lineData = "plug,room=0i watts=\"1\",power=220 2631819999000\n";
            // pre-create the table
            send(lineData);

            StringBuilder sb = new StringBuilder();
            for (int i = 0; i < rows; i++) {
                sb.append("plug,room=")
                        .append(i % 100)
                        .append("i watts=\"2\",power=220 ")
                        .append(2631819999000L)
                        .append('\n');
            }
            lineData = sb.toString();
            SqlException exception = sendWithAlterStatement(lineData,
                    "ALTER TABLE plug DROP COLUMN room",
                    false, 3);
            Assert.assertNull(exception);

            // The outcome of this test is non-deterministic, i.e. watts column may or may not
            // be present in the table. But in any case we expect all rows to be inserted.
            assertTableSize(rows + 1);
        });
    }

    @Test
    public void testDropColumnInTheMiddle() throws Exception {
        runInContext((server) -> {
            String lineData = "plug,room=6A watts=\"1\",power=220 2631819999000\n" +
                    "plug,room=6B watts=\"22\" 1631817902842\n" +
                    "plug,room=6C watts=\"333\",power=220 1531817902842\n";
            send(lineData);

            try (OperationFuture alterOperationFuture = executeAlterSql("ALTER TABLE plug DROP COLUMN watts")) {
                alterOperationFuture.await(10 * Timestamps.SECOND_MILLIS);
                Assert.assertEquals(OperationFuture.QUERY_COMPLETE, alterOperationFuture.getStatus());
            }

            // Send same data again
            send(lineData);
            drainWalQueue();

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
                    int dropCol = columnsAdded.get(rnd.nextPositiveInt() % columnsAdded.size());
                    try (OperationFuture alterOperationFuture = executeAlterSql("ALTER TABLE plug DROP COLUMN column_" + Math.abs(dropCol))) {
                        alterOperationFuture.await(10 * Timestamps.SECOND_MILLIS);
                        Assert.assertEquals(OperationFuture.QUERY_COMPLETE, alterOperationFuture.getStatus());
                        columnsAdded.remove((Object) dropCol);
                    }
                }
            }
            drainWalQueue();

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
    public void testRenameColumnConcurrently() throws Exception {
        final int rows = 10_000;
        runInContext((server) -> {
            String lineData = "plug,room=0i watts=\"1\",power=220 2631819999000\n";
            // pre-create the table
            send(lineData);

            StringBuilder sb = new StringBuilder();
            for (int i = 0; i < rows; i++) {
                sb.append("plug,room=")
                        .append(i % 100)
                        .append("i watts=\"2\",power=220 ")
                        .append(2631819999000L + 1000 * i)
                        .append('\n');
            }
            lineData = sb.toString();
            SqlException exception = sendWithAlterStatement(lineData,
                    "ALTER TABLE plug RENAME COLUMN room TO old_room",
                    true, 1);
            Assert.assertNull(exception);
            drainWalQueue();

            // The outcome of this test is non-deterministic, i.e. watts column may or may not
            // be present in the table. But in any case we expect all rows to be inserted.
            assertTableSize(10001);
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

            try (OperationFuture alterOperationFuture = executeAlterSql("ALTER TABLE plug DROP COLUMN room")) {
                alterOperationFuture.await(10 * Timestamps.SECOND_MILLIS);
                Assert.assertEquals(OperationFuture.QUERY_COMPLETE, alterOperationFuture.getStatus());
            }

            // Send same data again
            send(
                    "plug watts=\"1\",power=220 2631819999000\n" +
                            "plug,room=6BB watts=\"22\" 1631817902842\n" +
                            "plug,room=6C watts=\"333\",power=220 1531817902842\n"
            );
            drainWalQueue();

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
                SqlExecutionContext sqlExecutionContext = TestUtils.createSqlExecutionCtx(engine)
        ) {
            CompiledQuery cc = compiler.compile(sql, sqlExecutionContext);
            AlterOperation alterOp = cc.getAlterOperation();
            assert alterOp != null;

            return cc.execute(scSequence);
        }
    }

    private void send(String lineData) throws Exception {
        SqlException ex = sendWithoutAlterStatement(lineData);
        if (ex != null) {
            throw ex;
        }
    }

    private SqlException sendWithAlterStatement(String lineData, String alterTableCommand, boolean sendAlterConcurrently, int alterAttempts) {
        sqlException = null;
        int countDownCount = 3;
        SOCountDownLatch releaseAllLatch = new SOCountDownLatch(countDownCount);
        SOCountDownLatch getFirstLatch = new SOCountDownLatch(1);

        engine.setPoolListener((factoryType, thread, name, event, segment, position) -> {
            if (Chars.equalsNc("plug", name.getTableName())) {
                if (PoolListener.isWalOrWriter(factoryType)) {
                    if (event == PoolListener.EV_GET) {
                        LOG.info().$("EV_GET ").$(name).$();
                        getFirstLatch.countDown();
                    }
                    if (event == PoolListener.EV_RETURN) {
                        LOG.info().$("EV_RETURN ").$(name).$();
                        releaseAllLatch.countDown();
                    }
                }
            }
        });

        final long walPublisherCurrent = engine.getMessageBus().getWalTxnNotificationPubSequence().current();
        for (int at = 0; at < alterAttempts; at++) {
            new Thread(() -> {
                OperationFuture alterOperationFuture = null;
                try {
                    LOG.info().$("Busy waiting for txn notification event").$();
                    if (sendAlterConcurrently) {
                        if (alterAttempts == 1) {
                            // Wait for a writer to be obtained from the pool by an ILP I/O thread.
                            getFirstLatch.await(10 * Timestamps.SECOND_MILLIS);
                        }
                    } else {
                        // Wait for the next txn notification which would mean an INSERT.
                        Sequence txnPubSequence = engine.getMessageBus().getWalTxnNotificationPubSequence();
                        while (txnPubSequence.current() == walPublisherCurrent) {
                            Os.pause();
                        }
                    }
                    alterOperationFuture = executeAlterSql(alterTableCommand);
                    alterOperationFuture.await(10 * Timestamps.SECOND_MILLIS);
                } catch (Throwable e) {
                    if (alterAttempts == 1) {
                        if (e instanceof SqlException) {
                            sqlException = (SqlException) e;
                        }
                        LOG.error().$(e).$();
                    }
                } finally {
                    LOG.info().$("Stopped waiting for txn notification event").$();
                    Path.clearThreadLocals();
                    // If subscribed to global writer event queue, unsubscribe here
                    Misc.free(alterOperationFuture);
                    // exit this method if alter executed
                    releaseAllLatch.countDown();
                }
            }).start();
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
                    int sent = 0;
                    Rnd rnd = TestUtils.generateRandom(LOG);
                    while (sent < lineDataBytes.length) {
                        int rc = Net.send(fd, bufaddr + sent, Math.min(lineDataBytes.length - sent, 1024));
                        sent += rc;
                        if (sent < lineDataBytes.length && rnd.nextDouble() < 0.1) {
                            Os.sleep(1);
                        }
                    }
                } finally {
                    Unsafe.free(bufaddr, lineDataBytes.length, MemoryTag.NATIVE_DEFAULT);
                }
            } finally {
                Net.close(fd);
                Net.freeSockAddr(sockaddr);
            }
            releaseAllLatch.await();
            return sqlException;
        } finally {
            engine.setPoolListener(null);
        }
    }

    private SqlException sendWithoutAlterStatement(String lineData) {
        sqlException = null;
        SOCountDownLatch releaseLatch = new SOCountDownLatch(1);

        engine.setPoolListener((factoryType, thread, name, event, segment, position) -> {
            if (PoolListener.isWalOrWriter(factoryType)
                    && (event == PoolListener.EV_RETURN)
                    && Chars.equalsNc("plug", name.getTableName())) {
                LOG.info().$("EV_RETURN ").$(name).$();
                releaseLatch.countDown();
            }
        });

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
            releaseLatch.await();
            return sqlException;
        } finally {
            engine.setPoolListener(null);
        }
    }

    protected void assertTableSize(int expected) {
        try (TableReader reader = getReader("plug")) {
            TestUtils.assertEventually(() -> {
                drainWalQueue();
                reader.reload();
                Assert.assertEquals(expected, reader.size());
            }, 20);
        }
    }
}
