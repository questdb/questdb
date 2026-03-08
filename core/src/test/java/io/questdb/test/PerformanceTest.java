/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2026 QuestDB
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

package io.questdb.test;

import io.questdb.cairo.ColumnType;
import io.questdb.cairo.PartitionBy;
import io.questdb.cairo.TableReader;
import io.questdb.cairo.TableWriter;
import io.questdb.cairo.sql.Record;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.mp.SOCountDownLatch;
import io.questdb.std.NumericException;
import io.questdb.std.Rnd;
import io.questdb.std.datetime.millitime.DateFormatUtils;
import io.questdb.test.cairo.TableModel;
import io.questdb.test.cairo.TestTableReaderRecordCursor;
import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;

import java.util.concurrent.TimeUnit;

public class PerformanceTest extends AbstractCairoTest {

    private static final Log LOG = LogFactory.getLog(PerformanceTest.class);
    private static final int TEST_DATA_SIZE = 1_000_000;
    private long timeoutResult;

    @Test
    public void testCairoPartitionedReaderReloadSpeed() throws Exception {
        assertMemoryLeak(() -> {
            int operations = 1_00_000 * 100;
            double speed = measureReloadSpeed(1_00_000, operations, 100000);

            // Add 10x slowdown for slow / busy build server.
            Assert.assertTrue("Total reload should be around 300 ms", TimeUnit.NANOSECONDS.toMillis((long) (operations * speed)) < 30000);
        });
    }

    @Test
    public void testCairoPerformance() throws Exception {
        assertMemoryLeak(() -> {
            int count = 10;
            long t = 0;
            long result;

            String[] symbols = {"AGK.L", "BP.L", "TLW.L", "ABF.L", "LLOY.L", "BT-A.L", "WTB.L", "RRS.L", "ADM.L", "GKN.L", "HSBA.L"};
            TableModel model = new TableModel(configuration, "quote", PartitionBy.NONE)
                    .timestamp()
                    .col("sym", ColumnType.SYMBOL)
                    .col("bid", ColumnType.DOUBLE)
                    .col("ask", ColumnType.DOUBLE)
                    .col("bidSize", ColumnType.INT)
                    .col("askSize", ColumnType.INT)
                    .col("mode", ColumnType.SYMBOL).symbolCapacity(2)
                    .col("ex", ColumnType.SYMBOL).symbolCapacity(2);
            AbstractCairoTest.create(model);

            try (TableWriter writer = newOffPoolWriter(configuration, "quote")) {
                for (int i = -count; i < count; i++) {
                    if (i == 0) {
                        t = System.nanoTime();
                    }
                    writer.truncate();
                    long timestamp = DateFormatUtils.parseUTCDate("2013-10-05T10:00:00.000Z");
                    Rnd r = new Rnd();
                    int n = symbols.length - 1;
                    for (int i1 = 0; i1 < TEST_DATA_SIZE; i1++) {
                        TableWriter.Row row = writer.newRow(timestamp);
                        row.putSym(1, symbols[Math.abs(r.nextInt() % n)]);
                        row.putDouble(2, Math.abs(r.nextDouble()));
                        row.putDouble(3, Math.abs(r.nextDouble()));
                        row.putInt(4, Math.abs(r.nextInt()));
                        row.putInt(5, Math.abs(r.nextInt()));
                        row.putSym(6, "LXE");
                        row.putSym(7, "Fast trading");
                        row.append();
                        timestamp += 1000;
                    }
                    writer.commit();
                }
                result = System.nanoTime() - t;
            }
            long appendDuration = result / count;

            try (
                    TableReader reader = newOffPoolReader(configuration, "quote");
                    TestTableReaderRecordCursor cursor = new TestTableReaderRecordCursor().of(reader)
            ) {
                for (int i = -count; i < count; i++) {
                    if (i == 0) {
                        t = System.nanoTime();
                    }

                    Record record = cursor.getRecord();
                    while (cursor.hasNext()) {
                        record.getDate(0);
                        record.getSymA(1);
                        record.getDouble(2);
                        record.getDouble(3);
                        record.getInt(4);
                        record.getInt(5);
                        record.getSymA(6);
                        record.getSymA(7);
                    }
                }
                result = (System.nanoTime() - t) / count;
            }

            LOG.info().$("Cairo append (1M): ").$(TimeUnit.NANOSECONDS.toMillis(appendDuration)).$("ms").$();
            LOG.info().$("Cairo read (1M): ").$(TimeUnit.NANOSECONDS.toMillis(result)).$("ms").$();
        });
    }

    @Test
    @Ignore
    public void testFastestReloadIteration() throws Exception {
        assertMemoryLeak(() -> {
            double min = 100000;
            int iterations = 50;
            double avg = 0;
            for (int i = 0; i < iterations; i++) {
                double ns = measureReloadSpeed(1_00_000, 1_00_000 * 100, 100000);
                min = Math.min(min, ns);
                avg = (avg * i + ns) / (i + 1);
            }

            LOG.info().$("Min reload from ").$(iterations).$(" attempts: ").$(min).$("ns. Average: ").$(avg).$("ns").$();
            Assert.assertTrue(min < 10);
        });
    }

    private double measureReloadSpeed(int reloadTableRowCount, int reloadCount, int txCount) {
        String[] symbols = {"AGK.L", "BP.L", "TLW.L", "ABF.L", "LLOY.L", "BT-A.L", "WTB.L", "RRS.L", "ADM.L", "GKN.L", "HSBA.L"};
        TableModel model = new TableModel(configuration, "quote", PartitionBy.DAY)
                .timestamp()
                .col("sym", ColumnType.SYMBOL)
                .col("bid", ColumnType.DOUBLE)
                .col("ask", ColumnType.DOUBLE)
                .col("bidSize", ColumnType.INT)
                .col("askSize", ColumnType.INT)
                .col("mode", ColumnType.SYMBOL).symbolCapacity(2)
                .col("ex", ColumnType.SYMBOL).symbolCapacity(2);
        AbstractCairoTest.create(model);

        SOCountDownLatch stopLatch = new SOCountDownLatch(2);
        SOCountDownLatch startLatch = new SOCountDownLatch(2);
        try (
                TableWriter writer = newOffPoolWriter(configuration, "quote");
                TableReader reader = newOffPoolReader(configuration, "quote")
        ) {
            // Writing
            new Thread(() -> {
                try {
                    long timestamp = DateFormatUtils.parseUTCDate("2013-10-05T10:00:00.000Z");
                    Rnd r = new Rnd();
                    int n = symbols.length - 1;
                    int txSize = reloadTableRowCount / txCount;
                    startLatch.countDown();
                    startLatch.await();
                    for (int i = 0; i < txCount; i++) {
                        for (int i1 = 0; i1 < txSize; i1++) {
                            TableWriter.Row row = writer.newRow(timestamp);
                            row.putSym(1, symbols[Math.abs(r.nextInt() % n)]);
                            row.putDouble(2, Math.abs(r.nextDouble()));
                            row.putDouble(3, Math.abs(r.nextDouble()));
                            row.putInt(4, Math.abs(r.nextInt()));
                            row.putInt(5, Math.abs(r.nextInt()));
                            row.putSym(6, "LXE");
                            row.putSym(7, "Fast trading");
                            row.append();
                            timestamp += 1000;
                        }
                        writer.commit();
                    }
                } catch (NumericException e) {
                    e.printStackTrace();
                }
                stopLatch.countDown();
                LOG.info().$("Stopped writing").$();
            }).start();

            // Reload reader
            new Thread(() -> {
                long result = System.nanoTime();
                try {
                    startLatch.countDown();
                    startLatch.await();
                    for (int i = 0; i < reloadCount; i++) {
                        reader.reload();
                        if (reader.getPartitionCount() > 0) {
                            reader.openPartition(0);
                        }
                    }
                    result = System.nanoTime() - result;
                } finally {
                    stopLatch.countDown();
                }
                timeoutResult = result;
                LOG.info().$("reload done").$();
            }).start();

            stopLatch.await();
        }
        int million = 1_000_000;
        LOG.info().$("Cairo reload (").$(reloadCount / million).$("M) per operation: ").$(timeoutResult / reloadCount).$("ns").$();
        return (double) timeoutResult / reloadCount;
    }
}
