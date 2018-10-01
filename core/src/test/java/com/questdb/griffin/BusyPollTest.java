/*******************************************************************************
 *    ___                  _   ____  ____
 *   / _ \ _   _  ___  ___| |_|  _ \| __ )
 *  | | | | | | |/ _ \/ __| __| | | |  _ \
 *  | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *   \__\_\\__,_|\___||___/\__|____/|____/
 *
 * Copyright (C) 2014-2018 Appsicle
 *
 * This program is free software: you can redistribute it and/or  modify
 * it under the terms of the GNU Affero General Public License, version 3,
 * as published by the Free Software Foundation.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 *
 ******************************************************************************/

package com.questdb.griffin;

import com.questdb.cairo.*;
import com.questdb.cairo.sql.Record;
import com.questdb.griffin.engine.functions.bind.BindVariableService;
import com.questdb.std.Os;
import com.questdb.std.Rnd;
import com.questdb.std.Unsafe;
import org.HdrHistogram.Histogram;
import org.junit.Assert;
import org.junit.Test;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.TimeUnit;

public class BusyPollTest extends AbstractCairoTest {
    private final static Engine engine = new Engine(configuration);
    private final static SqlCompiler compiler = new SqlCompiler(engine, configuration);
    private final static BindVariableService bindVariableService = new BindVariableService();
    private static final Histogram HISTOGRAM = new Histogram(TimeUnit.SECONDS.toNanos(10), 3);

    @Test
    public void testPoll() throws SqlException, InterruptedException {
        compiler.compile("create table xyz (sequence INT, event BINARY, ts LONG)", bindVariableService);
        final int N = 3_000_000;
        final int blobSize = 128;

        CyclicBarrier barrier = new CyclicBarrier(2);
        CountDownLatch latch = new CountDownLatch(2);

        new Thread(() -> {
            try (TableWriter writer = engine.getWriter("xyz")) {
                Os.setCurrentThreadAffinity(1);
                barrier.await();
                long addr = Unsafe.malloc(blobSize);
                try {
                    Rnd rnd = new Rnd();
                    for (int i = 0; i < N; i++) {
                        TableWriter.Row row = writer.newRow(0);
                        row.putLong(0, i);
                        for (int k = 0; k < blobSize; k += 4) {
                            Unsafe.getUnsafe().putInt(addr + k, rnd.nextInt());
                        }
                        row.putBin(1, addr, blobSize);
                        row.putLong(2, System.nanoTime());
                        row.append();
                        writer.commit();
                    }
                } finally {
                    Unsafe.free(addr, blobSize);
                }
            } catch (Exception e) {
                e.printStackTrace();
            } finally {
                latch.countDown();
            }
        }).start();

        new Thread(() -> {
            try (TableReader reader = engine.getReader("xyz", 0)) {
                Os.setCurrentThreadAffinity(2);
                int count = 0;
                final TableReaderIncrementalRecordCursor cursor = new TableReaderIncrementalRecordCursor();
                cursor.of(reader);
                final Record record = cursor.getRecord();
                barrier.await();
                while (count < N) {
                    if (cursor.reload()) {
                        while (cursor.hasNext()) {
                            long timestamp = record.getLong(2);
                            if (count > 1_000_000) {
                                HISTOGRAM.recordValue(System.nanoTime() - timestamp);
                            }
                            count++;
                        }
                    }
                }
            } catch (Exception e) {
                e.printStackTrace();
            } finally {
                latch.countDown();
            }
        }).start();

        Assert.assertTrue(latch.await(600, TimeUnit.SECONDS));
        HISTOGRAM.outputPercentileDistribution(System.out, 1000.0);
    }
}
