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

package com.questdb.cairo;

import com.questdb.cairo.sql.Record;
import com.questdb.griffin.SqlCompiler;
import com.questdb.griffin.SqlException;
import com.questdb.griffin.engine.functions.bind.BindVariableService;
import com.questdb.std.Rnd;
import com.questdb.std.Unsafe;
import org.HdrHistogram.Histogram;
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
        final int N = 5_000_000;

        CyclicBarrier barrier = new CyclicBarrier(2);
        CountDownLatch latch = new CountDownLatch(2);

        new Thread(() -> {
            try (TableWriter writer = engine.getWriter("xyz")) {
//                Os.setCurrentThreadAffinity(3);
                barrier.await();
                long addr = Unsafe.malloc(128);
                Rnd rnd = new Rnd();
                for (int i = 0; i < N; i++) {
                    TableWriter.Row row = writer.newRow(0);
                    row.putLong(0, i);
                    for (int k = 0; k < 128; k += 4) {
                        Unsafe.getUnsafe().putInt(addr + k, rnd.nextInt());
                    }
                    row.putBin(1, addr, 128);
                    row.putLong(2, System.nanoTime());
                    row.append();
                    writer.commit();
                }
                System.out.println("WRITER QUIT");
            } catch (Exception e) {
                e.printStackTrace();
            } finally {
                latch.countDown();
            }
        }).start();

        new Thread(() -> {
            try (TableReader reader = engine.getReader("xyz", 0)) {
//                Os.setCurrentThreadAffinity(1);
                int count = 0;
                long lastRowid = -1;
                final TableReaderRecordCursor cursor = reader.getCursor();
                final Record record = cursor.getRecord();
                long txn = TableUtils.INITIAL_TXN;

                barrier.await();
                System.out.println("START");
                while (count < N) {
                    if (reader.reload() || reader.getTxn() > txn) {
                        txn = reader.getTxn();
//                        cursor.toTop();
                        if (lastRowid > -1) {
//                            System.out.println("starting from: "+lastRowid);
                            cursor.startFrom(lastRowid);
                        } else {
                            cursor.toTop();
                        }
                        while (cursor.hasNext()) {
//                            if (record.getRowId() <= lastRowid) {
//                                System.out.println("oops: "+record.getRowId());
//                            }
                            assert record.getRowId() > lastRowid;
                            long timestamp = record.getTimestamp(2);
                            if (count > 2_000_000 && count < 5_000_000) {
                                HISTOGRAM.recordValue(System.nanoTime() - timestamp);
                            }
                            count++;
                        }
                        lastRowid = record.getRowId();
//                        System.out.println("lastRowid: "+lastRowid +", count: "+count);
                    }
                }
                System.out.println("READER QUIT");
            } catch (Exception e) {
                e.printStackTrace();
            } finally {
                latch.countDown();
            }
        }).start();


        latch.await(600, TimeUnit.SECONDS);

        HISTOGRAM.outputPercentileDistribution(System.out, 1000.0);
    }
}
