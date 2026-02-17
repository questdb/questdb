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

package io.questdb.compat;

import io.questdb.ServerMain;
import io.questdb.griffin.SqlException;
import io.questdb.mp.SOCountDownLatch;
import io.questdb.std.Misc;
import io.questdb.std.Os;
import org.influxdb.BatchOptions;
import org.influxdb.InfluxDB;
import org.influxdb.dto.Point;
import org.junit.Test;

import java.time.Instant;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

public class InfluxDBClientStreamingTest extends AbstractTest {

    @Test
    public void testInfluxStreamingRetriesOnServerRestart() {
        ServerMain serverMain = null;
        try {
            serverMain = ServerMain.create(root);
            serverMain.start();
            AtomicInteger pointCounter = new AtomicInteger();
            SOCountDownLatch countDownLatch = new SOCountDownLatch(1);
            int batchSize = 1000;
            AtomicBoolean stop = new AtomicBoolean(false);
            String tableName = "abc";
            AtomicReference<Throwable> sendException = new AtomicReference<>();

            streamLinesAsync(serverMain, tableName, pointCounter, countDownLatch, batchSize, stop, sendException);
            countDownLatch.await();
            countDownLatch.setCount(1);

            while (pointCounter.get() < batchSize && sendException.get() == null) {
                Os.pause();
            }
            if (sendException.get() != null) {
                throw new RuntimeException(sendException.get());
            }

            LOG.info().$("=== Restarting server...").$();
            serverMain = Misc.free(serverMain);
            serverMain = ServerMain.create(root);
            serverMain.start();
            int pointsAfterRestart = pointCounter.get();

            // Wait more points to be sent after restart
            while (pointCounter.get() == pointsAfterRestart && sendException.get() == null) {
                Os.pause();
            }
            if (sendException.get() != null) {
                throw new RuntimeException(sendException.get());
            }

            // Stop and wait sending thread to finish
            stop.set(true);
            countDownLatch.await();

            if (sendException.get() != null) {
                throw new RuntimeException(sendException.get());
            }

            final ServerMain server = serverMain;
            assertEventually(() -> {
                // InfluxDB client can send the last batch of points
                // after the client object is closed from the background thread.
                server.awaitTable(tableName);
                try {
                    assertSql(server.getEngine(),
                            "select count(*) from " + tableName,
                            "count()\n" + pointCounter.get() + "\n"
                    );
                } catch (SqlException e) {
                    throw new RuntimeException(e);
                }
            });
        } finally {
            Misc.free(serverMain);
        }
    }

    private void streamLinesAsync(
            ServerMain serverMain,
            String tableName,
            AtomicInteger pointCounter,
            SOCountDownLatch countDownLatch,
            int batchSize,
            AtomicBoolean stop,
            AtomicReference<Throwable> exception
    ) {
        new Thread(() -> {
            int points = 0;
            try {
                try (final InfluxDB influxDB = InfluxDBUtils.getConnection(serverMain)) {
                    influxDB.setLogLevel(InfluxDB.LogLevel.BASIC);

                    countDownLatch.countDown();
                    influxDB.enableBatch(
                            BatchOptions.DEFAULTS
                                    .actions(batchSize)
                                    .bufferLimit(Integer.MAX_VALUE)
                                    .exceptionHandler((p, t) -> {
                                        LOG.error().$("Error sending points ").$(p).$(" ").$(t).$();
                                    })
                    );

                    while (!stop.get()) {
                        try {
                            Point point = Point.measurement(tableName)
                                    .tag("tag1", "value1")
                                    .addField("value", 55.15d)
                                    .time(Instant.now().minusSeconds(-10).toEpochMilli(), TimeUnit.MILLISECONDS)
                                    .build();

                            influxDB.write(point);
                            points++;
                            pointCounter.incrementAndGet();
                        } catch (Throwable e) {
                            LOG.error().$("Error sending points ").$(e).$();
                            exception.set(e);
                            break;
                        }
                    }
                    influxDB.flush();
                }
            } finally {
                LOG.info().$("=== Sent ").$(points).$(" points").$();
                countDownLatch.countDown();
            }
        }).start();
    }
}
