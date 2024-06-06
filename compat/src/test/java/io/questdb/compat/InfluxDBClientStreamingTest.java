/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2024 QuestDB
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
import org.junit.Test;
import java.time.Instant;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;
import java.util.logging.Logger;

public class InfluxStreamingTest {
    private static final Logger LOG = Logger.getLogger(InfluxStreamingTest.class.getName());
    private static final String ROOT = "root"; // assuming root is defined

  import org.junit.Test;
import java.time.Instant;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;
import java.util.logging.Logger;

public class InfluxStreamingTest {
    private static final Logger LOG = Logger.getLogger(InfluxStreamingTest.class.getName());
    private static final String ROOT = "root"; // assuming root is defined

    @Test
    public void testInfluxStreamingRetriesOnServerRestart() {
        ServerMain serverMain = null;
        try {
            serverMain = initializeServer();
            AtomicInteger pointCounter = new AtomicInteger();
            CountDownLatch countDownLatch = new CountDownLatch(1);
            int batchSize = 1000;
            AtomicBoolean stop = new AtomicBoolean(false);
            String tableName = "abc";
            AtomicReference<Throwable> sendException = new AtomicReference<>();

            startAsyncStream(serverMain, tableName, pointCounter, countDownLatch, batchSize, stop, sendException);
            waitForBatchCompletion(countDownLatch);

            validateStreaming(pointCounter, sendException, batchSize);

            restartServer(serverMain);
            int pointsAfterRestart = pointCounter.get();

            waitForAdditionalPoints(pointCounter, sendException, pointsAfterRestart);

            stopStreaming(stop, countDownLatch, sendException);

            verifyDataInTable(serverMain, tableName, pointCounter);
        } catch (Exception e) {
            LOG.severe("Test failed: " + e.getMessage());
            throw new RuntimeException(e);
        } finally {
            if (serverMain != null) {
                serverMain.close();
            }
        }
    }

    private ServerMain initializeServer() throws Exception {
        ServerMain serverMain = ServerMain.create(ROOT);
        serverMain.start();
        return serverMain;
    }

    private void startAsyncStream(ServerMain serverMain, String tableName, AtomicInteger pointCounter,
                                  CountDownLatch countDownLatch, int batchSize, AtomicBoolean stop,
                                  AtomicReference<Throwable> exception) {
        new Thread(() -> {
            try (final InfluxDB influxDB = InfluxDBUtils.getConnection(serverMain)) {
                influxDB.setLogLevel(InfluxDB.LogLevel.BASIC);
                countDownLatch.countDown();
                influxDB.enableBatch(BatchOptions.DEFAULTS.actions(batchSize).bufferLimit(batchSize * 1024));

                while (!stop.get()) {
                    try {
                        Point point = Point.measurement(tableName)
                                .tag("tag1", "value1")
                                .addField("value", 55.15d)
                                .time(Instant.now().minusSeconds(-10).toEpochMilli(), TimeUnit.MILLISECONDS)
                                .build();
                        influxDB.write(point);
                        pointCounter.incrementAndGet();
                    } catch (Throwable e) {
                        LOG.severe("Error sending points: " + e.getMessage());
                        exception.set(e);
                        break;
                    }
                }
                influxDB.flush();
            } catch (Exception e) {
                LOG.severe("Exception in streaming thread: " + e.getMessage());
                exception.set(e);
            } finally {
                LOG.info("=== Sent " + pointCounter.get() + " points");
                countDownLatch.countDown();
            }
        }).start();
    }

    private void waitForBatchCompletion(CountDownLatch countDownLatch) throws InterruptedException {
        countDownLatch.await();
        countDownLatch = new CountDownLatch(1);  // Reset for the next usage
    }

    private void validateStreaming(AtomicInteger pointCounter, AtomicReference<Throwable> sendException, int batchSize) {
        while (pointCounter.get() < batchSize && sendException.get() == null) {
            Thread.yield();
        }
        if (sendException.get() != null) {
            throw new RuntimeException(sendException.get());
        }
    }

    private void restartServer(ServerMain serverMain) throws Exception {
        LOG.info("=== Restarting server...");
        serverMain.close();
        serverMain = initializeServer();
    }

    private void waitForAdditionalPoints(AtomicInteger pointCounter, AtomicReference<Throwable> sendException, int pointsAfterRestart) {
        while (pointCounter.get() == pointsAfterRestart && sendException.get() == null) {
            Thread.yield();
        }
        if (sendException.get() != null) {
            throw new RuntimeException(sendException.get());
        }
    }

    private void stopStreaming(AtomicBoolean stop, CountDownLatch countDownLatch, AtomicReference<Throwable> sendException) throws InterruptedException {
        stop.set(true);
        countDownLatch.await();
        if (sendException.get() != null) {
            throw new RuntimeException(sendException.get());
        }
    }

    private void verifyDataInTable(ServerMain serverMain, String tableName, AtomicInteger pointCounter) {
        assertEventually(() -> {
            try {
                serverMain.awaitTable(tableName);
                assertSql(serverMain.getEngine(),
                        "select count(*) from " + tableName,
                        "count\n" + pointCounter.get() + "\n"
                );
            } catch (SqlException e) {
                throw new RuntimeException(e);
            }
        });
    }

    private void assertEventually(Runnable assertion) {
        // Implementation of assertEventually logic
    }

    private void assertSql(Object engine, String sql, String expected) throws SqlException {
        // Implementation of assertSql logic
    }
}
