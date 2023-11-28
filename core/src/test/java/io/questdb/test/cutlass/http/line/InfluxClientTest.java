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

package io.questdb.test.cutlass.http.line;

import io.questdb.PropertyKey;
import io.questdb.griffin.model.IntervalUtils;
import io.questdb.std.NumericException;
import io.questdb.std.ObjList;
import io.questdb.test.AbstractBootstrapTest;
import io.questdb.test.TestServerMain;
import io.questdb.test.tools.TestUtils;
import org.influxdb.InfluxDB;
import org.influxdb.InfluxDBFactory;
import org.influxdb.dto.BatchPoints;
import org.influxdb.dto.Point;
import org.junit.Before;
import org.junit.Test;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

public class InfluxClientTest extends AbstractBootstrapTest {
    @Before
    public void setUp() {
        super.setUp();
        TestUtils.unchecked(() -> createDummyConfiguration());
        dbPath.parent().$();
    }

    @Test
    public void testInsertWithIlpHttp() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables(
                    PropertyKey.HTTP_RECEIVE_BUFFER_SIZE.getEnvVarEquivalent(), "2048"
            )) {
                serverMain.start();

                String tableName = "h2o_feet";
                int count = 9250;

                int httpPort = serverMain.getConfiguration().getHttpServerConfiguration().getDispatcherConfiguration().getBindPort();
                sendIlp(tableName, count, httpPort);

                serverMain.waitWalTxnApplied(tableName, 1);
                serverMain.assertSql("SELECT count() FROM h2o_feet", "count\n" + count + "\n");
                serverMain.assertSql("SELECT sum(water_level) FROM h2o_feet", "sum\n" + (count * (count - 1) / 2) + "\n");
            }
        });
    }

    @Test
    public void testInsertWithIlpHttpParallel() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables(
                    PropertyKey.HTTP_RECEIVE_BUFFER_SIZE.getEnvVarEquivalent(), "2048"
            )) {
                serverMain.start();

                String tableName = "h2o_feet";
                int count = 10_000;

                int threads = 5;
                ObjList<Thread> threadList = new ObjList<>();
                int httpPort = serverMain.getConfiguration().getHttpServerConfiguration().getDispatcherConfiguration().getBindPort();
                AtomicReference<Throwable> error = new AtomicReference<>();

                for (int i = 0; i < threads; i++) {
                    final int threadNo = i;
                    threadList.add(new Thread(() -> {
                        try {
                            sendIlp(tableName + threadNo, count, httpPort);
                        } catch (Throwable e) {
                            e.printStackTrace();
                            error.set(e);
                        }
                    }));
                    threadList.getLast().start();
                }

                for (int i = 0; i < threads; i++) {
                    threadList.getQuick(i).join();
                }

                LOG.info().$("== all threads finished ==").$();

                if (error.get() != null) {
                    throw new RuntimeException(error.get());
                }

                for (int i = 0; i < threads; i++) {
                    String tn = "h2o_feet" + i;
                    serverMain.waitWalTxnApplied(tn, 1);
                    serverMain.assertSql("SELECT count() FROM " + tn, "count\n" + count + "\n");
                    serverMain.assertSql("SELECT sum(water_level) FROM " + tn, "sum\n" + (count * (count - 1) / 2) + "\n");
                }
            }
        });
    }

    private static void sendIlp(String tableName, int count, int port) throws NumericException {
        final String serverURL = "http://127.0.0.1:" + port, username = "root", password = "root";
        long timestamp = IntervalUtils.parseFloorPartialTimestamp("2023-11-27T18:53:24.834Z");
        int i = 0;

        try (final InfluxDB influxDB = InfluxDBFactory.connect(serverURL, username, password)) {

            BatchPoints batchPoints = BatchPoints
                    .database("test_db")
                    .tag("async", "true")
                    .build();

            for (; i < count / 2; i++) {
                batchPoints.point(Point.measurement(tableName)
                        .time(timestamp, TimeUnit.MICROSECONDS)
                        .tag("location", "santa_monica")
                        .addField("level description", "below 3 feet asd fasd fasfd asdf asdf asdfasdf asdf asdfasdfas dfads".substring(0, i % 68))
                        .addField("water_level", i)
                        .build());
            }
            influxDB.write(batchPoints);

            BatchPoints batchPoints2 = BatchPoints
                    .database("test_db")
                    .tag("async", "true")
                    .build();
            for (; i < count; i++) {
                batchPoints2.point(Point.measurement(tableName)
                        .time(timestamp, TimeUnit.MICROSECONDS)
                        .tag("location", "santa_monica")
                        .addField("level description", "below 3 feet asd fasd fasfd asdf asdf asdfasdf asdf asdfasdfas dfads".substring(0, i % 68))
                        .addField("water_level", i)
                        .build());
            }

            influxDB.write(batchPoints2);
        }
    }
}
