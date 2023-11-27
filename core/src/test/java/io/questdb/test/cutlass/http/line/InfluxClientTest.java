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
                int port = serverMain.getConfiguration().getHttpServerConfiguration().getDispatcherConfiguration().getBindPort();
                final String serverURL = "http://127.0.0.1:" + port, username = "root", password = "root";
                long timestamp = IntervalUtils.parseFloorPartialTimestamp("2023-11-27T18:53:24.834Z");
                int count = 100;
                try (final InfluxDB influxDB = InfluxDBFactory.connect(serverURL, username, password)) {

                    BatchPoints batchPoints = BatchPoints
                            .database("test_db")
                            .tag("async", "true")
                            .build();

                    for (int i = 0; i < count; i++) {
                        batchPoints.point(Point.measurement("h2o_feet")
                                .time(timestamp, TimeUnit.MICROSECONDS)
                                .tag("location", "santa_monica")
                                .addField("level description", "below 3 feet")
                                .addField("water_level", i * 2.064d)
                                .build());
                    }

                    influxDB.write(batchPoints);
                }

                serverMain.waitWalTxnApplied("h2o_feet", 1);
                serverMain.assertSql("SELECT count() FROM h2o_feet", "count\n" + count + "\n");
            }
        });
    }
}
