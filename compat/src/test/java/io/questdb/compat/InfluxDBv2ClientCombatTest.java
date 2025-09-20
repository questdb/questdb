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

import com.influxdb.client.InfluxDBClient;
import com.influxdb.client.WriteApi;
import com.influxdb.client.domain.WritePrecision;
import io.questdb.PropertyKey;
import io.questdb.ServerMain;
import io.questdb.cairo.NanosTimestampDriver;
import org.junit.Test;

import java.util.HashMap;

public class InfluxDBv2ClientCombatTest extends AbstractTest {
    @Test
    public void testTimestampPrecisionSupport() throws Exception {
        try (final ServerMain serverMain = ServerMain.create(root, new HashMap<>() {{
            put(PropertyKey.HTTP_RECEIVE_BUFFER_SIZE.getEnvVarName(), "2048");
        }})) {
            serverMain.start();

            try (final InfluxDBClient client = InfluxDBUtils.getV2Connection(serverMain);
                 WriteApi writeApi = client.makeWriteApi()) {
                long nanoTime = NanosTimestampDriver.floor("2022-02-24T06:00:00.000001");
                String record = "t1,location=north value=60.0 " + nanoTime;
                writeApi.writeRecord("my-bucket", "my-org", WritePrecision.NS, record);

                long microTime = NanosTimestampDriver.floor("2022-02-24T04:00:00.000001Z") / 1000L;
                String record1 = "t1,location=north value=60.0 " + microTime;
                writeApi.writeRecord("my-bucket", "my-org", WritePrecision.US, record1);

                long milliTime = NanosTimestampDriver.floor("2022-02-24T05:00:00.001001Z") / 1000_000L;
                String record2 = "t1,location=north value=60.0 " + milliTime;
                writeApi.writeRecord("my-bucket", "my-org", WritePrecision.MS, record2);

                long secondTime = NanosTimestampDriver.floor("2022-02-24T07:00:01") / 1000_000_000L;
                String record3 = "t1,location=north value=60.0 " + secondTime;
                writeApi.writeRecord("my-bucket", "my-org", WritePrecision.S, record3);
            }

            serverMain.awaitTable("t1");
            assertSql(
                    serverMain.getEngine(),
                    "SELECT * FROM t1", "location\tvalue\ttimestamp\n" +
                            "north\t60.0\t2022-02-24T04:00:00.000001Z\n" +
                            "north\t60.0\t2022-02-24T05:00:00.001000Z\n" +
                            "north\t60.0\t2022-02-24T06:00:00.000001Z\n" +
                            "north\t60.0\t2022-02-24T07:00:01.000000Z\n"
            );
        }
    }
}
