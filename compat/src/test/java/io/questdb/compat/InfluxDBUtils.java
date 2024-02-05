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

package io.questdb.compat;

import io.questdb.ServerMain;
import io.questdb.test.cutlass.http.line.LineHttpUtils;
import io.questdb.test.tools.TestUtils;
import org.influxdb.InfluxDB;
import org.influxdb.InfluxDBException;
import org.influxdb.InfluxDBFactory;
import org.jetbrains.annotations.NotNull;
import org.junit.Assert;

import java.util.List;

public class InfluxDBUtils {

    public static void assertRequestErrorContains(InfluxDB influxDB, List<String> points, String line, String... errors) {
        points.add(line);
        try {
            influxDB.write(points);
            Assert.fail();
        } catch (InfluxDBException e) {
            for (String error : errors) {
                TestUtils.assertContains(e.getMessage(), error);
            }
        }
        points.clear();
    }

    public static void assertRequestOk(InfluxDB influxDB, List<String> points, String line) {
        points.add(line);
        influxDB.write(points);
        points.clear();
    }

    @NotNull
    public static InfluxDB getConnection(ServerMain serverMain) {
        int httpPort = LineHttpUtils.getHttpPort(serverMain);
        final String serverURL = "http://127.0.0.1:" + httpPort, username = "root", password = "root";
        return InfluxDBFactory.connect(serverURL, username, password);
    }
}
