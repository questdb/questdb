/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2020 QuestDB
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

package io.questdb;

import io.questdb.cairo.*;
import io.questdb.std.*;
import io.questdb.std.microtime.MicrosecondClock;
import io.questdb.std.microtime.TimestampFormatUtils;
import io.questdb.std.str.Path;
import io.questdb.test.tools.*;

import org.junit.Assert;
import org.junit.Test;

import java.util.Properties;

public class TelemetryTest extends AbstractCairoTest {
    private final static FilesFacade FF = FilesFacadeImpl.INSTANCE;

    @Test
    public void testTelemetryDisabledByDefault() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (CairoEngine engine = new CairoEngine(configuration, null)) {
                try (Path path = new Path()) {
                    Assert.assertEquals(TableUtils.TABLE_DOES_NOT_EXIST, TableUtils.exists(FF, path, root, "telemetry"));
                    Assert.assertEquals(TableUtils.TABLE_DOES_NOT_EXIST, TableUtils.exists(FF, path, root, "telemetry_config"));
                }
            }
        });
    }

    @Test
    public void testTelemetryCreatesTablesWhenEnabled() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (CairoEngine engine = new CairoEngine(configuration, null)) {
                final TelemetryJob telemetryJob = new TelemetryJob(serverConfiguration, engine, messageBus);

                try (Path path = new Path()) {
                    Assert.assertEquals(TableUtils.TABLE_EXISTS, TableUtils.exists(FF, path, root, "telemetry"));
                    Assert.assertEquals(TableUtils.TABLE_EXISTS, TableUtils.exists(FF, path, root, "telemetry_config"));
                }

                Misc.free(telemetryJob);
            }
        });
    }

    @Test
    public void testTelemetryStoresUpAndDownEvents() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            serverConfiguration = new PropServerConfiguration(temp.toString(), new Properties()) {
                @Override
                public CairoConfiguration getCairoConfiguration() {
                    return new DefaultCairoConfiguration(root) {
                        @Override
                        public MicrosecondClock getMicrosecondClock() {
                            try {
                                return new TestMicroClock(TimestampFormatUtils.parseDateTime("2020-06-19T10:10:00.000000Z"), 10);
                            } catch (NumericException e) {
                                throw new RuntimeException(e);
                            }
                        }
                    };
                }
            };
            configuration = serverConfiguration.getCairoConfiguration();
            messageBus = new MessageBusImpl(serverConfiguration);
            CairoEngine engine = new CairoEngine(configuration, null);
            TelemetryJob telemetryJob = new TelemetryJob(serverConfiguration, engine, messageBus);
            Misc.free(engine);
            Misc.free(telemetryJob);

            final String expected = "2020-06-19T10:10:00.010000Z\t100\n" +
                    "2020-06-19T10:10:00.020000Z\t101\n";
            assertTable(expected, "telemetry");
        });
    }

    private void assertTable(CharSequence expected, CharSequence tableName) {
        try (TableReader reader = new TableReader(configuration, tableName)) {
            assertThat(expected, reader.getCursor(), reader.getMetadata(), false);
        }
    }
}
