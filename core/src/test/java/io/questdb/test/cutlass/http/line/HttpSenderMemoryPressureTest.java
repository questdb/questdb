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

package io.questdb.test.cutlass.http.line;

import io.questdb.PropertyKey;
import io.questdb.cairo.CairoException;
import io.questdb.client.Sender;
import io.questdb.std.Rnd;
import io.questdb.test.AbstractBootstrapTest;
import io.questdb.test.TestServerMain;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.time.Instant;
import java.time.temporal.ChronoUnit;

public class HttpSenderMemoryPressureTest extends AbstractBootstrapTest {

    @Before
    public void setUp() {
        super.setUp();
        TestUtils.unchecked(() -> createDummyConfiguration());
        dbPath.parent().$();
    }

    @Test
    public void testMemoryPressure() throws Exception {
        String tn = "table1";
        Rnd rnd = new Rnd();
        TestUtils.assertMemoryLeak(1, () -> {
            try (TestServerMain serverMain = startWithEnvVariables(
                    PropertyKey.HTTP_RECEIVE_BUFFER_SIZE.getEnvVarName(), "2048")
            ) {
                serverMain.start();
                serverMain.compile("create table " + tn +
                        "(b byte, s short, i int, l long, f float, d double, v varchar, sym symbol, tss timestamp, ts timestamp" +
                        ") timestamp(ts) partition by HOUR WAL");

                int port = serverMain.getHttpServerPort();
                try (Sender sender = Sender.builder(Sender.Transport.HTTP)
                        .address("localhost:" + port)
                        .build()
                ) {
                    for (int j = 0; j < 10; j++) {
                        for (int i = 0; i < 1_000_000; i++) {
                            sender.table(tn)
                                    .symbol("sym", rnd.nextString(2))
                                    .longColumn("b", rnd.nextByte())
                                    .longColumn("s", rnd.nextShort())
                                    .longColumn("i", rnd.nextInt(1000))
                                    .doubleColumn("f", rnd.nextFloat())
                                    .doubleColumn("d", rnd.nextDouble())
                                    .stringColumn("v", rnd.nextString(50))
                                    .timestampColumn("tss", Instant.ofEpochMilli(rnd.nextLong()))
                                    .at(rnd.nextLong(1000L * 3600 * 1000), ChronoUnit.MILLIS);
                        }
                        sender.flush();
                    }
                    try {
                        serverMain.awaitTable(tn);
                    } catch (CairoException e) {
                        if (!e.getMessage().contains("suspended")) {
                            Assert.fail("The only accepted error is 'table suspended'");
                        }
                    }
                }
            }
        });
    }
}
