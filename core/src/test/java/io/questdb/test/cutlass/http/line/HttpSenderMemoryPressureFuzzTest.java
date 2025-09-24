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
import io.questdb.cairo.CairoEngine;
import io.questdb.cairo.CairoException;
import io.questdb.cairo.ErrorTag;
import io.questdb.cairo.TableToken;
import io.questdb.cairo.wal.seq.TableSequencerAPI;
import io.questdb.client.Sender;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.std.Rnd;
import io.questdb.std.Unsafe;
import io.questdb.test.AbstractBootstrapTest;
import io.questdb.test.TestServerMain;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.concurrent.TimeUnit;

public class HttpSenderMemoryPressureFuzzTest extends AbstractBootstrapTest {
    private static final Log LOG = LogFactory.getLog(HttpSenderMemoryPressureFuzzTest.class);

    @Before
    public void setUp() {
        super.setUp();
        TestUtils.unchecked(() -> createDummyConfiguration());
        dbPath.parent().$();
    }

    @Test
    public void testMemoryPressureSingleSender() {
        final String tn = "table1";
        final long hourAsMillis = 3_600_000L;
        final long numPartitions = 150L;
        final Rnd rnd = TestUtils.generateRandom(LOG);
        final int additionalLoad = rnd.nextInt(3) * 100_000;

        try (TestServerMain serverMain = startWithEnvVariables(
                PropertyKey.HTTP_RECEIVE_BUFFER_SIZE.getEnvVarName(), "4096",
                // let's make CAIRO_WAL_MAX_LAG_SIZE a fraction of our max RSS
                // it's 75MB by default and that's fraction of RSS in the real world
                // this test has max RSS in 10s of MB, so let's make the MAX LAG size a fraction of that
                PropertyKey.CAIRO_WAL_MAX_LAG_SIZE.getEnvVarName(), "10",
                PropertyKey.WAL_APPLY_WORKER_COUNT.getEnvVarName(), "1")
        ) {
            serverMain.start();
            serverMain.execute("create table " + tn +
                    "(b byte, s short, i int, l long, f float, d double, v varchar, sym symbol, tss timestamp, ts timestamp" +
                    ") timestamp(ts) partition by HOUR WAL");

            // Warm up server apply jobs to allocate necessary resources
            serverMain.compile("insert into " + tn + "(b, ts) values (1, '2023-01-01T00:00:00Z')");
            serverMain.awaitTable(tn);

            int port = serverMain.getHttpServerPort();
            try (Sender sender = Sender.builder(Sender.Transport.HTTP)
                    .address("localhost:" + port)
                    .build()
            ) {
                CairoEngine engine = serverMain.getEngine();
                TableToken tableToken = engine.verifyTableName(tn);
                TableSequencerAPI sequencer = engine.getTableSequencerAPI();
                for (int j = 0; j < 5; j++) {
                    sequencer.suspendTable(tableToken, ErrorTag.OUT_OF_MEMORY, "test");
                    for (int i = 0; i < 200_000 + additionalLoad; i++) {
                        sender.table(tn)
                                .symbol("sym", rnd.nextString(2))
                                .longColumn("b", rnd.nextByte())
                                .longColumn("s", rnd.nextShort())
                                .longColumn("i", rnd.nextInt(1000))
                                .doubleColumn("f", rnd.nextFloat())
                                .doubleColumn("d", rnd.nextDouble())
                                .stringColumn("v", rnd.nextString(50))
                                .timestampColumn("tss", Instant.ofEpochMilli(rnd.nextLong() / 1000))
                                .at(rnd.nextLong(numPartitions * hourAsMillis), ChronoUnit.MILLIS);
                    }
                    sender.flush();
                    try {
                        Unsafe.setRssMemLimit(58 * (1L << 20));
                        try {
                            sequencer.resumeTable(tableToken, 0);
                            engine.awaitTable(tn, 10, TimeUnit.MINUTES);
                        } finally {
                            Unsafe.setRssMemLimit(0);
                        }
                    } catch (CairoException e) {
                        if (!e.getMessage().contains("table is suspended [tableName=table1]")) {
                            e.printStackTrace(System.err);
                            Assert.fail("The only accepted error is 'table is suspended [tableName=table1]', but got: " + e.getMessage());
                        }
                        System.out.printf("\n%s\n\n", e.getMessage());
                        break;
                    }
                }
            }
        }
    }
}
