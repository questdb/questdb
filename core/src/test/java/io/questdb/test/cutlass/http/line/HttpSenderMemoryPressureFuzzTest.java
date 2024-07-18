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
import io.questdb.cairo.TableToken;
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
    public void testMemoryPressureSingleSender() throws Exception {
        final String tn = "table1";
        final Rnd rnd = TestUtils.generateRandom(LOG);
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
                int batchSize10k = rnd.nextInt(5) + 1;
                int batchSize = batchSize10k * 10_000;
                int numIters = 2_000 / batchSize10k;
                LOG.infoW().$(String.format("batchSize %,d, numIters %,d", batchSize, numIters)).$();
                CairoEngine engine = serverMain.getEngine();
                TableToken tableToken = engine.verifyTableName(tn);
                senderLoop:
                for (int j = 0; j < numIters; j++) {
                    for (int i = 0; i < batchSize; i++) {
                        if (engine.getTableSequencerAPI().isSuspended(tableToken)) {
                            break senderLoop;
                        }
                        sender.table(tn)
                                .symbol("sym", rnd.nextString(2))
                                .longColumn("b", rnd.nextByte())
                                .longColumn("s", rnd.nextShort())
                                .longColumn("i", rnd.nextInt(1000))
                                .doubleColumn("f", rnd.nextFloat())
                                .doubleColumn("d", rnd.nextDouble())
                                .stringColumn("v", rnd.nextString(50))
                                .timestampColumn("tss", Instant.ofEpochMilli(rnd.nextLong()))
                                .at(rnd.nextLong(100L * 3_600_000), ChronoUnit.MILLIS);
                    }
                    long rssUsed = Unsafe.getRssMemUsed();
                    Unsafe.setRssMemLimit(rssUsed + 3 * (1L << 20));
                    try {
                        sender.flush();
                    } finally {
                        Unsafe.setRssMemLimit(0);
                    }
                }
                try {
                    engine.awaitTable(tn, 10, TimeUnit.MINUTES);
                } catch (CairoException e) {
                    if (!e.getMessage().contains("table is suspended [tableName=table1]")) {
                        e.printStackTrace(System.err);
                        Assert.fail("The only accepted error is 'table is suspended [tableName=table1]', but got: " + e.getMessage());
                    }
                    System.out.printf("\n\n%s\n\n", e.getMessage());
                }
            }
        }
    }
}
