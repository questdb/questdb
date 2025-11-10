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

package io.questdb.test.cutlass.line.tcp;

import io.questdb.client.Sender;
import io.questdb.griffin.engine.QueryProgress;
import io.questdb.log.LogFactory;
import io.questdb.std.Decimal256;
import io.questdb.test.tools.LogCapture;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.time.temporal.ChronoUnit;

import static io.questdb.client.Sender.PROTOCOL_VERSION_V3;

public class LineTcpDecimalCreateTest extends AbstractLineTcpReceiverTest {
    private static final LogCapture capture = new LogCapture();

    @Before
    @Override
    public void setUp() {
        LogFactory.enableGuaranteedLogging(QueryProgress.class);
        super.setUp();
        capture.start();
    }

    @After
    @Override
    public void tearDown() throws Exception {
        capture.stop();
        super.tearDown();
        LogFactory.disableGuaranteedLogging(QueryProgress.class);
    }

    @Test
    public void testInsertDecimalCreateTableAutomatically() throws Exception {
        runInContext(r -> {
            try (Sender sender = Sender.builder(Sender.Transport.TCP)
                    .address("127.0.0.1")
                    .port(bindPort)
                    .protocolVersion(PROTOCOL_VERSION_V3)
                    .build()
            ) {
                sender.table("decimal_test")
                        .decimalColumn("a", Decimal256.fromLong(12345, 2))
                        .at(100000000000L, ChronoUnit.MICROS);
                sender.flush();
            }
            capture.waitFor("decimal columns cannot be created automatically [table=decimal_test, columnName=a]");
        });
    }
}
