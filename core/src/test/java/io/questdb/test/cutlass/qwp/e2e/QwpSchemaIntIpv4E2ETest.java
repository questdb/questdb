/*+*****************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2026 QuestDB
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

package io.questdb.test.cutlass.qwp.e2e;

import io.questdb.client.LineSenderSchemaException;
import io.questdb.client.Sender;
import org.junit.Assert;
import org.junit.Test;

import java.time.temporal.ChronoUnit;
import java.util.concurrent.TimeUnit;

public class QwpSchemaIntIpv4E2ETest extends AbstractQwpWebSocketTest {

    @Test
    public void testPublicSenderPreservesIntBitsAndRollsBackPartialRow() throws Exception {
        runInContext(port -> {
            execute("create table schema_int_ipv4 "
                    + "(value IPv4, marker string, bad uuid, ts timestamp) "
                    + "timestamp(ts) partition by day wal");
            try (Sender sender = connectWs(
                    port,
                    0,
                    0,
                    TimeUnit.MILLISECONDS.toNanos(Integer.MAX_VALUE - 1L)
            )) {
                sender.table("schema_int_ipv4")
                        .intColumn("value", Integer.MIN_VALUE)
                        .stringColumn("marker", "int-null")
                        .at(1, ChronoUnit.MICROS);
                sender.table("schema_int_ipv4")
                        .intColumn("value", 0)
                        .stringColumn("marker", "ipv4-null")
                        .at(2, ChronoUnit.MICROS);
                append(sender, 1, "one", 3);
                append(sender, Integer.MIN_VALUE + 1, "min-plus-one", 4);
                append(sender, 0x0a000001, "ten", 5);
                append(sender, (int) 0xc0a80101L, "lan", 6);
                append(sender, Integer.MAX_VALUE, "max", 7);
                append(sender, -1, "all-ones", 8);

                sender.table("schema_int_ipv4")
                        .stringColumn("marker", "failed-B")
                        .intColumn("value", 0x01020304);
                LineSenderSchemaException error = Assert.assertThrows(
                        LineSenderSchemaException.class,
                        () -> sender.intColumn("bad", 1)
                );
                Assert.assertEquals(LineSenderSchemaException.Reason.UNSUPPORTED_FEATURE, error.getReason());

                sender.table("schema_int_ipv4")
                        .stringColumn("marker", "omitted")
                        .at(9, ChronoUnit.MICROS);
                long fsn = sender.flushAndGetSequence();
                Assert.assertTrue(fsn >= 0);
                Assert.assertTrue(sender.awaitAckedFsn(fsn, 10_000));
            }

            drainWalQueue();
            assertQuery("select coalesce(value::string, 'null') value, marker "
                    + "from schema_int_ipv4 order by ts")
                    .noLeakCheck()
                    .returnsOnce("value\tmarker\n"
                            + "null\tint-null\n"
                            + "null\tipv4-null\n"
                            + "0.0.0.1\tone\n"
                            + "128.0.0.1\tmin-plus-one\n"
                            + "10.0.0.1\tten\n"
                            + "192.168.1.1\tlan\n"
                            + "127.255.255.255\tmax\n"
                            + "255.255.255.255\tall-ones\n"
                            + "null\tomitted\n");
        });
    }

    private static void append(Sender sender, int value, String marker, long timestamp) {
        sender.table("schema_int_ipv4")
                .intColumn("value", value)
                .stringColumn("marker", marker)
                .at(timestamp, ChronoUnit.MICROS);
    }
}
