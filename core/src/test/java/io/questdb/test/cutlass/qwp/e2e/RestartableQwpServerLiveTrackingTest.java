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

package io.questdb.test.cutlass.qwp.e2e;

import io.questdb.client.Sender;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

import java.time.temporal.ChronoUnit;

/**
 * Pins what {@link RestartableQwpServer#stop()} counts: an upgraded connection that has
 * carried a frame and that the client still holds when the workers halt counts as one live
 * drop; a connection the client closed first, which the worker has already removed from the
 * live set, counts as zero. The reconnect fuzz test's DISCONNECTED floor is asserted against
 * this count.
 */
public class RestartableQwpServerLiveTrackingTest extends AbstractCairoTest {
    private static final String TABLE_NAME = "restartable_qwp_server_live_tracking";
    private static final long TS_NANOS = 1_700_000_000_000_000_000L;

    @Test
    public void testStopCountsOnlyConnectionsStillLiveWhenWorkersHalt() throws Exception {
        assertMemoryLeak(() -> {
            createTable();
            int port = RestartableQwpServer.pickFreePort();
            try (RestartableQwpServer server = new RestartableQwpServer(engine, configuration, port)) {
                server.start();
                Assert.assertEquals(0, server.liveConnectionCount());

                // A client that has sent a frame and is still connected when the server stops.
                try (Sender sender = Sender.fromConfig("ws::addr=localhost:" + port + ";")) {
                    sender.table(TABLE_NAME).longColumn("id", 1).at(TS_NANOS, ChronoUnit.NANOS);
                    sender.flush();
                    Assert.assertTrue("server never saw a frame from the sender",
                            server.awaitStableLiveConnection(10_000, 20));
                    Assert.assertEquals(1, server.liveConnectionCount());
                    Assert.assertEquals("stop() must report the live connection it killed", 1, server.stop());
                    Assert.assertEquals(0, server.liveConnectionCount());
                    // Bring the server back before the sender closes, so close() reconnects
                    // and completes instead of waiting out its flush timeout on a dead port.
                    server.start();
                }

                // A client that closed first: the worker processes the close and drops the fd.
                try (Sender sender = Sender.fromConfig("ws::addr=localhost:" + port + ";")) {
                    sender.table(TABLE_NAME).longColumn("id", 2).at(TS_NANOS + 1, ChronoUnit.NANOS);
                    sender.flush();
                    Assert.assertTrue("server never saw a frame from the second sender",
                            server.awaitStableLiveConnection(10_000, 20));
                }
                TestUtils.assertEventually(() -> Assert.assertEquals(0, server.liveConnectionCount()));
                Assert.assertEquals("stop() must not count a connection the client had already closed",
                        0, server.stop());
            }
        });
    }

    private void createTable() {
        try {
            execute("CREATE TABLE " + TABLE_NAME + " (id LONG, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
        } catch (Exception e) {
            throw new AssertionError("failed to create table", e);
        }
    }
}
