/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2022 QuestDB
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

package io.questdb.cutlass.pgwire;

import io.questdb.mp.MPSequence;
import io.questdb.std.Unsafe;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.Statement;

import static io.questdb.test.tools.TestUtils.assertEventually;

public class PGFlushQueryCacheTest extends BasePGTest {

    @BeforeClass
    public static void setUpStatic() {
        queryCacheEventQueueCapacity = 1;
        BasePGTest.setUpStatic();
        Assert.assertEquals(1, engine.getConfiguration().getQueryCacheEventQueueCapacity());
    }

    @Before
    public void setUp() {
        super.setUp();
        // Make sure to reset the publisher sequence after what we published to it in checkQueryCacheFlushed().
        engine.getMessageBus().getQueryCacheEventPubSeq().clear();
    }

    @Test
    public void testFlushQueryCache() throws Exception {
        assertMemoryLeak(() -> {
            try (PGWireServer server = createPGServer(2)) {
                workerPoolManager.startAll();
                try (
                        Connection connection = getConnection(server.getPort(), false, true);
                        Statement statement = connection.createStatement()
                ) {
                    statement.executeUpdate("CREATE TABLE test\n" +
                            "AS(\n" +
                            "    SELECT\n" +
                            "        x id,\n" +
                            "        timestamp_sequence(0L, 100000L) ts\n" +
                            "    FROM long_sequence(1000) x)\n" +
                            "TIMESTAMP(ts)\n" +
                            "PARTITION BY DAY");

                    Assert.assertEquals(0, metrics.pgWire().cachedSelectsGauge().getValue());

                    String sql = "SELECT *\n" +
                            "FROM test t1 JOIN test t2 \n" +
                            "ON t1.id = t2.id\n" +
                            "LIMIT 1";
                    statement.execute(sql);

                    assertEventually(() -> Assert.assertEquals(1, metrics.pgWire().cachedSelectsGauge().getValue()));

                    statement.execute("SELECT flush_query_cache()");

                    assertEventually(() -> Assert.assertEquals(0, metrics.pgWire().cachedSelectsGauge().getValue()));
                } finally {
                    workerPoolManager.closeAll();
                }
            }
        });
    }

    @Test
    public void testFlushUpdateCache() throws Exception {
        assertMemoryLeak(() -> {
            try (PGWireServer server = createPGServer(2)) {
                workerPoolManager.startAll();
                try (
                        Connection connection = getConnection(server.getPort(), false, true);
                        Statement statement = connection.createStatement()
                ) {
                    statement.executeUpdate("CREATE TABLE test\n" +
                            "AS(\n" +
                            "    SELECT\n" +
                            "        x id,\n" +
                            "        timestamp_sequence(0L, 100000L) ts\n" +
                            "    FROM long_sequence(1000) x)\n" +
                            "TIMESTAMP(ts)\n" +
                            "PARTITION BY DAY");

                    Assert.assertEquals(0, metrics.pgWire().cachedUpdatesGauge().getValue());

                    String sql = "UPDATE test t1 set id = ? \n" +
                            "FROM test t2 \n" +
                            "WHERE t1.id = t2.id";
                    try (PreparedStatement updateSt = connection.prepareStatement(sql)) {
                        updateSt.setLong(1, 1L);
                        updateSt.execute();
                    }

                    assertEventually(() -> Assert.assertEquals(1, metrics.pgWire().cachedUpdatesGauge().getValue()));

                    statement.execute("SELECT flush_query_cache()");

                    assertEventually(() -> Assert.assertEquals(0, metrics.pgWire().cachedUpdatesGauge().getValue()));
                } finally {
                    workerPoolManager.closeAll();
                }
            }
        });
    }

    private void checkQueryCacheFlushed(long memInitial, long memAfterJoin) {
        // We need to wait until PG Wire workers process the message. To do so, we simply try to
        // publish another query flush event. Since we set the queue size to 1, we're able to
        // publish only when all consumers (PG Wire workers) have processed the previous event.
        final MPSequence pubSeq = engine.getMessageBus().getQueryCacheEventPubSeq();
        pubSeq.waitForNext();

        // Sequence set to done before actual flush performed. We might have to try it a few times,
        // before memory usage drop is measured.
        assertEventually(() -> {
            long memAfterFlush = Unsafe.getMemUsed();
            Assert.assertTrue(
                    "flush_query_cache() should release native memory: " + memInitial + ", " + memAfterJoin + ", " + memAfterFlush,
                    memAfterFlush < memAfterJoin
            );
        });
    }
}
