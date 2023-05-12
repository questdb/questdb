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

package io.questdb.test.cutlass.pgwire;

import io.questdb.cutlass.pgwire.PGWireServer;
import io.questdb.mp.WorkerPool;
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
    public static void setUpStatic() throws Exception {
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
            try (
                    final PGWireServer server = createPGServer(2);
                    final WorkerPool workerPool = server.getWorkerPool()
            ) {
                workerPool.start(LOG);
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
                }
            }
        });
    }

    @Test
    public void testFlushUpdateCache() throws Exception {
        assertMemoryLeak(() -> {
            try (final PGWireServer server = createPGServer(2);
                 final WorkerPool workerPool = server.getWorkerPool()) {
                workerPool.start(LOG);
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
                }
            }
        });
    }
}
