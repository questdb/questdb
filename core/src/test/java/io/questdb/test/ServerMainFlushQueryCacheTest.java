/*******************************************************************************
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

package io.questdb.test;

import io.questdb.Metrics;
import io.questdb.PropertyKey;
import io.questdb.ServerMain;
import io.questdb.std.str.StringSink;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;

import static io.questdb.test.cutlass.pgwire.BasePGTest.assertResultSet;

public class ServerMainFlushQueryCacheTest extends AbstractBootstrapTest {
    private static final StringSink sink = new StringSink();

    @Before
    public void setUp() {
        super.setUp();
        TestUtils.unchecked(() -> createDummyConfiguration(
                PropertyKey.METRICS_ENABLED + "=true",
                PropertyKey.PG_SELECT_CACHE_ENABLED + "=true",
                PropertyKey.PG_WORKER_COUNT + "=4"
        ));
        dbPath.parent().$();
    }

    @Test
    public void testFlushQueryCache() throws Exception {
        try (final ServerMain serverMain = new ServerMain(getServerMainArgs())) {
            serverMain.start();
            try (Connection conn = DriverManager.getConnection(PG_CONNECTION_URI, PG_CONNECTION_PROPERTIES)) {
                try (Statement statement = conn.createStatement()) {
                    statement.execute("create table x as (select 'k' || (x % 3) k, x % 10000 l from long_sequence(1000000));");
                }

                final int nQueries = 10;
                for (int i = 0; i < nQueries; i++) {
                    String query = "select " + i + ";";
                    String expected = i + "[INTEGER]\n" +
                            i + "\n";
                    try (ResultSet rs = conn.prepareStatement(query).executeQuery()) {
                        sink.clear();
                        assertResultSet(expected, sink, rs);
                    }
                }

                final Metrics metrics = serverMain.getEngine().getMetrics();
                TestUtils.assertEventually(() -> Assert.assertEquals(nQueries, metrics.pgWireMetrics().cachedSelectsGauge().getValue()));

                try (Statement statement = conn.createStatement()) {
                    statement.execute("select flush_query_cache();");
                }

                // Only the select flush_query_cache(); query may remain in the cache.
                TestUtils.assertEventually(() -> Assert.assertTrue(metrics.pgWireMetrics().cachedSelectsGauge().getValue() <= 1));
            }
        }
    }
}
