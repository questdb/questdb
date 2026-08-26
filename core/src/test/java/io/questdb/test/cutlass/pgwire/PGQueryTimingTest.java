/*+*****************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|_| |_|____/
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

package io.questdb.test.cutlass.pgwire;

import io.questdb.PropertyKey;
import io.questdb.metrics.QueryTrace;
import io.questdb.mp.ConcurrentQueue;
import io.questdb.std.Os;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;

public class PGQueryTimingTest extends BasePGTest {

    @Before
    public void setupTracing() {
        node1.getConfigurationOverrides().setProperty(PropertyKey.QUERY_TRACING_ENABLED, true);
    }

    @Test
    public void testFragmentedSyncPortalSuspensionCountsAsWait() throws Exception {
        assertWithPgServerExtendedBinaryOnly(
                (connection, _, _, _) -> assertPortalSuspensionCountsAsWait(connection),
                this::setupTracingWithFragmentedSync
        );
    }

    @Test
    public void testPortalSuspensionCountsAsWait() throws Exception {
        assertWithPgServer(
                CONN_AWARE_EXTENDED,
                (connection, _, _, _) -> assertPortalSuspensionCountsAsWait(connection),
                this::setupTracing
        );
    }

    private void assertPortalSuspensionCountsAsWait(Connection connection) throws Exception {
        execute("CREATE TABLE tab AS (SELECT x FROM long_sequence(100))");
        final ConcurrentQueue<QueryTrace> queue = engine.getMessageBus().getQueryTraceQueue();
        drain(queue);
        connection.setAutoCommit(false);
        final String query = "SELECT x FROM tab";
        try (PreparedStatement statement = connection.prepareStatement(query)) {
            statement.setFetchSize(10);
            try (ResultSet resultSet = statement.executeQuery()) {
                int rows = 0;
                while (resultSet.next()) {
                    if (++rows % 10 == 0) {
                        Os.sleep(20);
                    }
                }
                Assert.assertEquals(100, rows);
            }
        }
        connection.commit();
        final QueryTrace trace = pollTraceFor(queue, query);
        Assert.assertTrue("expected wait > 0, got " + trace.waitNanos, trace.waitNanos > 0);
        Assert.assertTrue("expected portal wait >= 100000000, got " + trace.waitNanos, trace.waitNanos >= 100_000_000L);
        Assert.assertTrue(trace.waitNanos <= trace.executionNanos);
        Assert.assertTrue(trace.firstRowNanos >= 0);
        Assert.assertTrue(trace.firstRowNanos <= trace.executionNanos);
    }

    private static void drain(ConcurrentQueue<QueryTrace> queue) {
        final QueryTrace trace = new QueryTrace();
        while (queue.tryDequeue(trace)) {
        }
    }

    private static QueryTrace pollTraceFor(ConcurrentQueue<QueryTrace> queue, String query) {
        final QueryTrace trace = new QueryTrace();
        final long deadline = System.currentTimeMillis() + 10_000;
        while (System.currentTimeMillis() < deadline) {
            while (queue.tryDequeue(trace)) {
                if (query.equals(trace.queryText)) {
                    return trace;
                }
            }
            Os.sleep(50);
        }
        Assert.fail("no trace for query: " + query);
        return null;
    }

    private void setupTracingWithFragmentedSync() {
        setupTracing();
        // Force the retained portal's sync response through a pending flush.
        sendBufferSize = 256;
        forceSendFragmentationChunkSize = 64;
    }
}
