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

import io.questdb.cairo.DataUnavailableException;
import io.questdb.cairo.pool.ReaderPool;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.mp.WorkerPool;
import io.questdb.network.DefaultIODispatcherConfiguration;
import io.questdb.network.IODispatcherConfiguration;
import io.questdb.network.SuspendEvent;
import io.questdb.network.SuspendEventFactory;
import io.questdb.std.ConcurrentHashMap;
import io.questdb.std.Misc;
import io.questdb.std.ObjList;
import io.questdb.std.str.StringSink;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

import java.io.Closeable;
import java.io.IOException;
import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;


/**
 * This test verifies record cursor factory suspendability. To do so, it runs the same query
 * with data available immediately and with simulated cold storage scenario and then compares
 * the result set.
 */
@SuppressWarnings("SqlNoDataSourceInspection")
public class PGQuerySuspendabilityTest extends BasePGTest {

    private static final Log LOG = LogFactory.getLog(PGQuerySuspendabilityTest.class);
    private static final IODispatcherConfiguration ioDispatcherConfig = new DefaultIODispatcherConfiguration();
    private static final StringSink sinkB = new StringSink();
    private static final ObjList<TestCase> testCases = new ObjList<>();

    @Test
    public void testQuerySuspendability() throws Exception {
        assertMemoryLeak(() -> {
            try (
                    final PGWireServer server = createPGServer(1);
                    final WorkerPool workerPool = server.getWorkerPool()
            ) {
                workerPool.start(LOG);
                try (final Connection connection = getConnection(server.getPort(), true, true)) {
                    CallableStatement stmt = connection.prepareCall(
                            "create table x as ( " +
                                    "  select " +
                                    "    cast(x as int) i, " +
                                    "    rnd_double(2) d, " +
                                    "    rnd_long() l, " +
                                    "    rnd_str(5,16,2) s, " +
                                    "    rnd_symbol(4,4,4,2) sym, " +
                                    "    rnd_symbol('a','b','c') isym, " +
                                    "    timestamp_sequence(0, 100000000) ts " +
                                    "   from long_sequence(3000)" +
                                    "), index(isym) timestamp(ts) partition by day"
                    );
                    stmt.execute();
                    stmt = connection.prepareCall("create table y as (select * from x), index(isym) timestamp(ts) partition by day");
                    stmt.execute();

                    SuspendingReaderListener listener = null;
                    try {
                        for (int i = 0; i < testCases.size(); i++) {
                            TestCase tc = testCases.getQuick(i);

                            engine.setReaderListener(null);
                            try (PreparedStatement statement = connection.prepareStatement(tc.query)) {
                                sink.clear();
                                try (ResultSet rs = statement.executeQuery()) {
                                    long rows = printToSink(sink, rs);
                                    if (!tc.allowEmptyResultSet) {
                                        Assert.assertTrue("Query " + tc.query + " is expected to return non-empty result set", rows > 0);
                                    }
                                }
                            }

                            engine.releaseAllReaders();

                            listener = new SuspendingReaderListener();
                            // Yes, this write is racy, but it's not an issue in the test scenario.
                            engine.setReaderListener(listener);
                            try (PreparedStatement statement = connection.prepareStatement(tc.query)) {
                                sinkB.clear();
                                try (ResultSet rs = statement.executeQuery()) {
                                    printToSink(sinkB, rs);
                                }
                            }

                            TestUtils.assertEquals(tc.query, sink, sinkB);
                        }
                    } finally {
                        Misc.free(listener);
                    }
                }
            }
        });
    }

    private static void addTestCase(String query) {
        addTestCase(query, false);
    }

    private static void addTestCase(String query, boolean allowEmptyResultSet) {
        testCases.add(new TestCase(query, allowEmptyResultSet));
    }

    /**
     * This listener varies DataUnavailableException and successful execution of TableReader#openPartition()
     * in the following sequence: exception, success, exception, success, etc.
     */
    private static class SuspendingReaderListener implements ReaderPool.ReaderListener, Closeable {

        private final ConcurrentHashMap<SuspendEvent> suspendedPartitions = new ConcurrentHashMap<>();

        @Override
        public void close() throws IOException {
            suspendedPartitions.forEach((charSequence, suspendEvent) -> {
                suspendEvent.close();
            });
        }

        @Override
        public void onOpenPartition(String tableName, int partitionIndex) {
            final String key = tableName + "$" + partitionIndex;
            SuspendEvent computedEvent = suspendedPartitions.compute(key, (charSequence, prevEvent) -> {
                if (prevEvent != null) {
                    // Success case.
                    prevEvent.close();
                    return null;
                }
                // Exception case.
                SuspendEvent nextEvent = SuspendEventFactory.newInstance(ioDispatcherConfig);
                // Mark the event as immediately fulfilled.
                nextEvent.trigger();
                return nextEvent;
            });
            if (computedEvent != null) {
                throw DataUnavailableException.instance(tableName, String.valueOf(partitionIndex), computedEvent);
            }
        }
    }

    private static class TestCase {
        final boolean allowEmptyResultSet;
        final String query;

        public TestCase(String query, boolean allowEmptyResultSet) {
            this.query = query;
            this.allowEmptyResultSet = allowEmptyResultSet;
        }
    }

    static {
        addTestCase("select * from x");

        // LimitRecordCursorFactory
        addTestCase("select * from x limit 1");
        addTestCase("select * from x limit 1,3");
        addTestCase("select * from x limit 1,-1");
        addTestCase("select * from x limit 0,-1");
        addTestCase("select * from x limit -1");
        addTestCase("select * from x limit -4000");
        addTestCase("select * from x limit -3,-1");
        addTestCase("select * from x limit -3,-4", true);
        addTestCase("select * from (x union all y) limit 1");
        addTestCase("select * from (x union all y) limit 1,3");
        addTestCase("select * from (x union all y) limit 1,-1");
        addTestCase("select * from (x union all y) limit -1");
        addTestCase("select * from (x union all y) limit -3,-1");
        addTestCase("select * from (x union all (y where isym = 'a')) limit 1");
        addTestCase("select * from (x union all (y where isym = 'a')) limit 1,3");
        addTestCase("select * from (x union all (y where isym = 'a')) limit 1,-1");
        addTestCase("select * from (x union all (y where isym = 'a')) limit -1");
        addTestCase("select * from (x union all (y where isym = 'a')) limit -3,-1");
        addTestCase("select * from (x union all (y where isym = 'a')) limit -4000,-1");

        // TODO: fix all async offload variations
        //queries.add("select * from x where i = 42");
    }
}
