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

package io.questdb.test.cutlass.pgwire;

import io.questdb.cairo.CairoEngine;
import io.questdb.cairo.CairoException;
import io.questdb.cutlass.pgwire.PGConfiguration;
import io.questdb.cutlass.pgwire.PGServer;
import io.questdb.griffin.CompiledQuery;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.metrics.LongGauge;
import io.questdb.mp.WorkerPool;
import io.questdb.std.ConcurrentCacheConfiguration;
import io.questdb.std.DefaultConcurrentCacheConfiguration;
import io.questdb.std.str.BorrowableUtf8Sink;
import io.questdb.test.AbstractCairoTest;
import org.jetbrains.annotations.NotNull;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * A connection that closes while a SELECT still has an open cursor puts the SELECT's plan into the
 * shared select cache. Ending the statement's execution owner clears the cast-to-SYMBOL dictionaries
 * of that plan, so the owner must end before the plan is in the cache: another connection can take
 * it from there and use the dictionaries at once. The engine here routes protocol owners through the
 * query registry the way Enterprise does with resource groups on.
 */
public class PGDisconnectSelectCacheTest extends BasePGTest {
    private static final String CACHE_PUT = "cache.put";
    private static final List<String> EVENTS = Collections.synchronizedList(new ArrayList<>());
    private static final String EXPECTED_ROWS = """
            s[VARCHAR]
            1
            2
            3
            4
            5
            6
            7
            8
            9
            10
            """;
    private static final AtomicBoolean IS_OWNER_END_FAILING = new AtomicBoolean();
    private static final String OWNER_END = "owner.end";
    private static final String SQL = "SELECT x::symbol s FROM long_sequence(10)";
    private static volatile CountDownLatch disconnectLatch;

    @BeforeClass
    public static void setUpStatic() throws Exception {
        AbstractCairoTest.engineFactory = conf -> new CairoEngine(conf) {
            @Override
            public long beginSqlExecution(CharSequence query, SqlExecutionContext executionContext, short compiledQueryType) {
                return switch (compiledQueryType) {
                    case CompiledQuery.CREATE_TABLE_AS_SELECT,
                         CompiledQuery.INSERT_AS_SELECT,
                         CompiledQuery.SELECT -> getQueryRegistry().registerOwner(query, executionContext);
                    default -> -1;
                };
            }

            @Override
            public void endSqlExecution(long ownerId, SqlExecutionContext executionContext) {
                if (ownerId < 0) {
                    return;
                }
                record(OWNER_END);
                getQueryRegistry().unregister(ownerId, executionContext);
                if (IS_OWNER_END_FAILING.compareAndSet(true, false)) {
                    throw CairoException.nonCritical().put("owner end failed");
                }
            }

            @Override
            public void mountSqlExecution(long ownerId, SqlExecutionContext executionContext) {
                getQueryRegistry().mountOwner(ownerId, executionContext);
            }

            @Override
            public void unmountSqlExecution(long ownerId, SqlExecutionContext executionContext) {
                getQueryRegistry().unmountOwner(ownerId, executionContext);
            }
        };
        AbstractCairoTest.setUpStatic();
    }

    @Test
    public void testDisconnectWithSuspendedNamedPortalEndsOwnerBeforeCachingPlan() throws Exception {
        assertDisconnect(-2, false, connection -> {
            // A fetch size outside autocommit makes the driver read through a named portal.
            connection.setAutoCommit(false);
            final PreparedStatement statement = connection.prepareStatement(SQL);
            statement.setFetchSize(2);
            return statement;
        }, List.of(OWNER_END, CACHE_PUT));
    }

    @Test
    public void testDisconnectWithSuspendedNamedStatementEndsOwnerBeforeCachingPlan() throws Exception {
        // A prepare threshold of 1 makes the driver use a named statement on the first run, and a
        // row limit leaves its cursor suspended on the unnamed portal.
        assertDisconnect(1, false, connection -> {
            final PreparedStatement statement = connection.prepareStatement(SQL);
            statement.setMaxRows(2);
            return statement;
        }, List.of(OWNER_END, CACHE_PUT));
    }

    @Test
    public void testFailedOwnerEndOnDisconnectFreesPlanInsteadOfCachingIt() throws Exception {
        assertDisconnect(-2, true, connection -> {
            connection.setAutoCommit(false);
            final PreparedStatement statement = connection.prepareStatement(SQL);
            statement.setFetchSize(2);
            return statement;
        }, List.of(OWNER_END));
    }

    private static void record(String event) {
        final CountDownLatch latch = disconnectLatch;
        if (latch != null) {
            EVENTS.add(event);
            latch.countDown();
        }
    }

    private void assertDisconnect(
            int prepareThreshold,
            boolean isOwnerEndFailing,
            StatementFactory statementFactory,
            List<String> expectedEvents
    ) throws Exception {
        assertMemoryLeak(() -> {
            EVENTS.clear();
            disconnectLatch = null;
            final ConcurrentCacheConfiguration cacheConfiguration = new DefaultConcurrentCacheConfiguration() {
                @Override
                public LongGauge getCachedGauge() {
                    return new RecordingGauge();
                }
            };
            final PGConfiguration configuration = new Port0PGConfiguration() {
                @Override
                public ConcurrentCacheConfiguration getConcurrentCacheConfiguration() {
                    return cacheConfiguration;
                }

                @Override
                public int getWorkerCount() {
                    return 2;
                }
            };
            try (
                    PGServer server = createPGServer(configuration);
                    WorkerPool workerPool = server.getWorkerPool()
            ) {
                workerPool.start(LOG);
                try {
                    final Connection connection = getConnection(Mode.EXTENDED, server.getPort(), false, prepareThreshold);
                    try {
                        final PreparedStatement statement = statementFactory.of(connection);
                        final ResultSet resultSet = statement.executeQuery();
                        Assert.assertTrue(resultSet.next());
                        Assert.assertEquals("1", resultSet.getString(1));
                        Assert.assertTrue(resultSet.next());
                        Assert.assertEquals("2", resultSet.getString(1));
                        // The statement is still open on the server: its cursor waits for the next batch.
                        IS_OWNER_END_FAILING.set(isOwnerEndFailing);
                        disconnectLatch = new CountDownLatch(expectedEvents.size());
                    } finally {
                        // Close the connection without closing the statement or the result set first,
                        // so that the server closes the open cursor when the connection goes away.
                        connection.close();
                    }
                    Assert.assertTrue(disconnectLatch.await(30, TimeUnit.SECONDS));
                    disconnectLatch = null;
                    Assert.assertEquals(expectedEvents, new ArrayList<>(EVENTS));

                    // The same query on a new connection returns every row, whether it takes the
                    // plan from the cache or compiles it again.
                    try (
                            Connection other = getConnection(Mode.EXTENDED, server.getPort(), false, prepareThreshold);
                            PreparedStatement statement = other.prepareStatement(SQL);
                            ResultSet resultSet = statement.executeQuery()
                    ) {
                        sink.clear();
                        assertResultSet(EXPECTED_ROWS, sink, resultSet);
                    }
                } finally {
                    disconnectLatch = null;
                    IS_OWNER_END_FAILING.set(false);
                }
            }
        });
    }

    @FunctionalInterface
    private interface StatementFactory {
        PreparedStatement of(Connection connection) throws Exception;
    }

    private static final class RecordingGauge implements LongGauge {

        @Override
        public void add(long value) {
        }

        @Override
        public void dec() {
        }

        @Override
        public CharSequence getName() {
            return "pg_select_cache_size";
        }

        @Override
        public long getValue() {
            return 0;
        }

        @Override
        public void inc() {
            // The cache counts each plan it takes in, so this marks the put.
            record(CACHE_PUT);
        }

        @Override
        public void scrapeIntoPrometheus(@NotNull BorrowableUtf8Sink sink) {
        }

        @Override
        public void setValue(long value) {
        }
    }
}
