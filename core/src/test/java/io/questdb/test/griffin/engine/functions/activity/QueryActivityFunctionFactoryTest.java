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

package io.questdb.test.griffin.engine.functions.activity;

import io.questdb.cairo.CairoEngine;
import io.questdb.cairo.CairoException;
import io.questdb.cairo.security.AllowAllSecurityContext;
import io.questdb.cairo.security.ReadOnlySecurityContext;
import io.questdb.cairo.sql.AtomicBooleanCircuitBreaker;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.griffin.SqlCompiler;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContextImpl;
import io.questdb.mp.SOCountDownLatch;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

import java.util.concurrent.atomic.AtomicReference;

public class QueryActivityFunctionFactoryTest extends AbstractCairoTest {

    private SqlExecutionContextImpl adminUserContext1;
    private SqlExecutionContextImpl adminUserContext2;
    private SqlExecutionContextImpl regularUserContext1;

    @Override
    public void setUp() {
        super.setUp();

        regularUserContext1 = new SqlExecutionContextImpl(engine, 1).with(new UserContext(), null);
        regularUserContext1.with(new AtomicBooleanCircuitBreaker());

        adminUserContext1 = new SqlExecutionContextImpl(engine, 1).with(new AdminContext(), null);
        adminUserContext1.with(new AtomicBooleanCircuitBreaker());

        adminUserContext2 = new SqlExecutionContextImpl(engine, 1).with(new AdminContext(), null);
        adminUserContext2.with(new AtomicBooleanCircuitBreaker());
    }

    @Test
    public void testAdminCanSeeOtherPeoplesQueries() throws Exception {
        assertMemoryLeak(() -> {
            final String query = "select 1 t from long_sequence(1) where sleep(120000)";

            SOCountDownLatch started = new SOCountDownLatch(1);
            SOCountDownLatch stopped = new SOCountDownLatch(1);
            AtomicReference<Exception> error = new AtomicReference<>();

            new Thread(() -> {
                started.countDown();
                try {
                    try (SqlCompiler compiler = engine.getSqlCompiler()) {
                        assertQuery(compiler,
                                "t\n1\n",
                                query,
                                null, regularUserContext1, true, false);
                        Assert.fail("Query should have been cancelled");
                    } catch (Exception e) {
                        if (!e.getMessage().contains("cancelled by user")) {
                            error.set(e);
                        }
                    }
                } finally {
                    stopped.countDown();
                }
            }, "query_thread").start();

            started.await();

            try (SqlCompiler compiler = engine.getSqlCompiler()) {
                String activityQuery = "select query_id, query from query_activity() where query ='" + query + "'";

                long queryId = -1;
                try (final RecordCursorFactory factory = CairoEngine.select(compiler, activityQuery, adminUserContext1)) {
                    // admin can see admin's command
                    while (error.get() == null) {
                        try (RecordCursor cursor = factory.getCursor(adminUserContext1)) {
                            if (cursor.hasNext()) {
                                queryId = cursor.getRecord().getLong(0);
                                break;
                            }
                        }
                    }
                }

                ddl("cancel query " + queryId, adminUserContext1);
            } finally {
                stopped.await();
                if (error.get() != null) {
                    throw error.get();
                }
            }
        });
    }

    @Test
    public void testAdminUserCantCancelQueriesNotInRegistry() throws Exception {
        assertMemoryLeak(() -> {
            try {
                ddl("cancel query 123456789", adminUserContext1);
                Assert.fail();
            } catch (SqlException e) {
                TestUtils.assertContains(e.getMessage(), "query to cancel not found in registry [id=123456789]");
            }
        });
    }

    @Test
    public void testListQueriesWithNoQueryRunningShowsOwnSelect() throws Exception {
        assertMemoryLeak(() -> {
            assertQuery("username\tquery\n" +
                            "admin\tselect username, query from query_activity()\n",
                    "select username, query from query_activity()",
                    null, false, false);
        });
    }

    @Test
    public void testNonAdminCantSeeOtherUsersCommands() throws Exception {
        assertMemoryLeak(() -> {
            final String query = "select 1 t from long_sequence(1) where sleep(120000)";

            SOCountDownLatch started = new SOCountDownLatch(1);
            SOCountDownLatch stopped = new SOCountDownLatch(1);
            AtomicReference<Exception> error = new AtomicReference<>();

            new Thread(() -> {
                started.countDown();
                try {
                    try (SqlCompiler compiler = engine.getSqlCompiler()) {
                        assertQuery(compiler,
                                "t\n1\n",
                                query,
                                null, adminUserContext1, true, false);
                        Assert.fail("Query should have been cancelled");
                    } catch (Exception e) {
                        if (!e.getMessage().contains("cancelled by user")) {
                            error.set(e);
                        }
                    }
                } finally {
                    stopped.countDown();
                }
            }, "query_thread").start();

            started.await();

            try (SqlCompiler compiler = engine.getSqlCompiler()) {
                String activityQuery = "select query_id, query from query_activity() where query ='" + query + "'";

                long queryId = -1;
                try (final RecordCursorFactory factory = CairoEngine.select(compiler, activityQuery, adminUserContext2)) {
                    // admin can see admin's command
                    while (error.get() == null) {
                        try (RecordCursor cursor = factory.getCursor(adminUserContext2)) {
                            if (cursor.hasNext()) {
                                queryId = cursor.getRecord().getLong(0);
                                break;
                            }
                        }
                    }
                }

                // regular user can't see admin's command
                assertQuery(compiler,
                        "query_id\tquery\n",
                        activityQuery,
                        null, regularUserContext1, false, false);


                ddl("cancel query " + queryId, adminUserContext2);
            } finally {
                stopped.await();
                if (error.get() != null) {
                    throw error.get();
                }
            }
        });
    }

    @Test
    public void testQueryIdToCancelMustBeNonNegativeInteger() throws Exception {
        assertMemoryLeak(() -> {
            assertException("cancel ", 6, "'QUERY' expected", adminUserContext1);
            assertException("cancel SQL 1", 7, "'QUERY' expected", adminUserContext1);
            assertException("cancel query 9223372036854775808", 13, "non-negative integer literal expected as query id", adminUserContext1);
            assertException("cancel query -123456789", 13, "non-negative integer literal expected as query id", adminUserContext1);
            assertException("cancel query 123456789 BLAH", 23, "unexpected token [BLAH]", adminUserContext1);
            assertException("cancel query 12.01f", 15, "unexpected token [.]", adminUserContext1);
            assertException("cancel query 1A", 13, "non-negative integer literal expected as query id", adminUserContext1);
        });
    }

    @Test
    public void testRegularUserCantCancelQueries() throws Exception {
        assertMemoryLeak(() -> {
            assertException("cancel query 123456789", 0, "Write permission denied", regularUserContext1);
        });
    }

    private static class AdminContext extends AllowAllSecurityContext {
        @Override
        public void authorizeAdminAction() {
            // do nothing
        }

        @Override
        public void authorizeCancelQuery() {
            // do nothing
        }

        @Override
        public String getPrincipal() {
            return "admin";
        }
    }

    private static class UserContext extends ReadOnlySecurityContext {
        @Override
        public void authorizeAdminAction() {
            throw new CairoException();
        }

        @Override
        public String getPrincipal() {
            return "bob";
        }
    }
}
