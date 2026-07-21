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

package io.questdb.test.griffin.engine.functions.activity;

import io.questdb.PropertyKey;
import io.questdb.cairo.CairoEngine;
import io.questdb.cairo.CairoException;
import io.questdb.cairo.security.AllowAllSecurityContext;
import io.questdb.cairo.security.ReadOnlySecurityContext;
import io.questdb.cairo.sql.AtomicBooleanCircuitBreaker;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.griffin.SqlCompiler;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.SqlExecutionContextImpl;
import io.questdb.mp.SOCountDownLatch;
import io.questdb.std.Os;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

import java.util.concurrent.atomic.AtomicReference;

public class CancelQueryFunctionFactoryTest extends AbstractCairoTest {
    private SqlExecutionContextImpl adminUserContext1;
    private SqlExecutionContextImpl adminUserContext2;
    private SqlExecutionContextImpl readOnlyUserContext;
    private SqlExecutionContextImpl regularUserContext;

    @Override
    public void setUp() {
        super.setUp();

        node1.setProperty(PropertyKey.DEV_MODE_ENABLED, true);

        readOnlyUserContext = new SqlExecutionContextImpl(engine, 1).with(new ReadOnlyUserContext());
        readOnlyUserContext.with(new AtomicBooleanCircuitBreaker(engine));

        regularUserContext = new SqlExecutionContextImpl(engine, 1).with(new RegularUserContext());
        regularUserContext.with(new AtomicBooleanCircuitBreaker(engine));

        adminUserContext1 = new SqlExecutionContextImpl(engine, 1).with(new AdminContext());
        adminUserContext1.with(new AtomicBooleanCircuitBreaker(engine));

        adminUserContext2 = new SqlExecutionContextImpl(engine, 1).with(new AdminContext());
        adminUserContext2.with(new AtomicBooleanCircuitBreaker(engine));
    }

    @Test
    public void testAdminUserCanNotCancelQueriesNotInRegistry() throws Exception {
        assertMemoryLeak(() -> {
            try (
                    RecordCursorFactory f = select("select cancel_query(123456789) res", adminUserContext1);
                    RecordCursor cursor = f.getCursor(adminUserContext1)
            ) {
                assertCursor("false\n", cursor, f.getMetadata(), false);
            }
        });
    }

    @Test
    public void testCancelMultipleQueries() throws Exception {
        assertMemoryLeak(() -> {
            final String query = "select 1 t from long_sequence(1) where sleep(120000)";

            SOCountDownLatch started = new SOCountDownLatch(2);
            SOCountDownLatch stopped = new SOCountDownLatch(2);
            AtomicReference<Exception> error = new AtomicReference<>();

            for (int i = 0; i < 2; i++) {
                new Thread(() -> {
                    started.countDown();
                    try {
                        SqlExecutionContextImpl context = new SqlExecutionContextImpl(engine, 1).with(new ReadOnlyUserContext());
                        context.with(new AtomicBooleanCircuitBreaker(engine));

                        assertQuery(query)
                                .withContext(context)
                                .noLeakCheck()
                                .returnsOnce("t\n1\n");
                        Assert.fail("Query should have been cancelled");
                    } catch (Exception e) {
                        if (!e.getMessage().contains("cancelled by user")) {
                            error.set(e);
                        }
                    } finally {
                        stopped.countDown();
                    }
                }, "query_thread_" + i).start();
            }

            started.await();

            // wait for both queries to appear in registry
            try (RecordCursorFactory factory = select("select count(*) from query_activity() where query = '" + query + "'")) {
                while (error.get() == null) {
                    try (RecordCursor cursor = factory.getCursor(adminUserContext1)) {
                        cursor.hasNext();
                        if (cursor.getRecord().getLong(0) == 2) {
                            break;
                        }
                    }
                    Os.sleep(1);
                }
            }

            try {
                assertQuery("select query, cancel_query(query_id) was_cancelled from query_activity() where query = '" + query + "'")
                        .noLeakCheck()
                        .returnsOnce("""
                                query\twas_cancelled
                                select 1 t from long_sequence(1) where sleep(120000)\ttrue
                                select 1 t from long_sequence(1) where sleep(120000)\ttrue
                                """);
            } finally {
                stopped.await();
            }
            if (error.get() != null) {
                throw error.get();
            }
        });
    }

    @Test
    public void testQueryIdToCancelMustBeNonNegativeInteger() throws Exception {
        assertMemoryLeak(() -> {
            assertQuery("select cancel_query()")
                    .noLeakCheck()
                    .fails(7, "function `cancel_query` requires arguments: cancel_query(LONG)");
            assertQuery("select cancel_query(null)")
                    .noLeakCheck()
                    .fails(20, "non-negative integer literal expected as query id");
            assertQuery("select cancel_query(-1)")
                    .noLeakCheck()
                    .fails(20, "non-negative integer literal expected as query id");
            assertQuery("select cancel_query(12.01f)")
                    .noLeakCheck()
                    .fails(20, "argument type mismatch for function `cancel_query` at #1 expected: LONG, actual: FLOAT");
        });
    }

    @Test
    public void testRegularUserCanNotCancelOtherUsersCommands() throws Exception {
        assertMemoryLeak(() -> {
            final String query = "select 1 t from long_sequence(1) where sleep(5000)";

            SOCountDownLatch started = new SOCountDownLatch(1);
            SOCountDownLatch stopped = new SOCountDownLatch(1);
            AtomicReference<Exception> error = new AtomicReference<>();

            new Thread(() -> {
                started.countDown();
                try {
                    assertQuery(query)
                            .withContext(adminUserContext1)
                            .noLeakCheck()
                            .returnsOnce("t\n1\n");
                    Assert.fail("Query should have been cancelled");
                } catch (Exception e) {
                    if (!e.getMessage().contains("cancelled by user")) {
                        error.set(e);
                    }
                } finally {
                    stopped.countDown();
                }
            }, "query_thread").start();

            started.await();

            try {
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
                            Os.sleep(1);
                        }
                    }

                    // readonly user can't cancel any commands
                    assertQuery("select cancel_query(" + queryId + ")")
                            .withContext(readOnlyUserContext)
                            .noLeakCheck()
                            .fails(7, "Query cancellation is disabled");

                    // regular user can't cancel other user's commands
                    assertQuery("select cancel_query(" + queryId + ")")
                            .withContext(regularUserContext)
                            .noLeakCheck()
                            .fails(7, "Access denied for bob [SQL ENGINE ADMIN]");

                    execute("cancel query " + queryId, adminUserContext2);
                }

            } finally {
                stopped.await();
            }

            if (error.get() != null) {
                throw error.get();
            }
        });
    }

    @Test
    public void testValidateCancelQueryDoesNotCancel() throws Exception {
        assertMemoryLeak(() -> {
            final SqlExecutionContextImpl ctx = (SqlExecutionContextImpl) sqlExecutionContext;

            // Validation compiles CANCEL QUERY but skips the registry lookup, so it succeeds
            // even for an id that is not registered - no "query to cancel not found" error.
            ctx.setValidationOnly(true);
            try (SqlCompiler compiler = engine.getSqlCompiler()) {
                compiler.compile("cancel query 123456789", ctx);
            } finally {
                ctx.setValidationOnly(false);
            }

            // Real execution performs the lookup and fails because the id is not registered.
            try (SqlCompiler compiler = engine.getSqlCompiler()) {
                compiler.compile("cancel query 123456789", ctx);
                Assert.fail("expected query-not-found error");
            } catch (SqlException e) {
                TestUtils.assertContains(e.getFlyweightMessage(), "query to cancel not found in registry");
            }
        });
    }

    private static class AdminContext extends AllowAllSecurityContext {
        @Override
        public String getPrincipal() {
            return "admin";
        }
    }

    private static class ReadOnlyUserContext extends ReadOnlySecurityContext {
        @Override
        public String getPrincipal() {
            return "bob";
        }
    }

    private static class RegularUserContext extends AllowAllSecurityContext {
        @Override
        public void authorizeSqlEngineAdmin() {
            throw CairoException.authorization().put("Access denied for ").put(getPrincipal()).put(" [SQL ENGINE ADMIN]");
        }

        @Override
        public String getPrincipal() {
            return "bob";
        }
    }
}
