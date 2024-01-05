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
import io.questdb.griffin.CompiledQuery;
import io.questdb.griffin.SqlCompiler;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.SqlExecutionContextImpl;
import io.questdb.mp.SOCountDownLatch;
import io.questdb.std.FlyweightMessageContainer;
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

        readOnlyUserContext = new SqlExecutionContextImpl(engine, 1).with(new ReadOnlyUserContext(), null);
        readOnlyUserContext.with(new AtomicBooleanCircuitBreaker());

        regularUserContext = new SqlExecutionContextImpl(engine, 1).with(new RegularUserContext(), null);
        regularUserContext.with(new AtomicBooleanCircuitBreaker());

        adminUserContext1 = new SqlExecutionContextImpl(engine, 1).with(new AdminContext(), null);
        adminUserContext1.with(new AtomicBooleanCircuitBreaker());

        adminUserContext2 = new SqlExecutionContextImpl(engine, 1).with(new AdminContext(), null);
        adminUserContext2.with(new AtomicBooleanCircuitBreaker());
    }

    @Test
    public void testAdminUserCantCancelQueriesNotInRegistry() throws Exception {
        assertMemoryLeak(() -> {
            try (RecordCursorFactory f = select("select cancel_query(123456789) res", adminUserContext1);
                 RecordCursor cursor = f.getCursor(adminUserContext1)) {
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
                        SqlExecutionContextImpl context = new SqlExecutionContextImpl(engine, 1).with(new ReadOnlyUserContext(), null);
                        context.with(new AtomicBooleanCircuitBreaker());

                        try (SqlCompiler compiler = engine.getSqlCompiler()) {
                            assertQuery(compiler,
                                    "t\n1\n",
                                    query,
                                    null, context, true, false);
                            Assert.fail("Query should have been cancelled");
                        } catch (Exception e) {
                            if (!e.getMessage().contains("cancelled by user")) {
                                error.set(e);
                            }
                        }
                    } finally {
                        stopped.countDown();
                    }
                }, "query_thread_" + i).start();
            }

            started.await();

            //wait for both queries to appear in registry
            try (RecordCursorFactory factory = select("select count(*) from query_activity() where query = '" + query + "'")) {
                while (true) {
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
                assertSql("query\twas_cancelled\n" +
                                "select 1 t from long_sequence(1) where sleep(120000)\ttrue\n" +
                                "select 1 t from long_sequence(1) where sleep(120000)\ttrue\n",
                        "select query, cancel_query(query_id) was_cancelled from query_activity() where query = '" + query + "'");
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

            assertException("select cancel_query()", 7, "unexpected argument for function: cancel_query");
            assertException("select cancel_query(null)", 20, "non-negative integer literal expected as query id");
            assertException("select cancel_query(-1)", 20, "non-negative integer literal expected as query id");
            assertException("select cancel_query(12.01f)", 7, "unexpected argument for function: cancel_query. expected args: (LONG). actual args: (FLOAT constant)");
        });
    }

    @Test
    public void testRegularUserCantCancelOtherUsersCommands() throws Exception {
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
                        Os.sleep(1);
                    }
                }

                try {
                    // readonly user can't cancel any commands
                    assertException("select cancel_query(" + queryId + ")", 7, "Write permission denied", readOnlyUserContext);

                    // regular user can't cancel other user's commands
                    assertException("select cancel_query(" + queryId + ")", 7, "Access denied for bob [built-in admin user required]", regularUserContext);
                } finally {
                    ddl("cancel query " + queryId, adminUserContext2);
                }
            } finally {
                stopped.await();
                if (error.get() != null) {
                    throw error.get();
                }
            }
        });
    }

    protected static void assertException(CharSequence sql, int errorPos, CharSequence contains, SqlExecutionContext sqlExecutionContext) throws Exception {
        try {
            try (SqlCompiler compiler = engine.getSqlCompiler()) {
                compiler.setFullFatJoins(false);
                CompiledQuery cq = compiler.compile(sql, sqlExecutionContext);
                try (
                        RecordCursorFactory factory = cq.getRecordCursorFactory();
                        RecordCursor cursor = factory.getCursor(sqlExecutionContext)
                ) {
                    cursor.hasNext();
                    cursor.getRecord().getBool(0);
                }
            }
            Assert.fail();
        } catch (Throwable e) {
            if (e instanceof FlyweightMessageContainer) {
                TestUtils.assertContains(((FlyweightMessageContainer) e).getFlyweightMessage(), contains);
                if (errorPos > -1) {
                    Assert.assertEquals(errorPos, ((FlyweightMessageContainer) e).getPosition());
                }
            } else {
                throw e;
            }
        }
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

    private static class ReadOnlyUserContext extends ReadOnlySecurityContext {
        @Override
        public void authorizeAdminAction() {
            throw CairoException.authorization().put("Access denied for ").put(getPrincipal()).put(" [built-in admin user required]");
        }

        @Override
        public String getPrincipal() {
            return "bob";
        }
    }

    private static class RegularUserContext extends AllowAllSecurityContext {
        @Override
        public void authorizeAdminAction() {
            throw CairoException.authorization().put("Access denied for ").put(getPrincipal()).put(" [built-in admin user required]");
        }

        @Override
        public void authorizeCancelQuery() {
        }

        @Override
        public String getPrincipal() {
            return "bob";
        }
    }
}
