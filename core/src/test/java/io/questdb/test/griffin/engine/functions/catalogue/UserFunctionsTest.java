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

package io.questdb.test.griffin.engine.functions.catalogue;

import io.questdb.PropertyKey;
import io.questdb.cairo.SecurityContext;
import io.questdb.cairo.security.AllowAllSecurityContext;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.SqlExecutionContextImpl;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.mp.WorkerPool;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class UserFunctionsTest extends AbstractCairoTest {

    private static final Log LOG = LogFactory.getLog(UserFunctionsTest.class);
    private static final String SQL = "SELECT current_user(), session_user() FROM long_sequence(";

    @Override
    @Before
    public void setUp() {
        // Small page frames so the 1000-row parallel-filter table splits into several frames dispatched
        // across the workers, keeping the async filter plan deterministic instead of hinging on the
        // ambient default (1M rows = one frame, where async selection is marginal and suite-order
        // sensitive). Harmless for the other single-context tests here. See RuntimeConstFunctionTest.
        setProperty(PropertyKey.CAIRO_SQL_PAGE_FRAME_MAX_ROWS, 100);
        setProperty(PropertyKey.CAIRO_PAGE_FRAME_REDUCE_QUEUE_CAPACITY, 16);
        super.setUp();
    }

    @Test
    public void testUserFunctionsAreSharedAcrossParallelWorkers() throws Exception {
        final WorkerPool pool = new WorkerPool(() -> 4);
        TestUtils.execute(
                pool,
                (engine, compiler, executionContext) -> {
                    engine.execute("CREATE TABLE users AS (SELECT x FROM long_sequence(1000))", executionContext);

                    final CountingSecurityContext securityContext = new CountingSecurityContext("alice");
                    ((SqlExecutionContextImpl) executionContext).with(securityContext);

                    // The per-row "x > 0" conjunct (true for every row) is load-bearing: current_user()
                    // and session_user() are runtime constants, so a WHERE built only from them is a
                    // whole-predicate runtime constant and collapses to a single per-execution gate
                    // (RuntimeConstGateRecordCursorFactory) that never opens a parallel filter. Anchoring
                    // the predicate to a column keeps the optimizer on the async filter, which is where the
                    // point holds: the thread-safe user functions are shared across all 4 workers -- one
                    // filter instance, one identity resolution -- rather than copied per worker.
                    final String filterQuery = "SELECT x FROM users "
                            + "WHERE x > 0 AND current_user() = 'alice' AND session_user() = 'alice'";
                    assertQuery(filterQuery)
                            .withEngine(engine)
                            .withContext(executionContext)
                            .noLeakCheck()
                            .assertsPlanContaining("Async");

                    try (RecordCursorFactory factory = compiler.compile(filterQuery, executionContext).getRecordCursorFactory()) {
                        securityContext.resetCounts();
                        try (RecordCursor cursor = getBaseCursor(factory, executionContext)) {
                            int rowCount = 0;
                            while (cursor.hasNext()) {
                                rowCount++;
                            }
                            Assert.assertEquals(1000, rowCount);
                        }
                    }
                    assertIdentityReadCounts(securityContext, "parallel filter", 1, 1);

                    final String groupByQuery = "SELECT current_user(), session_user(), count() FROM users "
                            + "GROUP BY current_user(), session_user()";
                    assertQuery(groupByQuery)
                            .withEngine(engine)
                            .withContext(executionContext)
                            .noLeakCheck()
                            .assertsPlanContaining("Async");

                    try (RecordCursorFactory factory = compiler.compile(groupByQuery, executionContext).getRecordCursorFactory()) {
                        securityContext.resetCounts();
                        try (RecordCursor cursor = getBaseCursor(factory, executionContext)) {
                            final Record record = cursor.getRecord();
                            Assert.assertTrue(cursor.hasNext());
                            TestUtils.assertEquals("alice", record.getStrA(0));
                            TestUtils.assertEquals("alice", record.getStrA(1));
                            Assert.assertEquals(1000, record.getLong(2));
                            Assert.assertFalse(cursor.hasNext());
                        }
                    }
                    assertIdentityReadCounts(securityContext, "parallel group by", 1, 2);
                },
                configuration,
                LOG
        );
    }

    @Test
    public void testUserFunctionsDriveRuntimeConstantOptimizerPaths() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE timezone_events (ts TIMESTAMP) TIMESTAMP(ts)");
            final String queryPrefix = "SELECT count(), ts FROM timezone_events "
                    + "SAMPLE BY 1h ALIGN TO CALENDAR TIME ZONE ";

            ((SqlExecutionContextImpl) sqlExecutionContext).with(new CountingSecurityContext("UTC"));
            assertQuery(queryPrefix + "current_user()")
                    .noLeakCheck()
                    .assertsPlanContaining("timestamp_floor_utc('1h',ts,null,'00:00',current_user())");
            assertQuery(queryPrefix + "session_user()")
                    .noLeakCheck()
                    .assertsPlanContaining("timestamp_floor_utc('1h',ts,null,'00:00',session_user())");
        });
    }

    @Test
    public void testUserFunctionsResolveOncePerCursorNotPerRow() throws Exception {
        // current_user() and session_user() do not depend on the record -- which is exactly what their
        // factories have always declared through isRuntimeConstant(). The functions themselves did not, so
        // nothing downstream could act on it and the value was re-read on EVERY row. On the enterprise
        // dispatching proxy each read routes through a cached delegate: an atomic load, a volatile load and a
        // virtual call, per row, for a value that cannot change mid-traversal.
        //
        // Pin the exact cursor-initialization contract. Before the snapshot fix this grew with row count;
        // without the thread-safety declaration it grows with the parallel worker count instead.
        assertMemoryLeak(() -> {
            final CountingSecurityContext small = readPrincipalsOver(10);
            final CountingSecurityContext large = readPrincipalsOver(10_000);

            assertIdentityReadCounts(small, "small projection", 1, 1);
            assertIdentityReadCounts(large, "large projection", 1, 1);
        });
    }

    @Test
    public void testUserFunctionsRefreshOnEveryCursor() throws Exception {
        // Resolving in init() is only sound because init() runs on every getCursor(). Pin that, because the
        // failure mode is ugly: the HTTP select cache is shared across connections, so a compiled factory is
        // routinely reused by a different user. A snapshot that survived into the next cursor would report
        // the previous user's identity to this one.
        assertMemoryLeak(() -> {
            try (RecordCursorFactory factory = select(SQL + "1)")) {
                assertReportsUser(factory, "alice");
                assertReportsUser(factory, "bob");
                // and back again, so it tracks the context rather than merely changing once
                assertReportsUser(factory, "alice");
            }
        });
    }

    private void assertReportsUser(RecordCursorFactory factory, String user) throws SqlException {
        ((SqlExecutionContextImpl) sqlExecutionContext).with(new CountingSecurityContext(user));
        try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
            final Record record = cursor.getRecord();
            Assert.assertTrue(cursor.hasNext());
            TestUtils.assertEquals(user, record.getStrA(0));
            TestUtils.assertEquals(user, record.getStrA(1));
            Assert.assertFalse(cursor.hasNext());
        }
    }

    private static void assertIdentityReadCounts(
            CountingSecurityContext securityContext,
            String queryShape,
            int expectedPrincipalCalls,
            int expectedSessionPrincipalCalls
    ) {
        Assert.assertEquals(queryShape + " must resolve current_user() exactly once per function instance",
                expectedPrincipalCalls, securityContext.principalCalls);
        Assert.assertEquals(queryShape + " must resolve session_user() exactly once per function instance",
                expectedSessionPrincipalCalls, securityContext.sessionPrincipalCalls);
    }

    private static RecordCursor getBaseCursor(
            RecordCursorFactory factory,
            SqlExecutionContext executionContext
    ) throws SqlException {
        // QueryProgress reads the principal for registry/log metadata. Bypass that outer wrapper so the
        // counters isolate the user functions initialized by the execution factory.
        final RecordCursorFactory baseFactory = factory.getBaseFactory();
        Assert.assertNotNull(baseFactory);
        return baseFactory.getCursor(executionContext);
    }

    private CountingSecurityContext readPrincipalsOver(int rowCount) throws SqlException {
        final CountingSecurityContext securityContext = new CountingSecurityContext("bob");
        ((SqlExecutionContextImpl) sqlExecutionContext).with(securityContext);
        try (RecordCursorFactory factory = select(SQL + rowCount + ")")) {
            securityContext.resetCounts();
            try (RecordCursor cursor = getBaseCursor(factory, sqlExecutionContext)) {
                final Record record = cursor.getRecord();
                int rows = 0;
                while (cursor.hasNext()) {
                    TestUtils.assertEquals("bob", record.getStrA(0));
                    TestUtils.assertEquals("bob", record.getStrA(1));
                    rows++;
                }
                Assert.assertEquals(rowCount, rows);
            }
        }
        return securityContext;
    }

    private static class CountingSecurityContext extends AllowAllSecurityContext {
        final String user;
        int principalCalls;
        int sessionPrincipalCalls;

        CountingSecurityContext(String user) {
            this.user = user;
        }

        @Override
        public CharSequence getPrincipal() {
            principalCalls++;
            return user;
        }

        @Override
        public CharSequence getSessionPrincipal() {
            sessionPrincipalCalls++;
            return user;
        }

        @Override
        protected SecurityContext newPrincipalContext(CharSequence principal) {
            return this;
        }

        void resetCounts() {
            principalCalls = 0;
            sessionPrincipalCalls = 0;
        }
    }
}
