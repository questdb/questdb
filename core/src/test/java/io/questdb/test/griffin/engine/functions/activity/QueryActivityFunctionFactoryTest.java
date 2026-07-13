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
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.griffin.QueryRegistry;
import io.questdb.griffin.SqlCompiler;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContextImpl;
import io.questdb.mp.SOCountDownLatch;
import io.questdb.std.Chars;
import io.questdb.std.ObjList;
import io.questdb.std.Os;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

import java.lang.reflect.Field;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

public class QueryActivityFunctionFactoryTest extends AbstractCairoTest {
    private SqlExecutionContextImpl adminUserContext1;
    private SqlExecutionContextImpl adminUserContext2;
    private SqlExecutionContextImpl regularUserContext1;

    @Override
    public void setUp() {
        super.setUp();

        node1.setProperty(PropertyKey.DEV_MODE_ENABLED, true);

        regularUserContext1 = new SqlExecutionContextImpl(engine, 1).with(new UserContext());
        regularUserContext1.with(new AtomicBooleanCircuitBreaker(engine));

        adminUserContext1 = new SqlExecutionContextImpl(engine, 1).with(new AdminContext());
        adminUserContext1.with(new AtomicBooleanCircuitBreaker(engine));

        adminUserContext2 = new SqlExecutionContextImpl(engine, 1).with(new AdminContext());
        adminUserContext2.with(new AtomicBooleanCircuitBreaker(engine));
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
                    assertQuery(query)
                            .withContext(regularUserContext1)
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
                    try (final RecordCursorFactory factory = CairoEngine.select(compiler, activityQuery, adminUserContext1)) {
                        // admin can see admins command
                        while (error.get() == null) {
                            try (RecordCursor cursor = factory.getCursor(adminUserContext1)) {
                                if (cursor.hasNext()) {
                                    queryId = cursor.getRecord().getLong(0);
                                    break;
                                }
                            }
                        }
                    }

                    execute("cancel query " + queryId, adminUserContext1);
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
    public void testAdminUserCanNotCancelQueriesNotInRegistry() throws Exception {
        assertMemoryLeak(() -> {
            try {
                execute("cancel query 123456789", adminUserContext1);
                Assert.fail();
            } catch (SqlException e) {
                TestUtils.assertContains(e.getMessage(), "query to cancel not found in registry [id=123456789]");
            }
        });
    }

    @Test
    public void testListQueriesWithNoQueryRunningShowsOwnSelect() throws Exception {
        assertQuery("select username, query from query_activity()")
                .noRandomAccess()
                .returns("""
                        username\tquery
                        admin\tselect username, query from query_activity()
                        """);
        assertMemoryLeak(() -> assertQuery("select username, query from query_activity()")
                .noRandomAccess()
                .returns("""
                        username\tquery
                        admin\tselect username, query from query_activity()
                        """));
    }

    @Test
    public void testMemoryColumnsWithoutLimit() throws Exception {
        // With no per-query limit configured (the default), memory_limit is NULL
        // (0 means unlimited) while memory_used is still populated for the running
        // top-level query: its bound tracker reports a non-negative byte count.
        assertQuery("select memory_used >= 0 used_ok, memory_limit is null limit_null from query_activity()")
                .noRandomAccess()
                .returns("used_ok\tlimit_null\n" +
                        "true\ttrue\n");
    }

    @Test
    public void testNonAdminCanNotSeeOtherUsersCommands() throws Exception {
        assertMemoryLeak(() -> {
            final String query = "select 1 t from long_sequence(1) where sleep(120000)";

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
                        // admin can see admins command
                        while (error.get() == null) {
                            try (RecordCursor cursor = factory.getCursor(adminUserContext2)) {
                                if (cursor.hasNext()) {
                                    queryId = cursor.getRecord().getLong(0);
                                    break;
                                }
                            }
                        }
                    }

                    // regular user can't see admins command
                    assertQuery(activityQuery)
                            .noLeakCheck()
                            .withCompiler(compiler)
                            .withContext(regularUserContext1)
                            .noRandomAccess()
                            .returns("query_id\tquery\n");

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
    public void testQueryActivitySnapshotUnderChurn() throws Exception {
        // A reader runs query_activity() in a tight loop while several producers
        // register and unregister queries, recycling pooled Entry objects under
        // the reader. The optimistic snapshot in QueryActivityRecord.of()/copy()
        // must never expose a torn query string nor crash: every producer row the
        // reader accepts must carry one of the complete, known producer texts.
        assertMemoryLeak(() -> {
            final QueryRegistry registry = engine.getQueryRegistry();
            final int producerCount = 4;
            final int iterations = 20_000;

            // Producer texts alternate between a short and a long, distinctly-keyed
            // string so recycling an entry changes the StringSink length and
            // exercises copy()'s shrink/grow rejection. They start with 'A'/'B' so
            // the reader's own "SELECT ... query_activity()" text can never be
            // mistaken for a producer row.
            final String[][] producerTexts = new String[producerCount][2];
            final Set<String> validQueries = new HashSet<>();
            for (int p = 0; p < producerCount; p++) {
                final StringBuilder longText = new StringBuilder();
                for (int k = 0, n = 40 + p * 7; k < n; k++) {
                    longText.append('B').append(p);
                }
                producerTexts[p][0] = "A" + p;
                producerTexts[p][1] = longText.toString();
                validQueries.add(producerTexts[p][0]);
                validQueries.add(producerTexts[p][1]);
            }

            final AtomicInteger runningProducers = new AtomicInteger(producerCount);
            final AtomicLong producerRowsObserved = new AtomicLong();
            final AtomicReference<Throwable> fault = new AtomicReference<>();
            final CyclicBarrier startBarrier = new CyclicBarrier(producerCount + 1);
            final ObjList<Thread> threads = new ObjList<>();

            for (int p = 0; p < producerCount; p++) {
                final int slot = p;
                final Thread thread = new Thread(() -> {
                    try (SqlExecutionContextImpl context = new SqlExecutionContextImpl(engine, 1).with(AllowAllSecurityContext.INSTANCE)) {
                        startBarrier.await();
                        for (int i = 0; i < iterations && fault.get() == null; i++) {
                            final long queryId = registry.register(producerTexts[slot][i & 1], context);
                            // brief window for the reader to snapshot the entry
                            for (int j = 0; j < 8; j++) {
                                Os.pause();
                            }
                            registry.unregister(queryId, context);
                        }
                    } catch (Throwable t) {
                        fault.compareAndSet(null, t);
                    } finally {
                        runningProducers.decrementAndGet();
                    }
                }, "producer-" + slot);
                threads.add(thread);
            }

            final Thread reader = new Thread(() -> {
                try (
                        SqlExecutionContextImpl context = new SqlExecutionContextImpl(engine, 1).with(AllowAllSecurityContext.INSTANCE);
                        SqlCompiler compiler = engine.getSqlCompiler();
                        RecordCursorFactory factory = CairoEngine.select(compiler, "SELECT query_id, worker_pool, username, state, query FROM query_activity()", context)
                ) {
                    startBarrier.await();
                    while (runningProducers.get() > 0 && fault.get() == null) {
                        try (RecordCursor cursor = factory.getCursor(context)) {
                            final Record record = cursor.getRecord();
                            while (cursor.hasNext()) {
                                // force every snapshot column to be materialized
                                record.getLong(0);
                                record.getStrA(1);
                                record.getStrA(2);
                                record.getStrA(3);
                                final CharSequence query = record.getStrA(4);
                                if (query == null) {
                                    continue;
                                }
                                final String text = Chars.toString(query);
                                // producer texts are "A<digit>" or "B<digit>...": require the
                                // leading digit so an unrelated registry entry (e.g. an internal
                                // "ALTER ...") is never mistaken for a producer row, and charAt(1)
                                // stays in bounds.
                                if (text.length() >= 2 && (text.charAt(0) == 'A' || text.charAt(0) == 'B')
                                        && text.charAt(1) >= '0' && text.charAt(1) <= '9') {
                                    if (!validQueries.contains(text)) {
                                        throw new AssertionError("query_activity exposed a torn query: '" + text + "'");
                                    }
                                    producerRowsObserved.incrementAndGet();
                                }
                            }
                        }
                    }
                } catch (Throwable t) {
                    fault.compareAndSet(null, t);
                }
            }, "query_activity_reader");
            threads.add(reader);

            for (int i = 0, n = threads.size(); i < n; i++) {
                threads.getQuick(i).start();
            }
            for (int i = 0, n = threads.size(); i < n; i++) {
                threads.getQuick(i).join(120_000);
                Assert.assertFalse("worker thread hung", threads.getQuick(i).isAlive());
            }

            if (fault.get() != null) {
                throw new AssertionError("worker thread failed", fault.get());
            }
            Assert.assertTrue("reader never observed a live query", producerRowsObserved.get() > 0);
        });
    }

    @Test
    public void testQueryActivityRejectsGrowingStringSnapshot() throws Exception {
        assertInjectedPoolNameRejected("SELECT growing pool name", new GrowingCharSequence("pool"));
    }

    @Test
    public void testQueryActivityRejectsRowWhenEntryRetiresDuringCopy() throws Exception {
        assertMemoryLeak(() -> {
            final QueryRegistry registry = engine.getQueryRegistry();
            final CountDownLatch copyStarted = new CountDownLatch(1);
            final CountDownLatch releaseCopy = new CountDownLatch(1);
            final AtomicBoolean sawRow = new AtomicBoolean(true);
            final AtomicReference<Throwable> fault = new AtomicReference<>();

            try (
                    SqlExecutionContextImpl ownerContext = new SqlExecutionContextImpl(engine, 1).with(AllowAllSecurityContext.INSTANCE);
                    SqlExecutionContextImpl readerContext = new SqlExecutionContextImpl(engine, 1).with(AllowAllSecurityContext.INSTANCE)
            ) {
                final String query = "SELECT retiring during copy";
                final long queryId = registry.register(query, ownerContext);
                setPoolName(registry.getEntry(queryId), new BlockingCharSequence("pool", copyStarted, releaseCopy));

                final Thread readerThread = new Thread(() -> {
                    try {
                        sawRow.set(queryActivityHasRow(query, readerContext));
                    } catch (Throwable t) {
                        fault.compareAndSet(null, t);
                    }
                }, "query_activity_reader");

                readerThread.start();
                try {
                    Assert.assertTrue("query_activity did not start copying the row", copyStarted.await(5, TimeUnit.SECONDS));
                    registry.unregister(queryId, ownerContext);
                } finally {
                    releaseCopy.countDown();
                    readerThread.join(5_000);
                    if (registry.getEntry(queryId) != null) {
                        registry.unregister(queryId, ownerContext);
                    }
                }
                Assert.assertFalse("reader thread hung", readerThread.isAlive());
                if (fault.get() != null) {
                    throw new AssertionError("reader failed", fault.get());
                }
                Assert.assertFalse(sawRow.get());
            }
        });
    }

    @Test
    public void testQueryActivityRejectsShrinkingStringSnapshot() throws Exception {
        assertInjectedPoolNameRejected("SELECT shrinking pool name", new ShrinkingCharSequence());
    }

    @Test
    public void testQueryActivitySkipsCancellingEntry() throws Exception {
        assertMemoryLeak(() -> {
            final QueryRegistry registry = engine.getQueryRegistry();
            final CountDownLatch cancellerInGuard = new CountDownLatch(1);
            final CountDownLatch releaseCanceller = new CountDownLatch(1);
            final CountDownLatch cancellerDone = new CountDownLatch(1);
            final AtomicBoolean cancelResult = new AtomicBoolean();
            final AtomicReference<Throwable> fault = new AtomicReference<>();

            // The owner principal blocks the first time cancel() evaluates
            // Chars.equals(entry.principal, cancellerPrincipal): that comparison
            // reads entry.principal.length() inside the first CANCELLING guard, so
            // the entry stays CANCELLING while the reader snapshots the registry.
            // query_activity() must reject the non-active lifecycle and skip the row.
            final BlockingCharSequence ownerPrincipal = new BlockingCharSequence("owner", cancellerInGuard, releaseCanceller);

            try (
                    SqlExecutionContextImpl ownerContext = new SqlExecutionContextImpl(engine, 1).with(new CharSequencePrincipalSecurityContext(ownerPrincipal));
                    SqlExecutionContextImpl cancelContext = new SqlExecutionContextImpl(engine, 1).with(new PrincipalSecurityContext("admin"));
                    SqlExecutionContextImpl readerContext = new SqlExecutionContextImpl(engine, 1).with(AllowAllSecurityContext.INSTANCE)
            ) {
                final String query = "SELECT cancelling query";
                final long queryId = registry.register(query, ownerContext);
                final Thread cancellerThread = new Thread(() -> {
                    try {
                        cancelResult.set(registry.cancel(queryId, cancelContext));
                    } catch (Throwable t) {
                        fault.compareAndSet(null, t);
                    } finally {
                        cancellerDone.countDown();
                    }
                }, "query_activity_canceller");

                cancellerThread.start();
                try {
                    Assert.assertTrue("canceller did not enter the CANCELLING guard", cancellerInGuard.await(5, TimeUnit.SECONDS));
                    assertQueryActivityDoesNotReturn(query, readerContext);
                } finally {
                    releaseCanceller.countDown();
                    Assert.assertTrue("canceller did not finish", cancellerDone.await(5, TimeUnit.SECONDS));
                    cancellerThread.join(5_000);
                    if (registry.getEntry(queryId) != null) {
                        registry.unregister(queryId, ownerContext);
                    }
                }
                Assert.assertFalse("canceller thread hung", cancellerThread.isAlive());
                if (fault.get() != null) {
                    throw new AssertionError("canceller failed", fault.get());
                }
                // Once released, the cross-user admin cancel runs to completion.
                Assert.assertTrue("admin cancel should succeed after release", cancelResult.get());
            }
        });
    }

    @Test
    public void testQueryIdToCancelMustBeNonNegativeInteger() throws Exception {
        assertMemoryLeak(() -> {
            assertQuery("cancel ")
                    .withContext(adminUserContext1)
                    .noLeakCheck()
                    .fails(6, "'QUERY' expected");
            assertQuery("cancel SQL 1")
                    .withContext(adminUserContext1)
                    .noLeakCheck()
                    .fails(7, "'QUERY' expected");
            assertQuery("cancel query 9223372036854775808")
                    .withContext(adminUserContext1)
                    .noLeakCheck()
                    .fails(13, "non-negative integer literal expected as query id");
            assertQuery("cancel query -123456789")
                    .withContext(adminUserContext1)
                    .noLeakCheck()
                    .fails(13, "non-negative integer literal expected as query id");
            assertQuery("cancel query 123456789 BLAH")
                    .withContext(adminUserContext1)
                    .noLeakCheck()
                    .fails(23, "unexpected token [BLAH]");
            assertQuery("cancel query 12.01f")
                    .withContext(adminUserContext1)
                    .noLeakCheck()
                    .fails(15, "unexpected token [.]");
            assertQuery("cancel query 1A")
                    .withContext(adminUserContext1)
                    .noLeakCheck()
                    .fails(13, "non-negative integer literal expected as query id");
        });
    }

    @Test
    public void testRegularUserCanNotCancelQueries() throws Exception {
        assertQuery("cancel query 123456789")
                .withContext(regularUserContext1)
                .fails(13, "Query cancellation is disabled");
    }

    private void assertInjectedPoolNameRejected(String query, CharSequence poolName) throws Exception {
        assertMemoryLeak(() -> {
            final QueryRegistry registry = engine.getQueryRegistry();
            try (
                    SqlExecutionContextImpl ownerContext = new SqlExecutionContextImpl(engine, 1).with(AllowAllSecurityContext.INSTANCE);
                    SqlExecutionContextImpl readerContext = new SqlExecutionContextImpl(engine, 1).with(AllowAllSecurityContext.INSTANCE)
            ) {
                final long queryId = registry.register(query, ownerContext);
                try {
                    setPoolName(registry.getEntry(queryId), poolName);
                    assertQueryActivityDoesNotReturn(query, readerContext);
                } finally {
                    if (registry.getEntry(queryId) != null) {
                        registry.unregister(queryId, ownerContext);
                    }
                }
            }
        });
    }

    private void assertQueryActivityDoesNotReturn(String query, SqlExecutionContextImpl context) throws SqlException {
        Assert.assertFalse(queryActivityHasRow(query, context));
    }

    private boolean queryActivityHasRow(String query, SqlExecutionContextImpl context) throws SqlException {
        try (
                SqlCompiler compiler = engine.getSqlCompiler();
                RecordCursorFactory factory = CairoEngine.select(compiler, "SELECT query_id FROM query_activity() WHERE query = '" + query + "'", context);
                RecordCursor cursor = factory.getCursor(context)
        ) {
            return cursor.hasNext();
        }
    }

    private static void await(CountDownLatch latch, String message) {
        try {
            Assert.assertTrue(message, latch.await(5, TimeUnit.SECONDS));
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new AssertionError(message, e);
        }
    }

    private static void setPoolName(QueryRegistry.Entry entry, CharSequence poolName) throws Exception {
        Assert.assertNotNull(entry);
        final Field field = QueryRegistry.Entry.class.getDeclaredField("poolName");
        field.setAccessible(true);
        field.set(entry, poolName);
    }

    private static class AdminContext extends AllowAllSecurityContext {
        @Override
        public String getPrincipal() {
            return "admin";
        }
    }

    private static class BlockingCharSequence implements CharSequence {
        private final AtomicBoolean blocked = new AtomicBoolean();
        private final CountDownLatch entered;
        private final CountDownLatch release;
        private final String value;

        private BlockingCharSequence(String value, CountDownLatch entered, CountDownLatch release) {
            this.value = value;
            this.entered = entered;
            this.release = release;
        }

        @Override
        public char charAt(int index) {
            return value.charAt(index);
        }

        @Override
        public int length() {
            if (blocked.compareAndSet(false, true)) {
                entered.countDown();
                await(release, "blocked char sequence was not released");
            }
            return value.length();
        }

        @Override
        public CharSequence subSequence(int start, int end) {
            return value.subSequence(start, end);
        }
    }

    private static class CharSequencePrincipalSecurityContext extends AllowAllSecurityContext {
        private final CharSequence principal;

        private CharSequencePrincipalSecurityContext(CharSequence principal) {
            this.principal = principal;
        }

        @Override
        public CharSequence getPrincipal() {
            return principal;
        }
    }

    private static class GrowingCharSequence implements CharSequence {
        private final AtomicInteger lengthCalls = new AtomicInteger();
        private final String value;

        private GrowingCharSequence(String value) {
            this.value = value;
        }

        @Override
        public char charAt(int index) {
            return value.charAt(index);
        }

        @Override
        public int length() {
            return value.length() + Math.min(lengthCalls.getAndIncrement(), 1);
        }

        @Override
        public CharSequence subSequence(int start, int end) {
            return value.subSequence(start, end);
        }
    }

    private static class PrincipalSecurityContext extends AllowAllSecurityContext {
        private final String principal;

        private PrincipalSecurityContext(String principal) {
            this.principal = principal;
        }

        @Override
        public String getPrincipal() {
            return principal;
        }
    }

    private static class ShrinkingCharSequence implements CharSequence {
        @Override
        public char charAt(int index) {
            if (index > 0) {
                throw new IndexOutOfBoundsException();
            }
            return 'x';
        }

        @Override
        public int length() {
            return 2;
        }

        @Override
        public CharSequence subSequence(int start, int end) {
            throw new UnsupportedOperationException();
        }
    }

    private static class UserContext extends ReadOnlySecurityContext {
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
