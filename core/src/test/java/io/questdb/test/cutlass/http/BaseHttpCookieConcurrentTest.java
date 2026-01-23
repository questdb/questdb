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

package io.questdb.test.cutlass.http;

import io.questdb.ServerMain;
import io.questdb.cutlass.http.HttpConstants;
import io.questdb.cutlass.http.HttpCookie;
import io.questdb.cutlass.http.HttpSessionStore;
import io.questdb.cutlass.http.client.HttpClient;
import io.questdb.cutlass.http.client.HttpClientFactory;
import io.questdb.mp.SOCountDownLatch;
import io.questdb.std.ObjHashSet;
import io.questdb.std.Rnd;
import io.questdb.test.AbstractBootstrapTest;
import org.jetbrains.annotations.NotNull;
import org.junit.Assert;
import org.junit.Test;

import java.sql.SQLException;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Predicate;

import static io.questdb.cutlass.http.HttpConstants.SESSION_COOKIE_NAME_UTF8;
import static io.questdb.test.tools.TestUtils.*;

public abstract class BaseHttpCookieConcurrentTest extends AbstractBootstrapTest {
    protected static final String ADMIN_PWD = "quest";
    protected static final String ADMIN_USER = "admin";
    protected volatile int numOfThreads;

    @Test
    public void testConcurrentMultipleSessionsMultipleClients() throws Exception {
        runTest(false, (threadId, not_used_sessionId, currentMicros, sessionStore, sessionTimeout, rnd, barriers) -> {
            final int numOfIterations = 5 + rnd.nextInt(10);
            try (HttpClient httpClient = HttpClientFactory.newPlainTextInstance()) {
                for (int i = 0; i < numOfIterations; i++) {
                    final String sessionId = createSession(httpClient, sessionStore, getUserName(rnd), getPassword());
                    runSuccessfulQuery(httpClient, sessionId, rnd);
                    runFailedQuery(httpClient, sessionId, rnd);
                    closeSession(httpClient, sessionId);
                }
            }
        }, numOfSessions -> numOfSessions == 0);
    }

    @Test
    public void testConcurrentMultipleSessionsRotatedEvicted() throws Exception {
        runTest(false, (threadId, not_used_sessionId, currentMicros, sessionStore, sessionTimeout, rnd, barriers) -> {
            final long rotationIncrement = sessionTimeout / 2 + 1_000_000L;
            final long rotateAt = currentMicros.get() + sessionTimeout / 2;

            final ObjHashSet<String> sessionIds = new ObjHashSet<>();

            final String sessionId;
            try (HttpClient httpClient = HttpClientFactory.newPlainTextInstance()) {
                sessionId = createSession(httpClient, sessionStore, getUserName(rnd), getPassword());
            } catch (Exception e) {
                // although this thread failed, let other threads finish instead of making them wait for a long timeout
                awaitAllBarriers(barriers, 0);
                throw e;
            }

            // wait for all sessions created
            barriers[0].await();

            try (HttpClient httpClient = HttpClientFactory.newPlainTextInstance()) {
                final int numOfIterations = Math.max(1, rnd.nextInt(10));
                for (int i = 0; i < numOfIterations; i++) {
                    // select a thread to move the clock over rotateAt
                    // the 'i == numOfIterations-1' part of the condition guarantees that the clock
                    // will be moved before any thread finishes
                    synchronized (this) {
                        if ((rnd.nextBoolean() || i == numOfIterations - 1) && currentMicros.get() < rotateAt) {
                            LOG.info().$("clock moving from " + currentMicros.get())
                                    .$(" [threadId=").$(threadId)
                                    .$(", rotateAt=").$(rotateAt)
                                    .$("]").$();
                            currentMicros.addAndGet(rotationIncrement);
                            LOG.info().$("clock moved to " + currentMicros.get())
                                    .$(" [threadId=").$(threadId)
                                    .$(", rotateAt=").$(rotateAt)
                                    .$("]").$();
                        }
                    }

                    final String newSessionId = runSuccessfulQuery(httpClient, sessionId, rnd);
                    if (newSessionId != null) {
                        sessionIds.add(newSessionId);
                    }
                }
            } catch (Exception e) {
                // although this thread failed, let other threads finish instead of making them wait for a long timeout
                awaitAllBarriers(barriers, 1);
                throw e;
            }

            // wait for all rotations to happen
            barriers[1].await();

            final String newSessionId;
            final HttpSessionStore.SessionInfo session;
            try {
                if (sessionIds.size() == 1) {
                    newSessionId = sessionIds.getList().getQuick(0);

                    // assert that old session id still works
                    Assert.assertNotNull(session = sessionStore.getSession(sessionId));
                    // assert that old and new session ids belong to the same session
                    Assert.assertEquals(session, sessionStore.getSession(newSessionId));
                    Assert.assertEquals(newSessionId, session.getSessionId());
                } else {
                    // session id was not rotated
                    // we can return after asserting that there is no new session id
                    Assert.assertEquals(0, sessionIds.size());
                    return;
                }
            } catch (Exception e) {
                // although this thread failed, let other threads finish instead of making them wait for a long timeout
                awaitAllBarriers(barriers, 2);
                throw e;
            }

            // wait for all checks to be done with the old session id
            // has to happen before it gets evicted
            barriers[2].await();

            final long evictionIncrement = sessionTimeout / 3 + 1_000_000L;
            final long evictAt = currentMicros.get() + sessionTimeout / 3;

            // wait for all threads to calculate evictAt
            // because one thread could run away and move the clock already before
            // others calculated it, and that will result in rotating newSessionId
            barriers[3].await();

            try (HttpClient httpClient = HttpClientFactory.newPlainTextInstance()) {
                final int numOfIterations = Math.max(1, rnd.nextInt(10));
                for (int i = 0; i < numOfIterations; i++) {
                    // select a thread to move the clock over evictAt
                    // the 'i == numOfIterations-1' part of the condition guarantees that the clock
                    // will be moved before any thread finishes
                    synchronized (this) {
                        if ((rnd.nextBoolean() || i == numOfIterations - 1) && currentMicros.get() < evictAt) {
                            LOG.info().$("clock moving from " + currentMicros.get())
                                    .$(" [threadId=").$(threadId)
                                    .$(", evictAt=").$(evictAt)
                                    .$("]").$();
                            currentMicros.addAndGet(evictionIncrement);
                            LOG.info().$("clock moved to " + currentMicros.get())
                                    .$(" [threadId=").$(threadId)
                                    .$(", evictAt=").$(evictAt)
                                    .$("]").$();
                        }
                    }

                    Assert.assertNull(runSuccessfulQuery(httpClient, newSessionId, rnd));
                }
            }

            // assert that old session id is not registered anymore, has been evicted
            Assert.assertNull(sessionStore.getSession(sessionId));
            // assert that new session id is still registered
            Assert.assertEquals(session, sessionStore.getSession(newSessionId));
        }, numOfSessions -> numOfSessions <= numOfThreads);
    }

    @Test
    public void testConcurrentSingleSessionExpired() throws Exception {
        final AtomicInteger sessionCount = new AtomicInteger();
        runTest(true, (threadId, sessionId, currentMicros, sessionStore, sessionTimeout, rnd, barriers) -> {
            final long timeIncrement = sessionTimeout + 1_000_000L;
            final long expiresAt = currentMicros.get() + sessionTimeout;

            // all threads have to initialize expiresAt before the clock is moved
            barriers[0].await();

            try (HttpClient httpClient = HttpClientFactory.newPlainTextInstance()) {
                final int numOfIterations = Math.max(20, rnd.nextInt(40));
                for (int i = 0; i < numOfIterations; i++) {
                    // select a thread to move the clock over expiresAt
                    // the 'i == numOfIterations-1' part of the condition guarantees that the clock
                    // will be moved before any thread finishes
                    synchronized (this) {
                        if ((rnd.nextBoolean() || i == numOfIterations - 1) && currentMicros.get() < expiresAt) {
                            currentMicros.addAndGet(timeIncrement);
                            LOG.info().$("clock moved to " + currentMicros.get()).$();
                        }
                    }

                    try {
                        runSuccessfulQuery(httpClient, sessionId);
                    } catch (AssertionError e) {
                        assertEquals("expected:<200> but was:<401>", e.getMessage());

                        // this is expected to happen maximum once
                        // let's recover by starting a new session
                        // at the end of the test we will assert that the original session has been closed
                        // and that the num of open sessions are less or equal to the num of threads
                        sessionId = createSession(httpClient, sessionStore, ADMIN_USER, ADMIN_PWD);
                        sessionCount.incrementAndGet();
                    }
                }
            }
        }, numOfSessions -> numOfSessions == sessionCount.get() && numOfSessions <= numOfThreads);
    }

    @Test
    public void testConcurrentSingleSessionRotated() throws Exception {
        runTest(true, (threadId, sessionId, currentMicros, sessionStore, sessionTimeout, rnd, barriers) -> {
            final long timeIncrement = sessionTimeout / 2 + 1_000_000L;
            final long rotateAt = currentMicros.get() + sessionTimeout / 2;

            // all threads have to initialize rotateAt before the clock is moved
            barriers[0].await();

            try (HttpClient httpClient = HttpClientFactory.newPlainTextInstance()) {
                final int numOfIterations = Math.max(20, rnd.nextInt(40));
                for (int i = 0; i < numOfIterations; i++) {
                    // select a thread to move the clock over rotateAt
                    // the 'i == numOfIterations-1' part of the condition guarantees that the clock
                    // will be moved before any thread finishes
                    synchronized (this) {
                        if ((rnd.nextBoolean() || i == numOfIterations - 1) && currentMicros.get() < rotateAt) {
                            currentMicros.addAndGet(timeIncrement);
                            LOG.info().$("clock moved to " + currentMicros.get()).$();
                        }
                    }

                    runSuccessfulQuery(httpClient, sessionId);
                }
            }
        }, numOfSessions -> numOfSessions == 2);
    }

    private static void awaitAllBarriers(CyclicBarrier[] barriers, int from) throws Exception {
        for (int i = from; i < barriers.length; i++) {
            barriers[i].await();
        }
    }

    private static void closeSession(HttpClient httpClient, String sessionId) {
        try (HttpClient.ResponseHeaders responseHeaders = httpClient.newRequest("localhost", HTTP_PORT)
                .GET()
                .url("/exec")
                .query("query", "select 1")
                .query("session", "false")
                .setCookie(HttpConstants.SESSION_COOKIE_NAME, sessionId)
                .send()
        ) {
            responseHeaders.await();
            assertEquals("200", responseHeaders.getStatusCode());
            HttpUtils.assertChunkedBody(responseHeaders, "{\"query\":\"select 1\",\"columns\":[{\"name\":\"1\",\"type\":\"INT\"}],\"timestamp\":-1,\"dataset\":[[1]],\"count\":1}");
        }
    }

    // authenticate with auth header and open the session with 'session=true'
    private static @NotNull String createSession(HttpClient httpClient, HttpSessionStore sessionStore, String userName, String password) {
        final String sessionId;
        try (HttpClient.ResponseHeaders responseHeaders = httpClient.newRequest("localhost", HTTP_PORT)
                .GET()
                .url("/exec")
                .query("query", "select 1")
                .query("session", "true")
                .authBasic(userName, password)
                .send()
        ) {
            responseHeaders.await();
            assertEquals("200", responseHeaders.getStatusCode());
            sessionId = HttpUtils.assertSessionCookie(responseHeaders, false);
            HttpUtils.assertChunkedBody(responseHeaders, "{\"query\":\"select 1\",\"columns\":[{\"name\":\"1\",\"type\":\"INT\"}],\"timestamp\":-1,\"dataset\":[[1]],\"count\":1}");
        }

        Assert.assertNotNull(sessionStore.getSession(sessionId));
        return sessionId;
    }

    private static String extractSessionCookie(HttpClient.ResponseHeaders responseHeaders) {
        final HttpCookie sessionCookie = responseHeaders.getCookie(SESSION_COOKIE_NAME_UTF8);
        return sessionCookie != null ? sessionCookie.value.toString() : null;
    }

    // randomized usage of the `main` httpClient for failed queries
    private static void runFailedQuery(HttpClient httpClient, String sessionId, Rnd rnd) {
        if (rnd.nextBoolean()) {
            runFailedQuery(httpClient, sessionId);
            return;
        }
        try (HttpClient client = HttpClientFactory.newPlainTextInstance()) {
            runFailedQuery(client, sessionId);
        }
    }

    // send wrong session id and no auth header, query should fail
    private static void runFailedQuery(HttpClient httpClient, String sessionId) {
        try (HttpClient.ResponseHeaders responseHeaders = httpClient.newRequest("localhost", HTTP_PORT)
                .GET()
                .url("/exec")
                .query("query", "select 1")
                .setCookie(HttpConstants.SESSION_COOKIE_NAME, sessionId + "whatever")
                .send()
        ) {
            responseHeaders.await();
            assertEquals("401", responseHeaders.getStatusCode());
            HttpUtils.assertChunkedBody(responseHeaders, "Unauthorized\r\n");
        }
    }

    // randomized usage of the `main` httpClient for successful queries
    private static String runSuccessfulQuery(HttpClient httpClient, String sessionId, Rnd rnd) {
        if (rnd.nextBoolean()) {
            return runSuccessfulQuery(httpClient, sessionId);
        }
        try (HttpClient client = HttpClientFactory.newPlainTextInstance()) {
            return runSuccessfulQuery(client, sessionId);
        }
    }

    // use the session id without the auth header to run a query
    private static String runSuccessfulQuery(HttpClient httpClient, String sessionId) {
        try (HttpClient.ResponseHeaders responseHeaders = httpClient.newRequest("localhost", HTTP_PORT)
                .GET()
                .url("/exec")
                .query("query", "select 1")
                .setCookie(HttpConstants.SESSION_COOKIE_NAME, sessionId)
                .send()
        ) {
            responseHeaders.await();
            assertEquals("200", responseHeaders.getStatusCode());
            HttpUtils.assertChunkedBody(responseHeaders, "{\"query\":\"select 1\",\"columns\":[{\"name\":\"1\",\"type\":\"INT\"}],\"timestamp\":-1,\"dataset\":[[1]],\"count\":1}");
            return extractSessionCookie(responseHeaders);
        }
    }

    private void runTest(boolean openSession, TestCode test, Predicate<Integer> assertSessions) throws Exception {
        assertMemoryLeak(() -> {
            final Rnd rnd = generateRandom(LOG);
            final AtomicLong currentMicros = new AtomicLong(1761055200000000L);
            try (final ServerMain serverMain = getServerMain(currentMicros)) {
                serverMain.start();

                initTest(rnd);

                final HttpSessionStore sessionStore = serverMain.getConfiguration().getFactoryProvider().getHttpSessionStore();
                final long sessionTimeout = serverMain.getConfiguration().getHttpServerConfiguration().getHttpContextConfiguration().getSessionTimeout();

                numOfThreads = Math.max(2, rnd.nextInt(4));
                final ConcurrentHashMap<Integer, Throwable> errors = new ConcurrentHashMap<>();
                // these barriers are used to synchronize the test threads when they should reach certain phases together
                // for example, a barrier can be used to make sure all threads created a session before we move onto rotate/evict them
                final CyclicBarrier[] barriers = new CyclicBarrier[]{
                        new CyclicBarrier(numOfThreads),
                        new CyclicBarrier(numOfThreads),
                        new CyclicBarrier(numOfThreads),
                        new CyclicBarrier(numOfThreads)
                };
                final CyclicBarrier start = new CyclicBarrier(numOfThreads);
                final SOCountDownLatch end = new SOCountDownLatch(numOfThreads);

                final String sessionId;
                if (openSession) {
                    try (HttpClient httpClient = HttpClientFactory.newPlainTextInstance()) {
                        sessionId = createSession(httpClient, sessionStore, ADMIN_USER, ADMIN_PWD);
                    }
                } else {
                    sessionId = null;
                }

                for (int i = 0; i < numOfThreads; i++) {
                    final int threadId = i;
                    new Thread(() -> {
                        await(start);
                        try {
                            test.run(threadId, sessionId, currentMicros, sessionStore, sessionTimeout, rnd, barriers);
                        } catch (Throwable th) {
                            th.printStackTrace(System.out);
                            errors.put(threadId, th);
                        }
                        end.countDown();
                    }).start();
                }
                end.await();

                final int numOfSessions = getNumOfSessions(sessionStore);
                if (!assertSessions.test(numOfSessions)) {
                    errors.put(-1, new AssertionError("Assert sessions failed"));
                }

                if (!errors.isEmpty()) {
                    for (Map.Entry<Integer, Throwable> entry : errors.entrySet()) {
                        LOG.error().$("Error in thread [id=").$(entry.getKey()).$("] ").$(entry.getValue()).$();
                    }
                    Assert.fail("Error in threads");
                }
            }
        });
    }

    protected abstract int getNumOfSessions(HttpSessionStore sessionStore);

    protected abstract String getPassword();

    protected abstract ServerMain getServerMain(AtomicLong currentMicros);

    protected abstract String getUserName(Rnd rnd);

    protected abstract void initTest(Rnd rnd) throws SQLException;

    private interface TestCode {
        void run(
                int threadId,
                String sessionId,
                AtomicLong currentMicros,
                HttpSessionStore sessionStore,
                long sessionTimeout,
                Rnd rnd,
                CyclicBarrier[] barriers
        ) throws Exception;
    }
}
