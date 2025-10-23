/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2024 QuestDB
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

import io.questdb.Bootstrap;
import io.questdb.PropBootstrapConfiguration;
import io.questdb.PropertyKey;
import io.questdb.ServerMain;
import io.questdb.cutlass.http.HttpConstants;
import io.questdb.cutlass.http.HttpCookie;
import io.questdb.cutlass.http.HttpSessionStore;
import io.questdb.cutlass.http.client.Fragment;
import io.questdb.cutlass.http.client.HttpClient;
import io.questdb.cutlass.http.client.HttpClientFactory;
import io.questdb.cutlass.http.client.Response;
import io.questdb.mp.SOCountDownLatch;
import io.questdb.std.ObjHashSet;
import io.questdb.std.Rnd;
import io.questdb.std.ThreadLocal;
import io.questdb.std.datetime.MicrosecondClock;
import io.questdb.std.str.StringSink;
import io.questdb.std.str.Utf8s;
import org.jetbrains.annotations.NotNull;
import org.junit.Before;
import org.junit.Test;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Predicate;

import static io.questdb.cutlass.http.HttpConstants.*;
import static io.questdb.test.tools.TestUtils.assertEquals;
import static io.questdb.test.tools.TestUtils.*;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.*;

public class ServerMainHttpAuthConcurrentTest extends AbstractBootstrapTest {
    private static final String PASSWORD = "quest";
    private static final String USER = "admin";
    private static final ThreadLocal<StringSink> tlSink = new ThreadLocal<>(StringSink::new);
    private volatile int numOfThreads;

    @Before
    public void setUp() {
        super.setUp();
        unchecked(() -> createDummyConfiguration(
                PropertyKey.HTTP_USER.getPropertyPath() + "=" + USER,
                PropertyKey.HTTP_PASSWORD.getPropertyPath() + "=" + PASSWORD)
        );
        dbPath.parent().$();
    }

    @Test
    public void testConcurrentMultipleSessionsMultipleClients() throws Exception {
        runTest(false, (threadId, not_used_sessionId, currentMicros, sessionStore, sessionTimeout, rnd, barriers) -> {
            final int numOfIterations = 5 + rnd.nextInt(10);
            try (HttpClient httpClient = HttpClientFactory.newPlainTextInstance()) {
                for (int i = 0; i < numOfIterations; i++) {
                    final String sessionId = createSession(httpClient, sessionStore);
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
                sessionId = createSession(httpClient, sessionStore);
            } catch (Exception e) {
                // although this thread failed, let other threads finish instead of making them wait for a long timeout
                awaitAllBarriers(barriers, 0);
                throw e;
            }

            // wait for all sessions created
            barriers[0].await();

            try (HttpClient httpClient = HttpClientFactory.newPlainTextInstance()) {
                final int numOfIterations = 10 + rnd.nextInt(10);
                for (int i = 0; i < numOfIterations; i++) {
                    final String newSessionId = runSuccessfulQuery(httpClient, sessionId, rnd);
                    if (newSessionId != null) {
                        sessionIds.add(newSessionId);
                    }

                    // select a thread to move the clock over rotateAt
                    synchronized (this) {
                        if ((rnd.nextBoolean() || threadId == 0) && currentMicros.get() < rotateAt) {
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
                    assertNotNull(session = sessionStore.getSession(sessionId));
                    // assert that old and new session ids belong to the same session
                    assertEquals(session, sessionStore.getSession(newSessionId));
                    assertEquals(newSessionId, session.getSessionId());
                } else {
                    // session id was not rotated
                    // we can return after asserting that there is no new session id
                    assertEquals(0, sessionIds.size());
                    return;
                }
            } finally {
                // wait for all checks to be done with the old session id
                // has to happen before it gets evicted
                barriers[2].await();
            }

            final long evictionIncrement = sessionTimeout / 3 + 1_000_000L;
            final long evictAt = currentMicros.get() + sessionTimeout / 3;

            try (HttpClient httpClient = HttpClientFactory.newPlainTextInstance()) {
                final int numOfIterations = 10 + rnd.nextInt(10);
                for (int i = 0; i < numOfIterations; i++) {
                    assertNull(runSuccessfulQuery(httpClient, newSessionId, rnd));

                    // select a thread to move the clock over evictAt
                    synchronized (this) {
                        if ((rnd.nextBoolean() || threadId == 0) && currentMicros.get() < evictAt) {
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
                }
            }

            // assert that old session id is not registered anymore, has been evicted
            assertNull(sessionStore.getSession(sessionId));
            // assert that new session id is still registered
            assertEquals(session, sessionStore.getSession(newSessionId));
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

            final int numOfIterations = 20 + rnd.nextInt(30);
            for (int i = 0; i < numOfIterations; i++) {
                try (HttpClient httpClient = HttpClientFactory.newPlainTextInstance()) {
                    try {
                        runSuccessfulQuery(httpClient, sessionId);
                    } catch (AssertionError e) {
                        assertEquals("expected:<200> but was:<401>", e.getMessage());

                        // this is expected to happen maximum once
                        // let's recover by starting a new session
                        // at the end of the test we will assert that the original session has been closed
                        // and that the num of open sessions are less or equal to the num of threads
                        sessionId = createSession(httpClient, sessionStore);
                        sessionCount.incrementAndGet();
                    }

                    // select a thread to move the clock over expiresAt 
                    synchronized (this) {
                        if ((rnd.nextBoolean() || threadId == 0) && currentMicros.get() < expiresAt) {
                            currentMicros.addAndGet(timeIncrement);
                            LOG.info().$("clock moved to " + currentMicros.get()).$();
                        }
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

            final int numOfIterations = 20 + rnd.nextInt(20);
            for (int i = 0; i < numOfIterations; i++) {
                try (HttpClient httpClient = HttpClientFactory.newPlainTextInstance()) {
                    runSuccessfulQuery(httpClient, sessionId);

                    // select a thread to move the clock over rotateAt 
                    synchronized (this) {
                        if ((rnd.nextBoolean() || threadId == 0) && currentMicros.get() < rotateAt) {
                            currentMicros.addAndGet(timeIncrement);
                            LOG.info().$("clock moved to " + currentMicros.get()).$();
                        }
                    }
                }
            }
        }, numOfSessions -> numOfSessions == 2);
    }

    private static void assertResponse(HttpClient.ResponseHeaders responseHeaders, String expected) {
        final StringSink sink = tlSink.get();
        sink.clear();

        Response chunkedResponse = responseHeaders.getResponse();
        Fragment fragment;
        while ((fragment = chunkedResponse.recv()) != null) {
            Utf8s.utf8ToUtf16(fragment.lo(), fragment.hi(), sink);
        }
        assertEquals(expected, sink);
    }

    private static String assertSessionCookie(HttpClient.ResponseHeaders responseHeaders) {
        final HttpCookie sessionCookie = responseHeaders.getCookie(SESSION_COOKIE_NAME_UTF8);
        assertNotNull(sessionCookie);
        assertEquals(SESSION_COOKIE_NAME, sessionCookie.cookieName.toString());
        assertTrue(sessionCookie.httpOnly);
        assertEquals(-1L, sessionCookie.expires);
        assertEquals(SESSION_COOKIE_MAX_AGE_SECONDS, sessionCookie.maxAge);
        return sessionCookie.value.toString();
    }

    private static void awaitAllBarriers(CyclicBarrier[] barriers, int from) throws Exception {
        for (int i = from; i < barriers.length; i++) {
            barriers[i].await();
        }
    }

    // close the session
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
            assertResponse(responseHeaders, "{\"query\":\"select 1\",\"columns\":[{\"name\":\"1\",\"type\":\"INT\"}],\"timestamp\":-1,\"dataset\":[[1]],\"count\":1}");
        }
    }

    // authenticate with auth header and open the session with 'session=true'
    private static @NotNull String createSession(HttpClient httpClient, HttpSessionStore sessionStore) {
        final String sessionId;
        try (HttpClient.ResponseHeaders responseHeaders = httpClient.newRequest("localhost", HTTP_PORT)
                .GET()
                .url("/exec")
                .query("query", "select 1")
                .query("session", "true")
                .authBasic(USER, PASSWORD)
                .send()
        ) {
            responseHeaders.await();
            assertEquals("200", responseHeaders.getStatusCode());
            sessionId = assertSessionCookie(responseHeaders);
            assertResponse(responseHeaders, "{\"query\":\"select 1\",\"columns\":[{\"name\":\"1\",\"type\":\"INT\"}],\"timestamp\":-1,\"dataset\":[[1]],\"count\":1}");
        }

        assertNotNull(sessionStore.getSession(sessionId));
        return sessionId;
    }

    private static String extractSessionCookie(HttpClient.ResponseHeaders responseHeaders) {
        final HttpCookie sessionCookie = responseHeaders.getCookie(SESSION_COOKIE_NAME_UTF8);
        return sessionCookie != null ? sessionCookie.value.toString() : null;
    }

    private static @NotNull Bootstrap getBootstrapWithMockClock(AtomicLong currentMicros) {
        final MicrosecondClock testClock = currentMicros::get;
        return new Bootstrap(
                new PropBootstrapConfiguration() {
                    @Override
                    public MicrosecondClock getMicrosecondClock() {
                        return testClock;
                    }
                },
                getServerMainArgs()
        );
    }

    // randomized usage of the `main` httpClient
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
            assertResponse(responseHeaders, "Unauthorized\r\n");
        }
    }

    // randomized usage of the `main` httpClient
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
            assertResponse(responseHeaders, "{\"query\":\"select 1\",\"columns\":[{\"name\":\"1\",\"type\":\"INT\"}],\"timestamp\":-1,\"dataset\":[[1]],\"count\":1}");
            return extractSessionCookie(responseHeaders);
        }
    }

    private void runTest(boolean openSession, TestCode test, Predicate<Integer> assertSessions) throws Exception {
        assertMemoryLeak(() -> {
            final Rnd rnd = generateRandom(LOG);

            final AtomicLong currentMicros = new AtomicLong(1761055200000000L);
            final Bootstrap bootstrap = getBootstrapWithMockClock(currentMicros);
            try (final ServerMain serverMain = new ServerMain(bootstrap)) {
                serverMain.start();

                final HttpSessionStore sessionStore = serverMain.getConfiguration().getFactoryProvider().getHttpSessionStore();
                final long sessionTimeout = serverMain.getConfiguration().getHttpServerConfiguration().getHttpContextConfiguration().getSessionTimeout();

                numOfThreads = 5 + rnd.nextInt(5);
                final ConcurrentHashMap<Integer, Throwable> errors = new ConcurrentHashMap<>();
                // these barriers are used to synchronize the test threads when they should reach certain phases together
                // for example, a barrier can be used to make sure all threads created a session before we move onto rotate/evict them
                final CyclicBarrier[] barriers = new CyclicBarrier[]{
                        new CyclicBarrier(numOfThreads),
                        new CyclicBarrier(numOfThreads),
                        new CyclicBarrier(numOfThreads)
                };
                final CyclicBarrier start = new CyclicBarrier(numOfThreads);
                final SOCountDownLatch end = new SOCountDownLatch(numOfThreads);

                final String sessionId;
                if (openSession) {
                    try (HttpClient httpClient = HttpClientFactory.newPlainTextInstance()) {
                        sessionId = createSession(httpClient, sessionStore);
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

                final int numOfSessions = sessionStore.size(USER);
                if (!assertSessions.test(numOfSessions)) {
                    errors.put(-1, new AssertionError("Assert sessions failed"));
                }

                if (!errors.isEmpty()) {
                    for (Map.Entry<Integer, Throwable> entry : errors.entrySet()) {
                        LOG.error().$("Error in thread [id=").$(entry.getKey()).$("] ").$(entry.getValue()).$();
                    }
                    fail("Error in threads");
                }
            }
        });
    }

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
