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
import io.questdb.std.ObjList;
import io.questdb.std.Rnd;
import io.questdb.std.ThreadLocal;
import io.questdb.std.datetime.MicrosecondClock;
import io.questdb.std.str.StringSink;
import io.questdb.std.str.Utf8s;
import io.questdb.test.tools.TestUtils;
import org.jetbrains.annotations.NotNull;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Predicate;

import static io.questdb.cutlass.http.HttpConstants.*;
import static io.questdb.test.tools.TestUtils.await;
import static io.questdb.test.tools.TestUtils.generateRandom;
import static org.junit.Assert.*;

public class ServerMainHttpAuthConcurrentTest extends AbstractBootstrapTest {
    private static final String PASSWORD = "quest";
    private static final String USER = "admin";
    private static final ThreadLocal<StringSink> tlSink = new ThreadLocal<>(StringSink::new);
    private volatile int numOfThreads;

    @Before
    public void setUp() {
        super.setUp();
        TestUtils.unchecked(() -> createDummyConfiguration(
                PropertyKey.HTTP_USER.getPropertyPath() + "=" + USER,
                PropertyKey.HTTP_PASSWORD.getPropertyPath() + "=" + PASSWORD)
        );
        dbPath.parent().$();
    }

    @Test
    public void testConcurrentMultipleSessionsMultipleClients() throws Exception {
        runTest(false, (not_used_sessionId, currentMicros, sessionStore, sessionTimeout, rnd, barrier) -> {
            final int numOfIterations = 10 + rnd.nextInt(20);
            for (int i = 0; i < numOfIterations; i++) {
                try (HttpClient httpClient = HttpClientFactory.newPlainTextInstance()) {
                    final String sessionId = createSession(httpClient, sessionStore);
                    runSuccessfulQuery(httpClient, sessionId, rnd);
                    runFailedQuery(httpClient, sessionId, rnd);
                    closeSession(httpClient, sessionId);
                }
            }
        }, sessions -> sessions.size() == 0);
    }

    @Test
    public void testConcurrentMultipleSessionsRotatedEvicted() throws Exception {
        runTest(false, (not_used_sessionId, currentMicros, sessionStore, sessionTimeout, rnd, rotationsFinished) -> {
            final long rotationIncrement = sessionTimeout / 2 + 1_000_000L;
            final long rotateAt = currentMicros.get() + sessionTimeout / 2;

            final ObjList<String> sessionIds = new ObjList<>();

            final String sessionId;
            try (HttpClient httpClient = HttpClientFactory.newPlainTextInstance()) {
                sessionId = createSession(httpClient, sessionStore);
            }

            final int numOfIterations = 10 + rnd.nextInt(10);
            for (int i = 0; i < numOfIterations; i++) {
                try (HttpClient httpClient = HttpClientFactory.newPlainTextInstance()) {
                    final String newSessionId = runSuccessfulQuery(httpClient, sessionId, rnd);
                    if (newSessionId != null) {
                        sessionIds.add(newSessionId);
                    }

                    // randomly select a thread to move the clock over rotateAt 
                    synchronized (this) {
                        if (rnd.nextBoolean() && currentMicros.get() < rotateAt) {
                            currentMicros.addAndGet(rotationIncrement);
                        }
                    }
                }
            }

            // wait for all rotations
            rotationsFinished.await();

            final String newSessionId;
            final HttpSessionStore.SessionInfo session;
            if (sessionIds.size() == 1) {
                newSessionId = sessionIds.getQuick(0);

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

            final long evictionIncrement = sessionTimeout / 3;
            final long evictAt = currentMicros.get() + evictionIncrement;

            for (int i = 0; i < numOfIterations; i++) {
                try (HttpClient httpClient = HttpClientFactory.newPlainTextInstance()) {
                    assertNull(runSuccessfulQuery(httpClient, newSessionId, rnd));

                    // randomly select a thread to move the clock over evictAt
                    synchronized (this) {
                        if (rnd.nextBoolean() && currentMicros.get() < evictAt) {
                            currentMicros.addAndGet(evictionIncrement);
                        }
                    }
                }
            }

            // assert that old session id is not registered anymore, has been evicted
            assertNull(sessionStore.getSession(sessionId));
            // assert that new session id is still registered
            assertEquals(session, sessionStore.getSession(newSessionId));
        }, sessions -> sessions.size() <= numOfThreads);
    }

    @Test
    public void testConcurrentSingleSessionExpired() throws Exception {
        final AtomicInteger sessionCount = new AtomicInteger();
        runTest(true, (sessionId, currentMicros, sessionStore, sessionTimeout, rnd, barrier) -> {
            final long timeIncrement = sessionTimeout + 1_000_000L;
            final long expiresAt = currentMicros.get() + sessionTimeout;

            final int numOfIterations = 20 + rnd.nextInt(30);
            for (int i = 0; i < numOfIterations; i++) {
                try (HttpClient httpClient = HttpClientFactory.newPlainTextInstance()) {
                    try {
                        runSuccessfulQuery(httpClient, sessionId);
                    } catch (AssertionError e) {
                        Assert.assertEquals("expected:<200> but was:<401>", e.getMessage());

                        // this is expected to happen maximum once
                        // let's recover by starting a new session
                        // at the end of the test we will assert that the original session has been closed
                        // and that the num of open sessions are less or equal to the num of threads
                        sessionId = createSession(httpClient, sessionStore);
                        sessionCount.incrementAndGet();
                    }

                    // randomly select a thread to move the clock over expiresAt 
                    synchronized (this) {
                        if (rnd.nextBoolean() && currentMicros.get() < expiresAt) {
                            currentMicros.addAndGet(timeIncrement);
                        }
                    }
                }
            }
        }, sessions -> sessions.size() == sessionCount.get() && sessions.size() <= numOfThreads);
    }

    @Test
    public void testConcurrentSingleSessionRotated() throws Exception {
        runTest(true, (sessionId, currentMicros, sessionStore, sessionTimeout, rnd, barrier) -> {
            final long timeIncrement = sessionTimeout / 2 + 1_000_000L;
            final long rotateAt = currentMicros.get() + sessionTimeout / 2;

            final int numOfIterations = 20 + rnd.nextInt(20);
            for (int i = 0; i < numOfIterations; i++) {
                try (HttpClient httpClient = HttpClientFactory.newPlainTextInstance()) {
                    runSuccessfulQuery(httpClient, sessionId);

                    // randomly select a thread to move the clock over rotateAt 
                    synchronized (this) {
                        if (rnd.nextBoolean() && currentMicros.get() < rotateAt) {
                            currentMicros.addAndGet(timeIncrement);
                        }
                    }
                }
            }
        }, sessions -> sessions.size() == 1);
    }

    private static void assertResponse(HttpClient.ResponseHeaders responseHeaders, String expected) {
        final StringSink sink = tlSink.get();
        sink.clear();

        Response chunkedResponse = responseHeaders.getResponse();
        Fragment fragment;
        while ((fragment = chunkedResponse.recv()) != null) {
            Utf8s.utf8ToUtf16(fragment.lo(), fragment.hi(), sink);
        }
        TestUtils.assertEquals(expected, sink);
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
            TestUtils.assertEquals("200", responseHeaders.getStatusCode());
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
            TestUtils.assertEquals("200", responseHeaders.getStatusCode());
            sessionId = assertSessionCookie(responseHeaders);
            assertResponse(responseHeaders, "{\"query\":\"select 1\",\"columns\":[{\"name\":\"1\",\"type\":\"INT\"}],\"timestamp\":-1,\"dataset\":[[1]],\"count\":1}");
        }

        final ObjList<HttpSessionStore.SessionInfo> sessions = sessionStore.getSessions(USER);
        Assert.assertNotNull(sessions);
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
            TestUtils.assertEquals("401", responseHeaders.getStatusCode());
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
            TestUtils.assertEquals("200", responseHeaders.getStatusCode());
            assertResponse(responseHeaders, "{\"query\":\"select 1\",\"columns\":[{\"name\":\"1\",\"type\":\"INT\"}],\"timestamp\":-1,\"dataset\":[[1]],\"count\":1}");
            return extractSessionCookie(responseHeaders);
        }
    }

    private void runTest(boolean openSession, TestCode test, Predicate<ObjList<HttpSessionStore.SessionInfo>> assertSessions) throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            final Rnd rnd = generateRandom(LOG);

            final AtomicLong currentMicros = new AtomicLong(1761055200000000L);
            final Bootstrap bootstrap = getBootstrapWithMockClock(currentMicros);
            try (final ServerMain serverMain = new ServerMain(bootstrap)) {
                serverMain.start();

                final HttpSessionStore sessionStore = serverMain.getConfiguration().getFactoryProvider().getHttpSessionStore();
                final long sessionTimeout = serverMain.getConfiguration().getHttpServerConfiguration().getHttpContextConfiguration().getSessionTimeout();

                numOfThreads = 5 + rnd.nextInt(5);
                final ConcurrentHashMap<Integer, Throwable> errors = new ConcurrentHashMap<>();
                final CyclicBarrier barrier = new CyclicBarrier(numOfThreads);
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
                    final int threadIndex = i;
                    new Thread(() -> {
                        await(start);
                        try {
                            test.run(sessionId, currentMicros, sessionStore, sessionTimeout, rnd, barrier);
                        } catch (Throwable th) {
                            th.printStackTrace(System.out);
                            errors.put(threadIndex, th);
                        }
                        end.countDown();
                    }).start();
                }
                end.await();

                try (HttpClient httpClient = HttpClientFactory.newPlainTextInstance()) {
                    final ObjList<HttpSessionStore.SessionInfo> sessions = sessionStore.getSessions(USER);
                    if (!assertSessions.test(sessions)) {
                        errors.put(-1, new AssertionError("Assert sessions failed"));
                    }
                    if (sessions != null) {
                        // start iterating from the back, so index is not getting messed up
                        // closeSession() always removes the last element of the list
                        for (int i = sessions.size() - 1; i > -1; i--) {
                            closeSession(httpClient, sessions.getQuick(i).getSessionId());
                        }
                    }
                }

                if (!errors.isEmpty()) {
                    for (Map.Entry<Integer, Throwable> entry : errors.entrySet()) {
                        LOG.error().$("Error in thread [id=").$(entry.getKey()).$("] ").$(entry.getValue()).$();
                    }
                    fail("Error in threads");
                }

                final ObjList<HttpSessionStore.SessionInfo> sessions = sessionStore.getSessions(USER);
                Assert.assertNotNull(sessions);
                Assert.assertEquals(0, sessions.size());
            }
        });
    }

    private interface TestCode {
        void run(String sessionId, AtomicLong currentMicros, HttpSessionStore sessionStore, long sessionTimeout, Rnd rnd, CyclicBarrier barrier) throws Exception;
    }
}
