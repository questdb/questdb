package io.questdb.test.cutlass.http;

import io.questdb.ServerMain;
import io.questdb.cutlass.http.HttpSessionStore;
import io.questdb.cutlass.http.client.HttpClient;
import io.questdb.test.AbstractBootstrapTest;

import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Supplier;

import static io.questdb.cutlass.http.HttpConstants.SESSION_COOKIE_NAME;
import static io.questdb.test.cutlass.http.HttpUtils.*;
import static org.junit.Assert.*;

public abstract class BaseHttpCookieTest extends AbstractBootstrapTest {
    protected static final String PASSWORD = "quest";
    protected static final String USER = "admin";

    protected boolean isSecure() {
        return false;
    }

    protected void testExpiredSessionEvicted(Supplier<ServerMain> serverMainSupplier, Supplier<HttpClient> httpClientSupplier, AtomicLong currentMicros) throws Exception {
        assertMemoryLeak(() -> {
            try (final ServerMain questdb = serverMainSupplier.get()) {
                questdb.start();

                final String sessionId;
                final HttpSessionStore sessionStore = questdb.getConfiguration().getFactoryProvider().getHttpSessionStore();
                final long sessionTimeout = questdb.getConfiguration().getHttpServerConfiguration().getHttpContextConfiguration().getSessionTimeout();

                // authenticate with username + pwd, and pass 'session=true'
                try (HttpClient httpClient = httpClientSupplier.get()) {
                    HttpClient.ResponseHeaders responseHeaders = newHttpRequest(httpClient, HTTP_PORT, "SELECT x FROM long_sequence(3)", USER, PASSWORD, "true");

                    // successful authentication with username + pwd
                    awaitStatusCode(responseHeaders, "200");

                    // assert that session cookie is set
                    sessionId = assertSessionCookie(responseHeaders, isSecure());

                    assertChunkedBody(responseHeaders, "{" +
                            "\"query\":\"SELECT x FROM long_sequence(3)\"," +
                            "\"columns\":[{\"name\":\"x\",\"type\":\"LONG\"}]," +
                            "\"timestamp\":-1," +
                            "\"dataset\":[[1],[2],[3]]," +
                            "\"count\":3" +
                            "}");
                }

                final HttpSessionStore.SessionInfo session = assertSession(sessionStore, sessionId, currentMicros.get(), sessionTimeout);

                // move the clock after session timeout
                currentMicros.addAndGet(sessionTimeout + 25_000_000L);

                final String newSessionId;

                // with Authorization header and expired session cookie
                try (HttpClient httpClient = httpClientSupplier.get()) {
                    HttpClient.ResponseHeaders responseHeaders = newHttpRequest(httpClient, HTTP_PORT, "SELECT x FROM long_sequence(2)", SESSION_COOKIE_NAME, sessionId);

                    // successful authentication with the session cookie
                    awaitStatusCode(responseHeaders, "200");

                    // assert that a new session has been created for the user
                    newSessionId = assertSessionCookie(responseHeaders, isSecure());
                    assertNotEquals(sessionId, newSessionId);

                    assertChunkedBody(responseHeaders, "{" +
                            "\"query\":\"SELECT x FROM long_sequence(2)\"," +
                            "\"columns\":[{\"name\":\"x\",\"type\":\"LONG\"}]," +
                            "\"timestamp\":-1," +
                            "\"dataset\":[[1],[2]]," +
                            "\"count\":2" +
                            "}");
                }

                // assert that no session is present in the store with the old id
                assertNull(sessionStore.getSession(sessionId));

                // assert that a new session is created
                final HttpSessionStore.SessionInfo newSession = sessionStore.getSession(newSessionId);
                assertNotNull(newSession);
                assertNotEquals(session, newSession);
            }
        });
    }

    protected void testRotatedSessionDestroyed(Supplier<ServerMain> serverMainSupplier, Supplier<HttpClient> httpClientSupplier, AtomicLong currentMicros, boolean closeWithNewSessionId) throws Exception {
        assertMemoryLeak(() -> {
            try (final ServerMain questdb = serverMainSupplier.get()) {
                questdb.start();

                final String sessionId1;
                final long sessionTimeout = questdb.getConfiguration().getHttpServerConfiguration().getHttpContextConfiguration().getSessionTimeout();
                final HttpSessionStore sessionStore = questdb.getConfiguration().getFactoryProvider().getHttpSessionStore();

                // authenticate with username + pwd, and pass 'session=true'
                try (HttpClient httpClient = httpClientSupplier.get()) {
                    HttpClient.ResponseHeaders responseHeaders = newHttpRequest(httpClient, HTTP_PORT, "SELECT x FROM long_sequence(3)", USER, PASSWORD, "true");

                    // successful authentication with username + pwd
                    awaitStatusCode(responseHeaders, "200");

                    // assert that session cookie is set
                    sessionId1 = assertSessionCookie(responseHeaders, isSecure());

                    assertChunkedBody(responseHeaders, "{" +
                            "\"query\":\"SELECT x FROM long_sequence(3)\"," +
                            "\"columns\":[{\"name\":\"x\",\"type\":\"LONG\"}]," +
                            "\"timestamp\":-1," +
                            "\"dataset\":[[1],[2],[3]]," +
                            "\"count\":3" +
                            "}");
                }

                final HttpSessionStore.SessionInfo session = assertSession(sessionStore, sessionId1, currentMicros.get(), sessionTimeout);

                // move the clock after rotation time but before session timeout
                currentMicros.addAndGet(sessionTimeout / 2 + 1_000_000L);

                final String sessionId2;

                // no Authorization header, only the session cookie is sent back to the server
                try (HttpClient httpClient = httpClientSupplier.get()) {
                    HttpClient.ResponseHeaders responseHeaders = newHttpRequest(httpClient, HTTP_PORT, "SELECT x FROM long_sequence(2)", null, null, null, SESSION_COOKIE_NAME, sessionId1);

                    // successful authentication with the session cookie
                    awaitStatusCode(responseHeaders, "200");

                    // assert that a new session has been created for the user
                    sessionId2 = assertSessionCookie(responseHeaders, isSecure());
                    assertNotEquals(sessionId1, sessionId2);

                    assertChunkedBody(responseHeaders, "{" +
                            "\"query\":\"SELECT x FROM long_sequence(2)\"," +
                            "\"columns\":[{\"name\":\"x\",\"type\":\"LONG\"}]," +
                            "\"timestamp\":-1," +
                            "\"dataset\":[[1],[2]]," +
                            "\"count\":2" +
                            "}");
                }

                // assert that the old entry is still present in the store
                assertNotNull(sessionStore.getSession(sessionId1));

                // assert that a new entry is present too, and they are the same session object
                final HttpSessionStore.SessionInfo session2 = sessionStore.getSession(sessionId2);
                assertNotNull(session2);
                assertEquals(session, session2);

                // close the session
                try (HttpClient httpClient = httpClientSupplier.get()) {
                    HttpClient.ResponseHeaders responseHeaders = newHttpRequest(httpClient, HTTP_PORT, "SELECT x FROM long_sequence(2)", null, null, "false", SESSION_COOKIE_NAME, closeWithNewSessionId ? sessionId2 : sessionId1);

                    // successful authentication with the session cookie
                    awaitStatusCode(responseHeaders, "200");

                    // assert that no new session has been created for the user
                    assertNoSessionCookie(responseHeaders);

                    assertChunkedBody(responseHeaders, "{" +
                            "\"query\":\"SELECT x FROM long_sequence(2)\"," +
                            "\"columns\":[{\"name\":\"x\",\"type\":\"LONG\"}]," +
                            "\"timestamp\":-1," +
                            "\"dataset\":[[1],[2]]," +
                            "\"count\":2" +
                            "}");
                }

                // assert that session is closed
                assertNull(sessionStore.getSession(sessionId2));
                assertEquals(session, sessionStore.getSession(sessionId1));
                assertTrue(session.isInvalid());

                // new session id fails
                try (HttpClient httpClient = httpClientSupplier.get()) {
                    HttpClient.ResponseHeaders responseHeaders = newHttpRequest(httpClient, HTTP_PORT, "SELECT x FROM long_sequence(2)", null, null, null, SESSION_COOKIE_NAME, sessionId2);
                    awaitStatusCode(responseHeaders, "401");
                    assertChunkedBody(responseHeaders, "Unauthorized\r\n");
                }

                // old session id fails
                try (HttpClient httpClient = httpClientSupplier.get()) {
                    HttpClient.ResponseHeaders responseHeaders = newHttpRequest(httpClient, HTTP_PORT, "SELECT x FROM long_sequence(2)", null, null, null, SESSION_COOKIE_NAME, sessionId1);
                    awaitStatusCode(responseHeaders, "401");
                    assertChunkedBody(responseHeaders, "Unauthorized\r\n");
                }
            }
        });
    }

    protected void testSessionCookieAuthentication(Supplier<ServerMain> serverMainSupplier, Supplier<HttpClient> httpClientSupplier) throws Exception {
        assertMemoryLeak(() -> {
            try (final ServerMain questdb = serverMainSupplier.get()) {
                String sessionId;

                questdb.start();

                // authenticate with username + pwd, and pass 'session=true'
                try (HttpClient httpClient = httpClientSupplier.get()) {
                    HttpClient.ResponseHeaders responseHeaders = newHttpRequest(httpClient, HTTP_PORT, "SELECT x FROM long_sequence(3)", USER, PASSWORD, "true");

                    // successful authentication with username + pwd
                    awaitStatusCode(responseHeaders, "200");

                    // assert that session cookie is set
                    sessionId = assertSessionCookie(responseHeaders, isSecure());

                    assertChunkedBody(responseHeaders, "{" +
                            "\"query\":\"SELECT x FROM long_sequence(3)\"," +
                            "\"columns\":[{\"name\":\"x\",\"type\":\"LONG\"}]," +
                            "\"timestamp\":-1," +
                            "\"dataset\":[[1],[2],[3]]," +
                            "\"count\":3" +
                            "}");
                }

                for (int i = 0; i < 10; i++) {
                    // no Authorization header, only the session cookie is sent back to the server
                    try (HttpClient httpClient = httpClientSupplier.get()) {
                        HttpClient.ResponseHeaders responseHeaders = newHttpRequest(httpClient, HTTP_PORT, "SELECT x FROM long_sequence(2)", null, null, null, SESSION_COOKIE_NAME, sessionId);

                        // successful authentication with the session cookie
                        awaitStatusCode(responseHeaders, "200");

                        // assert that no new session cookie has been set in the response
                        assertNoSessionCookie(responseHeaders);

                        assertChunkedBody(responseHeaders, "{" +
                                "\"query\":\"SELECT x FROM long_sequence(2)\"," +
                                "\"columns\":[{\"name\":\"x\",\"type\":\"LONG\"}]," +
                                "\"timestamp\":-1," +
                                "\"dataset\":[[1],[2]]," +
                                "\"count\":2" +
                                "}");
                    }
                }

                // send wrong session id
                try (HttpClient httpClient = httpClientSupplier.get();
                     HttpClient.ResponseHeaders responseHeaders = newHttpRequest(httpClient, HTTP_PORT, "SELECT 3", null, null, null, SESSION_COOKIE_NAME, sessionId + "anything")) {

                    // failed authentication with the session cookie
                    awaitStatusCode(responseHeaders, "401");

                    // assert that no new session cookie has been set in the response
                    assertNoSessionCookie(responseHeaders);

                    assertChunkedBody(responseHeaders, "Unauthorized\r\n");
                }

                // closing session
                try (HttpClient httpClient = httpClientSupplier.get()) {
                    HttpClient.ResponseHeaders responseHeaders = newHttpRequest(httpClient, HTTP_PORT, "SELECT 1", null, null, "false", SESSION_COOKIE_NAME, sessionId);

                    // successful authentication with the session cookie
                    awaitStatusCode(responseHeaders, "200");

                    // assert that session cookie is set
                    assertNoSessionCookie(responseHeaders);

                    assertChunkedBody(responseHeaders, "{" +
                            "\"query\":\"SELECT 1\"," +
                            "\"columns\":[{\"name\":\"1\",\"type\":\"INT\"}]," +
                            "\"timestamp\":-1," +
                            "\"dataset\":[[1]]," +
                            "\"count\":1" +
                            "}");
                }
            }
        });
    }

    protected void testSessionCookieParsingError(Supplier<ServerMain> serverMainSupplier, Supplier<HttpClient> httpClientSupplier) throws Exception {
        assertMemoryLeak(() -> {
            try (final ServerMain questdb = serverMainSupplier.get()) {
                questdb.start();

                // authenticate with username + pwd, and pass 'session=true'
                try (HttpClient httpClient = httpClientSupplier.get()) {
                    HttpClient.ResponseHeaders responseHeaders = newHttpRequest(httpClient, HTTP_PORT, "SELECT x FROM long_sequence(3)", USER, PASSWORD, "true");

                    // successful authentication with username + pwd
                    awaitStatusCode(responseHeaders, "200");

                    // assert that session cookie is set
                    final String sessionId = assertSessionCookie(responseHeaders, isSecure());
                    assertNotNull(sessionId);
                    assertFalse(sessionId.isEmpty());

                    assertChunkedBody(responseHeaders, "{" +
                            "\"query\":\"SELECT x FROM long_sequence(3)\"," +
                            "\"columns\":[{\"name\":\"x\",\"type\":\"LONG\"}]," +
                            "\"timestamp\":-1," +
                            "\"dataset\":[[1],[2],[3]]," +
                            "\"count\":3" +
                            "}");
                }

                // with Authorization header, the session cookie is invalid
                try (HttpClient httpClient = httpClientSupplier.get()) {
                    HttpClient.ResponseHeaders responseHeaders = newHttpRequest(httpClient, HTTP_PORT, "SELECT x FROM long_sequence(2)", SESSION_COOKIE_NAME, "");

                    // successful authentication with the session cookie
                    awaitStatusCode(responseHeaders, "400");

                    // assert that no new session cookie has been set in the response
                    assertSessionCookieDeleted(responseHeaders);

                    assertChunkedBody(responseHeaders, "Empty cookie value [qdb_session=]\r\n");
                }
            }
        });
    }

    protected void testSessionCookieServerRestart(Supplier<ServerMain> serverMainSupplier, Supplier<HttpClient> httpClientSupplier) throws Exception {
        assertMemoryLeak(() -> {
            String sessionId;

            try (final ServerMain questdb = serverMainSupplier.get()) {
                questdb.start();

                // authenticate with username + pwd, and pass 'session=true'
                try (HttpClient httpClient = httpClientSupplier.get()) {
                    HttpClient.ResponseHeaders responseHeaders = newHttpRequest(httpClient, HTTP_PORT, "SELECT x FROM long_sequence(3)", USER, PASSWORD, "true");

                    // successful authentication with username + pwd
                    awaitStatusCode(responseHeaders, "200");

                    // assert that session cookie is set
                    sessionId = assertSessionCookie(responseHeaders, isSecure());

                    assertChunkedBody(responseHeaders, "{" +
                            "\"query\":\"SELECT x FROM long_sequence(3)\"," +
                            "\"columns\":[{\"name\":\"x\",\"type\":\"LONG\"}]," +
                            "\"timestamp\":-1," +
                            "\"dataset\":[[1],[2],[3]]," +
                            "\"count\":3" +
                            "}");
                }
            }

            // restart server, session lost on server side
            try (final ServerMain questdb = serverMainSupplier.get()) {
                questdb.start();

                // no Authorization header, only the session cookie is sent back to the server
                // since the session on the server has gone, we expect HTTP_UNAUTHORIZED error
                try (HttpClient httpClient = httpClientSupplier.get()) {
                    HttpClient.ResponseHeaders responseHeaders = newHttpRequest(httpClient, HTTP_PORT, "SELECT x FROM long_sequence(2)", null, null, null, SESSION_COOKIE_NAME, sessionId);

                    // failed authentication with the session cookie
                    awaitStatusCode(responseHeaders, "401");

                    // assert that no new session cookie has been set in the response
                    assertNoSessionCookie(responseHeaders);

                    assertChunkedBody(responseHeaders, "Unauthorized\r\n");
                }

                // this time sending Authorization header too, no only the session cookie
                // server opens a new session after authenticating the user
                try (HttpClient httpClient = httpClientSupplier.get()) {
                    HttpClient.ResponseHeaders responseHeaders = newHttpRequest(httpClient, HTTP_PORT, "SELECT x FROM long_sequence(4)", USER, PASSWORD, null, SESSION_COOKIE_NAME, sessionId);

                    // successful authentication with username + pwd
                    awaitStatusCode(responseHeaders, "200");

                    // assert that new session cookie is set
                    final String newSessionId = assertSessionCookie(responseHeaders, isSecure());
                    assertNotEquals(sessionId, newSessionId);
                    sessionId = newSessionId;

                    assertChunkedBody(responseHeaders, "{" +
                            "\"query\":\"SELECT x FROM long_sequence(4)\"," +
                            "\"columns\":[{\"name\":\"x\",\"type\":\"LONG\"}]," +
                            "\"timestamp\":-1," +
                            "\"dataset\":[[1],[2],[3],[4]]," +
                            "\"count\":4" +
                            "}");
                }

                // closing session
                try (HttpClient httpClient = httpClientSupplier.get()) {
                    HttpClient.ResponseHeaders responseHeaders = newHttpRequest(httpClient, HTTP_PORT, "SELECT 1", null, null, "false", SESSION_COOKIE_NAME, sessionId);

                    // successful authentication with the session cookie
                    awaitStatusCode(responseHeaders, "200");

                    // assert that session cookie is set
                    assertNoSessionCookie(responseHeaders);

                    assertChunkedBody(responseHeaders, "{" +
                            "\"query\":\"SELECT 1\"," +
                            "\"columns\":[{\"name\":\"1\",\"type\":\"INT\"}]," +
                            "\"timestamp\":-1," +
                            "\"dataset\":[[1]]," +
                            "\"count\":1" +
                            "}");
                }
            }
        });
    }

    protected void testSessionLifetimeExtended(Supplier<ServerMain> serverMainSupplier, Supplier<HttpClient> httpClientSupplier, AtomicLong currentMicros) throws Exception {
        assertMemoryLeak(() -> {
            try (final ServerMain questdb = serverMainSupplier.get()) {
                questdb.start();

                String sessionId;
                final HttpSessionStore sessionStore = questdb.getConfiguration().getFactoryProvider().getHttpSessionStore();
                final long sessionTimeout = questdb.getConfiguration().getHttpServerConfiguration().getHttpContextConfiguration().getSessionTimeout();

                // authenticate with username + pwd, and pass 'session=true'
                try (HttpClient httpClient = httpClientSupplier.get()) {
                    HttpClient.ResponseHeaders responseHeaders = newHttpRequest(httpClient, HTTP_PORT, "SELECT x FROM long_sequence(3)", USER, PASSWORD, "true");

                    // successful authentication with username + pwd
                    awaitStatusCode(responseHeaders, "200");

                    // assert that session cookie is set
                    sessionId = assertSessionCookie(responseHeaders, isSecure());

                    assertChunkedBody(responseHeaders, "{" +
                            "\"query\":\"SELECT x FROM long_sequence(3)\"," +
                            "\"columns\":[{\"name\":\"x\",\"type\":\"LONG\"}]," +
                            "\"timestamp\":-1," +
                            "\"dataset\":[[1],[2],[3]]," +
                            "\"count\":3" +
                            "}");
                }

                final HttpSessionStore.SessionInfo session = assertSession(sessionStore, sessionId, currentMicros.get(), sessionTimeout);

                // move the clock
                currentMicros.addAndGet(5_000_000L);

                // no Authorization header, only the session cookie is sent back to the server
                try (HttpClient httpClient = httpClientSupplier.get()) {
                    HttpClient.ResponseHeaders responseHeaders = newHttpRequest(httpClient, HTTP_PORT, "SELECT x FROM long_sequence(2)", SESSION_COOKIE_NAME, sessionId);

                    // successful authentication with the session cookie
                    awaitStatusCode(responseHeaders, "200");

                    // assert that no new session cookie has been set in the response
                    assertNoSessionCookie(responseHeaders);

                    assertChunkedBody(responseHeaders, "{" +
                            "\"query\":\"SELECT x FROM long_sequence(2)\"," +
                            "\"columns\":[{\"name\":\"x\",\"type\":\"LONG\"}]," +
                            "\"timestamp\":-1," +
                            "\"dataset\":[[1],[2]]," +
                            "\"count\":2" +
                            "}");
                }

                // assert that expiry time has moved together with the clock
                assertEquals(currentMicros.get() + sessionTimeout, session.getExpiresAt());
            }
        });
    }

    protected void testSessionRotation(Supplier<ServerMain> serverMainSupplier, Supplier<HttpClient> httpClientSupplier, AtomicLong currentMicros) throws Exception {
        assertMemoryLeak(() -> {
            try (final ServerMain questdb = serverMainSupplier.get()) {
                questdb.start();

                final String sessionId1;
                final HttpSessionStore sessionStore = questdb.getConfiguration().getFactoryProvider().getHttpSessionStore();
                final long sessionTimeout = questdb.getConfiguration().getHttpServerConfiguration().getHttpContextConfiguration().getSessionTimeout();

                // authenticate with username + pwd, and pass 'session=true'
                try (HttpClient httpClient = httpClientSupplier.get()) {
                    HttpClient.ResponseHeaders responseHeaders = newHttpRequest(httpClient, HTTP_PORT, "SELECT x FROM long_sequence(3)", USER, PASSWORD, "true");

                    // successful authentication with username + pwd
                    awaitStatusCode(responseHeaders, "200");

                    // assert that session cookie is set
                    sessionId1 = assertSessionCookie(responseHeaders, isSecure());

                    assertChunkedBody(responseHeaders, "{" +
                            "\"query\":\"SELECT x FROM long_sequence(3)\"," +
                            "\"columns\":[{\"name\":\"x\",\"type\":\"LONG\"}]," +
                            "\"timestamp\":-1," +
                            "\"dataset\":[[1],[2],[3]]," +
                            "\"count\":3" +
                            "}");
                }

                final HttpSessionStore.SessionInfo session = assertSession(sessionStore, sessionId1, currentMicros.get(), sessionTimeout);

                // move the clock after rotation time but before session timeout
                currentMicros.addAndGet(sessionTimeout / 2 + 1_000_000L);

                final String sessionId2;

                // no Authorization header, only the session cookie is sent back to the server
                try (HttpClient httpClient = httpClientSupplier.get()) {
                    HttpClient.ResponseHeaders responseHeaders = newHttpRequest(httpClient, HTTP_PORT, "SELECT x FROM long_sequence(2)", null, null, null, SESSION_COOKIE_NAME, sessionId1);

                    // successful authentication with the session cookie
                    awaitStatusCode(responseHeaders, "200");

                    // assert that a new session has been created for the user
                    sessionId2 = assertSessionCookie(responseHeaders, isSecure());
                    assertNotEquals(sessionId1, sessionId2);

                    assertChunkedBody(responseHeaders, "{" +
                            "\"query\":\"SELECT x FROM long_sequence(2)\"," +
                            "\"columns\":[{\"name\":\"x\",\"type\":\"LONG\"}]," +
                            "\"timestamp\":-1," +
                            "\"dataset\":[[1],[2]]," +
                            "\"count\":2" +
                            "}");
                }

                // assert that the old entry is still present in the store
                assertNotNull(sessionStore.getSession(sessionId1));

                // assert that a new entry is present too, and they are the same session object
                final HttpSessionStore.SessionInfo session2 = sessionStore.getSession(sessionId2);
                assertNotNull(session2);
                assertEquals(session, session2);

                // move the clock after rotation tolerance but before session timeout
                currentMicros.addAndGet(sessionTimeout / 3);

                // no Authorization header, only the session cookie is sent back to the server
                try (HttpClient httpClient = httpClientSupplier.get()) {
                    HttpClient.ResponseHeaders responseHeaders = newHttpRequest(httpClient, HTTP_PORT, "SELECT x FROM long_sequence(2)", null, null, null, SESSION_COOKIE_NAME, sessionId2);

                    // successful authentication with the session cookie
                    awaitStatusCode(responseHeaders, "200");

                    // assert that no new session has been created for the user
                    assertNoSessionCookie(responseHeaders);

                    assertChunkedBody(responseHeaders, "{" +
                            "\"query\":\"SELECT x FROM long_sequence(2)\"," +
                            "\"columns\":[{\"name\":\"x\",\"type\":\"LONG\"}]," +
                            "\"timestamp\":-1," +
                            "\"dataset\":[[1],[2]]," +
                            "\"count\":2" +
                            "}");
                }

                // assert that the original entry has been evicted
                assertNull(sessionStore.getSession(sessionId1));
            }
        });
    }
}
