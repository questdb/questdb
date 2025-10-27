package io.questdb.test.cutlass.http;

import io.questdb.Bootstrap;
import io.questdb.PropBootstrapConfiguration;
import io.questdb.PropertyKey;
import io.questdb.ServerMain;
import io.questdb.cutlass.http.client.HttpClientFactory;
import io.questdb.std.datetime.MicrosecondClock;
import io.questdb.test.tools.TestUtils;
import org.jetbrains.annotations.NotNull;
import org.junit.Before;
import org.junit.Test;

import java.util.concurrent.atomic.AtomicLong;

public class HttpCookieTest extends BaseHttpCookieTest {

    @Before
    @Override
    public void setUp() {
        super.setUp();
        TestUtils.unchecked(() -> createDummyConfiguration(
                PropertyKey.HTTP_USER.getPropertyPath() + "=" + USER,
                PropertyKey.HTTP_PASSWORD.getPropertyPath() + "=" + PASSWORD)
        );
        dbPath.parent().$();
    }

    @Test
    public void testExpiredSessionEvicted() throws Exception {
        final AtomicLong currentMicros = new AtomicLong(1760743438000000L);
        final Bootstrap bootstrap = getBootstrapWithMockClock(currentMicros);
        testExpiredSessionEvicted(() -> new ServerMain(bootstrap), HttpClientFactory::newPlainTextInstance, currentMicros);
    }

    @Test
    public void testRotatedSessionDestroyedWithNewSessionId() throws Exception {
        testRotatedSessionDestroyed(true);
    }

    @Test
    public void testRotatedSessionDestroyedWithOldSessionId() throws Exception {
        testRotatedSessionDestroyed(false);
    }

    @Test
    public void testSessionCookieAuthentication() throws Exception {
        testSessionCookieAuthentication(() -> new ServerMain(getServerMainArgs()), HttpClientFactory::newPlainTextInstance);
    }

    @Test
    public void testSessionCookieParsingError() throws Exception {
        testSessionCookieParsingError(() -> new ServerMain(getServerMainArgs()), HttpClientFactory::newPlainTextInstance);
    }

    @Test
    public void testSessionCookieServerRestart() throws Exception {
        testSessionCookieServerRestart(() -> new ServerMain(getServerMainArgs()), HttpClientFactory::newPlainTextInstance);
    }

    @Test
    public void testSessionLifetimeExtended() throws Exception {
        final AtomicLong currentMicros = new AtomicLong(1760743438000000L);
        final Bootstrap bootstrap = getBootstrapWithMockClock(currentMicros);
        testSessionLifetimeExtended(() -> new ServerMain(bootstrap), HttpClientFactory::newPlainTextInstance, currentMicros);
    }

    @Test
    public void testSessionRotation() throws Exception {
        final AtomicLong currentMicros = new AtomicLong(1760743438000000L);
        final Bootstrap bootstrap = getBootstrapWithMockClock(currentMicros);
        testSessionRotation(() -> new ServerMain(bootstrap), HttpClientFactory::newPlainTextInstance, currentMicros);
    }

    private void testRotatedSessionDestroyed(boolean closeWithNewSessionId) throws Exception {
        final AtomicLong currentMicros = new AtomicLong(1760743438000000L);
        final Bootstrap bootstrap = getBootstrapWithMockClock(currentMicros);
        testRotatedSessionDestroyed(() -> new ServerMain(bootstrap), HttpClientFactory::newPlainTextInstance, currentMicros, closeWithNewSessionId);
    }

    static @NotNull Bootstrap getBootstrapWithMockClock(AtomicLong currentMicros) {
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
}
