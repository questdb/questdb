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

package io.questdb.test.client;

import io.questdb.client.Sender;
import io.questdb.cutlass.line.LineSenderException;
import io.questdb.network.NetworkFacadeImpl;
import io.questdb.std.Files;
import io.questdb.test.cutlass.line.tcp.AbstractLineTcpReceiverTest;
import io.questdb.test.tools.TestUtils;
import io.questdb.test.tools.TlsProxyRule;
import org.junit.Assert;
import org.junit.ClassRule;
import org.junit.Test;

import static org.junit.Assert.fail;

public class LineSenderBuilderTest extends AbstractLineTcpReceiverTest {
    @ClassRule
    public static final TlsProxyRule TLS_PROXY = TlsProxyRule.toHostAndPort("localhost", 9002);

    private static final String LOCALHOST = "localhost";

    @Test
    public void testAddressDoubleSet_firstAddressThenAddress() throws Exception {
        assertMemoryLeak(() -> {
            Sender.LineSenderBuilder builder = Sender.builder().address(LOCALHOST);
            try {
                builder.address("127.0.0.1");
                fail("should not allow double host set");
            } catch (LineSenderException e) {
                TestUtils.assertContains(e.getMessage(), "already configured");
            }
        });
    }

    @Test
    public void testAddressEmpty() throws Exception {
        assertMemoryLeak(() -> {
            Sender.LineSenderBuilder builder = Sender.builder();
            try {
                builder.address("");
                fail("empty address should fail");
            } catch (LineSenderException e) {
                TestUtils.assertContains(e.getMessage(), "address cannot be empty");
            }
        });
    }

    @Test
    public void testAddressEndsWithColon() throws Exception {
        assertMemoryLeak(() -> {
            Sender.LineSenderBuilder builder = Sender.builder();
            try {
                builder.address("foo:");
                fail("should fail when address ends with colon");
            } catch (LineSenderException e) {
                TestUtils.assertContains(e.getMessage(), "invalid address");
            }
        });
    }

    @Test
    public void testAddressNull() throws Exception {
        assertMemoryLeak(() -> {
            Sender.LineSenderBuilder builder = Sender.builder();
            try {
                builder.address(null);
                fail("null address should fail");
            } catch (LineSenderException e) {
                TestUtils.assertContains(e.getMessage(), "null");
            }
        });
    }

    @Test
    public void testAuthDoubleSet() throws Exception {
        assertMemoryLeak(() -> {
            Sender.LineSenderBuilder builder = Sender.builder().enableAuth("foo").authToken(AUTH_TOKEN_KEY1);
            try {
                builder.enableAuth("bar");
                fail("should not allow double auth set");
            } catch (LineSenderException e) {
                TestUtils.assertContains(e.getMessage(), "already configured");
            }
        });
    }

    @Test
    public void testAuthTooSmallBuffer() throws Exception {
        assertMemoryLeak(() -> {
            try {
                Sender.LineSenderBuilder builder = Sender.builder()
                        .enableAuth("foo").authToken(AUTH_TOKEN_KEY1).address(LOCALHOST + ":9001")
                        .bufferCapacity(1);
                builder.build();
                fail("tiny buffer should NOT be allowed as it wont fit auth challenge");
            } catch (LineSenderException e) {
                TestUtils.assertContains(e.getMessage(), "minimalCapacity");
                TestUtils.assertContains(e.getMessage(), "requestedCapacity");
            }
        });
    }

    @Test
    public void testAuthWithBadToken() throws Exception {
        assertMemoryLeak(() -> {
            Sender.LineSenderBuilder.AuthBuilder builder = Sender.builder().enableAuth("foo");
            try {
                builder.authToken("bar token");
                fail("bad token should not be imported");
            } catch (LineSenderException e) {
                TestUtils.assertContains(e.getMessage(), "could not import token");
            }
        });
    }

    @Test
    public void testAutoFlushMustBePositive() {
        try (Sender s = Sender.builder().autoFlushRows(0).build()) {
            fail("auto-flush must be positive");
        } catch (LineSenderException e) {
            TestUtils.assertContains(e.getMessage(), "auto flush rows has to be positive [autoFlushRows=0]");
        }

        try (Sender s = Sender.builder().autoFlushRows(-1).build()) {
            fail("auto-flush must be positive");
        } catch (LineSenderException e) {
            TestUtils.assertContains(e.getMessage(), "auto flush rows has to be positive [autoFlushRows=-1]");
        }
    }

    @Test
    public void testAutoFlushRowsNotSupportedForTcp() throws Exception {
        assertMemoryLeak(() -> {
            try {
                Sender.builder().address(LOCALHOST).autoFlushRows(1).build();
                fail("auto flush rows should not be supported for TCP");
            } catch (LineSenderException e) {
                TestUtils.assertContains(e.getMessage(), "auto flush rows is not supported for TCP protocol");
            }
        });
    }

    @Test
    public void testAutoFlushRows_doubleConfiguration() throws Exception {
        assertMemoryLeak(() -> {
            try {
                Sender.builder().autoFlushRows(1).autoFlushRows(1);
            } catch (LineSenderException e) {
                TestUtils.assertContains(e.getMessage(), "auto flush rows was already configured [autoFlushRows=1]");
            }
        });
    }

    @Test
    public void testBufferSizeDoubleSet() throws Exception {
        assertMemoryLeak(() -> {
            Sender.LineSenderBuilder builder = Sender.builder().bufferCapacity(1024);
            try {
                builder.bufferCapacity(1024);
                fail("should not allow double buffer capacity set");
            } catch (LineSenderException e) {
                TestUtils.assertContains(e.getMessage(), "already configured");
            }
        });
    }

    @Test
    public void testConfString() throws Exception {
        assertMemoryLeak(() -> {
            assertConfStrError("foo", "invalid schema [schema=foo, supported-schemas=[http, https, tcp, tcps]]");
            assertConfStrError("http::addr=bar", "invalid address [error=missing trailing semicolon at position 14]");
            assertConfStrError("badschema::addr=bar;", "invalid schema [schema=badschema, supported-schemas=[http, https, tcp, tcps]]");
            assertConfStrError("http::addr=localhost:-1;", "invalid port [port=-1]");
            assertConfStrError("http::auto_flush=on;", "addr is missing");
            assertConfStrError("http::addr=localhost;tls_roots=/some/path;", "tls_roots was configured, but tls_roots_password is missing");
            assertConfStrError("http::addr=localhost;tls_roots_password=hunter123;", "tls_roots_password was configured, but tls_roots is missing");
            assertConfStrError("tcp::addr=localhost;user=foo;", "token cannot be empty nor null");
            assertConfStrError("tcp::addr=localhost;token=foo;", "TCP token is configured, but user is missing");
            assertConfStrError("http::addr=localhost;user=foo;", "password cannot be empty nor null");
            assertConfStrError("http::addr=localhost;pass=foo;", "HTTP password is configured, but username is missing");
            assertConfStrError("tcp::addr=localhost;pass=foo;", "password is not supported for TCP protocol");
            assertConfStrError("tcp::addr=localhost;retry_timeout=;", "retry_timeout cannot be empty");
            assertConfStrError("tcp::addr=localhost;max_buf_size=;", "max_buf_size cannot be empty");
            assertConfStrError("tcp::addr=localhost;init_buf_size=;", "init_buf_size cannot be empty");
            assertConfStrError("tcp::addr=localhost;invali=", "invalid parameter [error=missing trailing semicolon at position 27]");
            assertConfStrError("tcp::Řaddr=localhost;", "invalid configuration string [error=key must be consist of alpha-numerical ascii characters and underscore, not 'Ř' at position 5]");
            assertConfStrError("http::addr=localhost:8080;tls_verify=unsafe_off;", "TSL validation disabled, but TLS was not enabled");
            assertConfStrError("http::addr=localhost:8080;tls_verify=bad;", "invalid tls_verify [value=bad, allowed-values=[on, unsafe_off]]");
            assertConfStrError("tcps::addr=localhost;pass=unsafe_off;", "password is not supported for TCP protocol");
            assertConfStrError("http::addr=localhost:8080;max_buf_size=-32;", "maximum buffer capacity cannot be less than initial buffer capacity [maximumBufferCapacity=-32, initialBufferCapacity=65536]");
            assertConfStrError("http::addr=localhost:8080;max_buf_size=notanumber;", "invalid max_buf_size [value=notanumber]");
            assertConfStrError("http::addr=localhost:8080;init_buf_size=notanumber;", "invalid init_buf_size [value=notanumber]");
            assertConfStrError("http::addr=localhost:8080;init_buf_size=-42;", "buffer capacity cannot be negative [capacity=-42]");
            assertConfStrError("http::addr=localhost:8080;auto_flush_rows=0;", "invalid auto_flush_rows [value=0]");
            assertConfStrError("http::addr=localhost:8080;auto_flush_rows=notanumber;", "invalid auto_flush_rows [value=notanumber]");
            assertConfStrError("http::addr=localhost:8080;auto_flush=invalid;", "invalid auto_flush [value=invalid, allowed-values=[on, off]]");
            assertConfStrError("http::addr=localhost:8080;auto_flush=off;auto_flush_rows=100;", "cannot set auto flush rows when auto-flush is disabled");
            assertConfStrError("http::addr=localhost:8080;auto_flush_rows=100;auto_flush=off;", "auto flush rows was already configured [autoFlushRows=100]");
            assertConfStrError(Sender.builder().http(), "http::addr=localhost:8080;auto_flush_rows=100;auto_flush=off;", "protocol was already configured [protocol=http]");
            assertConfStrError(Sender.builder().tcp(), "http::addr=localhost:8080;auto_flush_rows=100;auto_flush=off;", "protocol was already configured [protocol=tcp]");
            assertConfStrError(Sender.builder().address("remote"), "http::addr=localhost:8080;auto_flush_rows=100;auto_flush=off;", "server address was already configured [address=remote]");
            assertConfStrError(Sender.builder().address("remote:1234"), "http::addr=localhost:8080;auto_flush_rows=100;auto_flush=off;", "server address was already configured [address=remote]");
            assertConfStrError(Sender.builder().port(1234), "http::addr=localhost:8080;auto_flush_rows=100;auto_flush=off;", "server port was already configured [port=1234]");
            assertConfStrError(Sender.builder().advancedTls().disableCertificateValidation(), "http::addr=localhost:8080;tls_verify=on;", "tls_verify was already configured");
            assertConfStrError(Sender.builder().enableTls(), "http::addr=localhost:8080;", "cannot use http protocol when TLS is enabled. use https instead");
            assertConfStrError(Sender.builder().enableTls(), "tcp::addr=localhost:8080;", "cannot use tcp protocol when TLS is enabled. use tcps instead");

            assertConfStrOk(Sender.builder().enableTls(), "https::addr=localhost:8080;");
            assertConfStrOk("http", "addr=localhost:8080", "auto_flush_rows=100");
            assertConfStrOk("http", "addr=localhost:8080", "auto_flush=on", "auto_flush_rows=100");
            assertConfStrOk("http", "addr=localhost:8080", "auto_flush_rows=100", "auto_flush=on");
            assertConfStrOk("http", "addr=localhost", "auto_flush=on");
            assertConfStrOk("http::addr=localhost;auto_flush=off;");
            assertConfStrOk("http::addr=localhost;");
            assertConfStrOk("http::addr=localhost:8080;");
            assertConfStrOk("http::addr=localhost:8080;token=foo;");
            assertConfStrOk("http::addr=localhost:8080;token=foo=bar;");
            assertConfStrOk("http", "addr=localhost:8080", "token=foo", "retry_timeout=1000", "max_buf_size=1000000");
            assertConfStrOk("http", "addr=localhost:8080", "token=foo", "retry_timeout=1000", "max_buf_size=1000000");
            assertConfStrOk("http::addr=localhost:8080;token=foo;max_buf_size=1000000;retry_timeout=1000;");
            assertConfStrOk("https::addr=localhost:8080;tls_verify=unsafe_off;auto_flush_rows=100;");
            assertConfStrOk("https::addr=localhost:8080;tls_verify=on;");
            assertConfStrError("https::addr=2001:0db8:85a3:0000:0000:8a2e:0370:7334;tls_verify=on;", "cannot parse a port from the address, use IPv4 address or a domain name [address=2001:0db8:85a3:0000:0000:8a2e:0370:7334]");
            assertConfStrError("https::addr=[2001:0db8:85a3:0000:0000:8a2e:0370:7334]:9000;tls_verify=on;", "cannot parse a port from the address, use IPv4 address or a domain name [address=[2001:0db8:85a3:0000:0000:8a2e:0370:7334]:9000]");
        });
    }

    @Test
    public void testConnectPlain() throws Exception {
        runInContext(r -> {
            try (Sender sender = Sender.builder().address(LOCALHOST).port(bindPort).build()) {
                sender.table("mytable").symbol("symbol", "symbol").atNow();
                sender.flush();
                assertTableExistsEventually(engine, "mytable");
            }
        });
    }

    @Test
    public void testConnectPlainAuthWithPrivKeySuccess() throws Exception {
        authKeyId = AUTH_KEY_ID1;
        runInContext(r -> {
            try (Sender sender = Sender.builder()
                    .address(LOCALHOST)
                    .port(bindPort)
                    .enableAuth(AUTH_KEY_ID1)
                    .privateKey(AUTH_PRIVATE_KEY1)
                    .build()) {
                sender.table("mytable").symbol("symbol", "symbol").atNow();
                sender.flush();
                assertTableExistsEventually(engine, "mytable");
            }
        });
    }

    @Test
    public void testConnectPlainAuthWithTokenFailure() throws Exception {
        authKeyId = AUTH_KEY_ID1;
        nf = new NetworkFacadeImpl() {
            @Override
            public int recvRaw(int fd, long buffer, int bufferLen) {
                // force server to fail to receive userId and this disconnect
                // mid-authentication
                return -1;
            }
        };
        runInContext(r -> {
            try {
                Sender.builder()
                        .address(LOCALHOST)
                        .port(bindPort)
                        .enableAuth(AUTH_KEY_ID1)
                        .authToken(AUTH_TOKEN_KEY1) // it does not really matter as server will never receive the challange response due to the custom NetworkFacade
                        .build();
                fail("should have failed");
            } catch (LineSenderException e) {
                // ignored
            }
        });
    }

    @Test
    public void testConnectPlainAuthWithTokenSuccess() throws Exception {
        authKeyId = AUTH_KEY_ID1;
        runInContext(r -> {
            try (Sender sender = Sender.builder()
                    .address(LOCALHOST)
                    .port(bindPort)
                    .enableAuth(AUTH_KEY_ID1)
                    .authToken(AUTH_TOKEN_KEY1)
                    .build()) {
                sender.table("mytable").symbol("symbol", "symbol").atNow();
                sender.flush();
                assertTableExistsEventually(engine, "mytable");
            }
        });
    }

    @Test
    public void testConnectTlsAuthWithPrivKeySuccess() throws Exception {
        authKeyId = AUTH_KEY_ID1;
        String truststore = Files.getResourcePath(getClass().getResource(TRUSTSTORE_PATH));
        runInContext(r -> {
            Sender.LineSenderBuilder builder = Sender.builder()
                    .address(LOCALHOST)
                    .port(TLS_PROXY.getListeningPort())
                    .enableAuth(AUTH_KEY_ID1).privateKey(AUTH_PRIVATE_KEY1)
                    .enableTls().advancedTls().customTrustStore(truststore, TRUSTSTORE_PASSWORD);
            try (Sender sender = builder.build()) {
                sender.table("mytable").symbol("symbol", "symbol").atNow();
                sender.flush();
                assertTableExistsEventually(engine, "mytable");
            }
        });
    }

    @Test
    public void testConnectTlsAuthWithTokenSuccess() throws Exception {
        authKeyId = AUTH_KEY_ID1;
        String truststore = Files.getResourcePath(getClass().getResource(TRUSTSTORE_PATH));
        runInContext(r -> {
            Sender.LineSenderBuilder builder = Sender.builder()
                    .address(LOCALHOST)
                    .port(TLS_PROXY.getListeningPort())
                    .enableAuth(AUTH_KEY_ID1).authToken(AUTH_TOKEN_KEY1)
                    .enableTls().advancedTls().customTrustStore(truststore, TRUSTSTORE_PASSWORD);
            try (Sender sender = builder.build()) {
                sender.table("mytable").symbol("symbol", "symbol").atNow();
                sender.flush();
                assertTableExistsEventually(engine, "mytable");
            }
        });
    }

    @Test
    public void testConnectTls_NonExistingTrustoreClaspath() throws Exception {
        String truststore = "classpath:/foo/whatever/non-existing";
        runInContext(r -> {
            Sender.LineSenderBuilder builder = Sender.builder()
                    .address(LOCALHOST)
                    .port(TLS_PROXY.getListeningPort())
                    .enableTls().advancedTls().customTrustStore(truststore, TRUSTSTORE_PASSWORD);
            try {
                builder.build();
                fail("nonexistent trust store should throw an exception");
            } catch (LineSenderException e) {
                TestUtils.assertContains(e.getMessage(), "configured trust store is unavailable [path=classpath:/foo/whatever/non-existing]");
            }
        });
    }

    @Test
    public void testConnectTls_NonExistingTrustoreFile() throws Exception {
        runInContext(r -> {
            String truststore = "/foo/whatever/non-existing";
            Sender.LineSenderBuilder builder = Sender.builder()
                    .address(LOCALHOST)
                    .port(TLS_PROXY.getListeningPort())
                    .enableTls().advancedTls().customTrustStore(truststore, TRUSTSTORE_PASSWORD);
            try {
                builder.build();
                fail("nonexistent trustore should throw an exception");
            } catch (LineSenderException e) {
                TestUtils.assertContains(e.getMessage(), "could not create SSL engine");
            }
        });
    }

    @Test
    public void testConnectTls_TruststoreClasspath() throws Exception {
        String truststore = Files.getResourcePath(getClass().getResource(TRUSTSTORE_PATH));
        runInContext(r -> {
            try (Sender sender = Sender.builder()
                    .address(LOCALHOST)
                    .port(TLS_PROXY.getListeningPort())
                    .enableTls().advancedTls().customTrustStore(truststore, TRUSTSTORE_PASSWORD)
                    .build()) {
                sender.table("mytable").symbol("symbol", "symbol").atNow();
                sender.flush();
                assertTableExistsEventually(engine, "mytable");
            }
        });
    }

    @Test
    public void testConnectTls_TruststoreFile() throws Exception {
        String truststore = TestUtils.getTestResourcePath(TRUSTSTORE_PATH);
        runInContext(r -> {
            try (Sender sender = Sender.builder()
                    .address(LOCALHOST)
                    .port(TLS_PROXY.getListeningPort())
                    .enableTls().advancedTls().customTrustStore(truststore, TRUSTSTORE_PASSWORD)
                    .build()) {
                sender.table("mytable").symbol("symbol", "symbol").atNow();
                sender.flush();
                assertTableExistsEventually(engine, "mytable");
            }
        });
    }

    @Test
    public void testConnectTls_WrongTruststorePassword() throws Exception {
        String truststore = Files.getResourcePath(getClass().getResource(TRUSTSTORE_PATH));
        runInContext(r -> {
            Sender.LineSenderBuilder builder = Sender.builder()
                    .address(LOCALHOST)
                    .port(TLS_PROXY.getListeningPort())
                    .enableTls().advancedTls().customTrustStore(truststore, "wrong password".toCharArray());
            try {
                builder.build();
                fail("nonexistent trust store should throw an exception");
            } catch (LineSenderException e) {
                TestUtils.assertContains(e.getMessage(), "could not create SSL engine");
                TestUtils.assertContains(e.getCause().getMessage(), "password");
            }
        });
    }

    @Test
    public void testCustomTrustorePasswordCannotBeNull() {
        try {
            Sender.builder().advancedTls().customTrustStore(TRUSTSTORE_PATH, null);
            fail("should not allow null trust store password");
        } catch (LineSenderException e) {
            TestUtils.assertContains(e.getMessage(), "trust store password cannot be null");
        }
    }

    @Test
    public void testCustomTrustorePathCannotBeBlank() {
        try {
            Sender.builder().advancedTls().customTrustStore("", TRUSTSTORE_PASSWORD);
            fail("should not allow blank trust store path");
        } catch (LineSenderException e) {
            TestUtils.assertContains(e.getMessage(), "trust store path cannot be empty nor null");
        }

        try {
            Sender.builder().advancedTls().customTrustStore(null, TRUSTSTORE_PASSWORD);
            fail("should not allow null trust store path");
        } catch (LineSenderException e) {
            TestUtils.assertContains(e.getMessage(), "trust store path cannot be empty nor null");
        }
    }

    @Test
    public void testCustomTruststoreButTlsNotEnabled() throws Exception {
        assertMemoryLeak(() -> {
            Sender.LineSenderBuilder builder = Sender.builder()
                    .advancedTls().customTrustStore(TRUSTSTORE_PATH, TRUSTSTORE_PASSWORD)
                    .address(LOCALHOST);
            try {
                builder.build();
                fail("should fail when custom trust store configured, but TLS not enabled");
            } catch (LineSenderException e) {
                TestUtils.assertContains(e.getMessage(), "TLS was not enabled");
            }
        });
    }

    @Test
    public void testCustomTruststoreDoubleSet() throws Exception {
        assertMemoryLeak(() -> {
            Sender.LineSenderBuilder builder = Sender.builder().advancedTls().customTrustStore(TRUSTSTORE_PATH, TRUSTSTORE_PASSWORD);
            try {
                builder.advancedTls().customTrustStore(TRUSTSTORE_PATH, TRUSTSTORE_PASSWORD);
                fail("should not allow double custom trust store set");
            } catch (LineSenderException e) {
                TestUtils.assertContains(e.getMessage(), "already configured");
            }
        });
    }

    @Test
    public void testDisableAutoFlushNotSupportedForTcp() throws Exception {
        assertMemoryLeak(() -> {
            try (Sender s = Sender.builder().address(LOCALHOST).tcp().disableAutoFlush().build();) {
                fail("TCP does not support disabling auto-flush");
            } catch (LineSenderException e) {
                TestUtils.assertContains(e.getMessage(), "auto-flush is not supported for TCP protocol");
            }
        });
    }

    @Test
    public void testDnsResolutionFail() throws Exception {
        assertMemoryLeak(() -> {
            try (Sender s = Sender.builder().address("this-domain-does-not-exist-i-hope-better-to-use-a-silly-tld.silly-tld").build()) {
                fail("dns resolution errors should fail fast");
            } catch (LineSenderException e) {
                TestUtils.assertContains(e.getMessage(), "could not resolve");
            }
        });
    }

    @Test
    public void testFailFastWhenSetCustomTrustoreTwice() {
        Sender.LineSenderBuilder builder = Sender.builder().advancedTls().customTrustStore(TRUSTSTORE_PATH, TRUSTSTORE_PASSWORD);
        try {
            builder.advancedTls().customTrustStore(TRUSTSTORE_PATH, TRUSTSTORE_PASSWORD);
            fail("should not allow double custom trust store set");
        } catch (LineSenderException e) {
            TestUtils.assertContains(e.getMessage(), "already configured");
        }
    }

    @Test
    public void testFirstTlsValidationDisabledThenCustomTruststore() throws Exception {
        assertMemoryLeak(() -> {
            Sender.LineSenderBuilder builder = Sender.builder()
                    .advancedTls().disableCertificateValidation();
            try {
                builder.advancedTls().customTrustStore(TRUSTSTORE_PATH, TRUSTSTORE_PASSWORD);
                fail("should not allow custom truststore when TLS validation was disabled disabled");
            } catch (LineSenderException e) {
                TestUtils.assertContains(e.getMessage(), "TLS validation was already disabled");
            }
        });
    }

    @Test
    public void testFromEnv() throws Exception {
        authKeyId = AUTH_KEY_ID1;
        runInContext(r -> {
            try (Sender sender = Sender.fromEnv()) {
                sender.table("mytable").symbol("symbol", "symbol").atNow();
                sender.flush();
            }
            assertTableExistsEventually(engine, "mytable");
        });
    }

    @Test
    public void testHostNorAddressSet() throws Exception {
        assertMemoryLeak(() -> {
            Sender.LineSenderBuilder builder = Sender.builder();
            try {
                builder.build();
                fail("not host should fail");
            } catch (LineSenderException e) {
                TestUtils.assertContains(e.getMessage(), "server address not set");
            }
        });
    }

    @Test
    public void testHttpTokenNotSupportedForTcp() throws Exception {
        assertMemoryLeak(() -> {
            try {
                Sender.builder().address(LOCALHOST).httpToken("foo").build();
                fail("HTTP token should not be supported for TCP");
            } catch (LineSenderException e) {
                TestUtils.assertContains(e.getMessage(), "HTTP token authentication is not supported for TCP protocol");
            }
        });
    }

    @Test
    public void testInvalidHttpTimeout() throws Exception {
        assertMemoryLeak(() -> {
            try {
                Sender.builder().address("someurl").http().httpTimeoutMillis(0);
                fail("should fail with bad http time");
            } catch (LineSenderException e) {
                TestUtils.assertContains(e.getMessage(), "HTTP timeout must be positive [timeout=0]");
            }

            try {
                Sender.builder().address("someurl").http().httpTimeoutMillis(-1);
                fail("should fail with bad http time");
            } catch (LineSenderException e) {
                TestUtils.assertContains(e.getMessage(), "HTTP timeout must be positive [timeout=-1]");
            }

            try {
                Sender.builder().address("someurl").http().httpTimeoutMillis(100).httpTimeoutMillis(200);
                fail("should fail with bad http time");
            } catch (LineSenderException e) {
                TestUtils.assertContains(e.getMessage(), "HTTP timeout was already configured [timeout=100]");
            }

            try {
                Sender.builder().address("localhost").httpTimeoutMillis(5000).build();
                fail("should fail with bad http time");
            } catch (LineSenderException e) {
                TestUtils.assertContains(e.getMessage(), "HTTP timeout is not supported for TCP protocol");
            }
        });
    }

    @Test
    public void testInvalidRetryTimeout() {
        try {
            Sender.builder().retryTimeoutMillis(-1);
            Assert.fail();
        } catch (LineSenderException e) {
            TestUtils.assertContains(e.getMessage(), "retry timeout cannot be negative [retryTimeoutMillis=-1]");
        }

        Sender.LineSenderBuilder builder = Sender.builder().retryTimeoutMillis(100);
        try {
            builder.retryTimeoutMillis(200);
            Assert.fail();
        } catch (LineSenderException e) {
            TestUtils.assertContains(e.getMessage(), "retry timeout was already configured [retryTimeoutMillis=100]");
        }
    }

    @Test
    public void testMalformedPortInAddress() throws Exception {
        assertMemoryLeak(() -> {
            Sender.LineSenderBuilder builder = Sender.builder();
            try {
                builder.address("foo:nonsense12334");
                fail("should fail with malformated port");
            } catch (LineSenderException e) {
                TestUtils.assertContains(e.getMessage(), "cannot parse a port from the address");
            }
        });
    }

    @Test
    public void testMaxRequestBufferSizeCannotBeLessThanDefault() throws Exception {
        assertMemoryLeak(() -> {
            try (Sender sender = Sender.builder()
                    .address("localhost:1")
                    .http()
                    .maxBufferCapacity(65535)
                    .build()
            ) {
                Assert.fail();
            } catch (LineSenderException e) {
                TestUtils.assertContains(e.getMessage(), "maximum buffer capacity cannot be less than initial buffer capacity [maximumBufferCapacity=65535, initialBufferCapacity=65536]");
            }
        });
    }

    @Test
    public void testMaxRequestBufferSizeCannotBeLessThanInitialBufferSize() throws Exception {
        assertMemoryLeak(() -> {
            try (Sender sender = Sender.builder()
                    .address("localhost:1")
                    .http()
                    .maxBufferCapacity(100_000)
                    .bufferCapacity(200_000)
                    .build()
            ) {
                Assert.fail();
            } catch (LineSenderException e) {
                TestUtils.assertContains(e.getMessage(), "maximum buffer capacity cannot be less than initial buffer capacity [maximumBufferCapacity=100000, initialBufferCapacity=200000]");
            }
        });
    }

    @Test
    public void testMaxRetriesNotSupportedForTcp() throws Exception {
        assertMemoryLeak(() -> {
            try {
                Sender.builder().address(LOCALHOST).retryTimeoutMillis(100).build();
                fail("max retries should not be supported for TCP");
            } catch (LineSenderException e) {
                TestUtils.assertContains(e.getMessage(), "retrying is not supported for TCP protocol");
            }
        });
    }

    @Test
    public void testPlainAuth_connectionRefused() throws Exception {
        assertMemoryLeak(() -> {
            Sender.LineSenderBuilder builder = Sender.builder()
                    .enableAuth("foo").authToken(AUTH_TOKEN_KEY1).address(LOCALHOST + ":19003");
            try {
                builder.build();
                fail("connection refused should fail fast");
            } catch (LineSenderException e) {
                TestUtils.assertContains(e.getMessage(), "could not connect");
            }
        });
    }

    @Test
    public void testPlainOldTokenNotSupportedForHttpProtocol() throws Exception {
        assertMemoryLeak(() -> {
            try {
                Sender.builder().address("localhost:9000").http().enableAuth("key").authToken(AUTH_TOKEN_KEY1).build();
                fail("HTTP token should not be supported for TCP");
            } catch (LineSenderException e) {
                TestUtils.assertContains(e.getMessage(), "old token authentication is not supported for HTTP protocol");
            }
        });
    }

    @Test
    public void testPlain_connectionRefused() throws Exception {
        assertMemoryLeak(() -> {
            Sender.LineSenderBuilder builder = Sender.builder().address(LOCALHOST + ":19003");
            try {
                builder.build();
                fail("connection refused should fail fast");
            } catch (LineSenderException e) {
                TestUtils.assertContains(e.getMessage(), "could not connect");
            }
        });
    }

    @Test
    public void testPortDoubleSet_firstAddressThenPort() throws Exception {
        assertMemoryLeak(() -> {
            Sender.LineSenderBuilder builder = Sender.builder().address(LOCALHOST + ":9000");
            try {
                builder.port(9000);
                fail("should not allow double port set");
            } catch (LineSenderException e) {
                TestUtils.assertContains(e.getMessage(), "already configured");
            }
        });
    }

    @Test
    public void testPortDoubleSet_firstPortThenAddress() throws Exception {
        assertMemoryLeak(() -> {
            Sender.LineSenderBuilder builder = Sender.builder().port(9000);
            try {
                builder.address(LOCALHOST + ":9000");
                fail("should not allow double port set");
            } catch (LineSenderException e) {
                TestUtils.assertContains(e.getMessage(), "already configured");
            }
        });
    }

    @Test
    public void testPortDoubleSet_firstPortThenPort() throws Exception {
        assertMemoryLeak(() -> {
            Sender.LineSenderBuilder builder = Sender.builder().port(9000);
            try {
                builder.port(9000);
                fail("should not allow double port set");
            } catch (LineSenderException e) {
                TestUtils.assertContains(e.getMessage(), "already configured");
            }
        });
    }

    @Test
    public void testTlsDoubleSet() throws Exception {
        assertMemoryLeak(() -> {
            Sender.LineSenderBuilder builder = Sender.builder().enableTls();
            try {
                builder.enableTls();
                fail("should not allow double tls set");
            } catch (LineSenderException e) {
                TestUtils.assertContains(e.getMessage(), "already enabled");
            }
        });
    }

    @Test
    public void testTlsValidationDisabledButTlsNotEnabled() throws Exception {
        assertMemoryLeak(() -> {
            Sender.LineSenderBuilder builder = Sender.builder()
                    .advancedTls().disableCertificateValidation()
                    .address(LOCALHOST);
            try {
                builder.build();
                fail("should fail when TLS validation is disabled, but TLS not enabled");
            } catch (LineSenderException e) {
                TestUtils.assertContains(e.getMessage(), "TLS was not enabled");
            }
        });
    }

    @Test
    public void testTlsValidationDisabledDoubleSet() throws Exception {
        assertMemoryLeak(() -> {
            Sender.LineSenderBuilder builder = Sender.builder()
                    .advancedTls().disableCertificateValidation();
            try {
                builder.advancedTls().disableCertificateValidation();
                fail("should not allow double TLS validation disabled");
            } catch (LineSenderException e) {
                TestUtils.assertContains(e.getMessage(), "TLS validation was already disabled");
            }
        });
    }

    @Test
    public void testTls_connectionRefused() throws Exception {
        assertMemoryLeak(() -> {
            Sender.LineSenderBuilder builder = Sender.builder().enableTls().address(LOCALHOST + ":19003");
            try {
                builder.build();
                fail("connection refused should fail fast");
            } catch (LineSenderException e) {
                TestUtils.assertContains(e.getMessage(), "could not connect");
            }
        });
    }

    @Test
    public void testMinRequestThroughputCannotBeNegative() throws Exception {
        assertMemoryLeak(() -> {
            try {
                Sender.builder().address(LOCALHOST).http().minRequestThroughput(-100).build();
                fail("minimum request throughput must not be negative");
            } catch (LineSenderException e) {
                TestUtils.assertContains(e.getMessage(), "minimum request throughput must not be negative [minRequestThroughput=-100]");
            }
        });
    }

    @Test
    public void testMinRequestThroughputNotSupportedForTcp() throws Exception {
        assertMemoryLeak(() -> {
            try {
                Sender.builder().address(LOCALHOST).minRequestThroughput(1).build();
                fail("min request throughput is not be supported for TCP and the builder should fail-fast");
            } catch (LineSenderException e) {
                TestUtils.assertContains(e.getMessage(), "minimum request throughput is not supported for TCP protocol");
            }
        });
    }

    @Test
    public void testUsernamePasswordAuthNotSupportedForTcp() throws Exception {
        assertMemoryLeak(() -> {
            try {
                Sender.builder().address(LOCALHOST).httpUsernamePassword("foo", "bar").build();
                fail("HTTP token should not be supported for TCP");
            } catch (LineSenderException e) {
                TestUtils.assertContains(e.getMessage(), "username/password authentication is not supported for TCP protocol");
            }
        });
    }

    private static void assertConfStrError(String conf, String expectedError) {
        assertConfStrError(Sender.builder(), conf, expectedError);
    }

    private static void assertConfStrError(Sender.LineSenderBuilder sb, String conf, String expectedError) {
        try {
            try (Sender s = sb.fromConfig(conf).build()) {
                fail("should fail with bad conf string");
            }
        } catch (LineSenderException e) {
            TestUtils.assertContains(e.getMessage(), expectedError);
        }
    }

    private static void assertConfStrOk(String schema, String... params) {
        StringBuilder sb = new StringBuilder();
        sb.append(schema).append("::");
        shuffle(params);
        for (int i = 0; i < params.length; i++) {
            sb.append(params[i]).append(";");
        }
        assertConfStrOk(Sender.builder(), sb.toString());
    }

    private static void assertConfStrOk(String conf) {
        assertConfStrOk(Sender.builder(), conf);
    }

    private static void assertConfStrOk(Sender.LineSenderBuilder sb, String conf) {
        try (Sender s = sb.fromConfig(conf).build()) {

        }
    }

    private static void shuffle(String[] input) {
        for (int i = 0; i < input.length; i++) {
            int j = (int) (Math.random() * input.length);
            String tmp = input[i];
            input[i] = input[j];
            input[j] = tmp;
        }
    }
}
