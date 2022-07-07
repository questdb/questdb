/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2022 QuestDB
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

package io.questdb.cutlass.line;

import io.questdb.cutlass.line.tcp.AbstractLineTcpReceiverTest;
import io.questdb.test.tools.TestUtils;
import io.questdb.test.tools.TlsProxyRule;
import org.junit.ClassRule;
import org.junit.Test;

import java.net.URL;

import static org.junit.Assert.assertNotNull;
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
                TestUtils.assertContains(e.getMessage(), "address cannot ends");
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
            Sender.LineSenderBuilder builder = Sender.builder()
                    .enableAuth("foo").authToken(AUTH_TOKEN_KEY1).address(LOCALHOST + ":9001")
                    .bufferCapacity(1);
            try {
                builder.build();
                fail("tiny buffer should be be allowed as it wont fit auth challenge");
            } catch (LineSenderException e) {
                TestUtils.assertContains(e.getMessage(), "capacity");
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
        String truststore = "classpath:" + TRUSTSTORE_PATH;
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
        String truststore = "classpath:" + TRUSTSTORE_PATH;
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
                fail("non existing trustore should throw an exception");
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
                fail("non existing trustore should throw an exception");
            } catch (LineSenderException e) {
                TestUtils.assertContains(e.getMessage(), "could not create SSL engine");
            }
        });
    }

    @Test
    public void testConnectTls_TruststoreClasspath() throws Exception {
        String truststore = "classpath:" + TRUSTSTORE_PATH;
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
        URL trustStoreResource = LineSenderBuilderTest.class.getResource(TRUSTSTORE_PATH);
        assertNotNull("Someone accidenteally deleted trust store?", trustStoreResource);
        String truststore = trustStoreResource.getFile();
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
        String truststore = "classpath:" + TRUSTSTORE_PATH;
        runInContext(r -> {
            Sender.LineSenderBuilder builder = Sender.builder()
                    .address(LOCALHOST)
                    .port(TLS_PROXY.getListeningPort())
                    .enableTls().advancedTls().customTrustStore(truststore, "wrong password".toCharArray());
            try {
                builder.build();
                fail("non existing trust store should throw an exception");
            } catch (LineSenderException e) {
                TestUtils.assertContains(e.getMessage(), "could not create SSL engine");
                TestUtils.assertContains(e.getCause().getMessage(), "password");
            }
        });
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
    public void testDnsResolutionFail() throws Exception {
        assertMemoryLeak(() -> {
            try {
                Sender.builder().address("this-domain-does-not-exist-i-hope-better-to-use-a-silly-tld.silly-tld").build();
                fail("dns resolution errors should fail fast");
            } catch (LineSenderException e) {
                TestUtils.assertContains(e.getMessage(), "could not resolve");
            }
        });
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
    public void testMalformedPortInAddress() throws Exception {
        assertMemoryLeak(() -> {
            Sender.LineSenderBuilder builder = Sender.builder();
            try {
                builder.address("foo:nonsense12334");
                fail("should fail with malformated port");
            } catch (LineSenderException e) {
                TestUtils.assertContains(e.getMessage(), "cannot parse port");
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
}
