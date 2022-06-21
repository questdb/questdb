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

import static org.junit.Assert.fail;

public class LineSenderBuilderTest extends AbstractLineTcpReceiverTest {
    @ClassRule
    public static final TlsProxyRule TLS_PROXY = TlsProxyRule.toHostAndPort("localhost", 9002);

    private static final String LOCALHOST = "127.0.0.1";

    @Test
    public void testHostNorAddressSet() {
        Sender.LineSenderBuilder builder = Sender.builder();
        try {
            builder.build();
            fail("not host should fail");
        } catch (LineSenderException e) {
            TestUtils.assertContains(e.getMessage(), "server address not set");
        }
    }

    @Test
    public void testAddressEmpty() {
        Sender.LineSenderBuilder builder = Sender.builder();
        try {
            builder.address("");
            fail("empty host should fail");
        } catch (LineSenderException e) {
            TestUtils.assertContains(e.getMessage(), "address cannot be empty");
        }
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
    public void testConnectPlainAuthWithTokenSuccess() throws Exception {
        authKeyId = AUTH_KEY_ID1;
        runInContext(r -> {
            try (Sender sender = Sender.builder()
                    .address(LOCALHOST)
                    .port(bindPort)
                    .enableAuth(AUTH_KEY_ID1)
                    .token(AUTH_TOKEN_KEY1)
                    .build()) {
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
    public void testConnectTls_TruststoreFile() throws Exception {
        String truststore = LineSenderBuilderTest.class.getResource(TRUSTSTORE_PATH).getFile();
        runInContext(r -> {
            try (Sender sender = Sender.builder()
                    .address(LOCALHOST)
                    .port(TLS_PROXY.getListeningPort())
                    .enableTls().customTrustStore(truststore, TRUSTSTORE_PASSWORD)
                    .build()) {
                sender.table("mytable").symbol("symbol", "symbol").atNow();
                sender.flush();
                assertTableExistsEventually(engine, "mytable");
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
                    .enableTls().customTrustStore(truststore, TRUSTSTORE_PASSWORD)
                    .build()) {
                sender.table("mytable").symbol("symbol", "symbol").atNow();
                sender.flush();
                assertTableExistsEventually(engine, "mytable");
            }
        });
    }

    @Test
    public void testConnectTls_NonExistingTrustoreFile() throws Exception {
        String truststore = "/foo/whatever/non-existing";
        Sender.LineSenderBuilder builder = Sender.builder()
                .address(LOCALHOST)
                .port(TLS_PROXY.getListeningPort())
                .enableTls().customTrustStore(truststore, TRUSTSTORE_PASSWORD);

        runInContext(r -> {
            try {
                builder.build();
                fail("non existing trustore should throw an exception");
            } catch (LineSenderException e) {
                TestUtils.assertContains(e.getMessage(), "error while creating openssl engine");
            }
        });
    }

    @Test
    public void testConnectTls_NonExistingTrustoreClaspath() throws Exception {
        String truststore = "classpath:/foo/whatever/non-existing";
        Sender.LineSenderBuilder builder = Sender.builder()
                .address(LOCALHOST)
                .port(TLS_PROXY.getListeningPort())
                .enableTls().customTrustStore(truststore, TRUSTSTORE_PASSWORD);

        runInContext(r -> {
            try {
                builder.build();
                fail("non existing trustore should throw an exception");
            } catch (LineSenderException e) {
                TestUtils.assertContains(e.getMessage(), "is unavailable on a classpath");
            }
        });
    }

    @Test
    public void testConnectTls_WrongTruststorePassword() throws Exception {
        String truststore = "classpath:" + TRUSTSTORE_PATH;
        Sender.LineSenderBuilder builder = Sender.builder()
                .address(LOCALHOST)
                .port(TLS_PROXY.getListeningPort())
                .enableTls().customTrustStore(truststore, "wrong password".toCharArray());
        runInContext(r -> {
            try (Sender sender = builder.build()) {
                fail("non existing trustore should throw an exception");
            } catch (LineSenderException e) {
                TestUtils.assertContains(e.getMessage(), "error while creating openssl engine");
                TestUtils.assertContains(e.getCause().getMessage(), "password");
            }
        });
    }

    @Test
    public void testConnectTlsAuthWithTokenSuccess() throws Exception {
        authKeyId = AUTH_KEY_ID1;
        String truststore = "classpath:" + TRUSTSTORE_PATH;
        Sender.LineSenderBuilder builder = Sender.builder()
                .address(LOCALHOST)
                .port(TLS_PROXY.getListeningPort())
                .enableAuth(AUTH_KEY_ID1).token(AUTH_TOKEN_KEY1)
                .enableTls().customTrustStore(truststore, TRUSTSTORE_PASSWORD);
        runInContext(r -> {
            try (Sender sender = builder.build()) {
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
        Sender.LineSenderBuilder builder = Sender.builder()
                .address(LOCALHOST)
                .port(TLS_PROXY.getListeningPort())
                .enableAuth(AUTH_KEY_ID1).privateKey(AUTH_PRIVATE_KEY1)
                .enableTls().customTrustStore(truststore, TRUSTSTORE_PASSWORD);
        runInContext(r -> {
            try (Sender sender = builder.build()) {
                sender.table("mytable").symbol("symbol", "symbol").atNow();
                sender.flush();
                assertTableExistsEventually(engine, "mytable");
            }
        });
    }

    @Test
    public void testAuthWithBadToken() throws Exception {
        Sender.LineSenderBuilder.AuthBuilder builder = Sender.builder().enableAuth("foo");
        try {
            builder.token("bar token");
            fail("bad token should not be imported");
        } catch (LineSenderException e) {
            TestUtils.assertContains(e.getMessage(), "cannot import token");
        }
    }

    @Test
    public void testAuthTooSmallBuffer() {
        Sender.LineSenderBuilder builder = Sender.builder()
                .enableAuth("foo").token(AUTH_TOKEN_KEY1).address(LOCALHOST+":9001")
                .bufferCapacity(1);
        try {
            builder.build();
            fail("tiny buffer should be be allowed as it wont fit auth challenge");
        } catch (LineSenderException e) {
            TestUtils.assertContains(e.getMessage(), "capacity");
        }
    }

    @Test
    public void testPlainAuth_connectionRefused() {
        Sender.LineSenderBuilder builder = Sender.builder()
                .enableAuth("foo").token(AUTH_TOKEN_KEY1).address(LOCALHOST+":19003");
        try {
            builder.build();
            fail("connection refused should fail fast");
        } catch (LineSenderException e) {
            TestUtils.assertContains(e.getMessage(), "could not connect");
        }
    }

    @Test
    public void testPlain_connectionRefused() {
        Sender.LineSenderBuilder builder = Sender.builder().address(LOCALHOST+":19003");
        try {
            builder.build();
            fail("connection refused should fail fast");
        } catch (LineSenderException e) {
            TestUtils.assertContains(e.getMessage(), "could not connect");
        }
    }

    @Test
    public void testTls_connectionRefused() {
        Sender.LineSenderBuilder builder = Sender.builder().enableTls().address(LOCALHOST+":19003");
        try {
            builder.build();
            fail("connection refused should fail fast");
        } catch (LineSenderException e) {
            TestUtils.assertContains(e.getMessage(), "could not connect");
        }
    }

    @Test
    public void testDnsResolutionFail() {
        try {
            Sender.builder().address("this-domain-does-not-exist-i-hope-better-to-use-a-silly-tld.silly-tld").build();
            fail("dns resolution errors should fail fast");
        } catch (LineSenderException e) {
            TestUtils.assertContains(e.getMessage(), "cannot resolve");
        }
    }

    @Test
    public void testAddressDoubleSet_firstAddressThenAddress() {
        Sender.LineSenderBuilder builder = Sender.builder().address(LOCALHOST);
        try {
            builder.address("127.0.0.1");
            fail("should not allow double host set");
        } catch (LineSenderException e) {
            TestUtils.assertContains(e.getMessage(), "already configured");
        }
    }

    @Test
    public void testPortDoubleSet_firstPortThenPort() {
        Sender.LineSenderBuilder builder = Sender.builder().port(9000);
        try {
            builder.port(9000);
            fail("should not allow double port set");
        } catch (LineSenderException e) {
            TestUtils.assertContains(e.getMessage(), "already configured");
        }
    }

    @Test
    public void testPortDoubleSet_firstAddressThenPort() {
        Sender.LineSenderBuilder builder = Sender.builder().address(LOCALHOST+":9000");
        try {
            builder.port(9000);
            fail("should not allow double port set");
        } catch (LineSenderException e) {
            TestUtils.assertContains(e.getMessage(), "already configured");
        }
    }

    @Test
    public void testPortDoubleSet_firstPortThenAddress() {
        Sender.LineSenderBuilder builder = Sender.builder().port(9000);
        try {
            builder.address(LOCALHOST + ":9000");
            fail("should not allow double port set");
        } catch (LineSenderException e) {
            TestUtils.assertContains(e.getMessage(), "already configured");
        }
    }

    @Test
    public void testAuthDoubleSet() {
        Sender.LineSenderBuilder builder = Sender.builder().enableAuth("foo").token(AUTH_TOKEN_KEY1);
        try {
            builder.enableAuth("bar");
            fail("should not allow double auth set");
        } catch (LineSenderException e) {
            TestUtils.assertContains(e.getMessage(), "already configured");
        }
    }

    @Test
    public void testTlsDoubleSet() {
        Sender.LineSenderBuilder builder = Sender.builder().enableTls();
        try {
            builder.enableTls();
            fail("should not allow double tls set");
        } catch (LineSenderException e) {
            TestUtils.assertContains(e.getMessage(), "already configured");
        }
    }

    @Test
    public void testBufferSizeDoubleSet() {
        Sender.LineSenderBuilder builder = Sender.builder().bufferCapacity(1024);
        try {
            builder.bufferCapacity(1024);
            fail("should not allow double buffer capacity set");
        } catch (LineSenderException e) {
            TestUtils.assertContains(e.getMessage(), "already configured");
        }
    }

    @Test
    public void testCustomTruststoreDoubleSet() {
        Sender.LineSenderBuilder builder = Sender.builder().customTrustStore(TRUSTSTORE_PATH, TRUSTSTORE_PASSWORD);
        try {
            builder.customTrustStore(TRUSTSTORE_PATH, TRUSTSTORE_PASSWORD);
            fail("should not allow double custom trust store set");
        } catch (LineSenderException e) {
            TestUtils.assertContains(e.getMessage(), "already configured");
        }
    }

    @Test
    public void testCustomTruststoreButTlsNotEnabled() {
        Sender.LineSenderBuilder builder = Sender.builder()
                .customTrustStore(TRUSTSTORE_PATH, TRUSTSTORE_PASSWORD)
                .address(LOCALHOST);
        try {
            builder.build();
            fail("should not fail when custom trust store configured, but TLS not enabled");
        } catch (LineSenderException e) {
            TestUtils.assertContains(e.getMessage(), "TLS was not enabled");
        }
    }
}
