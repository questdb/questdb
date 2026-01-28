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

package io.questdb.test.cutlass.line.tcp;

import io.questdb.client.Sender;
import io.questdb.client.cutlass.line.LineSenderException;
import io.questdb.std.Files;
import io.questdb.test.tools.TestUtils;
import io.questdb.test.tools.TlsProxyRule;
import org.junit.Rule;
import org.junit.Test;

import java.util.UUID;

import static org.junit.Assert.fail;


public class LineTlsTcpSenderTest extends AbstractLineTcpReceiverTest {

    @Rule
    public TlsProxyRule tlsProxy = TlsProxyRule.toHostAndPort("localhost", 9002);

    private static void withCustomProperty(RunnableWithException runnable, String key, String value) throws Exception {
        String orig = System.getProperty(key);
        System.setProperty(key, value);
        try {
            runnable.run();
        } finally {
            if (orig != null) {
                System.setProperty(key, orig);
            } else {
                System.clearProperty(key);
            }
        }
    }

    @Test
    public void simpleTest() throws Exception {
        authKeyId = AUTH_KEY_ID1;
        String tableName = UUID.randomUUID().toString();
        runInContext(c -> {
            try (Sender sender = Sender.builder(Sender.Transport.TCP)
                    .enableTls()
                    .address("localhost")
                    .port(tlsProxy.getListeningPort())
                    .enableAuth(AUTH_KEY_ID1).authToken(AUTH_TOKEN_KEY1)
                    .advancedTls().customTrustStore(Files.getResourcePath(getClass().getResource(TRUSTSTORE_PATH)), TRUSTSTORE_PASSWORD)
                    .build()) {
                sender.table(tableName).longColumn("value", 42).atNow();
                sender.flush();
                assertTableExistsEventually(engine, tableName);
            }
        });
    }

    @Test
    public void testCertValidationDisabled() throws Exception {
        String tableName = UUID.randomUUID().toString();
        int rows = 5_000;
        runInContext(c -> {
            try (Sender sender = Sender.builder(Sender.Transport.TCP)
                    .enableTls()
                    .address("127.0.0.1")
                    .port(tlsProxy.getListeningPort())
                    .advancedTls().disableCertificateValidation()
                    .build()) {

                for (int i = 0; i < rows; i++) {
                    sender.table(tableName).longColumn("value", 42).atNow();
                    sender.flush();
                }
                assertTableSizeEventually(engine, tableName, rows);
            }
        });
    }

    @Test
    public void testHostnameValidation() throws Exception {
        runInContext(c -> {
            Sender.LineSenderBuilder builder = Sender.builder(Sender.Transport.TCP)
                    .enableTls()
                    .address("127.0.0.1")
                    .port(tlsProxy.getListeningPort())
                    .advancedTls().customTrustStore(Files.getResourcePath(getClass().getResource(TRUSTSTORE_PATH)), TRUSTSTORE_PASSWORD);
            try {
                builder.build();
                fail("should have failed during handshake, because we are connecting to 127.0.0.1, " +
                        "but the server certificated was issued for localhost");
            } catch (LineSenderException expected) {
                TestUtils.assertContains(expected.getMessage(), "TLS handshake");
            }
        });
    }

    @Test
    public void testTinyTlsBuffers() throws Exception {
        String tableName = UUID.randomUUID().toString();
        int rows = 5_000;
        withCustomProperty(() -> {
            authKeyId = AUTH_KEY_ID1;
            runInContext(c -> {
                try (Sender sender = Sender.builder(Sender.Transport.TCP)
                        .enableTls()
                        .address("localhost")
                        .port(tlsProxy.getListeningPort())
                        .enableAuth(AUTH_KEY_ID1).authToken(AUTH_TOKEN_KEY1)
                        .advancedTls().customTrustStore(Files.getResourcePath(getClass().getResource(TRUSTSTORE_PATH)), TRUSTSTORE_PASSWORD)
                        .build()) {

                    for (int i = 0; i < rows; i++) {
                        sender.table(tableName).longColumn("value", 42).atNow();
                        sender.flush();
                    }
                    assertTableSizeEventually(engine, tableName, rows);
                }
            });
        }, "questdb.experimental.tls.buffersize", "1");
    }

    @Test
    public void testWithCustomTruststoreByFilename() throws Exception {
        authKeyId = AUTH_KEY_ID1;
        String tableName = UUID.randomUUID().toString();
        String truststore = Files.getResourcePath(getClass().getResource(TRUSTSTORE_PATH));
        runInContext(c -> {
            try (Sender sender = Sender.builder(Sender.Transport.TCP)
                    .enableTls()
                    .address("localhost")
                    .port(tlsProxy.getListeningPort())
                    .enableAuth(AUTH_KEY_ID1).authToken(AUTH_TOKEN_KEY1)
                    .advancedTls().customTrustStore(truststore, TRUSTSTORE_PASSWORD)
                    .build()) {
                sender.table(tableName).longColumn("value", 42).atNow();
                sender.flush();
                assertTableExistsEventually(engine, tableName);
            }
        });
    }

    @Test
    public void testWithoutExplicitFlushing() throws Exception {
        // no explicit flushing results in high buffers utilization

        authKeyId = AUTH_KEY_ID1;
        String tableName = UUID.randomUUID().toString();
        int hugeBufferSize = 1024 * 1024;
        int rows = 100_000;
        runInContext(c -> {
            try (Sender sender = Sender.builder(Sender.Transport.TCP)
                    .enableTls()
                    .bufferCapacity(hugeBufferSize)
                    .address("localhost")
                    .port(tlsProxy.getListeningPort())
                    .enableAuth(AUTH_KEY_ID1).authToken(AUTH_TOKEN_KEY1)
                    .advancedTls().customTrustStore(Files.getResourcePath(getClass().getResource(TRUSTSTORE_PATH)), TRUSTSTORE_PASSWORD)
                    .build()) {
                for (long l = 0; l < rows; l++) {
                    sender.table(tableName).longColumn("value", 42).atNow();
                }
                sender.flush();
                assertTableSizeEventually(engine, tableName, rows);
            }
        });
    }

    private interface RunnableWithException {
        void run() throws Exception;
    }
}
