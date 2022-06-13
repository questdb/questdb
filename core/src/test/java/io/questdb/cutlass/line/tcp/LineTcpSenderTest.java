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

package io.questdb.cutlass.line.tcp;

import io.questdb.cutlass.line.LineSenderException;
import io.questdb.cutlass.line.LineTcpSender;
import io.questdb.cutlass.line.Sender;
import io.questdb.network.Net;
import io.questdb.network.NetworkError;
import io.questdb.std.Os;
import org.junit.Test;

import java.net.Inet4Address;
import java.security.PrivateKey;

import static io.questdb.test.tools.TestUtils.assertContains;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.junit.Assert.*;

public class LineTcpSenderTest extends AbstractLineTcpReceiverTest {

    private final static String AUTH_KEY_ID1 = "testUser1";
    private final static String AUTH_KEY_ID2_INVALID = "invalid";
    private final static String TOKEN = "UvuVb1USHGRRT08gEnwN2zGZrvM4MsLQ5brgF6SVkAw=";
    private final static PrivateKey AUTH_PRIVATE_KEY1 = AuthDb.importPrivateKey(TOKEN);
    private final static int HOST = Net.parseIPv4("127.0.0.1");

    @Test
    public void testMinBufferSizeWhenAuth() {
        authKeyId = AUTH_KEY_ID1;
        int tinyCapacity = 42;
        try {
            LineTcpSender.authenticatedPlainTextSender(HOST, bindPort, tinyCapacity, AUTH_KEY_ID1, AUTH_PRIVATE_KEY1);
            fail();
        } catch (LineSenderException e) {
            assertContains(e.getMessage(), "buffer capacity");
        }
    }

    @Test
    public void testAuthSuccess() throws Exception {
        authKeyId = AUTH_KEY_ID1;
        runInContext(r -> {
            int bufferCapacity = 256 * 1024;

            try (LineTcpSender sender = LineTcpSender.authenticatedPlainTextSender(HOST, bindPort, bufferCapacity, AUTH_KEY_ID1, AUTH_PRIVATE_KEY1)) {
                sender.metric("mytable").field("my int field", 42).$();
                sender.flush();
            }

            assertTableExistsEventually(engine, "mytable");
        });
    }

    @Test
    public void testAuthWrongKey() throws Exception {
        authKeyId = AUTH_KEY_ID1;
        runInContext(r -> {
            int bufferCapacity = 2048;

            try (LineTcpSender sender = LineTcpSender.authenticatedPlainTextSender(HOST, bindPort, bufferCapacity, AUTH_KEY_ID2_INVALID, AUTH_PRIVATE_KEY1)) {
                //30 seconds should be enough even on a slow CI server
                long deadline = Os.currentTimeNanos() + SECONDS.toNanos(30);
                try {
                    while (Os.currentTimeNanos() < deadline) {
                        sender.metric("mytable").field("my int field", 42).$();
                        sender.flush();
                    }
                    fail("Client fail to detected qdb server closed a connection due to wrong credentials");
                } catch (LineSenderException e) {
                    // expected
                }
            }
        });
    }

    @Test
    public void testBuilderAuthSuccess() throws Exception {
        authKeyId = AUTH_KEY_ID1;
        String address = "127.0.0.1:" + bindPort;
        runInContext(r -> {
            try (Sender sender = Sender.builder()
                    .address(address)
                    .enableAuth(AUTH_KEY_ID1).token(TOKEN)
                    .build()) {
                sender.table("mytable").column("my int field", 42).atNow();
                sender.flush();
            }
            assertTableExistsEventually(engine, "mytable");
        });
    }

    @Test
    public void testBuilderPlainText_addressWithIpAndPort() throws Exception {
        String address = "127.0.0.1:" + bindPort;
        runInContext(r -> {
            try (Sender sender = Sender.builder()
                    .address(address)
                    .build()) {
                sender.table("mytable").column("my int field", 42).atNow();
                sender.flush();
            }
            assertTableExistsEventually(engine, "mytable");
        });
    }

    @Test
    public void testBuilderPlainText_addressWithHostnameAndPort() throws Exception {
        String address = "localhost:" + bindPort;
        runInContext(r -> {
            try (Sender sender = Sender.builder()
                    .address(address)
                    .build()) {
                sender.table("mytable").column("my int field", 42).atNow();
                sender.flush();
            }
            assertTableExistsEventually(engine, "mytable");
        });
    }

    @Test
    public void testBuilderPlainText_addressWithExplicitIpAndPort() throws Exception {
        runInContext(r -> {
            try (Sender sender = Sender.builder()
                    .address("127.0.0.1")
                    .port(bindPort)
                    .build()) {
                sender.table("mytable").column("my int field", 42).atNow();
                sender.flush();
            }
            assertTableExistsEventually(engine, "mytable");
        });
    }

    @Test
    public void testBuilderPlainText_withExplicitHostnameAndPort() throws Exception {
        runInContext(r -> {
            try (Sender sender = Sender.builder()
                    .host(Inet4Address.getByName("localhost"))
                    .port(bindPort)
                    .build()) {
                sender.table("mytable").column("my int field", 42).atNow();
                sender.flush();
            }
            assertTableExistsEventually(engine, "mytable");
        });
    }
}
