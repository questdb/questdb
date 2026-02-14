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

package io.questdb.test.cutlass.http.websocket;

import io.questdb.PropertyKey;
import io.questdb.test.AbstractBootstrapTest;
import io.questdb.test.TestServerMain;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.util.Base64;

/**
 * Tests for WebSocket upgrade handshake on the /write/v4 endpoint.
 * These tests verify that the server correctly handles WebSocket upgrade requests.
 */
public class QwpWebSocketHandshakeTest extends AbstractBootstrapTest {

    private static final String WEBSOCKET_GUID = "258EAFA5-E914-47DA-95CA-C5AB0DC85B11";

    @Override
    @Before
    public void setUp() {
        super.setUp();
        TestUtils.unchecked(() -> createDummyConfiguration());
        dbPath.parent().$();
    }

    @Test
    public void testWebSocketUpgradeSucceeds() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables(
                    PropertyKey.HTTP_RECEIVE_BUFFER_SIZE.getEnvVarName(), "65536"
            )) {
                int httpPort = serverMain.getHttpServerPort();

                // Generate a random WebSocket key
                byte[] keyBytes = new byte[16];
                for (int i = 0; i < 16; i++) {
                    keyBytes[i] = (byte) (Math.random() * 256);
                }
                String wsKey = Base64.getEncoder().encodeToString(keyBytes);

                // Send WebSocket upgrade request
                try (Socket socket = new Socket("localhost", httpPort)) {
                    socket.setSoTimeout(5000);
                    OutputStream out = socket.getOutputStream();

                    String request = "GET /write/v4 HTTP/1.1\r\n" +
                            "Host: localhost:" + httpPort + "\r\n" +
                            "Upgrade: websocket\r\n" +
                            "Connection: Upgrade\r\n" +
                            "Sec-WebSocket-Key: " + wsKey + "\r\n" +
                            "Sec-WebSocket-Version: 13\r\n" +
                            "\r\n";

                    out.write(request.getBytes(StandardCharsets.UTF_8));
                    out.flush();

                    // Read response
                    BufferedReader reader = new BufferedReader(
                            new InputStreamReader(socket.getInputStream(), StandardCharsets.UTF_8));

                    String statusLine = reader.readLine();
                    Assert.assertNotNull("Should receive response", statusLine);
                    Assert.assertTrue("Should be 101 Switching Protocols: " + statusLine,
                            statusLine.contains("101"));

                    // Read headers
                    String line;
                    String acceptKey = null;
                    boolean hasUpgradeHeader = false;
                    boolean hasConnectionHeader = false;

                    while ((line = reader.readLine()) != null && !line.isEmpty()) {
                        if (line.toLowerCase().startsWith("sec-websocket-accept:")) {
                            acceptKey = line.substring(line.indexOf(':') + 1).trim();
                        } else if (line.toLowerCase().startsWith("upgrade:")) {
                            hasUpgradeHeader = line.toLowerCase().contains("websocket");
                        } else if (line.toLowerCase().startsWith("connection:")) {
                            hasConnectionHeader = line.toLowerCase().contains("upgrade");
                        }
                    }

                    Assert.assertTrue("Should have Upgrade: websocket header", hasUpgradeHeader);
                    Assert.assertTrue("Should have Connection: Upgrade header", hasConnectionHeader);
                    Assert.assertNotNull("Should have Sec-WebSocket-Accept header", acceptKey);

                    // Verify the accept key
                    String expectedAccept = computeAcceptKey(wsKey);
                    Assert.assertEquals("Accept key should match", expectedAccept, acceptKey);
                }
            }
        });
    }

    @Test
    public void testWebSocketUpgradeWithMissingKeyFails() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables(
                    PropertyKey.HTTP_RECEIVE_BUFFER_SIZE.getEnvVarName(), "65536"
            )) {
                int httpPort = serverMain.getHttpServerPort();

                try (Socket socket = new Socket("localhost", httpPort)) {
                    socket.setSoTimeout(5000);
                    OutputStream out = socket.getOutputStream();

                    // Missing Sec-WebSocket-Key
                    String request = "GET /write/v4 HTTP/1.1\r\n" +
                            "Host: localhost:" + httpPort + "\r\n" +
                            "Upgrade: websocket\r\n" +
                            "Connection: Upgrade\r\n" +
                            "Sec-WebSocket-Version: 13\r\n" +
                            "\r\n";

                    out.write(request.getBytes(StandardCharsets.UTF_8));
                    out.flush();

                    BufferedReader reader = new BufferedReader(
                            new InputStreamReader(socket.getInputStream(), StandardCharsets.UTF_8));

                    String statusLine = reader.readLine();
                    Assert.assertNotNull("Should receive response", statusLine);
                    // Should NOT be 101 - missing key
                    Assert.assertFalse("Should not upgrade without key: " + statusLine,
                            statusLine.contains("101"));
                }
            }
        });
    }

    @Test
    public void testWebSocketUpgradeWithWrongVersionFails() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables(
                    PropertyKey.HTTP_RECEIVE_BUFFER_SIZE.getEnvVarName(), "65536"
            )) {
                int httpPort = serverMain.getHttpServerPort();

                byte[] keyBytes = new byte[16];
                for (int i = 0; i < 16; i++) {
                    keyBytes[i] = (byte) (Math.random() * 256);
                }
                String wsKey = Base64.getEncoder().encodeToString(keyBytes);

                try (Socket socket = new Socket("localhost", httpPort)) {
                    socket.setSoTimeout(5000);
                    OutputStream out = socket.getOutputStream();

                    // Wrong version (8 instead of 13)
                    String request = "GET /write/v4 HTTP/1.1\r\n" +
                            "Host: localhost:" + httpPort + "\r\n" +
                            "Upgrade: websocket\r\n" +
                            "Connection: Upgrade\r\n" +
                            "Sec-WebSocket-Key: " + wsKey + "\r\n" +
                            "Sec-WebSocket-Version: 8\r\n" +
                            "\r\n";

                    out.write(request.getBytes(StandardCharsets.UTF_8));
                    out.flush();

                    BufferedReader reader = new BufferedReader(
                            new InputStreamReader(socket.getInputStream(), StandardCharsets.UTF_8));

                    String statusLine = reader.readLine();
                    Assert.assertNotNull("Should receive response", statusLine);
                    // Should NOT be 101 - wrong version
                    Assert.assertFalse("Should not upgrade with wrong version: " + statusLine,
                            statusLine.contains("101"));
                }
            }
        });
    }

    /**
     * Computes the expected Sec-WebSocket-Accept value.
     */
    private String computeAcceptKey(String key) throws Exception {
        String combined = key + WEBSOCKET_GUID;
        MessageDigest sha1 = MessageDigest.getInstance("SHA-1");
        byte[] hash = sha1.digest(combined.getBytes(StandardCharsets.UTF_8));
        return Base64.getEncoder().encodeToString(hash);
    }
}
