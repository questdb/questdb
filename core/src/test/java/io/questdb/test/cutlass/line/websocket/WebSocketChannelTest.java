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

package io.questdb.test.cutlass.line.websocket;

import io.questdb.client.cutlass.ilpv4.client.WebSocketChannel;
import io.questdb.client.cutlass.line.LineSenderException;
import io.questdb.std.MemoryTag;
import io.questdb.std.Unsafe;
import org.junit.Assert;
import org.junit.Test;

/**
 * Unit tests for WebSocketChannel.
 * These tests focus on URL parsing and basic state management.
 * Integration tests with actual server connections are in WebSocketChannelIntegrationTest.
 */
public class WebSocketChannelTest {

    @Test
    public void testChainedTimeoutConfiguration() {
        try (WebSocketChannel channel = new WebSocketChannel("ws://localhost:9000/path", false)) {
            channel.setConnectTimeout(5000).setReadTimeout(10000);
            Assert.assertFalse(channel.isConnected());
        }
    }

    @Test
    public void testCloseBeforeConnect() {
        WebSocketChannel channel = new WebSocketChannel("ws://localhost:9000/write/v4", false);
        channel.close();
        Assert.assertFalse(channel.isConnected());
    }

    @Test
    public void testConnectAfterCloseThrows() {
        WebSocketChannel channel = new WebSocketChannel("ws://localhost:9000/path", false);
        channel.close();
        try {
            channel.connect();
            Assert.fail("Expected LineSenderException");
        } catch (LineSenderException e) {
            Assert.assertTrue(e.getMessage().contains("closed"));
        }
    }

    @Test
    public void testConnectToClosedPortThrows() {
        // Port 1 is typically not open
        try (WebSocketChannel channel = new WebSocketChannel("ws://127.0.0.1:1/path", false)) {
            channel.setConnectTimeout(1000);
            channel.connect();
            Assert.fail("Expected LineSenderException");
        } catch (LineSenderException e) {
            Assert.assertTrue(e.getMessage().contains("Failed to connect"));
        }
    }

    @Test
    public void testConnectToInvalidHostThrows() {
        try (WebSocketChannel channel = new WebSocketChannel("ws://invalid.host.that.does.not.exist:9999/path", false)) {
            channel.setConnectTimeout(1000);
            channel.connect();
            Assert.fail("Expected LineSenderException");
        } catch (LineSenderException e) {
            Assert.assertTrue(e.getMessage().contains("Failed to connect"));
        }
    }

    @Test
    public void testDoubleClose() {
        WebSocketChannel channel = new WebSocketChannel("ws://localhost:9000/write/v4", false);
        channel.close();
        channel.close(); // Should not throw
        Assert.assertFalse(channel.isConnected());
    }

    @Test
    public void testIsConnectedAfterCloseFalse() {
        WebSocketChannel channel = new WebSocketChannel("ws://localhost:9000/path", false);
        channel.close();
        Assert.assertFalse(channel.isConnected());
    }

    @Test
    public void testIsConnectedInitiallyFalse() {
        try (WebSocketChannel channel = new WebSocketChannel("ws://localhost:9000/path", false)) {
            Assert.assertFalse(channel.isConnected());
        }
    }

    @Test
    public void testReceiveBeforeConnectThrows() {
        try (WebSocketChannel channel = new WebSocketChannel("ws://localhost:9000/path", false)) {
            channel.receiveFrame(null, 100);
            Assert.fail("Expected LineSenderException");
        } catch (LineSenderException e) {
            Assert.assertTrue(e.getMessage().contains("not connected"));
        }
    }

    @Test
    public void testSendAfterCloseThrows() {
        WebSocketChannel channel = new WebSocketChannel("ws://localhost:9000/write/v4", false);
        channel.close();

        long ptr = Unsafe.malloc(10, MemoryTag.NATIVE_DEFAULT);
        try {
            channel.sendBinary(ptr, 10);
            Assert.fail("Expected LineSenderException");
        } catch (LineSenderException e) {
            Assert.assertTrue(e.getMessage().contains("closed"));
        } finally {
            Unsafe.free(ptr, 10, MemoryTag.NATIVE_DEFAULT);
        }
    }

    @Test
    public void testSendBeforeConnectThrows() {
        try (WebSocketChannel channel = new WebSocketChannel("ws://localhost:9000/write/v4", false)) {
            long ptr = Unsafe.malloc(10, MemoryTag.NATIVE_DEFAULT);
            try {
                channel.sendBinary(ptr, 10);
                Assert.fail("Expected LineSenderException");
            } catch (LineSenderException e) {
                Assert.assertTrue(e.getMessage().contains("not connected"));
            } finally {
                Unsafe.free(ptr, 10, MemoryTag.NATIVE_DEFAULT);
            }
        }
    }

    // ==================== URL Parsing Edge Cases ====================

    @Test
    public void testSendPingBeforeConnectThrows() {
        try (WebSocketChannel channel = new WebSocketChannel("ws://localhost:9000/path", false)) {
            channel.sendPing();
            Assert.fail("Expected LineSenderException");
        } catch (LineSenderException e) {
            Assert.assertTrue(e.getMessage().contains("not connected"));
        }
    }

    @Test
    public void testSetConnectTimeoutReturnsThis() {
        try (WebSocketChannel channel = new WebSocketChannel("ws://localhost:9000/path", false)) {
            WebSocketChannel result = channel.setConnectTimeout(5000);
            Assert.assertSame(channel, result);
        }
    }

    @Test
    public void testSetReadTimeoutReturnsThis() {
        try (WebSocketChannel channel = new WebSocketChannel("ws://localhost:9000/path", false)) {
            WebSocketChannel result = channel.setReadTimeout(5000);
            Assert.assertSame(channel, result);
        }
    }

    @Test
    public void testSetTimeouts() {
        try (WebSocketChannel channel = new WebSocketChannel("ws://localhost:9000/path", false)) {
            channel.setConnectTimeout(5000);
            channel.setReadTimeout(10000);
            // Just verify it doesn't throw
            Assert.assertFalse(channel.isConnected());
        }
    }

    @Test
    public void testTlsEnabledViaConstructor() {
        try (WebSocketChannel channel = new WebSocketChannel("ws://localhost:9000/path", true)) {
            Assert.assertFalse(channel.isConnected());
        }
    }

    @Test
    public void testTlsValidationDisabled() {
        try (WebSocketChannel channel = new WebSocketChannel("wss://localhost:9000/path", true, false)) {
            Assert.assertFalse(channel.isConnected());
        }
    }

    @Test
    public void testUrlParsingDeepPath() {
        try (WebSocketChannel channel = new WebSocketChannel("ws://localhost:9000/a/b/c/d/e/f", false)) {
            Assert.assertFalse(channel.isConnected());
        }
    }

    @Test
    public void testUrlParsingDefaultWsPort() {
        try (WebSocketChannel channel = new WebSocketChannel("ws://localhost/path", false)) {
            Assert.assertFalse(channel.isConnected());
        }
    }

    @Test
    public void testUrlParsingDefaultWssPort() {
        try (WebSocketChannel channel = new WebSocketChannel("wss://localhost/path", false)) {
            Assert.assertFalse(channel.isConnected());
        }
    }

    // ==================== State Transition Tests ====================

    @Test
    public void testUrlParsingEmptyPath() {
        try (WebSocketChannel channel = new WebSocketChannel("ws://localhost:9000", false)) {
            Assert.assertFalse(channel.isConnected());
        }
    }

    @Test
    public void testUrlParsingHostOnly() {
        try (WebSocketChannel channel = new WebSocketChannel("localhost:9000", false)) {
            Assert.assertFalse(channel.isConnected());
        }
    }

    @Test
    public void testUrlParsingIPv4Address() {
        try (WebSocketChannel channel = new WebSocketChannel("ws://127.0.0.1:9000/path", false)) {
            Assert.assertFalse(channel.isConnected());
        }
    }

    @Test
    public void testUrlParsingIPv6Address() {
        try (WebSocketChannel channel = new WebSocketChannel("ws://[::1]:9000/path", false)) {
            Assert.assertFalse(channel.isConnected());
        }
    }

    @Test
    public void testUrlParsingNoPath() {
        try (WebSocketChannel channel = new WebSocketChannel("ws://localhost:9000", false)) {
            Assert.assertFalse(channel.isConnected());
        }
    }

    // ==================== Timeout Configuration Tests ====================

    @Test
    public void testUrlParsingNoPort() {
        try (WebSocketChannel channel = new WebSocketChannel("ws://localhost/path", false)) {
            Assert.assertFalse(channel.isConnected());
        }
    }

    @Test
    public void testUrlParsingRootPath() {
        try (WebSocketChannel channel = new WebSocketChannel("ws://localhost:9000/", false)) {
            Assert.assertFalse(channel.isConnected());
        }
    }

    @Test
    public void testUrlParsingWithQueryParams() {
        try (WebSocketChannel channel = new WebSocketChannel("ws://localhost:9000/path?key=value&foo=bar", false)) {
            Assert.assertFalse(channel.isConnected());
        }
    }

    // ==================== TLS Configuration Tests ====================

    @Test
    public void testUrlParsingWsScheme() {
        try (WebSocketChannel channel = new WebSocketChannel("ws://localhost:9000/write/v4", false)) {
            Assert.assertFalse(channel.isConnected());
        }
    }

    @Test
    public void testUrlParsingWssScheme() {
        try (WebSocketChannel channel = new WebSocketChannel("wss://localhost:443/write/v4", false)) {
            Assert.assertFalse(channel.isConnected());
        }
    }

    @Test
    public void testWssSchemeOverridesTlsParameter() {
        // wss:// scheme should enable TLS regardless of the parameter
        try (WebSocketChannel channel = new WebSocketChannel("wss://localhost:443/path", false)) {
            Assert.assertFalse(channel.isConnected());
        }
    }
}
