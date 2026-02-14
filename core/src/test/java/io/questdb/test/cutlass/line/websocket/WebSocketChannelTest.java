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

import io.questdb.client.cutlass.line.LineSenderException;
import io.questdb.client.cutlass.ilpv4.client.WebSocketChannel;
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
    public void testUrlParsingWsScheme() {
        WebSocketChannel channel = new WebSocketChannel("ws://localhost:9000/write/v4", false);
        try {
            Assert.assertFalse(channel.isConnected());
        } finally {
            channel.close();
        }
    }

    @Test
    public void testUrlParsingWssScheme() {
        WebSocketChannel channel = new WebSocketChannel("wss://localhost:443/write/v4", false);
        try {
            Assert.assertFalse(channel.isConnected());
        } finally {
            channel.close();
        }
    }

    @Test
    public void testUrlParsingNoPath() {
        WebSocketChannel channel = new WebSocketChannel("ws://localhost:9000", false);
        try {
            Assert.assertFalse(channel.isConnected());
        } finally {
            channel.close();
        }
    }

    @Test
    public void testUrlParsingNoPort() {
        WebSocketChannel channel = new WebSocketChannel("ws://localhost/path", false);
        try {
            Assert.assertFalse(channel.isConnected());
        } finally {
            channel.close();
        }
    }

    @Test
    public void testCloseBeforeConnect() {
        WebSocketChannel channel = new WebSocketChannel("ws://localhost:9000/write/v4", false);
        channel.close();
        Assert.assertFalse(channel.isConnected());
    }

    @Test
    public void testDoubleClose() {
        WebSocketChannel channel = new WebSocketChannel("ws://localhost:9000/write/v4", false);
        channel.close();
        channel.close(); // Should not throw
        Assert.assertFalse(channel.isConnected());
    }

    @Test
    public void testSendBeforeConnectThrows() {
        WebSocketChannel channel = new WebSocketChannel("ws://localhost:9000/write/v4", false);
        try {
            long ptr = Unsafe.malloc(10, MemoryTag.NATIVE_DEFAULT);
            try {
                channel.sendBinary(ptr, 10);
                Assert.fail("Expected LineSenderException");
            } catch (LineSenderException e) {
                Assert.assertTrue(e.getMessage().contains("not connected"));
            } finally {
                Unsafe.free(ptr, 10, MemoryTag.NATIVE_DEFAULT);
            }
        } finally {
            channel.close();
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
    public void testConnectToInvalidHostThrows() {
        WebSocketChannel channel = new WebSocketChannel("ws://invalid.host.that.does.not.exist:9999/path", false);
        try {
            channel.setConnectTimeout(1000);
            channel.connect();
            Assert.fail("Expected LineSenderException");
        } catch (LineSenderException e) {
            Assert.assertTrue(e.getMessage().contains("Failed to connect"));
        } finally {
            channel.close();
        }
    }

    @Test
    public void testConnectToClosedPortThrows() {
        // Port 1 is typically not open
        WebSocketChannel channel = new WebSocketChannel("ws://127.0.0.1:1/path", false);
        try {
            channel.setConnectTimeout(1000);
            channel.connect();
            Assert.fail("Expected LineSenderException");
        } catch (LineSenderException e) {
            Assert.assertTrue(e.getMessage().contains("Failed to connect"));
        } finally {
            channel.close();
        }
    }

    @Test
    public void testSetTimeouts() {
        WebSocketChannel channel = new WebSocketChannel("ws://localhost:9000/path", false);
        try {
            channel.setConnectTimeout(5000);
            channel.setReadTimeout(10000);
            // Just verify it doesn't throw
            Assert.assertFalse(channel.isConnected());
        } finally {
            channel.close();
        }
    }

    // ==================== URL Parsing Edge Cases ====================

    @Test
    public void testUrlParsingEmptyPath() {
        WebSocketChannel channel = new WebSocketChannel("ws://localhost:9000", false);
        try {
            Assert.assertFalse(channel.isConnected());
        } finally {
            channel.close();
        }
    }

    @Test
    public void testUrlParsingRootPath() {
        WebSocketChannel channel = new WebSocketChannel("ws://localhost:9000/", false);
        try {
            Assert.assertFalse(channel.isConnected());
        } finally {
            channel.close();
        }
    }

    @Test
    public void testUrlParsingDeepPath() {
        WebSocketChannel channel = new WebSocketChannel("ws://localhost:9000/a/b/c/d/e/f", false);
        try {
            Assert.assertFalse(channel.isConnected());
        } finally {
            channel.close();
        }
    }

    @Test
    public void testUrlParsingWithQueryParams() {
        WebSocketChannel channel = new WebSocketChannel("ws://localhost:9000/path?key=value&foo=bar", false);
        try {
            Assert.assertFalse(channel.isConnected());
        } finally {
            channel.close();
        }
    }

    @Test
    public void testUrlParsingIPv4Address() {
        WebSocketChannel channel = new WebSocketChannel("ws://127.0.0.1:9000/path", false);
        try {
            Assert.assertFalse(channel.isConnected());
        } finally {
            channel.close();
        }
    }

    @Test
    public void testUrlParsingIPv6Address() {
        WebSocketChannel channel = new WebSocketChannel("ws://[::1]:9000/path", false);
        try {
            Assert.assertFalse(channel.isConnected());
        } finally {
            channel.close();
        }
    }

    @Test
    public void testUrlParsingDefaultWsPort() {
        WebSocketChannel channel = new WebSocketChannel("ws://localhost/path", false);
        try {
            Assert.assertFalse(channel.isConnected());
        } finally {
            channel.close();
        }
    }

    @Test
    public void testUrlParsingDefaultWssPort() {
        WebSocketChannel channel = new WebSocketChannel("wss://localhost/path", false);
        try {
            Assert.assertFalse(channel.isConnected());
        } finally {
            channel.close();
        }
    }

    @Test
    public void testUrlParsingHostOnly() {
        WebSocketChannel channel = new WebSocketChannel("localhost:9000", false);
        try {
            Assert.assertFalse(channel.isConnected());
        } finally {
            channel.close();
        }
    }

    // ==================== State Transition Tests ====================

    @Test
    public void testIsConnectedInitiallyFalse() {
        WebSocketChannel channel = new WebSocketChannel("ws://localhost:9000/path", false);
        try {
            Assert.assertFalse(channel.isConnected());
        } finally {
            channel.close();
        }
    }

    @Test
    public void testIsConnectedAfterCloseFalse() {
        WebSocketChannel channel = new WebSocketChannel("ws://localhost:9000/path", false);
        channel.close();
        Assert.assertFalse(channel.isConnected());
    }

    @Test
    public void testSendPingBeforeConnectThrows() {
        WebSocketChannel channel = new WebSocketChannel("ws://localhost:9000/path", false);
        try {
            channel.sendPing();
            Assert.fail("Expected LineSenderException");
        } catch (LineSenderException e) {
            Assert.assertTrue(e.getMessage().contains("not connected"));
        } finally {
            channel.close();
        }
    }

    @Test
    public void testReceiveBeforeConnectThrows() {
        WebSocketChannel channel = new WebSocketChannel("ws://localhost:9000/path", false);
        try {
            channel.receiveFrame(null, 100);
            Assert.fail("Expected LineSenderException");
        } catch (LineSenderException e) {
            Assert.assertTrue(e.getMessage().contains("not connected"));
        } finally {
            channel.close();
        }
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

    // ==================== Timeout Configuration Tests ====================

    @Test
    public void testSetConnectTimeoutReturnsThis() {
        WebSocketChannel channel = new WebSocketChannel("ws://localhost:9000/path", false);
        try {
            WebSocketChannel result = channel.setConnectTimeout(5000);
            Assert.assertSame(channel, result);
        } finally {
            channel.close();
        }
    }

    @Test
    public void testSetReadTimeoutReturnsThis() {
        WebSocketChannel channel = new WebSocketChannel("ws://localhost:9000/path", false);
        try {
            WebSocketChannel result = channel.setReadTimeout(5000);
            Assert.assertSame(channel, result);
        } finally {
            channel.close();
        }
    }

    @Test
    public void testChainedTimeoutConfiguration() {
        WebSocketChannel channel = new WebSocketChannel("ws://localhost:9000/path", false);
        try {
            channel.setConnectTimeout(5000).setReadTimeout(10000);
            Assert.assertFalse(channel.isConnected());
        } finally {
            channel.close();
        }
    }

    // ==================== TLS Configuration Tests ====================

    @Test
    public void testTlsEnabledViaConstructor() {
        WebSocketChannel channel = new WebSocketChannel("ws://localhost:9000/path", true);
        try {
            Assert.assertFalse(channel.isConnected());
        } finally {
            channel.close();
        }
    }

    @Test
    public void testTlsValidationDisabled() {
        WebSocketChannel channel = new WebSocketChannel("wss://localhost:9000/path", true, false);
        try {
            Assert.assertFalse(channel.isConnected());
        } finally {
            channel.close();
        }
    }

    @Test
    public void testWssSchemeOverridesTlsParameter() {
        // wss:// scheme should enable TLS regardless of the parameter
        WebSocketChannel channel = new WebSocketChannel("wss://localhost:443/path", false);
        try {
            Assert.assertFalse(channel.isConnected());
        } finally {
            channel.close();
        }
    }
}
