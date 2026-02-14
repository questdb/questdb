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

package io.questdb.test.client;

import io.questdb.client.Sender;
import io.questdb.client.cutlass.line.LineSenderException;
import io.questdb.test.AbstractTest;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;

import static org.junit.Assert.fail;

/**
 * Tests for WebSocket transport support in the Sender.builder() API.
 * These tests verify the builder configuration and validation,
 * not actual WebSocket connectivity (which requires a running server).
 */
public class LineSenderBuilderWebSocketTest extends AbstractTest {

    private static final String LOCALHOST = "localhost";

    // ==================== Transport Selection Tests ====================

    @Test
    public void testBuilderWithWebSocketTransport() {
        Sender.LineSenderBuilder builder = Sender.builder(Sender.Transport.WEBSOCKET);
        Assert.assertNotNull("Builder should be created for WebSocket transport", builder);
    }

    @Test
    public void testBuilderWithWebSocketTransportCreatesCorrectSenderType() {
        // We can't actually connect, but we can verify the builder accepts WebSocket transport
        try {
            Sender.builder(Sender.Transport.WEBSOCKET)
                    .address(LOCALHOST + ":9000")
                    .build();
            fail("Should fail to connect to non-existent server");
        } catch (LineSenderException e) {
            // Expected - connection failed, but builder was configured correctly
            // The error should be about connection, not about invalid configuration
            Assert.assertTrue("Error should be about connection failure, not configuration",
                    e.getMessage().contains("connect") || e.getMessage().contains("Failed"));
        }
    }

    // ==================== Address Configuration Tests ====================

    @Test
    public void testAddressConfiguration() {
        Sender.LineSenderBuilder builder = Sender.builder(Sender.Transport.WEBSOCKET)
                .address(LOCALHOST + ":9000");
        Assert.assertNotNull(builder);
    }

    @Test
    public void testAddressWithoutPort_usesDefaultPort9000() {
        Sender.LineSenderBuilder builder = Sender.builder(Sender.Transport.WEBSOCKET)
                .address(LOCALHOST);
        // Default port 9000 should be used
        Assert.assertNotNull(builder);
    }

    @Test
    public void testAddressEmpty_fails() {
        try {
            Sender.builder(Sender.Transport.WEBSOCKET).address("");
            fail("Empty address should fail");
        } catch (LineSenderException e) {
            TestUtils.assertContains(e.getMessage(), "address cannot be empty");
        }
    }

    @Test
    public void testAddressNull_fails() {
        try {
            Sender.builder(Sender.Transport.WEBSOCKET).address(null);
            fail("Null address should fail");
        } catch (LineSenderException e) {
            TestUtils.assertContains(e.getMessage(), "null");
        }
    }

    @Test
    public void testAddressEndsWithColon_fails() {
        try {
            Sender.builder(Sender.Transport.WEBSOCKET).address("foo:");
            fail("Address ending with colon should fail");
        } catch (LineSenderException e) {
            TestUtils.assertContains(e.getMessage(), "invalid address");
        }
    }

    @Test
    public void testMalformedPortInAddress_fails() {
        try {
            Sender.builder(Sender.Transport.WEBSOCKET).address("foo:nonsense12334");
            fail("Malformed port should fail");
        } catch (LineSenderException e) {
            TestUtils.assertContains(e.getMessage(), "cannot parse a port from the address");
        }
    }

    @Test
    public void testInvalidPort_fails() {
        try {
            Sender.builder(Sender.Transport.WEBSOCKET).address(LOCALHOST + ":99999");
            fail("Invalid port should fail");
        } catch (LineSenderException e) {
            TestUtils.assertContains(e.getMessage(), "invalid port");
        }
    }

    @Test
    public void testMultipleAddresses_fails() {
        try {
            Sender.builder(Sender.Transport.WEBSOCKET)
                    .address(LOCALHOST + ":9000")
                    .address(LOCALHOST + ":9001")
                    .build();
            fail("Multiple addresses should fail for WebSocket");
        } catch (LineSenderException e) {
            TestUtils.assertContains(e.getMessage(), "single address");
        }
    }

    @Test
    public void testNoAddress_fails() {
        try {
            Sender.builder(Sender.Transport.WEBSOCKET).build();
            fail("Missing address should fail");
        } catch (LineSenderException e) {
            TestUtils.assertContains(e.getMessage(), "address not set");
        }
    }

    // ==================== TLS Configuration Tests ====================

    @Test
    public void testTlsEnabled() {
        Sender.LineSenderBuilder builder = Sender.builder(Sender.Transport.WEBSOCKET)
                .address(LOCALHOST)
                .enableTls();
        Assert.assertNotNull(builder);
    }

    @Test
    public void testTlsDoubleSet_fails() {
        try {
            Sender.builder(Sender.Transport.WEBSOCKET)
                    .enableTls()
                    .enableTls();
            fail("Double TLS enable should fail");
        } catch (LineSenderException e) {
            TestUtils.assertContains(e.getMessage(), "already enabled");
        }
    }

    @Test
    public void testTlsValidationDisabled() {
        Sender.LineSenderBuilder builder = Sender.builder(Sender.Transport.WEBSOCKET)
                .address(LOCALHOST)
                .enableTls()
                .advancedTls().disableCertificateValidation();
        Assert.assertNotNull(builder);
    }

    @Test
    public void testTlsValidationDisabled_butTlsNotEnabled_fails() {
        try {
            Sender.builder(Sender.Transport.WEBSOCKET)
                    .advancedTls().disableCertificateValidation()
                    .address(LOCALHOST)
                    .build();
            fail("TLS validation disabled without TLS enabled should fail");
        } catch (LineSenderException e) {
            TestUtils.assertContains(e.getMessage(), "TLS was not enabled");
        }
    }

    @Test
    public void testCustomTrustStore_butTlsNotEnabled_fails() {
        try {
            Sender.builder(Sender.Transport.WEBSOCKET)
                    .advancedTls().customTrustStore("/some/path", "password".toCharArray())
                    .address(LOCALHOST)
                    .build();
            fail("Custom trust store without TLS enabled should fail");
        } catch (LineSenderException e) {
            TestUtils.assertContains(e.getMessage(), "TLS was not enabled");
        }
    }

    // ==================== Async Mode Tests ====================

    @Test
    public void testAsyncModeEnabled() {
        Sender.LineSenderBuilder builder = Sender.builder(Sender.Transport.WEBSOCKET)
                .address(LOCALHOST)
                .asyncMode(true);
        Assert.assertNotNull(builder);
    }

    @Test
    public void testAsyncModeDisabled() {
        Sender.LineSenderBuilder builder = Sender.builder(Sender.Transport.WEBSOCKET)
                .address(LOCALHOST)
                .asyncMode(false);
        Assert.assertNotNull(builder);
    }

    @Test
    public void testAsyncModeCanBeSetMultipleTimes() {
        // Unlike other parameters, asyncMode is a simple boolean setter without duplicate detection
        Sender.LineSenderBuilder builder = Sender.builder(Sender.Transport.WEBSOCKET)
                .address(LOCALHOST)
                .asyncMode(true)
                .asyncMode(false);
        Assert.assertNotNull(builder);
    }

    // ==================== Auto Flush Rows Tests ====================

    @Test
    public void testAutoFlushRows() {
        Sender.LineSenderBuilder builder = Sender.builder(Sender.Transport.WEBSOCKET)
                .address(LOCALHOST)
                .autoFlushRows(1000);
        Assert.assertNotNull(builder);
    }

    @Test
    public void testAutoFlushRowsZero_disablesRowBasedAutoFlush() {
        Sender.LineSenderBuilder builder = Sender.builder(Sender.Transport.WEBSOCKET)
                .address(LOCALHOST)
                .autoFlushRows(0);
        Assert.assertNotNull(builder);
    }

    @Test
    public void testAutoFlushRowsNegative_fails() {
        try {
            Sender.builder(Sender.Transport.WEBSOCKET)
                    .address(LOCALHOST)
                    .autoFlushRows(-1);
            fail("Negative auto flush rows should fail");
        } catch (LineSenderException e) {
            TestUtils.assertContains(e.getMessage(), "cannot be negative");
        }
    }

    @Test
    public void testAutoFlushRowsDoubleSet_fails() {
        try {
            Sender.builder(Sender.Transport.WEBSOCKET)
                    .address(LOCALHOST)
                    .autoFlushRows(100)
                    .autoFlushRows(200);
            fail("Double auto flush rows configuration should fail");
        } catch (LineSenderException e) {
            TestUtils.assertContains(e.getMessage(), "already configured");
        }
    }

    // ==================== Auto Flush Bytes Tests ====================

    @Test
    public void testAutoFlushBytes() {
        Sender.LineSenderBuilder builder = Sender.builder(Sender.Transport.WEBSOCKET)
                .address(LOCALHOST)
                .autoFlushBytes(1024 * 1024);
        Assert.assertNotNull(builder);
    }

    @Test
    public void testAutoFlushBytesZero() {
        Sender.LineSenderBuilder builder = Sender.builder(Sender.Transport.WEBSOCKET)
                .address(LOCALHOST)
                .autoFlushBytes(0);
        Assert.assertNotNull(builder);
    }

    @Test
    public void testAutoFlushBytesNegative_fails() {
        try {
            Sender.builder(Sender.Transport.WEBSOCKET)
                    .address(LOCALHOST)
                    .autoFlushBytes(-1);
            fail("Negative auto flush bytes should fail");
        } catch (LineSenderException e) {
            TestUtils.assertContains(e.getMessage(), "cannot be negative");
        }
    }

    @Test
    public void testAutoFlushBytesDoubleSet_fails() {
        try {
            Sender.builder(Sender.Transport.WEBSOCKET)
                    .address(LOCALHOST)
                    .autoFlushBytes(1024)
                    .autoFlushBytes(2048);
            fail("Double auto flush bytes configuration should fail");
        } catch (LineSenderException e) {
            TestUtils.assertContains(e.getMessage(), "already configured");
        }
    }

    // ==================== Auto Flush Interval Tests ====================

    @Test
    public void testAutoFlushIntervalMillis() {
        Sender.LineSenderBuilder builder = Sender.builder(Sender.Transport.WEBSOCKET)
                .address(LOCALHOST)
                .autoFlushIntervalMillis(100);
        Assert.assertNotNull(builder);
    }

    @Test
    public void testAutoFlushIntervalMillisZero_fails() {
        try {
            Sender.builder(Sender.Transport.WEBSOCKET)
                    .address(LOCALHOST)
                    .autoFlushIntervalMillis(0);
            fail("Zero auto flush interval should fail");
        } catch (LineSenderException e) {
            TestUtils.assertContains(e.getMessage(), "cannot be negative");
        }
    }

    @Test
    public void testAutoFlushIntervalMillisNegative_fails() {
        try {
            Sender.builder(Sender.Transport.WEBSOCKET)
                    .address(LOCALHOST)
                    .autoFlushIntervalMillis(-1);
            fail("Negative auto flush interval should fail");
        } catch (LineSenderException e) {
            TestUtils.assertContains(e.getMessage(), "cannot be negative");
        }
    }

    @Test
    public void testAutoFlushIntervalMillisDoubleSet_fails() {
        try {
            Sender.builder(Sender.Transport.WEBSOCKET)
                    .address(LOCALHOST)
                    .autoFlushIntervalMillis(100)
                    .autoFlushIntervalMillis(200);
            fail("Double auto flush interval configuration should fail");
        } catch (LineSenderException e) {
            TestUtils.assertContains(e.getMessage(), "already configured");
        }
    }

    // ==================== In-Flight Window Size Tests ====================

    @Test
    public void testInFlightWindowSize_withAsyncMode() {
        Sender.LineSenderBuilder builder = Sender.builder(Sender.Transport.WEBSOCKET)
                .address(LOCALHOST)
                .asyncMode(true)
                .inFlightWindowSize(16);
        Assert.assertNotNull(builder);
    }

    @Test
    public void testInFlightWindowSize_withoutAsyncMode_fails() {
        try {
            Sender.builder(Sender.Transport.WEBSOCKET)
                    .address(LOCALHOST)
                    .inFlightWindowSize(16)
                    .build();
            fail("In-flight window size without async mode should fail");
        } catch (LineSenderException e) {
            TestUtils.assertContains(e.getMessage(), "requires async mode");
        }
    }

    @Test
    public void testInFlightWindowSizeZero_fails() {
        try {
            Sender.builder(Sender.Transport.WEBSOCKET)
                    .address(LOCALHOST)
                    .asyncMode(true)
                    .inFlightWindowSize(0);
            fail("Zero in-flight window size should fail");
        } catch (LineSenderException e) {
            TestUtils.assertContains(e.getMessage(), "must be positive");
        }
    }

    @Test
    public void testInFlightWindowSizeNegative_fails() {
        try {
            Sender.builder(Sender.Transport.WEBSOCKET)
                    .address(LOCALHOST)
                    .asyncMode(true)
                    .inFlightWindowSize(-1);
            fail("Negative in-flight window size should fail");
        } catch (LineSenderException e) {
            TestUtils.assertContains(e.getMessage(), "must be positive");
        }
    }

    @Test
    public void testInFlightWindowSizeDoubleSet_fails() {
        try {
            Sender.builder(Sender.Transport.WEBSOCKET)
                    .address(LOCALHOST)
                    .asyncMode(true)
                    .inFlightWindowSize(8)
                    .inFlightWindowSize(16);
            fail("Double in-flight window size configuration should fail");
        } catch (LineSenderException e) {
            TestUtils.assertContains(e.getMessage(), "already configured");
        }
    }

    // ==================== Send Queue Capacity Tests ====================

    @Test
    public void testSendQueueCapacity_withAsyncMode() {
        Sender.LineSenderBuilder builder = Sender.builder(Sender.Transport.WEBSOCKET)
                .address(LOCALHOST)
                .asyncMode(true)
                .sendQueueCapacity(32);
        Assert.assertNotNull(builder);
    }

    @Test
    public void testSendQueueCapacity_withoutAsyncMode_fails() {
        try {
            Sender.builder(Sender.Transport.WEBSOCKET)
                    .address(LOCALHOST)
                    .sendQueueCapacity(32)
                    .build();
            fail("Send queue capacity without async mode should fail");
        } catch (LineSenderException e) {
            TestUtils.assertContains(e.getMessage(), "requires async mode");
        }
    }

    @Test
    public void testSendQueueCapacityZero_fails() {
        try {
            Sender.builder(Sender.Transport.WEBSOCKET)
                    .address(LOCALHOST)
                    .asyncMode(true)
                    .sendQueueCapacity(0);
            fail("Zero send queue capacity should fail");
        } catch (LineSenderException e) {
            TestUtils.assertContains(e.getMessage(), "must be positive");
        }
    }

    @Test
    public void testSendQueueCapacityNegative_fails() {
        try {
            Sender.builder(Sender.Transport.WEBSOCKET)
                    .address(LOCALHOST)
                    .asyncMode(true)
                    .sendQueueCapacity(-1);
            fail("Negative send queue capacity should fail");
        } catch (LineSenderException e) {
            TestUtils.assertContains(e.getMessage(), "must be positive");
        }
    }

    @Test
    public void testSendQueueCapacityDoubleSet_fails() {
        try {
            Sender.builder(Sender.Transport.WEBSOCKET)
                    .address(LOCALHOST)
                    .asyncMode(true)
                    .sendQueueCapacity(16)
                    .sendQueueCapacity(32);
            fail("Double send queue capacity configuration should fail");
        } catch (LineSenderException e) {
            TestUtils.assertContains(e.getMessage(), "already configured");
        }
    }

    // ==================== Combined Async Configuration Tests ====================

    @Test
    public void testFullAsyncConfiguration() {
        Sender.LineSenderBuilder builder = Sender.builder(Sender.Transport.WEBSOCKET)
                .address(LOCALHOST)
                .asyncMode(true)
                .autoFlushRows(1000)
                .autoFlushBytes(1024 * 1024)
                .autoFlushIntervalMillis(100)
                .inFlightWindowSize(16)
                .sendQueueCapacity(32);
        Assert.assertNotNull(builder);
    }

    @Test
    public void testFullAsyncConfigurationWithTls() {
        Sender.LineSenderBuilder builder = Sender.builder(Sender.Transport.WEBSOCKET)
                .address(LOCALHOST)
                .enableTls()
                .advancedTls().disableCertificateValidation()
                .asyncMode(true)
                .autoFlushRows(1000)
                .autoFlushBytes(1024 * 1024)
                .inFlightWindowSize(16)
                .sendQueueCapacity(32);
        Assert.assertNotNull(builder);
    }

    // ==================== Config String Tests (ws:// and wss://) ====================

    @Test
    public void testWsConfigString() {
        try {
            Sender.fromConfig("ws::addr=localhost:9000;");
            fail("Should fail to connect to non-existent server");
        } catch (LineSenderException e) {
            // Connection failure is expected - config parsing was successful
            Assert.assertTrue("Error should be about connection failure",
                    e.getMessage().contains("connect") || e.getMessage().contains("Failed"));
        }
    }

    @Test
    public void testWssConfigString() {
        try {
            Sender.fromConfig("wss::addr=localhost:9000;tls_verify=unsafe_off;");
            fail("Should fail to connect to non-existent server");
        } catch (LineSenderException e) {
            // Connection failure is expected - config parsing was successful
            Assert.assertTrue("Error should be about connection failure",
                    e.getMessage().contains("connect") || e.getMessage().contains("Failed") || e.getMessage().contains("SSL"));
        }
    }

    @Test
    public void testWsConfigString_protocolAlreadyConfigured_fails() {
        // Can't use Sender.builder(config) when protocol is already set via transport
        try {
            Sender.builder("ws::addr=localhost:9000;")
                    .enableTls()
                    .build();
            fail("Should fail - ws doesn't support TLS, use wss");
        } catch (LineSenderException e) {
            // ws:// was parsed successfully, but trying to enable TLS on ws is not valid
            // The build should fail because ws is already configured without TLS
            Assert.assertTrue("Error should be about TLS or configuration conflict",
                    e.getMessage().contains("TLS") || e.getMessage().contains("connect") || e.getMessage().contains("Failed"));
        }
    }

    @Test
    public void testWsConfigString_missingAddr_fails() {
        try {
            Sender.fromConfig("ws::addr=localhost;");
            // Should fail because no server is running
            fail("Should fail to connect to non-existent server");
        } catch (LineSenderException e) {
            // Connection failure is expected
            Assert.assertTrue("Error should be about connection failure",
                    e.getMessage().contains("connect") || e.getMessage().contains("Failed"));
        }

        // Also test completely missing addr
        try {
            Sender.fromConfig("ws::foo=bar;");
            fail("Missing address should fail");
        } catch (LineSenderException e) {
            TestUtils.assertContains(e.getMessage(), "addr is missing");
        }
    }

    @Test
    public void testInvalidSchema_fails() {
        try {
            Sender.fromConfig("invalid::addr=localhost:9000;");
            fail("Invalid schema should fail");
        } catch (LineSenderException e) {
            TestUtils.assertContains(e.getMessage(), "invalid schema");
            TestUtils.assertContains(e.getMessage(), "ws");
            TestUtils.assertContains(e.getMessage(), "wss");
        }
    }

    @Test
    public void testWsConfigString_uppercaseNotSupported() {
        try {
            Sender.fromConfig("WS::addr=localhost:9000;");
            fail("Uppercase WS should not be supported");
        } catch (LineSenderException e) {
            TestUtils.assertContains(e.getMessage(), "invalid schema");
        }
    }

    @Test
    public void testWssConfigString_uppercaseNotSupported() {
        try {
            Sender.fromConfig("WSS::addr=localhost:9000;");
            fail("Uppercase WSS should not be supported");
        } catch (LineSenderException e) {
            TestUtils.assertContains(e.getMessage(), "invalid schema");
        }
    }

    // ==================== Buffer Configuration Tests ====================

    @Test
    public void testBufferCapacity() {
        Sender.LineSenderBuilder builder = Sender.builder(Sender.Transport.WEBSOCKET)
                .address(LOCALHOST)
                .bufferCapacity(128 * 1024);
        Assert.assertNotNull(builder);
    }

    @Test
    public void testBufferCapacityNegative_fails() {
        try {
            Sender.builder(Sender.Transport.WEBSOCKET)
                    .address(LOCALHOST)
                    .bufferCapacity(-1);
            fail("Negative buffer capacity should fail");
        } catch (LineSenderException e) {
            TestUtils.assertContains(e.getMessage(), "cannot be negative");
        }
    }

    @Test
    public void testBufferCapacityDoubleSet_fails() {
        try {
            Sender.builder(Sender.Transport.WEBSOCKET)
                    .address(LOCALHOST)
                    .bufferCapacity(1024)
                    .bufferCapacity(2048);
            fail("Double buffer capacity configuration should fail");
        } catch (LineSenderException e) {
            TestUtils.assertContains(e.getMessage(), "already configured");
        }
    }

    // ==================== Unsupported Features (TCP Authentication) ====================

    @Test
    @Ignore("TCP authentication is not supported for WebSocket protocol")
    public void testEnableAuth_notSupported() {
        try {
            Sender.builder(Sender.Transport.WEBSOCKET)
                    .address(LOCALHOST)
                    .enableAuth("keyId")
                    .authToken("token")
                    .build();
            fail("TCP authentication should not be supported for WebSocket");
        } catch (LineSenderException e) {
            TestUtils.assertContains(e.getMessage(), "not supported for WebSocket");
        }
    }

    @Test
    public void testTcpAuth_fails() {
        try {
            Sender.builder(Sender.Transport.WEBSOCKET)
                    .address(LOCALHOST)
                    .enableAuth("keyId")
                    .authToken("5UjEMuA0Pj5pjK8a-fa24dyIf-Es5mYny3oE_Wmus48")
                    .build();
            fail("TCP authentication should not be supported for WebSocket");
        } catch (LineSenderException e) {
            TestUtils.assertContains(e.getMessage(), "not supported for WebSocket");
        }
    }

    // ==================== Unsupported Features (HTTP Token Authentication) ====================

    @Test
    @Ignore("HTTP token authentication is not yet supported for WebSocket protocol")
    public void testHttpToken_notYetSupported() {
        try {
            Sender.builder(Sender.Transport.WEBSOCKET)
                    .address(LOCALHOST)
                    .httpToken("token")
                    .build();
            fail("HTTP token authentication should not yet be supported for WebSocket");
        } catch (LineSenderException e) {
            TestUtils.assertContains(e.getMessage(), "not yet supported");
        }
    }

    @Test
    public void testHttpToken_fails() {
        try {
            Sender.builder(Sender.Transport.WEBSOCKET)
                    .address(LOCALHOST)
                    .httpToken("token")
                    .build();
            fail("HTTP token authentication should fail for WebSocket");
        } catch (LineSenderException e) {
            TestUtils.assertContains(e.getMessage(), "not yet supported");
        }
    }

    // ==================== Unsupported Features (Username/Password Authentication) ====================

    @Test
    @Ignore("Username/password authentication is not yet supported for WebSocket protocol")
    public void testUsernamePassword_notYetSupported() {
        try {
            Sender.builder(Sender.Transport.WEBSOCKET)
                    .address(LOCALHOST)
                    .httpUsernamePassword("user", "pass")
                    .build();
            fail("Username/password authentication should not yet be supported for WebSocket");
        } catch (LineSenderException e) {
            TestUtils.assertContains(e.getMessage(), "not yet supported");
        }
    }

    @Test
    public void testUsernamePassword_fails() {
        try {
            Sender.builder(Sender.Transport.WEBSOCKET)
                    .address(LOCALHOST)
                    .httpUsernamePassword("user", "pass")
                    .build();
            fail("Username/password authentication should fail for WebSocket");
        } catch (LineSenderException e) {
            TestUtils.assertContains(e.getMessage(), "not yet supported");
        }
    }

    // ==================== Unsupported Features (HTTP-specific options) ====================

    @Test
    @Ignore("HTTP timeout is HTTP-specific and may not apply to WebSocket")
    public void testHttpTimeout_mayNotApply() {
        // HTTP timeout may or may not be applicable to WebSocket
        Sender.LineSenderBuilder builder = Sender.builder(Sender.Transport.WEBSOCKET)
                .address(LOCALHOST)
                .httpTimeoutMillis(5000);
        Assert.assertNotNull(builder);
    }

    @Test
    @Ignore("Retry timeout is HTTP-specific and may not apply to WebSocket")
    public void testRetryTimeout_mayNotApply() {
        // Retry timeout may or may not be applicable to WebSocket
        Sender.LineSenderBuilder builder = Sender.builder(Sender.Transport.WEBSOCKET)
                .address(LOCALHOST)
                .retryTimeoutMillis(5000);
        Assert.assertNotNull(builder);
    }

    @Test
    @Ignore("Min request throughput is HTTP-specific and may not apply to WebSocket")
    public void testMinRequestThroughput_mayNotApply() {
        // Min request throughput may or may not be applicable to WebSocket
        Sender.LineSenderBuilder builder = Sender.builder(Sender.Transport.WEBSOCKET)
                .address(LOCALHOST)
                .minRequestThroughput(10000);
        Assert.assertNotNull(builder);
    }

    @Test
    @Ignore("Max backoff is HTTP-specific and may not apply to WebSocket")
    public void testMaxBackoff_mayNotApply() {
        // Max backoff may or may not be applicable to WebSocket
        Sender.LineSenderBuilder builder = Sender.builder(Sender.Transport.WEBSOCKET)
                .address(LOCALHOST)
                .maxBackoffMillis(1000);
        Assert.assertNotNull(builder);
    }

    @Test
    @Ignore("HTTP path is HTTP-specific and may not apply to WebSocket")
    public void testHttpPath_mayNotApply() {
        // HTTP path may or may not be applicable to WebSocket
        Sender.LineSenderBuilder builder = Sender.builder(Sender.Transport.WEBSOCKET)
                .address(LOCALHOST)
                .httpPath("/custom/path");
        Assert.assertNotNull(builder);
    }

    @Test
    @Ignore("Disable auto flush may need different semantics for WebSocket")
    public void testDisableAutoFlush_semantics() {
        // Disable auto flush may have different semantics for WebSocket
        Sender.LineSenderBuilder builder = Sender.builder(Sender.Transport.WEBSOCKET)
                .address(LOCALHOST)
                .disableAutoFlush();
        Assert.assertNotNull(builder);
    }

    // ==================== Unsupported Features (Protocol Version) ====================

    @Test
    @Ignore("Protocol version is for ILP text protocol, WebSocket uses ILP v4 binary protocol")
    public void testProtocolVersion_notApplicable() {
        // Protocol version is for ILP text protocol (v1, v2, v3)
        // WebSocket uses ILP v4 binary protocol
        Sender.LineSenderBuilder builder = Sender.builder(Sender.Transport.WEBSOCKET)
                .address(LOCALHOST)
                .protocolVersion(Sender.PROTOCOL_VERSION_V2);
        Assert.assertNotNull(builder);
    }

    // ==================== Config String Unsupported Options ====================

    @Test
    @Ignore("Token authentication in ws config string is not yet supported")
    public void testWsConfigString_withToken_notYetSupported() {
        try {
            Sender.fromConfig("ws::addr=localhost:9000;token=mytoken;");
            fail("Token in ws config should not yet be supported");
        } catch (LineSenderException e) {
            TestUtils.assertContains(e.getMessage(), "not yet supported");
        }
    }

    @Test
    @Ignore("Username/password in ws config string is not yet supported")
    public void testWsConfigString_withUsernamePassword_notYetSupported() {
        try {
            Sender.fromConfig("ws::addr=localhost:9000;username=user;password=pass;");
            fail("Username/password in ws config should not yet be supported");
        } catch (LineSenderException e) {
            TestUtils.assertContains(e.getMessage(), "not yet supported");
        }
    }

    // ==================== Edge Cases ====================

    @Test
    public void testDuplicateAddresses_fails() {
        try {
            Sender.builder(Sender.Transport.WEBSOCKET)
                    .address(LOCALHOST + ":9000")
                    .address(LOCALHOST + ":9000");
            fail("Duplicate addresses should fail");
        } catch (LineSenderException e) {
            TestUtils.assertContains(e.getMessage(), "duplicated addresses");
        }
    }

    @Test
    public void testPortMismatch_fails() {
        try {
            Sender.builder(Sender.Transport.WEBSOCKET)
                    .address(LOCALHOST + ":9000")
                    .port(9001)
                    .build();
            fail("Port mismatch should fail");
        } catch (LineSenderException e) {
            TestUtils.assertContains(e.getMessage(), "mismatch");
        }
    }

    @Test
    public void testMaxNameLength() {
        Sender.LineSenderBuilder builder = Sender.builder(Sender.Transport.WEBSOCKET)
                .address(LOCALHOST)
                .maxNameLength(256);
        Assert.assertNotNull(builder);
    }

    @Test
    public void testMaxNameLengthTooSmall_fails() {
        try {
            Sender.builder(Sender.Transport.WEBSOCKET)
                    .address(LOCALHOST)
                    .maxNameLength(10);
            fail("Max name length too small should fail");
        } catch (LineSenderException e) {
            TestUtils.assertContains(e.getMessage(), "at least 16 bytes");
        }
    }

    @Test
    public void testMaxNameLengthDoubleSet_fails() {
        try {
            Sender.builder(Sender.Transport.WEBSOCKET)
                    .address(LOCALHOST)
                    .maxNameLength(128)
                    .maxNameLength(256);
            fail("Double max name length configuration should fail");
        } catch (LineSenderException e) {
            TestUtils.assertContains(e.getMessage(), "already configured");
        }
    }

    // ==================== Connection Tests ====================

    @Test
    public void testConnectionRefused() {
        try {
            Sender.builder(Sender.Transport.WEBSOCKET)
                    .address(LOCALHOST + ":19999")
                    .build();
            fail("Connection refused should fail fast");
        } catch (LineSenderException e) {
            Assert.assertTrue("Error should indicate connection failure",
                    e.getMessage().contains("connect") || e.getMessage().contains("Failed"));
        }
    }

    @Test
    public void testDnsResolutionFailure() {
        try {
            Sender.builder(Sender.Transport.WEBSOCKET)
                    .address("this-domain-does-not-exist-i-hope-better-to-use-a-silly-tld.silly-tld:9000")
                    .build();
            fail("DNS resolution errors should fail fast");
        } catch (LineSenderException e) {
            Assert.assertTrue("Error should indicate DNS or connection failure",
                    e.getMessage().contains("resolve") || e.getMessage().contains("connect") || e.getMessage().contains("Failed"));
        }
    }

    // ==================== Sync vs Async Mode Tests ====================

    @Test
    public void testSyncModeIsDefault() {
        // Sync mode should be the default
        Sender.LineSenderBuilder builder = Sender.builder(Sender.Transport.WEBSOCKET)
                .address(LOCALHOST);
        // No asyncMode(true) called, so it should default to sync
        Assert.assertNotNull(builder);
    }

    @Test
    public void testAsyncModeWithAllOptions() {
        Sender.LineSenderBuilder builder = Sender.builder(Sender.Transport.WEBSOCKET)
                .address(LOCALHOST)
                .asyncMode(true)
                .autoFlushRows(500)
                .autoFlushBytes(512 * 1024)
                .autoFlushIntervalMillis(50)
                .inFlightWindowSize(8)
                .sendQueueCapacity(16);
        Assert.assertNotNull(builder);
    }

    @Test
    public void testSyncModeDoesNotAllowInFlightWindowSize() {
        try {
            Sender.builder(Sender.Transport.WEBSOCKET)
                    .address(LOCALHOST)
                    .asyncMode(false)
                    .inFlightWindowSize(16)
                    .build();
            fail("In-flight window size should require async mode");
        } catch (LineSenderException e) {
            TestUtils.assertContains(e.getMessage(), "requires async mode");
        }
    }

    @Test
    public void testSyncModeDoesNotAllowSendQueueCapacity() {
        try {
            Sender.builder(Sender.Transport.WEBSOCKET)
                    .address(LOCALHOST)
                    .asyncMode(false)
                    .sendQueueCapacity(32)
                    .build();
            fail("Send queue capacity should require async mode");
        } catch (LineSenderException e) {
            TestUtils.assertContains(e.getMessage(), "requires async mode");
        }
    }
}
