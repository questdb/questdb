/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2024 QuestDB
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

package io.questdb.test.cutlass.line.tcp.v4;

import io.questdb.cutlass.line.tcp.v4.*;
import io.questdb.std.MemoryTag;
import io.questdb.std.Unsafe;
import org.junit.Test;

import static io.questdb.cutlass.line.tcp.v4.IlpV4Constants.*;
import static org.junit.Assert.*;

/**
 * Tests for IlpV4ConnectionContext.
 */
public class IlpV4ConnectionContextTest {

    // ==================== State Tests ====================

    @Test
    public void testInitialState() {
        IlpV4ReceiverConfiguration config = new DefaultTestConfig();
        try (IlpV4ConnectionContext ctx = new IlpV4ConnectionContext(config)) {
            assertEquals(IlpV4ConnectionContext.STATE_HANDSHAKE_WAIT_REQUEST, ctx.getState());
            assertFalse(ctx.isHandshakeComplete());
            assertEquals(0, ctx.getMessagesReceived());
            assertEquals(0, ctx.getMessagesProcessed());
        }
    }

    @Test
    public void testReset() {
        IlpV4ReceiverConfiguration config = new DefaultTestConfig();
        try (IlpV4ConnectionContext ctx = new IlpV4ConnectionContext(config)) {
            // Simulate some activity
            ctx.onDataReceived(100);

            // Reset
            ctx.reset();

            assertEquals(IlpV4ConnectionContext.STATE_HANDSHAKE_WAIT_REQUEST, ctx.getState());
            assertFalse(ctx.isHandshakeComplete());
            assertEquals(0, ctx.getMessagesReceived());
            assertEquals(0, ctx.getBytesReceived());
        }
    }

    // ==================== Handshake Tests ====================

    @Test
    public void testHandshakeNeedsMoreData() {
        IlpV4ReceiverConfiguration config = new DefaultTestConfig();
        try (IlpV4ConnectionContext ctx = new IlpV4ConnectionContext(config)) {
            // Empty buffer - needs more data
            IlpV4ConnectionContext.ProcessResult result = ctx.process();
            assertEquals(IlpV4ConnectionContext.ProcessResult.NEEDS_READ, result);

            // Partial handshake - still needs more data
            ctx.onDataReceived(4); // Not enough for full request
            result = ctx.process();
            assertEquals(IlpV4ConnectionContext.ProcessResult.NEEDS_READ, result);
        }
    }

    @Test
    public void testHandshakeSuccess() {
        IlpV4ReceiverConfiguration config = new DefaultTestConfig();
        try (IlpV4ConnectionContext ctx = new IlpV4ConnectionContext(config)) {
            // Write a valid capability request to the receive buffer
            long recvAddr = ctx.getRecvAddress();
            IlpV4CapabilityRequest.encode(recvAddr, VERSION_1, VERSION_1, FLAG_GORILLA);
            ctx.onDataReceived(CAPABILITY_REQUEST_SIZE);

            // Process should prepare response
            IlpV4ConnectionContext.ProcessResult result = ctx.process();
            assertEquals(IlpV4ConnectionContext.ProcessResult.NEEDS_WRITE, result);
            assertEquals(IlpV4ConnectionContext.STATE_HANDSHAKE_SEND_RESPONSE, ctx.getState());

            // Simulate sending response
            int sendLen = ctx.getSendRemaining();
            assertEquals(CAPABILITY_RESPONSE_SIZE, sendLen);
            ctx.onDataSent(sendLen);

            // Now should be in receiving state
            assertEquals(IlpV4ConnectionContext.STATE_RECEIVING, ctx.getState());
            assertTrue(ctx.isHandshakeComplete());
            assertTrue(ctx.isGorillaEnabled());
        }
    }

    @Test
    public void testHandshakeGorillaDisabled() {
        IlpV4ReceiverConfiguration config = new IlpV4ReceiverConfiguration() {
            @Override
            public boolean isGorillaSupported() {
                return false;
            }
        };

        try (IlpV4ConnectionContext ctx = new IlpV4ConnectionContext(config)) {
            // Client requests Gorilla, server doesn't support it
            long recvAddr = ctx.getRecvAddress();
            IlpV4CapabilityRequest.encode(recvAddr, VERSION_1, VERSION_1, FLAG_GORILLA);
            ctx.onDataReceived(CAPABILITY_REQUEST_SIZE);

            ctx.process();
            ctx.onDataSent(ctx.getSendRemaining());

            assertTrue(ctx.isHandshakeComplete());
            assertFalse(ctx.isGorillaEnabled());
            assertEquals(0, ctx.getNegotiatedCapabilities() & FLAG_GORILLA);
        }
    }

    @Test
    public void testHandshakeNoFlags() {
        IlpV4ReceiverConfiguration config = new DefaultTestConfig();
        try (IlpV4ConnectionContext ctx = new IlpV4ConnectionContext(config)) {
            // Client requests no special capabilities
            long recvAddr = ctx.getRecvAddress();
            IlpV4CapabilityRequest.encode(recvAddr, VERSION_1, VERSION_1, 0);
            ctx.onDataReceived(CAPABILITY_REQUEST_SIZE);

            ctx.process();
            ctx.onDataSent(ctx.getSendRemaining());

            assertTrue(ctx.isHandshakeComplete());
            assertFalse(ctx.isGorillaEnabled());
            assertEquals(0, ctx.getNegotiatedCapabilities());
        }
    }

    // ==================== Message Reception Tests ====================

    @Test
    public void testReceiveNeedsMoreData() {
        IlpV4ReceiverConfiguration config = new DefaultTestConfig();
        try (IlpV4ConnectionContext ctx = new IlpV4ConnectionContext(config)) {
            // Complete handshake first
            completeHandshake(ctx);

            // Empty buffer - needs more data
            IlpV4ConnectionContext.ProcessResult result = ctx.process();
            assertEquals(IlpV4ConnectionContext.ProcessResult.NEEDS_READ, result);
        }
    }

    @Test
    public void testReceivePartialHeader() {
        IlpV4ReceiverConfiguration config = new DefaultTestConfig();
        try (IlpV4ConnectionContext ctx = new IlpV4ConnectionContext(config)) {
            completeHandshake(ctx);

            // Write partial header
            ctx.onDataReceived(8); // Less than HEADER_SIZE (12)

            IlpV4ConnectionContext.ProcessResult result = ctx.process();
            assertEquals(IlpV4ConnectionContext.ProcessResult.NEEDS_READ, result);
        }
    }

    @Test
    public void testReceivePartialMessage() {
        IlpV4ReceiverConfiguration config = new DefaultTestConfig();
        try (IlpV4ConnectionContext ctx = new IlpV4ConnectionContext(config)) {
            completeHandshake(ctx);

            // Write a message header claiming 100 bytes payload
            long recvAddr = ctx.getRecvAddress();
            encodeMessageHeader(recvAddr, 1, 100, false);
            ctx.onDataReceived(HEADER_SIZE); // Only header, no payload

            IlpV4ConnectionContext.ProcessResult result = ctx.process();
            assertEquals(IlpV4ConnectionContext.ProcessResult.NEEDS_READ, result);
        }
    }

    // ==================== Statistics Tests ====================

    @Test
    public void testBytesReceivedTracking() {
        IlpV4ReceiverConfiguration config = new DefaultTestConfig();
        try (IlpV4ConnectionContext ctx = new IlpV4ConnectionContext(config)) {
            assertEquals(0, ctx.getBytesReceived());

            ctx.onDataReceived(100);
            assertEquals(100, ctx.getBytesReceived());

            ctx.onDataReceived(50);
            assertEquals(150, ctx.getBytesReceived());
        }
    }

    @Test
    public void testBytesSentTracking() {
        IlpV4ReceiverConfiguration config = new DefaultTestConfig();
        try (IlpV4ConnectionContext ctx = new IlpV4ConnectionContext(config)) {
            assertEquals(0, ctx.getBytesSent());

            // Complete handshake to send response
            completeHandshake(ctx);

            assertEquals(CAPABILITY_RESPONSE_SIZE, ctx.getBytesSent());
        }
    }

    // ==================== Configuration Tests ====================

    @Test
    public void testCustomConfiguration() {
        IlpV4ReceiverConfiguration config = new IlpV4ReceiverConfiguration() {
            @Override
            public int getMaxMessageSize() {
                return 1024;
            }

            @Override
            public int getRecvBufferSize() {
                return 512;
            }

            @Override
            public boolean isSchemaCachingEnabled() {
                return false;
            }
        };

        try (IlpV4ConnectionContext ctx = new IlpV4ConnectionContext(config)) {
            // Should create successfully with custom config
            assertEquals(IlpV4ConnectionContext.STATE_HANDSHAKE_WAIT_REQUEST, ctx.getState());
        }
    }

    // ==================== Helper Methods ====================

    private void completeHandshake(IlpV4ConnectionContext ctx) {
        long recvAddr = ctx.getRecvAddress();
        IlpV4CapabilityRequest.encode(recvAddr, VERSION_1, VERSION_1, FLAG_GORILLA);
        ctx.onDataReceived(CAPABILITY_REQUEST_SIZE);
        ctx.process();
        ctx.onDataSent(ctx.getSendRemaining());
    }

    private void encodeMessageHeader(long address, int tableCount, int payloadLength, boolean gorilla) {
        // Magic "ILP4"
        Unsafe.getUnsafe().putByte(address, (byte) 'I');
        Unsafe.getUnsafe().putByte(address + 1, (byte) 'L');
        Unsafe.getUnsafe().putByte(address + 2, (byte) 'P');
        Unsafe.getUnsafe().putByte(address + 3, (byte) '4');
        // Version
        Unsafe.getUnsafe().putByte(address + 4, VERSION_1);
        // Flags
        Unsafe.getUnsafe().putByte(address + 5, gorilla ? FLAG_GORILLA : 0);
        // Table count
        Unsafe.getUnsafe().putShort(address + 6, (short) tableCount);
        // Payload length
        Unsafe.getUnsafe().putInt(address + 8, payloadLength);
    }

    /**
     * Default test configuration.
     */
    private static class DefaultTestConfig implements IlpV4ReceiverConfiguration {
        // Use all defaults
    }
}
