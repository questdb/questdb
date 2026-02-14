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

package io.questdb.cutlass.qwp.server;

import io.questdb.cutlass.qwp.protocol.*;

import io.questdb.cutlass.qwp.websocket.*;

import io.questdb.std.MemoryTag;
import io.questdb.std.Mutable;
import io.questdb.std.QuietCloseable;
import io.questdb.std.Unsafe;

/**
 * WebSocket connection context that manages state, buffering, and frame processing.
 *
 * <p>This class provides zero-allocation message processing on hot paths by
 * pre-allocating buffers and reusing the frame parser.
 *
 * <p>Thread safety: This class is NOT thread-safe. Each connection should
 * have its own context instance.
 */
public class WebSocketConnectionContext implements Mutable, QuietCloseable {
    /**
     * Connection is open and can send/receive messages.
     */
    public static final int STATE_OPEN = 0;

    /**
     * Close handshake in progress.
     */
    public static final int STATE_CLOSING = 1;

    /**
     * Connection is fully closed.
     */
    public static final int STATE_CLOSED = 2;

    // Default values
    private static final int DEFAULT_MAX_MESSAGE_SIZE = 16 * 1024 * 1024;  // 16MB
    private static final int MAX_CONTROL_FRAME_PAYLOAD = 125;

    // Connection state
    private int state = STATE_OPEN;
    private boolean closeFrameSent = false;
    private boolean closeFrameReceived = false;

    // Buffers
    private final WebSocketBuffer recvBuffer;
    private final WebSocketBuffer sendBuffer;
    private final WebSocketBuffer fragmentBuffer;  // For assembling fragmented messages

    // Frame parsing
    private final WebSocketFrameParser parser;
    private int fragmentOpcode = -1;  // Original opcode of fragmented message

    // Pending pong
    private long pendingPongPayload = 0;
    private int pendingPongLength = 0;
    private boolean hasPendingPong = false;

    // Configuration
    private long maxMessageSize = DEFAULT_MAX_MESSAGE_SIZE;

    public WebSocketConnectionContext(int bufferSize) {
        this.recvBuffer = new WebSocketBuffer(bufferSize);
        this.sendBuffer = new WebSocketBuffer(bufferSize);
        this.fragmentBuffer = new WebSocketBuffer(bufferSize);
        this.parser = new WebSocketFrameParser();
        this.parser.setServerMode(true);  // Expect masked frames from clients
    }

    /**
     * Returns the current connection state.
     */
    public int getState() {
        return state;
    }

    /**
     * Returns true if the connection is in closing state.
     */
    public boolean isClosing() {
        return state == STATE_CLOSING;
    }

    /**
     * Returns true if the connection is fully closed.
     */
    public boolean isClosed() {
        return state == STATE_CLOSED;
    }

    /**
     * Returns true if a close frame has been sent.
     */
    public boolean isCloseFrameSent() {
        return closeFrameSent;
    }

    /**
     * Returns true if there's a pending close response to send.
     */
    public boolean hasPendingCloseResponse() {
        return closeFrameReceived && !closeFrameSent;
    }

    /**
     * Returns true if there's a pending pong frame to send.
     */
    public boolean hasPendingPong() {
        return hasPendingPong;
    }

    /**
     * Returns true if there's data pending in the send buffer.
     */
    public boolean hasPendingSend() {
        return sendBuffer.readableBytes() > 0;
    }

    /**
     * Returns the receive buffer.
     */
    public WebSocketBuffer getRecvBuffer() {
        return recvBuffer;
    }

    /**
     * Returns the send buffer.
     */
    public WebSocketBuffer getSendBuffer() {
        return sendBuffer;
    }

    /**
     * Returns the maximum allowed message size.
     */
    public long getMaxMessageSize() {
        return maxMessageSize;
    }

    /**
     * Sets the maximum allowed message size.
     *
     * @param maxMessageSize the maximum size in bytes, or 0 to disable limit
     */
    public void setMaxMessageSize(long maxMessageSize) {
        this.maxMessageSize = maxMessageSize;
    }

    /**
     * Initiates a close handshake.
     *
     * @param code   the close status code
     * @param reason the close reason (may be null)
     */
    public void initiateClose(int code, String reason) {
        if (state == STATE_CLOSED || closeFrameSent) {
            return;
        }
        sendCloseFrame(code, reason);
        closeFrameSent = true;
        state = STATE_CLOSING;
    }

    /**
     * Called when a close frame is received.
     *
     * @param code the close status code
     */
    public void onCloseFrameReceived(int code) {
        closeFrameReceived = true;
        if (closeFrameSent) {
            state = STATE_CLOSED;
        } else {
            state = STATE_CLOSING;
        }
    }

    /**
     * Processes received data and dispatches to the processor.
     *
     * @param processor the processor to handle messages
     */
    public void handleRead(WebSocketProcessor processor) {
        while (recvBuffer.readableBytes() > 0) {
            parser.reset();

            long bufStart = recvBuffer.readAddress();
            long bufEnd = bufStart + recvBuffer.readableBytes();

            int consumed = parser.parse(bufStart, bufEnd);

            // Check for parse errors
            if (parser.getState() == WebSocketFrameParser.STATE_ERROR) {
                processor.onError(parser.getErrorCode(), "Protocol error");
                return;
            }

            // Need more data
            if (consumed == 0 || parser.getState() == WebSocketFrameParser.STATE_NEED_MORE) {
                break;
            }

            // Header parsed but payload incomplete
            if (parser.getState() == WebSocketFrameParser.STATE_NEED_PAYLOAD) {
                break;
            }

            // Complete frame received
            int opcode = parser.getOpcode();
            long payloadLength = parser.getPayloadLength();
            int headerSize = parser.getHeaderSize();
            long payloadPtr = bufStart + headerSize;

            // Unmask payload if needed
            if (parser.isMasked()) {
                parser.unmaskPayload(payloadPtr, payloadLength);
            }

            // Process frame based on opcode
            boolean handled = handleFrame(opcode, payloadPtr, (int) payloadLength, parser.isFin(), processor);
            if (!handled) {
                return;  // Error occurred
            }

            // Advance buffer
            recvBuffer.skip(consumed);
        }

        // Compact buffer if needed
        recvBuffer.compact();
    }

    /**
     * Handles a single WebSocket frame.
     *
     * @return true if handled successfully, false on error
     */
    private boolean handleFrame(int opcode, long payloadPtr, int payloadLength, boolean fin,
                                WebSocketProcessor processor) {
        // Control frames can appear between data fragments
        if (WebSocketOpcode.isControlFrame(opcode)) {
            return handleControlFrame(opcode, payloadPtr, payloadLength, processor);
        }

        // If we're in closing state, ignore data frames
        if (state == STATE_CLOSING && closeFrameSent) {
            return true;
        }

        // Handle data frames
        return handleDataFrame(opcode, payloadPtr, payloadLength, fin, processor);
    }

    /**
     * Handles a control frame.
     */
    private boolean handleControlFrame(int opcode, long payloadPtr, int payloadLength,
                                       WebSocketProcessor processor) {
        switch (opcode) {
            case WebSocketOpcode.PING:
                processor.onPing(payloadPtr, payloadLength);
                // Send pong response immediately (RFC 6455 requires a pong for each ping)
                sendPongFrameImmediate(payloadPtr, payloadLength);
                return true;

            case WebSocketOpcode.PONG:
                processor.onPong(payloadPtr, payloadLength);
                return true;

            case WebSocketOpcode.CLOSE:
                return handleCloseFrame(payloadPtr, payloadLength, processor);

            default:
                processor.onError(WebSocketCloseCode.PROTOCOL_ERROR, "Unknown control opcode");
                return false;
        }
    }

    /**
     * Handles a close frame.
     */
    private boolean handleCloseFrame(long payloadPtr, int payloadLength, WebSocketProcessor processor) {
        int code = -1;
        long reasonPtr = 0;
        int reasonLength = 0;

        if (payloadLength >= 2) {
            // Read close code (big-endian)
            int high = Unsafe.getUnsafe().getByte(payloadPtr) & 0xFF;
            int low = Unsafe.getUnsafe().getByte(payloadPtr + 1) & 0xFF;
            code = (high << 8) | low;
            reasonPtr = payloadPtr + 2;
            reasonLength = payloadLength - 2;
        }

        processor.onClose(code, reasonPtr, reasonLength);
        onCloseFrameReceived(code);
        return true;
    }

    /**
     * Handles a data frame.
     */
    private boolean handleDataFrame(int opcode, long payloadPtr, int payloadLength, boolean fin,
                                    WebSocketProcessor processor) {
        // Check for protocol violations
        if (opcode == WebSocketOpcode.CONTINUATION) {
            if (fragmentOpcode == -1) {
                // Continuation without preceding fragment
                processor.onError(WebSocketCloseCode.PROTOCOL_ERROR, "Unexpected continuation frame");
                return false;
            }
        } else {
            // New data frame (TEXT or BINARY)
            if (fragmentOpcode != -1) {
                // New data frame while fragment in progress
                processor.onError(WebSocketCloseCode.PROTOCOL_ERROR, "New frame during fragmentation");
                return false;
            }
        }

        // Handle fragmentation
        if (!fin) {
            // Start or continue fragmented message
            if (opcode != WebSocketOpcode.CONTINUATION) {
                fragmentOpcode = opcode;
            }

            // Check size limit
            long totalSize = fragmentBuffer.readableBytes() + payloadLength;
            if (maxMessageSize > 0 && totalSize > maxMessageSize) {
                processor.onError(WebSocketCloseCode.MESSAGE_TOO_BIG, "Message too large");
                return false;
            }

            // Append to fragment buffer
            fragmentBuffer.write(payloadPtr, payloadLength);
            return true;
        }

        // Final frame
        if (fragmentOpcode != -1) {
            // Complete fragmented message
            // Check final size limit
            long totalSize = fragmentBuffer.readableBytes() + payloadLength;
            if (maxMessageSize > 0 && totalSize > maxMessageSize) {
                processor.onError(WebSocketCloseCode.MESSAGE_TOO_BIG, "Message too large");
                return false;
            }

            fragmentBuffer.write(payloadPtr, payloadLength);
            deliverMessage(fragmentOpcode, fragmentBuffer.readAddress(),
                    (int) fragmentBuffer.readableBytes(), processor);
            fragmentBuffer.clear();
            fragmentOpcode = -1;
        } else {
            // Non-fragmented message
            if (maxMessageSize > 0 && payloadLength > maxMessageSize) {
                processor.onError(WebSocketCloseCode.MESSAGE_TOO_BIG, "Message too large");
                return false;
            }
            deliverMessage(opcode, payloadPtr, payloadLength, processor);
        }

        return true;
    }

    /**
     * Delivers a complete message to the processor.
     */
    private void deliverMessage(int opcode, long payload, int length, WebSocketProcessor processor) {
        switch (opcode) {
            case WebSocketOpcode.BINARY:
                processor.onBinaryMessage(payload, length);
                break;
            case WebSocketOpcode.TEXT:
                processor.onTextMessage(payload, length);
                break;
        }
    }

    /**
     * Stores the ping payload for later pong response.
     */
    private void storePendingPong(long payloadPtr, int payloadLength) {
        // Allocate or reuse pong payload buffer
        if (pendingPongPayload == 0 && payloadLength > 0) {
            pendingPongPayload = Unsafe.malloc(MAX_CONTROL_FRAME_PAYLOAD, MemoryTag.NATIVE_DEFAULT);
        }

        if (payloadLength > 0 && pendingPongPayload != 0) {
            Unsafe.getUnsafe().copyMemory(payloadPtr, pendingPongPayload, payloadLength);
        }
        pendingPongLength = payloadLength;
        hasPendingPong = true;
    }

    /**
     * Sends the pending pong response.
     */
    public void sendPendingPong() {
        if (!hasPendingPong) {
            return;
        }

        int frameSize = WebSocketFrameWriter.headerSize(pendingPongLength, false) + pendingPongLength;
        sendBuffer.ensureCapacity(frameSize);

        long writePtr = sendBuffer.writeAddress();
        int written;
        if (pendingPongLength > 0) {
            written = WebSocketFrameWriter.writePongFrame(writePtr, pendingPongPayload, pendingPongLength);
        } else {
            written = WebSocketFrameWriter.writeHeader(writePtr, true, WebSocketOpcode.PONG, 0, false);
        }
        sendBuffer.advanceWrite(written);

        hasPendingPong = false;
    }

    /**
     * Sends a binary frame.
     */
    public void sendBinaryFrame(byte[] data, int offset, int length) {
        sendDataFrame(WebSocketOpcode.BINARY, data, offset, length);
    }

    /**
     * Sends a text frame.
     */
    public void sendTextFrame(byte[] data, int offset, int length) {
        sendDataFrame(WebSocketOpcode.TEXT, data, offset, length);
    }

    /**
     * Sends a data frame (binary or text).
     */
    private void sendDataFrame(int opcode, byte[] data, int offset, int length) {
        int headerSize = WebSocketFrameWriter.headerSize(length, false);
        int frameSize = headerSize + length;
        sendBuffer.ensureCapacity(frameSize);

        long writePtr = sendBuffer.writeAddress();
        int written = WebSocketFrameWriter.writeHeader(writePtr, true, opcode, length, false);

        // Copy payload
        for (int i = 0; i < length; i++) {
            Unsafe.getUnsafe().putByte(writePtr + written + i, data[offset + i]);
        }
        sendBuffer.advanceWrite(written + length);
    }

    /**
     * Sends a close frame.
     */
    public void sendCloseFrame(int code, String reason) {
        int payloadLen = 2;
        byte[] reasonBytes = null;
        if (reason != null && !reason.isEmpty()) {
            reasonBytes = reason.getBytes();
            payloadLen += reasonBytes.length;
        }

        int frameSize = WebSocketFrameWriter.headerSize(payloadLen, false) + payloadLen;
        sendBuffer.ensureCapacity(frameSize);

        long writePtr = sendBuffer.writeAddress();
        int written = WebSocketFrameWriter.writeCloseFrame(writePtr, code, reason);
        sendBuffer.advanceWrite(written);
    }

    /**
     * Sends a pong frame.
     */
    public void sendPongFrame(byte[] data, int offset, int length) {
        int frameSize = WebSocketFrameWriter.headerSize(length, false) + length;
        sendBuffer.ensureCapacity(frameSize);

        long writePtr = sendBuffer.writeAddress();
        int written = WebSocketFrameWriter.writePongFrame(writePtr, data, offset, length);
        sendBuffer.advanceWrite(written);
    }

    /**
     * Sends a pong frame immediately from native memory.
     * Used internally to respond to ping frames.
     */
    private void sendPongFrameImmediate(long payloadPtr, int payloadLength) {
        int frameSize = WebSocketFrameWriter.headerSize(payloadLength, false) + payloadLength;
        sendBuffer.ensureCapacity(frameSize);

        long writePtr = sendBuffer.writeAddress();
        int written;
        if (payloadLength > 0) {
            written = WebSocketFrameWriter.writePongFrame(writePtr, payloadPtr, payloadLength);
        } else {
            written = WebSocketFrameWriter.writeHeader(writePtr, true, WebSocketOpcode.PONG, 0, false);
        }
        sendBuffer.advanceWrite(written);
    }

    /**
     * Sends a ping frame.
     */
    public void sendPingFrame(byte[] data, int offset, int length) {
        int frameSize = WebSocketFrameWriter.headerSize(length, false) + length;
        sendBuffer.ensureCapacity(frameSize);

        long writePtr = sendBuffer.writeAddress();
        int written = WebSocketFrameWriter.writePingFrame(writePtr, data, offset, length);
        sendBuffer.advanceWrite(written);
    }

    @Override
    public void clear() {
        state = STATE_OPEN;
        closeFrameSent = false;
        closeFrameReceived = false;
        fragmentOpcode = -1;
        hasPendingPong = false;
        pendingPongLength = 0;
        recvBuffer.clear();
        sendBuffer.clear();
        fragmentBuffer.clear();
        parser.reset();
    }

    @Override
    public void close() {
        recvBuffer.close();
        sendBuffer.close();
        fragmentBuffer.close();
        if (pendingPongPayload != 0) {
            Unsafe.free(pendingPongPayload, MAX_CONTROL_FRAME_PAYLOAD, MemoryTag.NATIVE_DEFAULT);
            pendingPongPayload = 0;
        }
    }
}
