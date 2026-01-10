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

package io.questdb.cutlass.line.tcp.v4;

import io.questdb.cairo.CairoEngine;
import io.questdb.cairo.SecurityContext;
import io.questdb.cutlass.line.tcp.IlpV4WalAppender;
import io.questdb.cutlass.line.tcp.LineTcpReceiverConfiguration;
import io.questdb.cutlass.line.tcp.TableUpdateDetails;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.network.Socket;
import io.questdb.std.MemoryTag;
import io.questdb.std.Unsafe;
import io.questdb.std.datetime.millitime.MillisecondClock;

import java.io.Closeable;

import static io.questdb.cutlass.line.tcp.v4.IlpV4Constants.*;

/**
 * Handles ILP v4 protocol processing within a LineTcpConnectionContext.
 * <p>
 * This handler is created when v4 protocol is detected and manages:
 * <ul>
 *   <li>Capability negotiation (handshake)</li>
 *   <li>Message decoding</li>
 *   <li>WAL integration</li>
 *   <li>Response generation</li>
 * </ul>
 * <p>
 * The handler works with the recv buffer provided by the parent context
 * and manages its own send buffer for responses.
 */
public class IlpV4ProtocolHandler implements Closeable {
    private static final Log LOG = LogFactory.getLog(IlpV4ProtocolHandler.class);

    // Handler states
    public static final int STATE_HANDSHAKE_WAIT_REQUEST = 0;
    public static final int STATE_HANDSHAKE_SEND_RESPONSE = 1;
    public static final int STATE_RECEIVING = 2;
    public static final int STATE_PROCESSING = 3;
    public static final int STATE_RESPONDING = 4;
    public static final int STATE_ERROR = 5;

    // Result codes for handleIO
    public static final int RESULT_NEEDS_READ = 0;
    public static final int RESULT_NEEDS_WRITE = 1;
    public static final int RESULT_CONTINUE = 2;
    public static final int RESULT_DISCONNECT = 3;
    public static final int RESULT_QUEUE_FULL = 4;

    private final LineTcpReceiverConfiguration configuration;
    private final IlpV4SchemaCache schemaCache;
    private final IlpV4MessageDecoder messageDecoder;
    private final IlpV4WalAppender walAppender;
    private final IlpV4CapabilityRequest capabilityRequest;
    private final IlpV4Negotiator negotiator;
    private final MillisecondClock milliClock;
    private final int fd;

    // Send buffer for responses
    private long sendBufferAddress;
    private int sendBufferSize;
    private int sendBufferPos;
    private int sendBufferLimit;

    // Recv buffer tracking (buffer owned by parent context)
    private long recvBufStart;
    private long recvBufProcessed; // How far we've processed in the recv buffer

    // State
    private int state;
    private short negotiatedCapabilities;
    private boolean gorillaEnabled;

    // Statistics
    private long messagesReceived;
    private long messagesProcessed;

    /**
     * Creates a new v4 protocol handler.
     *
     * @param configuration receiver configuration
     * @param fd            socket file descriptor (for logging)
     * @param milliClock    clock for timing
     */
    public IlpV4ProtocolHandler(
            LineTcpReceiverConfiguration configuration,
            int fd,
            MillisecondClock milliClock
    ) {
        this.configuration = configuration;
        this.fd = fd;
        this.milliClock = milliClock;

        // Initialize schema cache
        this.schemaCache = new IlpV4SchemaCache(256);

        // Initialize decoder and appender
        this.messageDecoder = new IlpV4MessageDecoder(schemaCache);
        this.walAppender = new IlpV4WalAppender(
                configuration.getAutoCreateNewColumns(),
                configuration.getMaxFileNameLength()
        );

        // Initialize handshake components
        this.capabilityRequest = new IlpV4CapabilityRequest();
        short serverCaps = FLAG_GORILLA; // Support Gorilla compression
        this.negotiator = new IlpV4Negotiator(VERSION_1, VERSION_1, serverCaps);

        // Initialize send buffer
        this.sendBufferSize = 1024;
        this.sendBufferAddress = Unsafe.malloc(sendBufferSize, MemoryTag.NATIVE_ILP_RSS);

        reset();
    }

    /**
     * Resets the handler state for a new connection.
     */
    public void reset() {
        state = STATE_HANDSHAKE_WAIT_REQUEST;
        sendBufferPos = 0;
        sendBufferLimit = 0;
        recvBufStart = 0;
        recvBufProcessed = 0;
        negotiatedCapabilities = 0;
        gorillaEnabled = false;
        messagesReceived = 0;
        messagesProcessed = 0;
        messageDecoder.reset();
    }

    /**
     * Sets the recv buffer addresses. Called by the parent context.
     *
     * @param bufStart start of the recv buffer
     */
    public void setRecvBuffer(long bufStart) {
        this.recvBufStart = bufStart;
        this.recvBufProcessed = bufStart;
    }

    /**
     * Returns how far we've processed in the recv buffer.
     * Used by parent context to compact the buffer.
     *
     * @return processed position
     */
    public long getRecvBufProcessed() {
        return recvBufProcessed;
    }

    /**
     * Returns the current handler state.
     *
     * @return state
     */
    public int getState() {
        return state;
    }

    /**
     * Returns true if the handshake is complete.
     *
     * @return true if handshake complete
     */
    public boolean isHandshakeComplete() {
        return state > STATE_HANDSHAKE_SEND_RESPONSE;
    }

    /**
     * Processes received data based on current state.
     *
     * @param recvBufPos current position in recv buffer (how much data is available)
     * @return result code indicating next action
     */
    public int process(long recvBufPos) {
        int available = (int) (recvBufPos - recvBufProcessed);

        switch (state) {
            case STATE_HANDSHAKE_WAIT_REQUEST:
                return processHandshakeRequest(available);

            case STATE_HANDSHAKE_SEND_RESPONSE:
                return RESULT_NEEDS_WRITE;

            case STATE_RECEIVING:
                return processMessageReceive(available);

            case STATE_PROCESSING:
                // Processing is done in processMessage() call
                return RESULT_CONTINUE;

            case STATE_RESPONDING:
                return RESULT_NEEDS_WRITE;

            case STATE_ERROR:
                return RESULT_DISCONNECT;

            default:
                LOG.error().$('[').$(fd).$("] unknown state [state=").$(state).$(']').$();
                return RESULT_DISCONNECT;
        }
    }

    /**
     * Processes the handshake request.
     */
    private int processHandshakeRequest(int available) {
        if (available < CAPABILITY_REQUEST_SIZE) {
            return RESULT_NEEDS_READ;
        }

        if (recvBufProcessed == 0) {
            LOG.error().$('[').$(fd).$("] recvBufProcessed is 0 during handshake").$();
            state = STATE_ERROR;
            return RESULT_DISCONNECT;
        }

        try {
            capabilityRequest.parse(recvBufProcessed, available);

            IlpV4Negotiator.NegotiationResult result = negotiator.negotiate(capabilityRequest);

            if (!result.success) {
                LOG.error().$('[').$(fd).$("] handshake negotiation failed").$();
                state = STATE_ERROR;
                return RESULT_DISCONNECT;
            }

            negotiatedCapabilities = (short) result.negotiatedFlags;
            gorillaEnabled = (negotiatedCapabilities & FLAG_GORILLA) != 0;

            // Prepare response
            IlpV4CapabilityResponse.encode(sendBufferAddress, result.negotiatedVersion, negotiatedCapabilities);
            sendBufferPos = 0;
            sendBufferLimit = CAPABILITY_RESPONSE_SIZE;

            // Mark request as processed
            recvBufProcessed += CAPABILITY_REQUEST_SIZE;

            state = STATE_HANDSHAKE_SEND_RESPONSE;
            LOG.info().$('[').$(fd).$("] v4 handshake request received, sending response").$();
            return RESULT_NEEDS_WRITE;

        } catch (IlpV4ParseException e) {
            LOG.error().$('[').$(fd).$("] handshake parse failed: ").$(e.getMessage()).$();
            state = STATE_ERROR;
            return RESULT_DISCONNECT;
        }
    }

    /**
     * Processes message reception.
     */
    private int processMessageReceive(int available) {
        if (available < HEADER_SIZE) {
            return RESULT_NEEDS_READ;
        }

        try {
            IlpV4MessageHeader header = messageDecoder.getMessageHeader();
            header.parse(recvBufProcessed, available);

            long totalLength = header.getTotalLength();

            // Check message size limit
            int maxMessageSize = 16 * 1024 * 1024; // 16 MB
            if (totalLength > maxMessageSize) {
                LOG.error().$('[').$(fd).$("] message too large [size=").$(totalLength)
                        .$(", max=").$(maxMessageSize).$(']').$();
                prepareErrorResponse("message too large");
                return RESULT_NEEDS_WRITE;
            }

            if (available < totalLength) {
                return RESULT_NEEDS_READ;
            }

            // Have complete message
            messagesReceived++;
            state = STATE_PROCESSING;
            return RESULT_CONTINUE;

        } catch (IlpV4ParseException e) {
            LOG.error().$('[').$(fd).$("] header parse failed: ").$(e.getMessage()).$();
            prepareErrorResponse(e.getMessage());
            return RESULT_NEEDS_WRITE;
        }
    }

    /**
     * Processes a complete message and appends to WAL.
     *
     * @param securityContext  security context for authorization
     * @param engine           Cairo engine for table operations
     * @param tudProvider      provider for table update details
     * @param recvBufPos       current recv buffer position
     * @return result code
     */
    public int processMessage(
            SecurityContext securityContext,
            CairoEngine engine,
            TableUpdateDetailsProvider tudProvider,
            long recvBufPos
    ) {
        if (state != STATE_PROCESSING) {
            LOG.error().$('[').$(fd).$("] processMessage called in wrong state [state=").$(state).$(']').$();
            return RESULT_DISCONNECT;
        }

        int available = (int) (recvBufPos - recvBufProcessed);

        try {
            // Decode message
            IlpV4DecodedMessage message = messageDecoder.decode(recvBufProcessed, available);

            // Process each table block
            for (int i = 0; i < message.getTableCount(); i++) {
                IlpV4DecodedTableBlock tableBlock = message.getTableBlock(i);
                String tableName = tableBlock.getTableName();

                TableUpdateDetails tud = tudProvider.getTableUpdateDetails(tableName);
                if (tud == null) {
                    if (!configuration.getAutoCreateNewTables()) {
                        LOG.error().$('[').$(fd).$("] table not found and auto-create disabled [table=").$(tableName).$(']').$();
                        prepareErrorResponse("table not found: " + tableName);
                        return RESULT_NEEDS_WRITE;
                    }

                    // Create table
                    tud = tudProvider.createTableUpdateDetails(tableName, tableBlock, engine, securityContext);
                    if (tud == null) {
                        prepareErrorResponse("failed to create table: " + tableName);
                        return RESULT_NEEDS_WRITE;
                    }
                }

                // Append to WAL
                walAppender.appendToWal(securityContext, tableBlock, tud);
            }

            messagesProcessed++;

            // Mark message as processed
            long totalLength = messageDecoder.getMessageHeader().getTotalLength();
            recvBufProcessed += totalLength;

            // Prepare OK response
            prepareOkResponse();
            return RESULT_NEEDS_WRITE;

        } catch (IlpV4ParseException e) {
            LOG.error().$('[').$(fd).$("] message decode failed: ").$(e.getMessage()).$();
            prepareErrorResponse(e.getMessage());
            return RESULT_NEEDS_WRITE;
        } catch (Exception e) {
            LOG.error().$('[').$(fd).$("] message processing failed: ").$(e.getMessage()).$();
            prepareErrorResponse("internal error: " + e.getMessage());
            return RESULT_NEEDS_WRITE;
        }
    }

    /**
     * Prepares an OK response.
     */
    private void prepareOkResponse() {
        IlpV4Response response = IlpV4Response.ok();
        int len = IlpV4ResponseEncoder.encode(response, sendBufferAddress, sendBufferSize);
        sendBufferPos = 0;
        sendBufferLimit = len;
        state = STATE_RESPONDING;
    }

    /**
     * Prepares an error response.
     */
    private void prepareErrorResponse(String message) {
        IlpV4Response response = IlpV4Response.parseError(message);
        int len = IlpV4ResponseEncoder.encode(response, sendBufferAddress, sendBufferSize);
        sendBufferPos = 0;
        sendBufferLimit = len;
        state = STATE_RESPONDING;
    }

    /**
     * Sends pending data from the send buffer.
     *
     * @param socket the socket to send to
     * @return result code
     */
    public int send(Socket socket) {
        if (sendBufferPos >= sendBufferLimit) {
            // Nothing to send
            onSendComplete();
            return RESULT_CONTINUE;
        }

        int remaining = sendBufferLimit - sendBufferPos;
        int sent = socket.send(sendBufferAddress + sendBufferPos, remaining);

        if (sent > 0) {
            sendBufferPos += sent;
            if (sendBufferPos >= sendBufferLimit) {
                onSendComplete();
                return RESULT_CONTINUE;
            }
            return RESULT_NEEDS_WRITE;
        } else if (sent == 0) {
            return RESULT_NEEDS_WRITE;
        } else {
            LOG.error().$('[').$(fd).$("] send failed").$();
            return RESULT_DISCONNECT;
        }
    }

    /**
     * Called when send is complete.
     */
    private void onSendComplete() {
        sendBufferPos = 0;
        sendBufferLimit = 0;

        if (state == STATE_HANDSHAKE_SEND_RESPONSE) {
            state = STATE_RECEIVING;
            LOG.info().$('[').$(fd).$("] v4 handshake complete [caps=0x")
                    .$(Integer.toHexString(negotiatedCapabilities & 0xFFFF))
                    .$(", gorilla=").$(gorillaEnabled)
                    .$(']').$();
        } else if (state == STATE_RESPONDING) {
            state = STATE_RECEIVING;
        }
    }

    /**
     * Returns whether Gorilla encoding is enabled.
     *
     * @return true if Gorilla enabled
     */
    public boolean isGorillaEnabled() {
        return gorillaEnabled;
    }

    /**
     * Returns the number of messages received.
     *
     * @return messages received
     */
    public long getMessagesReceived() {
        return messagesReceived;
    }

    /**
     * Returns the number of messages processed.
     *
     * @return messages processed
     */
    public long getMessagesProcessed() {
        return messagesProcessed;
    }

    @Override
    public void close() {
        if (sendBufferAddress != 0) {
            Unsafe.free(sendBufferAddress, sendBufferSize, MemoryTag.NATIVE_ILP_RSS);
            sendBufferAddress = 0;
        }
    }

    /**
     * Provider interface for table update details.
     */
    public interface TableUpdateDetailsProvider {
        /**
         * Gets table update details for an existing table.
         *
         * @param tableName table name
         * @return table update details, or null if table doesn't exist
         */
        TableUpdateDetails getTableUpdateDetails(String tableName);

        /**
         * Creates table update details for a new table.
         *
         * @param tableName       table name
         * @param tableBlock      decoded table block with schema info
         * @param engine          Cairo engine
         * @param securityContext security context
         * @return table update details, or null on failure
         */
        TableUpdateDetails createTableUpdateDetails(
                String tableName,
                IlpV4DecodedTableBlock tableBlock,
                CairoEngine engine,
                SecurityContext securityContext
        );
    }
}
