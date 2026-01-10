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

import io.questdb.cairo.CommitFailedException;
import io.questdb.cairo.SecurityContext;
import io.questdb.cutlass.line.tcp.IlpV4WalAppender;
import io.questdb.cutlass.line.tcp.TableUpdateDetails;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.std.MemoryTag;
import io.questdb.std.Unsafe;

import java.io.Closeable;

import static io.questdb.cutlass.line.tcp.v4.IlpV4Constants.*;

/**
 * Handles ILP v4 connection state and message processing.
 * <p>
 * This class manages the lifecycle of an ILP v4 connection:
 * <ul>
 *   <li>Capability negotiation (handshake)</li>
 *   <li>Message reception</li>
 *   <li>Message decoding and WAL integration</li>
 *   <li>Response generation</li>
 * </ul>
 * <p>
 * State machine:
 * <pre>
 * HANDSHAKE_WAIT_REQUEST → HANDSHAKE_SEND_RESPONSE → RECEIVING → PROCESSING → RESPONDING
 *                                                        ↑                         |
 *                                                        └─────────────────────────┘
 * </pre>
 */
public class IlpV4ConnectionContext implements Closeable {
    private static final Log LOG = LogFactory.getLog(IlpV4ConnectionContext.class);

    // Connection states
    public static final int STATE_HANDSHAKE_WAIT_REQUEST = 0;
    public static final int STATE_HANDSHAKE_SEND_RESPONSE = 1;
    public static final int STATE_RECEIVING = 2;
    public static final int STATE_PROCESSING = 3;
    public static final int STATE_RESPONDING = 4;
    public static final int STATE_ERROR = 5;
    public static final int STATE_CLOSED = 6;

    private final IlpV4ReceiverConfiguration config;
    private final IlpV4MessageDecoder messageDecoder;
    private final IlpV4WalAppender walAppender;
    private final IlpV4CapabilityRequest capabilityRequest;
    private final IlpV4CapabilityResponse capabilityResponse;
    private final IlpV4Negotiator negotiator;
    private final IlpV4SchemaCache schemaCache;

    // Buffer management
    private long recvBufferAddress;
    private int recvBufferSize;
    private int recvBufferPos;
    private int recvBufferLimit;

    private long sendBufferAddress;
    private int sendBufferSize;
    private int sendBufferPos;
    private int sendBufferLimit;

    // State
    private int state;
    private short negotiatedCapabilities;
    private boolean gorillaEnabled;
    private IlpV4Response pendingResponse;

    // Statistics
    private long messagesReceived;
    private long messagesProcessed;
    private long bytesReceived;
    private long bytesSent;

    /**
     * Creates a new connection context.
     *
     * @param config configuration
     */
    public IlpV4ConnectionContext(IlpV4ReceiverConfiguration config) {
        this.config = config;
        this.schemaCache = config.isSchemaCachingEnabled() ?
                new IlpV4SchemaCache(config.getMaxCachedSchemas()) : null;
        this.messageDecoder = new IlpV4MessageDecoder(schemaCache);
        this.walAppender = new IlpV4WalAppender(
                config.isAutoCreateNewColumns(),
                config.getMaxFileNameLength()
        );
        this.capabilityRequest = new IlpV4CapabilityRequest();
        this.capabilityResponse = new IlpV4CapabilityResponse();
        this.negotiator = new IlpV4Negotiator(
                VERSION_1, VERSION_1,
                config.getServerCapabilities()
        );

        // Initialize buffers
        this.recvBufferSize = config.getRecvBufferSize();
        this.recvBufferAddress = Unsafe.malloc(recvBufferSize, MemoryTag.NATIVE_ILP_RSS);

        this.sendBufferSize = 1024; // Response buffer
        this.sendBufferAddress = Unsafe.malloc(sendBufferSize, MemoryTag.NATIVE_ILP_RSS);

        reset();
    }

    /**
     * Resets the connection context for reuse.
     */
    public void reset() {
        state = STATE_HANDSHAKE_WAIT_REQUEST;
        recvBufferPos = 0;
        recvBufferLimit = 0;
        sendBufferPos = 0;
        sendBufferLimit = 0;
        negotiatedCapabilities = 0;
        gorillaEnabled = false;
        pendingResponse = null;
        messagesReceived = 0;
        messagesProcessed = 0;
        bytesReceived = 0;
        bytesSent = 0;
        messageDecoder.reset();
    }

    /**
     * Returns the current connection state.
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
     * Returns the address where received data should be written.
     *
     * @return receive buffer address + current position
     */
    public long getRecvAddress() {
        return recvBufferAddress + recvBufferPos;
    }

    /**
     * Returns the available space in the receive buffer.
     *
     * @return available bytes
     */
    public int getRecvAvailable() {
        return recvBufferSize - recvBufferPos;
    }

    /**
     * Called after data is received to update the buffer position.
     *
     * @param bytesRead number of bytes received
     */
    public void onDataReceived(int bytesRead) {
        if (bytesRead <= 0) {
            return;
        }
        recvBufferPos += bytesRead;
        bytesReceived += bytesRead;
    }

    /**
     * Processes received data based on current state.
     *
     * @return result indicating next action needed
     */
    public ProcessResult process() {
        switch (state) {
            case STATE_HANDSHAKE_WAIT_REQUEST:
                return processHandshakeRequest();

            case STATE_HANDSHAKE_SEND_RESPONSE:
                return ProcessResult.NEEDS_WRITE;

            case STATE_RECEIVING:
                return processMessageReceive();

            case STATE_PROCESSING:
                // Should not reach here - processing is synchronous
                return ProcessResult.CONTINUE;

            case STATE_RESPONDING:
                return ProcessResult.NEEDS_WRITE;

            case STATE_ERROR:
            case STATE_CLOSED:
                return ProcessResult.DISCONNECT;

            default:
                LOG.error().$("unknown state [state=").$(state).$(']').$();
                return ProcessResult.DISCONNECT;
        }
    }

    /**
     * Processes an incoming ILP v4 capability request.
     */
    private ProcessResult processHandshakeRequest() {
        // Need at least the request size
        if (recvBufferPos < CAPABILITY_REQUEST_SIZE) {
            return ProcessResult.NEEDS_READ;
        }

        try {
            // Parse capability request
            capabilityRequest.parse(recvBufferAddress, recvBufferPos);

            // Negotiate capabilities
            IlpV4Negotiator.NegotiationResult result = negotiator.negotiate(capabilityRequest);

            if (!result.success) {
                LOG.error().$("handshake negotiation failed").$();
                state = STATE_ERROR;
                return ProcessResult.DISCONNECT;
            }

            negotiatedCapabilities = (short) result.negotiatedFlags;
            gorillaEnabled = (negotiatedCapabilities & FLAG_GORILLA) != 0;

            // Prepare response
            IlpV4CapabilityResponse.encode(sendBufferAddress, result.negotiatedVersion, negotiatedCapabilities);
            sendBufferPos = 0;
            sendBufferLimit = CAPABILITY_RESPONSE_SIZE;

            // Consume the request from receive buffer
            int consumed = CAPABILITY_REQUEST_SIZE;
            if (recvBufferPos > consumed) {
                // Move remaining data to start of buffer
                Unsafe.getUnsafe().copyMemory(
                        recvBufferAddress + consumed,
                        recvBufferAddress,
                        recvBufferPos - consumed
                );
            }
            recvBufferPos -= consumed;

            state = STATE_HANDSHAKE_SEND_RESPONSE;
            return ProcessResult.NEEDS_WRITE;

        } catch (IlpV4ParseException e) {
            LOG.error().$("handshake failed: ").$(e.getMessage()).$();
            state = STATE_ERROR;
            return ProcessResult.DISCONNECT;
        }
    }

    /**
     * Processes message reception.
     */
    private ProcessResult processMessageReceive() {
        // Need at least header size to check message completeness
        if (recvBufferPos < HEADER_SIZE) {
            return ProcessResult.NEEDS_READ;
        }

        // Check if we have a complete message
        try {
            IlpV4MessageHeader header = messageDecoder.getMessageHeader();
            header.parse(recvBufferAddress, recvBufferPos);

            long totalLength = header.getTotalLength();
            if (totalLength > config.getMaxMessageSize()) {
                LOG.error().$("message too large [size=").$(totalLength)
                        .$(", max=").$(config.getMaxMessageSize()).$(']').$();
                pendingResponse = IlpV4Response.parseError("message too large");
                prepareResponse();
                return ProcessResult.NEEDS_WRITE;
            }

            if (recvBufferPos < totalLength) {
                // Need more data
                ensureRecvBufferCapacity((int) totalLength);
                return ProcessResult.NEEDS_READ;
            }

            // Have complete message - process it
            messagesReceived++;
            state = STATE_PROCESSING;
            return ProcessResult.CONTINUE;

        } catch (IlpV4ParseException e) {
            LOG.error().$("header parse failed: ").$(e.getMessage()).$();
            pendingResponse = IlpV4Response.parseError(e.getMessage());
            prepareResponse();
            return ProcessResult.NEEDS_WRITE;
        }
    }

    /**
     * Processes a complete message and appends to WAL.
     *
     * @param securityContext security context for authorization
     * @param tableUpdateDetailsProvider provider for table update details
     * @return result indicating next action
     * @throws CommitFailedException if WAL commit fails
     */
    public ProcessResult processMessage(
            SecurityContext securityContext,
            TableUpdateDetailsProvider tableUpdateDetailsProvider
    ) throws CommitFailedException {
        if (state != STATE_PROCESSING) {
            LOG.error().$("processMessage called in wrong state [state=").$(state).$(']').$();
            return ProcessResult.DISCONNECT;
        }

        try {
            // Decode message
            IlpV4DecodedMessage message = messageDecoder.decode(recvBufferAddress, recvBufferPos);

            // Process each table block
            for (int i = 0; i < message.getTableCount(); i++) {
                IlpV4DecodedTableBlock tableBlock = message.getTableBlock(i);
                String tableName = tableBlock.getTableName();

                TableUpdateDetails tud = tableUpdateDetailsProvider.getTableUpdateDetails(tableName);
                if (tud == null) {
                    if (!config.isAutoCreateNewTables()) {
                        LOG.error().$("table not found and auto-create disabled [table=").$(tableName).$(']').$();
                        pendingResponse = IlpV4Response.tableNotFound(tableName);
                        prepareResponse();
                        return ProcessResult.NEEDS_WRITE;
                    }
                    // Table creation should be handled by tableUpdateDetailsProvider
                    tud = tableUpdateDetailsProvider.createTableUpdateDetails(tableName, tableBlock.getSchema());
                    if (tud == null) {
                        pendingResponse = IlpV4Response.internalError("failed to create table: " + tableName);
                        prepareResponse();
                        return ProcessResult.NEEDS_WRITE;
                    }
                }

                // Append to WAL
                walAppender.appendToWal(securityContext, tableBlock, tud);
            }

            messagesProcessed++;

            // Consume the message from receive buffer
            long totalLength = messageDecoder.getMessageHeader().getTotalLength();
            if (recvBufferPos > totalLength) {
                Unsafe.getUnsafe().copyMemory(
                        recvBufferAddress + totalLength,
                        recvBufferAddress,
                        recvBufferPos - totalLength
                );
            }
            recvBufferPos -= (int) totalLength;

            // Send OK response
            pendingResponse = IlpV4Response.ok();
            prepareResponse();
            return ProcessResult.NEEDS_WRITE;

        } catch (IlpV4ParseException e) {
            LOG.error().$("message decode failed: ").$(e.getMessage()).$();
            pendingResponse = IlpV4Response.parseError(e.getMessage());
            prepareResponse();
            return ProcessResult.NEEDS_WRITE;
        }
    }

    /**
     * Prepares the response for sending.
     */
    private void prepareResponse() {
        if (pendingResponse != null) {
            int len = IlpV4ResponseEncoder.encode(pendingResponse, sendBufferAddress, sendBufferSize);
            sendBufferPos = 0;
            sendBufferLimit = len;
            state = STATE_RESPONDING;
        }
    }

    /**
     * Returns the address of data to send.
     *
     * @return send buffer address + current position
     */
    public long getSendAddress() {
        return sendBufferAddress + sendBufferPos;
    }

    /**
     * Returns the number of bytes to send.
     *
     * @return bytes remaining to send
     */
    public int getSendRemaining() {
        return sendBufferLimit - sendBufferPos;
    }

    /**
     * Called after data is sent to update the buffer position.
     *
     * @param bytesSent number of bytes sent
     */
    public void onDataSent(int bytesSent) {
        if (bytesSent <= 0) {
            return;
        }
        sendBufferPos += bytesSent;
        this.bytesSent += bytesSent;

        if (sendBufferPos >= sendBufferLimit) {
            // All data sent
            sendBufferPos = 0;
            sendBufferLimit = 0;

            if (state == STATE_HANDSHAKE_SEND_RESPONSE) {
                state = STATE_RECEIVING;
                LOG.info().$("handshake complete [caps=0x")
                        .$(Integer.toHexString(negotiatedCapabilities & 0xFFFF))
                        .$(", gorilla=").$(gorillaEnabled)
                        .$(']').$();
            } else if (state == STATE_RESPONDING) {
                pendingResponse = null;
                state = STATE_RECEIVING;
            }
        }
    }

    /**
     * Ensures the receive buffer has at least the specified capacity.
     *
     * @param capacity required capacity
     */
    private void ensureRecvBufferCapacity(int capacity) {
        if (capacity <= recvBufferSize) {
            return;
        }

        int newSize = Math.min(
                Math.max(recvBufferSize * 2, capacity),
                config.getMaxRecvBufferSize()
        );

        if (newSize > recvBufferSize) {
            long newAddress = Unsafe.realloc(recvBufferAddress, recvBufferSize, newSize, MemoryTag.NATIVE_ILP_RSS);
            recvBufferAddress = newAddress;
            recvBufferSize = newSize;
        }
    }

    /**
     * Returns whether Gorilla encoding is enabled for this connection.
     *
     * @return true if Gorilla enabled
     */
    public boolean isGorillaEnabled() {
        return gorillaEnabled;
    }

    /**
     * Returns the negotiated capabilities.
     *
     * @return negotiated capabilities
     */
    public short getNegotiatedCapabilities() {
        return negotiatedCapabilities;
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

    /**
     * Returns the total bytes received.
     *
     * @return bytes received
     */
    public long getBytesReceived() {
        return bytesReceived;
    }

    /**
     * Returns the total bytes sent.
     *
     * @return bytes sent
     */
    public long getBytesSent() {
        return bytesSent;
    }

    @Override
    public void close() {
        if (recvBufferAddress != 0) {
            Unsafe.free(recvBufferAddress, recvBufferSize, MemoryTag.NATIVE_ILP_RSS);
            recvBufferAddress = 0;
        }
        if (sendBufferAddress != 0) {
            Unsafe.free(sendBufferAddress, sendBufferSize, MemoryTag.NATIVE_ILP_RSS);
            sendBufferAddress = 0;
        }
        // Note: schemaCache doesn't need closing - it's just an in-memory map
        state = STATE_CLOSED;
    }

    /**
     * Result of processing received data.
     */
    public enum ProcessResult {
        /**
         * Need more data from network.
         */
        NEEDS_READ,

        /**
         * Need to send data to network.
         */
        NEEDS_WRITE,

        /**
         * Continue processing (synchronous).
         */
        CONTINUE,

        /**
         * Disconnect the connection.
         */
        DISCONNECT
    }

    /**
     * Provider for table update details.
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
         * @param tableName table name
         * @param schema    table schema
         * @return table update details, or null on failure
         */
        TableUpdateDetails createTableUpdateDetails(String tableName, IlpV4ColumnDef[] schema);
    }
}
