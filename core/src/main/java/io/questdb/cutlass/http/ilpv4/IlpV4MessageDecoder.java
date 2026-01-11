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

package io.questdb.cutlass.http.ilpv4;

import static io.questdb.cutlass.http.ilpv4.IlpV4Constants.*;

/**
 * Decodes complete ILP v4 messages containing multiple table blocks.
 * <p>
 * A message consists of:
 * <pre>
 * MESSAGE HEADER (12 bytes):
 *   Magic: "ILP4" (4 bytes)
 *   Version: uint8 (1 byte)
 *   Flags: uint8 (1 byte)
 *   Table count: uint16 (2 bytes)
 *   Payload length: uint32 (4 bytes)
 *
 * PAYLOAD:
 *   Table block 1
 *   Table block 2
 *   ...
 *   Table block N
 * </pre>
 * <p>
 * This decoder is reusable - create once and decode multiple messages.
 */
public class IlpV4MessageDecoder {

    private final IlpV4MessageHeader messageHeader = new IlpV4MessageHeader();
    private final IlpV4TableBlockDecoder tableBlockDecoder;
    private final IlpV4SchemaCache schemaCache;

    /**
     * Creates a message decoder without schema caching.
     */
    public IlpV4MessageDecoder() {
        this(null);
    }

    /**
     * Creates a message decoder with schema caching.
     *
     * @param schemaCache schema cache for schema reference mode
     */
    public IlpV4MessageDecoder(IlpV4SchemaCache schemaCache) {
        this.schemaCache = schemaCache;
        this.tableBlockDecoder = new IlpV4TableBlockDecoder(schemaCache);
    }

    /**
     * Decodes a complete message from direct memory.
     *
     * @param address starting address of the message
     * @param length  available bytes
     * @return decoded message
     * @throws IlpV4ParseException if parsing fails
     */
    public IlpV4DecodedMessage decode(long address, int length) throws IlpV4ParseException {
        // Parse message header
        messageHeader.parse(address, length);

        int tableCount = messageHeader.getTableCount();
        long payloadLength = messageHeader.getPayloadLength();
        byte flags = messageHeader.getFlags();
        boolean gorillaEnabled = messageHeader.isGorillaEnabled();

        // Validate we have enough data
        long totalLength = messageHeader.getTotalLength();
        if (length < totalLength) {
            throw IlpV4ParseException.create(
                    IlpV4ParseException.ErrorCode.INSUFFICIENT_DATA,
                    "incomplete message: need " + totalLength + " bytes, have " + length
            );
        }

        // Handle compression (skipped for now)
        if (messageHeader.isCompressed()) {
            throw IlpV4ParseException.create(
                    IlpV4ParseException.ErrorCode.DECOMPRESSION_ERROR,
                    "compression not yet supported"
            );
        }

        // Decode payload (table blocks)
        long payloadStart = address + HEADER_SIZE;
        long payloadEnd = payloadStart + payloadLength;

        return decodePayload(payloadStart, payloadEnd, tableCount, gorillaEnabled, flags, payloadLength);
    }

    /**
     * Decodes a complete message from a byte array.
     *
     * @param buf    buffer containing the message
     * @param offset starting offset
     * @param length available bytes
     * @return decoded message
     * @throws IlpV4ParseException if parsing fails
     */
    public IlpV4DecodedMessage decode(byte[] buf, int offset, int length) throws IlpV4ParseException {
        // Parse message header
        messageHeader.parse(buf, offset, length);

        int tableCount = messageHeader.getTableCount();
        long payloadLength = messageHeader.getPayloadLength();
        byte flags = messageHeader.getFlags();
        boolean gorillaEnabled = messageHeader.isGorillaEnabled();

        // Validate we have enough data
        long totalLength = messageHeader.getTotalLength();
        if (length < totalLength) {
            throw IlpV4ParseException.create(
                    IlpV4ParseException.ErrorCode.INSUFFICIENT_DATA,
                    "incomplete message: need " + totalLength + " bytes, have " + length
            );
        }

        // Handle compression (skipped for now)
        if (messageHeader.isCompressed()) {
            throw IlpV4ParseException.create(
                    IlpV4ParseException.ErrorCode.DECOMPRESSION_ERROR,
                    "compression not yet supported"
            );
        }

        // For byte array, we need to copy to direct memory for decoding
        // This is a simplification - production code might use a more efficient approach
        int payloadOffset = offset + HEADER_SIZE;
        int payloadLen = (int) payloadLength;

        // Allocate native memory and copy
        long payloadAddress = io.questdb.std.Unsafe.malloc(payloadLen, io.questdb.std.MemoryTag.NATIVE_DEFAULT);
        try {
            for (int i = 0; i < payloadLen; i++) {
                io.questdb.std.Unsafe.getUnsafe().putByte(payloadAddress + i, buf[payloadOffset + i]);
            }
            return decodePayload(payloadAddress, payloadAddress + payloadLen, tableCount, gorillaEnabled, flags, payloadLength);
        } finally {
            io.questdb.std.Unsafe.free(payloadAddress, payloadLen, io.questdb.std.MemoryTag.NATIVE_DEFAULT);
        }
    }

    /**
     * Decodes the payload containing table blocks.
     */
    private IlpV4DecodedMessage decodePayload(long payloadStart, long payloadEnd, int tableCount,
                                               boolean gorillaEnabled, byte flags, long payloadLength) throws IlpV4ParseException {
        IlpV4DecodedTableBlock[] tableBlocks = new IlpV4DecodedTableBlock[tableCount];
        long currentAddress = payloadStart;

        for (int i = 0; i < tableCount; i++) {
            if (currentAddress >= payloadEnd) {
                throw IlpV4ParseException.create(
                        IlpV4ParseException.ErrorCode.TABLE_COUNT_MISMATCH,
                        "ran out of data after " + i + " tables, expected " + tableCount
                );
            }

            // Decode table block
            IlpV4DecodedTableBlock block = tableBlockDecoder.decode(currentAddress, payloadEnd, gorillaEnabled);

            // Get how many bytes were consumed by this table block
            int consumed = tableBlockDecoder.getBytesConsumed();
            currentAddress += consumed;

            tableBlocks[i] = block;
        }

        // Verify we consumed all the payload
        long remaining = payloadEnd - currentAddress;
        if (remaining != 0) {
            throw IlpV4ParseException.create(
                        IlpV4ParseException.ErrorCode.PAYLOAD_TOO_LARGE,
                    "extra data after " + tableCount + " tables: " + remaining + " bytes remaining"
            );
        }

        return new IlpV4DecodedMessage(tableBlocks, flags, payloadLength);
    }

    /**
     * Gets the message header from the last decode operation.
     *
     * @return message header
     */
    public IlpV4MessageHeader getMessageHeader() {
        return messageHeader;
    }

    /**
     * Gets the table block decoder.
     *
     * @return table block decoder
     */
    public IlpV4TableBlockDecoder getTableBlockDecoder() {
        return tableBlockDecoder;
    }

    /**
     * Gets the schema cache.
     *
     * @return schema cache, or null if not configured
     */
    public IlpV4SchemaCache getSchemaCache() {
        return schemaCache;
    }

    /**
     * Resets the decoder for reuse.
     */
    public void reset() {
        messageHeader.reset();
    }
}
