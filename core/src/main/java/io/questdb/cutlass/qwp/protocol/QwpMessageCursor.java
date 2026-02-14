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

package io.questdb.cutlass.qwp.protocol;

import io.questdb.std.Mutable;
import io.questdb.std.ObjList;
import io.questdb.std.Unsafe;
import io.questdb.std.str.Utf8s;

import static io.questdb.cutlass.qwp.protocol.QwpConstants.*;

/**
 * Streaming cursor over an ILP v4 message.
 * <p>
 * Provides iteration through table blocks in a message without allocating
 * intermediate objects. The single {@link QwpTableBlockCursor} is reused
 * for each table block.
 * <p>
 * <b>Usage:</b>
 * <pre>
 * while (cursor.hasNextTable()) {
 *     QwpTableBlockCursor tableBlock = cursor.nextTable();
 *     while (tableBlock.hasNextRow()) {
 *         tableBlock.nextRow();
 *         // process row...
 *     }
 * }
 * </pre>
 */
public class QwpMessageCursor implements Mutable {

    private final QwpTableBlockCursor tableBlockCursor = new QwpTableBlockCursor();
    private final QwpMessageHeader messageHeader = new QwpMessageHeader();
    // Reusable array for varint parsing: [0]=value, [1]=bytes consumed
    private final long[] varintResult = new long[2];

    // Message state
    private long payloadAddress;
    private long payloadEnd;
    private int tableCount;
    private int currentTableIndex;
    private long currentTableAddress;
    private boolean gorillaEnabled;
    private boolean deltaSymbolDictEnabled;
    private QwpSchemaCache schemaCache;
    private ObjList<String> connectionSymbolDict;

    /**
     * Initializes this cursor for the given message data with delta symbol dictionary support.
     *
     * @param messageAddress       address of message (including header)
     * @param messageLength        total message length in bytes
     * @param schemaCache          schema cache for reference mode (may be null)
     * @param connectionSymbolDict connection-level symbol dictionary (may be null)
     * @throws QwpParseException if parsing fails
     */
    public void of(long messageAddress, int messageLength, QwpSchemaCache schemaCache,
                   ObjList<String> connectionSymbolDict) throws QwpParseException {
        this.schemaCache = schemaCache;
        this.connectionSymbolDict = connectionSymbolDict;

        // Parse message header
        messageHeader.parse(messageAddress, messageLength);

        this.tableCount = messageHeader.getTableCount();
        this.gorillaEnabled = messageHeader.isGorillaEnabled();
        this.deltaSymbolDictEnabled = messageHeader.isDeltaSymbolDictEnabled();

        // Calculate payload bounds
        long payloadLength = messageHeader.getPayloadLength();
        this.payloadAddress = messageAddress + HEADER_SIZE;
        this.payloadEnd = payloadAddress + payloadLength;
        this.currentTableAddress = payloadAddress;

        // Parse delta symbol dictionary if enabled
        if (deltaSymbolDictEnabled && connectionSymbolDict != null) {
            currentTableAddress = parseDeltaSymbolDict(currentTableAddress);
        }

        this.currentTableIndex = -1;
    }

    /**
     * Parses the delta symbol dictionary section at the start of the payload.
     * <p>
     * Wire format:
     * <pre>
     * [deltaStartId: varint]   - First ID in this delta
     * [deltaCount: varint]     - Number of new symbols
     * [symbol_0 length: varint][symbol_0 bytes]
     * [symbol_1 length: varint][symbol_1 bytes]
     * ...
     * </pre>
     *
     * @param address start address of delta section
     * @return address after delta section
     * @throws QwpParseException if parsing fails
     */
    private long parseDeltaSymbolDict(long address) throws QwpParseException {
        if (address >= payloadEnd) {
            throw QwpParseException.create(
                    QwpParseException.ErrorCode.INSUFFICIENT_DATA,
                    "truncated delta symbol dictionary"
            );
        }

        // Read deltaStartId
        readVarint(address, varintResult);
        int deltaStartId = (int) varintResult[0];
        address += varintResult[1];

        // Read deltaCount
        readVarint(address, varintResult);
        int deltaCount = (int) varintResult[0];
        address += varintResult[1];

        // Ensure connectionSymbolDict has capacity
        int requiredSize = deltaStartId + deltaCount;
        while (connectionSymbolDict.size() < requiredSize) {
            connectionSymbolDict.add(null);
        }

        // Read and accumulate symbols
        for (int i = 0; i < deltaCount; i++) {
            if (address >= payloadEnd) {
                throw QwpParseException.create(
                        QwpParseException.ErrorCode.INSUFFICIENT_DATA,
                        "truncated delta symbol entry"
                );
            }

            // Read symbol length
            readVarint(address, varintResult);
            int symbolLen = (int) varintResult[0];
            address += varintResult[1];

            if (address + symbolLen > payloadEnd) {
                throw QwpParseException.create(
                        QwpParseException.ErrorCode.INSUFFICIENT_DATA,
                        "truncated delta symbol value"
                );
            }

            // Read symbol value as UTF-8 directly from memory
            String symbol = Utf8s.stringFromUtf8Bytes(address, address + symbolLen);
            address += symbolLen;

            // Store in dictionary
            connectionSymbolDict.setQuick(deltaStartId + i, symbol);
        }

        return address;
    }

    /**
     * Reads a varint from direct memory.
     *
     * @param address memory address
     * @param result  output array: [0]=value, [1]=bytes consumed
     */
    private void readVarint(long address, long[] result) throws QwpParseException {
        long value = 0;
        int shift = 0;
        int bytesRead = 0;

        while (address + bytesRead < payloadEnd) {
            byte b = Unsafe.getUnsafe().getByte(address + bytesRead);
            bytesRead++;
            value |= (long) (b & 0x7F) << shift;
            if ((b & 0x80) == 0) {
                result[0] = value;
                result[1] = bytesRead;
                return;
            }
            shift += 7;
            if (shift > 63) {
                throw QwpParseException.create(
                        QwpParseException.ErrorCode.VARINT_OVERFLOW,
                        "varint overflow"
                );
            }
        }

        throw QwpParseException.create(
                QwpParseException.ErrorCode.INSUFFICIENT_DATA,
                "truncated varint"
        );
    }

    /**
     * Returns the number of tables in this message.
     */
    public int getTableCount() {
        return tableCount;
    }

    /**
     * Returns whether Gorilla timestamp encoding is enabled.
     */
    public boolean isGorillaEnabled() {
        return gorillaEnabled;
    }

    /**
     * Returns whether there are more tables to iterate.
     */
    public boolean hasNextTable() {
        return currentTableIndex + 1 < tableCount;
    }

    /**
     * Advances to the next table and returns the table block cursor.
     * <p>
     * <b>Important:</b> The returned cursor is reused across calls.
     * It is invalidated on the next call to {@link #nextTable()} or {@link #clear()}.
     *
     * @return table block cursor positioned at the new table
     * @throws QwpParseException if parsing fails
     */
    public QwpTableBlockCursor nextTable() throws QwpParseException {
        if (!hasNextTable()) {
            throw new IllegalStateException("No more tables");
        }

        currentTableIndex++;
        tableBlockCursor.clear();

        int remainingBytes = (int) (payloadEnd - currentTableAddress);
        int consumed = tableBlockCursor.of(
                currentTableAddress, remainingBytes, gorillaEnabled, schemaCache,
                connectionSymbolDict, deltaSymbolDictEnabled);
        currentTableAddress += consumed;

        return tableBlockCursor;
    }

    /**
     * Returns the current table block cursor without advancing.
     * <p>
     * Must be called after {@link #nextTable()}.
     */
    public QwpTableBlockCursor getCurrentTable() {
        return tableBlockCursor;
    }

    /**
     * Returns the current table index (0-based).
     */
    public int getCurrentTableIndex() {
        return currentTableIndex;
    }

    /**
     * Returns the message header for diagnostics.
     */
    public QwpMessageHeader getMessageHeader() {
        return messageHeader;
    }

    /**
     * Returns whether delta symbol dictionary mode is enabled.
     */
    public boolean isDeltaSymbolDictEnabled() {
        return deltaSymbolDictEnabled;
    }

    @Override
    public void clear() {
        tableBlockCursor.clear();
        messageHeader.reset();
        payloadAddress = 0;
        payloadEnd = 0;
        tableCount = 0;
        currentTableIndex = -1;
        currentTableAddress = 0;
        gorillaEnabled = false;
        deltaSymbolDictEnabled = false;
        schemaCache = null;
        connectionSymbolDict = null;
    }
}
