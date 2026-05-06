/*+*****************************************************************************
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

package io.questdb.cutlass.qwp.protocol;

import io.questdb.std.Mutable;
import io.questdb.std.ObjList;
import io.questdb.std.str.Utf8s;

import static io.questdb.cutlass.qwp.protocol.QwpConstants.DEFAULT_MAX_ROWS_PER_TABLE;
import static io.questdb.cutlass.qwp.protocol.QwpConstants.HEADER_SIZE;
import static io.questdb.cutlass.qwp.protocol.QwpConstants.MAX_SYMBOL_DICTIONARY_SIZE;

/**
 * Streaming cursor over a QWP v1 message.
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

    private final QwpMessageHeader messageHeader = new QwpMessageHeader();
    private final QwpTableBlockCursor tableBlockCursor;
    private final QwpVarint.DecodeResult varintResult = new QwpVarint.DecodeResult();
    private ObjList<String> connectionSymbolDict;
    private long currentTableAddress;
    private int currentTableIndex;
    private boolean deltaSymbolDictEnabled;
    private boolean gorillaEnabled;
    // Message state
    private long payloadAddress;
    private long payloadEnd;
    private QwpSchemaRegistry schemaRegistry;
    private int tableCount;

    public QwpMessageCursor() {
        this(DEFAULT_MAX_ROWS_PER_TABLE);
    }

    public QwpMessageCursor(int maxRowsPerTable) {
        this.tableBlockCursor = new QwpTableBlockCursor(maxRowsPerTable);
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
        schemaRegistry = null;
        connectionSymbolDict = null;
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
     * It is invalidated on the next call to nextTable() or {@link #clear()}.
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

        long remaining = payloadEnd - currentTableAddress;
        if (remaining < 0 || remaining > Integer.MAX_VALUE) {
            throw QwpParseException.create(
                    QwpParseException.ErrorCode.INSUFFICIENT_DATA,
                    "remaining payload size out of range: " + remaining
            );
        }
        int remainingBytes = (int) remaining;
        int consumed = tableBlockCursor.of(
                currentTableAddress, remainingBytes, gorillaEnabled, schemaRegistry,
                connectionSymbolDict, deltaSymbolDictEnabled);
        currentTableAddress += consumed;

        return tableBlockCursor;
    }

    /**
     * Initializes this cursor for the given message data with delta symbol dictionary support.
     *
     * @param messageAddress       address of message (including header)
     * @param messageLength        total message length in bytes
     * @param schemaRegistry       schema registry for reference mode (may be null)
     * @param connectionSymbolDict connection-level symbol dictionary (may be null)
     * @throws QwpParseException if parsing fails
     */
    public void of(
            long messageAddress,
            int messageLength,
            QwpSchemaRegistry schemaRegistry,
            ObjList<String> connectionSymbolDict
    ) throws QwpParseException {
        this.schemaRegistry = schemaRegistry;
        this.connectionSymbolDict = connectionSymbolDict;

        // Parse message header
        messageHeader.parse(messageAddress, messageLength);

        this.tableCount = messageHeader.getTableCount();
        this.gorillaEnabled = messageHeader.isGorillaEnabled();
        this.deltaSymbolDictEnabled = messageHeader.isDeltaSymbolDictEnabled();

        // Calculate payload bounds
        long payloadLength = messageHeader.getPayloadLength();
        long availablePayloadLength = messageLength - HEADER_SIZE;
        if (payloadLength > availablePayloadLength) {
            throw QwpParseException.create(
                    QwpParseException.ErrorCode.INSUFFICIENT_DATA,
                    "message payload exceeds available data [payloadLength=" + payloadLength + ", available=" + availablePayloadLength + ']'
            );
        }
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
        QwpVarint.decode(address, payloadEnd, varintResult);
        if (varintResult.value < 0 || varintResult.value > Integer.MAX_VALUE) {
            throw QwpParseException.create(
                    QwpParseException.ErrorCode.INSUFFICIENT_DATA,
                    "deltaStartId out of int range: " + varintResult.value
            );
        }
        int deltaStartId = (int) varintResult.value;
        address += varintResult.bytesRead;

        // Read deltaCount
        QwpVarint.decode(address, payloadEnd, varintResult);
        if (varintResult.value < 0 || varintResult.value > Integer.MAX_VALUE) {
            throw QwpParseException.create(
                    QwpParseException.ErrorCode.INSUFFICIENT_DATA,
                    "deltaCount out of int range: " + varintResult.value
            );
        }
        int deltaCount = (int) varintResult.value;
        address += varintResult.bytesRead;

        // Check for integer overflow and enforce upper bound.
        // A varint can encode billions in just 5 bytes, so without this
        // check a malicious client could exhaust heap memory.
        long requiredSizeLong = (long) deltaStartId + deltaCount;
        if (requiredSizeLong > MAX_SYMBOL_DICTIONARY_SIZE) {
            throw QwpParseException.create(
                    QwpParseException.ErrorCode.INSUFFICIENT_DATA,
                    "delta symbol dictionary size exceeds limit: " + requiredSizeLong
            );
        }
        int requiredSize = (int) requiredSizeLong;
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
            QwpVarint.decode(address, payloadEnd, varintResult);
            if (varintResult.value < 0 || varintResult.value > Integer.MAX_VALUE) {
                throw QwpParseException.create(
                        QwpParseException.ErrorCode.INSUFFICIENT_DATA,
                        "delta symbol length out of int range: " + varintResult.value
                );
            }
            int symbolLen = (int) varintResult.value;
            address += varintResult.bytesRead;

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

}
