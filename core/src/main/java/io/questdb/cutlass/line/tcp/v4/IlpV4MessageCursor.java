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

import io.questdb.std.Mutable;
import io.questdb.std.Unsafe;

import static io.questdb.cutlass.line.tcp.v4.IlpV4Constants.*;

/**
 * Streaming cursor over an ILP v4 message.
 * <p>
 * Provides iteration through table blocks in a message without allocating
 * intermediate objects. The single {@link IlpV4TableBlockCursor} is reused
 * for each table block.
 * <p>
 * <b>Usage:</b>
 * <pre>
 * while (cursor.hasNextTable()) {
 *     IlpV4TableBlockCursor tableBlock = cursor.nextTable();
 *     while (tableBlock.hasNextRow()) {
 *         tableBlock.nextRow();
 *         // process row...
 *     }
 * }
 * </pre>
 */
public class IlpV4MessageCursor implements Mutable {

    private final IlpV4TableBlockCursor tableBlockCursor = new IlpV4TableBlockCursor();
    private final IlpV4MessageHeader messageHeader = new IlpV4MessageHeader();

    // Message state
    private long payloadAddress;
    private long payloadEnd;
    private int tableCount;
    private int currentTableIndex;
    private long currentTableAddress;
    private boolean gorillaEnabled;
    private IlpV4SchemaCache schemaCache;

    /**
     * Initializes this cursor for the given message data.
     *
     * @param messageAddress address of message (including header)
     * @param messageLength  total message length in bytes
     * @param schemaCache    schema cache for reference mode (may be null)
     * @throws IlpV4ParseException if parsing fails
     */
    public void of(long messageAddress, int messageLength, IlpV4SchemaCache schemaCache) throws IlpV4ParseException {
        this.schemaCache = schemaCache;

        // Parse message header
        messageHeader.parse(messageAddress, messageLength);

        this.tableCount = messageHeader.getTableCount();
        this.gorillaEnabled = messageHeader.isGorillaEnabled();

        // Calculate payload bounds
        long payloadLength = messageHeader.getPayloadLength();
        this.payloadAddress = messageAddress + HEADER_SIZE;
        this.payloadEnd = payloadAddress + payloadLength;
        this.currentTableAddress = payloadAddress;
        this.currentTableIndex = -1;
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
     * @throws IlpV4ParseException if parsing fails
     */
    public IlpV4TableBlockCursor nextTable() throws IlpV4ParseException {
        if (!hasNextTable()) {
            throw new IllegalStateException("No more tables");
        }

        currentTableIndex++;
        tableBlockCursor.clear();

        int remainingBytes = (int) (payloadEnd - currentTableAddress);
        int consumed = tableBlockCursor.of(currentTableAddress, remainingBytes, gorillaEnabled, schemaCache);
        currentTableAddress += consumed;

        return tableBlockCursor;
    }

    /**
     * Returns the current table block cursor without advancing.
     * <p>
     * Must be called after {@link #nextTable()}.
     */
    public IlpV4TableBlockCursor getCurrentTable() {
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
    public IlpV4MessageHeader getMessageHeader() {
        return messageHeader;
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
        schemaCache = null;
    }
}
