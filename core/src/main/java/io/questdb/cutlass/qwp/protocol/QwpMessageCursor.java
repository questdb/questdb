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

import io.questdb.std.LongList;
import io.questdb.std.Mutable;
import io.questdb.std.ObjList;
import io.questdb.std.Unsafe;
import io.questdb.std.str.DirectUtf8String;
import io.questdb.std.str.Utf8Sequence;
import io.questdb.std.str.Utf8s;

import static io.questdb.cutlass.qwp.protocol.QwpConstants.DEFAULT_MAX_ROWS_PER_TABLE;
import static io.questdb.cutlass.qwp.protocol.QwpConstants.HEADER_SIZE;
import static io.questdb.cutlass.qwp.protocol.QwpConstants.MAX_SYMBOL_DICTIONARY_SIZE;
import static io.questdb.cutlass.qwp.protocol.QwpConstants.TABLE_OPTION_TAG_DESIGNATED_TIMESTAMP_NAME;

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

    private final DirectUtf8String designatedTsName = new DirectUtf8String();
    private final LongList designatedTsNameBounds = new LongList();
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
    private boolean symbolDictRedefined;
    private int tableCount;

    public QwpMessageCursor() {
        this(DEFAULT_MAX_ROWS_PER_TABLE);
    }

    public QwpMessageCursor(int maxRowsPerTable) {
        this.tableBlockCursor = new QwpTableBlockCursor(maxRowsPerTable);
    }

    @Override
    public void clear() {
        designatedTsName.clear();
        designatedTsNameBounds.clear();
        tableBlockCursor.clear();
        messageHeader.reset();
        payloadAddress = 0;
        payloadEnd = 0;
        tableCount = 0;
        currentTableIndex = -1;
        currentTableAddress = 0;
        gorillaEnabled = false;
        deltaSymbolDictEnabled = false;
        connectionSymbolDict = null;
        symbolDictRedefined = false;
    }

    /**
     * Returns the create-only designated timestamp name option for a table.
     * <p>
     * The returned sequence is a reused flyweight over the message buffer and
     * is invalidated by the next call to this method or {@link #clear()}.
     *
     * @param tableIndex zero-based table index in message order
     * @return designated timestamp name, or {@code null} when absent
     */
    public Utf8Sequence getDesignatedTsName(int tableIndex) {
        if (tableIndex < 0 || tableIndex >= tableCount) {
            throw new IndexOutOfBoundsException("table index out of bounds: " + tableIndex);
        }
        int offsetIndex = tableIndex * 2;
        long lo = designatedTsNameBounds.getQuick(offsetIndex);
        if (lo < 0) {
            return null;
        }
        return designatedTsName.of(lo, designatedTsNameBounds.getQuick(offsetIndex + 1));
    }

    /**
     * Returns whether there are more tables to iterate.
     */
    public boolean hasNextTable() {
        return currentTableIndex + 1 < tableCount;
    }

    /**
     * Returns whether this message's delta symbol dictionary remapped an
     * existing client symbol ID to a different string (e.g. orphan-adoption
     * replay of another sender's dict-from-0). When true, the per-connection
     * clientSymbolId -&gt; tableSymbolId cache must be invalidated, since its
     * watermark-based invalidation cannot detect a remap that introduces no
     * new table symbols.
     */
    public boolean isSymbolDictRedefined() {
        return symbolDictRedefined;
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
                currentTableAddress, remainingBytes, gorillaEnabled,
                connectionSymbolDict, deltaSymbolDictEnabled);
        currentTableAddress += consumed;

        return tableBlockCursor;
    }

    /**
     * Initializes this cursor for the given message data with delta symbol dictionary support.
     *
     * @param messageAddress       address of message (including header)
     * @param messageLength        total message length in bytes
     * @param connectionSymbolDict connection-level symbol dictionary (may be null)
     * @throws QwpParseException if parsing fails
     */
    public void of(
            long messageAddress,
            int messageLength,
            ObjList<String> connectionSymbolDict
    ) throws QwpParseException {
        this.connectionSymbolDict = connectionSymbolDict;
        this.symbolDictRedefined = false;

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
        this.designatedTsNameBounds.setAll(tableCount * 2, -1);

        if (messageHeader.isTableOptionsEnabled()) {
            parseTableOptions();
        }

        // Parse delta symbol dictionary if enabled
        if (deltaSymbolDictEnabled && connectionSymbolDict != null) {
            currentTableAddress = parseDeltaSymbolDict(currentTableAddress);
        }

        this.currentTableIndex = -1;
    }

    private void parseTableOptions() throws QwpParseException {
        long payloadSize = payloadEnd - payloadAddress;
        if (payloadSize < Integer.BYTES) {
            throw QwpParseException.create(
                    QwpParseException.ErrorCode.INSUFFICIENT_DATA,
                    "truncated table options footer"
            );
        }

        long footerAddress = payloadEnd - Integer.BYTES;
        long trailerLength = Unsafe.getInt(footerAddress) & 0xffffffffL;
        long maxTrailerLength = footerAddress - payloadAddress;
        if (trailerLength > maxTrailerLength) {
            throw QwpParseException.create(
                    QwpParseException.ErrorCode.INSUFFICIENT_DATA,
                    "table options trailer length out of bounds [trailerLength=" + trailerLength
                            + ", available=" + maxTrailerLength + ']'
            );
        }

        long trailerAddress = footerAddress - trailerLength;
        long address = trailerAddress;
        for (int tableIndex = 0; tableIndex < tableCount; tableIndex++) {
            if (address >= footerAddress) {
                throw QwpParseException.create(
                        QwpParseException.ErrorCode.INSUFFICIENT_DATA,
                        "missing table options block [tableIndex=" + tableIndex + ']'
                );
            }

            QwpVarint.decode(address, footerAddress, varintResult);
            long blockLength = varintResult.value;
            address += varintResult.bytesRead;
            if (blockLength < 0 || blockLength > footerAddress - address) {
                throw QwpParseException.create(
                        QwpParseException.ErrorCode.INSUFFICIENT_DATA,
                        "table options block overruns trailer [tableIndex=" + tableIndex
                                + ", blockLength=" + blockLength + ']'
                );
            }

            long blockEnd = address + blockLength;
            while (address < blockEnd) {
                int tag = Unsafe.getByte(address++) & 0xff;
                QwpVarint.decode(address, blockEnd, varintResult);
                long valueLength = varintResult.value;
                address += varintResult.bytesRead;
                if (valueLength < 0 || valueLength > blockEnd - address) {
                    throw QwpParseException.create(
                            QwpParseException.ErrorCode.INSUFFICIENT_DATA,
                            "table option value overruns block [tableIndex=" + tableIndex
                                    + ", tag=" + tag + ", valueLength=" + valueLength + ']'
                    );
                }

                long valueEnd = address + valueLength;
                if (tag == TABLE_OPTION_TAG_DESIGNATED_TIMESTAMP_NAME) {
                    int offsetIndex = tableIndex * 2;
                    designatedTsNameBounds.setQuick(offsetIndex, address);
                    designatedTsNameBounds.setQuick(offsetIndex + 1, valueEnd);
                }
                address = valueEnd;
            }
        }

        if (address != footerAddress) {
            throw QwpParseException.create(
                    QwpParseException.ErrorCode.INSUFFICIENT_DATA,
                    "unexpected bytes after table options blocks: " + (footerAddress - address)
            );
        }
        payloadEnd = trailerAddress;
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

            // Store in dictionary. Flag a redefinition when an existing client
            // symbol ID is remapped to a different string: orphan-adoption
            // replays a prior sender's dict-from-0 ahead of this sender's own,
            // so the same client symbol IDs are reused with different strings.
            // Re-sending an identical dict (the common dict-from-0 case within
            // one sender) overwrites with equal values and is not a redefinition.
            int dictIndex = deltaStartId + i;
            String previous = connectionSymbolDict.getQuick(dictIndex);
            if (previous != null && !previous.equals(symbol)) {
                symbolDictRedefined = true;
            }
            connectionSymbolDict.setQuick(dictIndex, symbol);
        }

        return address;
    }

}
