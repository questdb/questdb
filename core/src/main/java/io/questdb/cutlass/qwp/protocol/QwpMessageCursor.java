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

import io.questdb.cairo.CairoException;
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

    // Entries an in-flight delta is about to overwrite, so the finally can put them
    // back. setPos alone restores only the list's length, so without this an overlap
    // delta -- the shape of every full-dict frame and every orphan-adoption replay --
    // leaves its partial writes in place after a mid-frame throw. Reused across
    // messages; the common append-only delta leaves it empty.
    private final ObjList<String> dictRollbackScratch = new ObjList<>();
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
        releaseDictRollbackScratch();
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
        // A delta must extend the dictionary contiguously. A deltaStartId past
        // the current size leaves ids [size, deltaStartId) undefined, and
        // extendPos does not null-fill them -- so a gapped frame would expose
        // whatever STALE strings a prior delta left in those slots, not nulls.
        // That silently defeats the idx >= dictLimit guard in
        // QwpSymbolColumnCursor.of(): a row referencing one of those ids passes
        // the bounds check and reads back a misattributed symbol instead of
        // failing the frame. Reject the gap here, where it is still
        // attributable, rather than letting it become unattributable data.
        //
        // Only a client bug or a torn store-and-forward dictionary can produce
        // one: the ingestion client refuses to send a frame whose delta starts
        // above the coverage it has registered. Its own error code, DELTA_DICT_GAP,
        // keeps it out of PARSE_ERROR: the verdict depends on this connection's
        // dictionary coverage -- server state, not the frame's bytes -- so the
        // identical frame succeeds once the sender has re-registered from an id
        // the server actually holds, making the gap retriable rather than terminal.
        if (deltaStartId > connectionSymbolDict.size()) {
            throw QwpParseException.create(
                    QwpParseException.ErrorCode.DELTA_DICT_GAP,
                    "delta symbol dictionary gap: deltaStartId " + deltaStartId
                            + " exceeds dictionary size " + connectionSymbolDict.size()
            );
        }
        // Every entry costs at least its own 1-byte length varint, so deltaCount
        // <= payloadEnd - address is a necessary condition for a complete frame.
        // Checking it here, before extendPos, bounds both the allocation and the
        // finally's null-fill at O(payload) instead of O(deltaCount) on a frame
        // that claims up to MAX_SYMBOL_DICTIONARY_SIZE entries but carries none.
        if (deltaCount > payloadEnd - address) {
            throw QwpParseException.create(
                    QwpParseException.ErrorCode.INSUFFICIENT_DATA,
                    "delta symbol dictionary declares " + deltaCount
                            + " entries but only " + (payloadEnd - address) + " payload bytes remain"
            );
        }
        int sizeBefore = connectionSymbolDict.size();
        // The entry loop snapshots each pre-existing slot into dictRollbackScratch
        // just before overwriting it, so the finally can restore content alongside
        // the length: a frame that throws partway leaves the connection dictionary
        // exactly as it was. reject() neither closes the connection nor clears the
        // dictionary, and the symbolDictRedefined -> symbolCache.clear() consumer
        // runs only on success, so a half-applied overlap would leave the cache
        // mapping the old strings while the dictionary holds the new ones. The
        // scratch stays empty on a pure append.
        releaseDictRollbackScratch();
        boolean committed = false;
        try {
            // extendPos, not an add(null) loop: with the gap rejected above,
            // deltaStartId <= size(), so every slot in [size(), requiredSize) is
            // overwritten by the entry loop on the success path. Null-filling first was a
            // redundant O(new ids) write pass on every message carrying a delta.
            connectionSymbolDict.extendPos(requiredSize);

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

                // Read symbol value as UTF-8 directly from memory. Invalid UTF-8 is a protocol
                // parse error, not a transient storage failure.
                final String symbol;
                try {
                    symbol = Utf8s.stringFromUtf8Bytes(address, address + symbolLen);
                } catch (CairoException e) {
                    if (e.isMalformedUtf8()) {
                        throw QwpParseException.create(QwpParseException.ErrorCode.INVALID_UTF8, e.getFlyweightMessage());
                    }
                    throw e;
                }
                address += symbolLen;

                // Store in dictionary. Flag a redefinition when an existing client
                // symbol ID is remapped to a different string: orphan-adoption
                // replays a prior sender's dict-from-0 ahead of this sender's own,
                // so the same client symbol IDs are reused with different strings.
                // Re-sending an identical dict (the common dict-from-0 case within
                // one sender) overwrites with equal values and is not a redefinition.
                int dictIndex = deltaStartId + i;
                // Only a slot that existed before this frame can be REDEFINED. extendPos
                // does not null-fill and a rolled-back setPos does not clear, so a slot
                // above sizeBefore can still hold a stale string; reading it would falsely
                // raise symbolDictRedefined and force a needless symbolCache.clear().
                if (dictIndex < sizeBefore) {
                    String previous = connectionSymbolDict.getQuick(dictIndex);
                    // Snapshot before overwrite: the rollback in the finally restores
                    // [deltaStartId, deltaStartId + scratch.size()) -- exactly the
                    // pre-existing slots this loop rewrote before throwing.
                    dictRollbackScratch.add(previous);
                    if (previous != null && !previous.equals(symbol)) {
                        symbolDictRedefined = true;
                    }
                }
                connectionSymbolDict.setQuick(dictIndex, symbol);
            }
            committed = true;
            // Every entry this frame's delta overwrote is now superseded by the value
            // just written into connectionSymbolDict; the pre-overwrite copy has no
            // further use. Without this, a full-dictionary re-send (or an
            // orphan-adoption replay) leaves every entry double-pinned here, on a
            // pooled HttpConnectionContext, until the NEXT delta frame happens to
            // overlap and clear it at the top of this method -- or never, on a
            // connection that sends no further overlapping delta.
            releaseDictRollbackScratch();
        } finally {
            if (!committed) {
                // Content first, then length: setQuick asserts index < pos, so shrinking
                // before restoring would trip the assertion on the very slots being fixed.
                int restoreEnd = deltaStartId + dictRollbackScratch.size();
                for (int i = deltaStartId; i < restoreEnd; i++) {
                    connectionSymbolDict.setQuick(i, dictRollbackScratch.getQuick(i - deltaStartId));
                }
                // Null the tail the failed frame grew into before shrinking pos.
                // setPos alone leaves those Strings reachable above pos, and on a
                // first-frame failure (sizeBefore == 0) the connection-teardown
                // ObjList.clear() is a no-op -- it only Arrays.fill's when
                // pos > 0 -- so nothing else ever releases them off the pooled
                // context. Bounded by the live size() so it stays valid when
                // extendPos itself threw and pos is still sizeBefore.
                int grownSize = connectionSymbolDict.size();
                for (int i = sizeBefore; i < grownSize; i++) {
                    connectionSymbolDict.setQuick(i, null);
                }
                connectionSymbolDict.setPos(sizeBefore);
                releaseDictRollbackScratch();
            }
        }

        return address;
    }

    private void releaseDictRollbackScratch() {
        // Not ObjList.clear(): that Arrays.fill()s the WHOLE backing array,
        // and scratch capacity is lifetime-monotonic on a pooled connection,
        // so one large catch-up overlap would bill every later overlapping
        // frame the full historical capacity to release a handful of refs.
        // Slots above pos are already null: this is the only code that
        // shrinks pos, and it nulls everything it shrinks away.
        for (int i = 0, n = dictRollbackScratch.size(); i < n; i++) {
            dictRollbackScratch.setQuick(i, null);
        }
        dictRollbackScratch.setPos(0);
    }

}
