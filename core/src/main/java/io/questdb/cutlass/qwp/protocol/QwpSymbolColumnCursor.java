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

import io.questdb.std.IntList;
import io.questdb.std.ObjList;
import io.questdb.std.Unsafe;
import io.questdb.std.str.DirectUtf8Sequence;
import io.questdb.std.str.DirectUtf8String;
import io.questdb.std.str.StringSink;
import io.questdb.std.str.Utf8s;

import static io.questdb.cutlass.qwp.protocol.QwpConstants.MAX_SYMBOL_DICTIONARY_SIZE;
import static io.questdb.cutlass.qwp.protocol.QwpConstants.TYPE_SYMBOL;

/**
 * Streaming cursor for SYMBOL columns.
 * <p>
 * Wire format:
 * <pre>
 * [if null bitmap is present]: ceil(rowCount/8) bytes
 * [dictionary size]: varint
 * For each dictionary entry:
 *   [string length]: varint
 *   [string data]: UTF-8 bytes
 * [indices]: varint per non-null value, references dictionary entry
 * </pre>
 * <p>
 * Dictionary is parsed once during initialization. Symbol values are accessed
 * via dictionary index for zero-allocation iteration.
 */
public final class QwpSymbolColumnCursor implements QwpColumnCursor {

    private final QwpVarint.DecodeResult decodeResult = new QwpVarint.DecodeResult();
    // Pre-allocated dictionary storage (flyweights pointing to wire memory)
    private final ObjList<DirectUtf8String> dictionaryUtf8 = new ObjList<>();
    private final StringSink utf16Sink = new StringSink();
    // External dictionary reference (for delta mode)
    private ObjList<String> connectionDict;
    private boolean currentIsNull;
    // Iteration state
    private int currentRow;
    private int currentSymbolIndex;
    private int currentValueIndex;
    // Pre-decoded varint indices (reused across calls, grows to max batch size)
    private final IntList decodedIndices = new IntList();
    private boolean deltaMode;  // When true, use connectionDict instead of per-column dictionary
    private int dictionarySize;
    // Wire pointers
    private long nullBitmapAddress;
    private int valueCount;

    @Override
    public boolean advanceRow() {
        currentRow++;
        currentValueIndex++;

        if (nullBitmapAddress != 0) {
            currentIsNull = QwpNullBitmap.isNull(nullBitmapAddress, currentRow);
            if (currentIsNull) {
                currentSymbolIndex = -1;
                currentValueIndex--;
                return true;
            }
        } else {
            currentIsNull = false;
        }

        // Read from pre-decoded index list (decoded and validated in of())
        currentSymbolIndex = decodedIndices.getQuick(currentValueIndex);
        return false;
    }

    @Override
    public void clear() {
        dictionarySize = 0;
        deltaMode = false;
        connectionDict = null;
        nullBitmapAddress = 0;
        valueCount = 0;
        // Clear dictionary flyweights
        for (int i = 0; i < dictionaryUtf8.size(); i++) {
            dictionaryUtf8.getQuick(i).clear();
        }
        resetRowPosition();
    }

    /**
     * Returns current row's symbol value as a CharSequence (UTF-16).
     * <p>
     * <b>Zero-allocation for ASCII:</b> For ASCII symbols, returns a view over wire memory.
     * For non-ASCII, converts into an internal sink (valid until the next call).
     * <p>
     * In delta mode, retrieves from the connection-level dictionary.
     *
     * @return CharSequence value, or null if NULL
     */
    public CharSequence getSymbolCharSequence() {
        if (currentIsNull || currentSymbolIndex < 0) {
            return null;
        }
        if (deltaMode) {
            if (connectionDict != null && currentSymbolIndex < connectionDict.size()) {
                return connectionDict.getQuick(currentSymbolIndex);
            }
            return null;
        }
        utf16Sink.clear();
        return Utf8s.directUtf8ToUtf16(dictionaryUtf8.getQuick(currentSymbolIndex), utf16Sink);
    }

    /**
     * Returns current row's symbol index into the dictionary.
     *
     * @return dictionary index (0-based), or -1 if NULL
     */
    public int getSymbolIndex() {
        return currentSymbolIndex;
    }

    /**
     * Returns current row's symbol value as a UTF-8 sequence.
     * <p>
     * <b>Zero-allocation:</b> Returns a flyweight from the dictionary.
     * <p>
     * <b>Note:</b> Returns null in delta mode (use {@link #getSymbolCharSequence()} instead).
     *
     * @return UTF-8 sequence from dictionary, or null if NULL or delta mode
     */
    public DirectUtf8Sequence getSymbolUtf8() {
        if (currentIsNull || currentSymbolIndex < 0 || deltaMode) {
            return null;
        }
        return dictionaryUtf8.getQuick(currentSymbolIndex);
    }

    @Override
    public byte getTypeCode() {
        return TYPE_SYMBOL;
    }

    /**
     * Returns whether this cursor is using delta mode (connection dictionary).
     *
     * @return true if delta mode is enabled
     */
    public boolean isDeltaMode() {
        return deltaMode;
    }

    @Override
    public boolean isNull() {
        return currentIsNull;
    }

    /**
     * Initializes this cursor for the given column data.
     *
     * @param dataAddress address of column data
     * @param dataLength  available bytes
     * @param rowCount    number of rows
     * @return bytes consumed from dataAddress
     * @throws QwpParseException if parsing fails
     */
    public int of(long dataAddress, int dataLength, int rowCount) throws QwpParseException {
        return of(dataAddress, dataLength, rowCount, null);
    }

    /**
     * Initializes this cursor for the given column data with delta dictionary support.
     *
     * @param dataAddress    address of column data
     * @param dataLength     available bytes
     * @param rowCount       number of rows
     * @param connectionDict connection-level symbol dictionary (if not null, uses delta mode)
     * @return bytes consumed from dataAddress
     * @throws QwpParseException if parsing fails
     */
    public int of(
            long dataAddress,
            int dataLength,
            int rowCount,
            ObjList<String> connectionDict
    ) throws QwpParseException {
        this.deltaMode = connectionDict != null;
        this.connectionDict = connectionDict;

        long limit = dataAddress + dataLength;
        int offset = 0;

        // Read null bitmap flag
        if (offset >= dataLength) {
            throw QwpParseException.create(
                    QwpParseException.ErrorCode.INSUFFICIENT_DATA,
                    "symbol column data truncated: expected null bitmap flag"
            );
        }
        int nullCount;
        if (Unsafe.getByte(dataAddress + offset) != 0) {
            offset++;
            int bitmapSize = QwpNullBitmap.sizeInBytes(rowCount);
            if (offset + bitmapSize > dataLength) {
                throw QwpParseException.create(
                        QwpParseException.ErrorCode.INSUFFICIENT_DATA,
                        "symbol column data truncated: expected null bitmap"
                );
            }
            this.nullBitmapAddress = dataAddress + offset;
            nullCount = QwpNullBitmap.countNulls(nullBitmapAddress, rowCount);
            offset += bitmapSize;
        } else {
            offset++;
            this.nullBitmapAddress = 0;
            nullCount = 0;
        }

        if (deltaMode) {
            // Delta mode: no per-column dictionary, indices reference connection dictionary
            this.dictionarySize = 0;
        } else {
            // Standard mode: parse per-column dictionary
            // Parse dictionary size
            QwpVarint.decode(dataAddress + offset, limit, decodeResult);
            if (decodeResult.value < 0 || decodeResult.value > Integer.MAX_VALUE) {
                throw QwpParseException.create(
                        QwpParseException.ErrorCode.INSUFFICIENT_DATA,
                        "dictionary size out of int range: " + decodeResult.value
                );
            }
            if (decodeResult.value > dataLength) {
                throw QwpParseException.create(
                        QwpParseException.ErrorCode.INSUFFICIENT_DATA,
                        "dictionary size exceeds data length: " + decodeResult.value
                );
            }
            this.dictionarySize = (int) decodeResult.value;
            offset += decodeResult.bytesRead;

            if (dictionarySize > MAX_SYMBOL_DICTIONARY_SIZE) {
                throw QwpParseException.create(
                        QwpParseException.ErrorCode.INSUFFICIENT_DATA,
                        "symbol dictionary size exceeds limit: " + dictionarySize
                                + " (max " + MAX_SYMBOL_DICTIONARY_SIZE + ')'
                );
            }

            // Parse dictionary entries into flyweights
            ensureDictionaryCapacity(dictionarySize);
            for (int i = 0; i < dictionarySize; i++) {
                QwpVarint.decode(dataAddress + offset, limit, decodeResult);
                offset += decodeResult.bytesRead;

                long availableBytes = (long) dataLength - offset;
                if (offset > dataLength || decodeResult.value < 0 || decodeResult.value > Integer.MAX_VALUE) {
                    throw QwpParseException.create(
                            QwpParseException.ErrorCode.INSUFFICIENT_DATA,
                            "dictionary string length out of int range: " + decodeResult.value
                    );
                }
                if (availableBytes < 0 || decodeResult.value > availableBytes) {
                    throw QwpParseException.create(
                            QwpParseException.ErrorCode.INSUFFICIENT_DATA,
                            "dictionary string length out of bounds: " + decodeResult.value
                    );
                }
                int stringLen = (int) decodeResult.value;

                DirectUtf8String entry = dictionaryUtf8.getQuick(i);
                long strLo = dataAddress + offset;
                entry.of(strLo, strLo + stringLen, Utf8s.isAscii(strLo, stringLen));
                offset += stringLen;
            }
        }

        if (offset < 0 || offset > dataLength) {
            throw QwpParseException.create(
                    QwpParseException.ErrorCode.INSUFFICIENT_DATA,
                    "symbol column data truncated after dictionary parsing"
            );
        }

        // Decode all varint indices in one pass. This avoids re-decoding
        // during advanceRow() and lets us return the total bytes consumed.
        this.valueCount = rowCount - nullCount;
        decodedIndices.clear();
        decodedIndices.setPos(valueCount);
        int dictLimit = deltaMode ? (connectionDict != null ? connectionDict.size() : 0) : dictionarySize;
        long tempAddress = dataAddress + offset;
        for (int i = 0; i < valueCount; i++) {
            QwpVarint.decode(tempAddress, limit, decodeResult);
            if (decodeResult.value < 0 || decodeResult.value > Integer.MAX_VALUE) {
                throw QwpParseException.create(
                        QwpParseException.ErrorCode.INVALID_DICTIONARY_INDEX,
                        "symbol index out of int range: " + decodeResult.value
                );
            }
            int idx = (int) decodeResult.value;
            if (idx >= dictLimit) {
                throw QwpParseException.create(
                        QwpParseException.ErrorCode.INVALID_DICTIONARY_INDEX,
                        "symbol index out of range: " + idx
                );
            }
            decodedIndices.setQuick(i, idx);
            tempAddress += decodeResult.bytesRead;
        }

        resetRowPosition();
        return (int) (tempAddress - dataAddress);
    }

    @Override
    public void resetRowPosition() {
        currentRow = -1;
        currentSymbolIndex = -1;
        currentIsNull = false;
        currentValueIndex = -1;
    }

    private void ensureDictionaryCapacity(int capacity) {
        while (dictionaryUtf8.size() < capacity) {
            dictionaryUtf8.add(new DirectUtf8String());
        }
    }
}
