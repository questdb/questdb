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

import io.questdb.std.ObjList;
import io.questdb.std.str.DirectUtf8Sequence;
import io.questdb.std.str.DirectUtf8String;

import static io.questdb.cutlass.qwp.protocol.QwpConstants.TYPE_SYMBOL;

/**
 * Streaming cursor for SYMBOL columns.
 * <p>
 * Wire format:
 * <pre>
 * [null bitmap if nullable]: ceil(rowCount/8) bytes
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

    private final DirectUtf8String nameUtf8 = new DirectUtf8String();
    private final QwpVarint.DecodeResult decodeResult = new QwpVarint.DecodeResult();

    // Pre-allocated dictionary storage (flyweights pointing to wire memory)
    private final ObjList<DirectUtf8String> dictionaryUtf8 = new ObjList<>();
    // Lazy-populated String cache for dictionary entries
    private final ObjList<String> dictionaryStrings = new ObjList<>();

    // Configuration
    private boolean nullable;
    private int rowCount;
    private int dictionarySize;
    private boolean deltaMode;  // When true, use connectionDict instead of per-column dictionary

    // External dictionary reference (for delta mode)
    private ObjList<String> connectionDict;

    // Wire pointers
    private long nullBitmapAddress;
    private long indicesAddress;
    private long indicesEnd;

    // Iteration state
    private int currentRow;
    private int currentSymbolIndex;
    private boolean currentIsNull;
    private long currentIndexAddress;

    /**
     * Initializes this cursor for the given column data.
     *
     * @param dataAddress   address of column data
     * @param dataLength    available bytes
     * @param rowCount      number of rows
     * @param nullable      whether column is nullable
     * @param nameAddress   address of column name UTF-8 bytes
     * @param nameLength    column name length in bytes
     * @return bytes consumed from dataAddress
     * @throws QwpParseException if parsing fails
     */
    public int of(long dataAddress, int dataLength, int rowCount, boolean nullable,
                  long nameAddress, int nameLength) throws QwpParseException {
        return of(dataAddress, dataLength, rowCount, nullable, nameAddress, nameLength, null);
    }

    /**
     * Initializes this cursor for the given column data with delta dictionary support.
     *
     * @param dataAddress    address of column data
     * @param dataLength     available bytes
     * @param rowCount       number of rows
     * @param nullable       whether column is nullable
     * @param nameAddress    address of column name UTF-8 bytes
     * @param nameLength     column name length in bytes
     * @param connectionDict connection-level symbol dictionary (if not null, uses delta mode)
     * @return bytes consumed from dataAddress
     * @throws QwpParseException if parsing fails
     */
    public int of(long dataAddress, int dataLength, int rowCount, boolean nullable,
                  long nameAddress, int nameLength, ObjList<String> connectionDict) throws QwpParseException {
        this.nullable = nullable;
        this.rowCount = rowCount;
        this.nameUtf8.of(nameAddress, nameAddress + nameLength);
        this.deltaMode = connectionDict != null;
        this.connectionDict = connectionDict;

        long limit = dataAddress + dataLength;
        int offset = 0;

        if (nullable) {
            int bitmapSize = QwpNullBitmap.sizeInBytes(rowCount);
            this.nullBitmapAddress = dataAddress;
            offset += bitmapSize;
        } else {
            this.nullBitmapAddress = 0;
        }

        if (deltaMode) {
            // Delta mode: no per-column dictionary, indices reference connection dictionary
            this.dictionarySize = 0;
        } else {
            // Standard mode: parse per-column dictionary
            // Parse dictionary size
            QwpVarint.decode(dataAddress + offset, limit, decodeResult);
            this.dictionarySize = (int) decodeResult.value;
            offset += decodeResult.bytesRead;

            // Parse dictionary entries into flyweights
            ensureDictionaryCapacity(dictionarySize);
            for (int i = 0; i < dictionarySize; i++) {
                QwpVarint.decode(dataAddress + offset, limit, decodeResult);
                int stringLen = (int) decodeResult.value;
                offset += decodeResult.bytesRead;

                DirectUtf8String entry = dictionaryUtf8.getQuick(i);
                entry.of(dataAddress + offset, dataAddress + offset + stringLen);
                // Clear cached String for this entry
                if (i < dictionaryStrings.size()) {
                    dictionaryStrings.setQuick(i, null);
                }
                offset += stringLen;
            }
        }

        this.indicesAddress = dataAddress + offset;
        this.indicesEnd = limit;

        resetRowPosition();

        // Calculate bytes consumed by scanning indices
        // This is needed because indices are varint-encoded
        int nullCount = nullable ? QwpNullBitmap.countNulls(nullBitmapAddress, rowCount) : 0;
        int valueCount = rowCount - nullCount;
        long tempAddress = indicesAddress;
        for (int i = 0; i < valueCount; i++) {
            QwpVarint.decode(tempAddress, indicesEnd, decodeResult);
            tempAddress += decodeResult.bytesRead;
        }

        return (int) (tempAddress - dataAddress);
    }

    private void ensureDictionaryCapacity(int capacity) {
        while (dictionaryUtf8.size() < capacity) {
            dictionaryUtf8.add(new DirectUtf8String());
        }
        while (dictionaryStrings.size() < capacity) {
            dictionaryStrings.add(null);
        }
    }

    @Override
    public DirectUtf8Sequence getNameUtf8() {
        return nameUtf8;
    }

    @Override
    public byte getTypeCode() {
        return TYPE_SYMBOL;
    }

    @Override
    public boolean isNullable() {
        return nullable;
    }

    @Override
    public boolean isNull() {
        return currentIsNull;
    }

    @Override
    public boolean advanceRow() throws QwpParseException {
        currentRow++;

        if (nullable && nullBitmapAddress != 0) {
            currentIsNull = QwpNullBitmap.isNull(nullBitmapAddress, currentRow);
            if (currentIsNull) {
                currentSymbolIndex = -1;
                return true;
            }
        } else {
            currentIsNull = false;
        }

        // Read varint index
        QwpVarint.decode(currentIndexAddress, indicesEnd, decodeResult);
        currentSymbolIndex = (int) decodeResult.value;
        currentIndexAddress += decodeResult.bytesRead;
        return false;
    }

    @Override
    public int getCurrentRow() {
        return currentRow;
    }

    @Override
    public void resetRowPosition() {
        currentRow = -1;
        currentSymbolIndex = -1;
        currentIsNull = false;
        currentIndexAddress = indicesAddress;
    }

    @Override
    public void clear() {
        nameUtf8.clear();
        nullable = false;
        rowCount = 0;
        dictionarySize = 0;
        deltaMode = false;
        connectionDict = null;
        nullBitmapAddress = 0;
        indicesAddress = 0;
        indicesEnd = 0;
        // Clear dictionary flyweights
        for (int i = 0; i < dictionaryUtf8.size(); i++) {
            dictionaryUtf8.getQuick(i).clear();
        }
        // Clear cached strings
        for (int i = 0; i < dictionaryStrings.size(); i++) {
            dictionaryStrings.setQuick(i, null);
        }
        resetRowPosition();
    }

    /**
     * Returns the dictionary size.
     */
    public int getDictionarySize() {
        return dictionarySize;
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
     * <b>Note:</b> Returns null in delta mode (use {@link #getSymbolString()} instead).
     *
     * @return UTF-8 sequence from dictionary, or null if NULL or delta mode
     */
    public DirectUtf8Sequence getSymbolUtf8() {
        if (currentIsNull || currentSymbolIndex < 0 || deltaMode) {
            return null;
        }
        return dictionaryUtf8.getQuick(currentSymbolIndex);
    }

    /**
     * Returns current row's symbol value as a String.
     * <p>
     * <b>Lazy allocation:</b> String is created on first access per dictionary entry,
     * then cached for subsequent accesses.
     * <p>
     * In delta mode, retrieves from the connection-level dictionary.
     *
     * @return String value, or null if NULL
     */
    public String getSymbolString() {
        if (currentIsNull || currentSymbolIndex < 0) {
            return null;
        }
        if (deltaMode) {
            // In delta mode, use connection dictionary
            if (connectionDict != null && currentSymbolIndex < connectionDict.size()) {
                return connectionDict.getQuick(currentSymbolIndex);
            }
            return null;
        }
        // Standard mode: use per-column dictionary
        String cached = dictionaryStrings.getQuick(currentSymbolIndex);
        if (cached == null) {
            cached = dictionaryUtf8.getQuick(currentSymbolIndex).toString();
            dictionaryStrings.setQuick(currentSymbolIndex, cached);
        }
        return cached;
    }

    /**
     * Returns whether this cursor is using delta mode (connection dictionary).
     *
     * @return true if delta mode is enabled
     */
    public boolean isDeltaMode() {
        return deltaMode;
    }

    /**
     * Returns a dictionary entry by index as UTF-8.
     *
     * @param index dictionary index
     * @return UTF-8 sequence, or null if delta mode
     */
    public DirectUtf8Sequence getDictionaryEntry(int index) {
        if (deltaMode) {
            return null;
        }
        return dictionaryUtf8.getQuick(index);
    }
}
