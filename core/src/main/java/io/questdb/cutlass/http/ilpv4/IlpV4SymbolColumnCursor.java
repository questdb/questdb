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

import io.questdb.std.ObjList;
import io.questdb.std.Unsafe;
import io.questdb.std.str.DirectUtf8Sequence;
import io.questdb.std.str.DirectUtf8String;

import static io.questdb.cutlass.http.ilpv4.IlpV4Constants.TYPE_SYMBOL;

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
public final class IlpV4SymbolColumnCursor implements IlpV4ColumnCursor {

    private final DirectUtf8String nameUtf8 = new DirectUtf8String();
    private final IlpV4Varint.DecodeResult decodeResult = new IlpV4Varint.DecodeResult();

    // Pre-allocated dictionary storage (flyweights pointing to wire memory)
    private final ObjList<DirectUtf8String> dictionaryUtf8 = new ObjList<>();
    // Lazy-populated String cache for dictionary entries
    private final ObjList<String> dictionaryStrings = new ObjList<>();

    // Configuration
    private boolean nullable;
    private int rowCount;
    private int dictionarySize;

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
     * @throws IlpV4ParseException if parsing fails
     */
    public int of(long dataAddress, int dataLength, int rowCount, boolean nullable,
                  long nameAddress, int nameLength) throws IlpV4ParseException {
        this.nullable = nullable;
        this.rowCount = rowCount;
        this.nameUtf8.of(nameAddress, nameAddress + nameLength);

        long limit = dataAddress + dataLength;
        int offset = 0;

        if (nullable) {
            int bitmapSize = IlpV4NullBitmap.sizeInBytes(rowCount);
            this.nullBitmapAddress = dataAddress;
            offset += bitmapSize;
        } else {
            this.nullBitmapAddress = 0;
        }

        // Parse dictionary size
        IlpV4Varint.decode(dataAddress + offset, limit, decodeResult);
        this.dictionarySize = (int) decodeResult.value;
        offset += decodeResult.bytesRead;

        // Parse dictionary entries into flyweights
        ensureDictionaryCapacity(dictionarySize);
        for (int i = 0; i < dictionarySize; i++) {
            IlpV4Varint.decode(dataAddress + offset, limit, decodeResult);
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

        this.indicesAddress = dataAddress + offset;
        this.indicesEnd = limit;

        resetRowPosition();

        // Calculate bytes consumed by scanning indices
        // This is needed because indices are varint-encoded
        int nullCount = nullable ? IlpV4NullBitmap.countNulls(nullBitmapAddress, rowCount) : 0;
        int valueCount = rowCount - nullCount;
        long tempAddress = indicesAddress;
        for (int i = 0; i < valueCount; i++) {
            IlpV4Varint.decode(tempAddress, indicesEnd, decodeResult);
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
    public void advanceRow() throws IlpV4ParseException {
        currentRow++;

        if (nullable && nullBitmapAddress != 0) {
            currentIsNull = IlpV4NullBitmap.isNull(nullBitmapAddress, currentRow);
            if (currentIsNull) {
                currentSymbolIndex = -1;
                return;
            }
        } else {
            currentIsNull = false;
        }

        // Read varint index
        IlpV4Varint.decode(currentIndexAddress, indicesEnd, decodeResult);
        currentSymbolIndex = (int) decodeResult.value;
        currentIndexAddress += decodeResult.bytesRead;
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
     *
     * @return UTF-8 sequence from dictionary, or null if NULL
     */
    public DirectUtf8Sequence getSymbolUtf8() {
        if (currentIsNull || currentSymbolIndex < 0) {
            return null;
        }
        return dictionaryUtf8.getQuick(currentSymbolIndex);
    }

    /**
     * Returns current row's symbol value as a String.
     * <p>
     * <b>Lazy allocation:</b> String is created on first access per dictionary entry,
     * then cached for subsequent accesses.
     *
     * @return String value, or null if NULL
     */
    public String getSymbolString() {
        if (currentIsNull || currentSymbolIndex < 0) {
            return null;
        }
        String cached = dictionaryStrings.getQuick(currentSymbolIndex);
        if (cached == null) {
            cached = dictionaryUtf8.getQuick(currentSymbolIndex).toString();
            dictionaryStrings.setQuick(currentSymbolIndex, cached);
        }
        return cached;
    }

    /**
     * Returns a dictionary entry by index as UTF-8.
     *
     * @param index dictionary index
     * @return UTF-8 sequence
     */
    public DirectUtf8Sequence getDictionaryEntry(int index) {
        return dictionaryUtf8.getQuick(index);
    }
}
