/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2020 QuestDB
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

package io.questdb.cutlass.line.tcp;

import io.questdb.griffin.SqlKeywords;
import io.questdb.std.Numbers;
import io.questdb.std.NumericException;
import io.questdb.std.ObjList;
import io.questdb.std.Unsafe;
import io.questdb.std.str.DirectByteCharSequence;

import java.io.Closeable;

public class NewLineProtoParser implements Closeable {
    public static final long NULL_TIMESTAMP = Numbers.LONG_NaN;
    public static final int MAX_ALLOWED_STRING_LEN = 152; // 150 bytes plus quotes
    public static final byte ENTITY_TYPE_NULL = 0;
    public static final byte ENTITY_TYPE_TAG = 1;
    public static final byte ENTITY_TYPE_FLOAT = 2;
    public static final byte ENTITY_TYPE_INTEGER = 3;
    public static final byte ENTITY_TYPE_STRING = 4;
    public static final byte ENTITY_TYPE_SYMBOL = 5;
    public static final byte ENTITY_TYPE_BOOLEAN = 6;
    public static final byte ENTITY_TYPE_LONG256 = 7;
    public static final byte ENTITY_TYPE_CACHED_TAG = 8;
    public static final byte ENTITY_TYPE_GEOBYTE = 9;
    public static final byte ENTITY_TYPE_GEOSHORT = 10;
    public static final byte ENTITY_TYPE_GEOINT = 11;
    public static final byte ENTITY_TYPE_GEOLONG = 12;
    public static final int N_ENTITY_TYPES = ENTITY_TYPE_GEOLONG + 1;
    static final byte ENTITY_TYPE_NONE = (byte) 0xff; // visible for testing
    private final DirectByteCharSequence measurementName = new DirectByteCharSequence();
    private final DirectByteCharSequence charSeq = new DirectByteCharSequence();
    private final ObjList<ProtoEntity> entityCache = new ObjList<>();
    private final EntityHandler entityEndOfLineHandler = this::expectEndOfLine;
    private long bufAt;
    private long entityLo;
    private boolean tagsComplete;
    private boolean isQuotedValue;
    private int nEscapedChars;
    private int nEntities;
    private ProtoEntity currentEntity;
    private ErrorCode errorCode;
    private EntityHandler entityHandler;
    private final EntityHandler entityTableHandler = this::expectTableName;
    private long timestamp;
    private final EntityHandler entityTimestampHandler = this::expectTimestamp;
    private final EntityHandler entityValueHandler = this::expectEntityValue;
    private final EntityHandler entityNameHandler = this::expectEntityName;

    @Override
    public void close() {
    }

    public long getBufferAddress() {
        return bufAt;
    }

    public ProtoEntity getEntity(int n) {
        assert n < nEntities;
        return entityCache.get(n);
    }

    public ErrorCode getErrorCode() {
        return errorCode;
    }

    public DirectByteCharSequence getMeasurementName() {
        return measurementName;
    }

    public long getTimestamp() {
        return timestamp;
    }

    public int getnEntities() {
        return nEntities;
    }

    public boolean hasTimestamp() {
        return timestamp != NULL_TIMESTAMP;
    }

    public NewLineProtoParser of(long bufLo) {
        this.bufAt = bufLo - 1;
        startNextMeasurement();
        return this;
    }

    /**
     * Structure of an ilp message:
     * <pre>
     *     table[(,sym=s)*] fld=f(,fld=f)*[ timestamp_nano]\n
     * </pre>
     * where:
     * <ul>
     *     <li>table: name of the table to insert into.</li>
     *     <li>sym: name of a symbol column.</li>
     *     <li>s: symbol value (of type symbol, i.e. string without quotes).</li>
     *     <li>fld: name of a column.</li>
     *     <li>f: value for the column. The type of these is parsed based on the
     *     content of the token (e.g. with prefix '0x' and suffix 'i' it is assumed
     *     a long256).
     *     </li>
     *     <li>timestamp_nano: a long representing the epoch with nano precision.
     *     Optional, when missing it becomes the system's current time.</li>
     * </ul>
     * A white space in the line (other than if inside of a string, or escaped)
     * triggers the transition from expecting symbols, to columns to timestamp.<br/>
     * The '\n' is compulsory, it delimits the line.<br/>
     * There can be 0..n symbols, and 1..m columns. Columns support all types.
     *
     * @param bufHi start of the line
     * @return parse status result
     */
    public ParseResult parseMeasurement(long bufHi) {
        assert bufAt != 0 && bufHi >= bufAt;
        while (bufAt < bufHi) {
            byte b = Unsafe.getUnsafe().getByte(bufAt);
            boolean endOfLine = false;
            boolean appendByte = false;
            switch (b) {
                case (byte) '\n': // before '\n' you can find: field value or timestamp
                case (byte) '\r':
                    endOfLine = true;
                    b = '\n';
                case (byte) '=':
                case (byte) ',':
                case (byte) ' ':
                    isQuotedValue = false;
                    if (!entityHandler.completeEntity(b, bufHi)) {
                        if (errorCode == ErrorCode.EMPTY_LINE) {
                            // An empty line
                            bufAt++;
                            entityLo = bufAt;
                            break;
                        }
                        return ParseResult.ERROR;
                    }
                    if (endOfLine) {
                        if (nEntities > 0) {
                            entityHandler = entityEndOfLineHandler;
                            return ParseResult.MEASUREMENT_COMPLETE;
                        }
                        errorCode = ErrorCode.NO_FIELDS;
                        return ParseResult.ERROR;
                    }
                    bufAt++;
                    if (!isQuotedValue) {
                        nEscapedChars = 0;
                        entityLo = bufAt;
                    }
                    break;

                case (byte) '\\':
                    if ((bufAt + 1) >= bufHi) {
                        return ParseResult.BUFFER_UNDERFLOW;
                    }
                    nEscapedChars++;
                    bufAt++;
                    b = Unsafe.getUnsafe().getByte(bufAt);

                default:
                    appendByte = true;
                    break;
            }

            if (appendByte) {
                if (nEscapedChars > 0) {
                    Unsafe.getUnsafe().putByte(bufAt - nEscapedChars, b);
                }
                bufAt++;
            }
        }
        return ParseResult.BUFFER_UNDERFLOW;
    }

    public void shl(long shl) {
        bufAt -= shl;
        entityLo -= shl;
        measurementName.shl(shl);
        charSeq.shl(shl);
        for (int i = 0; i < nEntities; i++) {
            entityCache.getQuick(i).shl(shl);
        }
    }

    public ParseResult skipMeasurement(long bufHi) {
        assert bufAt != 0 && bufHi >= bufAt;
        while (bufAt < bufHi) {
            byte b = Unsafe.getUnsafe().getByte(bufAt);
            if (b == (byte) '\n' || b == (byte) '\r') {
                return ParseResult.MEASUREMENT_COMPLETE;
            }
            bufAt++;
        }
        return ParseResult.BUFFER_UNDERFLOW;
    }

    public void startNextMeasurement() {
        bufAt++;
        nEscapedChars = 0;
        entityLo = bufAt;
        errorCode = null;
        tagsComplete = false;
        isQuotedValue = false;
        nEntities = 0;
        currentEntity = null;
        entityHandler = entityTableHandler;
        timestamp = NULL_TIMESTAMP;
    }

    private boolean expectEndOfLine(byte endOfEntityByte, long bufHi) {
        assert endOfEntityByte == '\n';
        return true;
    }

    private boolean expectEntityName(byte endOfEntityByte, long bufHi) {
        if (endOfEntityByte == (byte) '=') {
            if (entityCache.size() <= nEntities) {
                currentEntity = new ProtoEntity();
                entityCache.add(currentEntity);
            } else {
                currentEntity = entityCache.get(nEntities);
                currentEntity.clear();
            }

            nEntities++;
            currentEntity.setName();
            entityHandler = entityValueHandler;

            if (tagsComplete) {
                if (bufAt + 1 < bufHi) { // peek oncoming value's 1st char
                    byte b = Unsafe.getUnsafe().getByte(bufAt + 1);
                    if (b == (byte) '"') {
                        // it is a string, get it ready for immediate consumption
                        entityLo = bufAt + 1; // from the quote
                        bufAt += 2;
                        boolean scape = false;
                        final long limit = Math.min(bufHi, entityLo + MAX_ALLOWED_STRING_LEN + 1); // limit the size of strings
                        while (bufAt < limit) { // consume until the next quote, '\n', or eof
                            b = Unsafe.getUnsafe().getByte(bufAt);
                            if (b != (byte) '\\') {
                                if (b == (byte) '"' && !scape) {
                                    isQuotedValue = true;
                                    return true;
                                }
                                if (b == (byte) '\n' && !scape) {
                                    errorCode = ErrorCode.INVALID_FIELD_VALUE;
                                    return false; // missing tail quote
                                }
                                scape = false;
                            } else {
                                scape = true;
                            }
                            bufAt++;
                        }
                        errorCode = ErrorCode.INVALID_FIELD_VALUE;
                        return false; // missing tail quote as the string extends past the max allowed size
                    }
                }
            }
            return true;
        }

        boolean emptyEntity = bufAt == entityLo;
        if (emptyEntity) {
            if (endOfEntityByte == (byte) ' ') {
                if (tagsComplete) {
                    entityHandler = entityTimestampHandler;
                } else {
                    tagsComplete = true;
                }
                return true;
            }

            if (endOfEntityByte == (byte) '\n') {
                return true;
            }
        }

        if (tagsComplete) {
            errorCode = ErrorCode.INCOMPLETE_FIELD;
        } else {
            errorCode = ErrorCode.INCOMPLETE_TAG;
        }
        return false;
    }

    private boolean expectEntityValue(byte endOfEntityByte, long bufHi) {
        boolean endOfSet = endOfEntityByte == (byte) ' ';
        if (endOfSet || endOfEntityByte == (byte) ',' || endOfEntityByte == (byte) '\n') {
            if (currentEntity.setValue()) {
                if (endOfSet) {
                    if (tagsComplete) {
                        entityHandler = entityTimestampHandler;
                    } else {
                        entityHandler = entityNameHandler;
                        tagsComplete = true;
                    }
                } else {
                    entityHandler = entityNameHandler;
                }
                return true;
            }

            errorCode = ErrorCode.INVALID_FIELD_VALUE;
            return false;
        }

        errorCode = ErrorCode.INVALID_FIELD_SEPARATOR;
        return false;
    }

    private boolean expectTableName(byte endOfEntityByte, long bufHi) {
        tagsComplete = endOfEntityByte == (byte) ' ';
        if (endOfEntityByte == (byte) ',' || tagsComplete) {
            measurementName.of(entityLo, bufAt);
            entityHandler = entityNameHandler;
            return true;
        }

        if (entityLo == bufAt) {
            errorCode = ErrorCode.EMPTY_LINE;
        } else {
            errorCode = ErrorCode.NO_FIELDS;
        }
        return false;
    }

    private boolean expectTimestamp(byte endOfEntityByte, long bufHi) {
        try {
            if (endOfEntityByte == (byte) '\n') {
                timestamp = Numbers.parseLong(charSeq.of(entityLo, bufAt - nEscapedChars));
                entityHandler = null;
                return true;
            }
            errorCode = ErrorCode.INVALID_FIELD_SEPARATOR;
            return false;
        } catch (NumericException ex) {
            errorCode = ErrorCode.INVALID_TIMESTAMP;
            return false;
        }
    }

    public enum ParseResult {
        MEASUREMENT_COMPLETE, BUFFER_UNDERFLOW, ERROR
    }

    public enum ErrorCode {
        EMPTY_LINE,
        NO_FIELDS,
        INCOMPLETE_TAG,
        INCOMPLETE_FIELD,
        INVALID_FIELD_SEPARATOR,
        INVALID_TIMESTAMP,
        INVALID_FIELD_VALUE
    }

    private interface EntityHandler {
        boolean completeEntity(byte endOfEntityByte, long bufHi);
    }

    public class ProtoEntity {
        private final DirectByteCharSequence name = new DirectByteCharSequence();
        private final DirectByteCharSequence value = new DirectByteCharSequence();
        private byte type = ENTITY_TYPE_NONE;
        private long integerValue;
        private boolean booleanValue;
        private double floatValue;

        public boolean getBooleanValue() {
            return booleanValue;
        }

        public double getFloatValue() {
            return floatValue;
        }

        public long getIntegerValue() {
            return integerValue;
        }

        public DirectByteCharSequence getName() {
            return name;
        }

        public byte getType() {
            return type;
        }

        public DirectByteCharSequence getValue() {
            return value;
        }

        public void shl(long shl) {
            name.shl(shl);
            value.shl(shl);
        }

        private void clear() {
            type = ENTITY_TYPE_NONE;
        }

        private boolean parse(byte last, int valueLen) {
            switch (last) {
                case 'i':
                    if (valueLen > 3 && value.charAt(0) == '0' && (value.charAt(1) | 32) == 'x') {
                        value.decHi(); // remove 'i'
                        type = ENTITY_TYPE_LONG256;
                        return true;
                    }
                    if (valueLen > 1 && value.charAt(1) != 'x') {
                        try {
                            charSeq.of(value.getLo(), value.getHi() - 1);
                            integerValue = Numbers.parseLong(charSeq);
                            value.decHi(); // remove 'i'
                            type = ENTITY_TYPE_INTEGER;
                        } catch (NumericException notANumber) {
                            type = ENTITY_TYPE_SYMBOL;
                        }
                        return true;
                    }
                    type = ENTITY_TYPE_SYMBOL;
                    return true;
                case 'e':
                case 'E':
                    // tru(e)
                    // fals(e)
                case 't':
                case 'T':
                case 'f':
                case 'F':
                    // f
                    // F
                    // t
                    // T
                    if (valueLen == 1) {
                        if (last != 'e') {
                            booleanValue = (last | 32) == 't';
                            type = ENTITY_TYPE_BOOLEAN;
                        } else {
                            type = ENTITY_TYPE_SYMBOL;
                        }
                    } else {
                        charSeq.of(value.getLo(), value.getHi());
                        if (SqlKeywords.isTrueKeyword(charSeq)) {
                            booleanValue = true;
                            type = ENTITY_TYPE_BOOLEAN;
                        } else if (SqlKeywords.isFalseKeyword(charSeq)) {
                            booleanValue = false;
                            type = ENTITY_TYPE_BOOLEAN;
                        } else {
                            type = ENTITY_TYPE_SYMBOL;
                        }
                    }
                    return true;
                case '"': {
                    byte b = value.byteAt(0);
                    if (valueLen > 1 && b == '"') {
                        value.squeeze();
                        type = ENTITY_TYPE_STRING;
                        return true;
                    }
                    return false;
                }
                default:
                    try {
                        floatValue = Numbers.parseDouble(value);
                        type = ENTITY_TYPE_FLOAT;
                    } catch (NumericException ex) {
                        if (value.byteAt(0) == '"') { // missing closing '"'
                            return false;
                        }
                        type = ENTITY_TYPE_SYMBOL;
                    }
                    return true;
            }
        }

        private void setName() {
            name.of(entityLo, bufAt - nEscapedChars);
        }

        private boolean setValue() {
            assert type == ENTITY_TYPE_NONE;
            long bufHi = bufAt - nEscapedChars;
            int valueLen = (int) (bufHi - entityLo);
            value.of(entityLo, bufHi);
            if (tagsComplete) {
                if (valueLen > 0) {
                    byte lastByte = value.byteAt(valueLen - 1);
                    return parse(lastByte, valueLen);
                } else {
                    type = ENTITY_TYPE_NULL;
                    return true;
                }
            }
            type = ENTITY_TYPE_TAG;
            return true;
        }
    }
}
