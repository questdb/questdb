/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2022 QuestDB
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
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.std.Numbers;
import io.questdb.std.NumericException;
import io.questdb.std.ObjList;
import io.questdb.std.Unsafe;
import io.questdb.std.str.DirectByteCharSequence;

public class LineTcpParser {

    public static final long NULL_TIMESTAMP = Numbers.LONG_NaN;
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
    public static final byte ENTITY_TYPE_TIMESTAMP = 13;
    public static final int N_ENTITY_TYPES = ENTITY_TYPE_TIMESTAMP + 1;
    public static final byte ENTITY_TYPE_LONG = 14;
    public static final byte ENTITY_TYPE_DOUBLE = 15;
    public static final byte ENTITY_TYPE_SHORT = 16;
    public static final byte ENTITY_TYPE_BYTE = 17;
    public static final byte ENTITY_TYPE_DATE = 18;
    public static final byte ENTITY_TYPE_CHAR = 19;
    public static final int N_MAPPED_ENTITY_TYPES = ENTITY_TYPE_CHAR + 1;
    static final byte ENTITY_TYPE_NONE = (byte) 0xff; // visible for testing

    private static final byte ENTITY_HANDLER_TABLE = 0;
    private static final byte ENTITY_HANDLER_NAME = 1;
    private static final byte ENTITY_HANDLER_VALUE = 2;
    private static final byte ENTITY_HANDLER_TIMESTAMP = 3;
    private static final byte ENTITY_HANDLER_NEW_LINE = 4;
    private static final Log LOG = LogFactory.getLog(LineTcpParser.class);
    private static final boolean[] controlChars;

    private final DirectByteCharSequence measurementName = new DirectByteCharSequence();
    private final DirectByteCharSequence charSeq = new DirectByteCharSequence();
    private final ObjList<ProtoEntity> entityCache = new ObjList<>();
    private final boolean stringAsTagSupported;
    private final boolean symbolAsFieldSupported;
    private long bufAt;
    private long entityLo;
    private boolean tagsComplete;
    private boolean tagStartsWithQuote;
    private int nEscapedChars;
    private boolean isQuotedFieldValue;
    private int nEntities;
    private ProtoEntity currentEntity;
    private ErrorCode errorCode;
    private byte entityHandler = -1;
    private long timestamp;
    private int nQuoteCharacters;
    private boolean scape;
    private boolean nextValueCanBeOpenQuote;
    private boolean hasNonAscii;

    public LineTcpParser(boolean stringAsTagSupported, boolean symbolAsFieldSupported) {
        this.stringAsTagSupported = stringAsTagSupported;
        this.symbolAsFieldSupported = symbolAsFieldSupported;
    }

    public long getBufferAddress() {
        return bufAt;
    }

    public ProtoEntity getEntity(int n) {
        assert n < nEntities;
        return entityCache.get(n);
    }

    public int getEntityCount() {
        return nEntities;
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

    public boolean hasNonAsciiChars() {
        return hasNonAscii;
    }

    public boolean hasTimestamp() {
        return timestamp != NULL_TIMESTAMP;
    }

    public LineTcpParser of(long bufLo) {
        this.bufAt = bufLo - 1;
        startNextMeasurement();
        return this;
    }

    public ParseResult parseMeasurement(long bufHi) {
        assert bufAt != 0 && bufHi >= bufAt;
        // We can resume from random place of the line message
        // the class member variables should resume byte by byte parsing from the last place
        // processing stopped.
        if (nQuoteCharacters == 1 && tagsComplete && entityHandler == ENTITY_HANDLER_VALUE) {
            // when nQuoteCharacters it means that parsing of quoted value has started.
            // continue parsing quoted value
            if (!prepareQuotedEntity(entityLo, bufHi)) {
                // quoted value parsing did not reach the end
                if (errorCode == ErrorCode.INVALID_FIELD_VALUE_STR_UNDERFLOW) {
                    // because buffer exhausted
                    return ParseResult.BUFFER_UNDERFLOW;
                }
                // because it reached EOL or another error
                return ParseResult.ERROR;
            }
            nQuoteCharacters = 0;
            bufAt++;
        }

        // Main parsing loop
        while (bufAt < bufHi) {
            // take the byte
            byte b = Unsafe.getUnsafe().getByte(bufAt);

            if (nEscapedChars == 0 && b >= 0 && !controlChars[b]) {
                // hot path
                nextValueCanBeOpenQuote = false;
                bufAt++;
                continue;
            }

            // slow path
            hasNonAscii |= b < 0;
            boolean endOfLine = false;
            boolean appendByte = false;
            // Important note: don't forget to update controlChars array when changing the following switch.
            switch (b) {
                case '\n':
                case '\r':
                    endOfLine = true;
                    b = '\n';
                case '=':
                case ',':
                case ' ':
                    isQuotedFieldValue = false;
                    if (!completeEntity(b, bufHi)) {
                        // parse of key or value is unsuccessful
                        if (errorCode == ErrorCode.EMPTY_LINE) {
                            // An empty line
                            bufAt++;
                            entityLo = bufAt;
                            break;
                        }
                        if (errorCode == ErrorCode.INVALID_FIELD_VALUE_STR_UNDERFLOW) {
                            return ParseResult.BUFFER_UNDERFLOW;
                        }
                        return ParseResult.ERROR;
                    }
                    if (endOfLine) {
                        // EOL reached, time to return
                        if (nEntities > 0) {
                            entityHandler = ENTITY_HANDLER_NEW_LINE;
                            return ParseResult.MEASUREMENT_COMPLETE;
                        }
                        errorCode = ErrorCode.NO_FIELDS;
                        return ParseResult.ERROR;
                    }
                    // skip the separator
                    bufAt++;
                    if (!isQuotedFieldValue) {
                        // reset few indicators
                        nEscapedChars = 0;
                        // start next value from here
                        entityLo = bufAt;
                    }
                    break;

                case '\\':
                    // escape next character
                    // look forward, skip the slash
                    if ((bufAt + 1) >= bufHi) {
                        return ParseResult.BUFFER_UNDERFLOW;
                    }
                    nEscapedChars++;
                    bufAt++;
                    b = Unsafe.getUnsafe().getByte(bufAt);
                    if (b == '\\' && (entityHandler != ENTITY_HANDLER_VALUE)) {
                        return getError();
                    }
                    hasNonAscii |= b < 0;
                    appendByte = true;
                    break;

                case '"':
                    if (nextValueCanBeOpenQuote && ++nQuoteCharacters == 1) {
                        // This means that the processing resumed from "
                        // and it's allowed to start quoted value at this point
                        bufAt += 1;
                        // parse quoted value
                        if (!prepareQuotedEntity(bufAt - 1, bufHi)) {
                            // parsing not successful
                            if (errorCode == ErrorCode.INVALID_FIELD_VALUE_STR_UNDERFLOW) {
                                // need more data
                                return ParseResult.BUFFER_UNDERFLOW;
                            }
                            // invalid character sequence in the quoted value or EOL found
                            return ParseResult.ERROR;
                        }
                        errorCode = ErrorCode.NONE;
                        nQuoteCharacters = 0;
                        bufAt += 1;
                        break;
                    } else if (isQuotedFieldValue) {
                        return getError();
                    } else if (entityLo == bufAt) {
                        tagStartsWithQuote = true;
                    }

                default:
                    appendByte = true;
                    nextValueCanBeOpenQuote = false;
                    break;

                case '\0':
                    LOG.info().$("could not parse [byte=\\0]").$();
                    return getError();
                case '/':
                    if (entityHandler != ENTITY_HANDLER_VALUE) {
                        LOG.info().$("could not parse [byte=/]").$();
                        return getError();
                    }
                    appendByte = true;
                    nextValueCanBeOpenQuote = false;
                    break;
            }

            if (appendByte) {
                // If there is escaped character, like \" or \\ then the escape slash has to be excluded
                // from the result key / value.
                // shift copy current byte back
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
        isQuotedFieldValue = false;
        entityLo = bufAt;
        tagsComplete = false;
        tagStartsWithQuote = false;
        nEntities = 0;
        currentEntity = null;
        entityHandler = ENTITY_HANDLER_TABLE;
        timestamp = NULL_TIMESTAMP;
        errorCode = ErrorCode.NONE;
        nQuoteCharacters = 0;
        scape = false;
        nextValueCanBeOpenQuote = false;
        hasNonAscii = false;
    }

    private boolean completeEntity(byte endOfEntityByte, long bufHi) {
        switch (entityHandler) {
            case ENTITY_HANDLER_TABLE:
                return expectTableName(endOfEntityByte);
            case ENTITY_HANDLER_NAME:
                return expectEntityName(endOfEntityByte, bufHi);
            case ENTITY_HANDLER_VALUE:
                return expectEntityValue(endOfEntityByte);
            case ENTITY_HANDLER_TIMESTAMP:
                return expectTimestamp(endOfEntityByte);
            case ENTITY_HANDLER_NEW_LINE:
                return expectEndOfLine(endOfEntityByte);
        }
        return false;
    }

    private boolean expectEndOfLine(byte endOfEntityByte) {
        assert endOfEntityByte == '\n';
        return true;
    }

    private boolean expectEntityName(byte endOfEntityByte, long bufHi) {
        if (endOfEntityByte == (byte) '=') {
            if (bufAt - entityLo - nEscapedChars == 0) { // no tag/field name
                errorCode = tagsComplete ? ErrorCode.INCOMPLETE_FIELD : ErrorCode.INCOMPLETE_TAG;
                return false;
            }

            if (entityCache.size() <= nEntities) {
                currentEntity = new ProtoEntity();
                entityCache.add(currentEntity);
            } else {
                currentEntity = entityCache.get(nEntities);
                currentEntity.clear();
            }

            nEntities++;
            currentEntity.setName();
            entityHandler = ENTITY_HANDLER_VALUE;
            if (tagsComplete) {
                if (bufAt + 3 < bufHi) { // peek oncoming value's 1st byte, only caring for valid strings (2 quotes plus a follow-up byte)
                    long candidateQuoteIdx = bufAt + 1;
                    byte b = Unsafe.getUnsafe().getByte(candidateQuoteIdx);
                    if (b == (byte) '"') {
                        nEscapedChars = 0;
                        nQuoteCharacters++;
                        bufAt += 2;
                        return prepareQuotedEntity(candidateQuoteIdx, bufHi);// go to first byte of the string, past the '"'
                    } else {
                        nextValueCanBeOpenQuote = false;
                    }
                } else {
                    nextValueCanBeOpenQuote = true;
                }
            }
            return true;
        }

        boolean emptyEntity = bufAt == entityLo;
        if (emptyEntity) {
            if (endOfEntityByte == (byte) ' ') {
                if (tagsComplete) {
                    entityHandler = ENTITY_HANDLER_TIMESTAMP;
                } else {
                    tagsComplete = true;
                }
                return true;
            }

            if (endOfEntityByte == (byte) '\n') {
                return true;
            }
        } else if (tagsComplete && (endOfEntityByte == '\n' || endOfEntityByte == '\r')) {
            if (currentEntity != null && currentEntity.getType() == ENTITY_TYPE_TAG) {
                // One token after last tag, and no fields
                // This must be the timestamp
                return expectTimestamp(endOfEntityByte);
            }
        }

        if (tagsComplete) {
            errorCode = ErrorCode.INCOMPLETE_FIELD;
        } else {
            errorCode = ErrorCode.INCOMPLETE_TAG;
        }
        return false;
    }

    private boolean expectEntityValue(byte endOfEntityByte) {
        boolean endOfSet = endOfEntityByte == (byte) ' ';
        if (endOfSet || endOfEntityByte == (byte) ',' || endOfEntityByte == (byte) '\n') {
            if (currentEntity.setValue()) {
                if (endOfSet) {
                    if (tagsComplete) {
                        entityHandler = ENTITY_HANDLER_TIMESTAMP;
                    } else {
                        entityHandler = ENTITY_HANDLER_NAME;
                        tagsComplete = true;
                    }
                } else {
                    entityHandler = ENTITY_HANDLER_NAME;
                }
                return true;
            }

            errorCode = tagsComplete ? ErrorCode.INVALID_FIELD_VALUE : ErrorCode.INVALID_TAG_VALUE;
            return false;
        }

        errorCode = ErrorCode.INVALID_FIELD_SEPARATOR;
        return false;
    }

    private boolean expectTableName(byte endOfEntityByte) {
        tagsComplete = endOfEntityByte == (byte) ' ';
        if (endOfEntityByte == (byte) ',' || tagsComplete) {
            long hi = bufAt - nEscapedChars;
            measurementName.of(entityLo, hi);
            entityHandler = ENTITY_HANDLER_NAME;
            return true;
        }

        if (entityLo == bufAt) {
            errorCode = ErrorCode.EMPTY_LINE;
        } else {
            errorCode = ErrorCode.NO_FIELDS;
        }
        return false;
    }

    private boolean expectTimestamp(byte endOfEntityByte) {
        try {
            if (endOfEntityByte == (byte) '\n') {
                if (entityLo < bufAt - nEscapedChars) {
                    timestamp = Numbers.parseLong(charSeq.of(entityLo, bufAt - nEscapedChars));
                }
                entityHandler = -1;
                return true;
            }
            errorCode = ErrorCode.INVALID_FIELD_SEPARATOR;
            return false;
        } catch (NumericException ex) {
            errorCode = ErrorCode.INVALID_TIMESTAMP;
            return false;
        }
    }

    private ParseResult getError() {
        switch (entityHandler) {
            case ENTITY_HANDLER_NAME:
                errorCode = ErrorCode.INVALID_COLUMN_NAME;
                break;
            case ENTITY_HANDLER_TABLE:
                errorCode = ErrorCode.INVALID_TABLE_NAME;
                break;
            case ENTITY_HANDLER_VALUE:
                errorCode = ErrorCode.INVALID_FIELD_VALUE;
                break;
        }
        return ParseResult.ERROR;
    }

    private boolean prepareQuotedEntity(long openQuoteIdx, long bufHi) {
        // the byte at openQuoteIdx (bufAt + 1) is '"', from here it can only be
        // the start of a string value. Get it ready for immediate consumption by
        // the next completeEntity call, moving butAt to the next '"'
        entityLo = openQuoteIdx; // from the quote
        boolean copyByte;
        while (bufAt < bufHi) { // consume until the next quote, '\n', or eof
            byte b = Unsafe.getUnsafe().getByte(bufAt);
            copyByte = true;
            hasNonAscii |= b < 0;
            switch (b) {
                case (byte) '\\':
                    if (!scape) {
                        nEscapedChars++;
                        copyByte = false;
                    }
                    scape = !scape;
                    break;
                case (byte) '"':
                    if (!scape) {
                        isQuotedFieldValue = true;
                        nQuoteCharacters--;
                        if (nEscapedChars > 0) {
                            Unsafe.getUnsafe().putByte(bufAt - nEscapedChars, b);
                        }
                        return true;
                    }
                    scape = false;
                    break;
                case (byte) '\n':
                    if (!scape) {
                        errorCode = ErrorCode.INVALID_FIELD_VALUE;
                        return false; // missing tail quote
                    }
                    scape = false;
                    break;
                default:
                    scape = false;
                    break;
            }
            nextValueCanBeOpenQuote = false;
            if (copyByte && nEscapedChars > 0) {
                Unsafe.getUnsafe().putByte(bufAt - nEscapedChars, b);
            }
            bufAt++;
        }
        errorCode = ErrorCode.INVALID_FIELD_VALUE_STR_UNDERFLOW;
        return false; // missing tail quote as the string extends past the max allowed size
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
        INVALID_TAG_VALUE,
        INVALID_FIELD_VALUE,
        INVALID_FIELD_VALUE_STR_UNDERFLOW,
        INVALID_TABLE_NAME,
        INVALID_COLUMN_NAME,
        NONE
    }

    public class ProtoEntity {
        private final DirectByteCharSequence name = new DirectByteCharSequence();
        private final DirectByteCharSequence value = new DirectByteCharSequence();
        private byte type = ENTITY_TYPE_NONE;
        private long longValue;
        private boolean booleanValue;
        private double floatValue;

        public boolean getBooleanValue() {
            return booleanValue;
        }

        public double getFloatValue() {
            return floatValue;
        }

        public long getLongValue() {
            return longValue;
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
                    if (valueLen > 1 && value.charAt(1) != 'x') {
                        return parseLong(ENTITY_TYPE_INTEGER);
                    }
                    if (valueLen > 3 && value.charAt(0) == '0' && (value.charAt(1) | 32) == 'x') {
                        value.decHi(); // remove 'i'
                        type = ENTITY_TYPE_LONG256;
                        return true;
                    }
                    type = ENTITY_TYPE_SYMBOL;
                    return true;
                case 't':
                    if (valueLen > 1) {
                        return parseLong(ENTITY_TYPE_TIMESTAMP);
                    }
                    // fall through
                case 'T':
                case 'f':
                case 'F':
                case 'e':
                case 'E':
                    // t
                    // T
                    // f
                    // F
                    // tru(e)
                    // fals(e)
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
                    type = ENTITY_TYPE_SYMBOL;
                    return true;
                }
                default:
                    try {
                        floatValue = Numbers.parseDouble(value.getLo(), value.length());
                        type = ENTITY_TYPE_FLOAT;
                    } catch (NumericException ex) {
                        type = ENTITY_TYPE_SYMBOL;
                    }
                    return true;
            }
        }

        private boolean parseLong(byte entityType) {
            try {
                charSeq.of(value.getLo(), value.getHi() - 1);
                longValue = Numbers.parseLong(charSeq);
                value.decHi(); // remove 'i'
                type = entityType;
            } catch (NumericException notANumber) {
                type = ENTITY_TYPE_SYMBOL;
            }
            return true;
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
                    return parse(lastByte, valueLen) && (symbolAsFieldSupported || type != ENTITY_TYPE_SYMBOL);
                }
                type = ENTITY_TYPE_NULL;
                return true;
            }
            type = ENTITY_TYPE_TAG;
            return !tagStartsWithQuote || valueLen < 2 || value.byteAt(valueLen - 1) != '"' || stringAsTagSupported;
        }
    }

    static {
        char[] chars = new char[]{'\n', '\r', '=', ',', ' ', '\\', '"', '\0', '/'};
        controlChars = new boolean[Byte.MAX_VALUE];
        for (char ch : chars) {
            controlChars[ch] = true;
        }
    }
}
