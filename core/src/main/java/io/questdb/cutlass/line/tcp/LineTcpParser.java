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

package io.questdb.cutlass.line.tcp;

import io.questdb.griffin.SqlKeywords;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.std.Numbers;
import io.questdb.std.NumericException;
import io.questdb.std.ObjList;
import io.questdb.std.Unsafe;
import io.questdb.std.str.DirectUtf8Sequence;
import io.questdb.std.str.DirectUtf8String;

public class LineTcpParser {

    public static final byte ENTITY_TYPE_BOOLEAN = 6;
    public static final byte ENTITY_TYPE_BYTE = 17;
    public static final byte ENTITY_TYPE_CACHED_TAG = 8;
    public static final byte ENTITY_TYPE_CHAR = 19;
    public static final byte ENTITY_TYPE_DATE = 18;
    public static final byte ENTITY_TYPE_DOUBLE = 15;
    public static final byte ENTITY_TYPE_FLOAT = 2;
    public static final byte ENTITY_TYPE_GEOBYTE = 9;
    public static final byte ENTITY_TYPE_GEOINT = 11;
    public static final byte ENTITY_TYPE_GEOLONG = 12;
    public static final byte ENTITY_TYPE_GEOSHORT = 10;
    public static final byte ENTITY_TYPE_INTEGER = 3;
    public static final byte ENTITY_TYPE_LONG = 14;
    public static final byte ENTITY_TYPE_LONG256 = 7;
    public static final byte ENTITY_TYPE_NONE = (byte) 0xff; // visible for testing
    public static final byte ENTITY_TYPE_NULL = 0;
    public static final byte ENTITY_TYPE_SHORT = 16;
    public static final byte ENTITY_TYPE_STRING = 4;
    public static final byte ENTITY_TYPE_SYMBOL = 5;
    public static final byte ENTITY_TYPE_TAG = 1;
    public static final byte ENTITY_TYPE_TIMESTAMP = 13;
    public static final byte ENTITY_TYPE_UUID = 20;
    public static final byte ENTITY_TYPE_VARCHAR = 21;
    public static final byte ENTITY_UNIT_NONE = 0;
    public static final byte ENTITY_UNIT_NANO = ENTITY_UNIT_NONE + 1;
    public static final byte ENTITY_UNIT_MICRO = ENTITY_UNIT_NANO + 1;
    public static final byte ENTITY_UNIT_MILLI = ENTITY_UNIT_MICRO + 1;
    public static final byte ENTITY_UNIT_SECOND = ENTITY_UNIT_MILLI + 1;
    public static final byte ENTITY_UNIT_MINUTE = ENTITY_UNIT_SECOND + 1;
    public static final byte ENTITY_UNIT_HOUR = ENTITY_UNIT_MINUTE + 1;
    public static final long NULL_TIMESTAMP = Numbers.LONG_NULL;
    public static final int N_ENTITY_TYPES = ENTITY_TYPE_TIMESTAMP + 1;
    public static final int N_MAPPED_ENTITY_TYPES = ENTITY_TYPE_VARCHAR + 1;
    private static final byte ENTITY_HANDLER_NAME = 1;
    private static final byte ENTITY_HANDLER_NEW_LINE = 4;
    private static final byte ENTITY_HANDLER_TABLE = 0;
    private static final byte ENTITY_HANDLER_TIMESTAMP = 3;
    private static final byte ENTITY_HANDLER_VALUE = 2;

    private static final Log LOG = LogFactory.getLog(LineTcpParser.class);

    private static final boolean[] controlBytes;
    private final DirectUtf8String charSeq = new DirectUtf8String();
    private final ObjList<ProtoEntity> entityCache = new ObjList<>();
    private final DirectUtf8String measurementName = new DirectUtf8String();
    private boolean asciiSegment;
    private long bufAt;
    private ProtoEntity currentEntity;
    private byte entityHandler = -1;
    private long entityLo;
    private ErrorCode errorCode;
    private boolean isQuotedFieldValue;
    private int nEntities;
    private int nEscapedChars;
    private int nQuoteCharacters;
    private boolean nextValueCanBeOpenQuote;
    private boolean scape;
    private boolean tagsComplete;
    private long timestamp;
    private byte timestampUnit;

    public LineTcpParser() {
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

    public DirectUtf8Sequence getErrorFieldValue() {
        if (currentEntity != null) {
            return currentEntity.value;
        }
        return null;
    }

    public DirectUtf8Sequence getErrorTimestampValue() {
        return charSeq;
    }

    public DirectUtf8Sequence getLastEntityName() {
        if (currentEntity != null) {
            return currentEntity.getName();
        }
        return null;
    }

    public DirectUtf8Sequence getMeasurementName() {
        return measurementName;
    }

    public long getTimestamp() {
        return timestamp;
    }

    public byte getTimestampUnit() {
        return timestampUnit;
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
            byte b = Unsafe.getUnsafe().getByte(bufAt);

            if (nEscapedChars == 0 && !controlBytes[b & 0xff]) {
                // hot path
                nextValueCanBeOpenQuote = false;
                bufAt++;
                continue;
            }

            // slow path
            asciiSegment &= b >= 0;
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
                        return getError(bufHi);
                    }
                    asciiSegment &= b >= 0;
                    appendByte = true;
                    break;

                case '"':
                    if (nextValueCanBeOpenQuote && ++nQuoteCharacters == 1) {
                        // This means that the processing resumed from "
                        // and it's allowed to start quoted value at this point
                        bufAt++;
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
                        return getError(bufHi);
                    }

                default:
                    appendByte = true;
                    nextValueCanBeOpenQuote = false;
                    break;

                case '\0':
                    LOG.info().$("could not parse [byte=\\0]").$();
                    return getError(bufHi);
                case '/':
                    if (entityHandler != ENTITY_HANDLER_VALUE) {
                        LOG.info().$("could not parse [byte=/]").$();
                        return getError(bufHi);
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
        nEntities = 0;
        currentEntity = null;
        entityHandler = ENTITY_HANDLER_TABLE;
        timestamp = NULL_TIMESTAMP;
        timestampUnit = ENTITY_UNIT_NONE;
        errorCode = ErrorCode.NONE;
        nQuoteCharacters = 0;
        scape = false;
        nextValueCanBeOpenQuote = false;
        asciiSegment = true;
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

            currentEntity = popEntity();
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

        // For error logging.
        if (!emptyEntity) {
            currentEntity = popEntity();
            nEntities++;
            currentEntity.setName();
        }
        if (tagsComplete) {
            errorCode = ErrorCode.MISSING_FIELD_VALUE;
        } else {
            errorCode = ErrorCode.MISSING_TAG_VALUE;
        }
        return false;
    }

    private boolean expectEntityValue(byte endOfEntityByte) {
        boolean endOfSet = endOfEntityByte == (byte) ' ';
        if (endOfSet || endOfEntityByte == (byte) ',' || endOfEntityByte == (byte) '\n') {
            if (currentEntity.setValueAndUnit()) {
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
            measurementName.of(entityLo, hi, asciiSegment);
            asciiSegment = true;
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
            if (endOfEntityByte == '\n') {
                final long entityHi = bufAt - nEscapedChars;
                if (entityLo < entityHi) {
                    charSeq.of(entityLo, entityHi, asciiSegment);
                    asciiSegment = true;
                    final int charSeqLen = charSeq.size();
                    final byte last = charSeq.byteAt(charSeqLen - 1);
                    switch (last) {
                        case 'n':
                            timestampUnit = ENTITY_UNIT_NANO;
                            timestamp = Numbers.parseLong(charSeq.decHi());
                            break;
                        case 't':
                            timestampUnit = ENTITY_UNIT_MICRO;
                            timestamp = Numbers.parseLong(charSeq.decHi());
                            break;
                        case 'm':
                            timestampUnit = ENTITY_UNIT_MILLI;
                            timestamp = Numbers.parseLong(charSeq.decHi());
                            break;
                        // fall through
                        default:
                            timestamp = Numbers.parseLong(charSeq);
                    }
                }
                entityHandler = -1;
                return true;
            }
            errorCode = ErrorCode.INVALID_FIELD_SEPARATOR;
            return false;
        } catch (NumericException ex) {
            timestampUnit = ENTITY_UNIT_NONE;
            errorCode = ErrorCode.INVALID_TIMESTAMP;
            return false;
        }
    }

    private ParseResult getError(long bufHi) {
        switch (entityHandler) {
            case ENTITY_HANDLER_NAME:
                // For error logging.
                if (bufAt > entityLo && bufAt < bufHi) {
                    currentEntity = popEntity();
                    nEntities++;
                    currentEntity.setName(Math.min(bufAt + 1, bufHi));
                }
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

    private ProtoEntity popEntity() {
        ProtoEntity currentEntity;
        if (entityCache.size() <= nEntities) {
            currentEntity = new ProtoEntity();
            entityCache.add(currentEntity);
        } else {
            currentEntity = entityCache.get(nEntities);
            currentEntity.clear();
        }
        return currentEntity;
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
            asciiSegment &= b >= 0;
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
        MISSING_FIELD_VALUE,
        MISSING_TAG_VALUE,
        NONE
    }

    public enum ParseResult {
        MEASUREMENT_COMPLETE, BUFFER_UNDERFLOW, ERROR
    }

    public class ProtoEntity {
        private final DirectUtf8String name = new DirectUtf8String();
        private final DirectUtf8String value = new DirectUtf8String();
        private boolean booleanValue;
        private double floatValue;
        private long longValue;
        private byte type = ENTITY_TYPE_NONE;
        private byte unit = ENTITY_UNIT_NONE;

        public boolean getBooleanValue() {
            return booleanValue;
        }

        public double getFloatValue() {
            return floatValue;
        }

        public long getLongValue() {
            return longValue;
        }

        public DirectUtf8Sequence getName() {
            return name;
        }

        public byte getType() {
            return type;
        }

        public byte getUnit() {
            return unit;
        }

        public DirectUtf8Sequence getValue() {
            return value;
        }

        public void shl(long shl) {
            name.shl(shl);
            value.shl(shl);
        }

        private void clear() {
            type = ENTITY_TYPE_NONE;
            unit = ENTITY_UNIT_NONE;
        }

        private boolean parse(byte last, int valueLen) {
            switch (last) {
                case 'i':
                    if (valueLen > 1 && value.byteAt(1) != 'x') {
                        return parseLong(ENTITY_TYPE_INTEGER);
                    }
                    if (valueLen > 3 && value.byteAt(0) == '0' && (value.byteAt(1) | 32) == 'x') {
                        value.decHi(); // remove 'i'
                        type = ENTITY_TYPE_LONG256;
                        return true;
                    }
                    type = ENTITY_TYPE_SYMBOL;
                    return false;
                case 'n':
                    if (valueLen > 1) {
                        unit = ENTITY_UNIT_NANO;
                        return parseLong(ENTITY_TYPE_TIMESTAMP);
                    }
                case 'm':
                    if (valueLen > 1) {
                        unit = ENTITY_UNIT_MILLI;
                        return parseLong(ENTITY_TYPE_TIMESTAMP);
                    }
                    // fall through
                    type = ENTITY_TYPE_SYMBOL;
                    return false;
                case 't':
                    if (valueLen > 1) {
                        unit = ENTITY_UNIT_MICRO;
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
                            booleanValue = ((last | 32) == 't');
                            type = ENTITY_TYPE_BOOLEAN;
                        } else {
                            type = ENTITY_TYPE_SYMBOL;
                            return false;
                        }
                    } else {
                        charSeq.of(value.lo(), value.hi(), asciiSegment);
                        asciiSegment = true;
                        if (SqlKeywords.isTrueKeyword(charSeq)) {
                            booleanValue = true;
                            type = ENTITY_TYPE_BOOLEAN;
                        } else if (SqlKeywords.isFalseKeyword(charSeq)) {
                            booleanValue = false;
                            type = ENTITY_TYPE_BOOLEAN;
                        } else {
                            type = ENTITY_TYPE_SYMBOL;
                            return false;
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
                    return false;
                }
                // fall through
                default:
                    try {
                        floatValue = Numbers.parseDouble(value.lo(), value.size());
                        type = ENTITY_TYPE_FLOAT;
                    } catch (NumericException ex) {
                        type = ENTITY_TYPE_SYMBOL;
                        return false;
                    }
                    return true;
            }
        }

        private boolean parseLong(byte entityType) {
            try {
                charSeq.of(value.lo(), value.hi() - 1, true);
                longValue = Numbers.parseLong(charSeq);
                value.decHi(); // remove the suffix ('i', 'n', 't', 'm')
                type = entityType;
            } catch (NumericException notANumber) {
                unit = ENTITY_UNIT_NONE;
                type = ENTITY_TYPE_SYMBOL;
                return false;
            }
            return true;
        }

        private void setName() {
            name.of(entityLo, bufAt - nEscapedChars, asciiSegment);
            asciiSegment = true;
        }

        private void setName(long hi) {
            name.of(entityLo, hi - nEscapedChars, asciiSegment);
            asciiSegment = true;
        }

        private boolean setValueAndUnit() {
            assert type == ENTITY_TYPE_NONE;
            long bufHi = bufAt - nEscapedChars;
            int valueLen = (int) (bufHi - entityLo);
            value.of(entityLo, bufHi, asciiSegment);
            asciiSegment = true;
            if (tagsComplete) {
                if (valueLen > 0) {
                    byte lastByte = value.byteAt(valueLen - 1);
                    return parse(lastByte, valueLen);
                }
                type = ENTITY_TYPE_NULL;
                return true;
            }
            type = ENTITY_TYPE_TAG;
            return true;
        }
    }

    static {
        char[] chars = new char[]{'\n', '\r', '=', ',', ' ', '\\', '"', '\0', '/'};
        controlBytes = new boolean[256];
        for (char ch : chars) {
            controlBytes[ch] = true;
        }
        // non-ascii chars require special handling
        for (int i = 128; i < 256; i++) {
            controlBytes[i] = true;
        }
    }
}
