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

import io.questdb.cairo.ColumnType;
import io.questdb.cutlass.line.tcp.LineTcpParser.ErrorCode;
import io.questdb.std.DirectIntList;
import io.questdb.std.IntList;
import io.questdb.std.Misc;
import io.questdb.std.Numbers;
import io.questdb.std.NumericException;
import io.questdb.std.QuietCloseable;
import io.questdb.std.ThreadLocal;
import io.questdb.std.ndarr.NdArrayBuffer;
import io.questdb.std.ndarr.NdArrayMeta;
import io.questdb.std.ndarr.NdArrayView;
import io.questdb.std.str.DirectUtf8String;
import io.questdb.std.str.Utf8s;
import org.jetbrains.annotations.NotNull;

/**
 * Parse N-dimensional arrays for ILP input.
 * <p>Here are a few examples:</p>
 * <p>A 1-D array of longs: <code>{6s1,2,3}</code></p>
 * <p>A 2-D array of doubles: <code>{6f{NaN,1},{2.5,3}}</code></p>
 * <p>The type marker is as follows: <code>[type_precision][type_class]</code></p>
 * <dl>
 *     <dt>type_precision</dt>
 *     <dd>power of two of number of bits in the numeric type, e.g.
 *         <code>0=1 bit (bool)</code>, <code>2=2 bit int</code> .. <code>5=32 bit</code> .. <code>6=64 bit</code></dd>
 *     <dt>type_class</dt>
 *     <dd>type of number, <code>i</code> for signed integer, <code>u</code> for unsigned,
 *         <code>f</code> for floating point</dd>
 * </dl>
 * <p><string>Obviously not all combinations are valid</strong>: Refer to `ColType.java`'s ND_ARRAY implementation.</p>
 */
public class NdArrayParser implements QuietCloseable {

    /**
     * Mutable array buffers.
     * <p>The <code>bufs.shape</code> list is treated special: It is a stack-like state that can go negative.</p>
     * <ul>
     *     <li>Each dimension is stored as a level.</li>
     *     <li>Each dimension can be either known, or unknown.</li>
     * </ul>
     * <p>Initially during parsing we don't know how many elements we will have:</p>
     * <ul>
     *     <li>As we start parsing, we start *DECREMENTING* the latest value each time we find a new value.</li>
     *     <li>At some point during parsing, once a `}` closing brace is encountered, the dimension is locked and its sign
     *     is flipped to positive.</li>
     * </ul>
     * <p>In short, negative (uncertain) dimensions are counted down, positive (determined) dimensions validate future data.</p>
     */
    private final NdArrayBuffer arrayBuf = new NdArrayBuffer();
    private final DirectUtf8String parsing = new DirectUtf8String();

    private final NdArrayView view = new NdArrayView();
    /**
     * Starting address first encountered when parsing. Used to calculate the current position.
     */
    private long baseLo = 0;

    /**
     * Count of how many elements we've stored.
     * <p>This is tracked separately since our <code>elements</code> is just bytes,
     * and we can't determine the elementsCount if the <code>type</code>
     * has a precision smaller than a byte.</p>
     */
    private int numValues = 0;

    @Override
    public void close() {
        Misc.free(arrayBuf);
    }

    /**
     * Obtain the parsed result.
     * <p>Throws if {@link #parse(DirectUtf8String)} returned an error.</p>
     */
    public @NotNull NdArrayView getView() {
        if (view.getType() == ColumnType.UNDEFINED)
            throw new IllegalStateException("Parsing error");
        return view;
    }

    /**
     * Resets state and parses.
     * <p>Check the return code. If it's {@link ErrorCode#NONE}, access the result via {@link #getView()}.</p>
     */
    public void parse(DirectUtf8String value) throws ParseException {
        reset();

        if (Utf8s.equalsAscii("{}", value)) {
            view.ofNull();
            return;
        }

        baseLo = value.lo();
        parsing.of(value);

        parseOpenBrace();
        parseDataType();
        parseElements();
        setArray();
    }

    /**
     * Get the position of the parsing error,
     * relative to the start of the input passed to {@link #parse(DirectUtf8String)}.
     */
    public int position() {
        return (int) (parsing.lo() - baseLo);
    }

    private void elementsPutByte(byte n) {
        arrayBuf.values.putByte(n);
        ++numValues;
    }

    private void elementsPutDouble(double n) {
        arrayBuf.values.putDouble(n);
        ++numValues;
    }

    private void elementsPutFloat(float n) {
        arrayBuf.values.putFloat(n);
        ++numValues;
    }

    private void elementsPutInt(int n) {
        arrayBuf.values.putInt(n);
        ++numValues;
    }

    private void elementsPutLong(long n) {
        arrayBuf.values.putLong(n);
        ++numValues;
    }

    private void parseDataType() throws ParseException {
        if (parsing.size() < 3) {
            throw ParseException.prematureEnd();
        }

        final byte typePrecision = parseTypePrecision();
        final char typeClass = parseTypeClass();
        int elementSize = 1 << typePrecision;
        switch (typeClass) {
            case 'i':
            case 'f':
                switch (elementSize) {
                    case 32:
                    case 64:
                        break;
                    default:
                        throw ParseException.invalidType();
                }
                break;
            case 'u':
                if (elementSize != 1) {
                    throw ParseException.invalidType();
                }
                break;
            default:
                assert false : "Unexpected type class";
        }

        final int arrayType = ColumnType.buildNdArrayType(typeClass, typePrecision);
        if (arrayType == -1) {
            throw ParseException.invalidType();
        }

        arrayBuf.type = arrayType;
        parsing.advance();
    }

    private void parseElement(char typeClass, int bitSize, int tokenLimit) throws ParseException {
        try {
            switch (typeClass) {
                case 'i':
                    switch (bitSize) {
                        case 32:
                            elementsPutInt(Numbers.parseInt(parsing, 0, tokenLimit));
                            break;
                        case 64:
                            elementsPutLong(Numbers.parseLong(parsing, 0, tokenLimit));
                            break;
                        default:
                            throw new AssertionError("Unexpected signed element size");
                    }
                    break;
                case 'u':
                    assert bitSize == 1 : "Unexpected unsigned element size";
                    int n = Numbers.parseInt(parsing, 0, tokenLimit);
                    if (n != 0 && n != 1) {
                        throw ParseException.unexpectedToken();
                    }
                    elementsPutByte((byte) n);
                    break;
                case 'f':
                    switch (bitSize) {
                        case 32:
                            elementsPutFloat(Numbers.parseFloat(parsing.ptr(), tokenLimit));
                            break;
                        case 64:
                            elementsPutDouble(Numbers.parseDouble(parsing.ptr(), tokenLimit));
                            break;
                        default:
                            throw new AssertionError("Unexpected floating-point element size");
                    }
                    break;
            }
        } catch (NumericException e) {
            throw ParseException.unexpectedToken();
        }
    }

    /**
     * Parse the outermost level of a row-major array.
     * <p>Generally, this would look something like so:</p>
     * <pre>
     *     {5f2.5,1.0,NaN}
     *        ^_____________ we start here!
     * </pre>
     * <p>Note that by the time we call this function, the opening left brace and type have already been parsed.</p>
     */
    private void parseElements() throws ParseException {
        final char elementType = ColumnType.getNdArrayElementTypeClass(arrayBuf.type);
        final int elementBitSize = 1 << ColumnType.getNdArrayElementTypePrecision(arrayBuf.type);
        final DirectIntList shape = arrayBuf.shape;

        int shapeSize = 0;
        int level = 0;
        int countAtCurrLevel = 0;
        while (parsing.size() > 0) {
            if (level == -1) {
                throw ParseException.unexpectedToken();
            }
            byte b = parsing.byteAt(0);
            switch (b) {
                case '{':
                    level++;
                    if (shapeSize > 0 && level + 1 > shapeSize) {
                        throw ParseException.irregularShape();
                    }
                    countAtCurrLevel = 0;
                    parsing.advance();
                    continue;
                case '}':
                    if (shapeSize == 0) {
                        shapeSize = level + 1;
                        shape.setCapacity(shapeSize);
                        shape.setPos(level);
                        shape.clear(IntList.NO_ENTRY_VALUE);
                        shape.setPos(level);
                        shape.add(countAtCurrLevel);
                    } else {
                        int determinedLevelSize = shape.get(level);
                        if (determinedLevelSize == IntList.NO_ENTRY_VALUE) {
                            shape.set(level, countAtCurrLevel);
                        } else if (countAtCurrLevel != determinedLevelSize) {
                            throw ParseException.irregularShape();
                        }
                    }
                    level--;
                    countAtCurrLevel = 0;
                    parsing.advance();
                    continue;
                case ',':
                    parsing.advance();
                    continue;
                default:
                    countAtCurrLevel++;
                    int tokenLimit = 0;
                    for (int n = parsing.size(), i = 1; i < n; i++) {
                        b = parsing.byteAt(i);
                        if (b == ',' || b == '}') {
                            tokenLimit = i;
                            break;
                        }
                    }
                    if (tokenLimit == 0) {
                        throw ParseException.prematureEnd();
                    }
                    parseElement(elementType, elementBitSize, tokenLimit);
                    parsing.advance(tokenLimit);
            }
        }
    }

    private void parseOpenBrace() throws ParseException {
        if (parsing.size() == 0) {
            throw ParseException.prematureEnd();
        }
        final byte b = parsing.byteAt(0);
        if (b != (byte) '{') {
            throw ParseException.unexpectedToken();
        }
        parsing.advance();
    }

    /**
     * Parse number class: i -> signed, u -> unsigned, f -> floating point
     */
    private char parseTypeClass() throws ParseException {
        final char ch = (char) parsing.byteAt(0);
        switch (ch) {
            case 'i':
            case 'u':
            case 'f':
                return ch;
            default:
                throw ParseException.invalidType();
        }
    }

    /**
     * Power of 2, number of bits: e.g. 0 -> bool, 1 -> int2, ..., 5 -> int32, 6 -> int64
     */
    private byte parseTypePrecision() throws ParseException {
        final char ch = (char) parsing.byteAt(0);
        if (ch < '0' || ch > '6') {
            throw ParseException.invalidType();
        }
        parsing.advance();
        return (byte) (ch - '0');  // parse the single digit
    }

    private void reset() {
        view.reset();
        arrayBuf.reset();
        numValues = 0;
        baseLo = 0;
    }

    private void setArray() throws ParseException {
        NdArrayMeta.setDefaultStrides(arrayBuf.shape.asSlice(), arrayBuf.strides);
        arrayBuf.updateView(view);
    }

    public static class ParseException extends Exception {
        private static final ThreadLocal<ParseException> tlException = new ThreadLocal<>(ParseException::new);
        ErrorCode errorCode;

        public static @NotNull ParseException invalidShape() {
            return instance().errorCode(ErrorCode.ND_ARR_INVALID_SHAPE);
        }

        public static @NotNull ParseException invalidType() {
            return instance().errorCode(ErrorCode.ND_ARR_INVALID_TYPE);
        }

        public static @NotNull ParseException invalidValuesSize() {
            return instance().errorCode(ErrorCode.ND_ARR_INVALID_VALUES_SIZE);
        }

        public static @NotNull ParseException irregularShape() {
            return instance().errorCode(ErrorCode.ND_ARR_IRREGULAR_SHAPE);
        }

        public static @NotNull ParseException malformed() {
            return instance().errorCode(ErrorCode.ND_ARR_MALFORMED);
        }

        public static @NotNull ParseException prematureEnd() {
            return instance().errorCode(ErrorCode.ND_ARR_TOO_SHORT);
        }

        public static @NotNull ParseException shapeStridesMismatch() {
            return instance().errorCode(ErrorCode.ND_ARR_SHAPE_STRIDES_MISMATCH);
        }

        public static @NotNull ParseException unexpectedToken() {
            return instance().errorCode(ErrorCode.ND_ARR_UNEXPECTED);
        }

        public ParseException errorCode(ErrorCode errorCode) {
            this.errorCode = errorCode;
            return this;
        }

        public ErrorCode errorCode() {
            return errorCode;
        }

        private static ParseException instance() {
            ParseException ex = tlException.get();
            // This is to have correct stack trace in local debugging with -ea option
            assert (ex = new ParseException()) != null;
            return ex;
        }
    }
}
