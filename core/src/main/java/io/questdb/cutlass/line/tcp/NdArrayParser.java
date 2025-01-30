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
import io.questdb.std.ndarr.NdArrayBuffers;
import io.questdb.std.ndarr.NdArrayMeta;
import io.questdb.std.ndarr.NdArrayView;
import io.questdb.std.str.DirectUtf8String;
import io.questdb.std.str.Utf8s;
import org.jetbrains.annotations.NotNull;

import static io.questdb.cutlass.line.tcp.LineTcpParser.ErrorCode.*;

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

    public static final int LEAF_LENGTH_LIMIT = 100;

    // bufs.shape is populated gradually during the parsing process. When we reach the
    // first occurrence of `}`, we know the size of the leaf dimension (rightmost in
    // the shape, deepest-nested). At that point we initialize shape to the number of
    // elements we encountered at the leaf dimension, and -1 ("not yet determined") for
    // all the upper dims. Later on, when we encounter further `}`, we check whether
    // the size of the current dimension has already been determined. If so, the size
    // of the element just being closed must match that; otherwise we're parsing a
    // jagged array, which is not allowed.
    private final NdArrayBuffers bufs = new NdArrayBuffers();
    private final DirectUtf8String input = new DirectUtf8String();
    private final NdArrayView view = new NdArrayView();

    /**
     * Address where the input string starts. Used to calculate the current position.
     */
    private long inputStartAddr = 0;

    /**
     * Count of how many elements we've stored.
     * <p>This is tracked separately since our <code>elements</code> is just bytes,
     * and we can't determine the elementsCount if the <code>type</code>
     * has a precision smaller than a byte.</p>
     */
    private int numValues = 0;

    @Override
    public void close() {
        Misc.free(bufs);
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

        inputStartAddr = value.lo();
        input.of(value);

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
        return (int) (input.lo() - inputStartAddr);
    }

    private void checkAndIncrementLevelCount(DirectIntList levelCounts, DirectIntList shape, int level) throws ParseException {
        int countSoFarAtCurrLevel = levelCounts.get(level);
        int dimSize = shape.get(level);
        if (countSoFarAtCurrLevel == dimSize) {
            throw ParseException.irregularShape(position());
        }
        levelCounts.set(level, countSoFarAtCurrLevel + 1);
    }

    private void parseDataType() throws ParseException {
        if (input.size() < 3) {
            throw ParseException.prematureEnd(position());
        }

        final byte typePrecision = parseTypePrecision();
        final char typeClass = parseTypeClass();
        int elementSize = 1 << typePrecision;
        switch (typeClass) {
            case 'i':
                switch (elementSize) {
                    case 8:
                    case 16:
                    case 32:
                    case 64:
                        break;
                    default:
                        throw ParseException.invalidType(position());
                }
                break;
            case 'f':
                switch (elementSize) {
                    case 32:
                    case 64:
                        break;
                    default:
                        throw ParseException.invalidType(position());
                }
                break;
            case 'u':
                if (elementSize != 1) {
                    throw ParseException.invalidType(position());
                }
                break;
            default:
                assert false : "Unexpected type class";
        }

        final int arrayType = ColumnType.buildNdArrayType(typeClass, typePrecision);
        if (arrayType == -1) {
            throw ParseException.invalidType(position());
        }

        bufs.type = arrayType;
        input.advance();
    }

    /**
     * Parse the outermost level of a row-major array.
     * <p>Generally, this would look something like this:</p>
     * <pre>
     *     {5f2.5,1.0,NaN}
     *        ^_____________ we start here!
     * </pre>
     * <p>Note that by the time we call this function, the opening left brace and type have already been parsed.</p>
     */
    private void parseElements() throws ParseException {
        final char numberType = ColumnType.getNdArrayElementTypeClass(bufs.type);
        final int numberBitSize = 1 << ColumnType.getNdArrayElementTypePrecision(bufs.type);
        final DirectIntList shape = bufs.shape;
        final DirectIntList levelCounts = bufs.currCoords;
        levelCounts.add(0);

        final int shapeSize;
        int level = 0;
        while (true) {
            if (input.size() == 0) {
                throw ParseException.prematureEnd(position());
            }
            if (input.byteAt(0) == '{') {
                levelCounts.set(level, 1);
                levelCounts.add(0);
                level++;
                input.advance();
                continue;
            }
            shapeSize = (int) levelCounts.size();
            shape.setCapacity(shapeSize);
            shape.setPos(shapeSize);
            shape.clear(IntList.NO_ENTRY_VALUE);
            // pos is used to calculate shape.size(), so set it to the correct value
            shape.setPos(shapeSize);
            break;
        }
        assert shapeSize > 0 && shape.size() == shapeSize && levelCounts.size() == shapeSize : "Broken shape calculation";
        boolean commaWelcome = false;
        while (input.size() > 0) {
            if (level < 0) {
                throw ParseException.unexpectedToken(position());
            }
            byte b = input.byteAt(0);
            switch (b) {
                case '{': {
                    assert level < shapeSize : "Nesting level is too much";
                    if (commaWelcome) {
                        throw ParseException.unexpectedToken(position());
                    }
                    checkAndIncrementLevelCount(levelCounts, shape, level);
                    level++;
                    if (level >= shapeSize) {
                        throw ParseException.irregularShape(position());
                    }
                    levelCounts.set(level, 0);
                    input.advance();
                    continue;
                }
                case '}': {
                    int countAtCurrLevel = levelCounts.get(level);
                    if (!commaWelcome) {
                        throw ParseException.unexpectedToken(position());
                    }
                    int dimSize = shape.get(level);
                    if (dimSize == IntList.NO_ENTRY_VALUE) {
                        shape.set(level, countAtCurrLevel);
                    } else if (countAtCurrLevel != dimSize) {
                        throw ParseException.irregularShape(position());
                    }
                    level--;
                    input.advance();
                    continue;
                }
                case ',': {
                    if (!commaWelcome) {
                        throw ParseException.unexpectedToken(position());
                    }
                    commaWelcome = false;
                    input.advance();
                    continue;
                }
                default: {
                    assert level < levelCounts.size() : "Level shot up while parsing leaves";
                    if (commaWelcome) {
                        throw ParseException.unexpectedToken(position());
                    }
                    checkAndIncrementLevelCount(levelCounts, shape, level);
                    int tokenLimit = 0;
                    for (int n = Math.min(input.size(), LEAF_LENGTH_LIMIT), i = 1; i < n; i++) {
                        b = input.byteAt(i);
                        if (b == ',' || b == '}') {
                            tokenLimit = i;
                            break;
                        }
                    }
                    if (tokenLimit == 0) {
                        throw (input.size() < LEAF_LENGTH_LIMIT)
                                ? ParseException.prematureEnd(position())
                                : ParseException.unexpectedToken(position());
                    }
                    parseLeaf(numberType, numberBitSize, tokenLimit);
                    numValues++;
                    commaWelcome = true;
                    input.advance(tokenLimit);
                }
            }
        }
    }

    private void parseLeaf(char typeClass, int bitSize, int tokenLimit) throws ParseException {
        try {
            switch (typeClass) {
                case 'i':
                    switch (bitSize) {
                        case 8:
                            bufs.values.putByte(Numbers.parseByte(input, 0, tokenLimit));
                            break;
                        case 16:
                            bufs.values.putShort(Numbers.parseShort(input, 0, tokenLimit));
                            break;
                        case 32:
                            bufs.values.putInt(Numbers.parseInt(input, 0, tokenLimit));
                            break;
                        case 64:
                            bufs.values.putLong(Numbers.parseLong(input, 0, tokenLimit));
                            break;
                        default:
                            throw new AssertionError("Unexpected signed element size");
                    }
                    break;
                case 'u':
                    assert bitSize == 1 : "Unexpected unsigned element size";
                    int n = Numbers.parseInt(input, 0, tokenLimit);
                    if (n != (n & 1)) {
                        throw ParseException.unexpectedToken(position());
                    }
                    bufs.values.putByte((byte) n);
                    break;
                case 'f':
                    switch (bitSize) {
                        case 32:
                            bufs.values.putFloat(Numbers.parseFloat(input.ptr(), tokenLimit));
                            break;
                        case 64:
                            bufs.values.putDouble(Numbers.parseDouble(input.ptr(), tokenLimit));
                            break;
                        default:
                            throw new AssertionError("Unexpected floating-point element size");
                    }
                    break;
            }
        } catch (NumericException e) {
            throw ParseException.unexpectedToken(position());
        }
    }

    private void parseOpenBrace() throws ParseException {
        if (input.size() == 0) {
            throw ParseException.prematureEnd(position());
        }
        final byte b = input.byteAt(0);
        if (b != (byte) '{') {
            throw ParseException.unexpectedToken(position());
        }
        input.advance();
    }

    /**
     * Parse number class: i -> signed, u -> unsigned, f -> floating point
     */
    private char parseTypeClass() throws ParseException {
        final char ch = (char) input.byteAt(0);
        switch (ch) {
            case 'i':
            case 'u':
            case 'f':
                return ch;
            default:
                throw ParseException.invalidType(position());
        }
    }

    /**
     * Power of 2, number of bits: e.g. 0 -> bool, 1 -> int2, ..., 5 -> int32, 6 -> int64
     */
    private byte parseTypePrecision() throws ParseException {
        final char ch = (char) input.byteAt(0);
        if (ch < '0' || ch > '6') {
            throw ParseException.invalidType(position());
        }
        input.advance();
        return (byte) (ch - '0');  // parse the single digit
    }

    private void reset() {
        view.reset();
        bufs.reset();
        numValues = 0;
        inputStartAddr = 0;
    }

    private void setArray() throws ParseException {
        NdArrayMeta.setDefaultStrides(bufs.shape.asSlice(), bufs.strides);
        bufs.updateView(view);
    }

    public static class ParseException extends Exception {
        private static final ThreadLocal<ParseException> tlException = new ThreadLocal<>(ParseException::new);
        private ErrorCode errorCode;
        private int position;

        public static @NotNull ParseException invalidShape(int position) {
            return instance().errorCode(ND_ARR_INVALID_SHAPE).position(position);
        }

        public static @NotNull ParseException invalidType(int position) {
            return instance().errorCode(ND_ARR_INVALID_TYPE).position(position);
        }

        public static @NotNull ParseException invalidValuesSize(int position) {
            return instance().errorCode(ND_ARR_INVALID_VALUES_SIZE).position(position);
        }

        public static @NotNull ParseException irregularShape(int position) {
            return instance().errorCode(ND_ARR_IRREGULAR_SHAPE).position(position);
        }

        public static @NotNull ParseException malformed(int position) {
            return instance().errorCode(ErrorCode.ND_ARR_MALFORMED).position(position);
        }

        public static @NotNull ParseException prematureEnd(int position) {
            return instance().errorCode(ND_ARR_PREMATURE_END).position(position);
        }

        public static @NotNull ParseException shapeStridesMismatch(int position) {
            return instance().errorCode(ND_ARR_SHAPE_STRIDES_MISMATCH).position(position);
        }

        public static @NotNull ParseException unexpectedToken(int position) {
            return instance().errorCode(ND_ARR_UNEXPECTED).position(position);
        }

        public ParseException errorCode(ErrorCode errorCode) {
            this.errorCode = errorCode;
            return this;
        }

        public ErrorCode errorCode() {
            return errorCode;
        }

        public @NotNull ParseException position(int position) {
            this.position = position;
            return this;
        }

        public int position() {
            return position;
        }

        private static ParseException instance() {
            ParseException ex = tlException.get();
            // This is to have correct stack trace in local debugging with -ea option
            assert (ex = new ParseException()) != null;
            return ex;
        }
    }
}
