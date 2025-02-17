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

import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.arr.DirectArrayView;
import io.questdb.cutlass.line.tcp.LineTcpParser.ErrorCode;
import io.questdb.std.IntList;
import io.questdb.std.Misc;
import io.questdb.std.Numbers;
import io.questdb.std.NumericException;
import io.questdb.std.QuietCloseable;
import io.questdb.std.ThreadLocal;
import io.questdb.std.str.DirectUtf8String;
import io.questdb.std.str.Utf8s;
import org.jetbrains.annotations.NotNull;

import static io.questdb.cutlass.line.tcp.LineTcpParser.ErrorCode.*;

/**
 * Parses an ND array literal used in ILP. Here are a few examples:
 * <p>
 * A 1-D array of longs: <code>[6i1,2,3]</code>
 * A 2-D array of doubles: <code>[6f[NaN,1],[2.5,3]]</code>
 *
 * <p>
 * The type marker is as follows: <code>&lt;type_precision&gt;&lt;type_class&gt;</code>
 * <dl>
 *     <dt>type_precision</dt>
 *     <dd>power of two of number of bits in the numeric type, e.g.
 *         <code>0=1 bit (bool)</code>, <code>2=2 bit int</code> .. <code>5=32 bit</code> .. <code>6=64 bit</code></dd>
 *     <dt>type_class</dt>
 *     <dd>type of number, <code>i</code> for signed integer, <code>u</code> for unsigned,
 *         <code>f</code> for floating point</dd>
 * </dl>
 * <p>
 * <strong>Not all combinations are valid</strong>: refer to `ColType.java`'s ND_ARRAY implementation.
 */
public class ArrayParser implements QuietCloseable {

    public static final int DIM_COUNT_LIMIT = 8;
    public static final int LEAF_LENGTH_LIMIT = 100;
    // bufs.shape is populated gradually during the parsing process.
    // We start by counting the initial `[` chars, this gives us the number of
    // dimensions in the array literal. We populate bufs.shape with that many instances
    // of the number -1 ("not yet determined").
    // Later on, each time we encounter a `]`, we check whether the size of the
    // dimension corresponding to the element just being closed has already been
    // determined. If so, the size of the element must match that; otherwise we're
    // parsing a jagged array, which is not allowed. If the size hasn't yet been
    // determined, we set it to the size of the current element.
    private final DirectArrayView array;
    private final DirectUtf8String input = new DirectUtf8String();
    /**
     * Address where the input string starts. Used to calculate the current position.
     */
    private long inputStartAddr = 0;

    public ArrayParser(CairoConfiguration configuration) {
        array = new DirectArrayView(configuration);
    }

    @Override
    public void close() {
        Misc.free(array);
    }

    /**
     * Obtains the parsed result.
     * <p>Throws an exception if {@link #parse(DirectUtf8String)} didn't succeed.</p>
     */
    public @NotNull DirectArrayView getArray() {
        if (array.getType() == ColumnType.UNDEFINED)
            throw new IllegalStateException("Parsing error");
        return array;
    }

    /**
     * Resets the state and parses the value.
     */
    public void parse(DirectUtf8String value) throws ParseException {
        reset();

        if (Utf8s.equalsAscii("[]", value)) {
            array.ofNull();
            return;
        }

        inputStartAddr = value.lo();
        input.of(value);

        parseOpenBrace();
        parseDataType();
        parseElements();
    }

    /**
     * Returns the current position within the input, as tracked by discarding the
     * already parsed input bytes.
     */
    public int position() {
        return (int) (input.lo() - inputStartAddr);
    }

    private void checkAndIncrementLevelCount(IntList levelCounts, IntList shape, int level) throws ParseException {
        int countSoFarAtCurrLevel = levelCounts.get(level);
        int dimLen = shape.get(level);
        if (countSoFarAtCurrLevel == dimLen) {
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
        final short colType;
        final int elementSize = 1 << typePrecision;
        switch (typeClass) {
            case 'i':
                switch (elementSize) {
                    case 8:
                        colType = ColumnType.BYTE;
                        break;
                    case 16:
                        colType = ColumnType.SHORT;
                        break;
                    case 32:
                        colType = ColumnType.INT;
                        break;
                    case 64:
                        colType = ColumnType.LONG;
                        break;
                    default:
                        throw ParseException.invalidType(position());
                }
                break;
            case 'f':
                switch (elementSize) {
                    case 32:
                        colType = ColumnType.FLOAT;
                        break;
                    case 64:
                        colType = ColumnType.DOUBLE;
                        break;
                    default:
                        throw ParseException.invalidType(position());
                }
                break;
            case 'u':
                if (elementSize != 1) {
                    throw ParseException.invalidType(position());
                }
                colType = ColumnType.BOOLEAN;
                break;
            default:
                throw new AssertionError("Unexpected type class");
        }

        final int arrayType = ColumnType.encodeArrayType(colType, 1);
        if (arrayType == -1) {
            throw ParseException.invalidType(position());
        }

        array.setType(arrayType);
        input.advance();
    }

    /**
     * Parses the body of a row-major array literal.
     * <p>Note that by the time we call this function, the initial left brace
     * and type have already been parsed. Example:</p>
     * <pre>
     *     [5f2.5,1.0,NaN]
     *        ^_____________ we start here!
     * </pre>
     */
    private void parseElements() throws ParseException {
        final short elemType = ColumnType.decodeArrayElementType(array.getType());
        final IntList shape = new IntList();
        final IntList levelCounts = new IntList();

        final int nDims;
        int level = 0;
        int flatElemCount = 0;
        while (true) {
            if (input.size() == 0) {
                throw ParseException.prematureEnd(position());
            }
            levelCounts.add(0);
            shape.add(IntList.NO_ENTRY_VALUE);
            if (input.byteAt(0) != '[') {
                nDims = level + 1;
                break;
            }
            levelCounts.set(level, 1);
            level++;
            if (level == DIM_COUNT_LIMIT) {
                throw ParseException.unexpectedToken(position());
            }
            input.advance();
        }
        assert nDims > 0 && shape.size() == nDims && levelCounts.size() == nDims : "Broken shape calculation";
        boolean commaWelcome = false;
        while (input.size() > 0) {
            if (level < 0) {
                throw ParseException.unexpectedToken(position());
            }
            byte b = input.byteAt(0);
            switch (b) {
                case '[': {
                    assert level < nDims : "Nesting level is too much";
                    if (commaWelcome) {
                        throw ParseException.unexpectedToken(position());
                    }
                    checkAndIncrementLevelCount(levelCounts, shape, level);
                    level++;
                    if (level >= nDims) {
                        throw ParseException.irregularShape(position());
                    }
                    levelCounts.set(level, 0);
                    input.advance();
                    continue;
                }
                case ']': {
                    int countAtCurrLevel = levelCounts.get(level);
                    if (!commaWelcome) {
                        throw ParseException.unexpectedToken(position());
                    }
                    int dimLen = shape.get(level);
                    if (dimLen == IntList.NO_ENTRY_VALUE) {
                        shape.set(level, countAtCurrLevel);
                    } else if (countAtCurrLevel != dimLen) {
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
                        if (b == ',' || b == ']') {
                            tokenLimit = i;
                            break;
                        }
                    }
                    if (tokenLimit == 0) {
                        throw (input.size() < LEAF_LENGTH_LIMIT)
                                ? ParseException.prematureEnd(position())
                                : ParseException.unexpectedToken(position());
                    }
                    parseLeaf(elemType, tokenLimit, flatElemCount++);
                    commaWelcome = true;
                    input.advance(tokenLimit);
                }
            }
        }
        array.setType(ColumnType.encodeArrayType(elemType, nDims));
        for (int n = shape.size(), i = 0; i < n; i++) {
            array.setDimLen(i, shape.getQuick(i));
        }
        array.applyShape(0);
    }

    private void parseLeaf(int elemType, int tokenLimit, int flatIndex) throws ParseException {
        try {
            switch (elemType) {
                case ColumnType.BOOLEAN:
                    int n = Numbers.parseInt(input, 0, tokenLimit);
                    if (n != (n & 1)) {
                        throw ParseException.unexpectedToken(position());
                    }
                    array.putByte(flatIndex, (byte) n);
                    break;
                case ColumnType.BYTE:
                    array.putByte(flatIndex, Numbers.parseByte(input, 0, tokenLimit));
                    break;
                case ColumnType.SHORT:
                    array.putShort(flatIndex, Numbers.parseShort(input, 0, tokenLimit));
                    break;
                case ColumnType.INT:
                    array.putInt(flatIndex, Numbers.parseInt(input, 0, tokenLimit));
                    break;
                case ColumnType.LONG:
                    array.putLong(flatIndex, Numbers.parseLong(input, 0, tokenLimit));
                    break;
                case ColumnType.FLOAT:
                    array.putFloat(flatIndex, Numbers.parseFloat(input.ptr(), tokenLimit));
                    break;
                case ColumnType.DOUBLE:
                    array.putDouble(flatIndex, Numbers.parseDouble(input.ptr(), tokenLimit));
                    break;
                default:
                    throw new AssertionError("Unexpected floating-point element size");
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
        if (b != (byte) '[') {
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
        array.clear();
        inputStartAddr = 0;
    }

    public static class ParseException extends Exception {
        private static final ThreadLocal<ParseException> tlException = new ThreadLocal<>(ParseException::new);
        private ErrorCode errorCode;
        private int position;

        public static @NotNull ParseException invalidType(int position) {
            return tlException.get().errorCode(ND_ARR_INVALID_TYPE).position(position);
        }

        public static @NotNull ParseException irregularShape(int position) {
            return tlException.get().errorCode(ND_ARR_IRREGULAR_SHAPE).position(position);
        }

        public static @NotNull ParseException prematureEnd(int position) {
            return tlException.get().errorCode(ND_ARR_PREMATURE_END).position(position);
        }

        public static @NotNull ParseException unexpectedToken(int position) {
            return tlException.get().errorCode(ND_ARR_UNEXPECTED_TOKEN).position(position);
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
    }
}
