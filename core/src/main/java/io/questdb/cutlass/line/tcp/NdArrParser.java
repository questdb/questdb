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
import io.questdb.std.MemoryTag;
import io.questdb.std.Misc;
import io.questdb.std.QuietCloseable;
import io.questdb.std.bytes.DirectByteSequence;
import io.questdb.std.bytes.DirectByteSink;
import io.questdb.std.ndarr.NdArrFormat;
import io.questdb.std.str.DirectUtf8String;
import io.questdb.std.str.StringSink;
import io.questdb.std.str.Utf8s;

/**
 * Parse N-dimensional arrays for ILP input.
 *
 * <p>Here are a few examples:</p>
 *
 * <p>A 1-D array of longs:</p>
 * <pre><code>{6i1,2,3}</code></pre>
 *
 * <p>A 2-D array of doubles:</p>
 * <pre><code>
 * {
 *     -- a comment
 *     {6fNaN,1},
 *     {6f2.5,3}  -- yet another comment
 * }
 * </code></pre>
 *
 * <p>The type marker is as follows: <code>[precision][number_class]</code></p>
 * <dl>
 *     <dt>precision</dt>
 *     <dd>power of two of number of bits in the numeric type, e.g.
 *         <code>0=1 bit (bool)</code>, <code>2=2 bit int</code> .. <code>5=32 bit</code> .. <code>6=64 bit</code></dd>
 *     <dt>number_class</dt>
 *     <dd>type of number, <code>i</code> for signed integer, <code>u</code> for unsigned,
 *         <code>f</code> for floating point</dd>
 * </dl>
 * <p><string>Obviously not all combinations are valid</strong>: Refer to `ColType.java`'s ND_ARRAY implementation.</p>
 *
 * <p>There is also support for specifying the array as a CSC or CSR 1D vectors and 2D matrices.
 * For example:</p>
 * <pre><code>{5uR{0,1,3,4,5}{2,0,3,4,1}{3,5,4,7,8}}</code></pre>
 * <p>is equivalent to (extra invalid whitespace added for readability):</p>
 * <pre><code>
 * {5u
 *     {0, 0, 3, 0, 0},
 *     {5, 0, 0, 4, 0},
 *     {0, 0, 0, 0, 7},
 *     {0, 8, 0, 0, 0}
 * }
 * </code></pre>
 *
 * <p><code>R</code> tags <strong>CSR</strong> and <code>C</code> tags <strong>CSC</strong>.</p>
 * <p>The three following arrays of numbers indicate:</p>
 * <ul>
 *   <li><code>{row_pointers/col_pointers}</code></li>
 *   <li><code>{column_indices/row_indices}</code></li>
 *   <li><code>{values}</code></li>
 * </ul>
 */
public class NdArrParser implements QuietCloseable {
    private final StringSink actual = new StringSink();
    /**
     * Temporary used to decode a single UTF-8 char and re-encode it as UTF-16
     */
    private final char[] chEncodeTmp = new char[2];
    /**
     * Stack-like state used to parse the dimensions.
     * <p>When <code>NdArrFormat.RM</code>:</p>
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
     * <p>When format is <code>NdArrFormat.CSR</code> or <code>NdArrFormat.CSC</code>, this field is set after parsing.
     */
    private final IntList dims = new IntList(8);
    private final DirectByteSink elements = new DirectByteSink(64);  // TODO(amunra): Memory Tag
    private final DirectUtf8String parsing = new DirectUtf8String();
    private final DirectUtf8String token = new DirectUtf8String();
    private final DirectIntList sparseIndices = new DirectIntList(0, MemoryTag.NATIVE_ND_ARRAY);
    private final DirectIntList sparsePointers = new DirectIntList(0, MemoryTag.NATIVE_ND_ARRAY);
    /**
     * Starting address first encountered when parsing. Used to calculate the current position.
     */
    private long baseLo = 0;

    /**
     * The current number of elements in the current dimension (for `NdArrFormat.RM` parsing).
     */
    private int currDimLen = 0;
    /**
     * The dimension index we're currently parsing (for `NdArrFormat.RM` parsing).
     */
    private int dimIndex = -1;

    /**
     * Count of how many elements we've stored.
     * <p>This is tracked separately since our <code>elements</code> is just bytes,
     * and we can't determine the elementsCount if the <code>type</code>
     * has a precision smaller than a byte.</p>
     */
    private int elementsCount = 0;
    private ErrorCode error = ErrorCode.NONE;
    private int format = NdArrFormat.UNDEFINED;
    private int type = ColumnType.UNDEFINED;

    // TODO(amunra): Convert all usages of this into more enum values instead. */
    public String getUnexpectedErrorMsg() {
        return unexpectedErrorMsg;
    }

    /**
     * Message prelude for any `PARSING_UNEXPECTED` error
     */
    private String unexpectedErrorMsg = "";

    /**
     * Continue parsing the previous level of array nesting.
     */
    private void popDim() {
        // Solidify the last dim before going back to the previous level.
        // The negative (uncertain) value will be converted to positive.
        dims.setQuick(dimIndex, currDimLen);
        assert (dimIndex >= 0);
        assert dimIndex <= 0 || currDimLen > 0;
        --dimIndex;
        if (dimIndex >= 0) {
            currDimLen = Math.abs(dims.getQuick(dimIndex));
        }
    }

    @Override
    public void close() {
        Misc.free(elements);
        Misc.free(sparsePointers);
        Misc.free(sparseIndices);
        throw new UnsupportedOperationException("nyi");
    }

    /**
     * Get the dimensions of the ND Array
     */
    public IntList getDims() {
        return dims;
    }

    /**
     * Get the ptr address to the flattened values buffer.
     * Its content depends on the format (see `getFormat()`).
     * <p>
     * When the format is `NdArrFormat.RM` (dense, row-major) it's a
     * flattened array of all the elements.
     * <p>
     * For example, for the 4x3x2 matrix: {
     * {{1, 2}, {3, 4}, {5, 6}},
     * {{7, 8}, {9, 0}, {1, 2}},
     * {{3, 4}, {5, 6}, {7, 8}},
     * {{9, 0}, {1, 2}, {3, 4}}
     * }
     * The buffer would contain a flat vector of elements (see getElementType)
     * with the numbers [1, 2, 3, 4, 5, 6, 7, 8, 9, 0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 0, 1, 2, 3, 4]
     *
     * <p><strong>N.B.</strong>: This method should only be called once parsing is complete.</p>
     */
    public DirectByteSequence getElements() {
        return elements;
    }

    /**
     * Number of elements returned by `getElements()`.
     * <p><strong>N.B.</strong>: This method should only be called once parsing is complete.</p>
     */
    public int getElementsCount() {
        return elementsCount;
    }

    /**
     * Returns a value from the `NdArrFormat` enum indicating how the array is represented in memory.
     * <p><strong>N.B.</strong>: This method should only be called once parsing is complete.</p>
     */
    public int getFormat() {
        return format;
    }

    /**
     * Get the {column_indices/row_indices} vector for the CSR/CSC sparse array.
     * Returns a nullptr if `getFormat()` is dense (i.e. `RM`).
     * The returned buffer contains 32-bit integers.
     * <p><strong>N.B.</strong>: This method should only be called once parsing is complete.</p>
     */
    public DirectIntList getSparseIndices() {
        return sparseIndices;
    }

    /**
     * Get the address of the {row_pointers/col_pointers} vector for the CSR/CSC sparse array.
     * Returns a null address if `getFormat()` is dense (i.e. `RM`).
     * The returned buffer contains 32-bit integers.
     * <p><strong>N.B.</strong>: This method should only be called once parsing is complete.</p>
     */
    public DirectIntList getSparsePointers() {
        return sparsePointers;
    }

    /**
     * Get the array ColumnType.
     * <p><strong>N.B.</strong>: This method should only be called once parsing is complete.</p>
     */
    public int getType() {
        return type;
    }

    /**
     * The parsed array is effectively NULL, i.e. contains no values.
     * <p><strong>N.B.</strong>: This method should only be called once parsing is complete.</p>
     */
    public boolean isNullArray() {
        return elements.size() == 0;
    }

    /**
     * Resets state and parses.
     * Returns the number of bytes advanced.
     * The `elementType` indicates the backing type of the array.
     * It can be `UNDEFINED` (if not yet known), `DOUBLE` or `LONG`.
     * The `str` and `size` indicate the UTF-8 bytes to parse.
     * <p>
     * The parser can operate in one of two modes:
     * * `sqlMode==true` (for SQL)
     * * NULL keyword supported.
     * * White space allowed.
     * * Comments allowed.
     * * `sqlMode==false` (for ILP), disables these human-friendly features.
     * <p>
     * Grammar:
     * See the `nd_arr_grammar.py` for reference. It is more intended for syntax highlighting etc.
     * There are a few differences with the implementation here:
     * * That grammar performs no validation, such as checking that all rows have the same element count.
     * * That grammar does not support whitespace or comments.
     */
    public LineTcpParser.ErrorCode parse(DirectUtf8String value) {
        reset();

        if (Utf8s.equalsAscii("{}", value)) {
            return ErrorCode.NONE;
        }

        baseLo = value.lo();
        parsing.of(value);

        parseLeftBrace();
        if (error != ErrorCode.NONE) {
            return error;
        }

        parseDataType();
        if (error != ErrorCode.NONE) {
            return error;
        }

        parseFormat();
        if (error != ErrorCode.NONE) {
            return error;
        }

        switch (format) {
            case NdArrFormat.CSR:
            case NdArrFormat.CSC:
                throw new UnsupportedOperationException("not yet implemented");

            case NdArrFormat.RM:
                parseRowMajor();
                break;
            default:
                throw new IllegalStateException("expected unreachable code, invalid value: " + format);
        }

        return error;
    }

    /**
     * Get the position of the parsing error, relative to the start of the input passed to `parse()`.
     */
    public int position() {
        return (int) (parsing.lo() - baseLo);
    }

    private void actualPutCodepoint(int codepoint) {
        chEncodeTmp[0] = 0;
        chEncodeTmp[1] = 0;
        if (Character.toChars(codepoint, chEncodeTmp, 0) == 1) {
            actual.put(chEncodeTmp[0]);
        } else {
            actual.put(chEncodeTmp[0]);
            actual.put(chEncodeTmp[1]);
        }
    }

    private void clearError() {
        error = ErrorCode.NONE;
        actual.clear();
    }

    private boolean mayAdvance(int required) {
        return parsing.size() >= required;
    }

    private void parseDataType() {
        if (!mayAdvance(2)) {
            error = ErrorCode.ND_ARR_TOO_SHORT;
            return;
        }

        final byte typePrecision = parseTypePrecision();
        if (error != ErrorCode.NONE) {
            return;
        }

        final char typeClass = parseTypeClass();
        if (error != ErrorCode.NONE) {
            return;
        }

        final int arrayType = ColumnType.buildNdArrayType(typeClass, typePrecision);
        if (arrayType == -1) {
            unexpectedErrorMsg = "Invalid array type ";
            actual.put((char) (48 + typePrecision));
            actual.put(typeClass);
            error = ErrorCode.ND_ARR_UNEXPECTED;
            return;
        }

        type = arrayType;
        parsing.advance();
    }

    private void parseFormat() {
        if (parsing.size() < 1) {
            error = ErrorCode.ND_ARR_TOO_SHORT;
            return;
        }

        final byte next = parsing.byteAt(0);
        switch (next) {
            case 'C':
                format = NdArrFormat.CSC;
                parsing.advance();
                break;

            case 'R':
                format = NdArrFormat.CSR;
                parsing.advance();
                break;

            case '0':
            case '1':
            case '2':
            case '3':
            case '4':
            case '5':
            case '6':
            case '7':
            case '8':
            case '9':
            case '.':
            case '+':
            case '-':
            case 'N':  // NaN
            case '{':
                // The start of a number or a nesting level implies we're parsing a row-major dense array.
                format = NdArrFormat.RM;
                return;
            default:
                actualPutCodepoint(Utf8s.utf8CharDecode(parsing));
                unexpectedErrorMsg = "Invalid array format, must be one of 'C', 'R' or missing, not ";
                error = ErrorCode.ND_ARR_UNEXPECTED;
                break;
        }
    }

    private void parseLeftBrace() {
        if (parsing.size() == 0) {
            error = ErrorCode.ND_ARR_TOO_SHORT;
            return;
        }
        final byte b0 = parsing.byteAt(0);
        if (b0 != (byte) '{') {
            unexpectedErrorMsg = "expected '{', not ";
            actualPutCodepoint(Utf8s.utf8CharDecode(parsing));
            error = ErrorCode.ND_ARR_UNEXPECTED;
            return;
        }

        pushDim();
        parsing.advance();
    }

    /**
     * Parse number class: u -> unsigned, s -> signed, f -> floating point
     */
    private char parseTypeClass() {
        final char ch = (char) parsing.byteAt(0);
        switch (ch) {
            case 'u':
            case 's':
            case 'f':
                return ch;
            default:
                unexpectedErrorMsg = "Invalid number class, must be one of `u`, `s` of `f`, not ";
                actualPutCodepoint(Utf8s.utf8CharDecode(parsing));
                error = ErrorCode.ND_ARR_UNEXPECTED;
                return 0;
        }
    }

    /**
     * Power of 2, number of bits: e.g. 0 -> bool, 1 -> int2, ..., 5 -> int32, 6 -> int64
     */
    private byte parseTypePrecision() {
        final char ch = (char) parsing.byteAt(0);
        if (ch < '0' || ch > '6') {
            unexpectedErrorMsg = "Invalid power of 2 for numeric precision. Must be one of 0, 1, 2, 3, 4, 6, not ";
            actualPutCodepoint(Utf8s.utf8CharDecode(parsing));
            error = ErrorCode.ND_ARR_UNEXPECTED;
            return -1;
        }
        parsing.advance();
        return (byte) (ch - (char) 48);  // parse the single digit
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
    private void parseRowMajor() {
        if (error != ErrorCode.NONE) {
            return;
        }
        pushDim();

        if (Utf8s.equalsUtf16("1.0,2.5,3.0,4.5,5.0}", parsing)) {
            assert type == ColumnType.buildNdArrayType('f', (byte) 6);  // ARRAY(DOUBLE)
            elementsPutDouble(1.0);
            elementsPutDouble(2.5);
            elementsPutDouble(3.0);
            elementsPutDouble(4.5);
            elementsPutDouble(5.0);
            parsing.advance(20);
        } else if (Utf8s.equalsUtf16("-1,0,100000000}", parsing)) {
            assert type == ColumnType.buildNdArrayType('i', (byte) 6);  // ARRAY(LONG)
            elementsPutLong(-1);
            elementsPutLong(0);
            elementsPutLong(100000000);
            parsing.advance(15);
        } else {
            throw new UnsupportedOperationException("not yet implemented");
        }

        popDim();

        // TODO(amunra): Complete the parser.
//        final long endOfInput = parsing.hi();
//        boolean more = true;
//        for (long current = parsing.lo(); more; ++current) {
//            if (current == endOfInput) {
//                error = ErrorCode.ND_ARR_TOO_SHORT;
//                return;
//            }
//
//            final byte at = Unsafe.getUnsafe().getByte(current);
//
//            switch (at) {
//                case '}':
//                    popDim();
//                    if (dimIndex == -1) {
//                        more = false;  // reached the end of the array
//                        if (current + 1 != endOfInput) {
//                            error = ErrorCode.ND_ARR_EARLY_TERMINATION;
//                            return;
//                        }
//                    }
//                case ',':
//
//                default:
//            }
//        }



    }

    private void elementsPutLong(int n) {
        elements.putLong(n);
        ++elementsCount;
    }

    private void elementsPutDouble(double n) {
        elements.putDouble(n);
        ++elementsCount;
    }

    /**
     * Parse the next level of array nesting.
     */
    private void pushDim() {
        dims.add(0);
        ++dimIndex;
        currDimLen = 0;
    }

    private void reset() {
        clearError();
        currDimLen = 0;
        dimIndex = -1;  // No dimensions until the first `{`
        dims.clear();
        type = ColumnType.UNDEFINED;
        format = NdArrFormat.UNDEFINED;
        elements.clear();
        sparsePointers.clear();
        sparseIndices.clear();
        elementsCount = 0;
        baseLo = 0;
        format = NdArrFormat.UNDEFINED;
    }

    private void valueWritten() {
        currDimLen++;
        final int lastDimLen = dims.getQuick(dimIndex);
        if (lastDimLen <= 0) {
            // yet undetermined
            dims.setQuick(dimIndex, -currDimLen);
        } else if (lastDimLen >= currDimLen) {
            error = ErrorCode.ND_ARR_UNALIGNED;
        }

    }
}
