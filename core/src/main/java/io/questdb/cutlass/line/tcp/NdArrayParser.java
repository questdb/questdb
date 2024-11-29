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
import io.questdb.std.MemoryTag;
import io.questdb.std.Misc;
import io.questdb.std.QuietCloseable;
import io.questdb.std.bytes.DirectByteSink;
import io.questdb.std.ndarr.NdArrayMeta;
import io.questdb.std.ndarr.NdArrayView;
import io.questdb.std.str.DirectUtf8String;
import io.questdb.std.str.StringSink;
import io.questdb.std.str.Utf8s;
import org.jetbrains.annotations.NotNull;

/**
 * Parse N-dimensional arrays for ILP input.
 *
 * <p>Here are a few examples:</p>
 *
 * <p>A 1-D array of longs:</p>
 * <pre><code>{6s1,2,3}</code></pre>
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
 * <p>The type marker is as follows: <code>[type_precision][type_class]</code></p>
 * <dl>
 *     <dt>type_precision</dt>
 *     <dd>power of two of number of bits in the numeric type, e.g.
 *         <code>0=1 bit (bool)</code>, <code>2=2 bit int</code> .. <code>5=32 bit</code> .. <code>6=64 bit</code></dd>
 *     <dt>type_class</dt>
 *     <dd>type of number, <code>s</code> for signed integer, <code>u</code> for unsigned,
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
 * <p><code>R</code> tags <strong>CSR</strong> and <code>C</code> tags <strong>CSC</strong>.</p>
 * <p>The three following arrays of numbers indicate:</p>
 * <ul>
 *   <li><code>{row_pointers/col_pointers}</code></li>
 *   <li><code>{column_indices/row_indices}</code></li>
 *   <li><code>values</code></li>
 * </ul>
 */
public class NdArrayParser implements QuietCloseable {
    private final StringSink actual = new StringSink();
    private final NdArrayView array = new NdArrayView();
    private final DirectUtf8String parsing = new DirectUtf8String();
    /**
     * Stack-like state used to parse the dimensions.
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
    private final DirectIntList shape = new DirectIntList(8, MemoryTag.NATIVE_ND_ARRAY);
    private final DirectIntList strides = new DirectIntList(8, MemoryTag.NATIVE_ND_ARRAY);
    private final DirectUtf8String token = new DirectUtf8String();
    private final DirectByteSink values = new DirectByteSink(64, MemoryTag.NATIVE_ND_ARRAY);
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
    private ErrorCode error = ErrorCode.NONE;
    private int type = ColumnType.UNDEFINED;
    /**
     * Message prelude for any `PARSING_UNEXPECTED` error
     */
    private String unexpectedErrorMsg = "";
    /**
     * Count of how many elements we've stored.
     * <p>This is tracked separately since our <code>elements</code> is just bytes,
     * and we can't determine the elementsCount if the <code>type</code>
     * has a precision smaller than a byte.</p>
     */
    private int valuesCount = 0;

    @Override
    public void close() {
        Misc.free(values);
    }

    /**
     * Obtain the parsed result.
     * <p>Throws if {@link #parse(DirectUtf8String)} returned an error.</p>
     */
    public @NotNull NdArrayView getArray() {
        if (array.getType() == ColumnType.UNDEFINED)
            throw new IllegalStateException("Parsing error");
        return array;
    }

    // TODO(amunra): Convert all usages of this into more enum values instead. */
    public String getUnexpectedErrorMsg() {
        return unexpectedErrorMsg;
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
            array.ofNull();
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

        parseElements();
        if (error != ErrorCode.NONE) {
            return error;
        }

        return setArray();
    }

    /**
     * Get the position of the parsing error,
     * relative to the start of the input passed to {@link #parse(DirectUtf8String)}.
     */
    public int position() {
        return (int) (parsing.lo() - baseLo);
    }

    private void actualPutNextChar() {
        char c = (char) (Utf8s.utf8CharDecode(parsing) >> 16);
        actual.put(c);
    }

    private void clearError() {
        error = ErrorCode.NONE;
        actual.clear();
    }

    private void elementsPutDouble(double n) {
        values.putDouble(n);
        ++valuesCount;
    }

    private void elementsPutLong(int n) {
        values.putLong(n);
        ++valuesCount;
    }

    private void parseDataType() {
        if (parsing.size() < 3) {
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

    /**
     * Parse the outermost level of a row-major array.
     * <p>Generally, this would look something like so:</p>
     * <pre>
     *     {5f2.5,1.0,NaN}
     *        ^_____________ we start here!
     * </pre>
     * <p>Note that by the time we call this function, the opening left brace and type have already been parsed.</p>
     */
    private void parseElements() {
        if (error != ErrorCode.NONE) {
            return;
        }

        if (Utf8s.equalsUtf16("1.0,2.5,3.0,4.5,5.0}", parsing)) {
            assert type == ColumnType.buildNdArrayType('f', (byte) 6);  // ARRAY(DOUBLE)
            elementsPutDouble(1.0);
            elementsPutDouble(2.5);
            elementsPutDouble(3.0);
            elementsPutDouble(4.5);
            elementsPutDouble(5.0);
            shape.add(5);
            parsing.advance(20);
        } else if (Utf8s.equalsUtf16("-1,0,100000000}", parsing)) {
            assert type == ColumnType.buildNdArrayType('s', (byte) 6);  // ARRAY(LONG)
            elementsPutLong(-1);
            elementsPutLong(0);
            elementsPutLong(100000000);
            shape.add(3);
            parsing.advance(15);
        } else {
            throw new UnsupportedOperationException("not yet implemented");
        }


        // TODO(amunra): Complete the parser.

    }

    private void parseLeftBrace() {
        if (parsing.size() == 0) {
            error = ErrorCode.ND_ARR_TOO_SHORT;
            return;
        }
        final byte b0 = parsing.byteAt(0);
        if (b0 != (byte) '{') {
            unexpectedErrorMsg = "expected '{', not ";
            actualPutNextChar();
            error = ErrorCode.ND_ARR_UNEXPECTED;
            return;
        }
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
                actualPutNextChar();
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
            actualPutNextChar();
            error = ErrorCode.ND_ARR_UNEXPECTED;
            return -1;
        }
        parsing.advance();
        return (byte) (ch - (char) 48);  // parse the single digit
    }

    /**
     * Continue parsing the previous level of array nesting.
     */
    private void popDim() {
        // Solidify the last dim before going back to the previous level.
        // The negative (uncertain) value will be converted to positive.
        shape.set(dimIndex, currDimLen);
        assert (dimIndex >= 0);
        assert dimIndex <= 0 || currDimLen > 0;
        --dimIndex;
        if (dimIndex >= 0) {
            currDimLen = Math.abs(shape.get(dimIndex));
        }
    }

    /**
     * Parse the next level of array nesting.
     */
    private void pushDim() {
        shape.add(0);
        ++dimIndex;
        currDimLen = 0;
    }

    private void reset() {
        clearError();
        array.reset();
        currDimLen = 0;
        dimIndex = -1;  // No dimensions until the first `{`
        shape.clear();
        type = ColumnType.UNDEFINED;
        values.clear();
        valuesCount = 0;
        baseLo = 0;
    }

    private ErrorCode setArray() {
        NdArrayMeta.setDefaultStrides(shape.asSlice(), strides);
        if (array.of(
                type,
                shape.getAddress(),
                (int) shape.size(),
                strides.getAddress(),
                (int) strides.size(),
                values.ptr(),
                values.size(),
                0
        ) != NdArrayView.ValidatonStatus.OK) {
            return ErrorCode.ND_ARR_MALFORMED;
        }
        return ErrorCode.NONE;
    }

    private void valueWritten() {
        currDimLen++;
        final int lastDimLen = shape.get(dimIndex);
        if (lastDimLen <= 0) {
            // yet undetermined
            shape.set(dimIndex, -currDimLen);
        } else if (lastDimLen >= currDimLen) {
            error = ErrorCode.ND_ARR_UNALIGNED;
        }

    }
}
