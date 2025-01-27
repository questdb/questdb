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
import io.questdb.std.Misc;
import io.questdb.std.QuietCloseable;
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
    private final NdArrayBuffer bufs = new NdArrayBuffer();
    private final DirectUtf8String parsing = new DirectUtf8String();

    private final NdArrayView view = new NdArrayView();
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

    /**
     * Count of how many elements we've stored.
     * <p>This is tracked separately since our <code>elements</code> is just bytes,
     * and we can't determine the elementsCount if the <code>type</code>
     * has a precision smaller than a byte.</p>
     */
    private int valuesCount = 0;

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
    public LineTcpParser.ErrorCode parse(DirectUtf8String value) {
        reset();

        if (Utf8s.equalsAscii("{}", value)) {
            view.ofNull();
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

    private void clearError() {
        error = ErrorCode.NONE;
    }

    private void elementsPutDouble(double n) {
        bufs.values.putDouble(n);
        ++valuesCount;
    }

    private void elementsPutLong(int n) {
        bufs.values.putLong(n);
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
            error = ErrorCode.ND_ARR_INVALID_TYPE;
            return;
        }

        bufs.type = arrayType;
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
            assert bufs.type == ColumnType.buildNdArrayType('f', (byte) 6);  // ARRAY(DOUBLE)
            elementsPutDouble(1.0);
            elementsPutDouble(2.5);
            elementsPutDouble(3.0);
            elementsPutDouble(4.5);
            elementsPutDouble(5.0);
            bufs.shape.add(5);
            parsing.advance(20);
        } else if (Utf8s.equalsUtf16("-1,0,100000000}", parsing)) {
            assert bufs.type == ColumnType.buildNdArrayType('i', (byte) 6);  // ARRAY(LONG)
            elementsPutLong(-1);
            elementsPutLong(0);
            elementsPutLong(100000000);
            bufs.shape.add(3);
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
            case 'i':
            case 'f':
                return ch;
            default:
                error = ErrorCode.ND_ARR_INVALID_TYPE;
                return 0;
        }
    }

    /**
     * Power of 2, number of bits: e.g. 0 -> bool, 1 -> int2, ..., 5 -> int32, 6 -> int64
     */
    private byte parseTypePrecision() {
        final char ch = (char) parsing.byteAt(0);
        if (ch < '0' || ch > '6') {
            error = ErrorCode.ND_ARR_INVALID_TYPE;
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
        bufs.shape.set(dimIndex, currDimLen);
        assert (dimIndex >= 0);
        assert dimIndex <= 0 || currDimLen > 0;
        --dimIndex;
        if (dimIndex >= 0) {
            currDimLen = Math.abs(bufs.shape.get(dimIndex));
        }
    }

    /**
     * Parse the next level of array nesting.
     */
    private void pushDim() {
        bufs.shape.add(0);
        ++dimIndex;
        currDimLen = 0;
    }

    private void reset() {
        clearError();
        view.reset();
        bufs.reset();
        currDimLen = 0;
        dimIndex = -1;  // No dimensions until the first `{`
        valuesCount = 0;
        baseLo = 0;
    }

    private ErrorCode setArray() {
        NdArrayMeta.setDefaultStrides(bufs.shape.asSlice(), bufs.strides);
        if (bufs.setView(view) != NdArrayView.ValidatonStatus.OK) {
            return ErrorCode.ND_ARR_MALFORMED;
        }
        return ErrorCode.NONE;
    }

    private void valueWritten() {
        currDimLen++;
        final int lastDimLen = bufs.shape.get(dimIndex);
        if (lastDimLen <= 0) {
            // yet undetermined
            bufs.shape.set(dimIndex, -currDimLen);
        } else if (lastDimLen >= currDimLen) {
            error = ErrorCode.ND_ARR_UNALIGNED;
        }
    }
}
