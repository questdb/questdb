/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2026 QuestDB
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
import io.questdb.cairo.arr.BorrowedArray;
import io.questdb.std.QuietCloseable;
import io.questdb.std.ThreadLocal;
import io.questdb.std.Unsafe;
import org.jetbrains.annotations.NotNull;

import static io.questdb.cutlass.line.tcp.LineTcpParser.ErrorCode.*;

/**
 * Parses the binary N-dimensional array format used in ILP.
 * <br>
 * An array is laid out as follows:
 * <pre>
 * +----------+----------+------------------------+--------------------+
 * | elemType |  nDims   |         shape          |    flat values     |
 * +----------+----------+------------------------+--------------------+
 * |  1 byte  |  1 byte  |    $nDims * 4 bytes    |                    |
 * </pre>
 *
 * <strong>Won't validate length of flat values for performance reason</strong>
 */
public class ArrayBinaryFormatParser implements QuietCloseable {

    private final BorrowedArray array = new BorrowedArray();
    private short elemType;
    private int nDims;
    private int nextBinaryPartExpectSize = 1;
    private long shapeAddr;
    private ParserState state;

    @Override
    public void close() {
        nextBinaryPartExpectSize = 1;
        state = ParserState.ELEMENT_TYPE;
    }

    public @NotNull BorrowedArray getArray() {
        assert state == ParserState.FINISH;
        return array;
    }

    public int getNextExpectSize() {
        return nextBinaryPartExpectSize;
    }

    public boolean processNextBinaryPart(long addr) throws ParseException {
        switch (state) {
            case ELEMENT_TYPE:
                elemType = Unsafe.getUnsafe().getByte(addr);
                // TODO(puzpuzpuz): remove this check completely once we update all clients to skip fields
                //  for null arrays instead of sending this code; this code is unreliable as it changes
                //  each time we add a new column type
                if (elemType == ColumnType.NULL) {
                    array.ofNull();
                    state = ParserState.FINISH;
                    return true;
                }
                if (!ColumnType.isSupportedArrayElementType(elemType)) {
                    throw ParseException.invalidType();
                }
                state = ParserState.N_DIMS;
                nextBinaryPartExpectSize = 1;
                return false;
            case N_DIMS:
                nDims = Unsafe.getUnsafe().getByte(addr) & 0xFF; // treat as unsigned
                if (nDims > ColumnType.ARRAY_NDIMS_LIMIT) {
                    throw ParseException.tooManyDims();
                }
                if (nDims == 0) {
                    array.ofNull();
                    state = ParserState.FINISH;
                    return true;
                }
                state = ParserState.SHAPE;
                nextBinaryPartExpectSize = nDims * Integer.BYTES;
                return false;
            case SHAPE:
                shapeAddr = addr;
                int n = nDims;
                for (long i = 0; i < n; i++) {
                    final int dimLength = Unsafe.getUnsafe().getInt(addr + i * Integer.BYTES);
                    if (dimLength == 0) {
                        int type = ColumnType.encodeArrayType(elemType, nDims);
                        array.of(type, shapeAddr, 0L, 0);
                        state = ParserState.FINISH;
                        return true;
                    }
                }
                nextBinaryPartExpectSize = ColumnType.sizeOf(elemType);
                for (long i = 0; i < n; i++) {
                    final int dimLength = Unsafe.getUnsafe().getInt(addr + i * Integer.BYTES);
                    try {
                        nextBinaryPartExpectSize = Math.multiplyExact(nextBinaryPartExpectSize, dimLength);
                    } catch (ArithmeticException e) {
                        throw ParseException.tooLarge();
                    }
                }
                state = ParserState.VALUES;
                return false;
            case VALUES:
                int type = ColumnType.encodeArrayType(elemType, nDims);
                array.of(type, shapeAddr, addr, nextBinaryPartExpectSize);
                state = ParserState.FINISH;
                return true;
            default:
                throw ParseException.invalidType();
        }
    }

    public void reset() {
        nextBinaryPartExpectSize = 1;
        state = ParserState.ELEMENT_TYPE;
        array.clear();
    }

    public void shl(long delta) {
        if (state == ParserState.FINISH && !array.isNull()) {
            array.borrowedFlatView().shl(delta);
        } else if (state == ParserState.VALUES) {
            this.shapeAddr -= delta;
        }
    }

    private enum ParserState {
        ELEMENT_TYPE,
        N_DIMS,
        SHAPE,
        VALUES,
        FINISH
    }

    public static class ParseException extends Exception {
        private static final io.questdb.std.ThreadLocal<ParseException> tlException = new ThreadLocal<>(ParseException::new);
        private LineTcpParser.ErrorCode errorCode;

        public static @NotNull ParseException invalidType() {
            return tlException.get().errorCode(ARRAY_INVALID_TYPE);
        }

        public static @NotNull ParseException tooLarge() {
            return tlException.get().errorCode(ARRAY_TOO_LARGE);
        }

        public static @NotNull ParseException tooManyDims() {
            return tlException.get().errorCode(ARRAY_TOO_MANY_DIMENSIONS);
        }

        public ParseException errorCode(LineTcpParser.ErrorCode errorCode) {
            this.errorCode = errorCode;
            return this;
        }

        public LineTcpParser.ErrorCode errorCode() {
            return errorCode;
        }
    }
}
