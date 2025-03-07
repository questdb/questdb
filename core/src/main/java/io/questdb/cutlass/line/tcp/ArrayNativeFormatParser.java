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
import io.questdb.cairo.arr.BorrowedFlatArrayView;
import io.questdb.cairo.arr.MmappedArray;
import io.questdb.std.QuietCloseable;
import io.questdb.std.ThreadLocal;
import io.questdb.std.Unsafe;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import static io.questdb.cutlass.line.tcp.LineTcpParser.ErrorCode.ND_ARR_INVALID_TYPE;
import static io.questdb.cutlass.line.tcp.LineTcpParser.ErrorCode.ND_ARR_LARGE_DIMENSIONS;

/**
 * Parses an ND array native format used in ILP, which achieves higher write performance compared to the array text format.
 * <br>
 * The array native format is laid out as follows:
 * <pre>
 * +----------+----------+------------------------+--------------------+
 * | elemType |  dims    |       shapes           |    flat values     |
 * +----------+----------+------------------------+--------------------+
 * |  1 byte  |  1 byte  |     $dims * 4 bytes    |                    |
 * </pre>
 *
 * <strong>Won't validate length of flat values for performance reason</strong>
 */
public class ArrayNativeFormatParser implements QuietCloseable {

    private final MmappedArray view = new MmappedArray();
    private BinaryPart binaryPart;
    private int dims;
    private short elemType;
    private int nextBinaryPartExpectSize = 1;
    private long shapeAddr;

    @Override
    public void close() {
        nextBinaryPartExpectSize = 1;
        binaryPart = BinaryPart.ELEMENT_TYPE;
    }

    public @Nullable MmappedArray getArray() {
        assert binaryPart == BinaryPart.FINISH;
        return view.isNull() ? null : view;
    }

    public int getNextExpectSize() {
        return nextBinaryPartExpectSize;
    }

    public boolean processNextBinaryPart(long addr) throws ParseException {
        switch (binaryPart) {
            case ELEMENT_TYPE:
                elemType = Unsafe.getUnsafe().getByte(addr);
                if (elemType == ColumnType.NULL) {
                    view.ofNull();
                    binaryPart = BinaryPart.FINISH;
                    return true;
                }
                if (!ColumnType.isSupportedArrayElementType(elemType)) {
                    throw ParseException.invalidType();
                }

                binaryPart = BinaryPart.DIMS;
                nextBinaryPartExpectSize = 1;
                return false;
            case DIMS:
                dims = Unsafe.getUnsafe().getByte(addr);
                if (dims > ColumnType.ARRAY_NDIMS_LIMIT) {
                    throw ParseException.largeDims();
                }
                if (dims == 0) {
                    view.ofNull();
                    binaryPart = BinaryPart.FINISH;
                    return true;
                }

                binaryPart = BinaryPart.SHAPES;
                nextBinaryPartExpectSize = dims * Integer.BYTES;
                return false;
            case SHAPES:
                shapeAddr = addr;
                nextBinaryPartExpectSize = ColumnType.sizeOf(elemType);
                for (int i = 0; i < dims; ++i) {
                    final int dimLength = Unsafe.getUnsafe().getInt(addr + (long) i * Integer.BYTES);
                    nextBinaryPartExpectSize = Math.multiplyExact(nextBinaryPartExpectSize, dimLength);
                }
                if (nextBinaryPartExpectSize == 0) {
                    view.ofNull();
                    binaryPart = BinaryPart.FINISH;
                    return true;
                }

                binaryPart = BinaryPart.VALUES;
                return false;
            case VALUES:
                int type = ColumnType.encodeArrayType(elemType, dims);
                view.of(
                        type,
                        dims,
                        shapeAddr,
                        addr,
                        nextBinaryPartExpectSize
                );
                binaryPart = BinaryPart.FINISH;
                return true;
        }

        throw ParseException.invalidType();
    }

    public void reset() {
        nextBinaryPartExpectSize = 1;
        binaryPart = BinaryPart.ELEMENT_TYPE;
        view.reset();
    }

    public void shl(long delta) {
        if (binaryPart == BinaryPart.FINISH && !view.isNull()) {
            ((BorrowedFlatArrayView) view.flatView()).shl(delta);
        } else if (binaryPart == BinaryPart.VALUES) {
            this.shapeAddr -= delta;
        }
    }

    private enum BinaryPart {
        ELEMENT_TYPE,
        DIMS,
        SHAPES,
        VALUES,
        FINISH
    }

    public static class ParseException extends Exception {
        private static final io.questdb.std.ThreadLocal<ParseException> tlException = new ThreadLocal<>(ParseException::new);
        private LineTcpParser.ErrorCode errorCode;

        public static @NotNull ParseException invalidType() {
            return tlException.get().errorCode(ND_ARR_INVALID_TYPE);
        }

        public static @NotNull ParseException largeDims() {
            return tlException.get().errorCode(ND_ARR_LARGE_DIMENSIONS);
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
