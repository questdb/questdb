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
import io.questdb.cairo.arr.ArrayMeta;
import io.questdb.cairo.arr.ArrayViewImpl;
import io.questdb.std.DirectIntList;
import io.questdb.std.MemoryTag;
import io.questdb.std.Misc;
import io.questdb.std.QuietCloseable;
import io.questdb.std.Unsafe;
import org.jetbrains.annotations.NotNull;

/**
 * Parses an ND array native format used in ILP, which achieves higher write performance compared to the array text format.
 * <br>
 * The array native format is laid out as follows:
 * <pre>
 * +----------+----------+------------------------+--------------------+
 * | elemType |  dims    |       shapes           |    flat values     |
 * +----------+----------+------------------------+--------------------+
 * |  1 byte  | 6 bytes  |  $dims *4 bytes bits   |       1 byte       |
 * </pre>
 *
 * <strong>Won't validate length of flat values for performance reason</strong>
 */
public class ArrayNativeFormatParser implements QuietCloseable {

    private final DirectIntList strides = new DirectIntList(0, MemoryTag.NATIVE_ND_ARRAY_DBG2);
    private final ArrayViewImpl view = new ArrayViewImpl();
    private short elemType;
    private int dims;
    private long shapeAddr;
    private int nextBinaryPartExpectSize = 1;
    private BinaryPart binaryPart;

    @Override
    public void close() {
        nextBinaryPartExpectSize = 1;
        binaryPart = BinaryPart.ELEMENT_TYPE;
        Misc.free(strides);
    }

    public @NotNull ArrayViewImpl getView() {
        assert binaryPart == BinaryPart.FINISH;
        return view;
    }

    public boolean processNextBinaryPart(long addr) throws ArrayParser.ParseException {
        switch (binaryPart) {
            case ELEMENT_TYPE:
                elemType = Unsafe.getUnsafe().getByte(addr);
                if (!ColumnType.isSupportedArrayElementType(elemType)) {
                    throw ArrayParser.ParseException.invalidType(0);
                }
                binaryPart = BinaryPart.DIMS;
                nextBinaryPartExpectSize = 4;
                return false;
            case DIMS:
                dims = Unsafe.getUnsafe().getInt(addr);
                if (dims > ArrayParser.DIM_COUNT_LIMIT) {
                    throw ArrayParser.ParseException.unexpectedToken(0);
                }
                if (dims == 0) {
                    view.ofNull();
                    binaryPart = BinaryPart.FINISH;
                    return true;
                }
                binaryPart = BinaryPart.SHAPES;
                nextBinaryPartExpectSize = dims * 4;
                return false;
            case SHAPES:
                shapeAddr = addr;
                ArrayMeta.determineDefaultStrides(shapeAddr, dims, strides);
                binaryPart = BinaryPart.VALUES;
                nextBinaryPartExpectSize = strides.get(0) * Unsafe.getUnsafe().getInt(shapeAddr) * ColumnType.sizeOf(elemType);
                return false;
            case VALUES:
                int type = ColumnType.encodeArrayType(elemType, dims);
                view.of(
                        type,
                        shapeAddr,
                        dims,
                        strides.getAddress(),
                        (int) strides.size(),
                        addr,
                        nextBinaryPartExpectSize,
                        0,
                        (short) 0
                );
                binaryPart = BinaryPart.FINISH;
                return true;
        }

        throw ArrayParser.ParseException.invalidType(0);
    }

    public void reset() {
        nextBinaryPartExpectSize = 1;
        binaryPart = BinaryPart.ELEMENT_TYPE;
        strides.clear();
        view.reset();
    }

    public int getNextExpectSize() {
        return nextBinaryPartExpectSize;
    }

    public void shl(long delta) {
        if (binaryPart == BinaryPart.FINISH && !view.isNull()) {
            view.getValues().shl(delta);
            view.getShapes().shl(delta);
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
}
