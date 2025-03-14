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

package io.questdb.cutlass.line;

import io.questdb.cairo.ColumnType;
import io.questdb.cairo.vm.api.MemoryA;
import io.questdb.client.Sender;
import io.questdb.cutlass.line.array.ArrayDataAppender;
import io.questdb.cutlass.line.array.ArrayShapeAppender;
import io.questdb.cutlass.line.array.DoubleArray;
import io.questdb.cutlass.line.array.FlattenArrayUtils;
import io.questdb.cutlass.line.array.LongArray;
import io.questdb.cutlass.line.tcp.LineTcpParser;
import io.questdb.std.BinarySequence;
import io.questdb.std.Long256;
import io.questdb.std.Unsafe;
import io.questdb.std.Vect;
import io.questdb.std.str.DirectUtf8Sequence;
import io.questdb.std.str.Utf8Sequence;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

public class LineTcpSenderV2 extends AbstractLineTcpSender implements MemoryA {
    public LineTcpSenderV2(LineChannel channel, int bufferCapacity) {
        super(channel, bufferCapacity);
    }

    @Override
    public Sender doubleArray(@NotNull CharSequence name, double[] values) {
        return arrayColumn(name, ColumnType.DOUBLE, (byte) 1, values,
                FlattenArrayUtils::putShapeToBuf,
                FlattenArrayUtils::putDataToBuf);
    }

    @Override
    public Sender doubleArray(@NotNull CharSequence name, double[][] values) {
        return arrayColumn(name, ColumnType.DOUBLE, (byte) 2, values,
                FlattenArrayUtils::putShapeToBuf,
                FlattenArrayUtils::putDataToBuf);
    }

    @Override
    public Sender doubleArray(@NotNull CharSequence name, double[][][] values) {
        return arrayColumn(name, ColumnType.DOUBLE, (byte) 3, values,
                FlattenArrayUtils::putShapeToBuf,
                FlattenArrayUtils::putDataToBuf);
    }

    @Override
    public Sender doubleArray(CharSequence name, DoubleArray values) {
        if (processNullArray(name, values)) {
            return this;
        }
        writeFieldName(name)
                .putAsciiInternal('=')
                .put(LineTcpParser.ENTITY_TYPE_ARRAY) // ARRAY binary format
                .put((byte) ColumnType.DOUBLE); // element type
        values.appendToBufPtr(this);
        return this;
    }

    @Override
    public Sender doubleColumn(CharSequence name, double value) {
        writeFieldName(name)
                .putAsciiInternal('=')
                .put(LineTcpParser.ENTITY_TYPE_DOUBLE);
        if (ptr + Double.BYTES >= hi) {
            send00();
        }
        Unsafe.getUnsafe().putDouble(ptr, value);
        ptr += Double.BYTES;
        return this;
    }

    @Override
    public long getAppendOffset() {
        return ptr;
    }

    @Override
    public long getExtendSegmentSize() {
        throw new UnsupportedOperationException();
    }

    @Override
    public void jumpTo(long offset) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Sender longArray(@NotNull CharSequence name, long[] values) {
        return arrayColumn(name, ColumnType.LONG, (byte) 1, values,
                FlattenArrayUtils::putShapeToBuf,
                FlattenArrayUtils::putDataToBuf);
    }

    @Override
    public Sender longArray(@NotNull CharSequence name, long[][] values) {
        return arrayColumn(name, ColumnType.LONG, (byte) 2, values,
                FlattenArrayUtils::putShapeToBuf,
                FlattenArrayUtils::putDataToBuf);
    }

    @Override
    public Sender longArray(@NotNull CharSequence name, long[][][] values) {
        return arrayColumn(name, ColumnType.LONG, (byte) 3, values,
                FlattenArrayUtils::putShapeToBuf,
                FlattenArrayUtils::putDataToBuf);
    }

    @Override
    public Sender longArray(@NotNull CharSequence name, LongArray values) {
        if (processNullArray(name, values)) {
            return this;
        }
        writeFieldName(name)
                .putAsciiInternal('=')
                .put(LineTcpParser.ENTITY_TYPE_ARRAY) // ARRAY binary format
                .put((byte) ColumnType.LONG); // element type
        values.appendToBufPtr(this);
        return this;
    }

    @Override
    public long putBin(BinarySequence value) {
        throw new UnsupportedOperationException();
    }

    @Override
    public long putBin(long from, long len) {
        putBlockOfBytes(from, len);
        return ptr;
    }

    @Override
    public void putBlockOfBytes(long from, long len) {
        while (len > 0) {
            if (ptr >= hi) {
                send00();
            }
            long copy = Math.min(len, hi - ptr);
            Vect.memcpy(ptr, from, copy);
            len -= copy;
            ptr += copy;
        }
    }

    @Override
    public void putBool(boolean value) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void putByte(byte value) {
        put(value);
    }

    @Override
    public void putChar(char value) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void putDouble(double value) {
        if (ptr + Double.BYTES >= hi) {
            send00();
        }
        Unsafe.getUnsafe().putDouble(ptr, value);
        ptr += Double.BYTES;
    }

    @Override
    public void putFloat(float value) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void putInt(int value) {
        if (ptr + Integer.BYTES >= hi) {
            send00();
        }
        Unsafe.getUnsafe().putInt(ptr, value);
        ptr += Integer.BYTES;
    }

    @Override
    public void putLong(long value) {
        if (ptr + Long.BYTES >= hi) {
            send00();
        }
        Unsafe.getUnsafe().putLong(ptr, value);
        ptr += Long.BYTES;
    }

    @Override
    public void putLong128(long lo, long hi) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void putLong256(long l0, long l1, long l2, long l3) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void putLong256(Long256 value) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void putLong256(@Nullable CharSequence hexString) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void putLong256(@NotNull CharSequence hexString, int start, int end) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void putLong256Utf8(@Nullable Utf8Sequence hexString) {
        throw new UnsupportedOperationException();
    }

    @Override
    public long putNullBin() {
        throw new UnsupportedOperationException();
    }

    @Override
    public long putNullStr() {
        throw new UnsupportedOperationException();
    }

    @Override
    public void putShort(short value) {
        throw new UnsupportedOperationException();
    }

    @Override
    public long putStr(CharSequence value) {
        throw new UnsupportedOperationException();
    }

    @Override
    public long putStr(char value) {
        throw new UnsupportedOperationException();
    }

    @Override
    public long putStr(CharSequence value, int pos, int len) {
        throw new UnsupportedOperationException();
    }

    @Override
    public long putStrUtf8(DirectUtf8Sequence value) {
        throw new UnsupportedOperationException();
    }

    @Override
    public long putVarchar(@NotNull Utf8Sequence value, int lo, int hi) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void skip(long bytes) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void truncate() {
        throw new UnsupportedOperationException();
    }

    @Override
    public void zeroMem(int length) {
        throw new UnsupportedOperationException();
    }

    private <T> Sender arrayColumn(
            CharSequence name,
            short columnType,
            byte nDims,
            T array,
            ArrayShapeAppender<T> shapeAppender,
            ArrayDataAppender<T> dataAppender
    ) {
        if (processNullArray(name, array)) {
            return this;
        }

        writeFieldName(name)
                .putAsciiInternal('=')
                .put(LineTcpParser.ENTITY_TYPE_ARRAY)
                .put((byte) columnType)
                .put(nDims);
        shapeAppender.append(this, array);
        dataAppender.append(this, array);
        return this;
    }

    private boolean processNullArray(CharSequence name, Object value) {
        if (value == null) {
            writeFieldName(name)
                    .putAsciiInternal('=') // binary format flag
                    .put(LineTcpParser.ENTITY_TYPE_ARRAY) // ARRAY binary format
                    .put((byte) ColumnType.NULL); // element type
            return true;
        }
        return false;
    }
}
