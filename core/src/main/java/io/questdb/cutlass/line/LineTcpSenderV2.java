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
import io.questdb.client.Sender;
import io.questdb.cutlass.line.array.ArrayBufferAppender;
import io.questdb.cutlass.line.array.ArrayDataAppender;
import io.questdb.cutlass.line.array.ArrayShapeAppender;
import io.questdb.cutlass.line.array.DoubleArray;
import io.questdb.cutlass.line.array.FlattenArrayUtils;
import io.questdb.cutlass.line.array.LongArray;
import io.questdb.cutlass.line.tcp.LineTcpParser;
import io.questdb.cutlass.line.tcp.PlainTcpLineChannel;
import io.questdb.network.NetworkFacadeImpl;
import io.questdb.std.Unsafe;
import io.questdb.std.Vect;
import org.jetbrains.annotations.NotNull;

public class LineTcpSenderV2 extends AbstractLineTcpSender implements ArrayBufferAppender {
    public LineTcpSenderV2(LineChannel channel, int bufferCapacity, int maxNameLength) {
        super(channel, bufferCapacity, maxNameLength);
    }

    /**
     * Create a new LineTcpSender.
     * <br>
     * IP address is encoded as <code>int</code> obtained via {@link io.questdb.network.Net#parseIPv4(CharSequence)}
     * <br>
     * This is meant to be used for testing only, it's not something most users want to use.
     * See {@link Sender} instead
     *
     * @param ip             IP address of a server
     * @param port           port where a server is listening
     * @param bufferCapacity capacity of an internal buffer in bytes
     * @return LineTcpSender instance of LineTcpSender
     */
    public static LineTcpSenderV2 newSender(int ip, int port, int bufferCapacity) {
        PlainTcpLineChannel channel = new PlainTcpLineChannel(NetworkFacadeImpl.INSTANCE, ip, port, bufferCapacity * 2);
        try {
            return new LineTcpSenderV2(channel, bufferCapacity, 127);
        } catch (Throwable t) {
            channel.close();
            throw t;
        }
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
    public Sender doubleArray(CharSequence name, DoubleArray array) {
        if (processNullArray(name, array)) {
            return this;
        }
        writeFieldName(name)
                .putAsciiInternal('=')
                .put(LineTcpParser.ENTITY_TYPE_ARRAY) // ARRAY binary format
                .put((byte) ColumnType.DOUBLE); // element type
        array.appendToBufPtr(this);
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
    public void putByte(byte value) {
        put(value);
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
