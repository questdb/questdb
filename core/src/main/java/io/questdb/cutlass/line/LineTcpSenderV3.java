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

import io.questdb.client.Sender;
import io.questdb.cutlass.line.array.ArrayBufferAppender;
import io.questdb.cutlass.line.tcp.LineTcpParser;
import io.questdb.cutlass.line.tcp.PlainTcpLineChannel;
import io.questdb.network.NetworkFacadeImpl;
import io.questdb.std.Decimal128;
import io.questdb.std.Decimal256;
import io.questdb.std.Decimal64;
import io.questdb.std.Numbers;
import io.questdb.std.NumericException;

@SuppressWarnings("resource")
public class LineTcpSenderV3 extends LineTcpSenderV2 implements ArrayBufferAppender {
    public LineTcpSenderV3(LineChannel channel, int bufferCapacity, int maxNameLength) {
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
    public static LineTcpSenderV3 newSender(int ip, int port, int bufferCapacity) {
        PlainTcpLineChannel channel = new PlainTcpLineChannel(NetworkFacadeImpl.INSTANCE, ip, port, bufferCapacity * 2);
        try {
            return new LineTcpSenderV3(channel, bufferCapacity, 127);
        } catch (Throwable t) {
            channel.close();
            throw t;
        }
    }

    @Override
    public Sender decimalColumn(CharSequence name, Decimal256 value) {
        if (value.isNull()) {
            return this;
        }
        writeFieldName(name)
                .putAsciiInternal('=')
                .put(LineTcpParser.ENTITY_TYPE_DECIMAL)
                .put((byte) value.getScale())
                .put((byte) 32); // Length
        putLong(Long.reverseBytes(value.getHh()));
        putLong(Long.reverseBytes(value.getHl()));
        putLong(Long.reverseBytes(value.getLh()));
        putLong(Long.reverseBytes(value.getLl()));
        return this;
    }

    @Override
    public Sender decimalColumn(CharSequence name, Decimal128 value) {
        if (value.isNull()) {
            return this;
        }
        writeFieldName(name)
                .putAsciiInternal('=')
                .put(LineTcpParser.ENTITY_TYPE_DECIMAL)
                .put((byte) value.getScale())
                .put((byte) 16); // Length
        putLong(Long.reverseBytes(value.getHigh()));
        putLong(Long.reverseBytes(value.getLow()));
        return this;
    }

    @Override
    public Sender decimalColumn(CharSequence name, Decimal64 value) {
        if (value.isNull()) {
            return this;
        }
        writeFieldName(name)
                .putAsciiInternal('=')
                .put(LineTcpParser.ENTITY_TYPE_DECIMAL)
                .put((byte) value.getScale())
                .put((byte) 8); // Length
        putLong(Long.reverseBytes(value.getValue()));
        return this;
    }

    @Override
    public Sender decimalColumn(CharSequence name, CharSequence value) {
        try {
            // Validate that the value is properly formatted
            Numbers.parseDouble(value);
        } catch (NumericException e) {
            throw new LineSenderException("Failed to parse sent decimal value: " + value, e);
        }
        writeFieldName(name);
        put(value).putAscii('d');
        return this;
    }
}
