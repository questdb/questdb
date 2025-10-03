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

import io.questdb.cairo.MicrosTimestampDriver;
import io.questdb.cairo.NanosTimestampDriver;
import io.questdb.client.Sender;
import io.questdb.cutlass.line.tcp.PlainTcpLineChannel;
import io.questdb.network.NetworkFacadeImpl;

import java.time.Instant;
import java.time.temporal.ChronoUnit;

/**
 * LineTcpSender is for testing purposes only. It has error-prone API and comes with no API guarantees
 * If you are looking for an ILP client for your application use {@link Sender} instead.
 */
public abstract class AbstractLineTcpSender extends AbstractLineSender {

    /**
     * @param ip             IP address of a server
     * @param port           port where a server is listening
     * @param bufferCapacity capacity of an internal buffer in bytes
     * @deprecated use {@link LineTcpSenderV2#newSender(int, int, int)} instead.
     * <br>
     * IP address is encoded as <code>int</code> obtained via {@link io.questdb.network.Net#parseIPv4(CharSequence)}
     */
    @Deprecated
    public AbstractLineTcpSender(int ip, int port, int bufferCapacity) {
        super(new PlainTcpLineChannel(NetworkFacadeImpl.INSTANCE, ip, port, bufferCapacity * 2), bufferCapacity, 127);
    }

    public AbstractLineTcpSender(LineChannel channel, int bufferCapacity, int maxNameLength) {
        super(channel, bufferCapacity, maxNameLength);
    }

    @Override
    public final void at(long timestamp, ChronoUnit unit) {
        putAsciiInternal(' ');
        putTimestamp(timestamp, unit);
        atNow();
    }

    @Override
    public final void at(Instant timestamp) {
        putAsciiInternal(' ');
        putTimestamp(timestamp);
        atNow();
    }

    @Override
    public void cancelRow() {
        throw new LineSenderException("cancelRow() not supported by TCP transport");
    }

    @Override
    public void flush() {
        validateNotClosed();
        sendAll();
    }

    @Override
    public final AbstractLineSender timestampColumn(CharSequence name, long value, ChronoUnit unit) {
        writeFieldName(name);
        putTimestamp(value, unit);
        return this;
    }

    @Override
    public final AbstractLineSender timestampColumn(CharSequence name, Instant value) {
        writeFieldName(name);
        putTimestamp(value);
        return this;
    }

    private void putTimestamp(long timestamp, ChronoUnit unit) {
        // nanos sent as nanos, everything else is sent as micros
        switch (unit) {
            case NANOS:
                put(timestamp).putAsciiInternal('n');
                break;
            case MICROS:
                put(timestamp).putAsciiInternal('t');
                break;
            default:
                // unit needs conversion to micros
                put(MicrosTimestampDriver.INSTANCE.from(timestamp, unit)).putAsciiInternal('t');
        }
    }

    private void putTimestamp(Instant timestamp) {
        // always send as nanos as long as it fits in a long 
        try {
            put(NanosTimestampDriver.INSTANCE.from(timestamp)).putAsciiInternal('n');
        } catch (ArithmeticException e) {
            // value does not fit in a long, sending as micros
            put(MicrosTimestampDriver.INSTANCE.from(timestamp)).putAsciiInternal('t');
        }
    }

    @Override
    protected void send00() {
        sendAll();
    }
}
