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
import io.questdb.cutlass.line.array.DoubleArray;
import io.questdb.cutlass.line.array.LongArray;
import io.questdb.cutlass.line.udp.UdpLineChannel;
import io.questdb.network.NetworkFacade;
import io.questdb.network.NetworkFacadeImpl;
import org.jetbrains.annotations.NotNull;

import java.time.Instant;
import java.time.temporal.ChronoUnit;

@SuppressWarnings("resource")
public class LineUdpSender extends AbstractLineSender {

    public LineUdpSender(int interfaceIPv4Address, int sendToIPv4Address, int sendToPort, int bufferCapacity, int ttl) {
        this(NetworkFacadeImpl.INSTANCE, interfaceIPv4Address, sendToIPv4Address, sendToPort, bufferCapacity, ttl);
    }

    public LineUdpSender(NetworkFacade nf, int interfaceIPv4Address, int sendToIPv4Address, int sendToPort, int capacity, int ttl) {
        super(new UdpLineChannel(nf, interfaceIPv4Address, sendToIPv4Address, sendToPort, ttl), capacity, Integer.MAX_VALUE);
    }

    @Override
    public final void at(long timestamp, ChronoUnit unit) {
        putAsciiInternal(' ').put(NanosTimestampDriver.INSTANCE.from(timestamp, unit));
        atNow();
    }

    @Override
    public final void at(Instant timestamp) {
        putAsciiInternal(' ').put(NanosTimestampDriver.INSTANCE.from(timestamp));
        atNow();
    }

    @Override
    public void cancelRow() {
        throw new LineSenderException("cancelRow() not supported by UDP transport");
    }

    @Override
    public Sender doubleArray(@NotNull CharSequence name, double[] values) {
        throw new LineSenderException("current protocol version does not support double-array");
    }

    @Override
    public Sender doubleArray(@NotNull CharSequence name, double[][] values) {
        throw new LineSenderException("current protocol version does not support double-array");
    }

    @Override
    public Sender doubleArray(@NotNull CharSequence name, double[][][] values) {
        throw new LineSenderException("current protocol version does not support double-array");
    }

    @Override
    public Sender doubleArray(CharSequence name, DoubleArray array) {
        throw new LineSenderException("current protocol version does not support double-array");
    }

    @Override
    public Sender doubleColumn(CharSequence name, double value) {
        writeFieldName(name).put(value);
        return this;
    }

    @Override
    public Sender longArray(@NotNull CharSequence name, long[] values) {
        throw new LineSenderException("current protocol version does not support long-array");
    }

    @Override
    public Sender longArray(@NotNull CharSequence name, long[][] values) {
        throw new LineSenderException("current protocol version does not support long-array");
    }

    @Override
    public Sender longArray(@NotNull CharSequence name, long[][][] values) {
        throw new LineSenderException("current protocol version does not support long-array");
    }

    @Override
    public Sender longArray(@NotNull CharSequence name, LongArray values) {
        throw new LineSenderException("current protocol version does not support long-array");
    }

    @Override
    public final AbstractLineSender timestampColumn(CharSequence name, long value, ChronoUnit unit) {
        writeFieldName(name).put(MicrosTimestampDriver.INSTANCE.from(value, unit)).putAsciiInternal('t');
        return this;
    }

    @Override
    public final AbstractLineSender timestampColumn(CharSequence name, Instant value) {
        writeFieldName(name).put(MicrosTimestampDriver.INSTANCE.from(value)).putAsciiInternal('t');
        return this;
    }
}
