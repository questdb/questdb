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

package io.questdb.cutlass.line.http;

import io.questdb.ClientTlsConfiguration;
import io.questdb.HttpClientConfiguration;
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.MicrosTimestampDriver;
import io.questdb.cairo.NanosTimestampDriver;
import io.questdb.client.Sender;
import io.questdb.cutlass.http.client.HttpClient;
import io.questdb.cutlass.line.array.ArrayDataAppender;
import io.questdb.cutlass.line.array.ArrayShapeAppender;
import io.questdb.cutlass.line.array.DoubleArray;
import io.questdb.cutlass.line.array.FlattenArrayUtils;
import io.questdb.cutlass.line.array.LongArray;
import io.questdb.cutlass.line.tcp.LineTcpParser;
import io.questdb.std.IntList;
import io.questdb.std.ObjList;
import io.questdb.std.Rnd;
import io.questdb.std.datetime.microtime.MicrosecondClockImpl;
import io.questdb.std.datetime.nanotime.NanosecondClockImpl;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.time.Instant;
import java.time.temporal.ChronoUnit;

public class LineHttpSenderV2 extends AbstractLineHttpSender {

    public LineHttpSenderV2(String host,
                            int port,
                            HttpClientConfiguration clientConfiguration,
                            ClientTlsConfiguration tlsConfig,
                            int autoFlushRows,
                            String authToken,
                            String username,
                            String password,
                            int maxNameLength,
                            long maxRetriesNanos,
                            int maxBackoffMillis,
                            long minRequestThroughput,
                            long flushIntervalNanos) {
        super(host,
                port,
                clientConfiguration,
                tlsConfig,
                autoFlushRows,
                authToken,
                username,
                password,
                maxNameLength,
                maxRetriesNanos,
                maxBackoffMillis,
                minRequestThroughput,
                flushIntervalNanos,
                new Rnd(NanosecondClockImpl.INSTANCE.getTicks(), MicrosecondClockImpl.INSTANCE.getTicks()));
    }

    public LineHttpSenderV2(ObjList<String> hosts,
                            IntList ports,
                            String path,
                            HttpClientConfiguration clientConfiguration,
                            ClientTlsConfiguration tlsConfig,
                            @Nullable HttpClient client,
                            int autoFlushRows,
                            String authToken,
                            String username,
                            String password,
                            int maxNameLength,
                            long maxRetriesNanos,
                            int maxBackoffMillis,
                            long minRequestThroughput,
                            long flushIntervalNanos,
                            int currentAddressIndex,
                            Rnd rnd) {
        super(hosts,
                ports,
                path,
                clientConfiguration,
                tlsConfig,
                client,
                autoFlushRows,
                authToken,
                username,
                password,
                maxNameLength,
                maxRetriesNanos,
                maxBackoffMillis,
                minRequestThroughput,
                flushIntervalNanos,
                currentAddressIndex,
                rnd);
    }

    @SuppressWarnings("unused")
    protected LineHttpSenderV2(String host,
                               int port,
                               String path,
                               HttpClientConfiguration clientConfiguration,
                               ClientTlsConfiguration tlsConfig,
                               HttpClient client,
                               int autoFlushRows,
                               String authToken,
                               String username,
                               String password,
                               int maxNameLength,
                               long maxRetriesNanos,
                               int maxBackoffMillis,
                               long minRequestThroughput,
                               long flushIntervalNanos,
                               Rnd rnd) {
        super(host,
                port,
                path,
                clientConfiguration,
                tlsConfig,
                client,
                autoFlushRows,
                authToken,
                username,
                password,
                maxNameLength,
                maxRetriesNanos,
                maxBackoffMillis,
                minRequestThroughput,
                flushIntervalNanos,
                rnd);
    }

    @Override
    public void at(long timestamp, ChronoUnit unit) {
        request.putAscii(' ');
        putTimestamp(timestamp, unit);
        atNow();
    }

    @Override
    public void at(Instant timestamp) {
        request.putAscii(' ');
        putTimestamp(timestamp);
        atNow();
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
                .putAscii('=') // binary format flag
                .put(LineTcpParser.ENTITY_TYPE_ARRAY) // ND_ARRAY binary format
                .put((byte) ColumnType.DOUBLE); // element type
        array.appendToBufPtr(request);
        return this;
    }

    @Override
    public Sender doubleColumn(CharSequence name, double value) {
        writeFieldName(name)
                .putAscii('=')
                .put(LineTcpParser.ENTITY_TYPE_DOUBLE)
                .putDouble(value);
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
                .putAscii('=') // binary format flag
                .put(LineTcpParser.ENTITY_TYPE_ARRAY) // ND_ARRAY binary format
                .put((byte) ColumnType.LONG); // element type
        values.appendToBufPtr(request);
        return this;
    }

    @Override
    public Sender timestampColumn(CharSequence name, long value, ChronoUnit unit) {
        writeFieldName(name);
        putTimestamp(value, unit);
        return this;
    }

    @Override
    public Sender timestampColumn(CharSequence name, Instant value) {
        writeFieldName(name);
        putTimestamp(value);
        return this;
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
                .putAscii('=') // binary format flag
                .put(LineTcpParser.ENTITY_TYPE_ARRAY) // ND_ARRAY binary format
                .put((byte) columnType) // element type
                .put(nDims); // dims.
        shapeAppender.append(request, array);
        dataAppender.append(request, array);
        return this;
    }

    private boolean processNullArray(CharSequence name, Object value) {
        if (value == null) {
            writeFieldName(name)
                    .putAscii('=') // binary format flag
                    .put(LineTcpParser.ENTITY_TYPE_ARRAY) // ND_ARRAY binary format
                    .put((byte) ColumnType.NULL); // element type
            return true;
        }
        return false;
    }

    private void putTimestamp(long timestamp, ChronoUnit unit) {
        // nanos sent as nanos, everything else is sent as micros
        switch (unit) {
            case NANOS -> request.put(timestamp).putAscii('n');
            case MICROS -> request.put(timestamp).putAscii('t');
            default ->
                // unit needs conversion to micros
                    request.put(MicrosTimestampDriver.INSTANCE.from(timestamp, unit)).putAscii('t');
        }
    }

    private void putTimestamp(Instant timestamp) {
        // always send as nanos as long as it fits in a long
        try {
            request.put(NanosTimestampDriver.INSTANCE.from(timestamp)).putAscii('n');
        } catch (ArithmeticException e) {
            // timestamp does not fit in a long, sending as micros
            request.put(MicrosTimestampDriver.INSTANCE.from(timestamp)).putAscii('t');
        }
    }
}
