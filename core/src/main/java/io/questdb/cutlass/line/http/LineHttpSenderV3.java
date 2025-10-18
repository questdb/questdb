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
import io.questdb.client.Sender;
import io.questdb.cutlass.http.client.HttpClient;
import io.questdb.cutlass.line.array.ArrayDataAppender;
import io.questdb.cutlass.line.array.ArrayShapeAppender;
import io.questdb.cutlass.line.array.DoubleArray;
import io.questdb.cutlass.line.array.FlattenArrayUtils;
import io.questdb.cutlass.line.array.LongArray;
import io.questdb.cutlass.line.tcp.LineTcpParser;
import io.questdb.std.Decimal256;
import io.questdb.std.IntList;
import io.questdb.std.ObjList;
import io.questdb.std.Rnd;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

public class LineHttpSenderV3 extends LineHttpSenderV2 {

    public LineHttpSenderV3(String host,
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
                flushIntervalNanos);
    }

    public LineHttpSenderV3(ObjList<String> hosts,
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
    protected LineHttpSenderV3(String host,
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
    public Sender decimalColumnText(CharSequence name, Decimal256 value) {
        var request = writeFieldName(name);
        if (value.isNull()) {
            request.put("NaNd");
        } else {
            request.put(value).putAscii('d');
        }
        return this;
    }

    @Override
    public Sender decimalColumn(CharSequence name, Decimal256 value) {
        var request = writeFieldName(name)
                .putAscii('=')
                .put(LineTcpParser.ENTITY_TYPE_DECIMAL)
                .put((byte) value.getScale());
        if (value.isNull()) {
            request.put((byte) 0); // Length (0 -> null)
            return this;
        }

        request.put((byte) 32); // Length
        request.putLong(Long.reverseBytes(value.getHh()));
        request.putLong(Long.reverseBytes(value.getHl()));
        request.putLong(Long.reverseBytes(value.getLh()));
        request.putLong(Long.reverseBytes(value.getLl()));
        return this;
    }
}
