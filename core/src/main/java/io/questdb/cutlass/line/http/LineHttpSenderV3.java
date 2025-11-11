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
import io.questdb.client.Sender;
import io.questdb.cutlass.http.client.HttpClient;
import io.questdb.cutlass.line.LineSenderException;
import io.questdb.cutlass.line.tcp.LineTcpParser;
import io.questdb.std.Decimal128;
import io.questdb.std.Decimal256;
import io.questdb.std.Decimal64;
import io.questdb.std.IntList;
import io.questdb.std.Numbers;
import io.questdb.std.NumericException;
import io.questdb.std.ObjList;
import io.questdb.std.Rnd;
import org.jetbrains.annotations.Nullable;

public class LineHttpSenderV3 extends LineHttpSenderV2 {

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
    public Sender decimalColumn(CharSequence name, CharSequence value) {
        try {
            // Validate that the value is properly formatted
            Numbers.parseDouble(value);
        } catch (NumericException e) {
            throw new LineSenderException("Failed to parse sent decimal value: " + value, e);
        }
        var request = writeFieldName(name);
        request.put(value).putAscii('d');
        return this;
    }

    @Override
    public Sender decimalColumn(CharSequence name, Decimal256 value) {
        if (value.isNull()) {
            return this;
        }

        var request = writeFieldName(name)
                .putAscii('=')
                .put(LineTcpParser.ENTITY_TYPE_DECIMAL)
                .put((byte) value.getScale())
                .put((byte) 32); // Length
        request.putLong(Long.reverseBytes(value.getHh()));
        request.putLong(Long.reverseBytes(value.getHl()));
        request.putLong(Long.reverseBytes(value.getLh()));
        request.putLong(Long.reverseBytes(value.getLl()));
        return this;
    }

    @Override
    public Sender decimalColumn(CharSequence name, Decimal128 value) {
        if (value.isNull()) {
            return this;
        }

        var request = writeFieldName(name)
                .putAscii('=')
                .put(LineTcpParser.ENTITY_TYPE_DECIMAL)
                .put((byte) value.getScale())
                .put((byte) 16); // Length
        request.putLong(Long.reverseBytes(value.getHigh()));
        request.putLong(Long.reverseBytes(value.getLow()));
        return this;
    }

    @Override
    public Sender decimalColumn(CharSequence name, Decimal64 value) {
        if (value.isNull()) {
            return this;
        }

        var request = writeFieldName(name)
                .putAscii('=')
                .put(LineTcpParser.ENTITY_TYPE_DECIMAL)
                .put((byte) value.getScale())
                .put((byte) 8); // Length
        request.putLong(Long.reverseBytes(value.getValue()));
        return this;
    }
}