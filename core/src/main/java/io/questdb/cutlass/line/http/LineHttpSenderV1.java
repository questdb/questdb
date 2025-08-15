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
import io.questdb.cutlass.line.array.DoubleArray;
import io.questdb.cutlass.line.array.LongArray;
import org.jetbrains.annotations.NotNull;

public class LineHttpSenderV1 extends AbstractLineHttpSender {

    protected LineHttpSenderV1(String host,
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
                               long minRequestThroughput,
                               long flushIntervalNanos) {
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
                minRequestThroughput,
                flushIntervalNanos);
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
        writeFieldName(name)
                .put(value);
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
}
