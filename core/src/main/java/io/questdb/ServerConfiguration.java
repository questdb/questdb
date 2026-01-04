/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2026 QuestDB
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

package io.questdb;

import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.CairoEngine;
import io.questdb.cutlass.http.HttpFullFatServerConfiguration;
import io.questdb.cutlass.http.HttpServerConfiguration;
import io.questdb.cutlass.line.tcp.LineTcpReceiverConfiguration;
import io.questdb.cutlass.line.udp.LineUdpReceiverConfiguration;
import io.questdb.cutlass.pgwire.PGConfiguration;
import io.questdb.metrics.MetricsConfiguration;
import io.questdb.mp.WorkerPoolConfiguration;
import io.questdb.std.str.Utf8StringSink;

public interface ServerConfiguration {
    String OSS = "OSS";

    /**
     * Exports subset of configuration options into the provided sink. The config is exported
     * int JSON format.
     *
     * @param sink the target sink
     */
    default void exportConfiguration(Utf8StringSink sink) {
        sink.putAscii("\"config\":{");
        // bitwise OR below is intentional, always want to append passthrough config
        if (
                getCairoConfiguration().exportConfiguration(sink)
                        | getPublicPassthroughConfiguration().exportConfiguration(sink)
        ) {
            sink.clear(sink.size() - 1);
        }
        sink.putAscii("},");
    }

    CairoConfiguration getCairoConfiguration();

    FactoryProvider getFactoryProvider();

    HttpServerConfiguration getHttpMinServerConfiguration();

    HttpFullFatServerConfiguration getHttpServerConfiguration();

    LineTcpReceiverConfiguration getLineTcpReceiverConfiguration();

    LineUdpReceiverConfiguration getLineUdpReceiverConfiguration();

    WorkerPoolConfiguration getMatViewRefreshPoolConfiguration();

    WorkerPoolConfiguration getExportPoolConfiguration();

    MemoryConfiguration getMemoryConfiguration();

    Metrics getMetrics();

    MetricsConfiguration getMetricsConfiguration();

    PGConfiguration getPGWireConfiguration();

    PublicPassthroughConfiguration getPublicPassthroughConfiguration();

    default String getReleaseType() {
        return OSS;
    }

    WorkerPoolConfiguration getSharedWorkerPoolNetworkConfiguration();

    WorkerPoolConfiguration getSharedWorkerPoolQueryConfiguration();

    WorkerPoolConfiguration getSharedWorkerPoolWriteConfiguration();

    // used to detect configuration reloads
    default long getVersion() {
        return 0;
    }

    WorkerPoolConfiguration getViewCompilerPoolConfiguration();

    WorkerPoolConfiguration getWalApplyPoolConfiguration();

    default void init(CairoEngine engine, FreeOnExit freeOnExit) {
    }
}
