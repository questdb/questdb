/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2023 QuestDB
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
import io.questdb.cutlass.http.HttpMinServerConfiguration;
import io.questdb.cutlass.http.HttpServerConfiguration;
import io.questdb.cutlass.line.tcp.LineTcpReceiverConfiguration;
import io.questdb.cutlass.line.udp.LineUdpReceiverConfiguration;
import io.questdb.cutlass.pgwire.PGWireConfiguration;
import io.questdb.metrics.MetricsConfiguration;
import io.questdb.mp.WorkerPoolConfiguration;

public interface ServerConfiguration {

    CairoConfiguration getCairoConfiguration();

    FactoryProvider getFactoryProvider();

    HttpMinServerConfiguration getHttpMinServerConfiguration();

    HttpServerConfiguration getHttpServerConfiguration();

    LineTcpReceiverConfiguration getLineTcpReceiverConfiguration();

    LineUdpReceiverConfiguration getLineUdpReceiverConfiguration();

    MetricsConfiguration getMetricsConfiguration();

    PGWireConfiguration getPGWireConfiguration();

    WorkerPoolConfiguration getWalApplyPoolConfiguration();

    WorkerPoolConfiguration getWorkerPoolConfiguration();

    default void init(CairoEngine engine, FreeOnExit freeOnExit) {
    }

    default boolean isIlpEnabled() {
        return true;
    }
}
