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

package io.questdb;

import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.DefaultCairoConfiguration;
import io.questdb.cairo.wal.DefaultWalApplyWorkerPoolConfiguration;
import io.questdb.cutlass.http.DefaultHttpServerConfiguration;
import io.questdb.cutlass.http.HttpFullFatServerConfiguration;
import io.questdb.cutlass.http.HttpServerConfiguration;
import io.questdb.cutlass.line.tcp.DefaultLineTcpReceiverConfiguration;
import io.questdb.cutlass.line.tcp.LineTcpReceiverConfiguration;
import io.questdb.cutlass.line.udp.DefaultLineUdpReceiverConfiguration;
import io.questdb.cutlass.line.udp.LineUdpReceiverConfiguration;
import io.questdb.cutlass.pgwire.DefaultPGWireConfiguration;
import io.questdb.cutlass.pgwire.PGWireConfiguration;
import io.questdb.metrics.DefaultMetricsConfiguration;
import io.questdb.metrics.MetricsConfiguration;
import io.questdb.mp.WorkerPoolConfiguration;

public class DefaultServerConfiguration implements ServerConfiguration {
    private final DefaultCairoConfiguration cairoConfiguration;
    private final DefaultHttpServerConfiguration httpServerConfiguration = new DefaultHttpServerConfiguration();
    private final DefaultLineTcpReceiverConfiguration lineTcpReceiverConfiguration = new DefaultLineTcpReceiverConfiguration();
    private final DefaultLineUdpReceiverConfiguration lineUdpReceiverConfiguration = new DefaultLineUdpReceiverConfiguration();
    private final DefaultMemoryConfiguration memoryConfiguration = new DefaultMemoryConfiguration();
    private final DefaultMetricsConfiguration metricsConfiguration = new DefaultMetricsConfiguration();
    private final DefaultPGWireConfiguration pgWireConfiguration = new DefaultPGWireConfiguration();
    private final PublicPassthroughConfiguration publicPassthroughConfiguration = new DefaultPublicPassthroughConfiguration();
    private final WorkerPoolConfiguration walApplyPoolConfiguration = new DefaultWalApplyWorkerPoolConfiguration();

    public DefaultServerConfiguration(CharSequence root) {
        this.cairoConfiguration = new DefaultCairoConfiguration(root);
    }

    @Override
    public CairoConfiguration getCairoConfiguration() {
        return cairoConfiguration;
    }

    @Override
    public FactoryProvider getFactoryProvider() {
        return DefaultFactoryProvider.INSTANCE;
    }

    @Override
    public HttpServerConfiguration getHttpMinServerConfiguration() {
        return null;
    }

    @Override
    public HttpFullFatServerConfiguration getHttpServerConfiguration() {
        return httpServerConfiguration;
    }

    @Override
    public LineTcpReceiverConfiguration getLineTcpReceiverConfiguration() {
        return lineTcpReceiverConfiguration;
    }

    @Override
    public LineUdpReceiverConfiguration getLineUdpReceiverConfiguration() {
        return lineUdpReceiverConfiguration;
    }

    @Override
    public MemoryConfiguration getMemoryConfiguration() {
        return memoryConfiguration;
    }

    @Override
    public Metrics getMetrics() {
        return Metrics.ENABLED;
    }

    @Override
    public MetricsConfiguration getMetricsConfiguration() {
        return metricsConfiguration;
    }

    @Override
    public PGWireConfiguration getPGWireConfiguration() {
        return pgWireConfiguration;
    }

    @Override
    public PublicPassthroughConfiguration getPublicPassthroughConfiguration() {
        return publicPassthroughConfiguration;
    }

    @Override
    public WorkerPoolConfiguration getWalApplyPoolConfiguration() {
        return walApplyPoolConfiguration;
    }

    @Override
    public WorkerPoolConfiguration getWorkerPoolConfiguration() {
        return httpServerConfiguration;
    }
}
