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
import io.questdb.cairo.DefaultCairoConfiguration;
import io.questdb.cutlass.http.DefaultHttpServerConfiguration;
import io.questdb.cutlass.http.HttpFullFatServerConfiguration;
import io.questdb.cutlass.http.HttpServerConfiguration;
import io.questdb.cutlass.line.tcp.DefaultLineTcpReceiverConfiguration;
import io.questdb.cutlass.line.tcp.LineTcpReceiverConfiguration;
import io.questdb.cutlass.line.udp.DefaultLineUdpReceiverConfiguration;
import io.questdb.cutlass.line.udp.LineUdpReceiverConfiguration;
import io.questdb.cutlass.pgwire.DefaultPGConfiguration;
import io.questdb.cutlass.pgwire.PGConfiguration;
import io.questdb.metrics.DefaultMetricsConfiguration;
import io.questdb.metrics.MetricsConfiguration;
import io.questdb.mp.WorkerPoolConfiguration;

public class DefaultServerConfiguration implements ServerConfiguration {
    private final DefaultCairoConfiguration cairoConfiguration;
    private final DefaultHttpServerConfiguration httpServerConfiguration;
    private final DefaultLineTcpReceiverConfiguration lineTcpReceiverConfiguration;
    private final DefaultLineUdpReceiverConfiguration lineUdpReceiverConfiguration = new DefaultLineUdpReceiverConfiguration();
    private final WorkerPoolConfiguration matViewRefreshPoolConfiguration;
    private final WorkerPoolConfiguration exportPoolConfiguration;
    private final WorkerPoolConfiguration viewCompilerPoolConfiguration;
    private final DefaultMemoryConfiguration memoryConfiguration = new DefaultMemoryConfiguration();
    private final DefaultMetricsConfiguration metricsConfiguration = new DefaultMetricsConfiguration();
    private final DefaultPGConfiguration pgWireConfiguration = new DefaultPGConfiguration();
    private final PublicPassthroughConfiguration publicPassthroughConfiguration = new DefaultPublicPassthroughConfiguration();
    private final DefaultWorkerPoolConfiguration sharedPoolNetworkConfiguration;
    private final DefaultWorkerPoolConfiguration sharedPoolQueryConfiguration;
    private final DefaultWorkerPoolConfiguration sharedPoolWriteConfiguration;
    private final WorkerPoolConfiguration walApplyPoolConfiguration;

    public DefaultServerConfiguration(CharSequence dbRoot, CharSequence installRoot) {
        this.cairoConfiguration = new DefaultCairoConfiguration(dbRoot, installRoot);
        this.lineTcpReceiverConfiguration = new DefaultLineTcpReceiverConfiguration(cairoConfiguration);
        this.httpServerConfiguration = new DefaultHttpServerConfiguration(cairoConfiguration);
        this.sharedPoolNetworkConfiguration = new DefaultWorkerPoolConfiguration("shared_network");
        this.sharedPoolQueryConfiguration = new DefaultWorkerPoolConfiguration("shared_query");
        this.sharedPoolWriteConfiguration = new DefaultWorkerPoolConfiguration("shared_write");
        this.matViewRefreshPoolConfiguration = new DefaultWorkerPoolConfiguration("mat_view_refresh");
        this.exportPoolConfiguration = new DefaultWorkerPoolConfiguration("export");
        this.viewCompilerPoolConfiguration = new DefaultWorkerPoolConfiguration("view_compiler");
        this.walApplyPoolConfiguration = new DefaultWorkerPoolConfiguration("wal_apply");
    }

    public DefaultServerConfiguration(CharSequence dbRoot) {
        this(dbRoot, null);
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
    public WorkerPoolConfiguration getViewCompilerPoolConfiguration() {
        return viewCompilerPoolConfiguration;
    }

    @Override
    public WorkerPoolConfiguration getMatViewRefreshPoolConfiguration() {
        return matViewRefreshPoolConfiguration;
    }

    @Override
    public WorkerPoolConfiguration getExportPoolConfiguration() {
        return exportPoolConfiguration;
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
    public PGConfiguration getPGWireConfiguration() {
        return pgWireConfiguration;
    }

    @Override
    public PublicPassthroughConfiguration getPublicPassthroughConfiguration() {
        return publicPassthroughConfiguration;
    }

    @Override
    public WorkerPoolConfiguration getSharedWorkerPoolNetworkConfiguration() {
        return sharedPoolNetworkConfiguration;
    }

    @Override
    public WorkerPoolConfiguration getSharedWorkerPoolQueryConfiguration() {
        return sharedPoolQueryConfiguration;
    }

    @Override
    public WorkerPoolConfiguration getSharedWorkerPoolWriteConfiguration() {
        return sharedPoolWriteConfiguration;
    }

    @Override
    public WorkerPoolConfiguration getWalApplyPoolConfiguration() {
        return walApplyPoolConfiguration;
    }

    private record DefaultWorkerPoolConfiguration(String name) implements WorkerPoolConfiguration {

        @Override
        public String getPoolName() {
            return name;
        }

        @Override
        public int getWorkerCount() {
            return 2;
        }
    }
}
