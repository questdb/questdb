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

package io.questdb.cutlass.http;

import io.questdb.FactoryProvider;
import io.questdb.Metrics;
import io.questdb.cutlass.http.processors.JsonQueryProcessorConfiguration;
import io.questdb.cutlass.http.processors.LineHttpProcessorConfiguration;
import io.questdb.cutlass.http.processors.StaticContentProcessorConfiguration;
import io.questdb.metrics.Counter;
import io.questdb.metrics.LongGauge;
import io.questdb.network.EpollFacade;
import io.questdb.network.KqueueFacade;
import io.questdb.network.NetworkFacade;
import io.questdb.network.SelectFacade;
import io.questdb.std.ConcurrentCacheConfiguration;
import io.questdb.std.ObjHashSet;
import io.questdb.std.datetime.millitime.MillisecondClock;

import java.util.concurrent.atomic.AtomicReference;

public class HttpServerConfigurationWrapper implements HttpFullFatServerConfiguration {
    private final AtomicReference<HttpFullFatServerConfiguration> delegate = new AtomicReference<>();
    private final Metrics metrics;

    public HttpServerConfigurationWrapper(Metrics metrics) {
        this.metrics = metrics;
        delegate.set(null);
    }

    @Override
    public long getAcceptLoopTimeout() {
        return getDelegate().getAcceptLoopTimeout();
    }

    @Override
    public int getBindIPv4Address() {
        return getDelegate().getBindIPv4Address();
    }

    @Override
    public int getBindPort() {
        return getDelegate().getBindPort();
    }

    @Override
    public MillisecondClock getClock() {
        return getDelegate().getClock();
    }

    @Override
    public ConcurrentCacheConfiguration getConcurrentCacheConfiguration() {
        return getDelegate().getConcurrentCacheConfiguration();
    }

    @Override
    public LongGauge getConnectionCountGauge() {
        return metrics.httpMetrics().connectionCountGauge();
    }

    @Override
    public ObjHashSet<String> getContextPathExec() {
        return getDelegate().getContextPathExec();
    }

    @Override
    public ObjHashSet<String> getContextPathExport() {
        return getDelegate().getContextPathExport();
    }

    @Override
    public ObjHashSet<String> getContextPathILP() {
        return getDelegate().getContextPathILP();
    }

    @Override
    public ObjHashSet<String> getContextPathILPPing() {
        return getDelegate().getContextPathILPPing();
    }

    @Override
    public ObjHashSet<String> getContextPathImport() {
        return getDelegate().getContextPathImport();
    }

    @Override
    public ObjHashSet<String> getContextPathSettings() {
        return getDelegate().getContextPathSettings();
    }

    @Override
    public ObjHashSet<String> getContextPathSqlValidation() {
        return getDelegate().getContextPathSqlValidation();
    }

    @Override
    public ObjHashSet<String> getContextPathTableStatus() {
        return getDelegate().getContextPathTableStatus();
    }

    @Override
    public ObjHashSet<String> getContextPathWarnings() {
        return getDelegate().getContextPathWarnings();
    }

    @Override
    public String getContextPathWebConsole() {
        return getDelegate().getContextPathWebConsole();
    }

    @Override
    public String getDispatcherLogName() {
        return getDelegate().getDispatcherLogName();
    }

    @Override
    public EpollFacade getEpollFacade() {
        return getDelegate().getEpollFacade();
    }

    @Override
    public int getEventCapacity() {
        return getDelegate().getEventCapacity();
    }

    @Override
    public FactoryProvider getFactoryProvider() {
        return getDelegate().getFactoryProvider();
    }

    @Override
    public long getHeartbeatInterval() {
        return getDelegate().getHeartbeatInterval();
    }

    @Override
    public boolean getHint() {
        return getDelegate().getHint();
    }

    @Override
    public HttpContextConfiguration getHttpContextConfiguration() {
        return getDelegate().getHttpContextConfiguration();
    }

    @Override
    public int getIOQueueCapacity() {
        return getDelegate().getIOQueueCapacity();
    }

    @Override
    public int getInitialBias() {
        return getDelegate().getInitialBias();
    }

    @Override
    public int getInterestQueueCapacity() {
        return getDelegate().getInterestQueueCapacity();
    }

    @Override
    public JsonQueryProcessorConfiguration getJsonQueryProcessorConfiguration() {
        return getDelegate().getJsonQueryProcessorConfiguration();
    }

    @Override
    public KqueueFacade getKqueueFacade() {
        return getDelegate().getKqueueFacade();
    }

    @Override
    public int getLimit() {
        return getDelegate().getLimit();
    }

    @Override
    public LineHttpProcessorConfiguration getLineHttpProcessorConfiguration() {
        return getDelegate().getLineHttpProcessorConfiguration();
    }

    @Override
    public int getListenBacklog() {
        return getDelegate().getListenBacklog();
    }

    @Override
    public Metrics getMetrics() {
        return metrics;
    }

    @Override
    public long getNapThreshold() {
        return getDelegate().getNapThreshold();
    }

    @Override
    public int getNetRecvBufferSize() {
        return getDelegate().getNetRecvBufferSize();
    }

    @Override
    public int getNetSendBufferSize() {
        return getDelegate().getNetSendBufferSize();
    }

    @Override
    public NetworkFacade getNetworkFacade() {
        return getDelegate().getNetworkFacade();
    }

    @Override
    public String getPassword() {
        return getDelegate().getPassword();
    }

    @Override
    public boolean getPeerNoLinger() {
        return getDelegate().getPeerNoLinger();
    }

    @Override
    public String getPoolName() {
        return getDelegate().getPoolName();
    }

    @Override
    public long getQueueTimeout() {
        return getDelegate().getQueueTimeout();
    }

    @Override
    public int getRecvBufferSize() {
        return getDelegate().getRecvBufferSize();
    }

    @Override
    public byte getRequiredAuthType() {
        return getDelegate().getRequiredAuthType();
    }

    @Override
    public SelectFacade getSelectFacade() {
        return getDelegate().getSelectFacade();
    }

    @Override
    public int getSendBufferSize() {
        return getDelegate().getSendBufferSize();
    }

    @Override
    public long getSleepThreshold() {
        return getDelegate().getSleepThreshold();
    }

    @Override
    public long getSleepTimeout() {
        return getDelegate().getSleepTimeout();
    }

    @Override
    public StaticContentProcessorConfiguration getStaticContentProcessorConfiguration() {
        return getDelegate().getStaticContentProcessorConfiguration();
    }

    @Override
    public int getTestConnectionBufferSize() {
        return getDelegate().getTestConnectionBufferSize();
    }

    @Override
    public long getTimeout() {
        return getDelegate().getTimeout();
    }

    @Override
    public String getUsername() {
        return getDelegate().getUsername();
    }

    @Override
    public WaitProcessorConfiguration getWaitProcessorConfiguration() {
        return getDelegate().getWaitProcessorConfiguration();
    }

    @Override
    public int[] getWorkerAffinity() {
        return getDelegate().getWorkerAffinity();
    }

    @Override
    public int getWorkerCount() {
        return getDelegate().getWorkerCount();
    }

    @Override
    public long getYieldThreshold() {
        return getDelegate().getYieldThreshold();
    }

    @Override
    public boolean haltOnError() {
        return getDelegate().haltOnError();
    }

    @Override
    public boolean isAcceptingWrites() {
        return getDelegate().isAcceptingWrites();
    }

    @Override
    public boolean isDaemonPool() {
        return getDelegate().isDaemonPool();
    }

    @Override
    public boolean isEnabled() {
        return getDelegate().isEnabled();
    }

    @Override
    public boolean isPessimisticHealthCheckEnabled() {
        return getDelegate().isPessimisticHealthCheckEnabled();
    }

    @Override
    public boolean isQueryCacheEnabled() {
        return getDelegate().isQueryCacheEnabled();
    }

    @Override
    public boolean isSettingsReadOnly() {
        return getDelegate().isSettingsReadOnly();
    }

    @Override
    public Counter listenerStateChangeCounter() {
        return getDelegate().listenerStateChangeCounter();
    }

    @Override
    public boolean preAllocateBuffers() {
        return getDelegate().preAllocateBuffers();
    }

    public void setDelegate(HttpFullFatServerConfiguration delegate) {
        this.delegate.set(delegate);
    }

    @Override
    public int workerPoolPriority() {
        return getDelegate().workerPoolPriority();
    }

    protected HttpFullFatServerConfiguration getDelegate() {
        return delegate.get();
    }
}
