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

package io.questdb.cutlass.line.tcp;

import io.questdb.FactoryProvider;
import io.questdb.Metrics;
import io.questdb.cairo.CairoConfiguration;
import io.questdb.metrics.Counter;
import io.questdb.metrics.LongGauge;
import io.questdb.mp.WorkerPoolConfiguration;
import io.questdb.network.EpollFacade;
import io.questdb.network.KqueueFacade;
import io.questdb.network.NetworkFacade;
import io.questdb.network.SelectFacade;
import io.questdb.std.FilesFacade;
import io.questdb.std.datetime.Clock;
import io.questdb.std.datetime.millitime.MillisecondClock;

import java.util.concurrent.atomic.AtomicReference;

public class LineTcpReceiverConfigurationWrapper implements LineTcpReceiverConfiguration {
    private final AtomicReference<LineTcpReceiverConfiguration> delegate = new AtomicReference<>();
    private final Metrics metrics;

    public LineTcpReceiverConfigurationWrapper(Metrics metrics) {
        this.metrics = metrics;
        delegate.set(null);
    }

    @Override
    public long getAcceptLoopTimeout() {
        return getDelegate().getAcceptLoopTimeout();
    }

    @Override
    public String getAuthDB() {
        return getDelegate().getAuthDB();
    }

    @Override
    public boolean getAutoCreateNewColumns() {
        return getDelegate().getAutoCreateNewColumns();
    }

    @Override
    public boolean getAutoCreateNewTables() {
        return getDelegate().getAutoCreateNewTables();
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
    public CairoConfiguration getCairoConfiguration() {
        return getDelegate().getCairoConfiguration();
    }

    @Override
    public MillisecondClock getClock() {
        return getDelegate().getClock();
    }

    @Override
    public long getCommitInterval() {
        return getDelegate().getCommitInterval();
    }

    @Override
    public long getCommitIntervalDefault() {
        return getDelegate().getCommitIntervalDefault();
    }

    @Override
    public double getCommitIntervalFraction() {
        return getDelegate().getCommitIntervalFraction();
    }

    @Override
    public LongGauge getConnectionCountGauge() {
        return metrics.lineMetrics().tcpConnectionCountGauge();
    }

    @Override
    public int getConnectionPoolInitialCapacity() {
        return getDelegate().getConnectionPoolInitialCapacity();
    }

    @Override
    public short getDefaultColumnTypeForFloat() {
        return getDelegate().getDefaultColumnTypeForFloat();
    }

    @Override
    public short getDefaultColumnTypeForInteger() {
        return getDelegate().getDefaultColumnTypeForInteger();
    }

    @Override
    public int getDefaultColumnTypeForTimestamp() {
        return getDelegate().getDefaultColumnTypeForTimestamp();
    }

    @Override
    public int getDefaultPartitionBy() {
        return getDelegate().getDefaultPartitionBy();
    }

    @Override
    public boolean getDisconnectOnError() {
        return getDelegate().getDisconnectOnError();
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
    public FilesFacade getFilesFacade() {
        return getDelegate().getFilesFacade();
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
    public int getIOQueueCapacity() {
        return getDelegate().getIOQueueCapacity();
    }

    @Override
    public WorkerPoolConfiguration getNetworkWorkerPoolConfiguration() {
        return getDelegate().getNetworkWorkerPoolConfiguration();
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
    public KqueueFacade getKqueueFacade() {
        return getDelegate().getKqueueFacade();
    }

    @Override
    public int getLimit() {
        return getDelegate().getLimit();
    }

    @Override
    public int getListenBacklog() {
        return getDelegate().getListenBacklog();
    }

    @Override
    public long getMaintenanceInterval() {
        return getDelegate().getMaintenanceInterval();
    }

    @Override
    public int getMaxFileNameLength() {
        return getDelegate().getMaxFileNameLength();
    }

    @Override
    public int getMaxMeasurementSize() {
        return getDelegate().getMaxMeasurementSize();
    }

    @Override
    public long getMaxRecvBufferSize() {
        return getDelegate().getMaxRecvBufferSize();
    }

    @Override
    public Metrics getMetrics() {
        return metrics;
    }

    @Override
    public Clock getMicrosecondClock() {
        return getDelegate().getMicrosecondClock();
    }

    @Override
    public MillisecondClock getMillisecondClock() {
        return getDelegate().getMillisecondClock();
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
    public boolean getPeerNoLinger() {
        return getDelegate().getPeerNoLinger();
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
    public SelectFacade getSelectFacade() {
        return getDelegate().getSelectFacade();
    }

    @Override
    public int getSendBufferSize() {
        return getDelegate().getSendBufferSize();
    }

    @Override
    public long getSymbolCacheWaitBeforeReload() {
        return getDelegate().getSymbolCacheWaitBeforeReload();
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
    public byte getTimestampUnit() {
        return getDelegate().getTimestampUnit();
    }

    @Override
    public long getWriterIdleTimeout() {
        return getDelegate().getWriterIdleTimeout();
    }

    @Override
    public int getWriterQueueCapacity() {
        return getDelegate().getWriterQueueCapacity();
    }

    @Override
    public WorkerPoolConfiguration getWriterWorkerPoolConfiguration() {
        return getDelegate().getWriterWorkerPoolConfiguration();
    }

    @Override
    public boolean isEnabled() {
        return getDelegate().isEnabled();
    }

    @Override
    public boolean isStringToCharCastAllowed() {
        return getDelegate().isStringToCharCastAllowed();
    }

    @Override
    public boolean isUseLegacyStringDefault() {
        return getDelegate().isUseLegacyStringDefault();
    }

    @Override
    public Counter listenerStateChangeCounter() {
        return getDelegate().listenerStateChangeCounter();
    }

    @Override
    public boolean logMessageOnError() {
        return getDelegate().logMessageOnError();
    }

    public void setDelegate(LineTcpReceiverConfiguration delegate) {
        this.delegate.set(delegate);
    }

    protected LineTcpReceiverConfiguration getDelegate() {
        return delegate.get();
    }
}
