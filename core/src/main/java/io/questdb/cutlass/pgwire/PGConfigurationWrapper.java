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

package io.questdb.cutlass.pgwire;

import io.questdb.FactoryProvider;
import io.questdb.Metrics;
import io.questdb.cairo.sql.SqlExecutionCircuitBreakerConfiguration;
import io.questdb.metrics.Counter;
import io.questdb.metrics.LongGauge;
import io.questdb.network.EpollFacade;
import io.questdb.network.KqueueFacade;
import io.questdb.network.NetworkFacade;
import io.questdb.network.SelectFacade;
import io.questdb.std.ConcurrentCacheConfiguration;
import io.questdb.std.Rnd;
import io.questdb.std.datetime.DateLocale;
import io.questdb.std.datetime.millitime.MillisecondClock;

import java.util.concurrent.atomic.AtomicReference;

public class PGConfigurationWrapper implements PGConfiguration {
    private final AtomicReference<PGConfiguration> delegate = new AtomicReference<>();
    private final Metrics metrics;

    public PGConfigurationWrapper(Metrics metrics) {
        this.metrics = metrics;
        delegate.set(null);
    }

    @Override
    public long getAcceptLoopTimeout() {
        return getDelegate().getAcceptLoopTimeout();
    }

    @Override
    public int getBinParamCountCapacity() {
        return getDelegate().getBinParamCountCapacity();
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
    public int getCharacterStoreCapacity() {
        return getDelegate().getCharacterStoreCapacity();
    }

    @Override
    public int getCharacterStorePoolCapacity() {
        return getDelegate().getCharacterStorePoolCapacity();
    }

    @Override
    public SqlExecutionCircuitBreakerConfiguration getCircuitBreakerConfiguration() {
        return getDelegate().getCircuitBreakerConfiguration();
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
        return metrics.pgWireMetrics().connectionCountGauge();
    }

    @Override
    public int getConnectionPoolInitialCapacity() {
        return getDelegate().getConnectionPoolInitialCapacity();
    }

    @Override
    public DateLocale getDefaultDateLocale() {
        return getDelegate().getDefaultDateLocale();
    }

    @Override
    public String getDefaultPassword() {
        return getDelegate().getDefaultPassword();
    }

    @Override
    public String getDefaultUsername() {
        return getDelegate().getDefaultUsername();
    }

    @Override
    public String getDispatcherLogName() {
        return getDelegate().getDispatcherLogName();
    }

    @Override
    public boolean getDumpNetworkTraffic() {
        return getDelegate().getDumpNetworkTraffic();
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
    public int getForceRecvFragmentationChunkSize() {
        return getDelegate().getForceRecvFragmentationChunkSize();
    }

    @Override
    public int getForceSendFragmentationChunkSize() {
        return getDelegate().getForceSendFragmentationChunkSize();
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
    public int getInitialBias() {
        return getDelegate().getInitialBias();
    }

    @Override
    public int getInsertCacheBlockCount() {
        return getDelegate().getInsertCacheBlockCount();
    }

    @Override
    public int getInsertCacheRowCount() {
        return getDelegate().getInsertCacheRowCount();
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
    public int getMaxBlobSizeOnQuery() {
        return getDelegate().getMaxBlobSizeOnQuery();
    }

    @Override
    public Metrics getMetrics() {
        return metrics;
    }

    @Override
    public int getNamedStatementCacheCapacity() {
        return getDelegate().getNamedStatementCacheCapacity();
    }

    @Override
    public int getNamedStatementLimit() {
        return getDelegate().getNamedStatementLimit();
    }

    @Override
    public int getNamesStatementPoolCapacity() {
        return getDelegate().getNamesStatementPoolCapacity();
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
    public boolean getPeerNoLinger() {
        return getDelegate().getPeerNoLinger();
    }

    @Override
    public int getPendingWritersCacheSize() {
        return getDelegate().getPendingWritersCacheSize();
    }

    @Override
    public int getPipelineCapacity() {
        return getDelegate().getPipelineCapacity();
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
    public Rnd getRandom() {
        return getDelegate().getRandom();
    }

    @Override
    public String getReadOnlyPassword() {
        return getDelegate().getReadOnlyPassword();
    }

    @Override
    public String getReadOnlyUsername() {
        return getDelegate().getReadOnlyUsername();
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
    public String getServerVersion() {
        return getDelegate().getServerVersion();
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
    public int getTestConnectionBufferSize() {
        return getDelegate().getTestConnectionBufferSize();
    }

    @Override
    public long getTimeout() {
        return getDelegate().getTimeout();
    }

    @Override
    public int getUpdateCacheBlockCount() {
        return getDelegate().getUpdateCacheBlockCount();
    }

    @Override
    public int getUpdateCacheRowCount() {
        return getDelegate().getUpdateCacheRowCount();
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
    public boolean isDaemonPool() {
        return getDelegate().isDaemonPool();
    }

    @Override
    public boolean isEnabled() {
        return getDelegate().isEnabled();
    }

    @Override
    public boolean isInsertCacheEnabled() {
        return getDelegate().isInsertCacheEnabled();
    }

    @Override
    public boolean isReadOnlyUserEnabled() {
        return getDelegate().isReadOnlyUserEnabled();
    }

    @Override
    public boolean isSelectCacheEnabled() {
        return getDelegate().isSelectCacheEnabled();
    }

    @Override
    public boolean isUpdateCacheEnabled() {
        return getDelegate().isUpdateCacheEnabled();
    }

    @Override
    public Counter listenerStateChangeCounter() {
        return getDelegate().listenerStateChangeCounter();
    }

    @Override
    public boolean readOnlySecurityContext() {
        return getDelegate().readOnlySecurityContext();
    }

    public void setDelegate(PGConfiguration delegate) {
        this.delegate.set(delegate);
    }

    @Override
    public int workerPoolPriority() {
        return getDelegate().workerPoolPriority();
    }

    protected PGConfiguration getDelegate() {
        return delegate.get();
    }
}
