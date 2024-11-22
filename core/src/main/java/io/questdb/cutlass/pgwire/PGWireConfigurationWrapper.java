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

package io.questdb.cutlass.pgwire;

import io.questdb.FactoryProvider;
import io.questdb.cairo.sql.SqlExecutionCircuitBreakerConfiguration;
import io.questdb.network.IODispatcherConfiguration;
import io.questdb.network.NetworkFacade;
import io.questdb.std.Rnd;
import io.questdb.std.datetime.DateLocale;

public class PGWireConfigurationWrapper implements PGWireConfiguration {
    private final PGWireConfiguration delegate;

    protected PGWireConfigurationWrapper() {
        delegate = null;
    }

    @Override
    public int getBinParamCountCapacity() {
        return getDelegate().getBinParamCountCapacity();
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
    public IODispatcherConfiguration getDispatcherConfiguration() {
        return getDelegate().getDispatcherConfiguration();
    }

    @Override
    public boolean getDumpNetworkTraffic() {
        return getDelegate().getDumpNetworkTraffic();
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
    public int getInsertCacheBlockCount() {
        return getDelegate().getInsertCacheBlockCount();
    }

    @Override
    public int getInsertCacheRowCount() {
        return getDelegate().getInsertCacheRowCount();
    }

    @Override
    public int getMaxBlobSizeOnQuery() {
        return getDelegate().getMaxBlobSizeOnQuery();
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
    public NetworkFacade getNetworkFacade() {
        return getDelegate().getNetworkFacade();
    }

    @Override
    public int getPendingWritersCacheSize() {
        return getDelegate().getPendingWritersCacheSize();
    }

    @Override
    public String getPoolName() {
        return getDelegate().getPoolName();
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
    public int getSelectCacheBlockCount() {
        return getDelegate().getSelectCacheBlockCount();
    }

    @Override
    public int getSelectCacheRowCount() {
        return getDelegate().getSelectCacheRowCount();
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
    public boolean isLegacyModeEnabled() {
        return getDelegate().isLegacyModeEnabled();
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
    public boolean readOnlySecurityContext() {
        return getDelegate().readOnlySecurityContext();
    }

    @Override
    public int workerPoolPriority() {
        return getDelegate().workerPoolPriority();
    }

    protected PGWireConfiguration getDelegate() {
        return delegate;
    }
}
