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

package io.questdb.cutlass.http;

import io.questdb.FactoryProvider;
import io.questdb.network.IODispatcherConfiguration;

public class HttpMinServerConfigurationWrapper implements HttpMinServerConfiguration {
    private final HttpMinServerConfiguration delegate;

    protected HttpMinServerConfigurationWrapper() {
        delegate = null;
    }

    @Override
    public IODispatcherConfiguration getDispatcherConfiguration() {
        return getDelegate().getDispatcherConfiguration();
    }

    @Override
    public FactoryProvider getFactoryProvider() {
        return getDelegate().getFactoryProvider();
    }

    @Override
    public HttpContextConfiguration getHttpContextConfiguration() {
        return getDelegate().getHttpContextConfiguration();
    }

    @Override
    public long getNapThreshold() {
        return getDelegate().getNapThreshold();
    }

    @Override
    public String getPoolName() {
        return getDelegate().getPoolName();
    }

    @Override
    public byte getRequiredAuthType() {
        return getDelegate().getRequiredAuthType();
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
    public boolean preAllocateBuffers() {
        return getDelegate().preAllocateBuffers();
    }

    @Override
    public int workerPoolPriority() {
        return getDelegate().workerPoolPriority();
    }

    protected HttpMinServerConfiguration getDelegate() {
        return delegate;
    }
}
