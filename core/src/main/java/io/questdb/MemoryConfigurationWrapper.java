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

import java.util.concurrent.atomic.AtomicReference;

public class MemoryConfigurationWrapper implements MemoryConfiguration {
    private final AtomicReference<MemoryConfiguration> delegate = new AtomicReference<>();

    public MemoryConfigurationWrapper() {
        delegate.set(null);
    }

    @Override
    public long getRamUsageLimitBytes() {
        return getDelegate().getRamUsageLimitBytes();
    }

    @Override
    public long getRamUsageLimitPercent() {
        return getDelegate().getRamUsageLimitPercent();
    }

    @Override
    public long getResolvedRamUsageLimitBytes() {
        return getDelegate().getResolvedRamUsageLimitBytes();
    }

    @Override
    public long getTotalSystemMemory() {
        return getDelegate().getTotalSystemMemory();
    }

    public void setDelegate(MemoryConfiguration delegate) {
        this.delegate.set(delegate);
    }

    protected MemoryConfiguration getDelegate() {
        return delegate.get();
    }
}
