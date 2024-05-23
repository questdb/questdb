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

import io.questdb.std.Os;

public class MemoryConfigurationImpl implements MemoryConfiguration {
    private final long configuredLimitMib;
    private final long configuredLimitPercent;
    private final long ramUsageLimit;
    private final long totalSystemMemory;

    public MemoryConfigurationImpl(long configuredLimitMib, long configuredLimitPercent) {
        this.configuredLimitMib = configuredLimitMib;
        this.configuredLimitPercent = configuredLimitPercent;
        this.totalSystemMemory = Os.getMemorySizeFromMXBean();
        assert totalSystemMemory >= -1 : "Os.getMemorySizeFromMXBean() reported negative memory size";
        long limitMib = Math.min(Long.MAX_VALUE >> 20, Math.max(configuredLimitMib, 0)) << 20;
        long limitPercent = (configuredLimitPercent > 0 && configuredLimitPercent <= 100 && totalSystemMemory != -1)
                ? totalSystemMemory / 100 * configuredLimitPercent
                : 0;
        this.ramUsageLimit = (limitMib == 0) ? limitPercent
                : (limitPercent == 0) ? limitMib
                : Math.min(limitMib, limitPercent);
    }

    @Override
    public long getEffectiveRamUsageLimit() {
        return ramUsageLimit;
    }

    @Override
    public long getRamUsageLimitMib() {
        return configuredLimitMib;
    }

    @Override
    public long getRamUsageLimitPercent() {
        return configuredLimitPercent;
    }

    @Override
    public long getTotalSystemMemory() {
        return totalSystemMemory;
    }
}
