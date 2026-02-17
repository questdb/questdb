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

import io.questdb.std.Os;

public class MemoryConfigurationImpl implements MemoryConfiguration {
    private final long configuredLimitBytes;
    private final long configuredLimitPercent;
    private final long ramUsageLimit;
    private final long totalSystemMemory;

    public MemoryConfigurationImpl(long configuredLimitBytes, long configuredLimitPercent) {
        this.configuredLimitBytes = configuredLimitBytes;
        this.configuredLimitPercent = configuredLimitPercent;
        this.totalSystemMemory = Os.getMemorySizeFromMXBean();
        assert totalSystemMemory >= -1 : "Os.getMemorySizeFromMXBean() reported negative memory size";
        this.ramUsageLimit = resolveRamUsageLimit(configuredLimitBytes, configuredLimitPercent, totalSystemMemory);
    }

    public static long resolveRamUsageLimit(
            long configuredLimitBytes, long configuredLimitPercent, long totalSystemMemory
    ) {
        long limitByPercent = (totalSystemMemory != -1) ? totalSystemMemory / 100 * configuredLimitPercent : 0;
        return (configuredLimitBytes == 0) ? limitByPercent
                : (limitByPercent == 0) ? configuredLimitBytes
                : Math.min(configuredLimitBytes, limitByPercent);
    }

    @Override
    public long getRamUsageLimitBytes() {
        return configuredLimitBytes;
    }

    @Override
    public long getRamUsageLimitPercent() {
        return configuredLimitPercent;
    }

    @Override
    public long getResolvedRamUsageLimitBytes() {
        return ramUsageLimit;
    }

    @Override
    public long getTotalSystemMemory() {
        return totalSystemMemory;
    }
}
