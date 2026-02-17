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

public interface MemoryConfiguration {

    /**
     * Returns the value of the configuration property ram.usage.limit.bytes.
     */
    long getRamUsageLimitBytes();

    /**
     * Returns the value of the configuration property ram.usage.limit.percent.
     */
    long getRamUsageLimitPercent();

    /**
     * Returns the QuestDB-imposed limit on the total memory allocated with any `NATIVE_*`
     * tag. As a special case, `NATIVE_PATH` is exempted from this limit.
     */
    long getResolvedRamUsageLimitBytes();

    /**
     * Returns the total RAM as reported by `OperatingSystemMXBean`. This takes into account
     * the limit set on our cgroup (such as in a Docker container), if any.
     */
    long getTotalSystemMemory();
}
