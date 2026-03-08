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

public class TelemetryConfigurationWrapper implements TelemetryConfiguration {
    private final TelemetryConfiguration configuration;

    public TelemetryConfigurationWrapper(TelemetryConfiguration configuration) {
        this.configuration = configuration;
    }

    @Override
    public long getDbSizeEstimateTimeout() {
        return configuration.getDbSizeEstimateTimeout();
    }

    @Override
    public boolean getDisableCompletely() {
        return configuration.getDisableCompletely();
    }

    @Override
    public boolean getEnabled() {
        return configuration.getEnabled();
    }

    @Override
    public int getQueueCapacity() {
        return configuration.getQueueCapacity();
    }

    @Override
    public long getThrottleIntervalMicros() {
        return configuration.getThrottleIntervalMicros();
    }

    @Override
    public int getTtlWeeks() {
        return configuration.getTtlWeeks();
    }

    @Override
    public boolean hideTables() {
        return configuration.hideTables();
    }
}
