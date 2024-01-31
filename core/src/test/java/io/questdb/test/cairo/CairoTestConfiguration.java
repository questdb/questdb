/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2023 QuestDB
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

package io.questdb.test.cairo;

import io.questdb.FactoryProvider;
import io.questdb.TelemetryConfiguration;
import io.questdb.VolumeDefinitions;
import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.CairoConfigurationWrapper;
import io.questdb.cairo.sql.SqlExecutionCircuitBreakerConfiguration;
import io.questdb.std.*;
import io.questdb.std.datetime.microtime.MicrosecondClock;
import io.questdb.std.datetime.millitime.MillisecondClock;
import org.jetbrains.annotations.NotNull;

import java.util.Map;

public class CairoTestConfiguration extends CairoConfigurationWrapper {
    private final Overrides overrides;
    private final String root;
    private final String snapshotRoot;
    private final TelemetryConfiguration telemetryConfiguration;
    private final VolumeDefinitions volumeDefinitions = new VolumeDefinitions();

    public CairoTestConfiguration(CharSequence root, TelemetryConfiguration telemetryConfiguration, Overrides overrides) {
        this.root = Chars.toString(root);
        this.snapshotRoot = Chars.toString(root) + Files.SEPARATOR + "snapshot";
        this.telemetryConfiguration = telemetryConfiguration;
        this.overrides = overrides;
    }

    @Override
    protected CairoConfiguration getDelegate() {
        return overrides.getConfiguration(root);
    }

    @Override
    public @NotNull String getRoot() {
        return root;
    }

    @Override
    public @NotNull CharSequence getSnapshotRoot() {
        return snapshotRoot;
    }
    @Override
    public @NotNull SqlExecutionCircuitBreakerConfiguration getCircuitBreakerConfiguration() {
        return overrides.getCircuitBreakerConfiguration() != null ? overrides.getCircuitBreakerConfiguration() : super.getCircuitBreakerConfiguration();
    }

    @Override
    public Map<String, String> getEnv() {
        return overrides.getEnv();
    }

    @Override
    public @NotNull FactoryProvider getFactoryProvider() {
        return overrides.getFactoryProvider() == null ? super.getFactoryProvider() : overrides.getFactoryProvider();
    }

    @Override
    public @NotNull FilesFacade getFilesFacade() {
        // This method gets called in super constructor, hence the extra null check.
        return overrides != null && overrides.getFilesFacade() != null ? overrides.getFilesFacade() : super.getFilesFacade();
    }

    @Override
    public long getInactiveWalWriterTTL() {
        return -10000;
    }

    @Override
    public int getMetadataPoolCapacity() {
        return 1;
    }

    @Override
    public @NotNull MicrosecondClock getMicrosecondClock() {
        return overrides.getTestMicrosClock();
    }

    @Override
    public @NotNull MillisecondClock getMillisecondClock() {
        return () -> overrides.getTestMicrosClock().getTicks() / 1000L;
    }

    @Override
    public @NotNull NanosecondClock getNanosecondClock() {
        return () -> overrides.getTestMicrosClock().getTicks() * 1000L;
    }

    @Override
    public int getPartitionPurgeListCapacity() {
        // Bump it to high number so that test doesn't fail with memory leak if LongList re-allocates
        return 512;
    }

    @Override
    public @NotNull RostiAllocFacade getRostiAllocFacade() {
        return overrides.getRostiAllocFacade() != null ? overrides.getRostiAllocFacade() : super.getRostiAllocFacade();
    }

    @Override
    public CharSequence getSqlCopyInputRoot() {
        return overrides.getInputRoot();
    }

    @Override
    public CharSequence getSqlCopyInputWorkRoot() {
        return overrides.getInputWorkRoot();
    }

    @Override
    public @NotNull TelemetryConfiguration getTelemetryConfiguration() {
        return telemetryConfiguration;
    }

    @Override
    public @NotNull VolumeDefinitions getVolumeDefinitions() {
        return volumeDefinitions;
    }

    @Override
    public boolean isMultiKeyDedupEnabled() {
        return true;
    }

    @Override
    public boolean mangleTableDirNames() {
        return overrides.mangleTableDirNames();
    }
}
