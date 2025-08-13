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

package io.questdb.test.cairo;

import io.questdb.FactoryProvider;
import io.questdb.Metrics;
import io.questdb.TelemetryConfiguration;
import io.questdb.VolumeDefinitions;
import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.CairoConfigurationWrapper;
import io.questdb.cairo.TableUtils;
import io.questdb.cairo.sql.SqlExecutionCircuitBreakerConfiguration;
import io.questdb.std.Chars;
import io.questdb.std.Files;
import io.questdb.std.FilesFacade;
import io.questdb.std.RostiAllocFacade;
import io.questdb.std.datetime.MicrosecondClock;
import io.questdb.std.datetime.NanosecondClock;
import io.questdb.std.datetime.millitime.MillisecondClock;
import io.questdb.test.AbstractCairoTest;
import org.jetbrains.annotations.NotNull;

import java.util.Map;

public class CairoTestConfiguration extends CairoConfigurationWrapper {
    private final String dbRoot;
    private final String installRoot;
    private final Overrides overrides;
    private final String snapshotRoot;
    private final TelemetryConfiguration telemetryConfiguration;
    private final VolumeDefinitions volumeDefinitions = new VolumeDefinitions();

    public CairoTestConfiguration(CharSequence dbRoot, TelemetryConfiguration telemetryConfiguration, Overrides overrides) {
        super(Metrics.ENABLED);
        this.dbRoot = Chars.toString(dbRoot);
        this.installRoot = java.nio.file.Paths.get(this.dbRoot).getParent().toAbsolutePath().toString();
        this.snapshotRoot = Chars.toString(dbRoot) + Files.SEPARATOR + TableUtils.CHECKPOINT_DIRECTORY;
        this.telemetryConfiguration = telemetryConfiguration;
        this.overrides = overrides;
    }

    @Override
    public boolean freeLeakedReaders() {
        return overrides.freeLeakedReaders();
    }

    @Override
    public @NotNull CharSequence getCheckpointRoot() {
        return snapshotRoot;
    }

    @Override
    public @NotNull SqlExecutionCircuitBreakerConfiguration getCircuitBreakerConfiguration() {
        return AbstractCairoTest.staticOverrides.getCircuitBreakerConfiguration() != null
                ? AbstractCairoTest.staticOverrides.getCircuitBreakerConfiguration()
                : super.getCircuitBreakerConfiguration();
    }

    @Override
    public @NotNull String getDbRoot() {
        return dbRoot;
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
    public @NotNull String getInstallRoot() {
        return installRoot;
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
        MicrosecondClock microsecondClock = overrides.getTestMicrosClock();
        return () -> microsecondClock.getTicks() / 1000L;
    }

    @Override
    public NanosecondClock getNanosecondClock() {
        MicrosecondClock microsecondClock = overrides.getTestMicrosClock();
        return () -> microsecondClock.getTicks() * 1000L;
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
    public long getSpinLockTimeout() {
        return overrides != null ? overrides.getSpinLockTimeout() : super.getSpinLockTimeout();
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

    @Override
    protected CairoConfiguration getDelegate() {
        return overrides.getConfiguration(dbRoot);
    }
}
