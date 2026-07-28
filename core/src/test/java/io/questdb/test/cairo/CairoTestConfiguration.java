/*+*****************************************************************************
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

package io.questdb.test.cairo;

import io.questdb.FactoryProvider;
import io.questdb.Metrics;
import io.questdb.TelemetryConfiguration;
import io.questdb.VolumeDefinitions;
import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.CairoConfigurationWrapper;
import io.questdb.cairo.TableUtils;
import io.questdb.cairo.sql.SqlExecutionCircuitBreakerConfiguration;
import io.questdb.cutlass.qwp.codec.QwpServerInfoProvider;
import io.questdb.std.Chars;
import io.questdb.std.Files;
import io.questdb.std.FilesFacade;
import io.questdb.std.RostiAllocFacade;
import io.questdb.std.datetime.MicrosecondClock;
import io.questdb.std.datetime.NanosecondClock;
import io.questdb.std.datetime.millitime.MillisecondClock;
import io.questdb.std.str.Path;
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
    public boolean cairoResourcePoolTracingEnabled() {
        return true;
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
    public @NotNull QwpServerInfoProvider getQwpServerInfoProvider() {
        return overrides.getQwpServerInfoProvider() != null
                ? overrides.getQwpServerInfoProvider()
                : super.getQwpServerInfoProvider();
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

    /**
     * Keeps the on-disk answer when the test root sits on a RAM disk.
     * <p>
     * A developer can move the whole suite off their SSD by pointing {@code java.io.tmpdir} at tmpfs (see
     * the {@code ram-disk-test-root} profile in core/pom.xml). tmpfs is not on QuestDB's
     * supported-filesystem list, so {@link io.questdb.std.FilesFacadeImpl#allowMixedIO} answers false for
     * it, and that answer reaches every writer through {@code writerMixedIOEnabled}: {@code O3CopyJob},
     * {@code O3OpenColumnJob} and {@code ContiguousFileVarFrameColumn} would all silently take their
     * non-mixed-IO variants. The local run would then exercise the OPPOSITE write path to the ext4/XFS
     * agents in CI, and a local green would say nothing about the path that ships.
     * <p>
     * Answering true is not a fiction: tmpfs is page-cache backed, so mmap and write are coherent on it,
     * which is the property mixed I/O actually needs. tmpfs is excluded from the supported list because it
     * is volatile storage, not because mixed I/O misbehaves there.
     * <p>
     * Scoped to tmpfs alone; every other filesystem defers to the delegate, so a genuinely unsupported
     * filesystem is still reported honestly. CI never runs on tmpfs, and its ZFS shard — which legitimately
     * answers false — is untouched.
     */
    @Override
    public boolean isWriterMixedIOEnabled() {
        try (Path path = new Path()) {
            if (Files.getFileSystemStatus(path.of(dbRoot).$()) == Files.TMPFS_MAGIC) {
                return true;
            }
        }
        return super.isWriterMixedIOEnabled();
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
