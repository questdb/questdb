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
import io.questdb.PropertyKey;
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
    // Set only when a developer asked for a RAM-disk test root (ram-disk-test-root profile).
    private static final boolean RAM_DISK_TEST_ROOT = System.getenv("QDB_TEST_TMPDIR") != null;
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
     * Keeps the on-disk answer for a test root deliberately placed on a RAM disk.
     * <p>
     * A developer can move the whole suite off their SSD with the {@code ram-disk-test-root} profile (see
     * core/pom.xml). tmpfs is not on QuestDB's supported-filesystem list, so
     * {@link io.questdb.std.FilesFacadeImpl#allowMixedIO} answers false for it, and that answer reaches
     * every writer through {@code writerMixedIOEnabled}: {@code O3CopyJob}, {@code O3OpenColumnJob} and
     * {@code ContiguousFileVarFrameColumn} would all silently take their non-mixed-IO variants, so the run
     * would exercise the opposite write path to an on-disk one. Answering true is not a fiction: tmpfs is
     * page-cache backed, so mmap and write are coherent on it, which is the property mixed I/O needs; it is
     * excluded from the supported list for being volatile.
     * <p>
     * Two guards, both learned the hard way — an unguarded version of this turned every WalWriterFuzzTest
     * red on all three CI platforms:
     * <ul>
     *   <li><b>Opt-in only.</b> Applies solely when {@code QDB_TEST_TMPDIR} is exported, i.e. when someone
     *   asked for a RAM-disk root. Never assume where a test root lives: an agent whose {@code /tmp} is
     *   itself tmpfs would otherwise have every test silently flipped onto the other write path.</li>
     *   <li><b>Never outvote an explicit choice.</b> {@code WalWriterFuzzTest} pins
     *   {@code DEBUG_CAIRO_ALLOW_MIXED_IO} to the real probe and asserts the configuration agrees; a test
     *   that states its own answer owns it.</li>
     * </ul>
     */
    @Override
    public boolean isWriterMixedIOEnabled() {
        if (RAM_DISK_TEST_ROOT && !overrides.isPropertySet(PropertyKey.DEBUG_CAIRO_ALLOW_MIXED_IO)) {
            try (Path path = new Path()) {
                if (Files.getFileSystemStatus(path.of(dbRoot).$()) == Files.TMPFS_MAGIC) {
                    return true;
                }
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
