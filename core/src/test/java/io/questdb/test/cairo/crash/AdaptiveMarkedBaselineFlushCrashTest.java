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

package io.questdb.test.cairo.crash;

import io.questdb.PropertyKey;
import io.questdb.cairo.DurableEpochManifest;
import io.questdb.cairo.RecoveryCoordinator;
import io.questdb.cairo.TableToken;
import io.questdb.cairo.TableUtils;
import io.questdb.cairo.wal.LocalDurabilityPolicy;
import io.questdb.std.str.Path;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.stream.Stream;

/**
 * A table carries the {@code _epoch.enrol} marker and no anchor after a replica tenure. The tenure applies rows
 * without flushing the column files, and the node then restarts as an adaptive primary. The page cache survives
 * the process restart, so nothing is lost before the boot. The boot baseline must flush those column files
 * before it anchors them: a power loss before the first epoch otherwise recovers a clean, durable-looking cut
 * whose tenure rows read as zeros.
 */
public class AdaptiveMarkedBaselineFlushCrashTest extends AbstractAdaptiveCrashTest {
    private static final int DURABLE_ROWS = 3;
    private static final int TENURE_ROWS = 4;

    @Test
    public void testAdaptiveReplicaRestartPowerLossBeforeApply() throws Exception {
        runWithFacade(new CrashFaultFilesFacade(), () -> assertRecoversTenureRows(true, false, true));
    }

    @Test
    public void testNosyncReplicaRestartPowerLossAfterLazyApply() throws Exception {
        runWithFacade(new CrashFaultFilesFacade(), () -> assertRecoversTenureRows(false, true, true));
    }

    @Test
    public void testNosyncReplicaRestartPowerLossBeforeApply() throws Exception {
        runWithFacade(new CrashFaultFilesFacade(), () -> assertRecoversTenureRows(false, false, true));
    }

    @Test
    public void testSingleFileSyncfsPowerLossAfterLazyApply() throws Exception {
        runWithFacade(new SingleFileSyncfsFacade(), () -> assertRecoversTenureRows(false, true, false));
    }

    @Test
    public void testSingleFileSyncfsPowerLossBeforeApply() throws Exception {
        runWithFacade(new SingleFileSyncfsFacade(), () -> assertRecoversTenureRows(false, false, false));
    }

    private static boolean fileExists(TableToken token, CharSequence fileName) {
        try (Path p = new Path()) {
            p.of(configuration.getDbRoot()).concat(token).concat(fileName);
            return configuration.getFilesFacade().exists(p.$());
        }
    }

    private static String insertSql(String table, int v) {
        return "INSERT INTO " + table + " VALUES ('" + String.format("2024-10-01T%02d:00:00.000000Z", v) + "', " + v + ")";
    }

    private static String expectedRows(int count) {
        final StringBuilder sb = new StringBuilder("ts\tv\n");
        for (int i = 0; i < count; i++) {
            sb.append(String.format("2024-10-01T%02d:00:00.000000Z", i)).append('\t').append(i).append('\n');
        }
        return sb.toString();
    }

    private void assertRecoversTenureRows(boolean adaptiveTenure, boolean applyAfterBoot, boolean fsWideSyncfs) throws Exception {
        setProperty(PropertyKey.CAIRO_COMMIT_MODE, adaptiveTenure ? "adaptive" : "nosync");
        setProperty(PropertyKey.CAIRO_ADAPTIVE_COMMIT_GROUP_WINDOW, "0");
        setProperty(PropertyKey.CAIRO_ADAPTIVE_EPOCH_INTERVAL, 0);
        setProperty(PropertyKey.CAIRO_ADAPTIVE_EPOCH_FLUSH_ON_CLOSE, "true");
        final String[] tables = {"t0", "t1"};
        final TableToken[] tokens = new TableToken[tables.length];
        try {
            for (int t = 0; t < tables.length; t++) {
                execute("CREATE TABLE " + tables[t] + " (ts TIMESTAMP, v LONG) TIMESTAMP(ts) PARTITION BY DAY WAL");
                tokens[t] = engine.verifyTableName(tables[t]);
                for (int i = 0; i < DURABLE_ROWS; i++) {
                    execute(insertSql(tables[t], i));
                }
            }
            drainWalQueue();
            markDurableBaseline();

            // Replica tenure: ReplicaRoleState marks every WAL table and clears its anchor, then applies rows
            // without a durable column flush.
            engine.setLocalDurabilityPolicy(LocalDurabilityPolicy.REPLICA_SKIP);
            for (TableToken token : tokens) {
                try (Path p = new Path()) {
                    final int rootLen = p.of(configuration.getDbRoot()).concat(token).size();
                    RecoveryCoordinator.markRestoredForEnrolment(configuration.getFilesFacade(), p, rootLen);
                    RecoveryCoordinator.removeAdaptiveEpochArtifacts(configuration.getFilesFacade(), p, rootLen);
                    DurableEpochManifest.fsyncDirectory(configuration, p, rootLen);
                }
            }
            for (String table : tables) {
                for (int i = DURABLE_ROWS; i < DURABLE_ROWS + TENURE_ROWS; i++) {
                    execute(insertSql(table, i));
                }
            }
            drainWalQueue();
            for (TableToken token : tokens) {
                markWalAndSequencerDurable(token);
            }

            // Restart as an adaptive primary. Writers close under REPLICA_SKIP, so no close-time epoch runs.
            releaseEngineHandles();
            setProperty(PropertyKey.CAIRO_COMMIT_MODE, "adaptive");
            engine.setLocalDurabilityPolicy(LocalDurabilityPolicy.ALWAYS_ON);
            final int syncfsBeforeBoot = crashFf.syncfsCount();
            reboot(tokens);
            for (TableToken token : tokens) {
                Assert.assertTrue(fileExists(token, TableUtils.SNAPSHOT_FILE_NAME));
                Assert.assertFalse(fileExists(token, RecoveryCoordinator.RESTORE_ENROL_FILE_NAME));
            }
            if (fsWideSyncfs) {
                // One filesystem-wide flush covers every marked table.
                Assert.assertEquals(1, crashFf.syncfsCount() - syncfsBeforeBoot);
            }

            int expectedRowCount = DURABLE_ROWS + TENURE_ROWS;
            if (applyAfterBoot) {
                // Suppress the post-batch cadence epoch, so the power loss lands before it.
                setProperty(PropertyKey.CAIRO_ADAPTIVE_EPOCH_INTERVAL, 3_600_000L);
                for (int t = 0; t < tables.length; t++) {
                    engine.getTableSequencerAPI().getTxnTracker(tokens[t])
                            .setLastEpochTs(configuration.getMicrosecondClock().getTicks() / 1000L);
                    execute(insertSql(tables[t], expectedRowCount));
                }
                drainWalQueue();
                expectedRowCount++;
                for (TableToken token : tokens) {
                    Assert.assertTrue(fileExists(token, TableUtils.SNAPSHOT_FILE_NAME));
                }
            }

            // Power loss, then recovery and WAL replay. Nothing runs at a power loss, so no close-time epoch.
            setProperty(PropertyKey.CAIRO_ADAPTIVE_EPOCH_FLUSH_ON_CLOSE, "false");
            setProperty(PropertyKey.CAIRO_ADAPTIVE_EPOCH_INTERVAL, 1000);
            recoverAfterCrash(tokens);

            Assert.assertFalse(anyTableSuspended(tokens));
            for (String table : tables) {
                assertQuery("SELECT ts, v FROM " + table)
                        .timestamp("ts")
                        .expectSize()
                        .returns(expectedRows(expectedRowCount));
            }
        } finally {
            engine.setLocalDurabilityPolicy(LocalDurabilityPolicy.ALWAYS_ON);
            releaseEngineHandles();
        }
    }

    private void markWalAndSequencerDurable(TableToken token) throws Exception {
        // Isolates the column-data question: the tenure's WAL and sequencer files are durable.
        final java.nio.file.Path tableDir = Paths.get(configuration.getDbRoot().toString(), token.getDirName());
        try (Stream<java.nio.file.Path> files = Files.walk(tableDir)) {
            files.filter(Files::isRegularFile).forEach(p -> {
                final String rel = tableDir.relativize(p).toString();
                if (rel.startsWith("wal") || rel.startsWith("txn_seq")) {
                    crashFf.markFileDurable(p.toAbsolutePath().toString());
                }
            });
        }
    }

    private void reboot(TableToken[] tokens) {
        for (TableToken token : tokens) {
            engine.getTxnScoreboardPool().remove(token);
            engine.getTableSequencerAPI().resetForReboot(token);
        }
        new RecoveryCoordinator(engine).recover();
    }

    private void runWithFacade(CrashFaultFilesFacade facade, TestUtils.LeakProneCode body) throws Exception {
        assumeCrashHarnessSupported();
        crashFf = facade;
        crashFf.setDbRoot(root);
        try {
            assertMemoryLeak(crashFf, body);
        } finally {
            setProperty(PropertyKey.CAIRO_COMMIT_MODE, "nosync");
            setProperty(PropertyKey.CAIRO_ADAPTIVE_EPOCH_INTERVAL, 1000);
            setProperty(PropertyKey.CAIRO_ADAPTIVE_EPOCH_FLUSH_ON_CLOSE, "true");
        }
    }

    /**
     * {@code syncfs(fd)} as macOS and Windows have it: a flush of that one file, not of the filesystem.
     */
    private static class SingleFileSyncfsFacade extends CrashFaultFilesFacade {
        @Override
        public boolean isSyncfsFileSystemWide() {
            return false;
        }

        @Override
        public void syncfs(long fd) {
            fsync(fd);
        }
    }
}
