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

package io.questdb.test.cairo.crash;

import io.questdb.PropertyKey;
import io.questdb.cairo.RecoveryCoordinator;
import io.questdb.cairo.TableToken;
import io.questdb.cairo.TableUtils;
import io.questdb.std.str.LPSZ;
import io.questdb.std.str.Utf8String;
import org.junit.Assert;
import org.junit.Test;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;

/**
 * DATA-BEFORE-POINTER for {@code ALTER TABLE ... REBASE WAL}.
 * <p>
 * The rebase builds the new table in a hidden {@code .rebase/} staging dir, makes that whole tree durable
 * (contents AND the staging dentries), then publishes it with an atomic {@code rename} into the db root and
 * immediately records the registry swap — drop old + add new — as one durable {@code tables.d} sync.
 * <p>
 * The rename alone does NOT make the published dentry durable: on POSIX a newly created directory entry
 * survives a power loss only once its PARENT directory is fsynced — the very rule
 * {@code WalUtils.syncStagingTreeDurable} already applies inside the staging tree. The registry sync, by
 * contrast, is unconditional in every commit mode ({@code GrowOnlyTableNameRegistryStore.logSwapTable}). So
 * without a db-root fsync between the two, a power loss in that window leaves a DURABLE pointer to a
 * directory that is gone: the name resolves to nothing, while the pre-rebase dir survives unregistered and
 * invisible. Under ADAPTIVE the same state aborts engine boot — {@code RecoveryCoordinator.recover()} reads
 * {@code _meta} for every registered WAL table and refuses to expose an adaptive table it cannot read
 * ({@code [-105] table does not exist}).
 * <p>
 * Today the window closes only by accident: the first fs-wide flush after the rebase (an adaptive epoch's
 * {@code syncfs}) publishes every pending dentry, which is why a COMPLETED rebase survives a crash and only
 * a crash INSIDE the window loses the table.
 */
public class RebaseWalPublishDurabilityCrashTest extends AbstractCrashConsistencyTest {

    /**
     * Bar 1 (ordering): the db root is fsynced after the publish rename and before any post-publish work,
     * so the dentry is durable no later than the registry entry that points at it.
     */
    @Test
    public void testPublishRenameFsyncsDbRootBeforeAnyPostPublishWork() throws Exception {
        configureRebase();
        runWithCrashFacade(() -> {
            seedSuspendedTable();
            execute("alter table t rebase wal");
            drainWalQueue();

            final String dbRoot = absPath(engine.getConfiguration().getDbRoot().toString());
            final String publishedDir = dbRoot + java.io.File.separator + engine.verifyTableName("t").getDirName();
            final List<String> order = crashFf.getSyncOrder();

            int lastStaging = -1;
            for (int i = 0, n = order.size(); i < n; i++) {
                if (order.get(i).contains(TableUtils.REBASE_TMP_DIR)) {
                    lastStaging = i;
                }
            }
            Assert.assertTrue("no staging-tree sync observed — the rebase did not run as expected", lastStaging >= 0);

            int rootFsync = -1;
            int firstPostPublish = -1;
            for (int i = lastStaging + 1, n = order.size(); i < n; i++) {
                final String p = order.get(i);
                if (rootFsync < 0 && p.equals(dbRoot)) {
                    rootFsync = i;
                }
                if (firstPostPublish < 0 && p.startsWith(publishedDir)) {
                    firstPostPublish = i;
                }
            }
            Assert.assertTrue(
                    "db root was never fsynced after the publish rename: the published dentry is not durable, "
                            + "yet the registry swap that points at it is",
                    rootFsync >= 0
            );
            Assert.assertTrue(
                    "db root fsync came too late — post-publish work at index " + firstPostPublish
                            + " ran before the dentry was durable (root fsync at " + rootFsync + ')',
                    firstPostPublish < 0 || rootFsync < firstPostPublish
            );
        });
    }

    /**
     * Bar 2 (crash state): a power loss on the first durability op after the publish rename must leave the
     * published table dir — with a readable {@code _meta} — on disk. That op is the registry swap's own
     * {@code tables.d} sync, i.e. the first instant a durable pointer can name the new dir.
     */
    @Test
    public void testCrashOnTheFirstOpAfterThePublishRenameKeepsTheTableDir() throws Exception {
        configureRebase();
        final int[] renameOp = new int[]{-1};
        crashFf = new CrashFaultFilesFacade() {
            @Override
            public int rename(LPSZ from, LPSZ to) {
                final boolean publish = Utf8String.newInstance(from).toString()
                        .contains(TableUtils.REBASE_TMP_DIR);
                final int rc = super.rename(from, to);
                if (rc == io.questdb.std.Files.FILES_RENAME_OK && publish && renameOp[0] < 0) {
                    renameOp[0] = durabilityOpCount();
                    // Power loss the instant the registry swap is durable — the first moment a durable pointer
                    // can name the new dir. With the publish barrier in place the two ops after the rename are
                    // [+1] the db-root fsync and [+2] the swap's tables.d sync; without it [+1] is the swap
                    // itself and [+2] the new sequencer's id-generator msync, which is still past the swap. So
                    // +2 is post-swap either way. The swap-happened assertion below fails loudly (rather than
                    // passing vacuously) should that op accounting ever drift.
                    armCrashAt(renameOp[0] + 2);
                }
                return rc;
            }
        };
        crashFf.setDbRoot(root);
        assertMemoryLeak(crashFf, () -> {
            seedSuspendedTable();
            final TableToken oldToken = engine.verifyTableName("t");

            try {
                execute("alter table t rebase wal");
                Assert.fail("armed crash never fired — the rebase completed");
            } catch (CrashSimulationError expected) {
                // the simulated power loss
            }
            Assert.assertTrue("publish rename never happened", renameOp[0] >= 0);
            Assert.assertFalse("armed crash never fired", crashFf.isCrashArmed());
            // The rebase mints the new token in-flight; the live registry is the only place it is named.
            final TableToken newToken = engine.verifyTableName("t");
            Assert.assertNotEquals("registry swap did not happen — nothing was published", oldToken.getDirName(), newToken.getDirName());

            crashAndReopen();
            releaseCrashOrphans();

            final Path published = Paths.get(engine.getConfiguration().getDbRoot().toString(), newToken.getDirName());
            Assert.assertTrue(
                    "published table dir lost to the crash [dir=" + newToken.getDirName()
                            + "] — the registry can name a table whose directory is gone",
                    Files.exists(published)
            );
            final Path meta = published.resolve(TableUtils.META_FILE_NAME);
            Assert.assertTrue("published _meta lost to the crash", Files.exists(meta));
            Assert.assertTrue("published _meta is empty after the crash", Files.size(meta) > 0);
        });
    }

    /**
     * Bar 3 (self-consistency): the dir the rename publishes carries its OWN generation-zero epoch anchor.
     * <p>
     * A crash BEFORE the registry swap leaves the published dir on disk, unregistered, holding the same table
     * name — the accepted dir-only leak. Startup's root-directory scan adopts such a dir once the name is
     * free ({@code TableNameRegistryStore.reloadFromRootDirectory}), so the clone must be recoverable on its
     * own terms. It is not if it inherits the SOURCE table's {@code .epoch} payloads: the clone resets
     * {@code _txn}/{@code _meta}, so every inherited candidate fails validation and recovery aborts startup
     * with "no trustworthy adaptive epoch generation" rather than exposing unverified live state.
     */
    @Test
    public void testAdoptedPublishOrphanCarriesItsOwnValidEpochAnchor() throws Exception {
        configureRebase();
        final int[] renameOp = new int[]{-1};
        crashFf = new CrashFaultFilesFacade() {
            @Override
            public int rename(LPSZ from, LPSZ to) {
                final boolean publish = Utf8String.newInstance(from).toString()
                        .contains(TableUtils.REBASE_TMP_DIR);
                final int rc = super.rename(from, to);
                if (rc == io.questdb.std.Files.FILES_RENAME_OK && publish && renameOp[0] < 0) {
                    renameOp[0] = durabilityOpCount();
                    // Crash on the publish barrier itself: past the rename, before the registry swap. The
                    // dir is durable (bar 1) but never became the live table.
                    armCrashAt(renameOp[0] + 1);
                }
                return rc;
            }
        };
        crashFf.setDbRoot(root);
        assertMemoryLeak(crashFf, () -> {
            seedSuspendedTable();
            final TableToken oldToken = engine.verifyTableName("t");
            try {
                execute("alter table t rebase wal");
                Assert.fail("armed crash never fired — the rebase completed");
            } catch (CrashSimulationError expected) {
                // the simulated power loss
            }
            Assert.assertFalse("armed crash never fired", crashFf.isCrashArmed());

            crashAndReopen();
            releaseCrashOrphans();

            // Free the name, exactly as an operator would after a failed rebase, so the orphan is adoptable.
            execute("drop table if exists t");
            drainWalQueue();
            drainPurgeJob();
            engine.getTableNameRegistry().reload(); // the boot-time registry rebuild that performs the adoption

            final TableToken adopted = engine.getTableTokenIfExists("t");
            Assert.assertNotNull("the published orphan was not adopted — nothing to recover, test is vacuous", adopted);
            Assert.assertNotEquals(
                    "adopted the PRE-rebase dir — the published orphan is not under test",
                    oldToken.getDirName(),
                    adopted.getDirName()
            );
            // The bar: recovery accepts the adopted clone instead of aborting startup.
            new RecoveryCoordinator(engine).recover();
        });
    }

    private static String absPath(String path) {
        return Paths.get(path).toAbsolutePath().toString();
    }

    private void configureRebase() {
        setProperty(PropertyKey.CAIRO_COMMIT_MODE, "adaptive");
        setProperty(PropertyKey.CAIRO_ADAPTIVE_COMMIT_GROUP_WINDOW, 0);
        // Keep the epoch timer from firing an incidental syncfs, which would publish every pending dentry
        // for free and hide the window under test.
        setProperty(PropertyKey.CAIRO_ADAPTIVE_EPOCH_INTERVAL, 3_600_000);
        setProperty(PropertyKey.DEV_MODE_ENABLED, "true");                       // SUSPEND WAL is dev-mode gated
        setProperty(PropertyKey.CAIRO_WAL_APPLY_SUSPENDED_WRITE_DENIED, "true"); // REBASE WAL demands this
    }

    /**
     * A crash unwound inside {@code TableWriter}/{@code WalWriter} construction on a LIVE JVM leaves pool
     * entries owned and fds open; a real power loss's process death reclaims both.
     */
    private void releaseCrashOrphans() {
        engine.releaseCrashOrphanedWriters();
        engine.releaseCrashOrphanedWalWriters();
        engine.releaseAllWalWriters();
        engine.releaseInactiveTableSequencers();
    }

    private void seedSuspendedTable() throws Exception {
        execute("create table t (ts timestamp, v long) timestamp(ts) partition by day wal");
        execute("insert into t values ('2024-01-01T00:00:00.000000Z', 1), ('2024-01-02T00:00:00.000000Z', 2)");
        drainWalQueue();
        markDurableBaseline();
        // REBASE WAL is a recovery op, permitted only on a suspended table. Suspend is data-preserving.
        execute("alter table t suspend wal");
    }
}
