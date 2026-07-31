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
import io.questdb.cairo.wal.WalUtils;
import io.questdb.std.str.LPSZ;
import io.questdb.std.str.Utf8String;
import org.junit.Assert;
import org.junit.Test;

import java.nio.file.Paths;
import java.util.List;

/**
 * RENAME ≠ PUBLISH, applied to the table-name registry's own file.
 * <p>
 * Registry compaction ({@code TableNameRegistryStore.compactTableNameFile}, run from a registry load once
 * enough dropped entries accumulate) writes {@code tables.d.tmp}, msyncs its CONTENT, renames it to
 * {@code tables.d.<N+1>}, and then unlinks {@code tables.d.<N>}. Both namespace changes live in the db root
 * and neither is durable until the db root itself is fsynced, so POSIX permits a power loss that keeps the
 * unlink and loses the new dentry — leaving the instance with NO registry file. The registry is the pointer
 * store every name resolves through; without it, startup falls back to a directory scan that cannot know
 * which tables were dropped but not yet purged, so dropped tables come back.
 * <p>
 * The barrier is one db-root fsync between the rename and the unlink: after it, at least one version is
 * always durably present. This is an ORDERING bar rather than a crash-state bar on purpose — the crash model
 * rolls back an unsynced rename AND an unsynced unlink together, so a state assertion would pass with or
 * without the barrier and prove nothing.
 */
public class TableRegistryCompactionCrashTest extends AbstractCrashConsistencyTest {

    @Test
    public void testCompactionFsyncsDbRootBeforeUnlinkingTheOldVersion() throws Exception {
        // Compact as soon as there is anything to compact, so a handful of drops triggers it.
        setProperty(PropertyKey.CAIRO_TABLE_REGISTRY_COMPACTION_THRESHOLD, 0);
        final int[] renameSyncMark = new int[]{-1}; // getSyncOrder() size when the new version is published
        final int[] unlinkSyncMark = new int[]{-1}; // ... and when the old version is unlinked
        crashFf = new CrashFaultFilesFacade() {
            @Override
            public int rename(LPSZ from, LPSZ to) {
                final boolean publish = Utf8String.newInstance(from).toString().endsWith(".tmp")
                        && Utf8String.newInstance(to).toString().contains(WalUtils.TABLE_REGISTRY_NAME_FILE);
                final int rc = super.rename(from, to);
                if (rc == io.questdb.std.Files.FILES_RENAME_OK && publish && renameSyncMark[0] < 0) {
                    renameSyncMark[0] = getSyncOrder().size();
                }
                return rc;
            }

            @Override
            public boolean removeQuiet(LPSZ name) {
                final boolean isRegistryVersion = Utf8String.newInstance(name).toString()
                        .contains(WalUtils.TABLE_REGISTRY_NAME_FILE + '.');
                if (isRegistryVersion && renameSyncMark[0] >= 0 && unlinkSyncMark[0] < 0) {
                    unlinkSyncMark[0] = getSyncOrder().size();
                }
                return super.removeQuiet(name);
            }
        };
        crashFf.setDbRoot(root);
        assertMemoryLeak(crashFf, () -> {
            // Enough create/drop churn that the next registry load compacts.
            for (int i = 0; i < 4; i++) {
                execute("create table keep" + i + " (ts timestamp, v long) timestamp(ts) partition by day wal");
                execute("create table gone" + i + " (ts timestamp, v long) timestamp(ts) partition by day wal");
                drainWalQueue();
                execute("drop table gone" + i);
                drainWalQueue();
                drainPurgeJob();
            }

            engine.getTableNameRegistry().reload(); // triggers compactTableNameFile

            Assert.assertTrue("compaction never published a new registry version — test is vacuous", renameSyncMark[0] >= 0);
            Assert.assertTrue("compaction never unlinked the old registry version — test is vacuous", unlinkSyncMark[0] >= 0);

            final String dbRoot = Paths.get(engine.getConfiguration().getDbRoot().toString()).toAbsolutePath().toString();
            final List<String> order = crashFf.getSyncOrder();
            boolean rootFsynced = false;
            for (int i = renameSyncMark[0], n = Math.min(unlinkSyncMark[0], order.size()); i < n; i++) {
                if (order.get(i).equals(dbRoot)) {
                    rootFsynced = true;
                    break;
                }
            }
            Assert.assertTrue(
                    "db root was not fsynced between publishing tables.d." + "<N+1> and unlinking tables.d.<N>: "
                            + "a power loss there can persist the unlink while losing the new dentry, leaving no "
                            + "registry file at all",
                    rootFsynced
            );
        });
    }
}
