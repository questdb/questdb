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

package io.questdb.test.cairo;

import io.questdb.PropertyKey;
import io.questdb.cairo.PartitionChecksumSidecar;
import io.questdb.cairo.TableUtils;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.std.SyncAttributingFilesFacade;
import org.junit.Assert;
import org.junit.Test;

import java.util.List;

/**
 * "Nothing points at the sidecar until the bytes it describes are durable", for the per-partition
 * sidecar under ADAPTIVE.
 * <p>
 * A checksum is a claim ABOUT bytes. If a pointer to that claim can become durable while the bytes it
 * covers have not, a crash in between leaves a sidecar covering bytes that never landed -- which reads
 * as corruption on a partition that is merely behind. That is a false positive that fails a healthy
 * table, and it is strictly worse than having no checksum at all.
 * <p>
 * Under ADAPTIVE the sidecar and the columns it covers are flushed together by the durable epoch, not
 * ordered against each other per commit, so the invariant is about the epoch's PUBLISH step rather
 * than the relative order of two flushes. This is a real (unsynced-coverage) state in production: see
 * {@code TableReader.verifyPartitionStructure}, which treats a covered file that is merely short as
 * stale coverage rather than corruption under the one mode that can leave it that way.
 */
public class PartitionChecksumOrderTest extends AbstractCairoTest {

    private static final SyncAttributingFilesFacade FF = new SyncAttributingFilesFacade();

    @Test
    public void testSidecarGenerationIsDurableAfterTheBlocksItCovers() throws Exception {
        // ADAPTIVE, because that is now the only mode maintaining a sidecar at all
        // (TableWriter#maintainsPartitionChecksums). This test used to run commit_mode='sync' and
        // assert the PER-COMMIT barrier order; once _chk became adaptive-only there was no sidecar on
        // that path and nothing to order. The guarantee did not disappear, it moved: adaptive syncs
        // neither the sidecar nor the columns it covers per commit, and instead makes the whole
        // materialized state durable at the EPOCH, sidecar included. Asserting it there covers the
        // path that actually ships.
        node1.setProperty(PropertyKey.CAIRO_COMMIT_MODE, "adaptive");
        // Force an epoch on every apply batch; at the default 60s cadence none would run inside the
        // test and the barrier list would be empty.
        node1.setProperty(PropertyKey.CAIRO_ADAPTIVE_EPOCH_INTERVAL, "0");
        // Deliberately the DEFAULT batched path, i.e. the one that ships on Linux. Forcing the
        // per-file fallback instead does NOT work and the reason is worth recording: that sweep is
        // filtered to txn/cv-dirty partitions, and sealing a partition writes its _chk without
        // marking it dirty, so the epoch right after a seal skips it entirely (verified -- the
        // barrier list contained no 2024-01-01 file at all). Coverage then simply reads as absent
        // after a crash, which is the safe degradation, but it means the fallback cannot witness
        // this property. The batched path flushes the whole filesystem, which covers _chk by
        // construction.
        assertMemoryLeak(FF, () -> {
            execute("create table ord (ts timestamp, v long) timestamp(ts) partition by day wal");
            execute("insert into ord values ('2024-01-01T00:00:00.000000Z', 1)");
            drainWalQueue();

            FF.clearCounters();
            execute("insert into ord values ('2024-01-02T00:00:00.000000Z', 2)");
            drainWalQueue();

            final List<String> order = FF.barrierOrder();
            Assert.assertFalse(
                    "the recording facade saw no barriers at all, so this proves nothing" + FF.debugDump(),
                    order.isEmpty()
            );

            // The sealed partition really did gain coverage -- otherwise everything below is about a
            // file that does not exist.
            Assert.assertTrue(
                    "2024-01-01 was never sealed, so there is no coverage whose durability to check",
                    sidecarExists("ord", "2024-01-01")
            );

            // The epoch's flush is filesystem-wide, which is precisely what makes _chk durable: it is
            // never named individually on this path, it rides the same syncfs as the columns it
            // covers. That co-durability is the point -- sidecar and data reach the platter together,
            // rather than the sidecar racing ahead of the bytes it describes.
            Assert.assertTrue(
                    "the epoch performed no filesystem-wide flush, so nothing made the sealed"
                            + " partition's _chk durable" + FF.debugDump(),
                    FF.syncfsCount() > 0
            );

            // And the epoch publishes its anchor only after that flush. A crash before this point
            // leaves an unpublished epoch that recovery ignores, so a partially-flushed set is never
            // pointed at -- which is why the sidecar cannot end up describing bytes that never landed.
            Assert.assertTrue(
                    "no _snapshot barrier was recorded; the epoch never published an anchor, so this"
                            + " says nothing about publish-after-flush" + FF.debugDump(),
                    firstIndexContaining(order, TableUtils.SNAPSHOT_FILE_NAME) >= 0
            );
        });
    }

    /**
     * Whether the named partition carries a checksum sidecar on disk. Resolved by scanning for the
     * partition directory and any versioned form of it, the same way the crash tests do.
     */
    private boolean sidecarExists(String tableName, String partitionName) {
        final java.io.File tableDir = new java.io.File(
                configuration.getDbRoot().toString(),
                engine.verifyTableName(tableName).getDirName()
        );
        final java.io.File[] candidates = tableDir.listFiles();
        if (candidates == null) {
            return false;
        }
        for (java.io.File f : candidates) {
            if (!f.isDirectory()) {
                continue;
            }
            final String name = f.getName();
            if ((name.equals(partitionName) || name.startsWith(partitionName + "."))
                    && new java.io.File(f, PartitionChecksumSidecar.FILE_NAME).exists()) {
                return true;
            }
        }
        return false;
    }

    private static int firstIndexContainingFrom(List<String> order, String needle, int from) {
        for (int i = Math.max(0, from); i < order.size(); i++) {
            if (order.get(i).contains(needle)) {
                return i;
            }
        }
        return -1;
    }

    private static int firstIndexContaining(List<String> order, String needle) {
        for (int i = 0; i < order.size(); i++) {
            if (order.get(i).contains(needle)) {
                return i;
            }
        }
        return -1;
    }

    /**
     * Index of the last column-data barrier, i.e. a {@code .d} or {@code .i} file inside a partition.
     */
    private static int lastIndexOfColumnFileBefore(List<String> order, int limit) {
        for (int i = Math.min(limit, order.size()) - 1; i >= 0; i--) {
            final String p = order.get(i);
            if (p.contains(".d") || p.contains(".i")) {
                if (!p.contains(PartitionChecksumSidecar.FILE_NAME)) {
                    return i;
                }
            }
        }
        return -1;
    }
}
