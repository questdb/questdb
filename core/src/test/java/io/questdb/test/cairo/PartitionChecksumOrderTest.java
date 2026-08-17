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
 * "Checksum trails data" for the per-partition sidecar.
 * <p>
 * A checksum is a claim ABOUT bytes, so it must become durable only after the bytes it describes. Get
 * this backwards and a crash between the two leaves a sidecar covering bytes that never landed --
 * which reads as corruption on a partition that is merely behind. That is a false positive that
 * fails a healthy table, and it is strictly worse than having no checksum at all.
 */
public class PartitionChecksumOrderTest extends AbstractCairoTest {

    private static final SyncAttributingFilesFacade FF = new SyncAttributingFilesFacade();

    @Test
    public void testSidecarGenerationIsDurableAfterTheBlocksItCovers() throws Exception {
        // SYNC commit mode: under the NOSYNC default nothing is synced at all, so the ordering would
        // be vacuously satisfied and the test would prove nothing.
        node1.setProperty(PropertyKey.CAIRO_COMMIT_MODE, "sync");
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

            final int chk = firstIndexContaining(order, PartitionChecksumSidecar.FILE_NAME);
            Assert.assertTrue(
                    "no _chk barrier was recorded; the sidecar was never made durable" + FF.debugDump(),
                    chk >= 0
            );

            final int lastColumn = lastIndexOfColumnFileBefore(order, order.size());
            Assert.assertTrue(
                    "no column-file barrier was recorded, so there is no ordering to check" + FF.debugDump(),
                    lastColumn >= 0
            );
            Assert.assertTrue(
                    "the sidecar became durable at index " + chk + " but a column file it covers was still"
                            + " being synced at index " + lastColumn + FF.debugDump(),
                    lastColumn < chk
            );

            // A drain applies several commits, each with its own _txn barrier, so "no _txn anywhere
            // before the sidecar" is not the invariant -- earlier commits legitimately have theirs.
            // What must hold is that the commit CARRYING the seal still publishes its pointer last.
            Assert.assertTrue(
                    "no _txn barrier follows the sidecar at index " + chk + "; the commit pointer for the"
                            + " commit that sealed must still be published after it" + FF.debugDump(),
                    firstIndexContainingFrom(order, TableUtils.TXN_FILE_NAME, chk + 1) >= 0
            );
        });
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
