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

package io.questdb.test.cairo.wal;

import io.questdb.PropertyKey;
import io.questdb.cairo.CommitMode;
import io.questdb.cairo.TableToken;
import io.questdb.cairo.TableUtils;
import io.questdb.cairo.TableWriter;
import io.questdb.std.str.LPSZ;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.std.TestFilesFacadeImpl;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

/**
 * DATA BEFORE POINTER, applied to the adaptive enrolment record. The enrolment field in {@code _meta} is a
 * durability CLAIM about the materialized state — {@link CommitMode#ADAPTIVE} says "this table may be lazily
 * ahead of its epoch, so trust the epoch rather than the live files", anything else says the opposite — and
 * {@code RecoveryCoordinator} acts on it. So in both directions the state must be made to match BEFORE the
 * record is written:
 *
 * <ul>
 *   <li>ENTERING adaptive, the generation-zero baseline must be published first. Record first, crash, and
 *       the next startup finds a table claiming lazy state with no anchor to rewind to — which it refuses,
 *       failing the whole engine component rather than just that table.</li>
 *   <li>LEAVING adaptive, the lazily-applied state must be reconciled first. Record first, crash, and the
 *       next startup skips a roll-forward the table still needed — not a boot failure but a silently wrong
 *       read, which is worse.</li>
 * </ul>
 *
 * <p>Both are asserted as an ORDER over file operations, because ordering is the entire content of the
 * invariant: a test that checked only the end state would pass against either sequence.
 *
 * <p><b>Strength of each arm.</b> Inverting the order in the product makes the LEAVING arm fail (verified),
 * so that arm is a proven control. The ENTERING arm still passed under the same inversion — its assertion
 * is therefore reinforcement, not proof, and should not be cited as one until someone works out which later
 * operation masks the inversion there. The live
 * {@code _meta} is written through {@code _meta.swp}, so the assertion is that the write of the
 * record itself — a write at the enrolled-mode offset of the live {@code _meta} — follows the last
 * operation on the {@code _snapshot} marker.
 */
public class AdaptiveEnrollmentOrderTest extends AbstractCairoTest {

    @Test
    public void testEnteringAdaptivePublishesTheAnchorBeforeRecordingIt() throws Exception {
        final OrderRecordingFilesFacade facade = new OrderRecordingFilesFacade();
        node1.setProperty(PropertyKey.CAIRO_COMMIT_MODE, "nosync");
        assertMemoryLeak(facade, () -> {
            execute("create table t (ts timestamp, v long) timestamp(ts) partition by day wal");
            execute("insert into t values ('2024-01-01T00:00:00.000000Z', 1)");
            drainWalQueue();
            final TableToken tt = engine.verifyTableName("t");
            final String dir = tt.getDirName();
            engine.releaseInactive();

            // The instance default flips: the next writer to open this table enrols it.
            node1.setProperty(PropertyKey.CAIRO_COMMIT_MODE, "adaptive");
            facade.clear();
            try (TableWriter w = getWriter(tt)) {
                Assert.assertEquals(CommitMode.ADAPTIVE, w.getEffectiveCommitMode());
            }

            facade.assertAnchorPrecedesMetadataWrite(dir, "entering adaptive");
        });
    }

    @Test
    public void testLeavingAdaptiveReconcilesTheStateBeforeClearingTheRecord() throws Exception {
        final OrderRecordingFilesFacade facade = new OrderRecordingFilesFacade();
        node1.setProperty(PropertyKey.CAIRO_COMMIT_MODE, "adaptive");
        assertMemoryLeak(facade, () -> {
            execute("create table t (ts timestamp, v long) timestamp(ts) partition by day wal");
            execute("insert into t values ('2024-01-01T00:00:00.000000Z', 1)");
            drainWalQueue();
            final TableToken tt = engine.verifyTableName("t");
            final String dir = tt.getDirName();
            engine.releaseInactive();

            // ...and back again: the next writer reconciles the table, then clears the record.
            node1.setProperty(PropertyKey.CAIRO_COMMIT_MODE, "nosync");
            facade.clear();
            try (TableWriter w = getWriter(tt)) {
                Assert.assertEquals(CommitMode.NOSYNC, w.getEffectiveCommitMode());
            }

            facade.assertAnchorPrecedesMetadataWrite(dir, "leaving adaptive");
        });
    }

    /**
     * Records the order in which files are opened for writing or renamed into place. Only the sequence
     * matters here, so this records paths rather than barrier counts: {@code SyncAttributingFilesFacade}
     * answers "how many", this answers "in what order".
     */
    private static class OrderRecordingFilesFacade extends TestFilesFacadeImpl {
        private final java.util.Map<Long, String> fdToPath = new java.util.HashMap<>();
        private final List<String> log = new ArrayList<>();

        public synchronized void assertAnchorPrecedesMetadataWrite(String tableDir, String direction) {
            final int anchor = lastIndexContaining(tableDir + "/_snapshot");
            // Pin the RECORD WRITE itself -- a write at the enrolled-mode offset of the live _meta -- not
            // merely "some _meta operation". The writer touches _meta for many reasons and the epoch
            // publication copies it to _meta.epoch.N, so anything coarser stops discriminating: an inverted
            // order would still find an unrelated _meta op after the marker and pass.
            // Matches the tail of a logged write line; the absolute path sits between the "WRITE " prefix
            // and the table dir, so the prefix must not be part of the needle.
            final int meta = lastIndexContaining(tableDir + "/_meta@"
                    + TableUtils.META_OFFSET_ENROLLED_COMMIT_MODE);
            Assert.assertTrue(
                    "no _snapshot operation recorded while " + direction + ", so this proves nothing about"
                            + " ordering" + dump(),
                    anchor >= 0
            );
            Assert.assertTrue(
                    "no enrolment-record write recorded while " + direction + ", so the record was never"
                            + " written" + dump(),
                    meta >= 0
            );
            Assert.assertTrue(
                    "the enrolment record was written BEFORE the anchor it speaks for was durable ("
                            + direction + "): last _snapshot op at " + anchor + ", record write at "
                            + meta + dump(),
                    anchor < meta
            );
        }

        public synchronized void clear() {
            log.clear();
        }

        @Override
        public synchronized long openCleanRW(LPSZ name, long size) {
            log.add(pathToString(name));
            final long fd = super.openCleanRW(name, size);
            remember(fd, name);
            return fd;
        }

        @Override
        public synchronized long openRW(LPSZ name, int opts) {
            log.add(pathToString(name));
            final long fd = super.openRW(name, opts);
            remember(fd, name);
            return fd;
        }

        @Override
        public synchronized long write(long fd, long address, long len, long offset) {
            final String path = fdToPath.get(fd);
            if (path != null) {
                log.add("WRITE " + path + '@' + offset);
            }
            return super.write(fd, address, len, offset);
        }

        private void remember(long fd, LPSZ name) {
            if (fd > -1) {
                fdToPath.put(fd, pathToString(name));
            }
        }

        @Override
        public synchronized int rename(LPSZ from, LPSZ to) {
            log.add(pathToString(to));
            return super.rename(from, to);
        }

        /**
         * Decode an {@link LPSZ} by its BYTES. {@code Path$PathLPSZ} does not override
         * {@code Object.toString()} and {@code Utf8s.toString} delegates to it, so either would log an
         * identity hash — every {@code contains()} lookup would then miss and the ordering assertions would
         * report "nothing recorded" for everything. Same decode, and same reason, as
         * {@code SyncAttributingFilesFacade}. Test paths are ASCII temp dirs, so this is exact.
         */
        private static String pathToString(LPSZ name) {
            final int n = name.size();
            final StringBuilder sb = new StringBuilder(n);
            for (int i = 0; i < n; i++) {
                sb.append((char) (name.byteAt(i) & 0xFF));
            }
            return sb.toString();
        }

        private String dump() {
            final StringBuilder sb = new StringBuilder("\nrecorded file operations:\n");
            for (int i = 0, n = log.size(); i < n; i++) {
                sb.append("  ").append(i).append(": ").append(log.get(i)).append('\n');
            }
            return sb.toString();
        }

        private int firstIndexContaining(String needle) {
            for (int i = 0, n = log.size(); i < n; i++) {
                if (log.get(i).contains(needle)) {
                    return i;
                }
            }
            return -1;
        }

        private int lastIndexContaining(String needle) {
            for (int i = log.size() - 1; i >= 0; i--) {
                if (log.get(i).contains(needle)) {
                    return i;
                }
            }
            return -1;
        }
    }
}
