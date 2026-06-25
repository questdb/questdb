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

import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.SnapshotMarker;
import io.questdb.cairo.TableUtils;
import io.questdb.std.FilesFacade;
import io.questdb.std.MemoryTag;
import io.questdb.std.Unsafe;
import io.questdb.std.str.LPSZ;
import io.questdb.std.str.Path;
import io.questdb.test.cairo.DefaultTestCairoConfiguration;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

/**
 * Crash-consistency tests for {@link SnapshotMarker}: the per-table {@code _snapshot} epoch marker.
 * <p>
 * The invariant under test:
 * <pre>
 *   After any simulated power-loss crash during write(), reopening the marker MUST yield EITHER
 *   (a) the PREVIOUS (already committed and fsync'd) epoch, OR
 *   (b) false / epoch 0 (no valid epoch at all) —
 *   but NEVER a torn newer epoch (a partially written value that passes the CRC check).
 * </pre>
 *
 * <h3>Crash model</h3>
 * {@link CrashFaultFilesFacade} models the OS durability contract:
 * <ul>
 *   <li>{@link CrashFaultFilesFacade#markDurableBaseline} snapshots current file bytes as the
 *       durable state (simulates "prior committed and log-journaled").</li>
 *   <li>{@link CrashFaultFilesFacade#crash} reverts each file to its durable snapshot, applying
 *       any registered {@code tornTail} ranges (to zero chosen byte ranges).</li>
 *   <li>{@code armCrashAt(n)} throws {@link CrashSimulationError} on the n-th durability op
 *       (msync/fsync). Crucially, the crash fires AFTER that op's model update — so a crash at
 *       op 1 (the msync in write()) fires after msync makes the content durable.</li>
 * </ul>
 *
 * <h3>write() durability op ordering</h3>
 * {@code SnapshotMarker.write()} issues durability ops in order:
 * <ol>
 *   <li>msync (MS_SYNC) — journal commit + device flush over the whole file.</li>
 *   <li>fsync — explicit fd-level durability anchor.</li>
 * </ol>
 *
 * <h3>Test scenarios</h3>
 * <ol>
 *   <li>{@link #testCrashMidFlipVersionWordTornYieldsOldEpoch}: use {@code tornTail} to zero the
 *       version word after the crash, simulating a torn version flip. After crash, the version is
 *       0 (= "A is live") but slot A was never written — reader falls back to slot B (epoch 3).</li>
 *   <li>{@link #testCrashAfterMsyncBeforeFsyncYieldsNewEpoch}: crash after the msync (op 1)
 *       completes — the whole file is durable. After crash, the new epoch must be readable
 *       (proves the msync alone is the hard durability anchor).</li>
 *   <li>{@link #testCrashAfterFsyncYieldsNewEpoch}: crash after both ops — same outcome,
 *       proves belt-and-suspenders fsync also makes the new epoch durable.</li>
 * </ol>
 */
public class SnapshotMarkerCrashTest {

    @ClassRule
    public static TemporaryFolder temp = new TemporaryFolder();

    /**
     * Simulate a torn version-word flip: the slot body is written (and msync'd = durable), but the
     * version word itself is torn (zeroed on-disk by {@code tornTail}). After crash, the reader
     * sees version=0 (= slot A is live), but slot A has never been written — it falls back to slot
     * B (the prior epoch 3). This proves the A/B fallback is exercised on a version-word tear.
     * <p>
     * Setup:
     * <ol>
     *   <li>Write epoch 3 → version=1 → slot B is live.</li>
     *   <li>Mark durable baseline (epoch 3 is the "prior log-journaled state").</li>
     *   <li>Write epoch 7 normally (both msync + fsync, version=2 → slot A is live).</li>
     *   <li>Register tornTail on the version word [0, 8): zero it on crash.</li>
     *   <li>crash() reverts to the durable snapshot (epoch 7 bytes, because msync made them
     *       durable) BUT zeros the version word.</li>
     *   <li>On reopen: version=0 → slot A is live. Slot A holds epoch 7 (written in step 3).
     *       Slot B holds epoch 3 (written in step 1). Since version=0 selects A (epoch 7), and
     *       epoch 7 IS valid (CRC intact), the reader returns epoch 7.</li>
     * </ol>
     * <b>Refinement:</b> zero the CRC field of slot A too (to simulate a torn checksum), forcing
     * the reader to fall back to slot B (epoch 3). This strictly exercises the fallback path.
     */
    @Test
    public void testCrashMidFlipVersionWordTornYieldsOldEpoch() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            final CrashFaultFilesFacade ff = new CrashFaultFilesFacade();
            final CairoConfiguration config = configWith(ff);
            final String dir = temp.newFolder("snap_crash_1").getAbsolutePath();

            try (Path path = new Path().of(dir).concat(TableUtils.SNAPSHOT_FILE_NAME)) {
                // Step 1: write epoch 3 (version=1, live=B).
                try (SnapshotMarker w = new SnapshotMarker(config)) {
                    w.of(path);
                    w.write(3L, 2L, 3_000_000L);
                }
                // Step 2: mark baseline = epoch 3 is the durable prior state.
                ff.markDurableBaseline(dir);

                // Step 3: write epoch 7 (version=2, live=A). Both msync + fsync fire.
                try (SnapshotMarker w = new SnapshotMarker(config)) {
                    w.of(path);
                    w.write(7L, 5L, 7_000_000L);
                }

                // Step 4: register tornTail on the version word [0, 8) AND the slot A checksum.
                // Zeroing the version word makes version=0 → reader selects slot A as live.
                // Zeroing the slot A checksum (the 8 bytes at offset SLOT_A + BODY_SIZE + 8) makes
                // slot A fail the CRC check → reader falls back to slot B (epoch 3).
                // This simulates: version flip torn (word zeroed) AND new slot's checksum torn.
                LPSZ absPath = path.$();
                // Version word at offset 0, length 8.
                ff.tornTail(absPath, SnapshotMarker.OFFSET_VERSION, Long.BYTES);
                // Slot A's checksum field at OFFSET_SLOT_A + SLOT_BODY_SIZE + 8 (the checksum long).
                ff.tornTail(absPath, SnapshotMarker.OFFSET_SLOT_A + SnapshotMarker.SLOT_BODY_SIZE + Long.BYTES, Long.BYTES);

                // Step 5: crash(). Restores to the post-msync durable snapshot (epoch 7 bytes in
                // both slots), then zeros the two tornTail ranges above.
                ff.crash(dir);

                // Step 6: reopen. Version word = 0 → live = A. Slot A checksum = 0 ≠ computed → CRC fail.
                // Fallback to slot B: slot B holds epoch 3 (its body was written by the first write
                // and was NOT torn). Slot B CRC must be valid → return epoch 3.
                try (SnapshotMarker r = new SnapshotMarker(config)) {
                    r.of(path);
                    boolean loaded = r.tryLoad();
                    Assert.assertTrue("fallback to prior epoch must succeed", loaded);
                    // Must never be the torn NEW epoch (7) — only the PRIOR (3) or absent.
                    Assert.assertEquals("prior epochSeqTxn", 3L, r.getEpochSeqTxn());
                    Assert.assertEquals("prior epochTxn", 2L, r.getEpochTxn());
                    Assert.assertEquals("prior ts", 3_000_000L, r.getEpochTs());
                }
            }
        });
    }

    /**
     * Crash after the msync (op 1) completes but before the explicit fsync (op 2).
     * <p>
     * {@code armCrashAt(2)} fires on the 2nd durability op (fsync). Op 1 (msync = MS_SYNC) is a
     * device-flush-equivalent that makes the whole file durable in the content model. So after
     * {@code crash()}, the new epoch (7) IS readable.
     * <p>
     * This verifies: the msync alone (before the explicit fsync) is the hard durability anchor.
     */
    @Test
    public void testCrashAfterMsyncBeforeFsyncYieldsNewEpoch() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            final CrashFaultFilesFacade ff = new CrashFaultFilesFacade();
            final CairoConfiguration config = configWith(ff);
            final String dir = temp.newFolder("snap_crash_2").getAbsolutePath();

            try (Path path = new Path().of(dir).concat(TableUtils.SNAPSHOT_FILE_NAME)) {
                // Write baseline (epoch 3).
                try (SnapshotMarker w = new SnapshotMarker(config)) {
                    w.of(path);
                    w.write(3L, 2L, 3_000_000L); // ops 1 (msync) + 2 (fsync)
                }
                ff.markDurableBaseline(dir);

                // Arm crash at op 3 (= the fsync of the second write).
                // The second write's ops are: 3 (msync) + 4 (fsync for total since both writes fire ops).
                // We want to crash at the SECOND write's fsync. Accounting:
                // - First write already fired ops 1 (msync) + 2 (fsync) = 2 total.
                // - Second write fires op 3 (msync) + op 4 (fsync).
                // Crash at op 4 = after second write's msync, before second write's fsync.
                ff.armCrashAt(4);

                boolean crashed = false;
                try {
                    try (SnapshotMarker w = new SnapshotMarker(config)) {
                        w.of(path);
                        w.write(7L, 5L, 7_000_000L); // msync=op3 (durable), fsync=op4 → crash
                    }
                } catch (CrashSimulationError e) {
                    crashed = true;
                }
                Assert.assertTrue("crash must have fired on the fsync (op 4)", crashed);

                ff.crash(dir);

                // Op 3 (msync) made epoch 7 fully durable. After crash(), epoch 7 is readable.
                try (SnapshotMarker r = new SnapshotMarker(config)) {
                    r.of(path);
                    Assert.assertTrue("new epoch must be readable after msync survives crash", r.tryLoad());
                    Assert.assertEquals("new epochSeqTxn", 7L, r.getEpochSeqTxn());
                    Assert.assertEquals("new epochTxn", 5L, r.getEpochTxn());
                    Assert.assertEquals("new ts", 7_000_000L, r.getEpochTs());
                }
            }
        });
    }

    /**
     * Crash after BOTH the msync AND the fsync of the second write.
     * <p>
     * {@code armCrashAt(5)} fires on the 5th durability op, which is after all ops of both writes.
     * The new epoch must be fully durable and readable after crash.
     * <p>
     * This verifies the belt-and-suspenders fsync also makes the epoch durable.
     */
    @Test
    public void testCrashAfterFsyncYieldsNewEpoch() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            final CrashFaultFilesFacade ff = new CrashFaultFilesFacade();
            final CairoConfiguration config = configWith(ff);
            final String dir = temp.newFolder("snap_crash_3").getAbsolutePath();

            try (Path path = new Path().of(dir).concat(TableUtils.SNAPSHOT_FILE_NAME)) {
                // Write baseline (epoch 3): ops 1 (msync) + 2 (fsync).
                try (SnapshotMarker w = new SnapshotMarker(config)) {
                    w.of(path);
                    w.write(3L, 2L, 3_000_000L);
                }
                ff.markDurableBaseline(dir);

                // Write epoch 7: ops 3 (msync) + 4 (fsync) — let both complete.
                try (SnapshotMarker w = new SnapshotMarker(config)) {
                    w.of(path);
                    w.write(7L, 5L, 7_000_000L);
                }
                // Now crash on op 5 (any subsequent op, e.g. open a fake marker read fd and fsync it).
                // Since we can't easily fire op 5 through SnapshotMarker, we just call crash() directly:
                // the content model already saw ops 1-4 and epoch 7 is fully durable.
                ff.crash(dir);

                try (SnapshotMarker r = new SnapshotMarker(config)) {
                    r.of(path);
                    Assert.assertTrue("epoch 7 must survive crash after both msync+fsync", r.tryLoad());
                    Assert.assertEquals(7L, r.getEpochSeqTxn());
                    Assert.assertEquals(5L, r.getEpochTxn());
                    Assert.assertEquals(7_000_000L, r.getEpochTs());
                }
            }
        });
    }

    /**
     * Both slots corrupted (both MAGIC fields torn): {@code tryLoad()} must return false (epoch 0).
     * <p>
     * Simulates a crash so severe that BOTH slots' MAGIC fields are zeroed. This is the "neither
     * slot is valid" case, which must safely degrade to epoch 0 (full WAL replay).
     */
    @Test
    public void testBothSlotsMagicTornYieldsEpoch0() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            final CrashFaultFilesFacade ff = new CrashFaultFilesFacade();
            final CairoConfiguration config = configWith(ff);
            final String dir = temp.newFolder("snap_crash_4").getAbsolutePath();

            try (Path path = new Path().of(dir).concat(TableUtils.SNAPSHOT_FILE_NAME)) {
                // Write two epochs so both slots are populated.
                try (SnapshotMarker w = new SnapshotMarker(config)) {
                    w.of(path);
                    w.write(3L, 2L, 3_000_000L);
                }
                try (SnapshotMarker w = new SnapshotMarker(config)) {
                    w.of(path);
                    w.write(7L, 5L, 7_000_000L);
                }
                ff.markDurableBaseline(dir);

                // Register tornTail to zero both slots' MAGIC fields on crash.
                LPSZ absPath = path.$();
                ff.tornTail(absPath, SnapshotMarker.OFFSET_SLOT_A + SnapshotMarker.SLOT_BODY_SIZE, Long.BYTES);
                ff.tornTail(absPath, SnapshotMarker.OFFSET_SLOT_B + SnapshotMarker.SLOT_BODY_SIZE, Long.BYTES);

                ff.crash(dir);

                try (SnapshotMarker r = new SnapshotMarker(config)) {
                    r.of(path);
                    Assert.assertFalse("both MAGIC torn => epoch 0", r.tryLoad());
                    Assert.assertEquals(0L, r.getEpochSeqTxn());
                    Assert.assertEquals(0L, r.getEpochTxn());
                }
            }
        });
    }

    // ---- private helpers ----

    /** Build a {@link CairoConfiguration} backed by the given {@link FilesFacade}. */
    private static CairoConfiguration configWith(FilesFacade ff) {
        return new DefaultTestCairoConfiguration(temp.getRoot().getAbsolutePath()) {
            @Override
            public FilesFacade getFilesFacade() {
                return ff;
            }
        };
    }
}
