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

import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.SnapshotMarker;
import io.questdb.cairo.TableUtils;
import io.questdb.std.FilesFacade;
import io.questdb.std.MemoryTag;
import io.questdb.std.Unsafe;
import io.questdb.std.str.Path;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.std.TestFilesFacadeImpl;
import org.junit.Assert;
import org.junit.Test;

/**
 * Unit tests for {@link SnapshotMarker}: the per-table {@code _snapshot} epoch marker.
 * <p>
 * Tests follow the TDD approach: assertions are designed to FAIL against a missing implementation
 * and PASS against the correct one. Each test exercises a distinct correctness property:
 * <ul>
 *   <li>{@link #testWriteAndReadBack}: basic round-trip.</li>
 *   <li>{@link #testLiveSlotCorruptionFallsBackToPriorSlot}: CRC mismatch on live slot → fallback to prior slot.</li>
 *   <li>{@link #testCorruptLatestEpochFallsBackToEarlier}: two epochs written; corrupt epoch 7 → reads epoch 3.</li>
 *   <li>{@link #testBothSlotsCorruptedReturnsFalse}: both slots corrupt → {@code tryLoad()} returns false.</li>
 *   <li>{@link #testAbsentFileReturnsFalse}: file absent → {@code tryLoad()} returns false.</li>
 *   <li>{@link #testMagicMismatchReturnsFalse}: MAGIC field zeroed → slot treated as absent.</li>
 * </ul>
 */
public class SnapshotMarkerTest extends AbstractCairoTest {

    // ---- basic round-trip ----

    @Test
    public void testWriteAndReadBack() throws Exception {
        assertMemoryLeak(() -> {
            try (Path path = new Path().of(root).concat(TableUtils.SNAPSHOT_FILE_NAME)) {
                try (SnapshotMarker w = new SnapshotMarker(configuration)) {
                    w.of(path);
                    w.write(5L, 3L, 1_000_000L);
                }

                // Reopen and verify.
                try (SnapshotMarker r = new SnapshotMarker(configuration)) {
                    r.of(path);
                    Assert.assertTrue("tryLoad should succeed", r.tryLoad());
                    Assert.assertEquals("epochSeqTxn", 5L, r.getEpochSeqTxn());
                    Assert.assertEquals("epochTxn", 3L, r.getEpochTxn());
                    Assert.assertEquals("ts", 1_000_000L, r.getEpochTs());
                }
            }
        });
    }

    @Test
    public void testCandidatesPreferNewestCutWhenSelectorReverts() throws Exception {
        assertMemoryLeak(() -> {
            final FilesFacade ff = TestFilesFacadeImpl.INSTANCE;
            try (Path path = new Path().of(root).concat(TableUtils.SNAPSHOT_FILE_NAME)) {
                try (SnapshotMarker marker = new SnapshotMarker(configuration)) {
                    marker.of(path);
                    marker.write(3L, 2L, 3_000_000L, 0);
                }
                try (SnapshotMarker marker = new SnapshotMarker(configuration)) {
                    marker.of(path);
                    marker.write(7L, 5L, 7_000_000L, 1);
                }

                final long currentVersion = peekLong(ff, path.$(), SnapshotMarker.OFFSET_VERSION);
                Assert.assertEquals(2L, currentVersion);
                // Model an unchecksummed selector sector reverting while both checksummed slots survive.
                pokeLong(ff, path.$(), SnapshotMarker.OFFSET_VERSION, currentVersion - 1);

                try (SnapshotMarker marker = new SnapshotMarker(configuration)) {
                    marker.of(path);
                    Assert.assertTrue(marker.tryLoad());
                    Assert.assertEquals("raw selector now names the older cut", 3L, marker.getEpochSeqTxn());
                    final SnapshotMarker.Candidate[] candidates = marker.loadCandidates();
                    Assert.assertEquals(2, candidates.length);
                    Assert.assertEquals("recovery ordering must ignore the reverted selector", 7L, candidates[0].epochSeqTxn);
                    Assert.assertEquals(1, candidates[0].generation);
                }
            }
        });
    }

    /**
     * {@code loadCandidates()} must fall back to the surviving slot when the LIVE one is torn.
     * <p>
     * {@code testLiveSlotCorruptionFallsBackToPriorSlot} and {@code testBothSlotsCorruptedReturnsFalse}
     * read as though they cover this, but they exercise {@code tryLoad()} -- a different method with
     * its own fallback. {@code loadCandidates()} is the one {@code RecoveryCoordinator} calls to
     * choose which epoch generation to adopt, and the coverage pass on PR #7411 found its
     * single-survivor and no-survivor branches unreached.
     * <p>
     * That distinction matters here more than usual: the selector word carries no checksum and can
     * "tear or revert independently during a power loss" (this class's own javadoc), so returning the
     * OTHER slot when the selected one is unreadable is the entire reason two slots exist.
     */
    @Test
    public void testCandidatesFallBackToTheSurvivingSlotWhenTheLiveOneIsTorn() throws Exception {
        assertMemoryLeak(() -> {
            final FilesFacade ff = TestFilesFacadeImpl.INSTANCE;
            try (Path path = new Path().of(root).concat(TableUtils.SNAPSHOT_FILE_NAME)) {
                try (SnapshotMarker marker = new SnapshotMarker(configuration)) {
                    marker.of(path);
                    marker.write(3L, 2L, 3_000_000L, 0);
                }
                try (SnapshotMarker marker = new SnapshotMarker(configuration)) {
                    marker.of(path);
                    marker.write(7L, 5L, 7_000_000L, 1);
                }
                // Precondition: both slots valid, so the fallback below is genuinely a fallback and
                // not an artefact of the other slot never having been written.
                try (SnapshotMarker marker = new SnapshotMarker(configuration)) {
                    marker.of(path);
                    Assert.assertEquals("precondition: both slots must be valid",
                            2, marker.loadCandidates().length);
                }
                // Tear the LIVE slot. Version is 2 => even => live is A, and A holds the NEWEST cut
                // (epochSeqTxn 7), because the second write landed there. So tearing it models losing
                // the newest epoch and leaves the PREVIOUS fully-bound one as the only survivor --
                // "it may reject the newest generation and adopt the previous", per loadCandidates'
                // own javadoc.
                pokeLong(ff, path.$(), SnapshotMarker.OFFSET_SLOT_A, 0xDEADBEEFL);

                try (SnapshotMarker marker = new SnapshotMarker(configuration)) {
                    marker.of(path);
                    final SnapshotMarker.Candidate[] candidates = marker.loadCandidates();
                    Assert.assertEquals("exactly the surviving slot must be offered", 1, candidates.length);
                    Assert.assertEquals("the survivor must be the previous cut, not the torn newest one",
                            3L, candidates[0].epochSeqTxn);
                    Assert.assertEquals("and it must carry that cut's generation",
                            0, candidates[0].generation);
                }
            }
        });
    }

    /**
     * Both slots torn: {@code loadCandidates()} must offer NOTHING rather than guess. Recovery then
     * has no candidate to adopt and falls back to full WAL replay, which is the safe direction --
     * returning a half-read slot here would hand recovery an anchor nobody validated.
     */
    @Test
    public void testCandidatesAreEmptyWhenBothSlotsAreTorn() throws Exception {
        assertMemoryLeak(() -> {
            final FilesFacade ff = TestFilesFacadeImpl.INSTANCE;
            try (Path path = new Path().of(root).concat(TableUtils.SNAPSHOT_FILE_NAME)) {
                try (SnapshotMarker marker = new SnapshotMarker(configuration)) {
                    marker.of(path);
                    marker.write(3L, 2L, 3_000_000L, 0);
                }
                try (SnapshotMarker marker = new SnapshotMarker(configuration)) {
                    marker.of(path);
                    marker.write(7L, 5L, 7_000_000L, 1);
                }
                try (SnapshotMarker marker = new SnapshotMarker(configuration)) {
                    marker.of(path);
                    Assert.assertEquals("precondition: both slots must start valid",
                            2, marker.loadCandidates().length);
                }
                pokeLong(ff, path.$(), SnapshotMarker.OFFSET_SLOT_A, 0xDEADBEEFL);
                pokeLong(ff, path.$(), SnapshotMarker.OFFSET_SLOT_B, 0xDEADBEEFL);

                try (SnapshotMarker marker = new SnapshotMarker(configuration)) {
                    marker.of(path);
                    Assert.assertEquals("two torn slots must yield no candidate at all",
                            0, marker.loadCandidates().length);
                }
            }
        });
    }

    // ---- CRC fallback tests ----

    /**
     * Corrupt the live slot's body bytes on disk → {@code tryLoad} must fall back to the other (prior)
     * slot, which was never written, so this should return false and report epoch 0.
     * <p>
     * After a single write the live slot is valid; if we corrupt it the OTHER slot has no valid data
     * either (MAGIC == 0 since the file was just created). Both fail → epoch 0.
     */
    @Test
    public void testLiveSlotCorruptionFallsBackToPriorSlot() throws Exception {
        assertMemoryLeak(() -> {
            final FilesFacade ff = TestFilesFacadeImpl.INSTANCE;
            try (Path path = new Path().of(root).concat(TableUtils.SNAPSHOT_FILE_NAME)) {
                // Write one epoch.
                try (SnapshotMarker w = new SnapshotMarker(configuration)) {
                    w.of(path);
                    w.write(7L, 4L, 2_000_000L);
                }

                // Determine which slot is live: version=1 => live = B (offset 56), other = A (offset 8).
                long version = peekLong(ff, path.$(), SnapshotMarker.OFFSET_VERSION);
                boolean liveIsA = (version & 1L) == 0L;
                long liveSlotOffset = liveIsA ? SnapshotMarker.OFFSET_SLOT_A : SnapshotMarker.OFFSET_SLOT_B;

                // Corrupt the first long of the live slot body (epochSeqTxn field). This invalidates the CRC
                // but DOES NOT touch the MAGIC so the checksum-verify path is exercised (not the MAGIC check).
                long origBody = peekLong(ff, path.$(), liveSlotOffset + SnapshotMarker.SLOT_OFFSET_EPOCH_SEQ_TXN);
                pokeLong(ff, path.$(), liveSlotOffset + SnapshotMarker.SLOT_OFFSET_EPOCH_SEQ_TXN, origBody ^ 0xDEADBEEFL);

                // Reopen and try to load: live slot fails CRC, other slot has no MAGIC => both fail.
                try (SnapshotMarker r = new SnapshotMarker(configuration)) {
                    r.of(path);
                    boolean loaded = r.tryLoad();
                    // After a single write, the other slot was never written and has no MAGIC.
                    // So both slots fail → tryLoad() must return false.
                    Assert.assertFalse("both slots invalid after first write + corrupt → epoch 0", loaded);
                }
            }
        });
    }

    /**
     * Write epoch 3 (first write), then epoch 7 (second write). Corrupt the live slot (epoch 7).
     * {@code tryLoad} must fall back to the OTHER slot (epoch 3) and return it.
     */
    @Test
    public void testCorruptLatestEpochFallsBackToEarlier() throws Exception {
        assertMemoryLeak(() -> {
            final FilesFacade ff = TestFilesFacadeImpl.INSTANCE;
            try (Path path = new Path().of(root).concat(TableUtils.SNAPSHOT_FILE_NAME)) {
                // First write: epoch 3.
                try (SnapshotMarker w = new SnapshotMarker(configuration)) {
                    w.of(path);
                    w.write(3L, 2L, 3_000_000L);
                }

                // Second write: epoch 7. This bumps version to 2, flipping the live slot.
                try (SnapshotMarker w = new SnapshotMarker(configuration)) {
                    w.of(path);
                    w.write(7L, 5L, 7_000_000L);
                }

                // Determine live slot (version=2 => live=A=offset 8, prior=B=offset 56).
                long version = peekLong(ff, path.$(), SnapshotMarker.OFFSET_VERSION);
                boolean liveIsA = (version & 1L) == 0L;
                long liveSlotOffset = liveIsA ? SnapshotMarker.OFFSET_SLOT_A : SnapshotMarker.OFFSET_SLOT_B;

                // Corrupt the live slot (epoch 7) body — flip some body bytes.
                long origBody = peekLong(ff, path.$(), liveSlotOffset + SnapshotMarker.SLOT_OFFSET_EPOCH_TXN);
                pokeLong(ff, path.$(), liveSlotOffset + SnapshotMarker.SLOT_OFFSET_EPOCH_TXN, origBody ^ 0xFF_FF_FF_FFL);

                // Reopen: live slot fails, fallback to prior slot (epoch 3) succeeds.
                try (SnapshotMarker r = new SnapshotMarker(configuration)) {
                    r.of(path);
                    Assert.assertTrue("fallback to prior epoch should succeed", r.tryLoad());
                    Assert.assertEquals("epochSeqTxn from prior slot", 3L, r.getEpochSeqTxn());
                    Assert.assertEquals("epochTxn from prior slot", 2L, r.getEpochTxn());
                    Assert.assertEquals("ts from prior slot", 3_000_000L, r.getEpochTs());
                }
            }
        });
    }

    /**
     * Write twice (epoch 3 then epoch 7). Corrupt BOTH slots. {@code tryLoad} must return false.
     */
    @Test
    public void testBothSlotsCorruptedReturnsFalse() throws Exception {
        assertMemoryLeak(() -> {
            final FilesFacade ff = TestFilesFacadeImpl.INSTANCE;
            try (Path path = new Path().of(root).concat(TableUtils.SNAPSHOT_FILE_NAME)) {
                // Write epoch 3, then epoch 7.
                try (SnapshotMarker w = new SnapshotMarker(configuration)) {
                    w.of(path);
                    w.write(3L, 2L, 3_000_000L);
                }
                try (SnapshotMarker w = new SnapshotMarker(configuration)) {
                    w.of(path);
                    w.write(7L, 5L, 7_000_000L);
                }

                // Corrupt BOTH slots' first body longs.
                long aBody = peekLong(ff, path.$(), SnapshotMarker.OFFSET_SLOT_A + SnapshotMarker.SLOT_OFFSET_EPOCH_SEQ_TXN);
                pokeLong(ff, path.$(), SnapshotMarker.OFFSET_SLOT_A + SnapshotMarker.SLOT_OFFSET_EPOCH_SEQ_TXN, aBody ^ 0x1234_5678L);

                long bBody = peekLong(ff, path.$(), SnapshotMarker.OFFSET_SLOT_B + SnapshotMarker.SLOT_OFFSET_EPOCH_SEQ_TXN);
                pokeLong(ff, path.$(), SnapshotMarker.OFFSET_SLOT_B + SnapshotMarker.SLOT_OFFSET_EPOCH_SEQ_TXN, bBody ^ 0x1234_5678L);

                try (SnapshotMarker r = new SnapshotMarker(configuration)) {
                    r.of(path);
                    Assert.assertFalse("both slots corrupt => epoch 0", r.tryLoad());
                    Assert.assertEquals("epoch 0 → epochSeqTxn must be 0", 0L, r.getEpochSeqTxn());
                    Assert.assertEquals("epoch 0 → epochTxn must be 0", 0L, r.getEpochTxn());
                    Assert.assertEquals("epoch 0 → epochTs must be 0", 0L, r.getEpochTs());
                }
            }
        });
    }

    /**
     * Absent file (never opened): {@code tryLoad} on a fresh instance (no {@code of()} called) must
     * return false. Also test opening an explicitly absent path.
     */
    @Test
    public void testAbsentFileReturnsFalse() throws Exception {
        assertMemoryLeak(() -> {
            try (SnapshotMarker r = new SnapshotMarker(configuration)) {
                // Do NOT call of(): mem is null.
                Assert.assertFalse("absent file => epoch 0", r.tryLoad());
                Assert.assertEquals(0L, r.getEpochSeqTxn());
                Assert.assertEquals(0L, r.getEpochTxn());
                Assert.assertEquals(0L, r.getEpochTs());
            }
        });
    }

    /**
     * Overwrite the MAGIC field of a valid slot to zero. The reader must treat the slot as absent
     * (magic-gated presence) rather than running the checksum verify.
     */
    @Test
    public void testMagicMismatchReturnsFalse() throws Exception {
        assertMemoryLeak(() -> {
            final FilesFacade ff = TestFilesFacadeImpl.INSTANCE;
            try (Path path = new Path().of(root).concat(TableUtils.SNAPSHOT_FILE_NAME)) {
                // Write once: version=1, live=slot B.
                try (SnapshotMarker w = new SnapshotMarker(configuration)) {
                    w.of(path);
                    w.write(9L, 6L, 9_000_000L);
                }

                long version = peekLong(ff, path.$(), SnapshotMarker.OFFSET_VERSION);
                boolean liveIsA = (version & 1L) == 0L;
                long liveSlotOffset = liveIsA ? SnapshotMarker.OFFSET_SLOT_A : SnapshotMarker.OFFSET_SLOT_B;

                // Zero the MAGIC of the live slot. MAGIC must be present for the slot to be considered valid.
                pokeLong(ff, path.$(), liveSlotOffset + SnapshotMarker.SLOT_BODY_SIZE, 0L);

                try (SnapshotMarker r = new SnapshotMarker(configuration)) {
                    r.of(path);
                    // Live slot: no MAGIC => skip. Other slot: never written, no MAGIC either => both absent.
                    Assert.assertFalse("zeroed MAGIC => slot absent => epoch 0", r.tryLoad());
                }
            }
        });
    }

    // ---- on-disk layout verification ----

    /**
     * Verify the actual on-disk constants: FILE_SIZE, slot offsets, SLOT_SIZE are consistent.
     */
    @Test
    public void testOnDiskLayoutConstants() {
        Assert.assertEquals(8, SnapshotMarker.OFFSET_SLOT_A);
        Assert.assertEquals(8 + SnapshotMarker.SLOT_SIZE, SnapshotMarker.OFFSET_SLOT_B);
        Assert.assertEquals(8 + 2 * SnapshotMarker.SLOT_SIZE, SnapshotMarker.FILE_SIZE);
        Assert.assertEquals(32 + 16, SnapshotMarker.SLOT_SIZE); // body + trailer
        Assert.assertEquals(48, SnapshotMarker.SLOT_SIZE);
        Assert.assertEquals(56, SnapshotMarker.OFFSET_SLOT_B);
        Assert.assertEquals(104, SnapshotMarker.FILE_SIZE);
    }

    // ---- private test helpers (mirrors ColumnVersionWriterTest helpers) ----

    private static long peekLong(FilesFacade ff, io.questdb.std.str.LPSZ path, long offset) {
        long fd = ff.openRO(path);
        Assert.assertTrue("open for read failed", fd > -1);
        long buf = Unsafe.malloc(Long.BYTES, MemoryTag.NATIVE_DEFAULT);
        try {
            Assert.assertEquals(Long.BYTES, ff.read(fd, buf, Long.BYTES, offset));
            return Unsafe.getLong(buf);
        } finally {
            Unsafe.free(buf, Long.BYTES, MemoryTag.NATIVE_DEFAULT);
            ff.close(fd);
        }
    }

    private static void pokeLong(FilesFacade ff, io.questdb.std.str.LPSZ path, long offset, long value) {
        long fd = ff.openRW(path, CairoConfiguration.O_NONE);
        Assert.assertTrue("open for write failed", fd > -1);
        long buf = Unsafe.malloc(Long.BYTES, MemoryTag.NATIVE_DEFAULT);
        try {
            Unsafe.putLong(buf, value);
            Assert.assertEquals(Long.BYTES, ff.write(fd, buf, Long.BYTES, offset));
            ff.fsync(fd);
        } finally {
            Unsafe.free(buf, Long.BYTES, MemoryTag.NATIVE_DEFAULT);
            ff.close(fd);
        }
    }
}
