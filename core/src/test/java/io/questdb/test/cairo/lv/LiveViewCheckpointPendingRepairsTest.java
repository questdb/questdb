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

package io.questdb.test.cairo.lv;

import io.questdb.cairo.lv.LiveViewCheckpointPendingRepairs;
import io.questdb.cairo.vm.Vm;
import io.questdb.cairo.vm.api.MemoryMARW;
import io.questdb.std.FilesFacade;
import io.questdb.std.MemoryTag;
import io.questdb.std.Numbers;
import io.questdb.std.str.Path;
import io.questdb.test.AbstractCairoTest;
import org.junit.Assert;
import org.junit.Test;

/**
 * Unit coverage for the durable pending-repair set itself: the merge rule, the drain
 * order, the cap, the round trip, and the three ways a set on disk reads as untrustworthy.
 * <p>
 * The last of those is the one worth having a unit test for. A set that fails to validate
 * cannot be dropped - its entries name closed segments whose corrections the view has
 * already consumed, and the base WAL floor has moved over the commits that carried them -
 * so the caller has to be able to tell "no set" from "a set I cannot read", and every path
 * that produces the second has to actually report it.
 */
public class LiveViewCheckpointPendingRepairsTest extends AbstractCairoTest {

    private static final long DEFINITION_TXN = 42;

    @Test
    public void testAddMergesIntoASegmentTheSetAlreadyHolds() {
        final LiveViewCheckpointPendingRepairs set = new LiveViewCheckpointPendingRepairs();
        Assert.assertTrue(set.add(100, 200, 130, 140, 3));
        Assert.assertTrue(set.add(100, 200, 110, 190, 5));

        Assert.assertEquals("one segment corrected twice is one entry", 1, set.size());
        Assert.assertEquals(110, set.getMinTs(0));
        Assert.assertEquals(190, set.getMaxTs(0));
        Assert.assertEquals("the row counts add up", 8, set.getRowCount(0));
        Assert.assertEquals(8, set.getTotalRowCount());
        Assert.assertTrue(set.contains(100));
        Assert.assertFalse(set.contains(200));
    }

    @Test
    public void testEntriesComeBackOldestFirstWhateverOrderTheyArriveIn() {
        final LiveViewCheckpointPendingRepairs set = new LiveViewCheckpointPendingRepairs();
        Assert.assertTrue(set.add(300, 400, 310, 320, 1));
        Assert.assertTrue(set.add(100, 200, 110, 120, 1));
        Assert.assertTrue(set.add(200, 300, 210, 220, 1));

        Assert.assertEquals(3, set.size());
        Assert.assertEquals(100, set.getSegmentStart(0));
        Assert.assertEquals(200, set.getSegmentStart(1));
        Assert.assertEquals(300, set.getSegmentStart(2));
        Assert.assertEquals(100, set.oldestSegmentStart());

        // A pass drains from the front, and what is left keeps the order.
        set.removeAt(0);
        Assert.assertEquals(200, set.oldestSegmentStart());
        Assert.assertEquals(300, set.getSegmentStart(1));
    }

    @Test
    public void testAFullSetRefusesAFreshSegmentAndStillMergesAKnownOne() {
        final LiveViewCheckpointPendingRepairs set = new LiveViewCheckpointPendingRepairs();
        for (int i = 0; i < LiveViewCheckpointPendingRepairs.MAX_SEGMENTS; i++) {
            final long start = i * 100L;
            Assert.assertTrue(set.add(start, start + 100, start + 10, start + 20, 1));
        }
        Assert.assertEquals(LiveViewCheckpointPendingRepairs.MAX_SEGMENTS, set.size());
        Assert.assertFalse("a fresh segment past the cap is refused", set.add(-100, 0, -90, -80, 1));
        Assert.assertTrue("a segment the set already holds still merges", set.add(0, 100, 5, 95, 7));
        Assert.assertEquals(LiveViewCheckpointPendingRepairs.MAX_SEGMENTS, set.size());
        Assert.assertEquals(8, set.getRowCount(0));
    }

    @Test
    public void testAnEmptySetIsWrittenAsNoFileAtAll() throws Exception {
        assertMemoryLeak(() -> {
            final LiveViewCheckpointPendingRepairs set = new LiveViewCheckpointPendingRepairs();
            Assert.assertTrue(set.add(100, 200, 110, 120, 1));
            withCheckpointsDir(dir -> {
                LiveViewCheckpointPendingRepairs.write(configuration, dir, DEFINITION_TXN, set);
                set.removeAt(0);
                LiveViewCheckpointPendingRepairs.write(configuration, dir, DEFINITION_TXN, set);

                final LiveViewCheckpointPendingRepairs read = new LiveViewCheckpointPendingRepairs();
                Assert.assertEquals(
                        "an empty set removes the file rather than leaving one to validate",
                        LiveViewCheckpointPendingRepairs.READ_ABSENT,
                        LiveViewCheckpointPendingRepairs.read(configuration, dir, DEFINITION_TXN, read)
                );
                Assert.assertTrue(read.isEmpty());
            });
        });
    }

    @Test
    public void testASetSurvivesTheRoundTrip() throws Exception {
        assertMemoryLeak(() -> {
            final LiveViewCheckpointPendingRepairs set = new LiveViewCheckpointPendingRepairs();
            Assert.assertTrue(set.add(300, 400, 310, 390, 4));
            Assert.assertTrue(set.add(100, 200, 110, 190, 2));
            withCheckpointsDir(dir -> {
                LiveViewCheckpointPendingRepairs.write(configuration, dir, DEFINITION_TXN, set);

                final LiveViewCheckpointPendingRepairs read = new LiveViewCheckpointPendingRepairs();
                Assert.assertEquals(
                        LiveViewCheckpointPendingRepairs.READ_OK,
                        LiveViewCheckpointPendingRepairs.read(configuration, dir, DEFINITION_TXN, read)
                );
                Assert.assertEquals(2, read.size());
                Assert.assertEquals(100, read.getSegmentStart(0));
                Assert.assertEquals(200, read.getSegmentEndExclusive(0));
                Assert.assertEquals(110, read.getMinTs(0));
                Assert.assertEquals(190, read.getMaxTs(0));
                Assert.assertEquals(2, read.getRowCount(0));
                Assert.assertEquals(300, read.getSegmentStart(1));
                Assert.assertEquals(6, read.getTotalRowCount());
            });
        });
    }

    @Test
    public void testACorruptedByteReadsAsCorruptRatherThanAsAbsent() throws Exception {
        assertMemoryLeak(() -> {
            final LiveViewCheckpointPendingRepairs set = new LiveViewCheckpointPendingRepairs();
            Assert.assertTrue(set.add(100, 200, 110, 190, 2));
            withCheckpointsDir(dir -> {
                LiveViewCheckpointPendingRepairs.write(configuration, dir, DEFINITION_TXN, set);
                // One byte inside the first entry, which the trailing CRC covers.
                flipByte(dir, LiveViewCheckpointPendingRepairs.HEADER_SIZE);

                final LiveViewCheckpointPendingRepairs read = new LiveViewCheckpointPendingRepairs();
                Assert.assertEquals(
                        LiveViewCheckpointPendingRepairs.READ_CORRUPT,
                        LiveViewCheckpointPendingRepairs.read(configuration, dir, DEFINITION_TXN, read)
                );
                Assert.assertTrue(read.isEmpty());
            });
        });
    }

    @Test
    public void testASetWrittenForAnotherViewReadsAsCorrupt() throws Exception {
        assertMemoryLeak(() -> {
            final LiveViewCheckpointPendingRepairs set = new LiveViewCheckpointPendingRepairs();
            Assert.assertTrue(set.add(100, 200, 110, 190, 2));
            withCheckpointsDir(dir -> {
                LiveViewCheckpointPendingRepairs.write(configuration, dir, DEFINITION_TXN, set);

                final LiveViewCheckpointPendingRepairs read = new LiveViewCheckpointPendingRepairs();
                Assert.assertEquals(
                        "a set naming another view's segments is not this view's empty set",
                        LiveViewCheckpointPendingRepairs.READ_CORRUPT,
                        LiveViewCheckpointPendingRepairs.read(configuration, dir, DEFINITION_TXN + 1, read)
                );
            });
        });
    }

    @Test
    public void testATruncatedFileReadsAsCorrupt() throws Exception {
        assertMemoryLeak(() -> {
            final LiveViewCheckpointPendingRepairs set = new LiveViewCheckpointPendingRepairs();
            Assert.assertTrue(set.add(100, 200, 110, 190, 2));
            Assert.assertTrue(set.add(300, 400, 310, 390, 2));
            withCheckpointsDir(dir -> {
                LiveViewCheckpointPendingRepairs.write(configuration, dir, DEFINITION_TXN, set);
                final FilesFacade ff = configuration.getFilesFacade();
                try (Path file = new Path()) {
                    LiveViewCheckpointPendingRepairs.pendingPath(file, dir);
                    final long fd = ff.openRW(file.$(), configuration.getWriterFileOpenOpts());
                    Assert.assertTrue(fd > -1);
                    try {
                        // Drops the second entry and the CRC, leaving a header that claims two.
                        Assert.assertTrue(ff.truncate(fd, LiveViewCheckpointPendingRepairs.HEADER_SIZE
                                + LiveViewCheckpointPendingRepairs.ENTRY_SIZE));
                    } finally {
                        ff.close(fd);
                    }
                }

                final LiveViewCheckpointPendingRepairs read = new LiveViewCheckpointPendingRepairs();
                Assert.assertEquals(
                        LiveViewCheckpointPendingRepairs.READ_CORRUPT,
                        LiveViewCheckpointPendingRepairs.read(configuration, dir, DEFINITION_TXN, read)
                );
            });
        });
    }

    @Test
    public void testCopyFromReplacesEveryEntry() {
        final LiveViewCheckpointPendingRepairs src = new LiveViewCheckpointPendingRepairs();
        Assert.assertTrue(src.add(100, 200, 110, 190, 2));
        Assert.assertTrue(src.add(300, 400, 310, 390, 4));

        final LiveViewCheckpointPendingRepairs dst = new LiveViewCheckpointPendingRepairs();
        Assert.assertTrue(dst.add(500, 600, 510, 590, 9));
        dst.copyFrom(src);

        Assert.assertEquals(2, dst.size());
        Assert.assertEquals(100, dst.getSegmentStart(0));
        Assert.assertEquals(300, dst.getSegmentStart(1));
        Assert.assertEquals(6, dst.getTotalRowCount());
        Assert.assertFalse(dst.contains(500));
    }

    @Test
    public void testAnEmptySetNamesNoOldestSegment() {
        final LiveViewCheckpointPendingRepairs set = new LiveViewCheckpointPendingRepairs();
        Assert.assertTrue(set.isEmpty());
        Assert.assertEquals(0, set.size());
        Assert.assertEquals(0, set.getTotalRowCount());
        Assert.assertEquals(Numbers.LONG_NULL, set.oldestSegmentStart());
    }

    private void flipByte(Path checkpointsDir, long offset) {
        final FilesFacade ff = configuration.getFilesFacade();
        try (Path file = new Path()) {
            LiveViewCheckpointPendingRepairs.pendingPath(file, checkpointsDir);
            final long length = ff.length(file.$());
            final MemoryMARW mem = Vm.getCMARWInstance();
            try {
                mem.of(ff, file.$(), length, -1, MemoryTag.MMAP_DEFAULT, io.questdb.cairo.CairoConfiguration.O_NONE, -1);
                mem.putByte(offset, (byte) (mem.getByte(offset) ^ 0xFF));
            } finally {
                mem.close(false);
            }
        }
    }

    /**
     * Runs {@code action} against a throwaway directory standing in for a view's
     * {@code _checkpoints}, and removes it afterwards. The set creates the directory
     * itself on the first write, which is the behaviour a view that defers before it ever
     * seals depends on.
     */
    private void withCheckpointsDir(CheckpointsDirAction action) throws Exception {
        final FilesFacade ff = configuration.getFilesFacade();
        try (Path dir = new Path()) {
            dir.of(configuration.getDbRoot()).concat("pending-repairs-test");
            try {
                action.run(dir);
            } finally {
                dir.of(configuration.getDbRoot()).concat("pending-repairs-test").slash();
                ff.rmdir(dir);
            }
        }
    }

    @FunctionalInterface
    private interface CheckpointsDirAction {
        void run(Path checkpointsDir) throws Exception;
    }
}
