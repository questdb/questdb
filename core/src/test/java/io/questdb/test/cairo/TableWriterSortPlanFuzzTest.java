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

import io.questdb.cairo.TableWriterSegmentCopyInfo;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.std.DirectLongList;
import io.questdb.std.IntList;
import io.questdb.std.LongList;
import io.questdb.std.MemoryTag;
import io.questdb.std.ObjList;
import io.questdb.std.Rnd;
import io.questdb.std.Unsafe;
import io.questdb.std.Vect;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

/**
 * Verifies that the WAL block sort plan, {@link TableWriterSegmentCopyInfo#buildSortPlan()} executed by
 * {@link Vect#sortManySegmentsIndexByPlan}, produces exactly the same shuffle index as
 * {@link Vect#radixSortManySegmentsIndexAsc} sorting all the rows, and that the rows are in (timestamp, seqTxn, row)
 * order, the order in which the WAL apply must write rows of equal timestamps.
 */
public class TableWriterSortPlanFuzzTest {
    private static final Log LOG = LogFactory.getLog(TableWriterSortPlanFuzzTest.class);

    @Test
    public void testAllCopiedWhenSegmentsDoNotOverlap() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            // 3 segments written in turns, every transaction is sorted and does not overlap the others
            Block block = new Block();
            long ts = 1_000_000;
            for (int t = 0; t < 30; t++) {
                block.addSortedTxn(t % 3, ts, 100, 10);
                ts += 100 * 10;
            }
            PlanResult result = block.assertPlanMatchesRadix();
            Assert.assertTrue(result.planned);
            Assert.assertEquals(block.totalRows, result.copyRows);
            Assert.assertEquals(1, result.itemCount);
        });
    }

    @Test
    public void testAllEmptyBlockIsNotPlanned() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            // a block of empty transactions only, e.g. the 2 empty transactions ALTER TABLE ... REBASE WAL
            // commits to one segment, has no rows to copy or sort
            Block block = new Block();
            block.addTxn(0, new long[0]);
            block.addTxn(0, new long[0]);
            PlanResult result = block.assertPlanMatchesRadix();
            Assert.assertFalse(result.planned);
            Assert.assertEquals(0, result.copyRows);
            Assert.assertEquals(0, result.itemCount);

            // empty transactions spread across segments
            block = new Block();
            for (int t = 0; t < 7; t++) {
                block.addTxn(t % 3, new long[0]);
            }
            result = block.assertPlanMatchesRadix();
            Assert.assertFalse(result.planned);
            Assert.assertEquals(0, result.copyRows);
            Assert.assertEquals(0, result.itemCount);
        });
    }

    @Test
    public void testCoalescingStopsAtSegmentEnd() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            // 9 transactions of 25 rows are short enough to coalesce the transactions of a segment into runs.
            // Segment 0 ends at ts 1074, where segment 1 starts with seqTxn 0. A run spanning the end of
            // segment 0 and the start of segment 1 would put the ts 1074 row of seqTxn 3 before the row of seqTxn 0.
            Block block = new Block();
            block.addSortedTxn(1, 1_074, 25, 1);
            block.addSortedTxn(0, 1_000, 25, 1);
            block.addSortedTxn(0, 1_025, 25, 1);
            block.addSortedTxn(0, 1_050, 25, 1);
            block.addSortedTxn(1, 1_099, 25, 1);
            block.addSortedTxn(1, 1_124, 25, 1);
            // the plan copies the run of segment 2, it does not overlap the others
            block.addSortedTxn(2, 0, 25, 1);
            block.addSortedTxn(2, 25, 25, 1);
            block.addSortedTxn(2, 50, 25, 1);
            PlanResult result = block.assertPlanMatchesRadix();
            // without coalescing 9 runs exceed the limit of 225 / 32 runs and there is no plan
            Assert.assertTrue(result.planned);
            // the plan copies the run that coalesces 3 transactions of segment 2 and sorts segments 0 and 1
            Assert.assertEquals(75, result.copyRows);
            Assert.assertEquals(2, result.itemCount);
        });
    }

    @Test
    public void testEqualTimestampsAtBoundary() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            // transactions touch at the boundary timestamp, seqTxn decides the order of the boundary rows
            Block block = new Block();
            block.addSortedTxn(1, 2_000, 100, 0);
            block.addSortedTxn(0, 1_000, 100, 10);
            block.addSortedTxn(0, 1_990, 100, 0);
            block.addSortedTxn(1, 2_000, 100, 10);
            block.addSortedTxn(2, 3_000, 100, 10);
            block.addSortedTxn(2, 3_990, 100, 0);
            block.assertPlanMatchesRadix();
        });
    }

    @Test
    public void testEqualTimestampsAtClusterBoundaryFollowSeqTxn() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            // seqTxn 1 ends at ts 2000, where seqTxn 0 starts. The rows of ts 2000 go in seqTxn order, seqTxn 0
            // first, although seqTxn 0 starts later in time. The plan must sort both transactions together
            // rather than copy them one after the other.
            Block block = new Block();
            block.addSortedTxn(0, 2_000, 100, 10);
            block.addSortedTxn(1, 1_010, 100, 10);
            // the plan copies seqTxn 2, it does not touch the others
            block.addSortedTxn(2, 3_000, 100, 10);
            PlanResult result = block.assertPlanMatchesRadix();
            Assert.assertTrue(result.planned);
            Assert.assertEquals(100, result.copyRows);
            Assert.assertEquals(2, result.itemCount);
        });
    }

    @Test
    public void testFuzz() throws Exception {
        final Rnd rnd = TestUtils.generateRandom(LOG);
        TestUtils.assertMemoryLeak(() -> {
            int planned = 0;
            int compared = 0;
            int copied = 0;
            for (int i = 0; i < 300; i++) {
                Block block = new Block();
                block.generate(rnd);
                PlanResult result = block.assertPlanMatchesRadix();
                planned += result.planned ? 1 : 0;
                compared += result.itemCount > 0 ? 1 : 0;
                copied += result.copyRows > 0 ? 1 : 0;
            }
            LOG.info().$("fuzz coverage [planned=").$(planned).$(", compared=").$(compared).$(", copied=").$(copied).I$();
            // the generator must keep exercising the plan
            Assert.assertTrue("compared " + compared, compared > 50);
            Assert.assertTrue("planned " + planned, planned > 20);
        });
    }

    @Test
    public void testLateTxnIsSortedWithOverlappingOnly() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            Block block = new Block();
            long ts = 1_000_000;
            for (int t = 0; t < 10; t++) {
                block.addSortedTxn(t % 2, ts, 1000, 3);
                ts += 1000 * 3;
            }
            // late transaction overlapping the 3rd transaction only
            block.addSortedTxn(2, 1_000_000 + 2 * 3000 + 5, 100, 1);
            PlanResult result = block.assertPlanMatchesRadix();
            Assert.assertTrue(result.planned);
            Assert.assertEquals(block.totalRows - 1000 - 100, result.copyRows);
            Assert.assertEquals(3, result.itemCount);
        });
    }

    @Test
    public void testNegativeTimestamps() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            Block block = new Block();
            block.addSortedTxn(0, -5_000_000, 200, 7);
            block.addSortedTxn(1, -10_000_000, 200, 7);
            block.addSortedTxn(0, 1_000, 200, 7);
            block.addSortedTxn(1, -2_000, 200, 7);
            PlanResult result = block.assertPlanMatchesRadix();
            Assert.assertTrue(result.planned);
            Assert.assertEquals(block.totalRows, result.copyRows);
        });
    }

    @Test
    public void testOutOfRangeSortTxnRejected() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            Block block = new Block();
            block.addSortedTxn(0, 1_000_000, 500, 10);
            block.addSortedTxn(1, 1_000_500, 500, 10);
            block.addSortedTxn(0, 2_000_000, 500, 10);
            // claims a narrower timestamp range than its rows have, it does not overlap other transactions
            block.addTxnWithMeta(1, new long[]{3_000_100, 3_000_050, 3_000_090}, 3_000_060, 3_000_090, false);
            block.assertPlanRejected();
        });
    }

    @Test
    public void testShortTxnsAreNotPlanned() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            // many 1-row transactions from interleaving writers, sorting is cheaper than planning
            Block block = new Block();
            for (int t = 0; t < 1000; t++) {
                block.addSortedTxn(t % 4, 1_000 + t, 1, 1);
            }
            PlanResult result = block.assertPlanMatchesRadix();
            Assert.assertFalse(result.planned);
        });
    }

    @Test
    public void testUnsortedCopyTxnRejected() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            Block block = new Block();
            block.addSortedTxn(0, 1_000_000, 500, 10);
            block.addSortedTxn(1, 2_000_000, 500, 10);
            long[] ts = new long[100];
            for (int i = 0; i < ts.length; i++) {
                ts[i] = 3_000_000 + (i % 2 == 0 ? i : -i);
            }
            // claims to be in order, but it is not, it does not overlap other transactions
            block.addTxnWithMeta(2, ts, 3_000_000 - 100, 3_000_000 + 100, true);
            block.assertPlanRejected();
        });
    }

    private static long readRev(long address, int bytes) {
        return switch (bytes) {
            case 1 -> Unsafe.getByte(address) & 0xFFL;
            case 2 -> Unsafe.getShort(address) & 0xFFFFL;
            case 4 -> Unsafe.getInt(address) & 0xFFFFFFFFL;
            default -> Unsafe.getLong(address);
        };
    }

    private static class Block {
        // per segment, the transactions of the segment in seqTxn order
        private final ObjList<ObjList<Txn>> segments = new ObjList<>();
        private long totalRows;
        private int txnCount;

        public void addSortedTxn(int segment, long startTs, int rows, long step) {
            long[] ts = new long[rows];
            for (int i = 0; i < rows; i++) {
                ts[i] = startTs + i * step;
            }
            addTxn(segment, ts);
        }

        public void addTxn(int segment, long[] ts) {
            long min = Long.MAX_VALUE;
            long max = Long.MIN_VALUE;
            boolean inOrder = true;
            for (int i = 0; i < ts.length; i++) {
                min = Math.min(min, ts[i]);
                max = Math.max(max, ts[i]);
                inOrder &= i == 0 || ts[i] >= ts[i - 1];
            }
            if (ts.length == 0) {
                // WAL metadata of an empty transaction, must not matter
                min = 0;
                max = 0;
            }
            addTxnWithMeta(segment, ts, min, max, inOrder);
        }

        public void addTxnWithMeta(int segment, long[] ts, long min, long max, boolean inOrder) {
            while (segments.size() <= segment) {
                segments.add(new ObjList<>());
            }
            segments.getQuick(segment).add(new Txn(txnCount++, ts, min, max, inOrder));
            totalRows += ts.length;
        }

        public PlanResult assertPlanMatchesRadix() {
            try (Sorter sorter = new Sorter(this)) {
                final PlanResult result = new PlanResult();
                result.planned = sorter.copyInfo.buildSortPlan();
                result.copyRows = sorter.copyInfo.getSortPlanCopyRows();
                result.itemCount = sorter.copyInfo.getSortPlanItemCount();

                if (totalRows == 0) {
                    // a block without rows has nothing to copy or sort, no plan is built for it
                    Assert.assertFalse("planned a block without rows", result.planned);
                    Assert.assertEquals("copy rows of a block without rows", 0, result.copyRows);
                    Assert.assertEquals("plan items of a block without rows", 0, result.itemCount);
                    return result;
                }

                final long radixFormat = sorter.sortAll();
                Assert.assertTrue("radix sort failed: " + radixFormat, Vect.isIndexSuccess(radixFormat));

                if (result.itemCount > 0) {
                    final long planFormat = sorter.sortByPlan();
                    Assert.assertTrue("plan sort failed: " + planFormat, Vect.isIndexSuccess(planFormat));
                    sorter.assertSameIndex(radixFormat, planFormat);
                    // checks the order independently of the radix sort
                    Assert.assertEquals("first plan row out of (ts, seqTxn, row) order", -1, sorter.findPlanOrderBreak(planFormat));
                }
                return result;
            }
        }

        public void assertPlanRejected() {
            try (Sorter sorter = new Sorter(this)) {
                sorter.copyInfo.buildSortPlan();
                Assert.assertTrue(sorter.copyInfo.getSortPlanItemCount() > 0);
                Assert.assertFalse(Vect.isIndexSuccess(sorter.sortByPlan()));
            }
        }

        public void generate(Rnd rnd) {
            final int segmentCount = 1 + rnd.nextInt(5);
            final int txns = 2 + rnd.nextInt(rnd.nextBoolean() ? 10 : 200);
            final long base = rnd.nextBoolean() ? 1_700_000_000_000_000L : -1_000_000_000_000L;
            final int maxRows = 1 + rnd.nextInt(rnd.nextBoolean() ? 10 : 2000);
            final long slotWidth = 1 + rnd.nextInt(100_000);
            // probabilities in percent
            final int overlapChance = rnd.nextInt(4) * 10;
            final int unsortedChance = rnd.nextInt(3) * 5;
            final int emptyChance = rnd.nextInt(2) * 5;
            // small step produces equal timestamps within and across transactions
            final long maxStep = rnd.nextBoolean() ? 1 : Math.max(1, slotWidth / maxRows);

            // transactions occupy shuffled time slots, some of them spill into the neighbour slots
            final LongList slots = new LongList();
            for (int i = 0; i < txns; i++) {
                slots.add(i);
            }
            for (int i = txns - 1; i > 0; i--) {
                int j = rnd.nextInt(i + 1);
                long tmp = slots.getQuick(i);
                slots.setQuick(i, slots.getQuick(j));
                slots.setQuick(j, tmp);
            }

            for (int t = 0; t < txns; t++) {
                final int rows = rnd.nextInt(100) < emptyChance ? 0 : 1 + rnd.nextInt(maxRows);
                long lo = base + slots.getQuick(t) * slotWidth;
                if (rnd.nextInt(100) < overlapChance) {
                    lo -= rnd.nextLong(3 * slotWidth);
                }
                final long[] ts = new long[rows];
                long cur = lo;
                for (int i = 0; i < rows; i++) {
                    ts[i] = cur;
                    cur += rnd.nextLong(maxStep + 1);
                }
                if (rnd.nextInt(100) < unsortedChance) {
                    for (int i = rows - 1; i > 0; i--) {
                        int j = rnd.nextInt(i + 1);
                        long tmp = ts[i];
                        ts[i] = ts[j];
                        ts[j] = tmp;
                    }
                }
                addTxn(rnd.nextInt(segmentCount), ts);
            }
        }
    }

    private static class PlanResult {
        long copyRows;
        long itemCount;
        boolean planned;
    }

    // Lays the block out in native memory the way TableWriter sees WAL segments and sorts it
    private static class Sorter implements AutoCloseable {
        private final long bufSize;
        private final TableWriterSegmentCopyInfo copyInfo = new TableWriterSegmentCopyInfo();
        private final long cpyPlan;
        private final long cpyRadix;
        private final long outPlan;
        private final long outRadix;
        // seqTxn of every row, by the position of the row in the transaction order
        private final IntList positionSeqTxns = new IntList();
        private final DirectLongList segmentAddresses = new DirectLongList(4, MemoryTag.NATIVE_DEFAULT);
        // per segment, the position of the segment row 0 in the transaction order
        private final LongList segmentBases = new LongList();
        // per segment, the row after the last written row
        private final LongList segmentRowHis = new LongList();
        // per segment, the first written row
        private final LongList segmentRowLos = new LongList();
        private final LongList segmentSizes = new LongList();
        private final long totalRows;
        private int segmentCount;

        Sorter(Block block) {
            totalRows = block.totalRows;
            bufSize = Math.max(totalRows, 1) * 3 * Long.BYTES + Long.BYTES;
            outRadix = Unsafe.malloc(bufSize, MemoryTag.NATIVE_DEFAULT);
            cpyRadix = Unsafe.malloc(bufSize, MemoryTag.NATIVE_DEFAULT);
            outPlan = Unsafe.malloc(bufSize, MemoryTag.NATIVE_DEFAULT);
            cpyPlan = Unsafe.malloc(bufSize, MemoryTag.NATIVE_DEFAULT);

            final Rnd rnd = new Rnd(block.txnCount, block.totalRows);
            for (int s = 0, n = block.segments.size(); s < n; s++) {
                final ObjList<Txn> txns = block.segments.getQuick(s);
                if (txns.size() == 0) {
                    continue;
                }
                // segments do not always start at row 0
                final long segmentLo = rnd.nextInt(3);
                long segmentRows = 0;
                for (int t = 0, m = txns.size(); t < m; t++) {
                    segmentRows += txns.getQuick(t).ts.length;
                }
                final long size = Math.max(1, segmentLo + segmentRows) * 2 * Long.BYTES;
                final long addr = Unsafe.malloc(size, MemoryTag.NATIVE_DEFAULT);
                segmentAddresses.add(addr);
                segmentBases.add(positionSeqTxns.size() - segmentLo);
                segmentSizes.add(size);

                long row = segmentLo;
                for (int t = 0, m = txns.size(); t < m; t++) {
                    final Txn txn = txns.getQuick(t);
                    copyInfo.addTxn(row, txn.seqTxn, txn.ts.length, segmentCount, txn.min, txn.max, txn.inOrder);
                    for (long tsValue : txn.ts) {
                        Unsafe.putLong(addr + row * 2 * Long.BYTES, tsValue);
                        Unsafe.putLong(addr + row * 2 * Long.BYTES + Long.BYTES, row);
                        positionSeqTxns.add(txn.seqTxn);
                        row++;
                    }
                }
                copyInfo.addSegment(1, s, segmentLo, row, false);
                segmentRowLos.add(segmentLo);
                segmentRowHis.add(row);
                segmentCount++;
            }
        }

        public void assertSameIndex(long radixFormat, long planFormat) {
            Assert.assertEquals(Vect.readIndexResultRowCount(radixFormat), Vect.readIndexResultRowCount(planFormat));
            Assert.assertEquals(totalRows, Vect.readIndexResultRowCount(planFormat));
            final int radixSegmentBits = (int) ((radixFormat >>> 48) & 0xF) * 8;
            final int planSegmentBits = (int) ((planFormat >>> 48) & 0xF) * 8;
            final int revBytes = (int) ((radixFormat >>> 52) & 0xF);
            Assert.assertEquals(revBytes, (int) ((planFormat >>> 52) & 0xF));

            for (long r = 0; r < totalRows; r++) {
                final long radixTs = Unsafe.getLong(outRadix + r * 16);
                final long planTs = Unsafe.getLong(outPlan + r * 16);
                final long radixI = Unsafe.getLong(outRadix + r * 16 + 8);
                final long planI = Unsafe.getLong(outPlan + r * 16 + 8);
                if (radixTs != planTs
                        || (radixI & ((1L << radixSegmentBits) - 1)) != (planI & ((1L << planSegmentBits) - 1))
                        || (radixI >>> radixSegmentBits) != (planI >>> planSegmentBits)) {
                    Assert.fail("index mismatch at row " + r
                            + ", radix [ts=" + radixTs + ", i=" + radixI
                            + "], plan [ts=" + planTs + ", i=" + planI
                            + "], first plan row out of (ts, seqTxn, row) order: " + findPlanOrderBreak(planFormat));
                }
            }

            Assert.assertEquals(totalRows, Unsafe.getLong(outPlan + totalRows * 16));
            final long radixRev = outRadix + totalRows * 16 + Long.BYTES;
            final long planRev = outPlan + totalRows * 16 + Long.BYTES;
            for (long r = 0; r < totalRows; r++) {
                Assert.assertEquals("reverse index at " + r, readRev(radixRev + r * revBytes, revBytes), readRev(planRev + r * revBytes, revBytes));
            }
        }

        @Override
        public void close() {
            Unsafe.free(outRadix, bufSize, MemoryTag.NATIVE_DEFAULT);
            Unsafe.free(cpyRadix, bufSize, MemoryTag.NATIVE_DEFAULT);
            Unsafe.free(outPlan, bufSize, MemoryTag.NATIVE_DEFAULT);
            Unsafe.free(cpyPlan, bufSize, MemoryTag.NATIVE_DEFAULT);
            for (int s = 0, n = (int) segmentAddresses.size(); s < n; s++) {
                Unsafe.free(segmentAddresses.get(s), segmentSizes.getQuick(s), MemoryTag.NATIVE_DEFAULT);
            }
            segmentAddresses.close();
            copyInfo.close();
        }

        // Returns the first row of the plan index that does not carry the timestamp of its source row or does not
        // follow the previous row in (timestamp, seqTxn, row) order, or -1 when all the rows pass both checks
        public long findPlanOrderBreak(long planFormat) {
            final int segmentBits = (int) ((planFormat >>> 48) & 0xF) * 8;
            final long segmentMask = (1L << segmentBits) - 1;
            long prevTs = Long.MIN_VALUE;
            int prevSeqTxn = -1;
            long prevRow = -1;
            for (long r = 0; r < totalRows; r++) {
                final long ts = Unsafe.getLong(outPlan + r * 16);
                final long i = Unsafe.getLong(outPlan + r * 16 + 8);
                final long segment = i & segmentMask;
                final long row = i >>> segmentBits;
                if (segment >= segmentBases.size()) {
                    return r;
                }
                final int s = (int) segment;
                if (row < segmentRowLos.getQuick(s) || row >= segmentRowHis.getQuick(s)) {
                    return r;
                }
                // the check does not trust the timestamp the plan sort writes, it reads the source row
                if (ts != Unsafe.getLong(segmentAddresses.get(s) + row * 2 * Long.BYTES)) {
                    return r;
                }
                final int seqTxn = positionSeqTxns.getQuick((int) (segmentBases.getQuick(s) + row));
                if (ts < prevTs || (ts == prevTs && (seqTxn < prevSeqTxn || (seqTxn == prevSeqTxn && row <= prevRow)))) {
                    return r;
                }
                prevTs = ts;
                prevSeqTxn = seqTxn;
                prevRow = row;
            }
            return -1;
        }

        public long sortAll() {
            return Vect.radixSortManySegmentsIndexAsc(
                    outRadix,
                    cpyRadix,
                    segmentAddresses.getAddress(),
                    segmentCount,
                    copyInfo.getTxnInfoAddress(),
                    copyInfo.getTxnCount(),
                    copyInfo.getMaxTxRowCount(),
                    0,
                    0,
                    copyInfo.getMinTimestamp(),
                    copyInfo.getMaxTimestamp(),
                    totalRows,
                    Vect.SHUFFLE_INDEX_FORMAT
            );
        }

        public long sortByPlan() {
            return Vect.sortManySegmentsIndexByPlan(
                    outPlan,
                    cpyPlan,
                    segmentAddresses.getAddress(),
                    copyInfo.getSegmentsAddress(),
                    segmentCount,
                    copyInfo.getTxnInfoAddress(),
                    copyInfo.getTxnCount(),
                    copyInfo.getMaxTxRowCount(),
                    copyInfo.getSortPlanItemsAddress(),
                    copyInfo.getSortPlanItemCount(),
                    copyInfo.getSortPlanTxnsAddress(),
                    copyInfo.getSortPlanTxnCount(),
                    totalRows
            );
        }
    }

    private record Txn(int seqTxn, long[] ts, long min, long max, boolean inOrder) {
    }
}
