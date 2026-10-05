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

package io.questdb.test.cairo.composite;

import io.questdb.cairo.DefaultCairoConfiguration;
import io.questdb.cairo.O3CompositeMergeStrategy;
import io.questdb.cairo.PartitionCompactionPolicy;
import io.questdb.cairo.PartitionGeometry;
import io.questdb.cairo.TxReader;
import io.questdb.std.FilesFacadeImpl;
import io.questdb.std.datetime.microtime.Micros;
import io.questdb.test.AbstractCairoTest;
import org.junit.Assert;
import org.junit.Test;

public class PartitionCompactionPolicyTest extends AbstractCairoTest {
    private static final long NOW = 100 * Micros.MINUTE_MICROS;

    @Test
    public void testAgeIsAvailableWithoutPressure() throws Exception {
        assertMemoryLeak(() -> {
            final TestConfiguration cfg = new TestConfiguration();
            cfg.hasPressureTrigger = false;
            try (TestTxWriter tx = new TestTxWriter(cfg); TestGeometry geometry = new TestGeometry(tx)) {
                final PartitionCompactionPolicy policy = new PartitionCompactionPolicy(cfg);
                Assert.assertEquals(0, policy.selectPartition(tx, geometry, 1, NOW, 0));
                Assert.assertEquals(PartitionCompactionPolicy.REASON_AGE, policy.getSelectedReason());
            }
        });
    }

    @Test
    public void testPressureRanksWasteAndNeverFallsThroughToAge() throws Exception {
        assertMemoryLeak(() -> {
            final TestConfiguration cfg = new TestConfiguration();
            try (TestTxWriter tx = new TestTxWriter(cfg); TestGeometry geometry = new TestGeometry(tx)) {
                final PartitionCompactionPolicy policy = new PartitionCompactionPolicy(cfg);
                Assert.assertEquals(3, policy.selectPartition(tx, geometry, 1, NOW, 0));
                Assert.assertEquals(PartitionCompactionPolicy.REASON_TABLE_PRESSURE, policy.getSelectedReason());
                tx.deadRows[3] = -1;
                policy.onPartitionUpdated(tx, geometry, 4, 1);
                Assert.assertEquals(2, policy.selectPartition(tx, geometry, 1, NOW, 0));
                tx.deadRows[2] = -1;
                policy.onPartitionUpdated(tx, geometry, 3, 1);
                Assert.assertEquals(-1, policy.selectPartition(tx, geometry, 1, NOW, 0));
                Assert.assertEquals(PartitionCompactionPolicy.REASON_NONE, policy.getSelectedReason());
                // The empty band retained the latch; a new eligible geometry enters it next commit.
                tx.deadRows[1] = 61;
                policy.onPartitionUpdated(tx, geometry, 2, 1);
                Assert.assertEquals(1, policy.selectPartition(tx, geometry, 1, NOW, 0));
                Assert.assertEquals(PartitionCompactionPolicy.REASON_TABLE_PRESSURE, policy.getSelectedReason());
            }
        });
    }

    @Test
    public void testPressureFloorAndRatioAreStrictAndRefreshPriorities() throws Exception {
        assertMemoryLeak(() -> {
            final TestConfiguration cfg = new TestConfiguration();
            cfg.deadMinSize = 90;
            try (TestTxWriter tx = new TestTxWriter(cfg); TestGeometry geometry = new TestGeometry(tx)) {
                final PartitionCompactionPolicy policy = new PartitionCompactionPolicy(cfg);
                Assert.assertEquals(-1, policy.selectPartition(tx, geometry, 1, NOW, 0));
                cfg.deadMinSize = 60;
                Assert.assertEquals(3, policy.selectPartition(tx, geometry, 1, NOW, 0));
                tx.deadRows[3] = -1;
                policy.onPartitionUpdated(tx, geometry, 4, 1);
                Assert.assertEquals(-1, policy.selectPartition(tx, geometry, 1, NOW, 0));
                cfg.deadMinSize = 0;
                cfg.pressureRatio = 0.6;
                Assert.assertEquals(-1, policy.selectPartition(tx, geometry, 1, NOW, 0));
                cfg.pressureRatio = 0.5;
                Assert.assertEquals(2, policy.selectPartition(tx, geometry, 1, NOW, 0));
            }
        });
    }

    @Test
    public void testPressureBackoffKeepsOtherWasteCandidatesAvailable() throws Exception {
        assertMemoryLeak(() -> {
            final TestConfiguration cfg = new TestConfiguration();
            try (TestTxWriter tx = new TestTxWriter(cfg); TestGeometry geometry = new TestGeometry(tx)) {
                final PartitionCompactionPolicy policy = new PartitionCompactionPolicy(cfg);
                Assert.assertEquals(3, policy.selectPartition(tx, geometry, 1, NOW, 0));
                policy.onDeclined(4, NOW);
                Assert.assertEquals(2, policy.selectPartition(tx, geometry, 1, NOW, 0));
                Assert.assertEquals(3, policy.selectPartition(tx, geometry, 1, NOW + Micros.MINUTE_MICROS, 0));
            }
        });
    }

    @Test
    public void testWasteAndPieceRulesRetainTheirOwnTiers() throws Exception {
        assertMemoryLeak(() -> {
            final TestConfiguration cfg = new TestConfiguration();
            try (TestTxWriter tx = new TestTxWriter(cfg); TestGeometry geometry = new TestGeometry(tx)) {
                tx.deadRows[0] = 120;
                final PartitionCompactionPolicy policy = new PartitionCompactionPolicy(cfg);
                Assert.assertEquals(0, policy.selectPartition(tx, geometry, 1, NOW, 0));
                Assert.assertEquals(PartitionCompactionPolicy.REASON_WASTE_RATIO, policy.getSelectedReason());
                tx.deadRows[0] = 9;
                geometry.pieceCount = 21;
                policy.onPartitionUpdated(tx, geometry, 1, 1);
                Assert.assertEquals(3, policy.selectPartition(tx, geometry, 1, NOW, 0));
                Assert.assertEquals(PartitionCompactionPolicy.REASON_TABLE_PRESSURE, policy.getSelectedReason());
                cfg.hasPressureTrigger = false;
                policy.clear();
                Assert.assertEquals(0, policy.selectPartition(tx, geometry, 1, NOW, 0));
                Assert.assertEquals(PartitionCompactionPolicy.REASON_PIECE_COUNT, policy.getSelectedReason());
            }
        });
    }

    @Test
    public void testPressureStillReportsHotPartitions() throws Exception {
        assertMemoryLeak(() -> {
            final TestConfiguration cfg = new TestConfiguration();
            cfg.hotCommits = 10;
            try (TestTxWriter tx = new TestTxWriter(cfg); TestGeometry geometry = new TestGeometry(tx)) {
                geometry.writerTxn = 100;
                final PartitionCompactionPolicy policy = new PartitionCompactionPolicy(cfg);
                Assert.assertEquals(3, policy.selectPartition(tx, geometry, 1, NOW, 0));
                Assert.assertTrue(policy.isSelectedPartitionHot());
            }
        });
    }

    @Test
    public void testDueMoveTailNeedsOnlyAMajorityColdPrefix() throws Exception {
        assertMemoryLeak(() -> {
            final TestConfiguration cfg = new TestConfiguration();
            cfg.hotCommits = 10;
            try (TestTxWriter tx = new TestTxWriter(cfg); TestGeometry geometry = new TestGeometry(tx)) {
                geometry.pieceCount = 3;
                Assert.assertFalse(O3CompositeMergeStrategy.isMoveTailTriggered(100, 9, 3, 50));
                // 60 cold rows, 40 hot rows: beneficial, but below the old two-to-one economics guard.
                Assert.assertEquals(1, PartitionCompactionPolicy.moveTailCut(cfg, tx, geometry, 0, 1, Long.MAX_VALUE));
                Assert.assertEquals(0, PartitionCompactionPolicy.moveTailCut(cfg, tx, geometry, 0, 1, 100));
                cfg.splitMinSize = 100;
                Assert.assertEquals(0, PartitionCompactionPolicy.moveTailCut(cfg, tx, geometry, 0, 1, Long.MAX_VALUE));
                cfg.splitMinSize = 50;
                cfg.maxSplits = 4;
                Assert.assertEquals(0, PartitionCompactionPolicy.moveTailCut(cfg, tx, geometry, 0, 1, Long.MAX_VALUE));
            }
        });
    }

    private static class TestConfiguration extends DefaultCairoConfiguration {
        private long deadMinSize;
        private boolean hasPressureTrigger = true;
        private int hotCommits;
        private int maxSplits = 20;
        private double pressureRatio = 0.5;
        private long splitMinSize = 50;

        private TestConfiguration() {
            super(root);
        }

        @Override
        public int getO3PartitionMaxSplits() {
            return maxSplits;
        }

        @Override
        public long getPartitionCompactionDeadMinSize() {
            return deadMinSize;
        }

        @Override
        public int getPartitionCompactionHotCommits() {
            return hotCommits;
        }

        @Override
        public long getPartitionCompactionIdleTimeout() {
            return 1;
        }

        @Override
        public long getPartitionCompactionTableDeadThreshold() {
            return hasPressureTrigger ? 1 : Long.MAX_VALUE;
        }

        @Override
        public long getPartitionCompactionTableDeadTrigger() {
            return hasPressureTrigger ? 1 : Long.MAX_VALUE;
        }

        @Override
        public double getPartitionCompactionTablePressureDeadRatio() {
            return pressureRatio;
        }

        @Override
        public long getPartitionO3SplitMinSize() {
            return splitMinSize;
        }
    }

    private static class TestGeometry extends PartitionGeometry {
        private final TestTxWriter tx;
        private int pieceCount = 1;
        private long writerTxn = 80;

        private TestGeometry(TestTxWriter tx) {
            this.tx = tx;
        }

        @Override
        public long getE(int partitionIndex) {
            return 100 + tx.deadRows[partitionIndex];
        }

        @Override
        public long getLastWriteMicros(int partitionIndex) {
            return (partitionIndex + 1L) * Micros.MINUTE_MICROS;
        }

        @Override
        public int getPieceCount(int partitionIndex) {
            return pieceCount;
        }

        @Override
        public long getPieceRowCount(int partitionIndex, int pieceIndex) {
            return switch (pieceIndex) {
                case 0 -> 60;
                case 1 -> 30;
                default -> 10;
            };
        }

        @Override
        public long getPieceRowOffset(int partitionIndex, int pieceIndex) {
            return switch (pieceIndex) {
                case 0 -> 5;
                case 1 -> 75;
                default -> 0;
            };
        }

        @Override
        public long getPieceTimestampHi(int partitionIndex, int pieceIndex) {
            return 100L + 100L * pieceIndex;
        }

        @Override
        public long getPieceTimestampLo(int partitionIndex, int pieceIndex) {
            return 1L + 100L * pieceIndex;
        }

        @Override
        public long getPieceWriterTxn(int partitionIndex, int pieceIndex) {
            return pieceIndex == 0 ? 80 : 100;
        }

        @Override
        public long getWriterTxn(int partitionIndex) {
            return writerTxn;
        }
    }

    private static class TestTxWriter extends TxReader {
        private final long[] deadRows = {9, 30, 60, 90};

        private TestTxWriter(TestConfiguration cfg) {
            super(FilesFacadeImpl.INSTANCE);
        }

        @Override
        public long getGeometryRef(int partitionIndex) {
            return deadRows[partitionIndex] + 1;
        }

        @Override
        public long getLogicalPartitionTimestamp(long timestamp) {
            return 1;
        }

        @Override
        public int getPartitionCount() {
            return deadRows.length;
        }

        @Override
        public int getPartitionIndex(long timestamp) {
            return (int) timestamp - 1;
        }

        @Override
        public long getPartitionSize(int partitionIndex) {
            return 100;
        }

        @Override
        public long getPartitionTimestampByIndex(int partitionIndex) {
            return partitionIndex + 1;
        }

        @Override
        public long getRowCount() {
            return 100L * deadRows.length;
        }

        @Override
        public long getTxn() {
            return 100;
        }

        @Override
        public boolean isPartitionComposite(int partitionIndex) {
            return deadRows[partitionIndex] >= 0;
        }
    }
}
