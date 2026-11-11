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
import io.questdb.cairo.CairoException;
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.MicrosTimestampDriver;
import io.questdb.cairo.PartitionBy;
import io.questdb.cairo.PartitionGeometry;
import io.questdb.cairo.TableReader;
import io.questdb.cairo.TableToken;
import io.questdb.std.MemoryTag;
import io.questdb.std.str.Path;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

/**
 * {@link PartitionGeometry#addPiece} refuses a piece that does not start strictly above the previous one. Readers
 * look pieces up by {@code tsLo}, so this has to hold in production too, not only under assertions.
 */
public class PartitionGeometryAddPieceTest extends AbstractCairoTest {
    private static final String DAY = "2020-02-03";

    @Test
    public void testAscendingPiecesAreAccepted() throws Exception {
        assertMemoryLeak(() -> withCompositeDay((geometry, partitionIndex) -> {
            final long e = geometry.getE(partitionIndex);
            geometry.beginUpdate(partitionIndex);
            geometry.addPiece(10, 20, 0, 5, 1, 1);
            // Touching pieces are legal: only the start timestamps must ascend.
            geometry.addPiece(20, 30, 5, 5, 1, 1);
            geometry.addPiece(31, 40, 10, 5, 1, 1);
            geometry.commitUpdate(partitionIndex, e);

            Assert.assertEquals(3, geometry.getPieceCount(partitionIndex));
            assertPiece(geometry, partitionIndex, 0, 10, 20, 5);
            assertPiece(geometry, partitionIndex, 1, 20, 30, 5);
            assertPiece(geometry, partitionIndex, 2, 31, 40, 5);
        }));
    }

    @Test
    public void testEqualTsLoIsRejected() throws Exception {
        assertRejected(10);
    }

    @Test
    public void testLowerTsLoIsRejected() throws Exception {
        assertRejected(9);
    }

    private static void assertPiece(PartitionGeometry geometry, int partitionIndex, int ordinal, long tsLo, long tsHi, long rowCount) {
        Assert.assertEquals(tsLo, geometry.getPieceTimestampLo(partitionIndex, ordinal));
        Assert.assertEquals(tsHi, geometry.getPieceTimestampHi(partitionIndex, ordinal));
        Assert.assertEquals(rowCount, geometry.getPieceRowCount(partitionIndex, ordinal));
    }

    private static void assertRejected(long secondTsLo) throws Exception {
        assertMemoryLeak(() -> withCompositeDay((geometry, partitionIndex) -> {
            final int pieceCount = geometry.getPieceCount(partitionIndex);
            final long firstTsLo = geometry.getPieceTimestampLo(partitionIndex, 0);
            final long e = geometry.getE(partitionIndex);

            geometry.beginUpdate(partitionIndex);
            geometry.addPiece(10, 20, 0, 5, 1, 1);
            try {
                geometry.addPiece(secondTsLo, 30, 5, 5, 1, 1);
                Assert.fail("a piece starting at " + secondTsLo + " after one starting at 10 was accepted");
            } catch (CairoException ex) {
                Assert.assertTrue(ex.isCritical());
                TestUtils.assertContains(ex.getFlyweightMessage(),
                        "pieces must ascend by tsLo [partitionIndex=" + partitionIndex + ", tsLo=" + secondTsLo + ", prevTsLo=10]");
            }

            // The half-built list is dropped, so nothing can commit it.
            AssertionError commitError = null;
            try {
                geometry.commitUpdate(partitionIndex, e);
            } catch (AssertionError ex) {
                commitError = ex;
            }
            Assert.assertNotNull("the rejected update could still be committed", commitError);
            TestUtils.assertContains(commitError.getMessage(), "commitUpdate for a record that beginUpdate did not open");
            Assert.assertEquals(pieceCount, geometry.getPieceCount(partitionIndex));
            Assert.assertEquals(firstTsLo, geometry.getPieceTimestampLo(partitionIndex, 0));
        }));
    }

    // A WAL day that an out-of-order backfill has cut into pieces, so the geometry resolves real pieces for it.
    private static void withCompositeDay(GeometryCode code) throws Exception {
        node1.setProperty(PropertyKey.CAIRO_O3_PARTITION_MERGE_APPEND_ENABLED, "true");
        node1.setProperty(PropertyKey.CAIRO_O3_PARTITION_SPLIT_MIN_SIZE, "1K");
        node1.setProperty(PropertyKey.CAIRO_PARTITION_COMPACTION_AVG_ROWS_PIECE_LIM, 8);

        execute("CREATE TABLE x (i INT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
        // A later day, so DAY is not the active partition and the backfill below goes through the O3 path.
        execute("INSERT INTO x VALUES (0, '2020-02-06T00:00:00.000000Z')");
        drainWalQueue();
        execute("INSERT INTO x SELECT x::INT, timestamp_sequence('" + DAY + "', 15 * 1_000_000L) ts FROM long_sequence(5_760)");
        drainWalQueue();
        execute("INSERT INTO x SELECT x::INT + 70_000, timestamp_sequence('" + DAY + "T00:05:07', 5 * 1_000_000L) ts FROM long_sequence(20)");
        drainWalQueue();

        final TableToken tt = engine.verifyTableName("x");
        try (
                TableReader reader = engine.getReader(tt);
                Path root = new Path()
        ) {
            final int partitionIndex = reader.getTxFile().getPartitionIndex(MicrosTimestampDriver.floor(DAY + "T00:00:00.000000Z"));
            Assert.assertTrue("the backfill did not make the day composite", reader.getTxFile().isPartitionComposite(partitionIndex));
            root.of(configuration.getDbRoot()).concat(tt.getDirName());
            try (PartitionGeometry geometry = new PartitionGeometry().of(
                    configuration.getFilesFacade(),
                    reader.getTxFile(),
                    root.toString(),
                    ColumnType.TIMESTAMP,
                    PartitionBy.DAY,
                    MemoryTag.NATIVE_TABLE_READER
            )) {
                code.run(geometry, partitionIndex);
            }
        }
    }

    @FunctionalInterface
    private interface GeometryCode {
        void run(PartitionGeometry geometry, int partitionIndex) throws Exception;
    }
}
