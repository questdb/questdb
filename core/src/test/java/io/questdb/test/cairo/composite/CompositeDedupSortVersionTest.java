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

package io.questdb.test.cairo.composite;

import io.questdb.PropertyKey;
import io.questdb.cairo.MicrosTimestampDriver;
import io.questdb.cairo.PartitionGeometry;
import io.questdb.cairo.PartitionGeometryFile;
import io.questdb.cairo.TableReader;
import io.questdb.cairo.TableToken;
import io.questdb.cairo.TableWriter;
import io.questdb.cairo.vm.api.MemoryCR;
import io.questdb.test.AbstractCairoTest;
import org.junit.Assert;
import org.junit.Test;

/**
 * A dedup commit hands its O3 rows to the partition sorted by the dedup keys within each timestamp, and a
 * composite piece that holds only such rows claims that order with the table's dedup version. A merge writes its
 * piece afresh in key order and claims it too, a kept piece keeps its own claim, and changing a key's type makes
 * every claim stale.
 */
public class CompositeDedupSortVersionTest extends AbstractCairoTest {
    private static final String DAY = "2020-02-03";
    private static final long UNSET = PartitionGeometryFile.DEDUP_SORT_VERSION_UNSET;

    @Test
    public void testDedupKeyTypeChangeBumpsDedupVersion() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE x (k LONG, sym SYMBOL, v INT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL DEDUP UPSERT KEYS(ts, k, sym)");
            final TableToken token = engine.verifyTableName("x");
            final long created = dedupVersion(token);
            // A key column changes type: its rows compare in a new order, so the version moves.
            execute("ALTER TABLE x ALTER COLUMN k TYPE INT");
            drainWalQueue();
            final long afterKeyChange = dedupVersion(token);
            Assert.assertTrue(afterKeyChange > created);
            // A non-key column changing type leaves it alone.
            execute("ALTER TABLE x ALTER COLUMN v TYPE LONG");
            drainWalQueue();
            Assert.assertEquals(afterKeyChange, dedupVersion(token));
            try (TableWriter writer = getWriter(token)) {
                Assert.assertEquals(afterKeyChange, writer.getDedupVersion());
            }
        });
    }

    @Test
    public void testDedupCommitStampsThePiecesItWrites() throws Exception {
        node1.setProperty(PropertyKey.CAIRO_O3_PARTITION_MERGE_APPEND_ENABLED, "true");
        node1.setProperty(PropertyKey.CAIRO_O3_PARTITION_SPLIT_MIN_SIZE, "1K");
        node1.setProperty(PropertyKey.CAIRO_PARTITION_COMPACTION_AVG_ROWS_PIECE_LIM, 8);
        assertMemoryLeak(() -> {
            execute("CREATE TABLE x (k LONG, sym SYMBOL, v INT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL DEDUP UPSERT KEYS(ts, k, sym)");
            final TableToken token = engine.verifyTableName("x");
            // A later day, so DAY is not the active partition and every write to it goes through the O3 path.
            execute("INSERT INTO x VALUES (0, 'a', 0, '2020-02-06')");
            drainWalQueue();
            // Two blocks of unique timestamps four hours apart. The first founds the partition, the second is
            // appended above its last timestamp: the day stays plain, with the dedup sorted bit in _txn.
            execute("INSERT INTO x SELECT 1000 - x, 's' || (x % 3), x::INT, timestamp_sequence('" + DAY + "', 15_000_000L) FROM long_sequence(240)");
            drainWalQueue();
            execute("INSERT INTO x SELECT 1000 - x, 's' || (x % 3), x::INT, timestamp_sequence('" + DAY + "T10:00', 15_000_000L) FROM long_sequence(240)");
            drainWalQueue();
            final long dedupVersion = dedupVersion(token);
            final long dayTs = MicrosTimestampDriver.floor(DAY + "T00:00:00.000000Z");
            try (TableReader reader = engine.getReader(token)) {
                final int partitionIndex = reader.getTxFile().getPartitionIndex(dayTs);
                Assert.assertFalse(reader.getTxFile().isPartitionComposite(partitionIndex));
                Assert.assertTrue(reader.getTxFile().isPartitionDedupSorted(partitionIndex));
            }

            // Into the gap: four timestamps of five rows each, keys in descending insertion order. The day turns
            // composite: the pieces cut from its rows take the claim the bit stood for, the new piece its own.
            execute("INSERT INTO x SELECT 20 - x, 's' || (x % 3), (x + 10_000)::INT, dateadd('s', ((x - 1) / 5)::INT, '" + DAY + "T05:00'::TIMESTAMP) FROM long_sequence(20)");
            drainWalQueue();
            final long gapTs = MicrosTimestampDriver.floor(DAY + "T05:00:00.000000Z");
            try (TableReader reader = engine.getReader(token)) {
                final int partitionIndex = reader.getTxFile().getPartitionIndex(dayTs);
                Assert.assertTrue(reader.getTxFile().isPartitionComposite(partitionIndex));
                // The bit is for plain partitions: a composite one carries the claim per piece.
                Assert.assertFalse(reader.getTxFile().isPartitionDedupSorted(partitionIndex));
                final PartitionGeometry geometry = reader.getGeometry();
                final int gapPiece = geometry.findPiece(partitionIndex, gapTs);
                Assert.assertEquals(gapTs, geometry.getPieceTimestampLo(partitionIndex, gapPiece));
                Assert.assertEquals(20, geometry.getPieceRowCount(partitionIndex, gapPiece));
                Assert.assertTrue(geometry.getPieceCount(partitionIndex) > 1);
                for (int p = 0, n = geometry.getPieceCount(partitionIndex); p < n; p++) {
                    Assert.assertEquals(dedupVersion, geometry.getPieceDedupSortVersion(partitionIndex, p));
                    assertPieceSorted(reader, partitionIndex, p);
                }
                Assert.assertTrue(countEqualTimestampRows(reader, partitionIndex, gapPiece) > 0);
            }

            // A commit that merges into the piece rewrites it: new keys before, between and after the stored
            // ones of a timestamp land in key order, and the piece keeps its claim.
            execute("INSERT INTO x VALUES (100, 's9', 1, '" + DAY + "T05:00:01'), (-5, 's9', 2, '" + DAY + "T05:00:01'), (12, 's9', 3, '" + DAY + "T05:00:01')");
            drainWalQueue();
            try (TableReader reader = engine.getReader(token)) {
                final int partitionIndex = reader.getTxFile().getPartitionIndex(dayTs);
                final PartitionGeometry geometry = reader.getGeometry();
                final int gapPiece = geometry.findPiece(partitionIndex, gapTs);
                Assert.assertEquals(23, geometry.getPieceRowCount(partitionIndex, gapPiece));
                Assert.assertEquals(dedupVersion, geometry.getPieceDedupSortVersion(partitionIndex, gapPiece));
                assertPieceSorted(reader, partitionIndex, gapPiece);
            }
            assertQuery("SELECT k FROM x WHERE ts = '" + DAY + "T05:00:01'").noLeakCheck().inferTimestamp().inferRandomAccess().sizeMayVary().returns("""
                    k
                    -5
                    10
                    11
                    12
                    12
                    13
                    14
                    100
                    """);

            // A key type change: the claims name the old version and are stale.
            execute("ALTER TABLE x ALTER COLUMN sym TYPE VARCHAR");
            drainWalQueue();
            Assert.assertTrue(dedupVersion(token) > dedupVersion);
            try (TableReader reader = engine.getReader(token)) {
                final int partitionIndex = reader.getTxFile().getPartitionIndex(dayTs);
                final PartitionGeometry geometry = reader.getGeometry();
                final int gapPiece = geometry.findPiece(partitionIndex, gapTs);
                Assert.assertEquals(dedupVersion, geometry.getPieceDedupSortVersion(partitionIndex, gapPiece));
                Assert.assertNotEquals(reader.getMetadata().getDedupVersion(), geometry.getPieceDedupSortVersion(partitionIndex, gapPiece));
            }
        });
    }

    @Test
    public void testMergePutsUnclaimedPieceInKeyOrder() throws Exception {
        node1.setProperty(PropertyKey.CAIRO_O3_PARTITION_MERGE_APPEND_ENABLED, "true");
        node1.setProperty(PropertyKey.CAIRO_O3_PARTITION_SPLIT_MIN_SIZE, "1K");
        node1.setProperty(PropertyKey.CAIRO_PARTITION_COMPACTION_AVG_ROWS_PIECE_LIM, 8);
        assertMemoryLeak(() -> {
            // Written without dedup: the rows of a timestamp are in insertion order, and no piece claims anything.
            execute("CREATE TABLE x (k LONG, sym SYMBOL, v INT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
            final TableToken token = engine.verifyTableName("x");
            execute("INSERT INTO x VALUES (0, 'a', 0, '2020-02-06')");
            drainWalQueue();
            execute("INSERT INTO x SELECT 1000 - x, 's' || (x % 3), x::INT, timestamp_sequence('" + DAY + "', 15_000_000L) FROM long_sequence(240)");
            execute("INSERT INTO x SELECT 1000 - x, 's' || (x % 3), x::INT, timestamp_sequence('" + DAY + "T10:00', 15_000_000L) FROM long_sequence(240)");
            drainWalQueue();
            execute("INSERT INTO x SELECT 20 - x, 's' || (x % 3), (x + 10_000)::INT, dateadd('s', ((x - 1) / 5)::INT, '" + DAY + "T05:00'::TIMESTAMP) FROM long_sequence(20)");
            drainWalQueue();
            execute("ALTER TABLE x DEDUP ENABLE UPSERT KEYS(ts, k, sym)");
            drainWalQueue();
            final long dedupVersion = dedupVersion(token);
            final long dayTs = MicrosTimestampDriver.floor(DAY + "T00:00:00.000000Z");
            final long gapTs = MicrosTimestampDriver.floor(DAY + "T05:00:00.000000Z");
            try (TableReader reader = engine.getReader(token)) {
                final int partitionIndex = reader.getTxFile().getPartitionIndex(dayTs);
                Assert.assertTrue(reader.getTxFile().isPartitionComposite(partitionIndex));
                final PartitionGeometry geometry = reader.getGeometry();
                for (int p = 0, n = geometry.getPieceCount(partitionIndex); p < n; p++) {
                    Assert.assertEquals(UNSET, geometry.getPieceDedupSortVersion(partitionIndex, p));
                }
            }

            // One new key into the piece whose rows are out of key order: the merge writes the piece afresh, all
            // its timestamps in key order, and it claims the order. The pieces the commit kept claim nothing.
            execute("INSERT INTO x VALUES (12, 's9', 3, '" + DAY + "T05:00:01')");
            drainWalQueue();
            try (TableReader reader = engine.getReader(token)) {
                final int partitionIndex = reader.getTxFile().getPartitionIndex(dayTs);
                final PartitionGeometry geometry = reader.getGeometry();
                final int gapPiece = geometry.findPiece(partitionIndex, gapTs);
                Assert.assertEquals(21, geometry.getPieceRowCount(partitionIndex, gapPiece));
                Assert.assertEquals(dedupVersion, geometry.getPieceDedupSortVersion(partitionIndex, gapPiece));
                assertPieceSorted(reader, partitionIndex, gapPiece);
                Assert.assertTrue(countEqualTimestampRows(reader, partitionIndex, gapPiece) > 0);
                for (int p = 0, n = geometry.getPieceCount(partitionIndex); p < n; p++) {
                    if (p != gapPiece) {
                        Assert.assertEquals(UNSET, geometry.getPieceDedupSortVersion(partitionIndex, p));
                    }
                }
            }
            assertQuery("SELECT count() c, sum(v) v FROM x WHERE ts IN '" + DAY + "'").noLeakCheck().inferTimestamp().inferRandomAccess().sizeMayVary().returns(
                    "c\tv\n501\t" + (2L * (240 * 241 / 2) + 20L * 10_000 + 20 * 21 / 2 + 3) + "\n"
            );
        });
    }

    /**
     * The piece's rows, read at their physical rows of the shared column files, ascend by timestamp and, within
     * one timestamp, by {@code (k, sym)}: a signed long and the symbol's integer id, as WAL apply compares them.
     */
    private static void assertPieceSorted(TableReader reader, int partitionIndex, int piece) {
        final PartitionGeometry geometry = reader.getGeometry();
        final long rowOffset = geometry.getPieceRowOffset(partitionIndex, piece);
        final long rowCount = geometry.getPieceRowCount(partitionIndex, piece);
        Assert.assertTrue(reader.openPartition(partitionIndex) > 0);
        final int base = reader.getColumnBase(partitionIndex);
        final MemoryCR k = reader.getColumn(TableReader.getPrimaryColumnIndex(base, reader.getMetadata().getColumnIndex("k")));
        final MemoryCR sym = reader.getColumn(TableReader.getPrimaryColumnIndex(base, reader.getMetadata().getColumnIndex("sym")));
        final MemoryCR ts = reader.getColumn(TableReader.getPrimaryColumnIndex(base, reader.getMetadata().getColumnIndex("ts")));
        for (long row = rowOffset + 1; row < rowOffset + rowCount; row++) {
            final long prevTs = ts.getLong((row - 1) * Long.BYTES);
            final long thisTs = ts.getLong(row * Long.BYTES);
            Assert.assertTrue(prevTs <= thisTs);
            if (prevTs < thisTs) {
                continue;
            }
            final long prevK = k.getLong((row - 1) * Long.BYTES);
            final long thisK = k.getLong(row * Long.BYTES);
            Assert.assertTrue("piece " + piece + ", row " + row + ": k " + prevK + " before " + thisK, prevK <= thisK);
            if (prevK == thisK) {
                Assert.assertTrue(sym.getInt((row - 1) * Integer.BYTES) <= sym.getInt(row * Integer.BYTES));
            }
        }
    }

    // The number of the piece's rows that share the timestamp of the row before them.
    private static long countEqualTimestampRows(TableReader reader, int partitionIndex, int piece) {
        final PartitionGeometry geometry = reader.getGeometry();
        final long rowOffset = geometry.getPieceRowOffset(partitionIndex, piece);
        final long rowCount = geometry.getPieceRowCount(partitionIndex, piece);
        Assert.assertTrue(reader.openPartition(partitionIndex) > 0);
        final MemoryCR ts = reader.getColumn(TableReader.getPrimaryColumnIndex(
                reader.getColumnBase(partitionIndex), reader.getMetadata().getColumnIndex("ts")));
        long count = 0;
        for (long row = rowOffset + 1; row < rowOffset + rowCount; row++) {
            if (ts.getLong((row - 1) * Long.BYTES) == ts.getLong(row * Long.BYTES)) {
                count++;
            }
        }
        return count;
    }

    private static long dedupVersion(TableToken token) {
        try (TableReader reader = engine.getReader(token)) {
            return reader.getMetadata().getDedupVersion();
        }
    }
}
