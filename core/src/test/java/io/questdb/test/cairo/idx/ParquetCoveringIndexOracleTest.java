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

package io.questdb.test.cairo.idx;

import io.questdb.PropertyKey;
import io.questdb.cairo.TableReader;
import io.questdb.cairo.idx.AbstractParquetPostingIndexReader;
import io.questdb.cairo.idx.CoveringRowCursor;
import io.questdb.cairo.idx.IndexReader;
import io.questdb.cairo.idx.PostingIndexReader;
import io.questdb.cairo.sql.RowCursor;
import io.questdb.std.LongList;
import io.questdb.std.Numbers;
import io.questdb.std.str.Path;
import io.questdb.test.AbstractCairoTest;

import java.io.File;
import org.junit.Assert;
import org.junit.Test;

/**
 * The native posting index is an exact oracle for the parquet-form one.
 * <p>
 * A partition never carries both forms at once, so the comparison builds the
 * SAME deterministic source data twice -- once sealed native, once sealed
 * parquet -- and compares the two readers key by key, direction by direction,
 * over a grid of row-id windows and cover-slot sets.
 * <p>
 * <b>The artifacts are asserted before anything is compared.</b> The project
 * ledger records a config-forwarding gap that once made a two-arm bake-off run
 * the NATIVE arm in both arms and agree with itself perfectly. Proving the two
 * partitions really are sealed differently is what stops this test passing for
 * that reason.
 */
public class ParquetCoveringIndexOracleTest extends AbstractCairoTest {

    private static final String INDEXED_PARTITION = "2024-01-01";
    private static final int ROW_COUNT = 60_000;

    @Test
    public void testTheParquetReaderMatchesTheNativeOneEverywhere() throws Exception {
        assertMemoryLeak(() -> {
            // Native arm first, under the DEFAULT format.
            node1.setProperty(PropertyKey.CAIRO_POSTING_INDEX_PARQUET_PARTITION_FORMAT, "native");
            createArm("native_arm");
            // Then the parquet arm, from an identical row sequence.
            node1.setProperty(PropertyKey.CAIRO_POSTING_INDEX_PARQUET_PARTITION_FORMAT, "parquet");
            createArm("parquet_arm");

            assertArmsAreSealedDifferently();

            try (
                    TableReader nativeReader = engine.getReader(engine.verifyTableName("native_arm"));
                    TableReader parquetReader = engine.getReader(engine.verifyTableName("parquet_arm"))
            ) {
                final int nativeCol = nativeReader.getMetadata().getColumnIndex("sym");
                final int parquetCol = parquetReader.getMetadata().getColumnIndex("sym");

                final IndexReader nativeFwd = nativeReader.getIndexReader(0, nativeCol, IndexReader.DIR_FORWARD);
                final IndexReader parquetFwd = parquetReader.getIndexReader(0, parquetCol, IndexReader.DIR_FORWARD);
                Assert.assertTrue(
                        "the parquet arm must actually dispatch to the parquet reader",
                        parquetFwd instanceof AbstractParquetPostingIndexReader
                );
                Assert.assertFalse(
                        "the native arm must NOT dispatch to the parquet reader",
                        nativeFwd instanceof AbstractParquetPostingIndexReader
                );

                final int keyCount = nativeFwd.getKeyCount();
                Assert.assertTrue("the fixture must have keys", keyCount > 1);

                final long half = ROW_COUNT / 2;
                final long[][] windows = {
                        {0, Long.MAX_VALUE},
                        {0, 0},
                        {0, half},
                        {half, ROW_COUNT},
                        {ROW_COUNT, Long.MAX_VALUE},
                        {half, half - 1}, // empty
                };
                final int[][] coverSets = {null, new int[]{0}, new int[]{0, 1}};

                for (int key = 0; key < keyCount; key++) {
                    for (long[] w : windows) {
                        for (int[] covers : coverSets) {
                            assertSameSequence(
                                    nativeReader, parquetReader, nativeCol, parquetCol,
                                    key, w[0], w[1], covers, IndexReader.DIR_FORWARD
                            );
                            assertSameSequence(
                                    nativeReader, parquetReader, nativeCol, parquetCol,
                                    key, w[0], w[1], covers, IndexReader.DIR_BACKWARD
                            );
                        }
                        assertSamePrimitives(
                                (PostingIndexReader) nativeReader.getIndexReader(0, nativeCol, IndexReader.DIR_FORWARD),
                                (PostingIndexReader) parquetReader.getIndexReader(0, parquetCol, IndexReader.DIR_FORWARD),
                                key, w[0], w[1]
                        );
                    }
                }
            }
        });
    }

    /**
     * Both partitions convert to parquet; only the seal format differs, so the
     * on-disk index form is the only thing that can distinguish them.
     */
    private void assertArmsAreSealedDifferently() {
        try (Path path = new Path()) {
            assertArmArtifacts(path, "native_arm", true);
            assertArmArtifacts(path, "parquet_arm", false);
        }
    }

    private void assertArmArtifacts(Path path, String table, boolean expectNative) {
        path.of(configuration.getDbRoot()).concat(engine.verifyTableName(table));
        final File tableDir = new File(path.toString());
        // The partition directory carries a name-txn suffix (2024-01-01.4), so
        // it is matched by prefix rather than named outright.
        final File[] partitions = tableDir.listFiles(
                (d, name) -> name.startsWith(INDEXED_PARTITION) && new File(d, name).isDirectory()
        );
        Assert.assertNotNull("no table directory at " + path, partitions);
        Assert.assertEquals(
                "expected exactly one " + INDEXED_PARTITION + "* directory under " + path,
                1, partitions.length
        );
        final String[] names = partitions[0].list();
        Assert.assertNotNull("no partition directory at " + partitions[0], names);
        boolean sawPidxParquet = false;
        boolean sawPidxIm = false;
        boolean sawPv = false;
        for (String name : names) {
            if (name.startsWith("sym.pidx.") && name.endsWith(".parquet")) {
                sawPidxParquet = true;
            } else if (name.startsWith("sym.pidx.") && name.endsWith("._im")) {
                sawPidxIm = true;
            } else if (name.startsWith("sym.pv.")) {
                sawPv = true;
            }
        }
        if (expectNative) {
            Assert.assertTrue(table + " must keep its native posting chain", sawPv);
            Assert.assertFalse(table + " must not carry a pidx parquet", sawPidxParquet);
            Assert.assertFalse(table + " must not carry a pidx _im", sawPidxIm);
        } else {
            Assert.assertTrue(table + " must carry a pidx parquet", sawPidxParquet);
            Assert.assertTrue(table + " must carry a pidx _im", sawPidxIm);
            // Deliberately NOT asserting the absence of sym.pv.*. The seal
            // discards the native chain LOGICALLY -- it leaves no visible
            // generation, which is why a native reader over this partition
            // answers "no keys, no rows" -- but the files themselves linger
            // until PostingSealPurgeJob reclaims them. Requiring their absence
            // here would make the test depend on purge timing rather than on
            // the seal, and it fails today for exactly that reason.
        }
    }

    private void assertSamePrimitives(
            PostingIndexReader nativeReader, PostingIndexReader parquetReader,
            int key, long min, long max
    ) {
        final long nativeCount = nativeReader.countMatchesClamped(key, min, max, max);
        final long parquetCount = parquetReader.countMatchesClamped(key, min, max, max);
        if (nativeCount != Numbers.LONG_NULL && parquetCount != Numbers.LONG_NULL) {
            Assert.assertEquals(
                    "countMatchesClamped disagreed [key=" + key + ", min=" + min + ", max=" + max + ']',
                    nativeCount, parquetCount
            );
            for (long k = 0; k < Math.min(nativeCount, 8); k++) {
                final long a = nativeReader.selectKthMatch(key, min, max, max, k);
                final long b = parquetReader.selectKthMatch(key, min, max, max, k);
                if (a != Numbers.LONG_NULL && b != Numbers.LONG_NULL) {
                    Assert.assertEquals(
                            "selectKthMatch disagreed [key=" + key + ", k=" + k + ']', a, b
                    );
                }
            }
        }
    }

    private void assertSameSequence(
            TableReader nativeReader, TableReader parquetReader,
            int nativeCol, int parquetCol,
            int key, long min, long max, int[] covers, int direction
    ) {
        final LongList expected = new LongList();
        final LongList expectedCover = new LongList();
        drain(nativeReader.getIndexReader(0, nativeCol, direction), key, min, max, covers, expected, expectedCover);

        final LongList actual = new LongList();
        final LongList actualCover = new LongList();
        drain(parquetReader.getIndexReader(0, parquetCol, direction), key, min, max, covers, actual, actualCover);

        final String where = "[key=" + key + ", min=" + min + ", max=" + max
                + ", covers=" + (covers == null ? "null" : covers.length)
                + ", dir=" + direction + ']';
        Assert.assertEquals("posting count disagreed " + where, expected.size(), actual.size());
        for (int i = 0, n = expected.size(); i < n; i++) {
            Assert.assertEquals("row id disagreed at " + i + ' ' + where, expected.getQuick(i), actual.getQuick(i));
        }
        Assert.assertEquals("covered value count disagreed " + where, expectedCover.size(), actualCover.size());
        for (int i = 0, n = expectedCover.size(); i < n; i++) {
            Assert.assertEquals(
                    "covered value disagreed at " + i + ' ' + where,
                    expectedCover.getQuick(i), actualCover.getQuick(i)
            );
        }
    }

    private void createArm(String table) throws Exception {
        execute("CREATE TABLE " + table + " (" +
                "ts TIMESTAMP, sym SYMBOL, price DOUBLE, qty LONG" +
                ") TIMESTAMP(ts) PARTITION BY DAY WAL");
        // Deterministic and identical across arms: no rnd_* without a seed, and
        // the values are a function of the row so a shifted gather fails on
        // every row rather than coincidentally matching.
        execute("INSERT INTO " + table + " SELECT" +
                " dateadd('u', x::INT, '" + INDEXED_PARTITION + "T00:00:00Z'::TIMESTAMP)," +
                " 's' || (x % 7)," +
                " x::DOUBLE," +
                " x * 3" +
                " FROM long_sequence(" + ROW_COUNT + ")");
        drainWalQueue();
        execute("ALTER TABLE " + table + " CONVERT PARTITION TO PARQUET LIST '" + INDEXED_PARTITION + "'");
        drainWalQueue();
        execute("ALTER TABLE " + table + " ALTER COLUMN sym ADD INDEX TYPE POSTING INCLUDE (price, qty)");
        drainWalQueue();
        engine.releaseInactive();
    }

    private void drain(
            IndexReader reader, int key, long min, long max, int[] covers,
            LongList rowIds, LongList coveredValues
    ) {
        try (RowCursor cursor = covers == null
                ? reader.getCursor(key, min, max)
                : reader.getCursor(key, min, max, covers)) {
            while (cursor.hasNext()) {
                rowIds.add(cursor.next());
                if (covers != null && cursor instanceof CoveringRowCursor crc) {
                    for (int slot : covers) {
                        // price is a DOUBLE and qty a LONG; comparing the raw
                        // bits keeps one list and still catches a wrong column.
                        coveredValues.add(slot == 0
                                ? Double.doubleToLongBits(crc.getCoveredDouble(slot))
                                : crc.getCoveredLong(slot));
                    }
                }
            }
        }
    }
}
