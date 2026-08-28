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
import io.questdb.cairo.sql.RowCursor;
import io.questdb.std.LongList;
import io.questdb.std.Rnd;
import io.questdb.std.str.Path;
import io.questdb.test.AbstractCairoTest;

import java.io.File;
import org.junit.Assert;
import org.junit.Test;

/**
 * {@link ParquetCoveringIndexOracleTest} compares the two readers over a
 * FIXED grid of keys, directions, windows and cover sets. That grid is
 * exactly the cases someone thought of -- it says nothing about the cases
 * nobody did. This is the same two-arm fixture driven by a seeded {@link Rnd}
 * instead: the row count and every query parameter (key, direction, window,
 * cover set) are drawn from one sequence, so a run that fails is reproducible
 * from its printed seed alone.
 * <p>
 * <b>Both arms are built from the SAME draw of the row count.</b> Drawing it
 * twice -- once per arm -- would size the two tables differently and turn
 * every subsequent comparison into a comparison between two different
 * datasets, which proves nothing about the reader. The row count is drawn
 * once, before the format property is flipped, and handed to both arms.
 */
public class ParquetCoveringIndexFuzzTest extends AbstractCairoTest {

    private static final String INDEXED_PARTITION = "2024-01-01";
    private static final int DRAWS = 200;
    /**
     * Distinct symbol values the fixture's data cycles through. Fixed rather
     * than randomised: the fuzzing surface here is the QUERY (key, window,
     * direction, covers), not the shape of the underlying table.
     */
    // Above WHOLE_GROUP_KEY_THRESHOLD (8), so the readers' whole-group decode
    // and cache-hit paths are reachable. At 7 they never were.
    private static final int SYM_CARDINALITY = 16;
    /**
     * A key's run longer than {@code ParquetIndexSeal.TARGET_ROW_GROUP_ROWS}
     * (100k) is split across index row groups. At the default cardinality no
     * run gets near that, so a fixture sized only for key diversity leaves the
     * whole multi-group machinery -- zone-map pruning, the group-skip
     * arithmetic in {@code selectKthMatch}, and the backward cross-group walk
     * -- outside the only independent-implementation comparison on the branch.
     * This cardinality with {@link #WIDE_RUN_ROWS} puts roughly 125k rows on
     * each key, so every key's run spans more than one group.
     */
    private static final int WIDE_RUN_CARDINALITY = 2;
    private static final long WIDE_RUN_ROWS = 250_000;

    @Test
    public void testRandomisedReadsAgreeWithTheNativeReader() throws Exception {
        final long seed = System.nanoTime();
        try {
            assertMemoryLeak(() -> {
                final Rnd rnd = new Rnd(seed, seed);
                // Two regimes, not one. The narrow fixture keeps key diversity;
                // the wide one puts a single key's run across several index row
                // groups, which is the only way the group-skip and cross-group
                // paths get compared against the native reader at all.
                fuzzOneFixture(rnd, rnd.nextInt(40_000) + 20_000, SYM_CARDINALITY, "narrow", false);
                final int wideGroups = fuzzOneFixture(rnd, WIDE_RUN_ROWS, WIDE_RUN_CARDINALITY, "wide", false);
                // Both regimes again under the packed payload arm. It is a
                // different on-disk encoding of the same postings reached by a
                // different reader path, so it earns its own draws rather than
                // riding on the per-posting arm's.
                fuzzOneFixture(rnd, rnd.nextInt(40_000) + 20_000, SYM_CARDINALITY, "narrow_packed", true);
                fuzzOneFixture(rnd, WIDE_RUN_ROWS, WIDE_RUN_CARDINALITY, "wide_packed", true);
                // Proof, not assumption, that the wide fixture crosses a group
                // boundary. An earlier version compared _im SIZES, which does
                // not establish this: the _im grows with the DATA row group
                // count too, so the wide fixture's bigger sidecar could come
                // entirely from having more rows. Ask for the index row group
                // count directly.
                Assert.assertTrue(
                        "the wide fixture must span more than one INDEX row group, or the"
                                + " group-skip and cross-group paths are still untested"
                                + " [indexRowGroups=" + wideGroups + ']',
                        wideGroups > 1
                );
            });
        } catch (Throwable t) {
            throw new AssertionError("fuzz seed=" + seed, t);
        }
    }

    /**
     * @param packed seal the parquet arm with the packed payload. Both arms are
     *               then sealed WITHOUT covered columns, because the seal
     *               declines the packed arm for a covering index, and the draws
     *               below ask for no covered value.
     */
    private int fuzzOneFixture(Rnd rnd, long rowCount, int cardinality, String label, boolean packed) throws Exception {
        final String nativeArm = "native_arm_" + label;
        final String parquetArm = "parquet_arm_" + label;
        node1.setProperty(PropertyKey.CAIRO_POSTING_INDEX_PARQUET_PACKED_PAYLOAD, packed);
        node1.setProperty(PropertyKey.CAIRO_POSTING_INDEX_PARQUET_PARTITION_FORMAT, "native");
        createArm(nativeArm, rowCount, cardinality, !packed);
        node1.setProperty(PropertyKey.CAIRO_POSTING_INDEX_PARQUET_PARTITION_FORMAT, "parquet");
        createArm(parquetArm, rowCount, cardinality, !packed);

        assertArmsAreSealedDifferently(nativeArm, parquetArm);

        try (
                TableReader nativeReader = engine.getReader(engine.verifyTableName(nativeArm));
                TableReader parquetReader = engine.getReader(engine.verifyTableName(parquetArm))
        ) {
                    Assert.assertEquals(
                            "both arms must hold the same row count or the comparison is between different data",
                            nativeReader.size(), parquetReader.size()
                    );

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
                    Assert.assertEquals(
                            "the parquet arm's payload does not match what this fixture asked for,"
                                    + " so the draws below exercise the wrong arm [label=" + label + ']',
                            packed,
                            ((AbstractParquetPostingIndexReader) parquetFwd).isPackedPayload()
                    );

                    final int keyCount = nativeFwd.getKeyCount();
                    Assert.assertTrue("the fixture must have keys", keyCount > 1);

                    for (int draw = 0; draw < DRAWS; draw++) {
                        final int key = rnd.nextInt(keyCount);
                        final int direction = rnd.nextBoolean()
                                ? IndexReader.DIR_FORWARD : IndexReader.DIR_BACKWARD;
                        final long lo = rnd.nextLong(rowCount);
                        final long hi = lo + rnd.nextLong(rowCount - lo + 1);
                        // The packed arm covers nothing, so there is no cover
                        // set to draw from -- asking for one would be asking for
                        // a slot the index does not have.
                        final int[] covers = packed ? null : switch (rnd.nextInt(3)) {
                            case 0 -> null;
                            case 1 -> new int[]{0};
                            default -> new int[]{0, 1};
                        };
                        // Same comparison ParquetCoveringIndexOracleTest makes:
                        // drain both readers and assert an identical row-id
                        // sequence and identical covered values.
                assertSameSequence(nativeReader, parquetReader, nativeCol, parquetCol,
                        key, lo, hi, covers, direction);
            }
            return ((AbstractParquetPostingIndexReader) parquetFwd).getIndexRowGroupCount();
        }
    }

    private void createArm(String table, long rowCount, int cardinality, boolean covered) throws Exception {
        execute("CREATE TABLE " + table + " (" +
                "ts TIMESTAMP, sym SYMBOL, price DOUBLE, qty LONG" +
                ") TIMESTAMP(ts) PARTITION BY DAY WAL");
        // Deterministic and identical across arms: no rnd_* without a seed, and
        // the values are a function of the row so a shifted gather fails on
        // every row rather than coincidentally matching.
        execute("INSERT INTO " + table + " SELECT" +
                " dateadd('u', x::INT, '" + INDEXED_PARTITION + "T00:00:00Z'::TIMESTAMP)," +
                " 's' || (x % " + cardinality + ")," +
                " x::DOUBLE," +
                " x * 3" +
                " FROM long_sequence(" + rowCount + ")");
        drainWalQueue();
        execute("ALTER TABLE " + table + " CONVERT PARTITION TO PARQUET LIST '" + INDEXED_PARTITION + "'");
        drainWalQueue();
        execute("ALTER TABLE " + table + " ALTER COLUMN sym ADD INDEX TYPE POSTING"
                + (covered ? " INCLUDE (price, qty)" : ""));
        drainWalQueue();
        engine.releaseInactive();
    }

    /**
     * Both partitions convert to parquet; only the seal format differs, so the
     * on-disk index form is the only thing that can distinguish them. Mirrors
     * {@code ParquetCoveringIndexOracleTest#assertArmsAreSealedDifferently} --
     * the project ledger records a config-forwarding gap that once made a
     * two-arm bake-off run the NATIVE arm in both arms and agree with itself
     * perfectly, so this is asserted before anything else is compared.
     */
    private void assertArmsAreSealedDifferently(String nativeArm, String parquetArm) {
        try (Path path = new Path()) {
            assertArmArtifacts(path, nativeArm, true);
            assertArmArtifacts(path, parquetArm, false);
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
