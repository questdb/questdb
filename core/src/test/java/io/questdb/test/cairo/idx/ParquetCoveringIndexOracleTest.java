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
import io.questdb.cairo.TableUtils;
import io.questdb.cairo.idx.AbstractParquetPostingIndexReader;
import io.questdb.cairo.idx.CoveringRowCursor;
import io.questdb.cairo.idx.IndexReader;
import io.questdb.cairo.idx.PostingIndexReader;
import io.questdb.cairo.idx.PostingIndexUtils;
import io.questdb.cairo.sql.RowCursor;
import io.questdb.std.DirectBitSet;
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
    /**
     * The synthetic implicit-null prefix length. Deliberately not a round
     * fraction of a row group so a window inside it cannot coincide with a
     * group boundary and pass on the pruning arithmetic alone.
     */
    private static final long COLUMN_TOP = 12_345;

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
                            // TWICE, deliberately. The readers cache a decoded
                            // row group and serve a second lookup from it, but
                            // only when the covers match the cached ones. This
                            // grid varies covers on every step, so a single pass
                            // overwrites the cache every time and NEVER takes the
                            // cache-hit path -- it went in untested. The repeat
                            // asks the same question twice and demands the same
                            // answer, which is exactly what a broken cache breaks.
                            for (int rep = 0; rep < 2; rep++) {
                                assertSameSequence(
                                        nativeReader, parquetReader, nativeCol, parquetCol,
                                        key, w[0], w[1], covers, IndexReader.DIR_FORWARD
                                );
                                assertSameSequence(
                                        nativeReader, parquetReader, nativeCol, parquetCol,
                                        key, w[0], w[1], covers, IndexReader.DIR_BACKWARD
                                );
                            }
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
     * The same grid, over a reader carrying an implicit-null (columnTop) PREFIX.
     * <p>
     * Rows below {@code columnTop} carry no value, are not in the index at all,
     * and key 0 (NULL) owns them implicitly. The native reader synthesises them
     * ahead of the postings; every one of the parquet reader's four answers --
     * both cursors and both metadata primitives -- has to agree with that, and
     * with each other, or {@code count(*)} disagrees with the rows a scan
     * produces.
     * <p>
     * <b>Why the columnTop is injected rather than built by SQL.</b> No SQL
     * sequence reaches a parquet-sealed index with a non-zero top:
     * {@code CONVERT PARTITION TO PARQUET} collapses an intermediate top to 0
     * (the NULLs become real key-0 postings in the parquet), an O3 append into a
     * parquet partition does the same, and a column whose top EQUALS the
     * partition size is never sealed at all -- the seal is guarded on
     * {@code partitionSize > columnTop} and the partition dispatches to
     * {@code IndexFwdNullReader} instead. {@code TableWriter} asserts that
     * invariant outright. So the state is reachable only through the binding
     * call itself, which is what this fixture drives: both readers are re-bound
     * through their PRODUCTION entry points ({@code of} / {@code ofParquet}) with
     * the same synthetic top over the same data, and the native answer is the
     * oracle. That keeps the reader honest for a future ATTACH PARQUET or
     * restore path that hands it one, which the class would otherwise answer
     * inconsistently -- the primitives counting a prefix the cursors do not
     * emit.
     * <p>
     * The fixture's symbols are never NULL, so key 0 has no postings at all and
     * the prefix is the WHOLE of its answer -- a cursor that skips it returns
     * empty rather than short, which no row-count assertion could miss.
     * <p>
     * Covered values are deliberately not compared here: the native cursor
     * serves the prefix with its covering state unset and returns type defaults
     * ({@code NaN} / 0) rather than the covered column's real value for those
     * rows, which is a native question and not this reader's.
     */
    /**
     * A parquet partition never presents a posting reader with a column top --
     * and the whole implicit-null prefix path depends on it.
     * <p>
     * There are exactly two ways a column can carry one, and conversion closes
     * both:
     * <ul>
     * <li><b>partial data</b> ({@code 0 < top < partitionRowCount}):
     * {@code zeroColumnTopsAfterParquetRewrite} zeroes it, because the encoder
     * materialises the top region as real NULL rows. Those NULLs are ordinary
     * rows in the parquet file and the seal indexes them as key 0 like any
     * other value -- there is nothing implicit left to synthesise.</li>
     * <li><b>no data at all</b> ({@code top >= partitionRowCount}): the column
     * top survives, but the column resolves to {@code IndexFwdNullReader} on
     * BOTH arms, so no parquet posting reader is constructed.</li>
     * </ul>
     * The consequence: {@code nullPrefixCount()} is structurally always 0 in
     * the parquet form, and the prefix emission in the cursors is unreachable
     * defensive code rather than a live path.
     * <p>
     * This test is the guard on that invariant. If it starts failing -- most
     * likely because {@code zeroColumnTopsAfterParquetRewrite} stopped zeroing,
     * or an all-NULL column began sealing to a real index -- then the prefix
     * logic in the cursors has become load-bearing, and
     * {@link #testTheParquetReaderMatchesTheNativeOneOverAnImplicitNullPrefix()}
     * changes from a defensive contract into a correctness requirement. It also
     * makes the KEY_SPACE_SIZE the seal records live: the seal stores the index
     * writer's raw key count, while the native reader reports
     * {@code keyCount + 1} when a column top is present, so the two would then
     * disagree by one on an EXCLUSIVE bound.
     */
    @Test
    public void testAParquetPartitionNeverPresentsAPostingReaderWithAColumnTop() throws Exception {
        assertMemoryLeak(() -> {
            node1.setProperty(PropertyKey.CAIRO_POSTING_INDEX_PARQUET_PARTITION_FORMAT, "parquet");
            createArmWithLateColumn("partial_top", true);
            createArmWithLateColumn("absent_top", false);

            // Partial data: the column top is zeroed and a real parquet posting
            // reader takes over, so the NULLs must be reachable as key 0.
            try (TableReader reader = engine.getReader(engine.verifyTableName("partial_top"))) {
                final int col = reader.getMetadata().getColumnIndex("sym2");
                final IndexReader fwd = reader.getIndexReader(0, col, IndexReader.DIR_FORWARD);
                Assert.assertTrue(
                        "a partially-populated column must seal to the parquet form",
                        fwd instanceof AbstractParquetPostingIndexReader
                );
                Assert.assertEquals(
                        "conversion must zero a mid column top; a surviving one would make"
                                + " the cursors' implicit-null prefix load-bearing",
                        0,
                        fwd.getColumnTop()
                );
                Assert.assertEquals(
                        "the NULLs the encoder materialised must be indexed as ordinary key-0 rows",
                        ROW_COUNT,
                        countPostings(fwd, 0)
                );
            }

            // No data at all: the column top survives conversion, but nothing
            // dispatches to a posting reader, so it cannot observe one either.
            try (TableReader reader = engine.getReader(engine.verifyTableName("absent_top"))) {
                final int col = reader.getMetadata().getColumnIndex("sym2");
                final IndexReader fwd = reader.getIndexReader(0, col, IndexReader.DIR_FORWARD);
                Assert.assertFalse(
                        "an all-NULL column must not reach a parquet posting reader",
                        fwd instanceof AbstractParquetPostingIndexReader
                );
            }
        });
    }

    private long countPostings(IndexReader reader, int key) {
        final RowCursor cursor = reader.getCursor(key, 0, Long.MAX_VALUE);
        long n = 0;
        while (cursor.hasNext()) {
            cursor.next();
            n++;
        }
        return n;
    }

    /**
     * @param sameDay when {@code true} the late column gets values inside the
     *                partition being indexed, giving it a column top in
     *                {@code (0, partitionRowCount)} -- the branch conversion
     *                zeroes. When {@code false} the values land on the next
     *                day, leaving the indexed partition with no values for the
     *                column at all -- the branch whose column top survives.
     */
    private void createArmWithLateColumn(String table, boolean sameDay) throws Exception {
        execute("CREATE TABLE " + table + " (" +
                "ts TIMESTAMP, sym SYMBOL, price DOUBLE, qty LONG" +
                ") TIMESTAMP(ts) PARTITION BY DAY WAL");
        execute("INSERT INTO " + table + " SELECT" +
                " dateadd('u', x::INT, '" + INDEXED_PARTITION + "T00:00:00Z'::TIMESTAMP)," +
                " 's' || (x % 5)," +
                " x::DOUBLE," +
                " x" +
                " FROM long_sequence(" + ROW_COUNT + ")");
        drainWalQueue();
        execute("ALTER TABLE " + table + " ADD COLUMN sym2 SYMBOL");
        drainWalQueue();
        final String base = sameDay
                ? "dateadd('u', (x + " + ROW_COUNT + ")::INT, '" + INDEXED_PARTITION + "T00:00:00Z'::TIMESTAMP)"
                : "dateadd('u', x::INT, dateadd('d', 1, '" + INDEXED_PARTITION + "T00:00:00Z'::TIMESTAMP))";
        execute("INSERT INTO " + table + " SELECT" +
                " " + base + "," +
                " 's' || (x % 5)," +
                " x::DOUBLE," +
                " x," +
                " 't' || (x % 3)" +
                " FROM long_sequence(2000)");
        drainWalQueue();
        execute("ALTER TABLE " + table + " CONVERT PARTITION TO PARQUET LIST '" + INDEXED_PARTITION + "'");
        drainWalQueue();
        execute("ALTER TABLE " + table + " ALTER COLUMN sym2 ADD INDEX TYPE POSTING INCLUDE (price, qty)");
        drainWalQueue();
        engine.releaseInactive();
    }

    /**
     * NOTE: this forces a column top by rebinding a reader. A parquet partition
     * cannot actually present a posting reader with one -- see
     * {@link #testAParquetPartitionNeverPresentsAPostingReaderWithAColumnTop()},
     * which proves both routes are closed. So this pins the cursors' defensive
     * prefix contract, not a reachable production path. It stays because the
     * invariant it depends on lives in the WRITER, far from these cursors: if
     * that invariant is ever relaxed, this is what makes the prefix correct
     * rather than merely present.
     */
    @Test
    public void testTheParquetReaderMatchesTheNativeOneOverAnImplicitNullPrefix() throws Exception {
        assertMemoryLeak(() -> {
            node1.setProperty(PropertyKey.CAIRO_POSTING_INDEX_PARQUET_PARTITION_FORMAT, "native");
            createArm("native_top_arm");
            node1.setProperty(PropertyKey.CAIRO_POSTING_INDEX_PARQUET_PARTITION_FORMAT, "parquet");
            createArm("parquet_top_arm");

            try (
                    TableReader nativeReader = engine.getReader(engine.verifyTableName("native_top_arm"));
                    TableReader parquetReader = engine.getReader(engine.verifyTableName("parquet_top_arm"))
            ) {
                final int nativeCol = nativeReader.getMetadata().getColumnIndex("sym");
                final int parquetCol = parquetReader.getMetadata().getColumnIndex("sym");

                // Bound BEFORE the rebind, so the assertion below sees the form
                // the dispatch actually chose rather than one this test picked.
                final IndexReader nativeFwd = nativeReader.getIndexReader(0, nativeCol, IndexReader.DIR_FORWARD);
                final IndexReader nativeBwd = nativeReader.getIndexReader(0, nativeCol, IndexReader.DIR_BACKWARD);
                final IndexReader parquetFwd = parquetReader.getIndexReader(0, parquetCol, IndexReader.DIR_FORWARD);
                final IndexReader parquetBwd = parquetReader.getIndexReader(0, parquetCol, IndexReader.DIR_BACKWARD);
                Assert.assertTrue(
                        "the parquet arm must actually dispatch to the parquet reader",
                        parquetFwd instanceof AbstractParquetPostingIndexReader
                );
                Assert.assertFalse(
                        "the native arm must NOT dispatch to the parquet reader",
                        nativeFwd instanceof AbstractParquetPostingIndexReader
                );
                // The premise this whole fixture rests on: with the top at 0 the
                // prefix is empty and none of the code under test runs, so a
                // reader that came back already carrying one would mean the
                // injection below was measuring something else.
                Assert.assertEquals("a sealed parquet index must carry a zero column top", 0, parquetFwd.getColumnTop());
                Assert.assertEquals(0, nativeFwd.getColumnTop());

                rebindWithColumnTop(nativeReader, nativeFwd, nativeCol, COLUMN_TOP);
                rebindWithColumnTop(nativeReader, nativeBwd, nativeCol, COLUMN_TOP);
                rebindWithColumnTop(parquetReader, parquetFwd, parquetCol, COLUMN_TOP);
                rebindWithColumnTop(parquetReader, parquetBwd, parquetCol, COLUMN_TOP);
                Assert.assertEquals(COLUMN_TOP, parquetFwd.getColumnTop());
                Assert.assertEquals(COLUMN_TOP, nativeFwd.getColumnTop());

                // Windows that start below, inside, at and past the prefix, plus
                // the two that bound it exactly. A window starting at 0 alone
                // would leave the missing `- minValue` term invisible.
                final long[][] windows = {
                        {0, Long.MAX_VALUE},
                        {0, COLUMN_TOP - 1},
                        {0, COLUMN_TOP},
                        {1, Long.MAX_VALUE},
                        {COLUMN_TOP / 2, Long.MAX_VALUE},
                        {COLUMN_TOP / 2, COLUMN_TOP / 2 + 100},
                        {COLUMN_TOP - 1, COLUMN_TOP + 1},
                        {COLUMN_TOP, Long.MAX_VALUE},
                        {COLUMN_TOP + 1000, Long.MAX_VALUE},
                        {ROW_COUNT, Long.MAX_VALUE},
                };

                // Key 0 is the one the prefix belongs to; the rest are the
                // control that says the prefix was added to key 0 ONLY.
                for (int key = 0; key < 8; key++) {
                    for (long[] w : windows) {
                        assertSameCursorSequence(nativeFwd, parquetFwd, key, w[0], w[1], IndexReader.DIR_FORWARD);
                        assertSameCursorSequence(nativeBwd, parquetBwd, key, w[0], w[1], IndexReader.DIR_BACKWARD);
                        // The primitives answer the same question the forward
                        // cursor walks, so they are asserted against the same
                        // oracle and against that cursor.
                        assertSamePrefixCount(
                                (PostingIndexReader) nativeFwd, (PostingIndexReader) parquetFwd,
                                nativeFwd, parquetFwd, key, w[0], w[1], w[1]
                        );
                        // nullMaxValue is the UNCLAMPED caller max and is a
                        // separate parameter, so it has to be exercised apart
                        // from the clamped one -- including at Long.MAX_VALUE,
                        // where a nullMaxValue + 1 wraps to Long.MIN_VALUE and
                        // the count comes back hugely negative.
                        assertSamePrefixCount(
                                (PostingIndexReader) nativeFwd, (PostingIndexReader) parquetFwd,
                                nativeFwd, parquetFwd, key, w[0], Long.MAX_VALUE, w[1]
                        );
                    }
                }
            }
        });
    }

    /**
     * {@code collectDistinctKeysInRange} must report the keys that occur in the
     * range, not the keys that occur in a row group the range touches.
     * <p>
     * {@code PostingIndexDistinctRecordCursorFactory} calls the ranged form for
     * every frame that is not a whole partition, so this is what
     * {@code SELECT DISTINCT sym} under a timestamp filter reads. Over-reporting
     * is doubly wrong there: the extra symbols are returned outright, and the
     * inflated count satisfies the factory's {@code foundCount < totalExpected}
     * scan loop early, so later partitions are never visited.
     * <p>
     * The fixture's symbols cycle every seven rows, so a window narrower than a
     * cycle provably holds only some of them while the row group holding it
     * holds them all -- which is exactly the gap between row-group pruning and a
     * per-posting test.
     */
    @Test
    public void testDistinctKeysInRangeMatchTheNativeReader() throws Exception {
        assertDistinctKeysInRangeMatchTheNativeReader(false);
    }

    /**
     * The same, under the packed payload arm.
     * <p>
     * Its own test rather than a dimension of the one above because it reaches
     * DIFFERENT code: the straddling-group branch has no {@code key_id} per
     * posting to walk and no {@code row_id} column to decode, so it answers from
     * the key directory plus one widen per key instead. Nothing else in the
     * suite enters that branch -- verified by making it throw, at which point
     * the cursor and primitive tests carried on passing and only this one
     * failed.
     */
    @Test
    public void testDistinctKeysInRangeMatchTheNativeReaderWhenPacked() throws Exception {
        assertDistinctKeysInRangeMatchTheNativeReader(true);
    }

    private void assertDistinctKeysInRangeMatchTheNativeReader(boolean packed) throws Exception {
        assertMemoryLeak(() -> {
            node1.setProperty(PropertyKey.CAIRO_POSTING_INDEX_PARQUET_PACKED_PAYLOAD, packed);
            node1.setProperty(PropertyKey.CAIRO_POSTING_INDEX_PARQUET_PARTITION_FORMAT, "native");
            createArm("native_distinct_arm", !packed);
            node1.setProperty(PropertyKey.CAIRO_POSTING_INDEX_PARQUET_PARTITION_FORMAT, "parquet");
            createArm("parquet_distinct_arm", !packed);

            try (
                    TableReader nativeReader = engine.getReader(engine.verifyTableName("native_distinct_arm"));
                    TableReader parquetReader = engine.getReader(engine.verifyTableName("parquet_distinct_arm"))
            ) {
                final int nativeCol = nativeReader.getMetadata().getColumnIndex("sym");
                final int parquetCol = parquetReader.getMetadata().getColumnIndex("sym");
                final IndexReader nativeIdx = nativeReader.getIndexReader(0, nativeCol, IndexReader.DIR_FORWARD);
                final IndexReader parquetIdx = parquetReader.getIndexReader(0, parquetCol, IndexReader.DIR_FORWARD);
                Assert.assertTrue(
                        "the parquet arm must actually dispatch to the parquet reader",
                        parquetIdx instanceof AbstractParquetPostingIndexReader
                );
                Assert.assertEquals(
                        "the parquet arm's payload does not match what this fixture asked for",
                        packed,
                        ((AbstractParquetPostingIndexReader) parquetIdx).isPackedPayload()
                );

                final long[][] ranges = {
                        {0, Long.MAX_VALUE},
                        {0, 2},
                        {0, 10},
                        {100, 106},
                        {1000, 1001},
                        {30_000, 30_003},
                        {ROW_COUNT - 5, ROW_COUNT - 1},
                        {ROW_COUNT, Long.MAX_VALUE},
                };
                for (long[] r : ranges) {
                    assertSameDistinctKeys(nativeIdx, parquetIdx, r[0], r[1]);
                }
            }
        });
    }

    private void assertSameDistinctKeys(IndexReader nativeReader, IndexReader parquetReader, long rowLo, long rowHi) {
        final String where = "[rowLo=" + rowLo + ", rowHi=" + rowHi + ']';
        try (
                DirectBitSet expected = new DirectBitSet(64);
                DirectBitSet actual = new DirectBitSet(64)
        ) {
            final int expectedFound = nativeReader.collectDistinctKeysInRange(expected, rowLo, rowHi);
            final int actualFound = parquetReader.collectDistinctKeysInRange(actual, rowLo, rowHi);
            final StringBuilder expectedKeys = new StringBuilder();
            final StringBuilder actualKeys = new StringBuilder();
            for (int k = 0; k < 64; k++) {
                if (expected.get(k)) {
                    expectedKeys.append(k).append(' ');
                }
                if (actual.get(k)) {
                    actualKeys.append(k).append(' ');
                }
            }
            Assert.assertEquals(
                    "the marked key set disagreed " + where, expectedKeys.toString(), actualKeys.toString()
            );
            // The count is what the caller's scan loop terminates on, so an
            // inflated one skips partitions even when the bit set happens to
            // match.
            Assert.assertEquals("the newly-found count disagreed " + where, expectedFound, actualFound);
        }
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

    /**
     * Drains two already-bound readers over one window and compares the row-id
     * sequences. Unlike {@link #assertSameSequence} it does not re-fetch the
     * readers from their {@code TableReader}, because that would rebind them and
     * discard the injected column top.
     */
    private void assertSameCursorSequence(
            IndexReader nativeReader, IndexReader parquetReader,
            int key, long min, long max, int direction
    ) {
        final LongList expected = new LongList();
        final LongList ignored = new LongList();
        drain(nativeReader, key, min, max, null, expected, ignored);

        final LongList actual = new LongList();
        drain(parquetReader, key, min, max, null, actual, ignored);

        final String where = "[key=" + key + ", min=" + min + ", max=" + max + ", dir=" + direction + ']';
        Assert.assertEquals("posting count disagreed " + where, expected.size(), actual.size());
        for (int i = 0, n = expected.size(); i < n; i++) {
            Assert.assertEquals("row id disagreed at " + i + ' ' + where, expected.getQuick(i), actual.getQuick(i));
        }
    }

    /**
     * {@code countMatchesClamped} over a reader carrying an implicit-null
     * prefix, against the native answer AND against what the forward cursor
     * actually walks. Both comparisons are needed: agreeing with the native
     * reader alone would still admit a count the reader's own cursor
     * contradicts, and that disagreement is what the covered parallel decode
     * turns into a hard failure.
     */
    private void assertSamePrefixCount(
            PostingIndexReader nativeReader, PostingIndexReader parquetReader,
            IndexReader nativeCursors, IndexReader parquetCursors,
            int key, long min, long nullMax, long maxClamped
    ) {
        final String where = "[key=" + key + ", min=" + min + ", nullMax=" + nullMax
                + ", maxClamped=" + maxClamped + ']';
        final long expected = nativeReader.countMatchesClamped(key, min, nullMax, maxClamped);
        final long actual = parquetReader.countMatchesClamped(key, min, nullMax, maxClamped);
        Assert.assertNotEquals("-1 is consumed as a count, never as a sentinel " + where, -1, actual);
        if (expected == Numbers.LONG_NULL) {
            // The native reader bails in strictly more cases than this one, so
            // its declining says nothing about the parquet answer.
            return;
        }
        // LONG_NULL is Long.MIN_VALUE, so an overflowed count is indistinguishable
        // from "cannot answer" and degrades silently to a cursor walk instead of
        // reporting a negative total. Requiring an answer wherever the native
        // reader manages one is what makes that visible.
        Assert.assertNotEquals(
                "the parquet primitive must answer where the native one does " + where,
                Numbers.LONG_NULL, actual
        );
        Assert.assertTrue("a count can never be negative " + where + " got " + actual, actual >= 0);
        Assert.assertEquals("countMatchesClamped disagreed " + where, expected, actual);
        // selectKthMatch's contract is an ABSOLUTE row id -- the caller bounds a
        // chunk with it -- where the cursors return one relative to minValue.
        // Sampling both ends and the middle catches an off-by-minValue on every
        // window whose lower bound is not 0, which is the only kind that can
        // show it.
        for (long k : new long[]{0, 1, 2, actual / 2, actual - 2, actual - 1, actual}) {
            if (k < 0) {
                continue;
            }
            final long a = nativeReader.selectKthMatch(key, min, nullMax, maxClamped, k);
            final long b = parquetReader.selectKthMatch(key, min, nullMax, maxClamped, k);
            final String at = " [k=" + k + "] " + where;
            Assert.assertNotEquals("-1 is consumed as an absolute row id, never as a sentinel" + at, -1, b);
            if (a == Numbers.LONG_NULL) {
                continue;
            }
            Assert.assertNotEquals(
                    "the parquet primitive must answer where the native one does" + at,
                    Numbers.LONG_NULL, b
            );
            Assert.assertEquals("selectKthMatch disagreed" + at, a, b);
            Assert.assertTrue(
                    "an absolute row id must sit inside the window it was selected from" + at + " got " + b,
                    b >= min
            );
        }
        // Only when the two bounds coincide does the count describe exactly the
        // window the cursor walks; with a separate nullMax it may legitimately
        // include prefix rows past maxClamped.
        if (nullMax == maxClamped) {
            final LongList walked = new LongList();
            final LongList ignored = new LongList();
            drain(parquetCursors, key, min, maxClamped, null, walked, ignored);
            Assert.assertEquals(
                    "the count must equal what this reader's own cursor walks " + where,
                    walked.size(), actual
            );
            walked.clear();
            drain(nativeCursors, key, min, maxClamped, null, walked, ignored);
            Assert.assertEquals("premise: the native cursor must agree too " + where, walked.size(), expected);
        }
    }

    /**
     * Re-binds one index reader through the production entry point its form
     * uses -- {@code ofParquet} for the parquet form, the nine-argument
     * {@code of} for the native one -- with {@code columnTop} substituted and
     * everything else exactly what {@code TableReader.getIndexReader} would have
     * passed.
     */
    private void rebindWithColumnTop(TableReader reader, IndexReader indexReader, int columnIndex, long columnTop) {
        final long partitionTimestamp = reader.getPartitionTimestampByIndex(0);
        final long partitionTxn = reader.getTxFile().getPartitionNameTxn(0);
        final int writerIndex = reader.getMetadata().getWriterIndex(columnIndex);
        final long columnNameTxn = reader.getColumnVersionReader().getColumnNameTxn(partitionTimestamp, writerIndex);
        final CharSequence columnName = reader.getMetadata().getColumnName(columnIndex);
        final int timestampType = reader.getMetadata().getColumnType(reader.getMetadata().getTimestampIndex());
        try (Path path = new Path()) {
            path.of(configuration.getDbRoot()).concat(reader.getTableToken());
            TableUtils.setPathForNativePartition(
                    path, timestampType, reader.getPartitionedBy(), partitionTimestamp, partitionTxn
            );
            if (indexReader instanceof AbstractParquetPostingIndexReader parquet) {
                parquet.ofParquet(
                        configuration, path, columnName, columnNameTxn, partitionTxn, columnTop,
                        reader.getMetadata(), reader.getColumnVersionReader(), partitionTimestamp,
                        reader.getPartitionIndexTxn(0, columnIndex),
                        reader.getPartitionIndexImFileSize(0, columnIndex)
                );
            } else {
                indexReader.of(
                        configuration, path, columnName, columnNameTxn, partitionTxn, columnTop,
                        reader.getMetadata(), reader.getColumnVersionReader(), partitionTimestamp
                );
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
        createArm(table, true);
    }

    private void createArm(String table, boolean covered) throws Exception {
        execute("CREATE TABLE " + table + " (" +
                "ts TIMESTAMP, sym SYMBOL, price DOUBLE, qty LONG" +
                ") TIMESTAMP(ts) PARTITION BY DAY WAL");
        // Deterministic and identical across arms: no rnd_* without a seed, and
        // the values are a function of the row so a shifted gather fails on
        // every row rather than coincidentally matching.
        execute("INSERT INTO " + table + " SELECT" +
                " dateadd('u', x::INT, '" + INDEXED_PARTITION + "T00:00:00Z'::TIMESTAMP)," +
                // 16, not 7: the readers' whole-group decode and cache-hit
                // paths are gated on a row group holding at least
                // WHOLE_GROUP_KEY_THRESHOLD (8) key ids. At 7 those two paths
                // were unreachable, so the differential grid below could not
                // see them -- the backward reader's absolute-index pairing in
                // particular went in with no test entering it at all.
                " 's' || (x % 16)," +
                " x::DOUBLE," +
                " x * 3" +
                " FROM long_sequence(" + ROW_COUNT + ")");
        drainWalQueue();
        execute("ALTER TABLE " + table + " CONVERT PARTITION TO PARQUET LIST '" + INDEXED_PARTITION + "'");
        drainWalQueue();
        execute("ALTER TABLE " + table + " ALTER COLUMN sym ADD INDEX TYPE POSTING"
                + (covered ? " INCLUDE (price, qty)" : ""));
        drainWalQueue();
        engine.releaseInactive();
    }

    /**
     * The packed payload arm ({@code PAYLOAD_KIND 1}) against the same native
     * oracle, over the same grid of keys, windows and both directions.
     * <p>
     * Uncovered on both sides, because the packed arm only seals for an index
     * that covers nothing -- see the seal's fallback. That makes this a narrower
     * grid than the covered test above, and it is the whole grid that applies.
     * <p>
     * The liveness assertion is not ceremony. The property is silently ignored
     * whenever the seal declines the arm (covered columns, a compressing codec),
     * and a test that merely set it would then compare arm N against arm N and
     * pass having exercised nothing -- which is exactly how {@code latest_on}
     * was measured for weeks without touching the index at all.
     */
    @Test
    public void testThePackedPayloadReaderMatchesTheNativeOneEverywhere() throws Exception {
        assertMemoryLeak(() -> {
            node1.setProperty(PropertyKey.CAIRO_POSTING_INDEX_PARQUET_PARTITION_FORMAT, "native");
            createArm("native_arm", true);
            node1.setProperty(PropertyKey.CAIRO_POSTING_INDEX_PARQUET_PARTITION_FORMAT, "parquet");
            node1.setProperty(PropertyKey.CAIRO_POSTING_INDEX_PARQUET_PACKED_PAYLOAD, true);
            createArm("parquet_arm", true);

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
                Assert.assertTrue(
                        "the packed payload property did not take, so this compares the"
                                + " per-posting arm against itself",
                        ((AbstractParquetPostingIndexReader) parquetFwd).isPackedPayload()
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

                // The packed arm carries covered columns as one blob per row
                // group, so the covered gather is exercised here exactly as it
                // is for the per-posting arm -- it is the case a covering index
                // exists for, and the arm refused it until the blobs were added.
                final int[][] coverSets = {null, new int[]{0}, new int[]{0, 1}};
                for (int key = 0; key < keyCount; key++) {
                    for (long[] w : windows) {
                        for (int[] covers : coverSets) {
                            for (int rep = 0; rep < 2; rep++) {
                                assertSameSequence(
                                        nativeReader, parquetReader, nativeCol, parquetCol,
                                        key, w[0], w[1], covers, IndexReader.DIR_FORWARD
                                );
                                assertSameSequence(
                                        nativeReader, parquetReader, nativeCol, parquetCol,
                                        key, w[0], w[1], covers, IndexReader.DIR_BACKWARD
                                );
                            }
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
     * Every fixture above gives a key hundreds of postings, which is the shape
     * the seal answers with a linear-prediction block per key. This one gives a
     * key two, which is the shape it answers with one frame-of-reference array
     * for the whole group -- a 29-byte per-key block header costs more than the
     * two row ids it would describe.
     * <p>
     * That layout has its own addressing: group ordinals rather than
     * key-relative ones, a group-wide base and bit width rather than a block,
     * and no per-key progression to solve in closed form. None of it is reached
     * by a 16-key fixture, so without this the differential grid would go green
     * having never entered the branch.
     */
    private void createNarrowArm(String table) throws Exception {
        execute("CREATE TABLE " + table + " (" +
                "ts TIMESTAMP, sym SYMBOL, price DOUBLE, qty LONG" +
                ") TIMESTAMP(ts) PARTITION BY DAY WAL");
        // Two postings a key, against a crossover of about 3.6.
        execute("INSERT INTO " + table + " SELECT" +
                " dateadd('u', x::INT, '" + INDEXED_PARTITION + "T00:00:00Z'::TIMESTAMP)," +
                " 's' || (x % " + (ROW_COUNT / 2) + ")," +
                " x::DOUBLE," +
                " x * 3" +
                " FROM long_sequence(" + ROW_COUNT + ")");
        drainWalQueue();
        execute("ALTER TABLE " + table + " CONVERT PARTITION TO PARQUET LIST '" + INDEXED_PARTITION + "'");
        drainWalQueue();
        execute("ALTER TABLE " + table + " ALTER COLUMN sym ADD INDEX TYPE POSTING");
        drainWalQueue();
        engine.releaseInactive();
    }

    @Test
    public void testThePackedReaderMatchesTheNativeOneWhereRowIdsAreLaidOutFlat() throws Exception {
        assertMemoryLeak(() -> {
            node1.setProperty(PropertyKey.CAIRO_POSTING_INDEX_PARQUET_PARTITION_FORMAT, "native");
            createNarrowArm("native_flat_arm");
            node1.setProperty(PropertyKey.CAIRO_POSTING_INDEX_PARQUET_PARTITION_FORMAT, "parquet");
            node1.setProperty(PropertyKey.CAIRO_POSTING_INDEX_PARQUET_PACKED_PAYLOAD, true);
            createNarrowArm("parquet_flat_arm");

            try (
                    TableReader nativeReader = engine.getReader(engine.verifyTableName("native_flat_arm"));
                    TableReader parquetReader = engine.getReader(engine.verifyTableName("parquet_flat_arm"))
            ) {
                final int nativeCol = nativeReader.getMetadata().getColumnIndex("sym");
                final int parquetCol = parquetReader.getMetadata().getColumnIndex("sym");

                final IndexReader nativeFwd = nativeReader.getIndexReader(0, nativeCol, IndexReader.DIR_FORWARD);
                final IndexReader parquetFwd = parquetReader.getIndexReader(0, parquetCol, IndexReader.DIR_FORWARD);
                Assert.assertTrue(
                        "the parquet arm must actually dispatch to the parquet reader",
                        parquetFwd instanceof AbstractParquetPostingIndexReader
                );
                final AbstractParquetPostingIndexReader packed = (AbstractParquetPostingIndexReader) parquetFwd;
                Assert.assertTrue("the packed payload property did not take", packed.isPackedPayload());
                // Without this the grid below would compare the per-key path
                // against the oracle and report the flat path as covered.
                Assert.assertTrue(
                        "no row group was laid out flat, so this exercises the per-key path only",
                        packed.flatRowIdGroupCount() > 0
                );

                final int keyCount = nativeFwd.getKeyCount();
                Assert.assertTrue("the fixture must have many keys", keyCount > 1000);

                final long half = ROW_COUNT / 2;
                final long[][] windows = {
                        {0, Long.MAX_VALUE},
                        {0, 0},
                        {0, half},
                        {half, ROW_COUNT},
                        {ROW_COUNT, Long.MAX_VALUE},
                        {half, half - 1}, // empty
                };

                // Sampled, not exhaustive: 30,000 keys across the full grid is
                // minutes of runtime for no more coverage than a stride that is
                // coprime with the key count reaches. The first and last keys
                // are included explicitly -- they are the group edges, where an
                // ordinal that should be group-relative and is not shows up.
                for (int key = 0; key < keyCount; key += 331) {
                    for (long[] w : windows) {
                        assertSameSequence(
                                nativeReader, parquetReader, nativeCol, parquetCol,
                                key, w[0], w[1], null, IndexReader.DIR_FORWARD
                        );
                        assertSameSequence(
                                nativeReader, parquetReader, nativeCol, parquetCol,
                                key, w[0], w[1], null, IndexReader.DIR_BACKWARD
                        );
                        assertSamePrimitives(
                                (PostingIndexReader) nativeReader.getIndexReader(0, nativeCol, IndexReader.DIR_FORWARD),
                                (PostingIndexReader) parquetReader.getIndexReader(0, parquetCol, IndexReader.DIR_FORWARD),
                                key, w[0], w[1]
                        );
                    }
                }
                for (int key : new int[]{0, 1, keyCount - 2, keyCount - 1}) {
                    for (long[] w : windows) {
                        assertSameSequence(
                                nativeReader, parquetReader, nativeCol, parquetCol,
                                key, w[0], w[1], null, IndexReader.DIR_FORWARD
                        );
                        assertSameSequence(
                                nativeReader, parquetReader, nativeCol, parquetCol,
                                key, w[0], w[1], null, IndexReader.DIR_BACKWARD
                        );
                    }
                }
            }
        });
    }

    /**
     * Which row-id layout the seal actually picks, across the shapes the
     * benchmark ladder measures.
     * <p>
     * The seal can emit three, and picks per group by size. A layout that is
     * never picked is dead weight -- a mode byte, a decode branch and a
     * reader path that no file exercises -- and the only way to know is to
     * seal the shapes and look. Reported as a table rather than asserted on
     * counts, because the point is the distribution, not any one number; the
     * one assertion is that every group got SOME layout, which catches a
     * costing bug that leaves a group unaddressable.
     */
    @Test
    public void testWhichRowIdLayoutTheSealPicksAcrossShapes() throws Exception {
        assertMemoryLeak(() -> {
            node1.setProperty(PropertyKey.CAIRO_POSTING_INDEX_PARQUET_PARTITION_FORMAT, "parquet");
            node1.setProperty(PropertyKey.CAIRO_POSTING_INDEX_PARQUET_PACKED_PAYLOAD, true);

            // keys, and whether a key's postings arrive together (clustered)
            // or spread across the partition (round robin). Both matter: a
            // clustered key's ids are a near-progression, which is what a
            // per-key block compresses best.
            final int[] keyCounts = {16, 2_000, ROW_COUNT / 200, ROW_COUNT / 4, ROW_COUNT / 2};
            final boolean[] clustered = {false, true};

            final StringBuilder table = new StringBuilder(
                    "\n  keys      postings/key  layout   flat  perKeyTable  perKeyUniform\n");
            for (boolean cluster : clustered) {
                for (int keys : keyCounts) {
                    final String name = "layout_" + (cluster ? "c" : "r") + keys;
                    execute("CREATE TABLE " + name + " (" +
                            "ts TIMESTAMP, sym SYMBOL, price DOUBLE, qty LONG" +
                            ") TIMESTAMP(ts) PARTITION BY DAY WAL");
                    // SKEWED, not x % keys. An even split gives every key the
                    // same posting count and so the same compressed block
                    // size, which picks the uniform layout by construction --
                    // the offset-table layout exists precisely for the groups
                    // an even split cannot produce. A square distribution puts
                    // a few keys on many rows and most keys on few, which is
                    // what a real symbol column looks like.
                    final String sym = cluster
                            ? "'s' || ((x - 1) / " + Math.max(1, ROW_COUNT / keys) + ")"
                            : "'s' || ((x * x) % " + keys + ")";
                    execute("INSERT INTO " + name + " SELECT" +
                            " dateadd('u', x::INT, '" + INDEXED_PARTITION + "T00:00:00Z'::TIMESTAMP)," +
                            " " + sym + "," +
                            " x::DOUBLE," +
                            " x * 3" +
                            " FROM long_sequence(" + ROW_COUNT + ")");
                    drainWalQueue();
                    execute("ALTER TABLE " + name + " CONVERT PARTITION TO PARQUET LIST '" + INDEXED_PARTITION + "'");
                    drainWalQueue();
                    execute("ALTER TABLE " + name + " ALTER COLUMN sym ADD INDEX TYPE POSTING");
                    drainWalQueue();
                    engine.releaseInactive();

                    try (TableReader r = engine.getReader(engine.verifyTableName(name))) {
                        final int col = r.getMetadata().getColumnIndex("sym");
                        final AbstractParquetPostingIndexReader idx =
                                (AbstractParquetPostingIndexReader) r.getIndexReader(0, col, IndexReader.DIR_FORWARD);
                        Assert.assertTrue("the packed payload property did not take", idx.isPackedPayload());
                        final int flat = idx.rowIdGroupCountByMode(PostingIndexUtils.STRIDE_MODE_FLAT);
                        final int tbl = idx.rowIdGroupCountByMode(PostingIndexUtils.PACKED_MODE_PER_KEY_BLOCKS);
                        final int uni = idx.rowIdGroupCountByMode(PostingIndexUtils.PACKED_MODE_PER_KEY_UNIFORM);
                        table.append(String.format(
                                "  %-9d %-13d %-8s %-5d %-12d %d%n",
                                keys, ROW_COUNT / keys, cluster ? "clust" : "rr", flat, tbl, uni));
                        Assert.assertTrue(
                                "a group got no layout at all [keys=" + keys + ", clustered=" + cluster + ']',
                                flat + tbl + uni > 0);
                    }
                    execute("DROP TABLE " + name);
                    drainWalQueue();
                }
            }
            System.out.println(table);
        });
    }

    /**
     * Whether a full scan re-widens or re-decodes what it has already emitted.
     * <p>
     * The question a bulk cursor would answer is "does each step redo work",
     * and it is worth measuring before designing one: if a fetch re-widened a
     * chunk, the fix is batching, and if it does not, the per-row cost is the
     * cursor PROTOCOL and batching buys nothing.
     * <p>
     * Reported per emitted row. One means every row id is produced exactly
     * once; above one is waste.
     */
    @Test
    public void testAScanProducesEachRowIdExactlyOnce() throws Exception {
        assertMemoryLeak(() -> {
            node1.setProperty(PropertyKey.CAIRO_POSTING_INDEX_PARQUET_PARTITION_FORMAT, "parquet");
            node1.setProperty(PropertyKey.CAIRO_POSTING_INDEX_PARQUET_PACKED_PAYLOAD, true);

            final StringBuilder report = new StringBuilder(
                    "\n  shape             emitted   widened   decoded   widened/row%n".replace("%n", "\n"));
            // Wide keys take the per-key block path, narrow keys the flat one,
            // and they widen through different code. The third shape is the one
            // that matters most: x % keys makes every key's rows an arithmetic
            // progression, which the closed form emits without touching memory,
            // so measuring only those two would report zero work for a reason
            // that has nothing to do with batching. x*x % keys is not linear in
            // x, so those runs are not progressions and MUST be widened.
            for (int shape = 0; shape < 3; shape++) {
                final int keys = shape == 1 ? ROW_COUNT / 2 : 16;
                final String expr = shape == 2
                        ? "'s' || ((x * x) % " + keys + ")"
                        : "'s' || (x % " + keys + ")";
                final String label = shape == 2 ? keys + " keys, scattered" : keys + " keys";
                final String name = "scanwork_" + shape;
                execute("CREATE TABLE " + name + " (" +
                        "ts TIMESTAMP, sym SYMBOL, price DOUBLE, qty LONG" +
                        ") TIMESTAMP(ts) PARTITION BY DAY WAL");
                execute("INSERT INTO " + name + " SELECT" +
                        " dateadd('u', x::INT, '" + INDEXED_PARTITION + "T00:00:00Z'::TIMESTAMP)," +
                        " " + expr + "," +
                        " x::DOUBLE," +
                        " x * 3" +
                        " FROM long_sequence(" + ROW_COUNT + ")");
                drainWalQueue();
                execute("ALTER TABLE " + name + " CONVERT PARTITION TO PARQUET LIST '" + INDEXED_PARTITION + "'");
                drainWalQueue();
                execute("ALTER TABLE " + name + " ALTER COLUMN sym ADD INDEX TYPE POSTING");
                drainWalQueue();
                engine.releaseInactive();

                try (TableReader r = engine.getReader(engine.verifyTableName(name))) {
                    final int col = r.getMetadata().getColumnIndex("sym");
                    final AbstractParquetPostingIndexReader idx =
                            (AbstractParquetPostingIndexReader) r.getIndexReader(0, col, IndexReader.DIR_FORWARD);
                    Assert.assertTrue(idx.isPackedPayload());
                    final int keyCount = idx.getKeyCount();
                    long emitted = 0;
                    for (int k = 0; k < keyCount; k++) {
                        try (RowCursor c = idx.getCursor(k, 0, Long.MAX_VALUE)) {
                            while (c.hasNext()) {
                                c.next();
                                emitted++;
                            }
                        }
                    }
                    final long widened = idx.getWidenedRowIdCount();
                    report.append(String.format(
                            "  %-16s %8d  %8d  %8d   %.3f%n",
                            label, emitted, widened, idx.getDecodedRowCount(),
                            widened / (double) emitted));
                    Assert.assertEquals("the scan must produce every row", ROW_COUNT, emitted);
                    // The batching already sizes each widen to what the cursor
                    // then drains, so nothing is produced twice. If this ever
                    // exceeds one, a fetch is redoing work and BATCHING is the
                    // fix; while it holds, the per-row cost is the protocol.
                    Assert.assertTrue(
                            "a scan widened more row ids than it emitted [emitted=" + emitted
                                    + ", widened=" + widened + ']',
                            widened <= emitted);
                }
                execute("DROP TABLE " + name);
                drainWalQueue();
            }
            System.out.println(report);
        });
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
