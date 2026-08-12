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
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.IndexMetaFileReader;
import io.questdb.cairo.PartitionBy;
import io.questdb.cairo.TableReader;
import io.questdb.cairo.TableToken;
import io.questdb.cairo.TableUtils;
import io.questdb.std.FilesFacade;
import io.questdb.std.str.Path;
import io.questdb.test.AbstractCairoTest;
import org.junit.Assert;
import org.junit.Test;

import java.io.File;

/**
 * Covers the covering-index seal that emits {@code <col>.pidx.<indexTxn>.parquet}
 * plus its {@code _im} sidecar instead of the native {@code .pv} / {@code .pc*}
 * files, selected by {@code cairo.posting.index.parquet.partition.format=parquet}.
 * <p>
 * The indexed partition deliberately carries a sparse key set: its symbols are a
 * subset of the table's, so the key space bound the {@code _im} records is far
 * larger than the count of distinct keys present. Passing the distinct count
 * instead makes every key above the first report as absent with no error at all,
 * which is why the two are asserted separately here.
 */
public class ParquetIndexSealTest extends AbstractCairoTest {

    // Symbol 's15' is the highest key id the indexed partition carries, so the
    // key space bound is its index key (15 + 1 for the null slot) plus one.
    // Both columns are covered and the key is a single symbol, so this is served
    // from the posting index rather than by scanning the parquet. Written out
    // rather than built from TABLE_NAME and INDEXED_PARTITION, which are declared
    // after it.
    private static final String COVERED_QUERY =
            "select price, qty from t_pidx where sym = 's0' and ts in '2024-01-01'";
    private static final int EXPECTED_KEY_SPACE_SIZE = 17;
    private static final String INDEXED_PARTITION = "2024-01-01";
    // Index keys of 's0', 's7' and 's15', that is the symbol id plus one for the
    // null slot. These three of the table's sixteen symbols are all the indexed
    // partition holds, which is what makes its key ids sparse.
    // Row group first keys and row counts the packed fixture must produce. Read
    // the createPackedKeyTable javadoc for how each boundary arises; between them
    // the five groups exercise every branch of planRowGroups.
    private static final int NULLS_ROW_COUNT = 20_000;
    private static final String NULLS_TABLE_NAME = "t_pidx_nulls";
    private static final int[] PACKED_GROUP_FIRST_KEYS = {1, 201, 343, 344, 344};
    private static final long[] PACKED_GROUP_ROW_COUNTS = {100_000, 99_400, 700, 100_000, 50_000};
    // Key id of the packed fixture's row id, as SQL. Symbol ids are handed out in
    // first-appearance order and an index key is the symbol id plus one for the
    // null slot, so 'q0' is key 1, 'q342' is key 343 and 'q999' is key 344.
    private static final String PACKED_KEY_OF_ROW_ID =
            "case when row_id < 100000 then row_id / 500 + 1" +
                    " when row_id < 200100 then 201 + (row_id - 100000) / 700" +
                    " else 344 end";
    private static final int PACKED_ROW_COUNT = 350_100;
    private static final String PACKED_TABLE_NAME = "t_pidx_packed";
    private static final int[] PRESENT_KEYS = {1, 8, 16};
    private static final int SKEWED_ROW_COUNT = 300_000;
    private static final String TABLE_NAME = "t_pidx";

    @Test
    public void testCoveredColumnAddedAfterThePartitionIsSealedAsNulls() throws Exception {
        node1.setProperty(PropertyKey.CAIRO_POSTING_INDEX_PARQUET_PARTITION_FORMAT, "parquet");
        assertMemoryLeak(() -> {
            inputRoot = root;
            execute("CREATE TABLE " + NULLS_TABLE_NAME + " (" +
                    "ts TIMESTAMP, sym SYMBOL, price DOUBLE" +
                    ") TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("INSERT INTO " + NULLS_TABLE_NAME + " SELECT" +
                    " dateadd('u', x::INT, '" + INDEXED_PARTITION + "T00:00:00Z'::TIMESTAMP)," +
                    " 'n' || (x % 2)," +
                    " x::DOUBLE" +
                    " FROM long_sequence(" + NULLS_ROW_COUNT + ")");
            drainWalQueue();
            execute("INSERT INTO " + NULLS_TABLE_NAME + " VALUES ('2024-01-02T00:00:00Z', 'n0', 1.0)");
            drainWalQueue();
            execute("ALTER TABLE " + NULLS_TABLE_NAME + " CONVERT PARTITION TO PARQUET LIST '" + INDEXED_PARTITION + "'");
            drainWalQueue();
            // Added after the partition became parquet, so it is not in the
            // parquet at all and its top in that partition is the partition size.
            // The native seal covers this by emitting nulls; so must this one.
            execute("ALTER TABLE " + NULLS_TABLE_NAME + " ADD COLUMN qty LONG");
            drainWalQueue();
            execute("ALTER TABLE " + NULLS_TABLE_NAME + " ALTER COLUMN sym ADD INDEX TYPE POSTING INCLUDE (price, qty)");
            drainWalQueue();
            engine.releaseInactive();

            final String indexParquetPath;
            try (Path path = new Path()) {
                final String indexParquet = onlyFileNamed(partitionPath(path, NULLS_TABLE_NAME), "sym.pidx.", ".parquet");
                indexParquetPath = partitionPath(path, NULLS_TABLE_NAME).concat(indexParquet).toString();
            }

            assertPostingsCoverEveryRow(indexParquetPath, NULLS_ROW_COUNT);
            assertPostingKeysAgree(indexParquetPath, "case when (row_id + 1) % 2 = 1 then 1 else 2 end");
            assertIndexParquetQuery(
                    "select count() from read_parquet('" + indexParquetPath + "')" +
                            " where qty is not null",
                    "count\n0\n"
            );
            // The covered columns that do exist are unaffected by the all-null one.
            assertIndexParquetQuery(
                    "select count() from read_parquet('" + indexParquetPath + "')" +
                            " where price is null or price <> row_id + 1" +
                            " or ts <> dateadd('u', (row_id + 1)::int, '" + INDEXED_PARTITION + "T00:00:00Z'::timestamp)",
                    "count\n0\n"
            );
        });
    }

    @Test
    public void testPostingIndexReadIsRefusedWhileTheParquetFormatIsSelected() throws Exception {
        node1.setProperty(PropertyKey.CAIRO_POSTING_INDEX_PARQUET_PARTITION_FORMAT, "parquet");
        assertMemoryLeak(() -> {
            createIndexedSparseKeyTable();
            // The seal wrote the index as parquet and discarded the native chain,
            // which a reader would otherwise read as "no keys, no rows" and answer
            // with an empty cursor. Nothing reads the parquet form yet, so the
            // read must fail rather than answer.
            assertQuery(COVERED_QUERY).failsWith("has no reader yet");
        });
    }

    @Test
    public void testPostingIndexReadIsServedWhileTheNativeFormatIsSelected() throws Exception {
        assertMemoryLeak(() -> {
            createIndexedSparseKeyTable();
            // The negative control for the refusal above: on the default format
            // the same query over the same partition is served, so the refusal
            // cannot be reached by a user who has not set the property.
            assertQuery("select count() from (" + COVERED_QUERY + ')')
                    .inferRandomAccess()
                    .expectSize()
                    .returns("count\n75000\n");
        });
    }

    @Test
    public void testSealPacksManySmallKeysIntoSharedRowGroups() throws Exception {
        node1.setProperty(PropertyKey.CAIRO_POSTING_INDEX_PARQUET_PARTITION_FORMAT, "parquet");
        assertMemoryLeak(() -> {
            inputRoot = root;
            createPackedKeyTable();

            execute("ALTER TABLE " + PACKED_TABLE_NAME + " CONVERT PARTITION TO PARQUET LIST '" + INDEXED_PARTITION + "'");
            drainWalQueue();
            execute("ALTER TABLE " + PACKED_TABLE_NAME + " ALTER COLUMN sym ADD INDEX TYPE POSTING INCLUDE (price, qty)");
            drainWalQueue();
            engine.releaseInactive();

            final String indexParquetPath;
            try (Path path = new Path()) {
                final String indexParquet = onlyFileNamed(partitionPath(path, PACKED_TABLE_NAME), "sym.pidx.", ".parquet");
                final String indexMeta = onlyFileNamed(partitionPath(path, PACKED_TABLE_NAME), "sym.pidx.", "._im");
                indexParquetPath = partitionPath(path, PACKED_TABLE_NAME).concat(indexParquet).toString();

                final IndexMetaFileReader reader = new IndexMetaFileReader();
                IndexMetaFileReader.openAndMapRO(
                        configuration.getFilesFacade(),
                        partitionPath(path, PACKED_TABLE_NAME).concat(indexMeta).$(),
                        reader
                );
                try {
                    assertRowGroupsAreKeyAligned(reader);
                    assertRowGroupsArePacked(reader);
                } finally {
                    reader.close();
                }
            }

            assertPostingsCoverEveryRow(indexParquetPath, PACKED_ROW_COUNT);
            assertPostingKeysAgree(indexParquetPath, PACKED_KEY_OF_ROW_ID);
            assertCoveredValuesAgree(indexParquetPath, INDEXED_PARTITION);
        });
    }

    @Test
    public void testSealWritesAKeyAlignedParquetIndex() throws Exception {
        node1.setProperty(PropertyKey.CAIRO_POSTING_INDEX_PARQUET_PARTITION_FORMAT, "parquet");
        assertMemoryLeak(() -> {
            inputRoot = root;
            createSparseKeyTable();

            execute("ALTER TABLE " + TABLE_NAME + " CONVERT PARTITION TO PARQUET LIST '" + INDEXED_PARTITION + "'");
            drainWalQueue();
            execute("ALTER TABLE " + TABLE_NAME + " ALTER COLUMN sym ADD INDEX TYPE POSTING INCLUDE (price, qty)");
            drainWalQueue();
            engine.releaseInactive();

            final String indexParquetPath;
            try (Path path = new Path()) {
                final String indexParquet = onlyFileNamed(partitionPath(path), "sym.pidx.", ".parquet");
                final String indexMeta = onlyFileNamed(partitionPath(path), "sym.pidx.", "._im");
                Assert.assertEquals(
                        "the index parquet and its _im must share one index txn",
                        indexParquet.substring(0, indexParquet.length() - ".parquet".length()),
                        indexMeta.substring(0, indexMeta.length() - "._im".length())
                );
                indexParquetPath = partitionPath(path).concat(indexParquet).toString();

                final FilesFacade ff = configuration.getFilesFacade();
                final IndexMetaFileReader reader = new IndexMetaFileReader();
                IndexMetaFileReader.openAndMapRO(ff, partitionPath(path).concat(indexMeta).$(), reader);
                try {
                    assertKeySpaceIsTheKeyIdBound(reader);
                    assertRowGroupsAreKeyAligned(reader);
                    assertEveryPresentKeyResolves(reader);
                } finally {
                    reader.close();
                }
            }

            assertPostingsCoverEveryRow(indexParquetPath, SKEWED_ROW_COUNT);
            // The whole point of the payload assertion: the _im says nothing
            // about what the parquet holds, so the key of every posting is
            // checked against the row id it is filed under. 's0' takes the rows
            // whose x is divisible by 4, 's7' those with x % 4 == 1 and 's15'
            // the rest, and x is row id + 1.
            assertPostingKeysAgree(
                    indexParquetPath,
                    "case when (row_id + 1) % 4 = 0 then 1 when (row_id + 1) % 4 = 1 then 8 else 16 end"
            );
            assertCoveredValuesAgree(indexParquetPath, INDEXED_PARTITION);
            assertIndexParquetQuery(
                    "select key_id, count() from read_parquet('" + indexParquetPath + "') order by key_id",
                    "key_id\tcount\n1\t75000\n8\t75000\n16\t150000\n"
            );
        });
    }

    /**
     * Every key present in the partition resolves to a non-empty row group range.
     */
    private static void assertEveryPresentKeyResolves(IndexMetaFileReader reader) {
        for (int key : PRESENT_KEYS) {
            Assert.assertNotEquals(
                    "key " + key + " must resolve",
                    IndexMetaFileReader.KEY_ABSENT,
                    reader.getRowGroupRangeForKey(key)
            );
            final int lo = reader.getRowGroupLoForKey(key);
            final int hi = reader.getRowGroupHiForKey(key);
            Assert.assertTrue("key " + key + " range [" + lo + ", " + hi + ']', lo >= 0 && hi >= lo);
            for (int rg = lo; rg <= hi; rg++) {
                Assert.assertTrue("row group " + rg + " must hold rows", reader.getRowGroupNumRows(rg) > 0);
            }
        }
        // The hot key has more postings than a row group targets, so it must
        // occupy consecutive dedicated groups. Without this the alignment check
        // above would pass on a single-group file that proves nothing.
        final int hotKey = PRESENT_KEYS[PRESENT_KEYS.length - 1];
        Assert.assertTrue(
                "key " + hotKey + " must span consecutive dedicated row groups",
                reader.getRowGroupHiForKey(hotKey) > reader.getRowGroupLoForKey(hotKey)
        );
    }

    /**
     * KEY_SPACE_SIZE is the exclusive upper bound on key ids, not a count of
     * distinct keys present. The partition's key ids are sparse, so the two
     * differ, and a reader built on the wrong one resolves the wrong rows.
     */
    private static void assertKeySpaceIsTheKeyIdBound(IndexMetaFileReader reader) {
        Assert.assertEquals(EXPECTED_KEY_SPACE_SIZE, reader.getKeySpaceSize());
        Assert.assertTrue(
                "the partition's key set must be sparse for this test to tell the key space"
                        + " bound apart from a distinct-key count",
                reader.getKeySpaceSize() > PRESENT_KEYS.length
        );
    }

    /**
     * No row group boundary falls mid-key, read back from the {@code key_id}
     * chunk's own statistics rather than taken on the writer's word. A key that
     * ends one row group and starts the next has its earlier postings silently
     * dropped by the reader's exact-match resolution, and nothing at read time
     * can detect it.
     */
    private static void assertRowGroupsAreKeyAligned(IndexMetaFileReader reader) {
        final int keyIdColumn = reader.getKeyIdColumn();
        final int rowGroupCount = reader.getIndexRowGroupCount();
        Assert.assertTrue("a single row group would make this check vacuous", rowGroupCount > 1);
        long previousMax = Long.MIN_VALUE;
        for (int i = 0; i < rowGroupCount; i++) {
            Assert.assertTrue("row group " + i + " has no key id min stat", reader.hasChunkMinStat(i, keyIdColumn));
            Assert.assertTrue("row group " + i + " has no key id max stat", reader.hasChunkMaxStat(i, keyIdColumn));
            final long min = reader.getChunkMinStat(i, keyIdColumn);
            final long max = reader.getChunkMaxStat(i, keyIdColumn);
            Assert.assertEquals("row group " + i + " first key must be its key id min", min, reader.getRowGroupFirstKey(i));
            Assert.assertTrue("row group " + i + " key id range [" + min + ", " + max + ']', min <= max);
            if (i > 0) {
                if (min == previousMax) {
                    // A key larger than the target occupies consecutive dedicated
                    // groups, so a group may repeat the previous group's single
                    // key - but only if it holds nothing else.
                    Assert.assertEquals(
                            "row group " + i + " shares key " + min + " with row group " + (i - 1)
                                    + ", so it must be dedicated to that key",
                            min,
                            max
                    );
                } else {
                    Assert.assertTrue("row group " + i + " starts below row group " + (i - 1), min > previousMax);
                }
            }
            previousMax = max;
        }
    }

    /**
     * The packed fixture's row groups are the ones the key-alignment invariant
     * exists for: two of them hold hundreds of keys apiece. The skewed fixture
     * produces only single-key groups, which makes every statistics assertion
     * above trivially true, so the exact layout is pinned here.
     */
    private static void assertRowGroupsArePacked(IndexMetaFileReader reader) {
        Assert.assertEquals("row group count", PACKED_GROUP_FIRST_KEYS.length, reader.getIndexRowGroupCount());
        final int keyIdColumn = reader.getKeyIdColumn();
        int sharedGroups = 0;
        for (int i = 0; i < PACKED_GROUP_FIRST_KEYS.length; i++) {
            Assert.assertEquals("row group " + i + " first key", PACKED_GROUP_FIRST_KEYS[i], reader.getRowGroupFirstKey(i));
            Assert.assertEquals("row group " + i + " row count", PACKED_GROUP_ROW_COUNTS[i], reader.getRowGroupNumRows(i));
            if (reader.getChunkMaxStat(i, keyIdColumn) > reader.getChunkMinStat(i, keyIdColumn)) {
                sharedGroups++;
            }
        }
        Assert.assertEquals("groups holding more than one key", 2, sharedGroups);
        // Group 0 is closed by the branch that fires when a key boundary lands
        // exactly on the target, group 1 by the branch that closes an open shared
        // group before a key that would overflow it. Neither runs on the skewed
        // fixture.
        Assert.assertEquals(
                "the group closed on the exact target must hold 200 keys",
                200,
                reader.getChunkMaxStat(0, keyIdColumn) - reader.getChunkMinStat(0, keyIdColumn) + 1
        );
        Assert.assertEquals(
                "the group closed before an overflowing key must hold 142 keys",
                142,
                reader.getChunkMaxStat(1, keyIdColumn) - reader.getChunkMinStat(1, keyIdColumn) + 1
        );
    }

    private static String onlyFileNamed(Path partitionPath, String prefix, String suffix) {
        final File[] files = new File(partitionPath.toString())
                .listFiles((_, name) -> name.startsWith(prefix) && name.endsWith(suffix));
        Assert.assertNotNull("no directory at " + partitionPath, files);
        Assert.assertEquals(
                "expected exactly one " + prefix + "*" + suffix + " under " + partitionPath,
                1,
                files.length
        );
        return files[0].getName();
    }

    /**
     * The fixtures write {@code price}, {@code qty} and the row's timestamp from
     * the row's own {@code x}, which is its row id plus one, so a covered value
     * that does not satisfy that arithmetic was gathered from the wrong row.
     * This is what a wrong cover stride or a wrong {@code firstRowId} offset in
     * {@code sortPostingsByKey} looks like from outside; nothing in the
     * {@code _im} can see it. The designated timestamp is a covered column too,
     * because {@code cairo.posting.index.auto.include.timestamp} defaults on.
     */
    private void assertCoveredValuesAgree(String indexParquetPath, String partition) throws Exception {
        assertIndexParquetQuery(
                "select count() from read_parquet('" + indexParquetPath + "')" +
                        " where price is null or qty is null or ts is null" +
                        " or price <> row_id + 1 or qty <> row_id + 1" +
                        " or ts <> dateadd('u', (row_id + 1)::int, '" + partition + "T00:00:00Z'::timestamp)",
                "count\n0\n"
        );
    }

    /**
     * Runs a query over the emitted index parquet on the non-parallel
     * {@code read_parquet} path.
     * <p>
     * The parallel page-frame path cannot read this file: it projects columns by
     * QuestDB column id and, in
     * {@code ReadParquetRecordCursor.canProjectMetadata}, substitutes the
     * parquet column index for any negative id. The index parquet's synthetic
     * {@code key_id} and {@code row_id} carry id -1 (the {@code _im} writer
     * requires exactly that to tell them from the covered columns), so
     * {@code key_id} is remapped to id 0 and collides with any covered column
     * whose writer index is 0 - the designated timestamp, here. {@code key_id}
     * then serves the timestamp's page truncated to 32 bits. The file itself is
     * correct; only that projection is wrong, and it is out of this task's
     * scope.
     */
    private void assertIndexParquetQuery(String sql, String expected) throws Exception {
        final boolean wasParallel = sqlExecutionContext.isParallelReadParquetEnabled();
        try {
            sqlExecutionContext.setParallelReadParquetEnabled(false);
            assertQuery(sql).inferRandomAccess().expectSize().returns(expected);
        } finally {
            sqlExecutionContext.setParallelReadParquetEnabled(wasParallel);
        }
    }

    /**
     * Every posting is filed under the key the row actually holds.
     * {@code keyIdOfRowId} is the fixture's own row id to key id mapping, so a
     * counting sort that placed a row under a neighbouring key is caught even
     * though the per-key counts would still add up.
     */
    private void assertPostingKeysAgree(String indexParquetPath, String keyIdOfRowId) throws Exception {
        assertIndexParquetQuery(
                "select count() from read_parquet('" + indexParquetPath + "')" +
                        " where key_id <> (" + keyIdOfRowId + ")",
                "count\n0\n"
        );
    }

    /**
     * The index carries exactly one posting per row of the partition, and the
     * row ids it carries are the partition's own {@code [0, rowCount)} with no
     * duplicate and no gap. A dropped row, a duplicated row or a shifted row id
     * base all fail here.
     */
    private void assertPostingsCoverEveryRow(String indexParquetPath, long partitionRowCount) throws Exception {
        assertIndexParquetQuery(
                "select count() postings, count_distinct(row_id) distinctRowIds," +
                        " min(row_id) minRowId, max(row_id) maxRowId" +
                        " from read_parquet('" + indexParquetPath + "')",
                "postings\tdistinctRowIds\tminRowId\tmaxRowId\n"
                        + partitionRowCount + '\t' + partitionRowCount + "\t0\t" + (partitionRowCount - 1) + '\n'
        );
    }

    /**
     * The sparse-key fixture with its parquet partition indexed, that is
     * everything the two format tests share.
     */
    private void createIndexedSparseKeyTable() throws Exception {
        createSparseKeyTable();
        execute("ALTER TABLE " + TABLE_NAME + " CONVERT PARTITION TO PARQUET LIST '" + INDEXED_PARTITION + "'");
        drainWalQueue();
        execute("ALTER TABLE " + TABLE_NAME + " ALTER COLUMN sym ADD INDEX TYPE POSTING INCLUDE (price, qty)");
        drainWalQueue();
        engine.releaseInactive();
    }

    /**
     * The table's symbols are inserted into a later partition first, so the
     * symbol map assigns ids 0..15 there; the indexed partition then uses only
     * 's0', 's7' and 's15', leaving its key ids sparse.
     */
    private void createSparseKeyTable() throws Exception {
        execute("CREATE TABLE " + TABLE_NAME + " (" +
                "ts TIMESTAMP, sym SYMBOL, price DOUBLE, qty LONG" +
                ") TIMESTAMP(ts) PARTITION BY DAY WAL");
        execute("INSERT INTO " + TABLE_NAME + " SELECT" +
                " dateadd('m', x::INT, '2024-01-02T00:00:00Z'::TIMESTAMP)," +
                " 's' || (x - 1)," +
                " x::DOUBLE," +
                " x" +
                " FROM long_sequence(16)");
        drainWalQueue();
        // 75_000 rows for 's0', 75_000 for 's7' and 150_000 for 's15'. The skew
        // is deliberate: 's15' has more postings than a row group targets, so it
        // must land in consecutive dedicated groups rather than sharing one.
        execute("INSERT INTO " + TABLE_NAME + " SELECT" +
                " dateadd('u', x::INT, '2024-01-01T00:00:00Z'::TIMESTAMP)," +
                " CASE WHEN x % 4 = 0 THEN 's0' WHEN x % 4 = 1 THEN 's7' ELSE 's15' END," +
                " x::DOUBLE," +
                " x" +
                " FROM long_sequence(" + SKEWED_ROW_COUNT + ")");
        drainWalQueue();
    }

    /**
     * Builds a partition whose keys are mostly small, so row groups pack many
     * keys into one. Over the row's own {@code x}, which is its row id plus one:
     * <ul>
     *     <li>{@code x} in [1, 100000]: 200 keys of 500 rows. The 200th key ends
     *     exactly on the 100000-row target, closing a 200-key shared group.</li>
     *     <li>{@code x} in [100001, 200100]: 143 keys of 700 rows. The 143rd would
     *     take the open group to 100100 rows, so the group is closed before it
     *     with 142 keys in it.</li>
     *     <li>{@code x} above 200100: one key of 150000 rows, which exceeds the
     *     target on its own and so takes consecutive dedicated groups.</li>
     * </ul>
     */
    private void createPackedKeyTable() throws Exception {
        execute("CREATE TABLE " + PACKED_TABLE_NAME + " (" +
                "ts TIMESTAMP, sym SYMBOL, price DOUBLE, qty LONG" +
                ") TIMESTAMP(ts) PARTITION BY DAY WAL");
        execute("INSERT INTO " + PACKED_TABLE_NAME + " SELECT" +
                " dateadd('u', x::INT, '" + INDEXED_PARTITION + "T00:00:00Z'::TIMESTAMP)," +
                " CASE WHEN x <= 100000 THEN 'q' || ((x - 1) / 500)" +
                " WHEN x <= 200100 THEN 'q' || (200 + (x - 100001) / 700)" +
                " ELSE 'q999' END," +
                " x::DOUBLE," +
                " x" +
                " FROM long_sequence(" + PACKED_ROW_COUNT + ")");
        drainWalQueue();
        // A later partition, so the indexed one is not the active partition and
        // can be converted to parquet. Its symbol already exists, so it moves no
        // key id.
        execute("INSERT INTO " + PACKED_TABLE_NAME + " VALUES ('2024-01-02T00:00:00Z', 'q0', 1.0, 1)");
        drainWalQueue();
    }

    private Path partitionPath(Path path) {
        return partitionPath(path, TABLE_NAME);
    }

    private Path partitionPath(Path path, String tableName) {
        final TableToken token = engine.verifyTableName(tableName);
        final long partitionTs;
        final long nameTxn;
        try (TableReader reader = engine.getReader(token)) {
            partitionTs = reader.getTxFile().getPartitionTimestampByIndex(0);
            nameTxn = reader.getTxFile().getPartitionNameTxn(0);
        }
        path.of(configuration.getDbRoot()).concat(token);
        TableUtils.setPathForNativePartition(path, ColumnType.TIMESTAMP, PartitionBy.DAY, partitionTs, nameTxn);
        return path;
    }
}
