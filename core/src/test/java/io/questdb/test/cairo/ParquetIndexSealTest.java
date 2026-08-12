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
    private static final int EXPECTED_KEY_SPACE_SIZE = 17;
    private static final String INDEXED_PARTITION = "2024-01-01";
    // Index keys of 's0', 's7' and 's15', that is the symbol id plus one for the
    // null slot. These three of the table's sixteen symbols are all the indexed
    // partition holds, which is what makes its key ids sparse.
    private static final int[] PRESENT_KEYS = {1, 8, 16};
    private static final int SKEWED_ROW_COUNT = 300_000;
    private static final String TABLE_NAME = "t_pidx";

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

    private Path partitionPath(Path path) {
        final TableToken token = engine.verifyTableName(TABLE_NAME);
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
