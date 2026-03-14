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

package io.questdb.test.cairo.parquet;

import io.questdb.PropertyKey;
import io.questdb.cairo.TableReader;
import io.questdb.cairo.sql.PartitionFormat;
import io.questdb.griffin.engine.table.ParquetRowGroupFilter;
import io.questdb.griffin.engine.table.parquet.PartitionDecoder;
import io.questdb.test.AbstractCairoTest;
import org.junit.Assert;
import org.junit.Test;

/**
 * Integration tests for O3 writes into Parquet partitions, covering the three
 * rewrite triggers:
 * <ul>
 *   <li>Single row group &mdash; always rewritten to avoid dead space</li>
 *   <li>Unused-bytes ratio exceeds threshold</li>
 *   <li>Absolute unused bytes exceeds threshold</li>
 * </ul>
 */
public class ParquetWriteTest extends AbstractCairoTest {

    @Test
    public void testCopyRowGroupPreservesBloomFilterOffset() throws Exception {
        // Small row group size to produce 3 row groups.
        // Set absolute threshold low so the second O3 triggers a rewrite.
        node1.setProperty(PropertyKey.CAIRO_PARTITION_ENCODER_PARQUET_ROW_GROUP_SIZE, 4);
        node1.setProperty(PropertyKey.CAIRO_PARTITION_ENCODER_PARQUET_O3_REWRITE_UNUSED_RATIO, "1.0");
        node1.setProperty(PropertyKey.CAIRO_PARTITION_ENCODER_PARQUET_O3_REWRITE_UNUSED_MAX_BYTES, 100);
        assertMemoryLeak(() -> {
            execute(
                    """
                            CREATE TABLE x (x INT, ts TIMESTAMP)
                            TIMESTAMP(ts) PARTITION BY DAY WAL
                            """
            );
            // Insert 12 rows → 3 row groups (RG0, RG1, RG2) of 4 rows each.
            // Values chosen so the bloom filter is the only way to skip
            // the copied row group for specific query values.
            execute(
                    """
                            INSERT INTO x(x, ts) VALUES
                            (1, '2020-01-01T00:00:00.000Z'),
                            (2, '2020-01-01T01:00:00.000Z'),
                            (3, '2020-01-01T02:00:00.000Z'),
                            (4, '2020-01-01T03:00:00.000Z'),
                            (5, '2020-01-01T04:00:00.000Z'),
                            (6, '2020-01-01T05:00:00.000Z'),
                            (7, '2020-01-01T06:00:00.000Z'),
                            (8, '2020-01-01T07:00:00.000Z'),
                            (9, '2020-01-01T08:00:00.000Z'),
                            (10, '2020-01-01T09:00:00.000Z'),
                            (11, '2020-01-01T10:00:00.000Z'),
                            (12, '2020-01-01T11:00:00.000Z')
                            """
            );
            // Extra row in a different partition so 2020-01-01 is not the last.
            execute("INSERT INTO x(x, ts) VALUES (1000, '2020-01-02T00:00:00.000Z')");
            drainWalQueue();

            // Convert with bloom filters on the 'x' column.
            execute("ALTER TABLE x CONVERT PARTITION TO PARQUET LIST '2020-01-01' WITH (bloom_filter_columns = 'x')");
            drainWalQueue();

            // First O3: UPDATE mode. Inserts into RG0's time range.
            // This replaces RG0, appending the new RG0' at the end of the file,
            // leaving dead RG0 data at the original position near the file start.
            // RG0' values: {1, 100, 2, 101, 3, 4} → min=1, max=101
            execute(
                    """
                            INSERT INTO x(x, ts) VALUES
                            (100, '2020-01-01T00:30:00.000Z'),
                            (101, '2020-01-01T01:30:00.000Z')
                            """
            );
            drainWalQueue();

            // Second O3: targets RG1's time range (different from the first O3).
            // accumulated unused_bytes > 100 → REWRITE mode.
            // In the REWRITE, RG0' (appended at the end of the old file) is
            // raw-copied to the beginning of the new file via copy_row_group,
            // creating a large offset_delta. copy_row_group must adjust
            // bloom_filter_offset by the same delta as data_page_offset.
            execute(
                    """
                            INSERT INTO x(x, ts) VALUES
                            (200, '2020-01-01T04:30:00.000Z'),
                            (201, '2020-01-01T05:30:00.000Z')
                            """
            );
            drainWalQueue();

            // After REWRITE, the parquet partition has 3 row groups:
            //   RG0' (copied): x in {1, 100, 2, 101, 3, 4} → min=1, max=101
            //   RG1' (merged): x in {5, 200, 6, 201, 7, 8} → min=5, max=201
            //   RG2  (copied): x in {9, 10, 11, 12}         → min=9, max=12
            //
            // Query: WHERE x = 50
            //   RG0': 50 in [1,101] → can't skip by min/max → bloom filter needed
            //         50 NOT in {1,100,2,101,3,4} → bloom SHOULD skip
            //   RG1': 50 in [5,201] → can't skip by min/max → bloom filter needed
            //         50 NOT in {5,200,6,201,7,8} → bloom SHOULD skip
            //   RG2:  50 > 12 → skipped by min/max alone
            //
            // If bloom_filter_offset is stale on copied RG0', its bloom filter
            // read fails silently → RG0' is NOT skipped → fewer skips.
            // Verify data correctness.
            assertSql(
                    """
                            x\tts
                            1\t2020-01-01T00:00:00.000000Z
                            100\t2020-01-01T00:30:00.000000Z
                            2\t2020-01-01T01:00:00.000000Z
                            101\t2020-01-01T01:30:00.000000Z
                            3\t2020-01-01T02:00:00.000000Z
                            4\t2020-01-01T03:00:00.000000Z
                            5\t2020-01-01T04:00:00.000000Z
                            200\t2020-01-01T04:30:00.000000Z
                            6\t2020-01-01T05:00:00.000000Z
                            201\t2020-01-01T05:30:00.000000Z
                            7\t2020-01-01T06:00:00.000000Z
                            8\t2020-01-01T07:00:00.000000Z
                            9\t2020-01-01T08:00:00.000000Z
                            10\t2020-01-01T09:00:00.000000Z
                            11\t2020-01-01T10:00:00.000000Z
                            12\t2020-01-01T11:00:00.000000Z
                            1000\t2020-01-02T00:00:00.000000Z
                            """,
                    "SELECT * FROM x"
            );

            // Bloom filter skip check. assertSql runs a single query execution,
            // so the counter reflects exactly one scan of the 3 row groups.
            // Query: WHERE x = 50
            //   RG0' (copied): 50 in [1,101] → needs bloom filter → should skip
            //   RG1' (merged): 50 in [5,201] → needs bloom filter → should skip
            //   RG2  (copied): 50 > 12       → skipped by min/max
            // All 3 row groups should be skipped → counter = 3.
            // With stale bloom_filter_offset on copied RG0', bloom filter read
            // fails silently → RG0' NOT skipped → counter = 2.
            ParquetRowGroupFilter.resetRowGroupsSkipped();
            assertSql("x\n", "SELECT x FROM x WHERE x = 50");
            Assert.assertEquals(
                    "bloom filter should skip all 3 row groups (2 by bloom, 1 by min/max)",
                    3,
                    ParquetRowGroupFilter.getRowGroupsSkipped()
            );
        });
    }

    @Test
    public void testFooterCacheUsedInUpdateMode() throws Exception {
        // Small row group size to produce multiple row groups.
        // Disable rewrite: ratio=1.0 (impossible to exceed), max_bytes=Long.MAX_VALUE.
        // This forces every O3 merge to use the UPDATE path, exercising footer_cache.
        node1.setProperty(PropertyKey.CAIRO_PARTITION_ENCODER_PARQUET_ROW_GROUP_SIZE, 4);
        node1.setProperty(PropertyKey.CAIRO_PARTITION_ENCODER_PARQUET_O3_REWRITE_UNUSED_RATIO, "1.0");
        node1.setProperty(PropertyKey.CAIRO_PARTITION_ENCODER_PARQUET_O3_REWRITE_UNUSED_MAX_BYTES, Long.MAX_VALUE);
        assertMemoryLeak(() -> {
            execute(
                    """
                            CREATE TABLE x (x INT, ts TIMESTAMP)
                            TIMESTAMP(ts) PARTITION BY DAY WAL
                            """
            );
            execute(
                    """
                            INSERT INTO x(x, ts) VALUES
                            (1, '2020-01-01T00:00:00.000Z'),
                            (2, '2020-01-01T01:00:00.000Z'),
                            (3, '2020-01-01T02:00:00.000Z'),
                            (4, '2020-01-01T03:00:00.000Z'),
                            (5, '2020-01-01T04:00:00.000Z'),
                            (6, '2020-01-01T05:00:00.000Z'),
                            (7, '2020-01-01T06:00:00.000Z'),
                            (8, '2020-01-01T07:00:00.000Z')
                            """
            );
            execute("INSERT INTO x(x, ts) VALUES (100, '2020-01-02T00:00:00.000Z')");
            drainWalQueue();

            // 8 rows with row group size 4 → 2 row groups.
            execute("ALTER TABLE x CONVERT PARTITION TO PARQUET LIST '2020-01-01'");
            drainWalQueue();

            int rowGroupCountBefore;
            try (TableReader reader = getReader("x")) {
                for (int i = 0, n = reader.getPartitionCount(); i < n; i++) {
                    if (reader.getPartitionFormat(i) != PartitionFormat.PARQUET) {
                        continue;
                    }
                    reader.openPartition(i);
                    PartitionDecoder decoder = reader.getAndInitParquetPartitionDecoders(i);
                    rowGroupCountBefore = decoder.metadata().getRowGroupCount();
                    Assert.assertEquals("initial row group count", 2, rowGroupCountBefore);
                }
            }

            // First O3: UPDATE mode. FooterCache parses the original footer,
            // scans row group offsets, and appends a replacement row group.
            execute(
                    """
                            INSERT INTO x(x, ts) VALUES
                            (9, '2020-01-01T00:30:00.000Z'),
                            (10, '2020-01-01T01:30:00.000Z')
                            """
            );
            drainWalQueue();

            int rowGroupCountAfterFirst = 0;
            long unusedAfterFirst = 0;
            try (TableReader reader = getReader("x")) {
                for (int i = 0, n = reader.getPartitionCount(); i < n; i++) {
                    if (reader.getPartitionFormat(i) != PartitionFormat.PARQUET) {
                        continue;
                    }
                    reader.openPartition(i);
                    PartitionDecoder decoder = reader.getAndInitParquetPartitionDecoders(i);
                    rowGroupCountAfterFirst = decoder.metadata().getRowGroupCount();
                    unusedAfterFirst = decoder.metadata().getUnusedBytes();
                    Assert.assertTrue(
                            "row group count should increase after first O3 update, was 2, got " + rowGroupCountAfterFirst,
                            rowGroupCountAfterFirst > 2
                    );
                    Assert.assertTrue(
                            "unused_bytes should be > 0 after first update, got " + unusedAfterFirst,
                            unusedAfterFirst > 0
                    );
                }
            }

            // Second O3: UPDATE mode again. FooterCache re-parses the updated footer
            // (which already contains dead space from the first update) and appends
            // another replacement row group.
            execute(
                    """
                            INSERT INTO x(x, ts) VALUES
                            (11, '2020-01-01T04:30:00.000Z'),
                            (12, '2020-01-01T05:30:00.000Z')
                            """
            );
            drainWalQueue();

            int rowGroupCountAfterSecond;
            long unusedAfterSecond;
            try (TableReader reader = getReader("x")) {
                for (int i = 0, n = reader.getPartitionCount(); i < n; i++) {
                    if (reader.getPartitionFormat(i) != PartitionFormat.PARQUET) {
                        continue;
                    }
                    reader.openPartition(i);
                    PartitionDecoder decoder = reader.getAndInitParquetPartitionDecoders(i);
                    rowGroupCountAfterSecond = decoder.metadata().getRowGroupCount();
                    unusedAfterSecond = decoder.metadata().getUnusedBytes();
                    Assert.assertTrue(
                            "row group count should increase after second O3 update, was " + rowGroupCountAfterFirst + ", got " + rowGroupCountAfterSecond,
                            rowGroupCountAfterSecond > rowGroupCountAfterFirst
                    );
                    Assert.assertTrue(
                            "unused_bytes should grow after second update, got " + unusedAfterSecond,
                            unusedAfterSecond > unusedAfterFirst
                    );
                }
            }

            assertSql(
                    """
                            x\tts
                            1\t2020-01-01T00:00:00.000000Z
                            9\t2020-01-01T00:30:00.000000Z
                            2\t2020-01-01T01:00:00.000000Z
                            10\t2020-01-01T01:30:00.000000Z
                            3\t2020-01-01T02:00:00.000000Z
                            4\t2020-01-01T03:00:00.000000Z
                            5\t2020-01-01T04:00:00.000000Z
                            11\t2020-01-01T04:30:00.000000Z
                            6\t2020-01-01T05:00:00.000000Z
                            12\t2020-01-01T05:30:00.000000Z
                            7\t2020-01-01T06:00:00.000000Z
                            8\t2020-01-01T07:00:00.000000Z
                            100\t2020-01-02T00:00:00.000000Z
                            """,
                    "SELECT * FROM x"
            );
        });
    }

    @Test
    public void testO3AfterAddColumnSuspendsTable() throws Exception {
        assertMemoryLeak(() -> {
            execute(
                    """
                            CREATE TABLE x (x INT, s SYMBOL, ts TIMESTAMP)
                            TIMESTAMP(ts) PARTITION BY DAY WAL
                            """
            );
            execute(
                    """
                            INSERT INTO x(x, s, ts) VALUES
                            (1, 'a', '2020-01-01T00:00:00.000Z'),
                            (2, 'b', '2020-01-01T06:00:00.000Z'),
                            (3, 'a', '2020-01-01T12:00:00.000Z'),
                            (4, 'c', '2020-01-01T18:00:00.000Z')
                            """
            );
            execute("INSERT INTO x(x, s, ts) VALUES (100, 'd', '2020-01-02T00:00:00.000Z')");
            drainWalQueue();

            execute("ALTER TABLE x CONVERT PARTITION TO PARQUET LIST '2020-01-01'");
            drainWalQueue();

            // Add a new column after converting to Parquet.
            // The Parquet file has 3 columns (x, s, ts), the table now has 4 (x, s, ts, y).
            execute("ALTER TABLE x ADD COLUMN y DOUBLE");
            drainWalQueue();

            // O3 insert into the parquet partition that overlaps existing data.
            // The merge fails because O3 merge does not support column remapping:
            // the Parquet file has 3 columns but the table schema now has 4.
            // The partition must be reconverted before O3 merges can succeed.
            execute(
                    """
                            INSERT INTO x(x, s, y, ts) VALUES
                            (5, 'b', 1.5, '2020-01-01T03:00:00.000Z'),
                            (6, 'a', 2.5, '2020-01-01T09:00:00.000Z'),
                            (7, 'c', 3.5, '2020-01-01T15:00:00.000Z')
                            """
            );
            drainWalQueue();

            // The O3 merge failure suspends the table. The failing WAL transaction
            // is rolled back, so the O3 rows are not visible. Original data remains intact.
            Assert.assertTrue(engine.getTableSequencerAPI().isSuspended(engine.verifyTableName("x")));
        });
    }

    @Test
    public void testO3MergeWithVarSizeColumnTop() throws Exception {
        // Small row group size so the column-top covers entire row groups.
        node1.setProperty(PropertyKey.CAIRO_PARTITION_ENCODER_PARQUET_ROW_GROUP_SIZE, 4);
        assertMemoryLeak(() -> {
            execute(
                    """
                            CREATE TABLE x (x INT, ts TIMESTAMP)
                            TIMESTAMP(ts) PARTITION BY DAY WAL
                            """
            );
            // Insert 8 rows without the STRING column.
            execute(
                    """
                            INSERT INTO x(x, ts) VALUES
                            (1, '2020-01-01T00:00:00.000Z'),
                            (2, '2020-01-01T01:00:00.000Z'),
                            (3, '2020-01-01T02:00:00.000Z'),
                            (4, '2020-01-01T03:00:00.000Z'),
                            (5, '2020-01-01T04:00:00.000Z'),
                            (6, '2020-01-01T05:00:00.000Z'),
                            (7, '2020-01-01T06:00:00.000Z'),
                            (8, '2020-01-01T07:00:00.000Z')
                            """
            );
            execute("INSERT INTO x(x, ts) VALUES (100, '2020-01-02T00:00:00.000Z')");
            drainWalQueue();

            // Add STRING column. column_top = 8 for this partition.
            execute("ALTER TABLE x ADD COLUMN s STRING");
            drainWalQueue();

            // Insert 4 more rows with s values.
            execute(
                    """
                            INSERT INTO x(x, s, ts) VALUES
                            (9, 'abc', '2020-01-01T08:00:00.000Z'),
                            (10, 'def', '2020-01-01T09:00:00.000Z'),
                            (11, 'ghi', '2020-01-01T10:00:00.000Z'),
                            (12, 'jkl', '2020-01-01T11:00:00.000Z')
                            """
            );
            drainWalQueue();

            // 12 rows with row group size 4 → 3 row groups.
            // RG0 (rows 0-3): s is entirely topped (column_top=8 ≥ 4)
            // RG1 (rows 4-7): s is entirely topped (column_top=8 ≥ 8)
            // RG2 (rows 8-11): s has actual data
            execute("ALTER TABLE x CONVERT PARTITION TO PARQUET LIST '2020-01-01'");
            drainWalQueue();

            // O3 insert targeting RG0 (timestamp range 00:00-03:00).
            // The merge decodes RG0 where s has column_top → null aux_ptr.
            // Exercises the null source buffer allocation for var-size columns
            // (STRING null markers need a non-zero data buffer).
            execute(
                    """
                            INSERT INTO x(x, s, ts) VALUES
                            (13, 'new', '2020-01-01T00:30:00.000Z')
                            """
            );
            drainWalQueue();

            assertSql(
                    """
                            x\tts\ts
                            1\t2020-01-01T00:00:00.000000Z\t
                            13\t2020-01-01T00:30:00.000000Z\tnew
                            2\t2020-01-01T01:00:00.000000Z\t
                            3\t2020-01-01T02:00:00.000000Z\t
                            4\t2020-01-01T03:00:00.000000Z\t
                            5\t2020-01-01T04:00:00.000000Z\t
                            6\t2020-01-01T05:00:00.000000Z\t
                            7\t2020-01-01T06:00:00.000000Z\t
                            8\t2020-01-01T07:00:00.000000Z\t
                            9\t2020-01-01T08:00:00.000000Z\tabc
                            10\t2020-01-01T09:00:00.000000Z\tdef
                            11\t2020-01-01T10:00:00.000000Z\tghi
                            12\t2020-01-01T11:00:00.000000Z\tjkl
                            100\t2020-01-02T00:00:00.000000Z\t
                            """,
                    "SELECT * FROM x"
            );
        });
    }

    @Test
    public void testRewriteAbsoluteUnusedBytesThreshold() throws Exception {
        // Use small row group size to get multiple row groups.
        // Disable ratio check (set to 1.0 = impossible to exceed).
        // Set absolute threshold very low so the second O3 triggers a rewrite.
        node1.setProperty(PropertyKey.CAIRO_PARTITION_ENCODER_PARQUET_ROW_GROUP_SIZE, 4);
        node1.setProperty(PropertyKey.CAIRO_PARTITION_ENCODER_PARQUET_O3_REWRITE_UNUSED_RATIO, "1.0");
        node1.setProperty(PropertyKey.CAIRO_PARTITION_ENCODER_PARQUET_O3_REWRITE_UNUSED_MAX_BYTES, 100);
        assertMemoryLeak(() -> {
            execute(
                    """
                            CREATE TABLE x (x INT, s SYMBOL, v VARCHAR, ts TIMESTAMP)
                            TIMESTAMP(ts) PARTITION BY DAY WAL
                            """
            );
            execute(
                    """
                            INSERT INTO x(x, s, v, ts) VALUES
                            (1, 'a', 'foo', '2020-01-01T00:00:00.000Z'),
                            (2, 'b', 'bar', '2020-01-01T01:00:00.000Z'),
                            (3, 'a', 'baz', '2020-01-01T02:00:00.000Z'),
                            (4, 'c', 'qux', '2020-01-01T03:00:00.000Z'),
                            (5, 'b', 'abc', '2020-01-01T04:00:00.000Z'),
                            (6, 'a', 'def', '2020-01-01T05:00:00.000Z'),
                            (7, 'c', 'ghi', '2020-01-01T06:00:00.000Z'),
                            (8, 'b', 'jkl', '2020-01-01T07:00:00.000Z')
                            """
            );
            execute("INSERT INTO x(x, s, v, ts) VALUES (100, 'd', 'end', '2020-01-02T00:00:00.000Z')");
            drainWalQueue();

            // 8 rows with row group size 4 → 2 row groups.
            execute("ALTER TABLE x CONVERT PARTITION TO PARQUET LIST '2020-01-01'");
            drainWalQueue();

            // First O3: unused_bytes = 0, multiple row groups → UPDATE mode.
            // Replaces one row group, accumulating dead space.
            execute(
                    """
                            INSERT INTO x(x, s, v, ts) VALUES
                            (9, 'b', 'mno', '2020-01-01T00:30:00.000Z'),
                            (10, 'c', 'pqr', '2020-01-01T01:30:00.000Z')
                            """
            );
            drainWalQueue();

            long partitionTs = parseFloorPartialTimestamp("2020-01-01");
            long versionAfterUpdate = getPartitionNameTxn("x", partitionTs);

            // Second O3: accumulated unused_bytes > 100 → REWRITE.
            // Rewrite copies unchanged row groups (exercises copy_row_group
            // with dictionary pages from SYMBOL columns).
            execute(
                    """
                            INSERT INTO x(x, s, v, ts) VALUES
                            (11, 'a', 'stu', '2020-01-01T04:30:00.000Z'),
                            (12, 'c', 'vwx', '2020-01-01T05:30:00.000Z')
                            """
            );
            drainWalQueue();

            long versionAfterRewrite = getPartitionNameTxn("x", partitionTs);
            Assert.assertNotEquals("partition version should change on REWRITE", versionAfterUpdate, versionAfterRewrite);

            assertSql(
                    """
                            x\ts\tv\tts
                            1\ta\tfoo\t2020-01-01T00:00:00.000000Z
                            9\tb\tmno\t2020-01-01T00:30:00.000000Z
                            2\tb\tbar\t2020-01-01T01:00:00.000000Z
                            10\tc\tpqr\t2020-01-01T01:30:00.000000Z
                            3\ta\tbaz\t2020-01-01T02:00:00.000000Z
                            4\tc\tqux\t2020-01-01T03:00:00.000000Z
                            5\tb\tabc\t2020-01-01T04:00:00.000000Z
                            11\ta\tstu\t2020-01-01T04:30:00.000000Z
                            6\ta\tdef\t2020-01-01T05:00:00.000000Z
                            12\tc\tvwx\t2020-01-01T05:30:00.000000Z
                            7\tc\tghi\t2020-01-01T06:00:00.000000Z
                            8\tb\tjkl\t2020-01-01T07:00:00.000000Z
                            100\td\tend\t2020-01-02T00:00:00.000000Z
                            """,
                    "SELECT * FROM x"
            );
        });
    }

    @Test
    public void testRewriteResetsUnusedBytesToZero() throws Exception {
        // Use small row group size to get multiple row groups.
        // Set absolute threshold low so the second O3 triggers a rewrite.
        node1.setProperty(PropertyKey.CAIRO_PARTITION_ENCODER_PARQUET_ROW_GROUP_SIZE, 4);
        node1.setProperty(PropertyKey.CAIRO_PARTITION_ENCODER_PARQUET_O3_REWRITE_UNUSED_RATIO, "1.0");
        node1.setProperty(PropertyKey.CAIRO_PARTITION_ENCODER_PARQUET_O3_REWRITE_UNUSED_MAX_BYTES, 100);
        assertMemoryLeak(() -> {
            execute(
                    """
                            CREATE TABLE x (x INT, ts TIMESTAMP)
                            TIMESTAMP(ts) PARTITION BY DAY WAL
                            """
            );
            execute(
                    """
                            INSERT INTO x(x, ts) VALUES
                            (1, '2020-01-01T00:00:00.000Z'),
                            (2, '2020-01-01T01:00:00.000Z'),
                            (3, '2020-01-01T02:00:00.000Z'),
                            (4, '2020-01-01T03:00:00.000Z'),
                            (5, '2020-01-01T04:00:00.000Z'),
                            (6, '2020-01-01T05:00:00.000Z'),
                            (7, '2020-01-01T06:00:00.000Z'),
                            (8, '2020-01-01T07:00:00.000Z')
                            """
            );
            execute("INSERT INTO x(x, ts) VALUES (100, '2020-01-02T00:00:00.000Z')");
            drainWalQueue();

            // 8 rows with row group size 4 -> 2 row groups.
            execute("ALTER TABLE x CONVERT PARTITION TO PARQUET LIST '2020-01-01'");
            drainWalQueue();

            // First O3: UPDATE mode. Replaces one row group, accumulating dead space.
            execute(
                    """
                            INSERT INTO x(x, ts) VALUES
                            (9, '2020-01-01T00:30:00.000Z'),
                            (10, '2020-01-01T01:30:00.000Z')
                            """
            );
            drainWalQueue();

            // Verify unused_bytes > 0 after in-place update.
            long unusedAfterUpdate;
            try (TableReader reader = getReader("x")) {
                for (int i = 0, n = reader.getPartitionCount(); i < n; i++) {
                    if (reader.getPartitionFormat(i) != PartitionFormat.PARQUET) {
                        continue;
                    }
                    reader.openPartition(i);
                    PartitionDecoder decoder = reader.getAndInitParquetPartitionDecoders(i);
                    unusedAfterUpdate = decoder.metadata().getUnusedBytes();
                    Assert.assertTrue("unused_bytes should be > 0 after update, got " + unusedAfterUpdate, unusedAfterUpdate > 0);
                }
            }

            // Second O3: accumulated unused_bytes > 100 -> REWRITE.
            execute(
                    """
                            INSERT INTO x(x, ts) VALUES
                            (11, '2020-01-01T04:30:00.000Z'),
                            (12, '2020-01-01T05:30:00.000Z')
                            """
            );
            drainWalQueue();

            // Verify unused_bytes == 0 after rewrite.
            try (TableReader reader = getReader("x")) {
                for (int i = 0, n = reader.getPartitionCount(); i < n; i++) {
                    if (reader.getPartitionFormat(i) != PartitionFormat.PARQUET) {
                        continue;
                    }
                    reader.openPartition(i);
                    PartitionDecoder decoder = reader.getAndInitParquetPartitionDecoders(i);
                    long unusedAfterRewrite = decoder.metadata().getUnusedBytes();
                    Assert.assertEquals("unused_bytes should be 0 after rewrite", 0, unusedAfterRewrite);
                }
            }

            assertSql(
                    """
                            x\tts
                            1\t2020-01-01T00:00:00.000000Z
                            9\t2020-01-01T00:30:00.000000Z
                            2\t2020-01-01T01:00:00.000000Z
                            10\t2020-01-01T01:30:00.000000Z
                            3\t2020-01-01T02:00:00.000000Z
                            4\t2020-01-01T03:00:00.000000Z
                            5\t2020-01-01T04:00:00.000000Z
                            11\t2020-01-01T04:30:00.000000Z
                            6\t2020-01-01T05:00:00.000000Z
                            12\t2020-01-01T05:30:00.000000Z
                            7\t2020-01-01T06:00:00.000000Z
                            8\t2020-01-01T07:00:00.000000Z
                            100\t2020-01-02T00:00:00.000000Z
                            """,
                    "SELECT * FROM x"
            );
        });
    }

    @Test
    public void testRewriteSingleRowGroup() throws Exception {
        assertMemoryLeak(() -> {
            execute(
                    """
                            CREATE TABLE x (x INT, s SYMBOL, v VARCHAR, ts TIMESTAMP)
                            TIMESTAMP(ts) PARTITION BY DAY WAL
                            """
            );
            execute(
                    """
                            INSERT INTO x(x, s, v, ts) VALUES
                            (1, 'a', 'foo', '2020-01-01T00:00:00.000Z'),
                            (2, 'b', 'bar', '2020-01-01T06:00:00.000Z'),
                            (3, 'a', 'baz', '2020-01-01T12:00:00.000Z'),
                            (4, 'c', 'qux', '2020-01-01T18:00:00.000Z')
                            """
            );
            // Insert into the next day so 2020-01-01 is not the last partition.
            execute("INSERT INTO x(x, s, v, ts) VALUES (100, 'd', 'end', '2020-01-02T00:00:00.000Z')");
            drainWalQueue();

            // 4 rows with default test row group size (1000) → single row group.
            execute("ALTER TABLE x CONVERT PARTITION TO PARQUET LIST '2020-01-01'");
            drainWalQueue();

            long partitionTs = parseFloorPartialTimestamp("2020-01-01");
            long versionBeforeO3 = getPartitionNameTxn("x", partitionTs);

            // O3 insert into the parquet partition.
            // Single row group always triggers a full REWRITE.
            execute(
                    """
                            INSERT INTO x(x, s, v, ts) VALUES
                            (5, 'b', 'abc', '2020-01-01T03:00:00.000Z'),
                            (6, 'a', 'def', '2020-01-01T09:00:00.000Z'),
                            (7, 'c', 'ghi', '2020-01-01T15:00:00.000Z')
                            """
            );
            drainWalQueue();

            long versionAfterRewrite = getPartitionNameTxn("x", partitionTs);
            Assert.assertNotEquals("partition version should change on REWRITE", versionBeforeO3, versionAfterRewrite);

            assertSql(
                    """
                            x\ts\tv\tts
                            1\ta\tfoo\t2020-01-01T00:00:00.000000Z
                            5\tb\tabc\t2020-01-01T03:00:00.000000Z
                            2\tb\tbar\t2020-01-01T06:00:00.000000Z
                            6\ta\tdef\t2020-01-01T09:00:00.000000Z
                            3\ta\tbaz\t2020-01-01T12:00:00.000000Z
                            7\tc\tghi\t2020-01-01T15:00:00.000000Z
                            4\tc\tqux\t2020-01-01T18:00:00.000000Z
                            100\td\tend\t2020-01-02T00:00:00.000000Z
                            """,
                    "SELECT * FROM x"
            );
        });
    }

    @Test
    public void testRewriteUnusedBytesRatioThreshold() throws Exception {
        // Use small row group size to get multiple row groups.
        // Set ratio to 10% to trigger rewrite after one update round.
        // Disable absolute bytes threshold.
        node1.setProperty(PropertyKey.CAIRO_PARTITION_ENCODER_PARQUET_ROW_GROUP_SIZE, 4);
        node1.setProperty(PropertyKey.CAIRO_PARTITION_ENCODER_PARQUET_O3_REWRITE_UNUSED_RATIO, "0.1");
        node1.setProperty(PropertyKey.CAIRO_PARTITION_ENCODER_PARQUET_O3_REWRITE_UNUSED_MAX_BYTES, Long.MAX_VALUE);
        assertMemoryLeak(() -> {
            execute(
                    """
                            CREATE TABLE x (x INT, s SYMBOL, v VARCHAR, ts TIMESTAMP)
                            TIMESTAMP(ts) PARTITION BY DAY WAL
                            """
            );
            execute(
                    """
                            INSERT INTO x(x, s, v, ts) VALUES
                            (1, 'a', 'foo', '2020-01-01T00:00:00.000Z'),
                            (2, 'b', 'bar', '2020-01-01T01:00:00.000Z'),
                            (3, 'a', 'baz', '2020-01-01T02:00:00.000Z'),
                            (4, 'c', 'qux', '2020-01-01T03:00:00.000Z'),
                            (5, 'b', 'abc', '2020-01-01T04:00:00.000Z'),
                            (6, 'a', 'def', '2020-01-01T05:00:00.000Z'),
                            (7, 'c', 'ghi', '2020-01-01T06:00:00.000Z'),
                            (8, 'b', 'jkl', '2020-01-01T07:00:00.000Z')
                            """
            );
            execute("INSERT INTO x(x, s, v, ts) VALUES (100, 'd', 'end', '2020-01-02T00:00:00.000Z')");
            drainWalQueue();

            // 8 rows with row group size 4 → 2 row groups.
            execute("ALTER TABLE x CONVERT PARTITION TO PARQUET LIST '2020-01-01'");
            drainWalQueue();

            // First O3: unused_bytes = 0, multiple row groups → UPDATE mode.
            // Replaces one row group, accumulating dead space > 10% of file.
            execute(
                    """
                            INSERT INTO x(x, s, v, ts) VALUES
                            (9, 'b', 'mno', '2020-01-01T00:30:00.000Z'),
                            (10, 'c', 'pqr', '2020-01-01T01:30:00.000Z')
                            """
            );
            drainWalQueue();

            long partitionTs = parseFloorPartialTimestamp("2020-01-01");
            long versionAfterUpdate = getPartitionNameTxn("x", partitionTs);

            // Second O3: unused_bytes / file_size > 0.1 → REWRITE.
            execute(
                    """
                            INSERT INTO x(x, s, v, ts) VALUES
                            (11, 'a', 'stu', '2020-01-01T04:30:00.000Z'),
                            (12, 'c', 'vwx', '2020-01-01T05:30:00.000Z')
                            """
            );
            drainWalQueue();

            long versionAfterRewrite = getPartitionNameTxn("x", partitionTs);
            Assert.assertNotEquals("partition version should change on REWRITE", versionAfterUpdate, versionAfterRewrite);

            assertSql(
                    """
                            x\ts\tv\tts
                            1\ta\tfoo\t2020-01-01T00:00:00.000000Z
                            9\tb\tmno\t2020-01-01T00:30:00.000000Z
                            2\tb\tbar\t2020-01-01T01:00:00.000000Z
                            10\tc\tpqr\t2020-01-01T01:30:00.000000Z
                            3\ta\tbaz\t2020-01-01T02:00:00.000000Z
                            4\tc\tqux\t2020-01-01T03:00:00.000000Z
                            5\tb\tabc\t2020-01-01T04:00:00.000000Z
                            11\ta\tstu\t2020-01-01T04:30:00.000000Z
                            6\ta\tdef\t2020-01-01T05:00:00.000000Z
                            12\tc\tvwx\t2020-01-01T05:30:00.000000Z
                            7\tc\tghi\t2020-01-01T06:00:00.000000Z
                            8\tb\tjkl\t2020-01-01T07:00:00.000000Z
                            100\td\tend\t2020-01-02T00:00:00.000000Z
                            """,
                    "SELECT * FROM x"
            );
        });
    }

    @Test
    public void testRewriteWithColumnTop() throws Exception {
        // Regression test: after a rewrite-mode O3 merge on a partition with
        // column_top > 0, stale column_top values in the Parquet QDB metadata
        // caused the decoder to skip row groups that now contain actual data.
        // The fix zeroes column_top in the rewritten file so the decoder reads
        // the (null) pages instead of skipping them.
        //
        // Steps:
        // 1. Create table, insert rows, add a new column (column_top > 0).
        // 2. Insert more rows with the new column populated.
        // 3. Convert to Parquet (single row group → rewrite is guaranteed).
        // 4. O3 insert into the Parquet partition, triggering REWRITE.
        // 5. Read back all data — without the fix, the decoder would return
        //    wrong results for the new column in copied row group regions.
        assertMemoryLeak(() -> {
            execute(
                    """
                            CREATE TABLE x (x INT, ts TIMESTAMP)
                            TIMESTAMP(ts) PARTITION BY DAY WAL
                            """
            );
            // Insert 4 rows without the STRING column.
            execute(
                    """
                            INSERT INTO x(x, ts) VALUES
                            (1, '2020-01-01T00:00:00.000Z'),
                            (2, '2020-01-01T06:00:00.000Z'),
                            (3, '2020-01-01T12:00:00.000Z'),
                            (4, '2020-01-01T18:00:00.000Z')
                            """
            );
            // Second partition so 2020-01-01 is not the last.
            execute("INSERT INTO x(x, ts) VALUES (100, '2020-01-02T00:00:00.000Z')");
            drainWalQueue();

            // Add STRING column. column_top = 4 for the 2020-01-01 partition.
            execute("ALTER TABLE x ADD COLUMN s STRING");
            drainWalQueue();

            // Insert 2 rows with s values, still in the same partition.
            execute(
                    """
                            INSERT INTO x(x, s, ts) VALUES
                            (5, 'hello', '2020-01-01T20:00:00.000Z'),
                            (6, 'world', '2020-01-01T22:00:00.000Z')
                            """
            );
            drainWalQueue();

            // 6 rows, default row group size (1000) → single row group.
            // column_top = 4: first 4 rows have s = NULL, last 2 have data.
            execute("ALTER TABLE x CONVERT PARTITION TO PARQUET LIST '2020-01-01'");
            drainWalQueue();

            long partitionTs = parseFloorPartialTimestamp("2020-01-01");
            long versionBeforeO3 = getPartitionNameTxn("x", partitionTs);

            // O3 insert into the Parquet partition.
            // Single row group always triggers a full REWRITE.
            execute(
                    """
                            INSERT INTO x(x, s, ts) VALUES
                            (7, 'mid', '2020-01-01T09:00:00.000Z')
                            """
            );
            drainWalQueue();

            long versionAfterRewrite = getPartitionNameTxn("x", partitionTs);
            Assert.assertNotEquals("partition version should change on REWRITE", versionBeforeO3, versionAfterRewrite);

            // Verify all data is correct. Without the column_top fix, the
            // decoder would see stale column_top and skip pages for 's',
            // returning incorrect NULL values for rows that have actual data.
            assertSql(
                    """
                            x\tts\ts
                            1\t2020-01-01T00:00:00.000000Z\t
                            2\t2020-01-01T06:00:00.000000Z\t
                            7\t2020-01-01T09:00:00.000000Z\tmid
                            3\t2020-01-01T12:00:00.000000Z\t
                            4\t2020-01-01T18:00:00.000000Z\t
                            5\t2020-01-01T20:00:00.000000Z\thello
                            6\t2020-01-01T22:00:00.000000Z\tworld
                            100\t2020-01-02T00:00:00.000000Z\t
                            """,
                    "SELECT * FROM x"
            );
        });
    }

    private long getPartitionNameTxn(String tableName, long partitionTimestamp) {
        try (TableReader reader = getReader(tableName)) {
            return reader.getTxFile().getPartitionNameTxnByPartitionTimestamp(partitionTimestamp);
        }
    }
}
