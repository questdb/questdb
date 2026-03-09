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
    public void testO3AfterAddColumnMultipleRowGroups() throws Exception {
        // Small row group size → multiple row groups. Schema mismatch forces
        // rewrite, exercising copyRowGroupWithNullColumns() for untouched RGs.
        node1.setProperty(PropertyKey.CAIRO_PARTITION_ENCODER_PARQUET_ROW_GROUP_SIZE, 4);
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
                            (2, 'b', '2020-01-01T01:00:00.000Z'),
                            (3, 'a', '2020-01-01T02:00:00.000Z'),
                            (4, 'c', '2020-01-01T03:00:00.000Z'),
                            (5, 'b', '2020-01-01T04:00:00.000Z'),
                            (6, 'a', '2020-01-01T05:00:00.000Z'),
                            (7, 'c', '2020-01-01T06:00:00.000Z'),
                            (8, 'b', '2020-01-01T07:00:00.000Z')
                            """
            );
            execute("INSERT INTO x(x, s, ts) VALUES (100, 'd', '2020-01-02T00:00:00.000Z')");
            drainWalQueue();

            // 8 rows with row group size 4 → 2 row groups.
            execute("ALTER TABLE x CONVERT PARTITION TO PARQUET LIST '2020-01-01'");
            drainWalQueue();

            execute("ALTER TABLE x ADD COLUMN y DOUBLE");
            drainWalQueue();

            // O3 insert overlapping first row group (00:00-03:00).
            // RG0 is merged (null fill for y), RG1 is copied via
            // copyRowGroupWithNullColumns (null column chunk for y).
            execute(
                    """
                            INSERT INTO x(x, s, y, ts) VALUES
                            (9, 'b', 1.5, '2020-01-01T00:30:00.000Z'),
                            (10, 'c', 2.5, '2020-01-01T01:30:00.000Z')
                            """
            );
            drainWalQueue();

            Assert.assertFalse(engine.getTableSequencerAPI().isSuspended(engine.verifyTableName("x")));

            assertSql(
                    """
                            x\ts\tts\ty
                            1\ta\t2020-01-01T00:00:00.000000Z\tnull
                            9\tb\t2020-01-01T00:30:00.000000Z\t1.5
                            2\tb\t2020-01-01T01:00:00.000000Z\tnull
                            10\tc\t2020-01-01T01:30:00.000000Z\t2.5
                            3\ta\t2020-01-01T02:00:00.000000Z\tnull
                            4\tc\t2020-01-01T03:00:00.000000Z\tnull
                            5\tb\t2020-01-01T04:00:00.000000Z\tnull
                            6\ta\t2020-01-01T05:00:00.000000Z\tnull
                            7\tc\t2020-01-01T06:00:00.000000Z\tnull
                            8\tb\t2020-01-01T07:00:00.000000Z\tnull
                            100\td\t2020-01-02T00:00:00.000000Z\tnull
                            """,
                    "SELECT * FROM x"
            );
        });
    }

    @Test
    public void testO3AfterAddColumnRewriteMode() throws Exception {
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

            // O3 insert into the parquet partition.
            // Single row group → always REWRITE. Original rows get NULL for y.
            execute(
                    """
                            INSERT INTO x(x, s, y, ts) VALUES
                            (5, 'b', 1.5, '2020-01-01T03:00:00.000Z'),
                            (6, 'a', 2.5, '2020-01-01T09:00:00.000Z'),
                            (7, 'c', 3.5, '2020-01-01T15:00:00.000Z')
                            """
            );
            drainWalQueue();

            Assert.assertFalse(engine.getTableSequencerAPI().isSuspended(engine.verifyTableName("x")));

            assertSql(
                    """
                            x\ts\tts\ty
                            1\ta\t2020-01-01T00:00:00.000000Z\tnull
                            5\tb\t2020-01-01T03:00:00.000000Z\t1.5
                            2\tb\t2020-01-01T06:00:00.000000Z\tnull
                            6\ta\t2020-01-01T09:00:00.000000Z\t2.5
                            3\ta\t2020-01-01T12:00:00.000000Z\tnull
                            7\tc\t2020-01-01T15:00:00.000000Z\t3.5
                            4\tc\t2020-01-01T18:00:00.000000Z\tnull
                            100\td\t2020-01-02T00:00:00.000000Z\tnull
                            """,
                    "SELECT * FROM x"
            );
        });
    }

    @Test
    public void testO3AfterMultipleAddColumns() throws Exception {
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
                            (2, '2020-01-01T06:00:00.000Z'),
                            (3, '2020-01-01T12:00:00.000Z'),
                            (4, '2020-01-01T18:00:00.000Z')
                            """
            );
            execute("INSERT INTO x(x, ts) VALUES (100, '2020-01-02T00:00:00.000Z')");
            drainWalQueue();

            execute("ALTER TABLE x CONVERT PARTITION TO PARQUET LIST '2020-01-01'");
            drainWalQueue();

            // Add two columns after conversion. The parquet file has 2 columns,
            // the table now has 4.
            execute("ALTER TABLE x ADD COLUMN a DOUBLE");
            execute("ALTER TABLE x ADD COLUMN b VARCHAR");
            drainWalQueue();

            // O3 insert with both new columns.
            execute(
                    """
                            INSERT INTO x(x, a, b, ts) VALUES
                            (5, 1.5, 'hello', '2020-01-01T03:00:00.000Z'),
                            (6, 2.5, 'world', '2020-01-01T09:00:00.000Z')
                            """
            );
            drainWalQueue();

            Assert.assertFalse(engine.getTableSequencerAPI().isSuspended(engine.verifyTableName("x")));

            assertSql(
                    """
                            x\tts\ta\tb
                            1\t2020-01-01T00:00:00.000000Z\tnull\t
                            5\t2020-01-01T03:00:00.000000Z\t1.5\thello
                            2\t2020-01-01T06:00:00.000000Z\tnull\t
                            6\t2020-01-01T09:00:00.000000Z\t2.5\tworld
                            3\t2020-01-01T12:00:00.000000Z\tnull\t
                            4\t2020-01-01T18:00:00.000000Z\tnull\t
                            100\t2020-01-02T00:00:00.000000Z\tnull\t
                            """,
                    "SELECT * FROM x"
            );
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
    public void testReadParquetAfterAddColumn() throws Exception {
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
                            (2, '2020-01-01T06:00:00.000Z'),
                            (3, '2020-01-01T12:00:00.000Z'),
                            (4, '2020-01-01T18:00:00.000Z')
                            """
            );
            execute("INSERT INTO x(x, ts) VALUES (100, '2020-01-02T00:00:00.000Z')");
            drainWalQueue();

            execute("ALTER TABLE x CONVERT PARTITION TO PARQUET LIST '2020-01-01', '2020-01-02'");
            drainWalQueue();

            // Add column after conversion. No O3 — just query.
            execute("ALTER TABLE x ADD COLUMN y DOUBLE");
            drainWalQueue();

            // Insert with the new column into the non-parquet partition.
            execute("INSERT INTO x(x, y, ts) VALUES (5, 1.5, '2020-01-02T06:00:00.000Z')");
            drainWalQueue();

            // Parquet partition returns NULLs for the missing column.
            assertSql(
                    """
                            x\tts\ty
                            1\t2020-01-01T00:00:00.000000Z\tnull
                            2\t2020-01-01T06:00:00.000000Z\tnull
                            3\t2020-01-01T12:00:00.000000Z\tnull
                            4\t2020-01-01T18:00:00.000000Z\tnull
                            100\t2020-01-02T00:00:00.000000Z\tnull
                            5\t2020-01-02T06:00:00.000000Z\t1.5
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
    public void testParquetToNativeWithDeletedColumns() throws Exception {
        // Tests that converting parquet→native correctly maps columns by writer ID
        // when columns have been deleted, causing gaps in the table column indices.
        assertMemoryLeak(() -> {
            // Create table with several column types including VARCHAR
            execute(
                    """
                            CREATE TABLE x (x INT, v VARCHAR, s SYMBOL, ts TIMESTAMP)
                            TIMESTAMP(ts) PARTITION BY DAY WAL
                            """
            );
            execute(
                    """
                            INSERT INTO x(x, v, s, ts) VALUES
                            (1, 'foo', 'a', '2020-01-01T00:00:00.000Z'),
                            (2, 'bar', 'b', '2020-01-01T01:00:00.000Z'),
                            (3, 'baz', 'a', '2020-01-01T02:00:00.000Z'),
                            (4, 'qux', 'c', '2020-01-01T03:00:00.000Z')
                            """
            );
            drainWalQueue();

            // Case 1: remove a column BEFORE converting to parquet.
            // The parquet file won't contain 'x' and its sequential column indices
            // will differ from the table column indices.
            execute("ALTER TABLE x DROP COLUMN x");
            drainWalQueue();

            execute("ALTER TABLE x CONVERT PARTITION TO PARQUET LIST '2020-01-01'");
            drainWalQueue();

            // Convert back to native — this is the operation that was failing
            // because the conversion used parquet sequential indices instead of
            // table column IDs for file naming.
            execute("ALTER TABLE x CONVERT PARTITION TO NATIVE LIST '2020-01-01'");
            drainWalQueue();

            Assert.assertFalse(engine.getTableSequencerAPI().isSuspended(engine.verifyTableName("x")));

            assertSql(
                    """
                            v\ts\tts
                            foo\ta\t2020-01-01T00:00:00.000000Z
                            bar\tb\t2020-01-01T01:00:00.000000Z
                            baz\ta\t2020-01-01T02:00:00.000000Z
                            qux\tc\t2020-01-01T03:00:00.000000Z
                            """,
                    "SELECT * FROM x"
            );

            // Case 2: convert to parquet again, then remove a column AFTER
            // conversion, then convert back. The parquet contains the column
            // but the table metadata marks it deleted, shifting sequential indices.
            execute("ALTER TABLE x CONVERT PARTITION TO PARQUET LIST '2020-01-01'");
            drainWalQueue();

            execute("ALTER TABLE x DROP COLUMN s");
            drainWalQueue();

            execute("ALTER TABLE x CONVERT PARTITION TO NATIVE LIST '2020-01-01'");
            drainWalQueue();

            Assert.assertFalse(engine.getTableSequencerAPI().isSuspended(engine.verifyTableName("x")));

            assertSql(
                    """
                            v\tts
                            foo\t2020-01-01T00:00:00.000000Z
                            bar\t2020-01-01T01:00:00.000000Z
                            baz\t2020-01-01T02:00:00.000000Z
                            qux\t2020-01-01T03:00:00.000000Z
                            """,
                    "SELECT * FROM x"
            );

            // Case 3: convert to parquet, then rename a VARCHAR column, then convert back.
            // The parquet stores the old column name but openPartition expects the new name.
            execute("ALTER TABLE x CONVERT PARTITION TO PARQUET LIST '2020-01-01'");
            drainWalQueue();

            execute("ALTER TABLE x RENAME COLUMN v TO v_renamed");
            drainWalQueue();

            execute("ALTER TABLE x CONVERT PARTITION TO NATIVE LIST '2020-01-01'");
            drainWalQueue();

            Assert.assertFalse(engine.getTableSequencerAPI().isSuspended(engine.verifyTableName("x")));

            assertSql(
                    """
                            v_renamed\tts
                            foo\t2020-01-01T00:00:00.000000Z
                            bar\t2020-01-01T01:00:00.000000Z
                            baz\t2020-01-01T02:00:00.000000Z
                            qux\t2020-01-01T03:00:00.000000Z
                            """,
                    "SELECT * FROM x"
            );
        });
    }

    @Test
    public void testParquetToNativeWithColumnTops() throws Exception {
        // Tests that converting parquet→native correctly zeroes column tops.
        // The parquet decoder materializes NULLs for column-top rows, so the
        // native files contain ALL rows. Without zeroing, the reader offsets
        // into the file incorrectly, producing wrong data.
        assertMemoryLeak(() -> {
            execute(
                    """
                            CREATE TABLE x (ts TIMESTAMP, v VARCHAR, s SYMBOL)
                            TIMESTAMP(ts) PARTITION BY DAY WAL
                            """
            );
            execute(
                    """
                            INSERT INTO x(ts, v, s) VALUES
                            ('2020-01-01T00:00:00.000Z', 'foo', 'a'),
                            ('2020-01-01T01:00:00.000Z', 'bar', 'b'),
                            ('2020-01-01T02:00:00.000Z', 'baz', 'a'),
                            ('2020-01-01T03:00:00.000Z', 'qux', 'c')
                            """
            );
            drainWalQueue();

            // Add columns AFTER data exists, creating non-zero column tops
            execute("ALTER TABLE x ADD COLUMN n INT");
            execute("ALTER TABLE x ADD COLUMN v2 VARCHAR");
            execute("ALTER TABLE x ADD COLUMN s2 SYMBOL");
            drainWalQueue();

            // Insert more rows so the new columns have some non-null data
            execute(
                    """
                            INSERT INTO x(ts, v, s, n, v2, s2) VALUES
                            ('2020-01-01T04:00:00.000Z', 'aaa', 'd', 42, 'hello', 'x'),
                            ('2020-01-01T05:00:00.000Z', 'bbb', 'e', 99, 'world', 'y')
                            """
            );
            drainWalQueue();

            final String expected = """
                    ts\tv\ts\tn\tv2\ts2
                    2020-01-01T00:00:00.000000Z\tfoo\ta\tnull\t\t
                    2020-01-01T01:00:00.000000Z\tbar\tb\tnull\t\t
                    2020-01-01T02:00:00.000000Z\tbaz\ta\tnull\t\t
                    2020-01-01T03:00:00.000000Z\tqux\tc\tnull\t\t
                    2020-01-01T04:00:00.000000Z\taaa\td\t42\thello\tx
                    2020-01-01T05:00:00.000000Z\tbbb\te\t99\tworld\ty
                    """;

            // Verify data before conversion
            assertSql(expected, "SELECT * FROM x");

            // Convert to parquet
            execute("ALTER TABLE x CONVERT PARTITION TO PARQUET LIST '2020-01-01'");
            drainWalQueue();

            // Verify data in parquet format
            assertSql(expected, "SELECT * FROM x");

            // Convert back to native
            execute("ALTER TABLE x CONVERT PARTITION TO NATIVE LIST '2020-01-01'");
            drainWalQueue();

            Assert.assertFalse(engine.getTableSequencerAPI().isSuspended(engine.verifyTableName("x")));

            // Verify data after converting back to native
            assertSql(expected, "SELECT * FROM x");
        });
    }

    @Test
    public void testNativeToParquetRoundTripColumnTopEqualsRowCount() throws Exception {
        // When a column is added to a single-partition table, column_top ==
        // partitionRowCount. The parquet encoder stores column_top = rowCount
        // in the file metadata. The Rust decoder skips columns whose
        // column_top >= row_group_size, producing 0-byte native files.
        // zeroColumnTopsAfterParquetRewrite must NOT zero these column tops,
        // otherwise the subsequent parquet→native conversion opens empty
        // native files and crashes (SIGBUS / AssertionError).
        assertMemoryLeak(() -> {
            execute(
                    """
                            CREATE TABLE x (ts TIMESTAMP, x INT)
                            TIMESTAMP(ts) PARTITION BY DAY WAL
                            """
            );
            execute(
                    """
                            INSERT INTO x(ts, x) VALUES
                            ('2020-01-01T00:00:00.000Z', 1),
                            ('2020-01-01T01:00:00.000Z', 2),
                            ('2020-01-01T02:00:00.000Z', 3),
                            ('2020-01-01T03:00:00.000Z', 4)
                            """
            );
            drainWalQueue();

            // Add columns after all data exists — column_top == 4 == partitionRowCount.
            // Cover various types: fixed (LONG), var-size (VARCHAR), and SYMBOL.
            execute("ALTER TABLE x ADD COLUMN n LONG");
            execute("ALTER TABLE x ADD COLUMN v VARCHAR");
            execute("ALTER TABLE x ADD COLUMN s SYMBOL");
            drainWalQueue();

            final String expected = """
                    ts\tx\tn\tv\ts
                    2020-01-01T00:00:00.000000Z\t1\tnull\t\t
                    2020-01-01T01:00:00.000000Z\t2\tnull\t\t
                    2020-01-01T02:00:00.000000Z\t3\tnull\t\t
                    2020-01-01T03:00:00.000000Z\t4\tnull\t\t
                    """;

            assertSql(expected, "SELECT * FROM x");

            // native → parquet → native round-trip
            execute("ALTER TABLE x CONVERT PARTITION TO PARQUET LIST '2020-01-01'");
            drainWalQueue();
            assertSql(expected, "SELECT * FROM x");

            execute("ALTER TABLE x CONVERT PARTITION TO NATIVE LIST '2020-01-01'");
            drainWalQueue();

            Assert.assertFalse(engine.getTableSequencerAPI().isSuspended(engine.verifyTableName("x")));
            assertSql(expected, "SELECT * FROM x");

            // Second round-trip to verify column tops survive correctly
            execute("ALTER TABLE x CONVERT PARTITION TO PARQUET LIST '2020-01-01'");
            drainWalQueue();

            execute("ALTER TABLE x CONVERT PARTITION TO NATIVE LIST '2020-01-01'");
            drainWalQueue();

            Assert.assertFalse(engine.getTableSequencerAPI().isSuspended(engine.verifyTableName("x")));
            assertSql(expected, "SELECT * FROM x");
        });
    }

    @Test
    public void testNativeToParquetRoundTripNonExistentColumn() throws Exception {
        // When a column is added at a later partition, earlier partitions have
        // no column-version record for it (getColumnTop returns -1). The
        // parquet encoder adds the column as all-NULL with
        // column_top = partitionRowCount. The Rust decoder skips it entirely.
        // zeroColumnTopsAfterParquetRewrite must NOT create a column-version
        // record with column_top = 0 for these columns, otherwise a
        // subsequent parquet→native conversion maps 0-byte files expecting
        // partitionRowCount * elementSize bytes.
        assertMemoryLeak(() -> {
            execute(
                    """
                            CREATE TABLE x (ts TIMESTAMP, x INT)
                            TIMESTAMP(ts) PARTITION BY DAY WAL
                            """
            );
            execute(
                    """
                            INSERT INTO x(ts, x) VALUES
                            ('2020-01-01T00:00:00.000Z', 1),
                            ('2020-01-01T01:00:00.000Z', 2),
                            ('2020-01-01T02:00:00.000Z', 3),
                            ('2020-01-01T03:00:00.000Z', 4)
                            """
            );
            // Insert into a second partition so that it becomes the active one
            execute("INSERT INTO x(ts, x) VALUES ('2020-01-02T00:00:00.000Z', 10)");
            drainWalQueue();

            // Add columns while 2020-01-02 is the active partition.
            // For 2020-01-01, getColumnTop returns -1 (column does not exist).
            execute("ALTER TABLE x ADD COLUMN n LONG");
            execute("ALTER TABLE x ADD COLUMN v VARCHAR");
            execute("ALTER TABLE x ADD COLUMN s SYMBOL");
            drainWalQueue();

            final String expected01 = """
                    ts\tx\tn\tv\ts
                    2020-01-01T00:00:00.000000Z\t1\tnull\t\t
                    2020-01-01T01:00:00.000000Z\t2\tnull\t\t
                    2020-01-01T02:00:00.000000Z\t3\tnull\t\t
                    2020-01-01T03:00:00.000000Z\t4\tnull\t\t
                    """;

            assertSql(expected01, "SELECT * FROM x WHERE ts < '2020-01-02'");

            // native → parquet → native round-trip for 2020-01-01
            execute("ALTER TABLE x CONVERT PARTITION TO PARQUET LIST '2020-01-01'");
            drainWalQueue();
            assertSql(expected01, "SELECT * FROM x WHERE ts < '2020-01-02'");

            execute("ALTER TABLE x CONVERT PARTITION TO NATIVE LIST '2020-01-01'");
            drainWalQueue();

            Assert.assertFalse(engine.getTableSequencerAPI().isSuspended(engine.verifyTableName("x")));
            assertSql(expected01, "SELECT * FROM x WHERE ts < '2020-01-02'");

            // Second round-trip to verify column tops survive correctly
            execute("ALTER TABLE x CONVERT PARTITION TO PARQUET LIST '2020-01-01'");
            drainWalQueue();

            execute("ALTER TABLE x CONVERT PARTITION TO NATIVE LIST '2020-01-01'");
            drainWalQueue();

            Assert.assertFalse(engine.getTableSequencerAPI().isSuspended(engine.verifyTableName("x")));
            assertSql(expected01, "SELECT * FROM x WHERE ts < '2020-01-02'");
        });
    }

    @Test
    public void testNativeToParquetRoundTripMultiDimArrayNulls() throws Exception {
        // A DOUBLE[][] column whose values are arrays of null elements
        // (e.g., [[null,null],[null,null]]) must survive a native→parquet→native
        // round-trip without collapsing into plain NULL.
        assertMemoryLeak(() -> {
            execute(
                    """
                            CREATE TABLE x (ts TIMESTAMP, x INT)
                            TIMESTAMP(ts) PARTITION BY DAY WAL
                            """
            );
            execute(
                    """
                            INSERT INTO x(ts, x) VALUES
                            ('2020-01-01T00:00:00.000Z', 1),
                            ('2020-01-01T01:00:00.000Z', 2),
                            ('2020-01-01T02:00:00.000Z', 3),
                            ('2020-01-01T03:00:00.000Z', 4)
                            """
            );
            drainWalQueue();

            // Add a DOUBLE[][] column with column top = 4
            execute("ALTER TABLE x ADD COLUMN a DOUBLE[][]");
            drainWalQueue();

            // Insert rows where 'a' is an array of null elements
            execute(
                    """
                            INSERT INTO x(ts, x, a) VALUES
                            ('2020-01-01T04:00:00.000Z', 5, ARRAY[[null::double, null::double, null::double], [null::double, null::double, null::double]]),
                            ('2020-01-01T05:00:00.000Z', 6, ARRAY[[1.5, null::double], [null::double, 2.5]]),
                            ('2020-01-01T06:00:00.000Z', 7, ARRAY[[null::double]])
                            """
            );
            drainWalQueue();

            final String expected = """
                    ts\tx\ta
                    2020-01-01T00:00:00.000000Z\t1\tnull
                    2020-01-01T01:00:00.000000Z\t2\tnull
                    2020-01-01T02:00:00.000000Z\t3\tnull
                    2020-01-01T03:00:00.000000Z\t4\tnull
                    2020-01-01T04:00:00.000000Z\t5\t[[null,null,null],[null,null,null]]
                    2020-01-01T05:00:00.000000Z\t6\t[[1.5,null],[null,2.5]]
                    2020-01-01T06:00:00.000000Z\t7\t[[null]]
                    """;

            assertSql(expected, "SELECT * FROM x");

            // native → parquet → native round-trip
            execute("ALTER TABLE x CONVERT PARTITION TO PARQUET LIST '2020-01-01'");
            drainWalQueue();
            assertSql(expected, "SELECT * FROM x");

            execute("ALTER TABLE x CONVERT PARTITION TO NATIVE LIST '2020-01-01'");
            drainWalQueue();

            Assert.assertFalse(engine.getTableSequencerAPI().isSuspended(engine.verifyTableName("x")));
            assertSql(expected, "SELECT * FROM x");
        });
    }

    @Test
    public void testO3IntoParquetWithMultiDimArrayNulls() throws Exception {
        // Reproduces a bug where DOUBLE[][] values consisting of arrays of null
        // elements (e.g., [[null,null,null],...]) collapse to plain NULL after
        // an O3 merge into a parquet partition via the Rust updater.
        assertMemoryLeak(() -> {
            execute(
                    """
                            CREATE TABLE x (ts TIMESTAMP, x INT)
                            TIMESTAMP(ts) PARTITION BY DAY WAL
                            """
            );
            execute(
                    """
                            INSERT INTO x(ts, x) VALUES
                            ('2020-01-01T00:00:00.000Z', 1),
                            ('2020-01-01T01:00:00.000Z', 2),
                            ('2020-01-01T02:00:00.000Z', 3),
                            ('2020-01-01T03:00:00.000Z', 4)
                            """
            );
            drainWalQueue();

            // Add a DOUBLE[][] column — existing rows get column_top = 4
            execute("ALTER TABLE x ADD COLUMN a DOUBLE[][]");
            drainWalQueue();

            // Insert rows with DOUBLE[][] values that contain null elements
            execute(
                    """
                            INSERT INTO x(ts, x, a) VALUES
                            ('2020-01-01T04:00:00.000Z', 5, ARRAY[[null::double, null::double, null::double], [null::double, null::double, null::double], [null::double, null::double, null::double]]),
                            ('2020-01-01T05:00:00.000Z', 6, ARRAY[[1.5, null::double], [null::double, 2.5]]),
                            ('2020-01-01T06:00:00.000Z', 7, ARRAY[[null::double]])
                            """
            );
            drainWalQueue();

            final String expected = """
                    ts\tx\ta
                    2020-01-01T00:00:00.000000Z\t1\tnull
                    2020-01-01T01:00:00.000000Z\t2\tnull
                    2020-01-01T02:00:00.000000Z\t3\tnull
                    2020-01-01T03:00:00.000000Z\t4\tnull
                    2020-01-01T04:00:00.000000Z\t5\t[[null,null,null],[null,null,null],[null,null,null]]
                    2020-01-01T05:00:00.000000Z\t6\t[[1.5,null],[null,2.5]]
                    2020-01-01T06:00:00.000000Z\t7\t[[null]]
                    """;

            assertSql(expected, "SELECT * FROM x");

            // Convert to parquet
            execute("ALTER TABLE x CONVERT PARTITION TO PARQUET LIST '2020-01-01'");
            drainWalQueue();
            assertSql(expected, "SELECT * FROM x");

            // O3 insert into the parquet partition → triggers Rust O3 updater
            execute(
                    """
                            INSERT INTO x(ts, x, a) VALUES
                            ('2020-01-01T02:30:00.000Z', 10, ARRAY[[null::double, null::double, null::double]])
                            """
            );
            drainWalQueue();

            final String expectedAfterO3 = """
                    ts\tx\ta
                    2020-01-01T00:00:00.000000Z\t1\tnull
                    2020-01-01T01:00:00.000000Z\t2\tnull
                    2020-01-01T02:00:00.000000Z\t3\tnull
                    2020-01-01T02:30:00.000000Z\t10\t[[null,null,null]]
                    2020-01-01T03:00:00.000000Z\t4\tnull
                    2020-01-01T04:00:00.000000Z\t5\t[[null,null,null],[null,null,null],[null,null,null]]
                    2020-01-01T05:00:00.000000Z\t6\t[[1.5,null],[null,2.5]]
                    2020-01-01T06:00:00.000000Z\t7\t[[null]]
                    """;

            Assert.assertFalse(engine.getTableSequencerAPI().isSuspended(engine.verifyTableName("x")));
            assertSql(expectedAfterO3, "SELECT * FROM x");
        });
    }

    @Test
    public void testParquetToNativePreservesColumnTopZeroing() throws Exception {
        // Reproduces a bug where convertPartitionParquetToNative materializes
        // ALL rows (including column_top nulls) but does not zero the
        // column_tops in ColumnVersionWriter. A subsequent native→parquet
        // conversion reads the stale column_top, causing data to shift by
        // column_top positions and corrupting array values.
        assertMemoryLeak(() -> {
            execute(
                    """
                            CREATE TABLE x (ts TIMESTAMP, x INT)
                            TIMESTAMP(ts) PARTITION BY DAY WAL
                            """
            );

            // Insert 4 rows into partition 2020-01-01
            execute(
                    """
                            INSERT INTO x(ts, x) VALUES
                            ('2020-01-01T00:00:00.000Z', 1),
                            ('2020-01-01T01:00:00.000Z', 2),
                            ('2020-01-01T02:00:00.000Z', 3),
                            ('2020-01-01T03:00:00.000Z', 4)
                            """
            );
            drainWalQueue();

            // Add a DOUBLE[][] column → column_top = 4 for the existing rows
            execute("ALTER TABLE x ADD COLUMN a DOUBLE[][]");
            drainWalQueue();

            // Insert 4 more rows with actual DOUBLE[][] data
            execute(
                    """
                            INSERT INTO x(ts, x, a) VALUES
                            ('2020-01-01T04:00:00.000Z', 5, ARRAY[[null::double, null::double, null::double], [null::double, null::double, null::double], [null::double, null::double, null::double]]),
                            ('2020-01-01T05:00:00.000Z', 6, ARRAY[[1.5, null::double], [null::double, 2.5]]),
                            ('2020-01-01T06:00:00.000Z', 7, ARRAY[[null::double]]),
                            ('2020-01-01T07:00:00.000Z', 8, ARRAY[[3.0, 4.0]])
                            """
            );
            drainWalQueue();

            final String expected = """
                    ts\tx\ta
                    2020-01-01T00:00:00.000000Z\t1\tnull
                    2020-01-01T01:00:00.000000Z\t2\tnull
                    2020-01-01T02:00:00.000000Z\t3\tnull
                    2020-01-01T03:00:00.000000Z\t4\tnull
                    2020-01-01T04:00:00.000000Z\t5\t[[null,null,null],[null,null,null],[null,null,null]]
                    2020-01-01T05:00:00.000000Z\t6\t[[1.5,null],[null,2.5]]
                    2020-01-01T06:00:00.000000Z\t7\t[[null]]
                    2020-01-01T07:00:00.000000Z\t8\t[[3.0,4.0]]
                    """;

            assertSql(expected, "SELECT * FROM x");

            // Convert to parquet (encodes with column_top=4)
            execute("ALTER TABLE x CONVERT PARTITION TO PARQUET LIST '2020-01-01'");
            drainWalQueue();
            assertSql(expected, "SELECT * FROM x");

            // Convert back to native — decoder materializes ALL rows.
            // Without the fix, column_top stays at 4 in ColumnVersionWriter.
            execute("ALTER TABLE x CONVERT PARTITION TO NATIVE LIST '2020-01-01'");
            drainWalQueue();
            assertSql(expected, "SELECT * FROM x");

            // Convert to parquet again — with the stale column_top, the
            // encoder would read from offset 0 in the native file but skip 4
            // rows, shifting DOUBLE[][] data by 4 positions.
            execute("ALTER TABLE x CONVERT PARTITION TO PARQUET LIST '2020-01-01'");
            drainWalQueue();

            Assert.assertFalse(engine.getTableSequencerAPI().isSuspended(engine.verifyTableName("x")));
            assertSql(expected, "SELECT * FROM x");
        });
    }

    @Test
    public void testO3IntoParquetAfterColumnTopAndConversionCycles() throws Exception {
        // Reproduces the DOUBLE[][] data corruption seen in WalWriterFuzzTest
        // with seeds 286709679787041L, 1772793547818L.
        //
        // Key sequence:
        // 1. Create partition with initial rows (no array column).
        // 2. ADD COLUMN arr DOUBLE[][] → column_top = initial rows.
        // 3. Insert rows with array data.
        // 4. Convert to parquet (column_top > 0 in QDB metadata).
        // 5. Convert back to native (decoder materializes all rows).
        // 6. Insert more rows.
        // 7. Convert to parquet (column_top = 0 now).
        // 8. O3 insert → merge into parquet.
        // 9. Verify array values are preserved.
        assertMemoryLeak(() -> {
            execute(
                    """
                            CREATE TABLE x (ts TIMESTAMP, x INT)
                            TIMESTAMP(ts) PARTITION BY DAY WAL
                            """
            );

            // Insert initial rows into partition 2020-01-01 (timestamps every minute)
            StringBuilder sb = new StringBuilder();
            sb.append("INSERT INTO x(ts, x) VALUES\n");
            for (int i = 0; i < 100; i++) {
                if (i > 0) {
                    sb.append(",\n");
                }
                sb.append("('2020-01-01T").append(String.format("%02d:%02d", i / 60, i % 60))
                        .append(":00.000Z', ").append(i).append(')');
            }
            execute(sb.toString());

            // Insert a row in the next partition so 2020-01-01 is non-active
            execute("INSERT INTO x(ts, x) VALUES ('2020-01-02T00:00:00.000Z', 999)");
            drainWalQueue();

            // Add DOUBLE[][] column → column_top = 100 for partition 2020-01-01
            execute("ALTER TABLE x ADD COLUMN a DOUBLE[][]");
            drainWalQueue();

            // Insert rows with actual DOUBLE[][] data (timestamps after existing max)
            sb = new StringBuilder();
            sb.append("INSERT INTO x(ts, x, a) VALUES\n");
            for (int i = 100; i < 150; i++) {
                if (i > 100) {
                    sb.append(",\n");
                }
                sb.append("('2020-01-01T").append(String.format("%02d:%02d", i / 60, i % 60))
                        .append(":00.000Z', ").append(i)
                        .append(", ARRAY[[null::double, null::double, null::double], [null::double, null::double, null::double], [null::double, null::double, null::double]])");
            }
            execute(sb.toString());
            drainWalQueue();

            // Verify baseline
            String expected100 = "ts\tx\ta\n";
            StringBuilder expectedSb = new StringBuilder(expected100);
            for (int i = 0; i < 100; i++) {
                expectedSb.append("2020-01-01T").append(String.format("%02d:%02d", i / 60, i % 60))
                        .append(":00.000000Z\t").append(i).append("\tnull\n");
            }
            for (int i = 100; i < 150; i++) {
                expectedSb.append("2020-01-01T").append(String.format("%02d:%02d", i / 60, i % 60))
                        .append(":00.000000Z\t").append(i)
                        .append("\t[[null,null,null],[null,null,null],[null,null,null]]\n");
            }
            assertSql(expectedSb.toString(), "SELECT * FROM x WHERE ts >= '2020-01-01' AND ts < '2020-01-02' ORDER BY ts");

            // Step 4: Convert to parquet (column_top=100 for arr)
            execute("ALTER TABLE x CONVERT PARTITION TO PARQUET LIST '2020-01-01'");
            drainWalQueue();
            assertSql(expectedSb.toString(), "SELECT * FROM x WHERE ts >= '2020-01-01' AND ts < '2020-01-02' ORDER BY ts");

            // Step 5: Convert back to native
            execute("ALTER TABLE x CONVERT PARTITION TO NATIVE LIST '2020-01-01'");
            drainWalQueue();
            assertSql(expectedSb.toString(), "SELECT * FROM x WHERE ts >= '2020-01-01' AND ts < '2020-01-02' ORDER BY ts");

            // Step 6: Insert more rows with array data
            sb = new StringBuilder();
            sb.append("INSERT INTO x(ts, x, a) VALUES\n");
            for (int i = 150; i < 200; i++) {
                if (i > 150) {
                    sb.append(",\n");
                }
                sb.append("('2020-01-01T").append(String.format("%02d:%02d", i / 60, i % 60))
                        .append(":00.000Z', ").append(i)
                        .append(", ARRAY[[null::double, null::double, null::double], [null::double, null::double, null::double], [null::double, null::double, null::double]])");
            }
            execute(sb.toString());
            drainWalQueue();

            // Update expected
            for (int i = 150; i < 200; i++) {
                expectedSb.append("2020-01-01T").append(String.format("%02d:%02d", i / 60, i % 60))
                        .append(":00.000000Z\t").append(i)
                        .append("\t[[null,null,null],[null,null,null],[null,null,null]]\n");
            }
            assertSql(expectedSb.toString(), "SELECT * FROM x WHERE ts >= '2020-01-01' AND ts < '2020-01-02' ORDER BY ts");

            // Step 7: Convert to parquet (column_top = 0 now)
            execute("ALTER TABLE x CONVERT PARTITION TO PARQUET LIST '2020-01-01'");
            drainWalQueue();
            assertSql(expectedSb.toString(), "SELECT * FROM x WHERE ts >= '2020-01-01' AND ts < '2020-01-02' ORDER BY ts");

            // Step 8: O3 insert into the parquet partition
            // Insert rows with timestamps that fall BEFORE the existing max,
            // triggering O3 merge with the parquet partition.
            execute(
                    """
                            INSERT INTO x(ts, x, a) VALUES
                            ('2020-01-01T00:00:30.000Z', 1000, ARRAY[[42.0, 43.0]]),
                            ('2020-01-01T01:30:30.000Z', 1001, ARRAY[[44.0]]),
                            ('2020-01-01T02:15:30.000Z', 1002, ARRAY[[null::double, null::double, null::double], [null::double, null::double, null::double], [null::double, null::double, null::double]])
                            """
            );
            drainWalQueue();

            // Step 9: Verify all data
            Assert.assertFalse(engine.getTableSequencerAPI().isSuspended(engine.verifyTableName("x")));

            // Spot-check specific array values that should NOT be null
            assertSql(
                    """
                            ts\tx\ta
                            2020-01-01T01:40:00.000000Z\t100\t[[null,null,null],[null,null,null],[null,null,null]]
                            """,
                    "SELECT * FROM x WHERE x = 100"
            );
            assertSql(
                    """
                            ts\tx\ta
                            2020-01-01T02:29:00.000000Z\t149\t[[null,null,null],[null,null,null],[null,null,null]]
                            """,
                    "SELECT * FROM x WHERE x = 149"
            );
            assertSql(
                    """
                            ts\tx\ta
                            2020-01-01T02:30:00.000000Z\t150\t[[null,null,null],[null,null,null],[null,null,null]]
                            """,
                    "SELECT * FROM x WHERE x = 150"
            );
            assertSql(
                    """
                            ts\tx\ta
                            2020-01-01T02:15:30.000000Z\t1002\t[[null,null,null],[null,null,null],[null,null,null]]
                            """,
                    "SELECT * FROM x WHERE x = 1002"
            );
            assertSql(
                    """
                            ts\tx\ta
                            2020-01-01T00:00:30.000000Z\t1000\t[[42.0,43.0]]
                            """,
                    "SELECT * FROM x WHERE x = 1000"
            );
        });
    }

    @Test
    public void testO3IntoParquetAfterColumnTopMultipleO3Merges() throws Exception {
        // Closer to the actual fuzz failure: exercises multiple O3 merges into
        // parquet after ADD COLUMN with column_top and parquet↔native cycles.
        assertMemoryLeak(() -> {
            execute(
                    """
                            CREATE TABLE x (ts TIMESTAMP, x INT)
                            TIMESTAMP(ts) PARTITION BY DAY WAL
                            """
            );

            // Insert rows with timestamps every second (large enough to have
            // substantial column_top when column is added)
            StringBuilder sb = new StringBuilder("INSERT INTO x(ts, x) VALUES\n");
            for (int i = 0; i < 500; i++) {
                if (i > 0) {
                    sb.append(",\n");
                }
                // Timestamps every 2 seconds to leave gaps for O3
                sb.append("('2020-01-01T00:").append(String.format("%02d:%02d", (i * 2) / 60, (i * 2) % 60))
                        .append(".000Z', ").append(i).append(')');
            }
            execute(sb.toString());

            // Row in next partition
            execute("INSERT INTO x(ts, x) VALUES ('2020-01-02T00:00:00.000Z', -1)");
            drainWalQueue();

            // Convert to parquet, then back to native (cycle 1)
            execute("ALTER TABLE x CONVERT PARTITION TO PARQUET LIST '2020-01-01'");
            drainWalQueue();
            execute("ALTER TABLE x CONVERT PARTITION TO NATIVE LIST '2020-01-01'");
            drainWalQueue();

            // Append more rows
            sb = new StringBuilder("INSERT INTO x(ts, x) VALUES\n");
            for (int i = 500; i < 700; i++) {
                if (i > 500) {
                    sb.append(",\n");
                }
                sb.append("('2020-01-01T00:").append(String.format("%02d:%02d", (i * 2) / 60, (i * 2) % 60))
                        .append(".000Z', ").append(i).append(')');
            }
            execute(sb.toString());
            drainWalQueue();

            // ADD COLUMN with column_top = 700
            execute("ALTER TABLE x ADD COLUMN a DOUBLE[][]");
            drainWalQueue();

            // Insert rows with actual DOUBLE[][] data
            sb = new StringBuilder("INSERT INTO x(ts, x, a) VALUES\n");
            for (int i = 700; i < 900; i++) {
                if (i > 700) {
                    sb.append(",\n");
                }
                sb.append("('2020-01-01T00:").append(String.format("%02d:%02d", (i * 2) / 60, (i * 2) % 60))
                        .append(".000Z', ").append(i)
                        .append(", ARRAY[[null::double, null::double, null::double], [null::double, null::double, null::double], [null::double, null::double, null::double]])");
            }
            execute(sb.toString());
            drainWalQueue();

            // Convert to parquet (column_top = 700)
            execute("ALTER TABLE x CONVERT PARTITION TO PARQUET LIST '2020-01-01'");
            drainWalQueue();

            // Convert back to native
            execute("ALTER TABLE x CONVERT PARTITION TO NATIVE LIST '2020-01-01'");
            drainWalQueue();

            // Append more rows
            sb = new StringBuilder("INSERT INTO x(ts, x, a) VALUES\n");
            for (int i = 900; i < 1100; i++) {
                if (i > 900) {
                    sb.append(",\n");
                }
                sb.append("('2020-01-01T00:").append(String.format("%02d:%02d", (i * 2) / 60, (i * 2) % 60))
                        .append(".000Z', ").append(i)
                        .append(", ARRAY[[null::double, null::double, null::double], [null::double, null::double, null::double], [null::double, null::double, null::double]])");
            }
            execute(sb.toString());
            drainWalQueue();

            // Convert to parquet again (column_top = 0)
            execute("ALTER TABLE x CONVERT PARTITION TO PARQUET LIST '2020-01-01'");
            drainWalQueue();

            // O3 insert #1: overlaps with existing data
            sb = new StringBuilder("INSERT INTO x(ts, x, a) VALUES\n");
            for (int i = 0; i < 200; i++) {
                if (i > 0) {
                    sb.append(",\n");
                }
                // Odd seconds to interleave with even seconds
                sb.append("('2020-01-01T00:").append(String.format("%02d:%02d", (i * 2 + 1) / 60, (i * 2 + 1) % 60))
                        .append(".000Z', ").append(10_000 + i)
                        .append(", ARRAY[[").append(i).append("]])");
            }
            execute(sb.toString());
            drainWalQueue();

            // O3 insert #2: more interleaving
            sb = new StringBuilder("INSERT INTO x(ts, x, a) VALUES\n");
            for (int i = 200; i < 500; i++) {
                if (i > 200) {
                    sb.append(",\n");
                }
                sb.append("('2020-01-01T00:").append(String.format("%02d:%02d", (i * 2 + 1) / 60, (i * 2 + 1) % 60))
                        .append(".000Z', ").append(10_000 + i)
                        .append(", ARRAY[[").append(i).append("]])");
            }
            execute(sb.toString());
            drainWalQueue();

            // Verify
            Assert.assertFalse(engine.getTableSequencerAPI().isSuspended(engine.verifyTableName("x")));

            // Check that array values are correct for rows after column_top
            assertSql(
                    """
                            ts\tx\ta
                            2020-01-01T00:23:20.000000Z\t700\t[[null,null,null],[null,null,null],[null,null,null]]
                            """,
                    "SELECT * FROM x WHERE x = 700"
            );
            assertSql(
                    """
                            ts\tx\ta
                            2020-01-01T00:29:58.000000Z\t899\t[[null,null,null],[null,null,null],[null,null,null]]
                            """,
                    "SELECT * FROM x WHERE x = 899"
            );
            assertSql(
                    """
                            ts\tx\ta
                            2020-01-01T00:30:00.000000Z\t900\t[[null,null,null],[null,null,null],[null,null,null]]
                            """,
                    "SELECT * FROM x WHERE x = 900"
            );
            // Rows before column_top should be null
            assertSql(
                    """
                            ts\tx\ta
                            2020-01-01T00:00:00.000000Z\t0\tnull
                            """,
                    "SELECT * FROM x WHERE x = 0"
            );
            // O3 rows should have their values
            assertSql(
                    """
                            ts\tx\ta
                            2020-01-01T00:00:01.000000Z\t10000\t[[0.0]]
                            """,
                    "SELECT * FROM x WHERE x = 10000"
            );
        });
    }

    @Test
    public void testArrayCorruptionAfterWriterReopenBeforeParquetConversion() throws Exception {
        // Reproduces DOUBLE[][] data corruption from WalWriterFuzzTest
        // (seeds 286709679787041L, 1772793547818L).
        //
        // Matches the fuzz test's exact scenario for partition 2022-02-27:
        //  - 896 initial rows (no array column)
        //  - ADD COLUMN DOUBLE[][] → column_top = 896
        //  - INSERT 202 rows with array data
        //  - Writer closes (engine.releaseInactive)
        //  - Convert partition to parquet (column_top=896)
        //  - Then: native→append→parquet→native→append→parquet→O3 merge
        //
        // Tests both A (writer stays open) and B (writer closes) scenarios
        // to detect if close/reopen introduces corruption.
        assertMemoryLeak(() -> {
            execute(
                    """
                            CREATE TABLE x (ts TIMESTAMP, x INT)
                            TIMESTAMP(ts) PARTITION BY HOUR WAL
                            """
            );

            // Insert 896 rows (matching fuzz test column_top=896)
            // Timestamps: 2020-01-01T00:00:00 through ~2020-01-01T00:14:56 (every second)
            for (int batch = 0; batch < 9; batch++) {
                int lo = batch * 100;
                int hi = Math.min(lo + 100, 896);
                StringBuilder sb = new StringBuilder("INSERT INTO x(ts, x) VALUES\n");
                for (int i = lo; i < hi; i++) {
                    if (i > lo) {
                        sb.append(",\n");
                    }
                    sb.append(String.format("('2020-01-01T00:%02d:%02d.000Z', ", i / 60, i % 60))
                            .append(i).append(')');
                }
                execute(sb.toString());
            }
            // Row in next partition so 2020-01-01T00 is non-active
            execute("INSERT INTO x(ts, x) VALUES ('2020-01-01T01:00:00.000Z', -1)");
            drainWalQueue();
            engine.releaseInactive();

            // ADD COLUMN DOUBLE[][] → column_top = 896
            execute("ALTER TABLE x ADD COLUMN a DOUBLE[][]");
            // INSERT 202 rows with 3x3 null-double arrays (matching fuzz test)
            StringBuilder sb = new StringBuilder("INSERT INTO x(ts, x, a) VALUES\n");
            for (int i = 896; i < 1098; i++) {
                if (i > 896) {
                    sb.append(",\n");
                }
                sb.append(String.format("('2020-01-01T00:%02d:%02d.000Z', ", i / 60, i % 60))
                        .append(i)
                        .append(", ARRAY[[null::double, null::double, null::double], [null::double, null::double, null::double], [null::double, null::double, null::double]])");
            }
            execute(sb.toString());
            // Apply ADD COLUMN + INSERT together (same WAL batch)
            drainWalQueue();

            // Verify baseline before any conversions
            assertSql(
                    """
                            ts\tx\ta
                            2020-01-01T00:14:56.000000Z\t896\t[[null,null,null],[null,null,null],[null,null,null]]
                            """,
                    "SELECT * FROM x WHERE x = 896"
            );
            assertSql(
                    """
                            ts\tx\ta
                            2020-01-01T00:14:57.000000Z\t897\t[[null,null,null],[null,null,null],[null,null,null]]
                            """,
                    "SELECT * FROM x WHERE x = 897"
            );

            // *** Critical point: writer closes before convert to parquet ***
            engine.releaseInactive();

            // Convert to parquet (column_top=896, 1098 total rows)
            execute("ALTER TABLE x CONVERT PARTITION TO PARQUET LIST '2020-01-01T00'");
            drainWalQueue();

            // Check immediately after parquet conversion
            assertSql(
                    """
                            ts\tx\ta
                            2020-01-01T00:14:56.000000Z\t896\t[[null,null,null],[null,null,null],[null,null,null]]
                            """,
                    "SELECT * FROM x WHERE x = 896"
            );
            assertSql(
                    """
                            ts\tx\ta
                            2020-01-01T00:14:57.000000Z\t897\t[[null,null,null],[null,null,null],[null,null,null]]
                            """,
                    "SELECT * FROM x WHERE x = 897"
            );

            // Convert back to native (materializes all 1098 rows)
            execute("ALTER TABLE x CONVERT PARTITION TO NATIVE LIST '2020-01-01T00'");
            drainWalQueue();

            // Check after native conversion
            assertSql(
                    """
                            ts\tx\ta
                            2020-01-01T00:14:56.000000Z\t896\t[[null,null,null],[null,null,null],[null,null,null]]
                            """,
                    "SELECT * FROM x WHERE x = 896"
            );
            assertSql(
                    """
                            ts\tx\ta
                            2020-01-01T00:14:57.000000Z\t897\t[[null,null,null],[null,null,null],[null,null,null]]
                            """,
                    "SELECT * FROM x WHERE x = 897"
            );

            engine.releaseInactive();

            // Append more rows, then convert to parquet again (column_top=0)
            sb = new StringBuilder("INSERT INTO x(ts, x, a) VALUES\n");
            for (int i = 1098; i < 1722; i++) {
                if (i > 1098) {
                    sb.append(",\n");
                }
                sb.append(String.format("('2020-01-01T00:%02d:%02d.000Z', ", i / 60, i % 60))
                        .append(i)
                        .append(", ARRAY[[null::double, null::double, null::double], [null::double, null::double, null::double], [null::double, null::double, null::double]])");
            }
            execute(sb.toString());
            drainWalQueue();
            engine.releaseInactive();

            execute("ALTER TABLE x CONVERT PARTITION TO PARQUET LIST '2020-01-01T00'");
            drainWalQueue();
            engine.releaseInactive();

            execute("ALTER TABLE x CONVERT PARTITION TO NATIVE LIST '2020-01-01T00'");
            drainWalQueue();
            engine.releaseInactive();

            // Append more
            sb = new StringBuilder("INSERT INTO x(ts, x, a) VALUES\n");
            for (int i = 1722; i < 2256; i++) {
                if (i > 1722) {
                    sb.append(",\n");
                }
                sb.append(String.format("('2020-01-01T00:%02d:%02d.000Z', ", i / 60, i % 60))
                        .append(i)
                        .append(", ARRAY[[null::double, null::double, null::double], [null::double, null::double, null::double], [null::double, null::double, null::double]])");
            }
            execute(sb.toString());
            drainWalQueue();
            engine.releaseInactive();

            // Convert to parquet (this is the file the O3 merge will read)
            execute("ALTER TABLE x CONVERT PARTITION TO PARQUET LIST '2020-01-01T00'");
            drainWalQueue();

            // Check the parquet file
            assertSql(
                    """
                            ts\tx\ta
                            2020-01-01T00:14:56.000000Z\t896\t[[null,null,null],[null,null,null],[null,null,null]]
                            """,
                    "SELECT * FROM x WHERE x = 896"
            );
            assertSql(
                    """
                            ts\tx\ta
                            2020-01-01T00:14:57.000000Z\t897\t[[null,null,null],[null,null,null],[null,null,null]]
                            """,
                    "SELECT * FROM x WHERE x = 897"
            );

            engine.releaseInactive();

            // O3 merge into parquet — matches fuzz test's 949-row merge
            sb = new StringBuilder("INSERT INTO x(ts, x, a) VALUES\n");
            for (int i = 0; i < 949; i++) {
                if (i > 0) {
                    sb.append(",\n");
                }
                // Use half-second timestamps to interleave with existing full-second data
                sb.append(String.format("('2020-01-01T00:%02d:%02d.500Z', ", i / 60, i % 60))
                        .append(20_000 + i)
                        .append(", ARRAY[[").append(i).append("]])");
            }
            execute(sb.toString());
            drainWalQueue();

            // Verify after O3 merge
            Assert.assertFalse(engine.getTableSequencerAPI().isSuspended(engine.verifyTableName("x")));

            assertSql(
                    """
                            ts\tx\ta
                            2020-01-01T00:14:56.000000Z\t896\t[[null,null,null],[null,null,null],[null,null,null]]
                            """,
                    "SELECT * FROM x WHERE x = 896"
            );
            assertSql(
                    """
                            ts\tx\ta
                            2020-01-01T00:14:57.000000Z\t897\t[[null,null,null],[null,null,null],[null,null,null]]
                            """,
                    "SELECT * FROM x WHERE x = 897"
            );
            assertSql(
                    """
                            ts\tx\ta
                            2020-01-01T00:00:00.000000Z\t0\tnull
                            """,
                    "SELECT * FROM x WHERE x = 0"
            );
            assertSql(
                    """
                            ts\tx\ta
                            2020-01-01T00:00:00.500000Z\t20000\t[[0.0]]
                            """,
                    "SELECT * FROM x WHERE x = 20000"
            );
        });
    }

    @Test
    public void testArrayCorruptionMinimalRoundTripInSingleWriterSession() throws Exception {
        // Minimal reproducer: just CONVERT TO PARQUET → CONVERT TO NATIVE →
        // CONVERT TO PARQUET in one writer session. No inserts between conversions.
        assertMemoryLeak(() -> {
            execute(
                    """
                            CREATE TABLE x (ts TIMESTAMP, x INT)
                            TIMESTAMP(ts) PARTITION BY HOUR WAL
                            """
            );

            // Insert 896 rows
            for (int batch = 0; batch < 9; batch++) {
                int lo = batch * 100;
                int hi = Math.min(lo + 100, 896);
                StringBuilder sb = new StringBuilder("INSERT INTO x(ts, x) VALUES\n");
                for (int i = lo; i < hi; i++) {
                    if (i > lo) {
                        sb.append(",\n");
                    }
                    sb.append(String.format("('2020-01-01T00:%02d:%02d.000Z', ", i / 60, i % 60))
                            .append(i).append(')');
                }
                execute(sb.toString());
            }
            drainWalQueue();
            engine.releaseInactive();

            // ADD COLUMN DOUBLE[][] → column_top = 896
            execute("ALTER TABLE x ADD COLUMN a DOUBLE[][]");
            StringBuilder sb = new StringBuilder("INSERT INTO x(ts, x, a) VALUES\n");
            for (int i = 896; i < 1098; i++) {
                if (i > 896) {
                    sb.append(",\n");
                }
                sb.append(String.format("('2020-01-01T00:%02d:%02d.000Z', ", i / 60, i % 60))
                        .append(i)
                        .append(", ARRAY[[null::double, null::double, null::double], [null::double, null::double, null::double], [null::double, null::double, null::double]])");
            }
            execute(sb.toString());
            drainWalQueue();
            engine.releaseInactive();

            // Queue just: CONVERT TO PARQUET → CONVERT TO NATIVE → CONVERT TO PARQUET
            execute("ALTER TABLE x CONVERT PARTITION TO PARQUET LIST '2020-01-01T00'");
            execute("ALTER TABLE x CONVERT PARTITION TO NATIVE LIST '2020-01-01T00'");
            execute("ALTER TABLE x CONVERT PARTITION TO PARQUET LIST '2020-01-01T00'");

            drainWalQueue();

            assertSql(
                    """
                            ts\tx\ta
                            2020-01-01T00:14:56.000000Z\t896\t[[null,null,null],[null,null,null],[null,null,null]]
                            """,
                    "SELECT * FROM x WHERE x = 896"
            );
        });
    }

    @Test
    public void testArrayCorruptionMultipleRoundTripsInSingleWriterSession() throws Exception {
        // Reproduces DOUBLE[][] data corruption when multiple parquet↔native
        // round-trips happen in a single writer session without ejection.
        //
        // Bug: WalWriterFuzzTest#testConvertPartitionToParquet
        //      seeds 286709679787041L, 1772793547818L
        // Error: Row 1144 column new_col_8[DOUBLE[][]]
        //        expected:<...[[[null,null,null],[null,null,null],[null,null,null]]]>
        //        but was:<...[null]>
        //
        // The critical difference from testArrayCorruptionAfterWriterReopenBeforeParquetConversion:
        // no engine.releaseInactive() between operations — all conversions
        // and inserts execute in one writer session via a single drainWalQueue().
        assertMemoryLeak(() -> {
            execute(
                    """
                            CREATE TABLE x (ts TIMESTAMP, x INT)
                            TIMESTAMP(ts) PARTITION BY HOUR WAL
                            """
            );

            // Insert 896 rows (matching fuzz test column_top=896)
            for (int batch = 0; batch < 9; batch++) {
                int lo = batch * 100;
                int hi = Math.min(lo + 100, 896);
                StringBuilder sb = new StringBuilder("INSERT INTO x(ts, x) VALUES\n");
                for (int i = lo; i < hi; i++) {
                    if (i > lo) {
                        sb.append(",\n");
                    }
                    sb.append(String.format("('2020-01-01T00:%02d:%02d.000Z', ", i / 60, i % 60))
                            .append(i).append(')');
                }
                execute(sb.toString());
            }
            drainWalQueue();
            engine.releaseInactive();

            // ADD COLUMN DOUBLE[][] → column_top = 896
            execute("ALTER TABLE x ADD COLUMN a DOUBLE[][]");
            // INSERT 202 rows with 3x3 null-double arrays (matching fuzz test)
            StringBuilder sb = new StringBuilder("INSERT INTO x(ts, x, a) VALUES\n");
            for (int i = 896; i < 1098; i++) {
                if (i > 896) {
                    sb.append(",\n");
                }
                sb.append(String.format("('2020-01-01T00:%02d:%02d.000Z', ", i / 60, i % 60))
                        .append(i)
                        .append(", ARRAY[[null::double, null::double, null::double], [null::double, null::double, null::double], [null::double, null::double, null::double]])");
            }
            execute(sb.toString());
            drainWalQueue();
            engine.releaseInactive();

            // Verify baseline
            assertSql(
                    """
                            ts\tx\ta
                            2020-01-01T00:14:57.000000Z\t897\t[[null,null,null],[null,null,null],[null,null,null]]
                            """,
                    "SELECT * FROM x WHERE x = 897"
            );

            // === Queue ALL critical operations for a single writer session ===

            // 1. native→parquet (column_top=896→zeroed to 0)
            execute("ALTER TABLE x CONVERT PARTITION TO PARQUET LIST '2020-01-01T00'");

            // 2. parquet→native (materializes all 1098 rows, column_top=0)
            execute("ALTER TABLE x CONVERT PARTITION TO NATIVE LIST '2020-01-01T00'");

            // 3. Insert 624 more rows (timestamps 00:18:18–00:28:41)
            sb = new StringBuilder("INSERT INTO x(ts, x, a) VALUES\n");
            for (int i = 1098; i < 1722; i++) {
                if (i > 1098) {
                    sb.append(",\n");
                }
                sb.append(String.format("('2020-01-01T00:%02d:%02d.000Z', ", i / 60, i % 60))
                        .append(i)
                        .append(", ARRAY[[null::double, null::double, null::double], [null::double, null::double, null::double], [null::double, null::double, null::double]])");
            }
            execute(sb.toString());

            // 4. native→parquet (1722 rows)
            execute("ALTER TABLE x CONVERT PARTITION TO PARQUET LIST '2020-01-01T00'");

            // 5. parquet→native (1722 rows)
            execute("ALTER TABLE x CONVERT PARTITION TO NATIVE LIST '2020-01-01T00'");

            // 6. Insert 534 more rows (timestamps 00:28:42–00:37:35)
            sb = new StringBuilder("INSERT INTO x(ts, x, a) VALUES\n");
            for (int i = 1722; i < 2256; i++) {
                if (i > 1722) {
                    sb.append(",\n");
                }
                sb.append(String.format("('2020-01-01T00:%02d:%02d.000Z', ", i / 60, i % 60))
                        .append(i)
                        .append(", ARRAY[[null::double, null::double, null::double], [null::double, null::double, null::double], [null::double, null::double, null::double]])");
            }
            execute(sb.toString());

            // 7. native→parquet (2256 rows — corruption manifests here)
            execute("ALTER TABLE x CONVERT PARTITION TO PARQUET LIST '2020-01-01T00'");

            // Apply ALL operations in one writer session
            drainWalQueue();

            // Verify row 897 has 3x3 matrix, not NULL
            assertSql(
                    """
                            ts\tx\ta
                            2020-01-01T00:14:56.000000Z\t896\t[[null,null,null],[null,null,null],[null,null,null]]
                            """,
                    "SELECT * FROM x WHERE x = 896"
            );
            assertSql(
                    """
                            ts\tx\ta
                            2020-01-01T00:14:57.000000Z\t897\t[[null,null,null],[null,null,null],[null,null,null]]
                            """,
                    "SELECT * FROM x WHERE x = 897"
            );
            assertSql(
                    """
                            ts\tx\ta
                            2020-01-01T00:00:00.000000Z\t0\tnull
                            """,
                    "SELECT * FROM x WHERE x = 0"
            );
            assertSql(
                    """
                            ts\tx\ta
                            2020-01-01T00:18:18.000000Z\t1098\t[[null,null,null],[null,null,null],[null,null,null]]
                            """,
                    "SELECT * FROM x WHERE x = 1098"
            );
        });
    }

    private long getPartitionNameTxn(String tableName, long partitionTimestamp) {
        try (TableReader reader = getReader(tableName)) {
            return reader.getTxFile().getPartitionNameTxnByPartitionTimestamp(partitionTimestamp);
        }
    }
}
