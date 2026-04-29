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
import io.questdb.cairo.ParquetMetaFileReader;
import io.questdb.cairo.TableReader;
import io.questdb.cairo.sql.PartitionFormat;
import io.questdb.test.AbstractCairoTest;
import org.junit.Assert;
import org.junit.Test;

/**
 * Integration tests verifying that _pm metadata files are generated correctly
 * alongside parquet files during batch encoding and O3 merge.
 */
public class ParquetMetaWriteTest extends AbstractCairoTest {

    @Test
    public void testParquetMetaFileCreatedOnConvertToParquet() throws Exception {
        assertMemoryLeak(() -> {
            execute(
                    """
                            CREATE TABLE x (x INT, y LONG, ts TIMESTAMP)
                            TIMESTAMP(ts) PARTITION BY DAY WAL
                            """
            );
            execute(
                    """
                            INSERT INTO x(x, y, ts) VALUES
                            (1, 100, '2020-01-01T00:00:00.000Z'),
                            (2, 200, '2020-01-01T01:00:00.000Z'),
                            (3, 300, '2020-01-01T02:00:00.000Z')
                            """
            );
            execute("INSERT INTO x(x, y, ts) VALUES (99, 999, '2020-01-02T00:00:00.000Z')");
            drainWalQueue();

            execute("ALTER TABLE x CONVERT PARTITION TO PARQUET LIST '2020-01-01'");
            drainWalQueue();

            assertHasParquetPartition("x");
            assertParquetMetadata("x", 3);
            assertSql(
                    """
                            x\ty\tts
                            1\t100\t2020-01-01T00:00:00.000000Z
                            2\t200\t2020-01-01T01:00:00.000000Z
                            3\t300\t2020-01-01T02:00:00.000000Z
                            99\t999\t2020-01-02T00:00:00.000000Z
                            """,
                    "SELECT * FROM x"
            );
        });
    }

    @Test
    public void testParquetMetaWithMultipleRowGroups() throws Exception {
        node1.setProperty(PropertyKey.CAIRO_PARTITION_ENCODER_PARQUET_ROW_GROUP_SIZE, 4);
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
                            (8, '2020-01-01T07:00:00.000Z'),
                            (9, '2020-01-01T08:00:00.000Z'),
                            (10, '2020-01-01T09:00:00.000Z')
                            """
            );
            execute("INSERT INTO x(x, ts) VALUES (99, '2020-01-02T00:00:00.000Z')");
            drainWalQueue();

            execute("ALTER TABLE x CONVERT PARTITION TO PARQUET LIST '2020-01-01'");
            drainWalQueue();

            assertHasParquetPartition("x");
            assertSql("count\n10\n", "SELECT count() FROM x WHERE ts >= '2020-01-01' AND ts < '2020-01-02'");
        });
    }

    @Test
    public void testParquetMetaSurvivesRoundTrip() throws Exception {
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
                            (3, '2020-01-01T02:00:00.000Z')
                            """
            );
            execute("INSERT INTO x(x, ts) VALUES (99, '2020-01-02T00:00:00.000Z')");
            drainWalQueue();

            execute("ALTER TABLE x CONVERT PARTITION TO PARQUET LIST '2020-01-01'");
            drainWalQueue();
            assertSql("count\n3\n", "SELECT count() FROM x WHERE ts >= '2020-01-01' AND ts < '2020-01-02'");

            execute("ALTER TABLE x CONVERT PARTITION TO NATIVE LIST '2020-01-01'");
            drainWalQueue();
            assertSql("count\n3\n", "SELECT count() FROM x WHERE ts >= '2020-01-01' AND ts < '2020-01-02'");

            execute("ALTER TABLE x CONVERT PARTITION TO PARQUET LIST '2020-01-01'");
            drainWalQueue();

            assertHasParquetPartition("x");
            assertSql("count\n3\n", "SELECT count() FROM x WHERE ts >= '2020-01-01' AND ts < '2020-01-02'");
        });
    }

    @Test
    public void testParquetMetaCreatedOnO3Rewrite() throws Exception {
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
                            (3, '2020-01-01T02:00:00.000Z')
                            """
            );
            execute("INSERT INTO x(x, ts) VALUES (99, '2020-01-02T00:00:00.000Z')");
            drainWalQueue();

            execute("ALTER TABLE x CONVERT PARTITION TO PARQUET LIST '2020-01-01'");
            drainWalQueue();

            // Single row group → O3 triggers rewrite.
            execute("INSERT INTO x(x, ts) VALUES (50, '2020-01-01T01:30:00.000Z')");
            drainWalQueue();

            assertNotSuspended("x");
            assertHasParquetPartition("x");
            assertParquetMetadata("x", 2);
            assertSql(
                    """
                            x\tts
                            1\t2020-01-01T00:00:00.000000Z
                            2\t2020-01-01T01:00:00.000000Z
                            50\t2020-01-01T01:30:00.000000Z
                            3\t2020-01-01T02:00:00.000000Z
                            99\t2020-01-02T00:00:00.000000Z
                            """,
                    "SELECT * FROM x"
            );
        });
    }

    @Test
    public void testParquetMetaIncrementalUpdateOnO3() throws Exception {
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
                            (8, '2020-01-01T07:00:00.000Z'),
                            (9, '2020-01-01T08:00:00.000Z'),
                            (10, '2020-01-01T09:00:00.000Z'),
                            (11, '2020-01-01T10:00:00.000Z'),
                            (12, '2020-01-01T11:00:00.000Z')
                            """
            );
            execute("INSERT INTO x(x, ts) VALUES (99, '2020-01-02T00:00:00.000Z')");
            drainWalQueue();

            execute("ALTER TABLE x CONVERT PARTITION TO PARQUET LIST '2020-01-01'");
            drainWalQueue();

            // O3 insert triggers incremental update (not rewrite).
            execute("INSERT INTO x(x, ts) VALUES (100, '2020-01-01T04:30:00.000Z')");
            drainWalQueue();

            assertNotSuspended("x");
            assertHasParquetPartition("x");
            assertParquetMetadata("x", 2);
            assertSql(
                    """
                            x\tts
                            1\t2020-01-01T00:00:00.000000Z
                            2\t2020-01-01T01:00:00.000000Z
                            3\t2020-01-01T02:00:00.000000Z
                            4\t2020-01-01T03:00:00.000000Z
                            5\t2020-01-01T04:00:00.000000Z
                            100\t2020-01-01T04:30:00.000000Z
                            6\t2020-01-01T05:00:00.000000Z
                            7\t2020-01-01T06:00:00.000000Z
                            8\t2020-01-01T07:00:00.000000Z
                            9\t2020-01-01T08:00:00.000000Z
                            10\t2020-01-01T09:00:00.000000Z
                            11\t2020-01-01T10:00:00.000000Z
                            12\t2020-01-01T11:00:00.000000Z
                            99\t2020-01-02T00:00:00.000000Z
                            """,
                    "SELECT * FROM x"
            );
        });
    }

    @Test
    public void testParquetMetaIncrementalMultipleO3Merges() throws Exception {
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
            execute("INSERT INTO x(x, ts) VALUES (99, '2020-01-02T00:00:00.000Z')");
            drainWalQueue();

            execute("ALTER TABLE x CONVERT PARTITION TO PARQUET LIST '2020-01-01'");
            drainWalQueue();

            // Three successive O3 merges.
            for (int merge = 0; merge < 3; merge++) {
                String ts = "2020-01-01T0" + merge + ":30:00.000Z";
                execute("INSERT INTO x(x, ts) VALUES (" + (100 + merge) + ", '" + ts + "')");
                drainWalQueue();
                assertNotSuspended("x");
            }

            assertHasParquetPartition("x");
            assertParquetMetadata("x", 2);
            assertSql("count\n11\n", "SELECT count() FROM x WHERE ts >= '2020-01-01' AND ts < '2020-01-02'");
        });
    }

    @Test
    public void testParquetMetaWithMultipleColumnTypes() throws Exception {
        assertMemoryLeak(() -> {
            execute(
                    """
                            CREATE TABLE x (
                                b BOOLEAN,
                                i INT,
                                l LONG,
                                f FLOAT,
                                d DOUBLE,
                                dt DATE,
                                ts TIMESTAMP,
                                s STRING,
                                v VARCHAR
                            ) TIMESTAMP(ts) PARTITION BY DAY WAL
                            """
            );
            execute(
                    """
                            INSERT INTO x VALUES
                            (true, 1, 100, 1.5, 2.5, '2020-01-01', '2020-01-01T00:00:00.000Z', 'hello', 'world'),
                            (false, 2, 200, 3.5, 4.5, '2020-01-02', '2020-01-01T01:00:00.000Z', 'foo', 'bar')
                            """
            );
            execute("INSERT INTO x VALUES (true, 99, 999, 9.9, 9.9, '2020-01-03', '2020-01-02T00:00:00.000Z', 'z', 'z')");
            drainWalQueue();

            execute("ALTER TABLE x CONVERT PARTITION TO PARQUET LIST '2020-01-01'");
            drainWalQueue();

            assertHasParquetPartition("x");
            assertParquetMetadata("x", 9);
            // Note: FLOAT column 'f' renders with DOUBLE precision when read through
            // the _pm decode path because the parquet decoder promotes f32 to f64.
            assertSql(
                    """
                            b\ti\tl\tf\td\tdt\tts\ts\tv
                            true\t1\t100\t1.5\t2.5\t2020-01-01T00:00:00.000Z\t2020-01-01T00:00:00.000000Z\thello\tworld
                            false\t2\t200\t3.5\t4.5\t2020-01-02T00:00:00.000Z\t2020-01-01T01:00:00.000000Z\tfoo\tbar
                            true\t99\t999\t9.9\t9.9\t2020-01-03T00:00:00.000Z\t2020-01-02T00:00:00.000000Z\tz\tz
                            """,
                    "SELECT * FROM x"
            );
        });
    }

    @Test
    public void testParquetMetaWithNullColumns() throws Exception {
        assertMemoryLeak(() -> {
            execute(
                    """
                            CREATE TABLE x (
                                a INT,
                                b STRING,
                                c DOUBLE,
                                ts TIMESTAMP
                            ) TIMESTAMP(ts) PARTITION BY DAY WAL
                            """
            );
            execute(
                    """
                            INSERT INTO x(a, ts) VALUES
                            (1, '2020-01-01T00:00:00.000Z'),
                            (2, '2020-01-01T01:00:00.000Z'),
                            (3, '2020-01-01T02:00:00.000Z')
                            """
            );
            execute("INSERT INTO x(a, ts) VALUES (99, '2020-01-02T00:00:00.000Z')");
            drainWalQueue();

            execute("ALTER TABLE x CONVERT PARTITION TO PARQUET LIST '2020-01-01'");
            drainWalQueue();

            assertHasParquetPartition("x");
            assertParquetMetadata("x", 4);
            assertSql(
                    """
                            a\tb\tc\tts
                            1\t\tnull\t2020-01-01T00:00:00.000000Z
                            2\t\tnull\t2020-01-01T01:00:00.000000Z
                            3\t\tnull\t2020-01-01T02:00:00.000000Z
                            99\t\tnull\t2020-01-02T00:00:00.000000Z
                            """,
                    "SELECT * FROM x"
            );
        });
    }

    private void assertHasParquetPartition(String tableName) throws Exception {
        try (TableReader reader = getReader(tableName)) {
            boolean found = false;
            for (int i = 0; i < reader.getPartitionCount(); i++) {
                if (reader.getPartitionFormat(i) == PartitionFormat.PARQUET) {
                    found = true;
                    break;
                }
            }
            Assert.assertTrue("expected at least one parquet partition in " + tableName, found);
        }
    }

    private void assertNotSuspended(String tableName) {
        Assert.assertFalse(
                "table " + tableName + " should not be suspended",
                engine.getTableSequencerAPI().isSuspended(engine.verifyTableName(tableName))
        );
    }

    private void assertParquetMetadata(String tableName, int expectedColumnCount) {
        try (TableReader reader = getReader(tableName)) {
            for (int i = 0; i < reader.getPartitionCount(); i++) {
                if (reader.getPartitionFormat(i) != PartitionFormat.PARQUET) {
                    continue;
                }
                reader.openPartition(i);
                ParquetMetaFileReader meta = reader
                        .getAndInitParquetPartitionDecoder(i)
                        .metadata();
                Assert.assertTrue("row group count must be > 0", meta.getRowGroupCount() > 0);
                Assert.assertEquals("column count", expectedColumnCount, meta.getColumnCount());
                Assert.assertTrue("parquet file size must be > 0", meta.getParquetFileSize() > 0);
            }
        }
    }
}
