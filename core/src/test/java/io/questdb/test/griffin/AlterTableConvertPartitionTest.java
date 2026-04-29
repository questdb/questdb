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

package io.questdb.test.griffin;

import io.questdb.PropertyKey;
import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.PartitionBy;
import io.questdb.cairo.SymbolMapWriter;
import io.questdb.cairo.TableToken;
import io.questdb.griffin.engine.table.ParquetRowGroupFilter;
import io.questdb.std.FilesFacade;
import io.questdb.std.datetime.microtime.Micros;
import io.questdb.std.str.Path;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.TestTimestampType;
import io.questdb.test.cairo.Overrides;
import io.questdb.test.cairo.TableModel;
import io.questdb.test.std.TestFilesFacadeImpl;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

import static io.questdb.cairo.TableUtils.PARQUET_PARTITION_NAME;

public class AlterTableConvertPartitionTest extends AbstractCairoTest {
    private final TestTimestampType timestampType;

    public AlterTableConvertPartitionTest() {
        this.timestampType = TestUtils.getTimestampType();
    }

    @Test
    public void testConvertAllPartition() throws Exception {
        assertConvertAllPartitionsWithKeyword("partition");
    }


    @Test
    public void testConvertAllPartitions() throws Exception {
        assertConvertAllPartitionsWithKeyword("partitions");
    }

    @Test
    public void testConvertAllPartitionsToParquetAndBack() throws Exception {
        assertMemoryLeak(TestFilesFacadeImpl.INSTANCE, () -> {
            final String tableName = "testConvertAllPartitionsToParquetAndBack";
            createTableStr(
                    "INSERT INTO " + tableName + " VALUES(1, 'abc', '2024-06-10T00:00:00.000000Z')",
                    "INSERT INTO " + tableName + " VALUES(2, 'edf', '2024-06-11T00:00:00.000000Z')",
                    "INSERT INTO " + tableName + " VALUES(3, 'abc', '2024-06-12T00:00:00.000000Z')",
                    "INSERT INTO " + tableName + " VALUES(4, 'edf', '2024-06-12T00:00:01.000000Z')",
                    "INSERT INTO " + tableName + " VALUES(5, 'abc', '2024-06-15T00:00:00.000000Z')",
                    "INSERT INTO " + tableName + " VALUES(6, 'edf', '2024-06-12T00:00:02.000000Z')"
            );

            execute("ALTER TABLE " + tableName + " CONVERT PARTITION TO PARQUET WHERE timestamp > 0");

            assertPartitionExists(tableName, "2024-06-10.8");
            assertPartitionExists(tableName, "2024-06-11.6");
            assertPartitionExists(tableName, "2024-06-12.7");

            execute("ALTER TABLE " + tableName + " CONVERT PARTITION TO NATIVE WHERE timestamp > 0");
            assertPartitionDoesNotExist(tableName, "2024-06-10.12");
            assertPartitionDoesNotExist(tableName, "2024-06-11.11");
            assertPartitionDoesNotExist(tableName, "2024-06-12.10");
            assertSql(
                    replaceTimestampSuffix("""
                            id\tstr\ttimestamp
                            1\tabc\t2024-06-10T00:00:00.000000Z
                            2\tedf\t2024-06-11T00:00:00.000000Z
                            3\tabc\t2024-06-12T00:00:00.000000Z
                            4\tedf\t2024-06-12T00:00:01.000000Z
                            6\tedf\t2024-06-12T00:00:02.000000Z
                            5\tabc\t2024-06-15T00:00:00.000000Z
                            """, timestampType.getTypeName()),
                    "select * from " + tableName
            );
        });
    }

    @Test
    public void testConvertAllPartitionsWithBloomFilterColumns() throws Exception {
        assertMemoryLeak(TestFilesFacadeImpl.INSTANCE, () -> {
            final String tableName = "testConvertWithBloom";
            createTable(
                    tableName,
                    "INSERT INTO " + tableName + " VALUES(1, '2024-06-10T00:00:00.000000Z')",
                    "INSERT INTO " + tableName + " VALUES(2, '2024-06-11T00:00:00.000000Z')",
                    "INSERT INTO " + tableName + " VALUES(3, '2024-06-12T00:00:00.000000Z')"
            );

            execute("ALTER TABLE " + tableName + " CONVERT PARTITION TO PARQUET LIST '2024-06-10' WITH (bloom_filter_columns = 'id')");
            assertPartitionExists(tableName, "2024-06-10.3");
        });
    }

    @Test
    public void testConvertAllPartitionsWithBloomFilterColumnsAndFpp() throws Exception {
        assertMemoryLeak(TestFilesFacadeImpl.INSTANCE, () -> {
            final String tableName = "testConvertWithBloomAndFpp";
            createTable(
                    tableName,
                    "INSERT INTO " + tableName + " VALUES(1, '2024-06-10T00:00:00.000000Z')",
                    "INSERT INTO " + tableName + " VALUES(2, '2024-06-11T00:00:00.000000Z')",
                    "INSERT INTO " + tableName + " VALUES(3, '2024-06-12T00:00:00.000000Z')"
            );

            execute("ALTER TABLE " + tableName + " CONVERT PARTITION TO PARQUET LIST '2024-06-10' WITH (bloom_filter_columns = 'id', fpp = '0.05')");
            assertPartitionExists(tableName, "2024-06-10.3");
        });
    }

    @Test
    public void testConvertAllPartitionsWithBloomFilterFpp() throws Exception {
        assertMemoryLeak(TestFilesFacadeImpl.INSTANCE, () -> {
            final String tableName = "testConvertWithFpp";
            createTable(
                    tableName,
                    "INSERT INTO " + tableName + " VALUES(1, '2024-06-10T00:00:00.000000Z')",
                    "INSERT INTO " + tableName + " VALUES(2, '2024-06-11T00:00:00.000000Z')",
                    "INSERT INTO " + tableName + " VALUES(3, '2024-06-12T00:00:00.000000Z')"
            );

            execute("ALTER TABLE " + tableName + " CONVERT PARTITION TO PARQUET LIST '2024-06-10' WITH (fpp = '0.01')");
            assertPartitionExists(tableName, "2024-06-10.3");
        });
    }

    @Test
    public void testConvertAllPartitionsWithBloomFilterWhereClause() throws Exception {
        assertMemoryLeak(TestFilesFacadeImpl.INSTANCE, () -> {
            final String tableName = "testConvertWithBloomWhere";
            createTable(
                    tableName,
                    "INSERT INTO " + tableName + " VALUES(1, '2024-06-10T00:00:00.000000Z')",
                    "INSERT INTO " + tableName + " VALUES(2, '2024-06-11T00:00:00.000000Z')",
                    "INSERT INTO " + tableName + " VALUES(3, '2024-06-12T00:00:00.000000Z')",
                    "INSERT INTO " + tableName + " VALUES(4, '2024-06-15T00:00:00.000000Z')"
            );

            execute("ALTER TABLE " + tableName + " CONVERT PARTITION TO PARQUET WHERE timestamp > 0 WITH (bloom_filter_columns = 'id', fpp = '0.1')");
            assertPartitionExists(tableName, "2024-06-10.6");
            assertPartitionExists(tableName, "2024-06-11.4");
            assertPartitionExists(tableName, "2024-06-12.5");
        });
    }

    @Test
    public void testConvertAllPartitionsWithBloomFilterWithClauseErrors() throws Exception {
        assertMemoryLeak(TestFilesFacadeImpl.INSTANCE, () -> {
            final String tableName = "x";
            createTable(
                    tableName,
                    "INSERT INTO " + tableName + " VALUES(1, '2024-06-10T00:00:00.000000Z')",
                    "INSERT INTO " + tableName + " VALUES(2, '2024-06-11T00:00:00.000000Z')"
            );

            // missing '('
            assertException(
                    "ALTER TABLE x CONVERT PARTITION TO PARQUET LIST '2024-06-10' WITH bloom_filter_columns",
                    66,
                    "'(' expected"
            );

            // unknown option
            assertException(
                    "ALTER TABLE x CONVERT PARTITION TO PARQUET LIST '2024-06-10' WITH (unknown_option = 'val')",
                    67,
                    "bloom_filter_columns or fpp expected"
            );

            // missing '=' after bloom_filter_columns
            assertException(
                    "ALTER TABLE x CONVERT PARTITION TO PARQUET LIST '2024-06-10' WITH (bloom_filter_columns 'id')",
                    88,
                    "'=' expected"
            );

            // missing '=' after fpp
            assertException(
                    "ALTER TABLE x CONVERT PARTITION TO PARQUET LIST '2024-06-10' WITH (fpp '0.05')",
                    71,
                    "'=' expected"
            );

            // fpp = 0 (out of range)
            assertException(
                    "ALTER TABLE x CONVERT PARTITION TO PARQUET LIST '2024-06-10' WITH (fpp = 0)",
                    73,
                    "fpp must be between 0 and 1 (exclusive)"
            );

            // fpp = 1 (out of range)
            assertException(
                    "ALTER TABLE x CONVERT PARTITION TO PARQUET LIST '2024-06-10' WITH (fpp = 1)",
                    73,
                    "fpp must be between 0 and 1 (exclusive)"
            );

            // fpp = 1.5 (out of range)
            assertException(
                    "ALTER TABLE x CONVERT PARTITION TO PARQUET LIST '2024-06-10' WITH (fpp = '1.5')",
                    73,
                    "fpp must be between 0 and 1 (exclusive)"
            );

            // fpp = -0.1 (negative, out of range)
            assertException(
                    "ALTER TABLE x CONVERT PARTITION TO PARQUET LIST '2024-06-10' WITH (fpp = '-0.1')",
                    73,
                    "fpp must be between 0 and 1 (exclusive)"
            );

            // fpp = non-numeric
            assertException(
                    "ALTER TABLE x CONVERT PARTITION TO PARQUET LIST '2024-06-10' WITH (fpp = abc)",
                    73,
                    "invalid fpp value"
            );

            // non-existent bloom filter column
            assertException(
                    "ALTER TABLE x CONVERT PARTITION TO PARQUET LIST '2024-06-10' WITH (bloom_filter_columns = 'nonexistent')",
                    90,
                    "bloom_filter_columns contains non-existent column: nonexistent"
            );

            // bad delimiter (missing ',' or ')')
            assertException(
                    "ALTER TABLE x CONVERT PARTITION TO PARQUET LIST '2024-06-10' WITH (bloom_filter_columns = 'id' fpp = 0.05)",
                    95,
                    "',' or ')' expected"
            );
        });
    }

    // ============================================================
    // Metadata-driven bloom filter conversion tests (A1-A4, B1, C1-C2, D1-D2, H1)
    // ============================================================

    @Test
    public void testMetadataBloomFilterBasic() throws Exception {
        assertMemoryLeak(TestFilesFacadeImpl.INSTANCE, () -> {
            execute("""
                    CREATE TABLE x (
                        val INT PARQUET(BLOOM_FILTER),
                        ts TIMESTAMP
                    ) TIMESTAMP(ts) PARTITION BY DAY
                    """);
            execute("""
                    INSERT INTO x VALUES
                    (1, '2024-01-01T00:00:00.000000Z'),
                    (50_000, '2024-01-01T01:00:00.000000Z'),
                    (100_000, '2024-01-01T02:00:00.000000Z'),
                    (100_001, '2024-01-02T01:00:00.000000Z')
                    """);

            // Convert WITHOUT explicit bloom_filter_columns — should use metadata
            execute("ALTER TABLE x CONVERT PARTITION TO PARQUET WHERE ts >= 0");

            // Verify bloom filter is active by querying for a non-existent value
            ParquetRowGroupFilter.resetRowGroupsSkipped();
            assertQueryNoLeakCheck("val\n", "SELECT val FROM x WHERE val = 25_000", null, true, false);
            Assert.assertTrue("metadata bloom filter should enable row group skipping",
                    ParquetRowGroupFilter.getRowGroupsSkipped() > 0);

            // Verify existing value is still found
            assertQueryNoLeakCheck("val\n50000\n", "SELECT val FROM x WHERE val = 50_000", null, true, false);
        });
    }

    @Test
    public void testMetadataBloomFilterSelectiveColumns() throws Exception {
        assertMemoryLeak(TestFilesFacadeImpl.INSTANCE, () -> {
            execute("""
                    CREATE TABLE x (
                        a INT PARQUET(BLOOM_FILTER),
                        b INT,
                        ts TIMESTAMP
                    ) TIMESTAMP(ts) PARTITION BY DAY
                    """);
            execute("""
                    INSERT INTO x VALUES
                    (1, 1, '2024-01-01T00:00:00.000000Z'),
                    (50_000, 50_000, '2024-01-01T01:00:00.000000Z'),
                    (100_000, 100_000, '2024-01-01T02:00:00.000000Z'),
                    (100_001, 100_001, '2024-01-02T01:00:00.000000Z')
                    """);

            execute("ALTER TABLE x CONVERT PARTITION TO PARQUET WHERE ts >= 0");

            // Column 'a' has bloom filter — row groups should be skipped
            ParquetRowGroupFilter.resetRowGroupsSkipped();
            assertQueryNoLeakCheck("a\n", "SELECT a FROM x WHERE a = 25_000", null, true, false);
            long skippedWithBloom = ParquetRowGroupFilter.getRowGroupsSkipped();
            Assert.assertTrue("column with BLOOM_FILTER should skip row groups", skippedWithBloom > 0);
        });
    }

    @Test
    public void testMetadataBloomFilterNoColumns() throws Exception {
        assertMemoryLeak(TestFilesFacadeImpl.INSTANCE, () -> {
            execute("""
                    CREATE TABLE x (
                        val INT,
                        ts TIMESTAMP
                    ) TIMESTAMP(ts) PARTITION BY DAY
                    """);
            execute("""
                    INSERT INTO x VALUES
                    (1, '2024-01-01T00:00:00.000000Z'),
                    (2, '2024-01-02T01:00:00.000000Z')
                    """);

            // No bloom filter columns — should convert without error
            execute("ALTER TABLE x CONVERT PARTITION TO PARQUET WHERE ts >= 0");

            assertQueryNoLeakCheck("""
                    val
                    1
                    """, "SELECT val FROM x WHERE val = 1", null, true, false);
        });
    }

    @Test
    public void testMetadataBloomFilterWithEncodingAndCompression() throws Exception {
        assertMemoryLeak(TestFilesFacadeImpl.INSTANCE, () -> {
            execute("""
                    CREATE TABLE x (
                        val INT PARQUET(DELTA_BINARY_PACKED, ZSTD(3), BLOOM_FILTER),
                        ts TIMESTAMP
                    ) TIMESTAMP(ts) PARTITION BY DAY
                    """);
            execute("""
                    INSERT INTO x VALUES
                    (1, '2024-01-01T00:00:00.000000Z'),
                    (50_000, '2024-01-01T01:00:00.000000Z'),
                    (100_000, '2024-01-01T02:00:00.000000Z'),
                    (100_001, '2024-01-02T01:00:00.000000Z')
                    """);

            execute("ALTER TABLE x CONVERT PARTITION TO PARQUET WHERE ts >= 0");

            ParquetRowGroupFilter.resetRowGroupsSkipped();
            assertQueryNoLeakCheck("val\n", "SELECT val FROM x WHERE val = 25_000", null, true, false);
            Assert.assertTrue("bloom filter with custom encoding should still skip row groups",
                    ParquetRowGroupFilter.getRowGroupsSkipped() > 0);
        });
    }

    @Test
    public void testMetadataBloomFilterExplicitOverride() throws Exception {
        assertMemoryLeak(TestFilesFacadeImpl.INSTANCE, () -> {
            // Column 'a' has bloom filter in metadata, column 'b' does not
            execute("""
                    CREATE TABLE x (
                        a INT PARQUET(BLOOM_FILTER),
                        b INT,
                        ts TIMESTAMP
                    ) TIMESTAMP(ts) PARTITION BY DAY
                    """);
            execute("""
                    INSERT INTO x VALUES
                    (1, 1, '2024-01-01T00:00:00.000000Z'),
                    (50_000, 50_000, '2024-01-01T01:00:00.000000Z'),
                    (100_000, 100_000, '2024-01-01T02:00:00.000000Z'),
                    (100_001, 100_001, '2024-01-02T01:00:00.000000Z')
                    """);

            // Explicit override: bloom filter on 'b' only, ignoring metadata on 'a'
            execute("ALTER TABLE x CONVERT PARTITION TO PARQUET WHERE ts >= 0 WITH (bloom_filter_columns = 'b')");

            // Column 'b' should have bloom filter (from explicit override)
            ParquetRowGroupFilter.resetRowGroupsSkipped();
            assertQueryNoLeakCheck("b\n", "SELECT b FROM x WHERE b = 25_000", null, true, false);
            Assert.assertTrue("explicit bloom_filter_columns should override metadata",
                    ParquetRowGroupFilter.getRowGroupsSkipped() > 0);
        });
    }

    @Test
    public void testMetadataBloomFilterSetViaDdlThenConvert() throws Exception {
        assertMemoryLeak(TestFilesFacadeImpl.INSTANCE, () -> {
            execute("""
                    CREATE TABLE x (
                        val INT,
                        ts TIMESTAMP
                    ) TIMESTAMP(ts) PARTITION BY DAY
                    """);
            execute("""
                    INSERT INTO x VALUES
                    (1, '2024-01-01T00:00:00.000000Z'),
                    (50_000, '2024-01-01T01:00:00.000000Z'),
                    (100_000, '2024-01-01T02:00:00.000000Z'),
                    (100_001, '2024-01-02T01:00:00.000000Z')
                    """);

            // Add bloom filter via ALTER TABLE, then convert
            execute("ALTER TABLE x ALTER COLUMN val SET PARQUET(BLOOM_FILTER)");
            execute("ALTER TABLE x CONVERT PARTITION TO PARQUET WHERE ts >= 0");

            ParquetRowGroupFilter.resetRowGroupsSkipped();
            assertQueryNoLeakCheck("val\n", "SELECT val FROM x WHERE val = 25_000", null, true, false);
            Assert.assertTrue("bloom filter set via ALTER TABLE should be used during conversion",
                    ParquetRowGroupFilter.getRowGroupsSkipped() > 0);
        });
    }

    @Test
    public void testMetadataBloomFilterClearViaDdlThenConvert() throws Exception {
        assertMemoryLeak(TestFilesFacadeImpl.INSTANCE, () -> {
            execute("""
                    CREATE TABLE x (
                        val INT PARQUET(BLOOM_FILTER),
                        ts TIMESTAMP
                    ) TIMESTAMP(ts) PARTITION BY DAY
                    """);
            execute("""
                    INSERT INTO x VALUES
                    (1, '2024-01-01T00:00:00.000000Z'),
                    (50_000, '2024-01-01T01:00:00.000000Z'),
                    (100_000, '2024-01-01T02:00:00.000000Z'),
                    (100_001, '2024-01-02T01:00:00.000000Z')
                    """);

            // Clear bloom filter, then convert
            execute("ALTER TABLE x ALTER COLUMN val SET PARQUET(default)");
            execute("ALTER TABLE x CONVERT PARTITION TO PARQUET WHERE ts >= 0");

            // With bloom filter cleared, row group skipping should not happen via bloom filter
            // (min/max statistics may still skip some groups, so we just verify no error)
            assertQueryNoLeakCheck("val\n50000\n", "SELECT val FROM x WHERE val = 50_000", null, true, false);
        });
    }

    @Test
    public void testMetadataBloomFilterWithDeletedColumn() throws Exception {
        assertMemoryLeak(TestFilesFacadeImpl.INSTANCE, () -> {
            execute("""
                    CREATE TABLE x (
                        a INT PARQUET(BLOOM_FILTER),
                        b INT PARQUET(BLOOM_FILTER),
                        ts TIMESTAMP
                    ) TIMESTAMP(ts) PARTITION BY DAY
                    """);
            execute("""
                    INSERT INTO x VALUES
                    (1, 1, '2024-01-01T00:00:00.000000Z'),
                    (50_000, 50_000, '2024-01-01T01:00:00.000000Z'),
                    (100_000, 100_000, '2024-01-01T02:00:00.000000Z'),
                    (100_001, 100_001, '2024-01-02T01:00:00.000000Z')
                    """);

            // Drop column 'a', then convert — bloom filter on 'b' should still work
            execute("ALTER TABLE x DROP COLUMN a");
            execute("ALTER TABLE x CONVERT PARTITION TO PARQUET WHERE ts >= 0");

            ParquetRowGroupFilter.resetRowGroupsSkipped();
            assertQueryNoLeakCheck("b\n", "SELECT b FROM x WHERE b = 25_000", null, true, false);
            Assert.assertTrue("bloom filter on remaining column should work after sibling column dropped",
                    ParquetRowGroupFilter.getRowGroupsSkipped() > 0);
        });
    }

    @Test
    public void testMetadataBloomFilterWithColumnTop() throws Exception {
        assertMemoryLeak(TestFilesFacadeImpl.INSTANCE, () -> {
            execute("""
                    CREATE TABLE x (
                        a INT,
                        ts TIMESTAMP
                    ) TIMESTAMP(ts) PARTITION BY DAY
                    """);
            execute("""
                    INSERT INTO x VALUES
                    (1, '2024-01-01T00:00:00.000000Z'),
                    (2, '2024-01-01T01:00:00.000000Z')
                    """);

            // Add column with bloom filter after initial data — creates a column top
            execute("ALTER TABLE x ADD COLUMN b INT");
            execute("ALTER TABLE x ALTER COLUMN b SET PARQUET(BLOOM_FILTER)");
            execute("""
                    INSERT INTO x VALUES
                    (3, '2024-01-01T02:00:00.000000Z', 50_000),
                    (4, '2024-01-02T01:00:00.000000Z', 100_000)
                    """);

            // Convert — column 'b' has a column top in the first partition
            execute("ALTER TABLE x CONVERT PARTITION TO PARQUET WHERE ts >= 0");

            // Verify data is correct (nulls for column top rows)
            assertSql("""
                    a\tts\tb
                    1\t2024-01-01T00:00:00.000000Z\tnull
                    2\t2024-01-01T01:00:00.000000Z\tnull
                    3\t2024-01-01T02:00:00.000000Z\t50000
                    """, "SELECT * FROM x WHERE ts < '2024-01-02'");
        });
    }

    @Test
    public void testMetadataBloomFilterSymbolColumn() throws Exception {
        assertMemoryLeak(TestFilesFacadeImpl.INSTANCE, () -> {
            execute("""
                    CREATE TABLE x (
                        s SYMBOL PARQUET(BLOOM_FILTER),
                        ts TIMESTAMP
                    ) TIMESTAMP(ts) PARTITION BY DAY
                    """);
            execute("""
                    INSERT INTO x VALUES
                    ('aaa', '2024-01-01T00:00:00.000000Z'),
                    ('bbb', '2024-01-01T01:00:00.000000Z'),
                    ('ccc', '2024-01-01T02:00:00.000000Z'),
                    ('ddd', '2024-01-02T01:00:00.000000Z')
                    """);

            execute("ALTER TABLE x CONVERT PARTITION TO PARQUET WHERE ts >= 0");

            ParquetRowGroupFilter.resetRowGroupsSkipped();
            assertQueryNoLeakCheck("s\n", "SELECT s FROM x WHERE s = 'zzz'", null, true, false);
            Assert.assertTrue("bloom filter on SYMBOL column should enable row group skipping",
                    ParquetRowGroupFilter.getRowGroupsSkipped() > 0);

            assertQueryNoLeakCheck("s\naaa\n", "SELECT s FROM x WHERE s = 'aaa'", null, true, false);
        });
    }

    @Test
    public void testMetadataBloomFilterSurvivesNativeRoundTrip() throws Exception {
        assertMemoryLeak(TestFilesFacadeImpl.INSTANCE, () -> {
            execute("""
                    CREATE TABLE x (
                        val INT PARQUET(BLOOM_FILTER),
                        ts TIMESTAMP
                    ) TIMESTAMP(ts) PARTITION BY DAY
                    """);
            execute("""
                    INSERT INTO x VALUES
                    (1, '2024-01-01T00:00:00.000000Z'),
                    (50_000, '2024-01-01T01:00:00.000000Z'),
                    (100_000, '2024-01-01T02:00:00.000000Z'),
                    (100_001, '2024-01-02T01:00:00.000000Z')
                    """);

            // Convert to parquet, back to native, then re-convert
            execute("ALTER TABLE x CONVERT PARTITION TO PARQUET WHERE ts >= 0");
            execute("ALTER TABLE x CONVERT PARTITION TO NATIVE WHERE ts >= 0");
            execute("ALTER TABLE x CONVERT PARTITION TO PARQUET WHERE ts >= 0");

            // Bloom filter metadata should still be active after round-trip
            ParquetRowGroupFilter.resetRowGroupsSkipped();
            assertQueryNoLeakCheck("val\n", "SELECT val FROM x WHERE val = 25_000", null, true, false);
            Assert.assertTrue("bloom filter should survive parquet→native→parquet cycle",
                    ParquetRowGroupFilter.getRowGroupsSkipped() > 0);
        });
    }

    @Test
    public void testMetadataBloomFilterWithNulls() throws Exception {
        assertMemoryLeak(TestFilesFacadeImpl.INSTANCE, () -> {
            execute("""
                    CREATE TABLE x (
                        val INT PARQUET(BLOOM_FILTER),
                        ts TIMESTAMP
                    ) TIMESTAMP(ts) PARTITION BY DAY
                    """);
            execute("""
                    INSERT INTO x VALUES
                    (NULL, '2024-01-01T00:00:00.000000Z'),
                    (50_000, '2024-01-01T01:00:00.000000Z'),
                    (NULL, '2024-01-01T02:00:00.000000Z'),
                    (100_001, '2024-01-02T01:00:00.000000Z')
                    """);

            execute("ALTER TABLE x CONVERT PARTITION TO PARQUET WHERE ts >= 0");

            // Bloom filter should still skip row groups for absent values
            ParquetRowGroupFilter.resetRowGroupsSkipped();
            assertQueryNoLeakCheck("val\n", "SELECT val FROM x WHERE val = 25_000", null, true, false);
            Assert.assertTrue("bloom filter should handle NULL values and still skip row groups",
                    ParquetRowGroupFilter.getRowGroupsSkipped() > 0);

            // Present value should still be found
            assertQueryNoLeakCheck("val\n50000\n", "SELECT val FROM x WHERE val = 50_000", null, true, false);
        });
    }

    @Test
    public void testMetadataBloomFilterWithEmptyStrings() throws Exception {
        assertMemoryLeak(TestFilesFacadeImpl.INSTANCE, () -> {
            execute("""
                    CREATE TABLE x (
                        val VARCHAR PARQUET(BLOOM_FILTER),
                        ts TIMESTAMP
                    ) TIMESTAMP(ts) PARTITION BY DAY
                    """);
            execute("""
                    INSERT INTO x VALUES
                    ('', '2024-01-01T00:00:00.000000Z'),
                    ('hello', '2024-01-01T01:00:00.000000Z'),
                    ('world', '2024-01-01T02:00:00.000000Z'),
                    ('test', '2024-01-02T01:00:00.000000Z')
                    """);

            execute("ALTER TABLE x CONVERT PARTITION TO PARQUET WHERE ts >= 0");

            // Bloom filter should skip row groups for absent values
            ParquetRowGroupFilter.resetRowGroupsSkipped();
            assertQueryNoLeakCheck("val\n", "SELECT val FROM x WHERE val = 'nonexistent'", null, true, false);
            Assert.assertTrue("bloom filter should handle empty strings and skip row groups for absent values",
                    ParquetRowGroupFilter.getRowGroupsSkipped() > 0);

            // Empty string should still be found
            assertQueryNoLeakCheck("val\n\n", "SELECT val FROM x WHERE val = ''", null, true, false);
        });
    }

    @Test
    public void testMetadataBloomFilterMultiplePartitions() throws Exception {
        assertMemoryLeak(TestFilesFacadeImpl.INSTANCE, () -> {
            execute("""
                    CREATE TABLE x (
                        val INT PARQUET(BLOOM_FILTER),
                        ts TIMESTAMP
                    ) TIMESTAMP(ts) PARTITION BY DAY
                    """);
            execute("""
                    INSERT INTO x VALUES
                    (1, '2024-01-01T00:00:00.000000Z'),
                    (50_000, '2024-01-01T01:00:00.000000Z'),
                    (100_000, '2024-01-02T00:00:00.000000Z'),
                    (200_000, '2024-01-02T01:00:00.000000Z'),
                    (300_000, '2024-01-03T00:00:00.000000Z'),
                    (400_000, '2024-01-03T01:00:00.000000Z'),
                    (999_999, '2024-01-04T00:00:00.000000Z')
                    """);

            // Convert all sealed partitions at once
            execute("ALTER TABLE x CONVERT PARTITION TO PARQUET WHERE ts >= 0");

            // Bloom filter should work across multiple parquet partitions
            ParquetRowGroupFilter.resetRowGroupsSkipped();
            assertQueryNoLeakCheck("val\n", "SELECT val FROM x WHERE val = 25_000", null, true, false);
            Assert.assertTrue("bloom filter should work across multiple converted partitions",
                    ParquetRowGroupFilter.getRowGroupsSkipped() > 0);

            // Verify a value in a later partition is found
            assertQueryNoLeakCheck("val\n300000\n", "SELECT val FROM x WHERE val = 300_000", null, true, false);
        });
    }

    @Test
    public void testMetadataBloomFilterLong() throws Exception {
        assertMemoryLeak(TestFilesFacadeImpl.INSTANCE, () -> {
            execute("""
                    CREATE TABLE x (
                        val LONG PARQUET(BLOOM_FILTER),
                        ts TIMESTAMP
                    ) TIMESTAMP(ts) PARTITION BY DAY
                    """);
            execute("""
                    INSERT INTO x VALUES
                    (1, '2024-01-01T00:00:00.000000Z'),
                    (10_000_000_000, '2024-01-01T01:00:00.000000Z'),
                    (20_000_000_000, '2024-01-01T02:00:00.000000Z'),
                    (30_000_000_000, '2024-01-02T01:00:00.000000Z')
                    """);

            execute("ALTER TABLE x CONVERT PARTITION TO PARQUET WHERE ts >= 0");

            ParquetRowGroupFilter.resetRowGroupsSkipped();
            assertQueryNoLeakCheck("val\n", "SELECT val FROM x WHERE val = 5_000_000_000", null, true, false);
            Assert.assertTrue("bloom filter on LONG column should skip row groups",
                    ParquetRowGroupFilter.getRowGroupsSkipped() > 0);

            assertQueryNoLeakCheck("val\n10000000000\n",
                    "SELECT val FROM x WHERE val = 10_000_000_000", null, true, false);
        });
    }

    @Test
    public void testMetadataBloomFilterVarchar() throws Exception {
        assertMemoryLeak(TestFilesFacadeImpl.INSTANCE, () -> {
            execute("""
                    CREATE TABLE x (
                        val VARCHAR PARQUET(BLOOM_FILTER),
                        ts TIMESTAMP
                    ) TIMESTAMP(ts) PARTITION BY DAY
                    """);
            execute("""
                    INSERT INTO x VALUES
                    ('alpha', '2024-01-01T00:00:00.000000Z'),
                    ('bravo', '2024-01-01T01:00:00.000000Z'),
                    ('charlie', '2024-01-01T02:00:00.000000Z'),
                    ('delta', '2024-01-02T01:00:00.000000Z')
                    """);

            execute("ALTER TABLE x CONVERT PARTITION TO PARQUET WHERE ts >= 0");

            ParquetRowGroupFilter.resetRowGroupsSkipped();
            assertQueryNoLeakCheck("val\n", "SELECT val FROM x WHERE val = 'zulu'", null, true, false);
            Assert.assertTrue("bloom filter on VARCHAR column should skip row groups",
                    ParquetRowGroupFilter.getRowGroupsSkipped() > 0);

            assertQueryNoLeakCheck("val\nalpha\n",
                    "SELECT val FROM x WHERE val = 'alpha'", null, true, false);
        });
    }

    @Test
    public void testConvertLastPartition() throws Exception {
        final long rows = 10;
        assertMemoryLeak(TestFilesFacadeImpl.INSTANCE, () -> {
            execute(
                    "create table x as (select" +
                            " x id," +
                            " rnd_boolean() a_boolean," +
                            " rnd_byte() a_byte," +
                            " timestamp_sequence('2024-06', 500)::" + timestampType.getTypeName() + " designated_ts" +
                            " from long_sequence(" + rows + ")) timestamp(designated_ts) partition by month"
            );

            execute("ALTER TABLE x CONVERT PARTITION TO PARQUET LIST '2024-06'");
            assertPartitionDoesNotExist("x", "2024-06.1");

            execute("insert into x(designated_ts) values('1970-01')");
            execute("ALTER TABLE x CONVERT PARTITION TO PARQUET LIST '1970-01'");
            assertPartitionExists("x", "1970-01.2");
        });
    }

    @Test
    public void testConvertLastPartitionWal() throws Exception {
        assertMemoryLeak(TestFilesFacadeImpl.INSTANCE, () -> {
            execute(
                    "CREATE TABLE x AS (SELECT" +
                            " x id," +
                            " timestamp_sequence('2024-06', 500)::" + timestampType.getTypeName() + " designated_ts" +
                            " FROM long_sequence(10)) timestamp(designated_ts) PARTITION BY MONTH WAL"
            );
            drainWalQueue();

            // Verify initial row count.
            assertQueryNoLeakCheck(
                    "count\n10\n",
                    "SELECT count() FROM x",
                    null, false, true
            );

            // Convert the last (and only) partition to parquet.
            execute("ALTER TABLE x CONVERT PARTITION TO PARQUET LIST '2024-06'");
            drainWalQueue();

            // Verify parquet file exists and count is still correct after conversion.
            assertPartitionExists("x", "2024-06.1");
            assertSql(
                    "count\n10\n",
                    "SELECT count() FROM x"
            );

            // Insert O3 data into the same parquet partition.
            execute("INSERT INTO x(id, designated_ts) VALUES (100, '2024-06-15T00:00:00.000000Z')");
            drainWalQueue();

            // Verify data correctness: original 10 rows + 1 new row.
            assertSql(
                    "count\n11\n",
                    "SELECT count() FROM x"
            );

            // Verify the O3-inserted row is present.
            assertSql(
                    "id\tdesignated_ts\n" +
                            "100\t2024-06-15T00:00:00.000000" + (timestampType == TestTimestampType.NANO ? "000Z" : "Z") + "\n",
                    "SELECT id, designated_ts FROM x WHERE id = 100"
            );
        });
    }

    @Test
    public void testConvertListPartitions() throws Exception {
        assertConvertListPartitionsWithKeyword("partition");
    }


    @Test
    public void testConvertListPartitionsPlural() throws Exception {
        assertConvertListPartitionsWithKeyword("partitions");
    }

    @Test
    public void testConvertListZeroSizeArrayData() throws Exception {
        assertMemoryLeak(TestFilesFacadeImpl.INSTANCE, () -> {
            execute(
                    "create table x (" +
                            " an_array double[]," +
                            " a_ts timestamp" +
                            ") timestamp(a_ts) partition by month;"
            );

            execute("insert into x(an_array, a_ts) values(array[], '2024-07');");
            execute("insert into x(an_array, a_ts) values(array[1.0, 2.0, 3.0], '2024-08');");
            execute("ALTER TABLE x CONVERT PARTITION TO PARQUET WHERE a_ts > 0;");
            assertPartitionExists("x", "2024-07.2");
        });
    }

    @Test
    public void testConvertListZeroSizeVarcharData() throws Exception {
        assertMemoryLeak(TestFilesFacadeImpl.INSTANCE, () -> {
            execute(
                    "create table x as (select" +
                            " case when x % 2 = 0 then rnd_varchar(1, 40, 1) end as a_varchar," +
                            " to_timestamp('2024-07', 'yyyy-MM')::" + timestampType.getTypeName() + " as a_ts," +
                            " from long_sequence(1)) timestamp (a_ts) partition by MONTH"
            );

            execute("insert into x(a_varchar, a_ts) values('', '2024-08')");
            execute("ALTER TABLE x CONVERT PARTITION TO PARQUET WHERE a_ts > 0");
            assertPartitionExists("x", "2024-07.2");
        });
    }

    @Test
    public void testConvertPartitionAllTypes() throws Exception {
        final long rows = 1000;
        assertMemoryLeak(TestFilesFacadeImpl.INSTANCE, () -> {
            execute(
                    "create table x as (select" +
                            " x id," +
                            " rnd_boolean() a_boolean," +
                            " rnd_byte() a_byte," +
                            " rnd_short() a_short," +
                            " rnd_char() a_char," +
                            " rnd_int() an_int," +
                            " rnd_long() a_long," +
                            " rnd_float() a_float," +
                            " rnd_double() a_double," +
                            " rnd_symbol('a','b','c') a_symbol," +
                            " rnd_geohash(4) a_geo_byte," +
                            " rnd_geohash(8) a_geo_short," +
                            " rnd_geohash(16) a_geo_int," +
                            " rnd_geohash(32) a_geo_long," +
                            " rnd_str('hello', 'world', '!') a_string," +
                            " rnd_bin() a_bin," +
                            " rnd_varchar('ганьба','слава','добрий','вечір') a_varchar," +
                            " rnd_ipv4() a_ip," +
                            " rnd_uuid4() a_uuid," +
                            " rnd_long256() a_long256," +
                            " to_long128(rnd_long(), rnd_long()) a_long128," +
                            " rnd_double_array(1) an_array," +
                            " cast(timestamp_sequence(600000000000, 700) as date) a_date," +
                            " timestamp_sequence(500000000000, 600)::" + timestampType.getTypeName() + " a_ts," +
                            " timestamp_sequence(400000000000, " + Micros.DAY_MICROS / 12 + ")::" + timestampType.getTypeName() + " designated_ts" +
                            " from long_sequence(" + rows + ")), index(a_symbol) timestamp(designated_ts) partition by month"
            );

            assertException(
                    "ALTER TABLE x CONVERT PARTITION TO PARQUET LIST '2024-06'",
                    0,
                    "cannot convert partition to parquet, partition does not exist"
            );

            execute("ALTER TABLE x CONVERT PARTITION TO PARQUET LIST '1970-01', '1970-02'");
            assertPartitionExists("x", "1970-01.1");
            assertPartitionExists("x", "1970-02.2");
            assertPartitionDoesNotExist("x", "1970-03.3");
        });
    }

    @Test
    public void testConvertPartitionBrokenSymbols() throws Exception {
        final long rows = 10;
        final String tableName = "x";
        assertMemoryLeak(TestFilesFacadeImpl.INSTANCE, () -> {
            execute(
                    "create table " + tableName + " as (select" +
                            " x id," +
                            " rnd_symbol('a','b','c') a_symbol," +
                            " timestamp_sequence(400000000000, " + Micros.DAY_MICROS * 5 + ")::" + timestampType.getTypeName() + " designated_ts" +
                            " from long_sequence(" + rows + ")) timestamp(designated_ts) partition by month"
            );

            engine.releaseInactive();
            try (Path path = new Path().of(configuration.getDbRoot())) {
                TableToken tableToken = engine.getTableTokenIfExists(tableName);
                path.concat(tableToken.getDirName()).concat("a_symbol").put(".o");
                FilesFacade ff = configuration.getFilesFacade();
                Assert.assertTrue(ff.exists(path.$()));
                long fd = ff.openRW(path.$(), CairoConfiguration.O_NONE);
                Assert.assertTrue(configuration.getFilesFacade().truncate(fd, SymbolMapWriter.HEADER_SIZE - 2));
                ff.close(fd);
            }
            try {
                execute("ALTER TABLE " + tableName + " CONVERT PARTITION TO PARQUET LIST '1970-01'");
                Assert.fail();
            } catch (Exception e) {
                TestUtils.assertContains(e.getMessage(), " SymbolMap is too short");
            }
            assertPartitionDoesNotExist(tableName, "1970-01.1");
        });
    }

    @Test
    public void testConvertPartitionParquetAndBackAllTypes() throws Exception {
        final long rows = 1000;
        Overrides overrides = node1.getConfigurationOverrides();
        // test multiple row groups
        overrides.setProperty(PropertyKey.CAIRO_PARTITION_ENCODER_PARQUET_ROW_GROUP_SIZE, 101);
        assertMemoryLeak(TestFilesFacadeImpl.INSTANCE, () -> {
            execute(
                    "create table x as (select" +
                            " x id," +
                            " rnd_boolean() a_boolean," +
                            " rnd_byte() a_byte," +
                            " rnd_short() a_short," +
                            " rnd_char() a_char," +
                            " rnd_int() an_int," +
                            " rnd_long() a_long," +
                            " rnd_float() a_float," +
                            " rnd_double() a_double," +
                            " rnd_symbol('a','b','c') a_symbol," +
                            " rnd_geohash(4) a_geo_byte," +
                            " rnd_geohash(8) a_geo_short," +
                            " rnd_geohash(16) a_geo_int," +
                            " rnd_geohash(32) a_geo_long," +
                            " rnd_str('abc', 'def', 'ghk') a_string," +
                            " rnd_bin() a_bin," +
                            " rnd_ipv4() a_ip," +
                            " rnd_varchar('ганьба','слава','добрий','вечір', '1111111111111111') a_varchar," +
                            " rnd_uuid4() a_uuid," +
                            " rnd_long256() a_long256," +
                            " to_long128(rnd_long(), rnd_long()) a_long128," +
                            " rnd_double_array(1) an_array," +
                            " cast(timestamp_sequence(600000000000, 700) as date) a_date," +
                            " timestamp_sequence(500000000000, 600)::" + timestampType.getTypeName() + " a_ts," +
                            " timestamp_sequence(400000000000, " + Micros.DAY_MICROS / 12 + ")::" + timestampType.getTypeName() + " designated_ts" +
                            " from long_sequence(" + rows + ")), index(a_symbol) timestamp(designated_ts) partition by month"
            );

            execute("create table y as (select * from x)", sqlExecutionContext);

            assertException(
                    "ALTER TABLE x CONVERT PARTITION TO PARQUET LIST '2024-06'",
                    0,
                    "cannot convert partition to parquet, partition does not exist"
            );

            execute("ALTER TABLE x CONVERT PARTITION TO PARQUET LIST '1970-01'");
            assertPartitionExists("x", "1970-01.1");
            execute("ALTER TABLE x CONVERT PARTITION TO NATIVE LIST '1970-01'");
            assertPartitionDoesNotExist("x", "1970-01.1");

            assertSqlCursors("select * from x", "select * from y");
        });
    }

    @Test
    public void testConvertPartitionSymbolMapDoesNotExist() throws Exception {
        final long rows = 10;
        final String tableName = "x";
        assertMemoryLeak(TestFilesFacadeImpl.INSTANCE, () -> {
            execute(
                    "create table " + tableName + " as (select" +
                            " x id," +
                            " rnd_symbol('a','b','c') a_symbol," +
                            " timestamp_sequence(400000000000, 500)::" + timestampType.getTypeName() + " designated_ts" +
                            " from long_sequence(" + rows + ")) timestamp(designated_ts) partition by month"
            );

            engine.releaseInactive();
            try (Path path = new Path().of(configuration.getDbRoot())) {
                TableToken tableToken = engine.getTableTokenIfExists(tableName);
                path.concat(tableToken.getDirName()).concat("a_symbol").put(".o");
                FilesFacade ff = configuration.getFilesFacade();
                ff.remove(path.$());
            }
            try {
                execute("ALTER TABLE " + tableName + " CONVERT PARTITION TO PARQUET LIST '1970-01'");
                Assert.fail();
            } catch (Exception e) {
                TestUtils.assertContains(e.getMessage(), "SymbolMap does not exist");
            }
            assertPartitionDoesNotExist(tableName, "1970-01.1");
        });
    }

    @Test
    public void testConvertPartitionsWithColTops() throws Exception {
        assertMemoryLeak(TestFilesFacadeImpl.INSTANCE, () -> {
            final String tableName = "testConvertPartitionsWithColTops";
            createTable(
                    tableName,
                    "INSERT INTO " + tableName + " VALUES(1, '2024-06-10T00:00:00.000000Z')",
                    "INSERT INTO " + tableName + " VALUES(2, '2024-06-11T00:00:00.000000Z')",
                    "INSERT INTO " + tableName + " VALUES(3, '2024-06-12T00:00:00.000000Z')",
                    "INSERT INTO " + tableName + " VALUES(4, '2024-06-12T00:00:01.000000Z')",
                    "INSERT INTO " + tableName + " VALUES(5, '2024-06-15T00:00:00.000000Z')",
                    "INSERT INTO " + tableName + " VALUES(6, '2024-06-12T00:00:02.000000Z')"
            );

            execute("ALTER TABLE " + tableName + " ADD COLUMN a int");
            execute("INSERT INTO " + tableName + " VALUES(7, '2024-06-10T00:00:00.000000Z', 1)");

            execute("ALTER TABLE " + tableName + " CONVERT PARTITION TO PARQUET WHERE timestamp > 0 AND timestamp < '2024-06-15'");

            assertPartitionExists(tableName, "2024-06-10.10");
            assertPartitionExists(tableName, "2024-06-11.8");
            assertPartitionExists(tableName, "2024-06-12.9");

            assertPartitionDoesNotExist(tableName, "2024-06-15.3");
        });
    }

    @Test
    public void testConvertPartitionsWithColTopsSelect() throws Exception {
        assertMemoryLeak(TestFilesFacadeImpl.INSTANCE, () -> {
            final String tableName = "testConvertPartitionsWithColTopsSelect";
            createTable(
                    tableName,
                    "INSERT INTO " + tableName + " VALUES(1, '2024-11-01T00:00:00.000000Z')",
                    "INSERT INTO " + tableName + " VALUES(2, '2024-11-02T00:00:00.000000Z')",
                    "INSERT INTO " + tableName + " VALUES(3, '2024-11-03T00:00:00.000000Z')",
                    "INSERT INTO " + tableName + " VALUES(4, '2024-11-04T00:00:00.000000Z')",
                    "INSERT INTO " + tableName + " VALUES(5, '2024-11-05T00:00:00.000000Z')"
            );

            execute("ALTER TABLE " + tableName + " ADD COLUMN a int");
            execute("INSERT INTO " + tableName + " VALUES(5, '2024-11-05T00:00:00.000000Z', 5)");
            execute("INSERT INTO " + tableName + " VALUES(6, '2024-11-06T00:00:00.000000Z', 6)");
            execute("INSERT INTO " + tableName + " VALUES(7, '2024-11-07T00:00:00.000000Z', 7)");

            execute("ALTER TABLE " + tableName + " CONVERT PARTITION TO PARQUET WHERE timestamp >= 0");

            assertQuery(
                    replaceTimestampSuffix("""
                            id\ttimestamp\ta
                            1\t2024-11-01T00:00:00.000000Z\tnull
                            2\t2024-11-02T00:00:00.000000Z\tnull
                            3\t2024-11-03T00:00:00.000000Z\tnull
                            4\t2024-11-04T00:00:00.000000Z\tnull
                            5\t2024-11-05T00:00:00.000000Z\tnull
                            5\t2024-11-05T00:00:00.000000Z\t5
                            6\t2024-11-06T00:00:00.000000Z\t6
                            7\t2024-11-07T00:00:00.000000Z\t7
                            """, timestampType.getTypeName()),
                    tableName,
                    "timestamp",
                    true,
                    true
            );
        });
    }

    @Test
    public void testConvertSecondCallIgnored() throws Exception {
        assertMemoryLeak(TestFilesFacadeImpl.INSTANCE, () -> {
            execute(
                    "create table x as (select" +
                            " x a_long," +
                            " to_timestamp('2024-07', 'yyyy-MM')::" + timestampType.getTypeName() + " as a_ts," +
                            " from long_sequence(10)) timestamp (a_ts) partition by MONTH"
            );

            execute("insert into x(a_long, a_ts) values('42', '2024-08')");
            execute("ALTER TABLE x CONVERT PARTITION TO PARQUET WHERE a_ts > 0");
            assertPartitionExists("x", "2024-07.2");

            // Second call should be ignored
            execute("ALTER TABLE x CONVERT PARTITION TO PARQUET WHERE a_ts > 0");
            assertPartitionExists("x", "2024-07.2");
        });
    }

    @Test
    public void testConvertTimestampPartitions() throws Exception {
        assertMemoryLeak(TestFilesFacadeImpl.INSTANCE, () -> {
            final String tableName = "testConvertTimestampPartitions";
            createTable(
                    tableName,
                    "INSERT INTO " + tableName + " VALUES(1, '2024-06-10T00:00:00.000000Z')",
                    "INSERT INTO " + tableName + " VALUES(2, '2024-06-11T00:00:00.000000Z')",
                    "INSERT INTO " + tableName + " VALUES(3, '2024-06-12T00:00:00.000000Z')",
                    "INSERT INTO " + tableName + " VALUES(4, '2024-06-12T00:00:01.000000Z')",
                    "INSERT INTO " + tableName + " VALUES(5, '2024-06-15T00:00:00.000000Z')",
                    "INSERT INTO " + tableName + " VALUES(6, '2024-06-12T00:00:02.000000Z')"
            );

            assertQueryNoLeakCheck(
                    """
                            index\tname\treadOnly\tisParquet\tparquetFileSize
                            0\t2024-06-10\tfalse\tfalse\t-1
                            1\t2024-06-11\tfalse\tfalse\t-1
                            2\t2024-06-12\tfalse\tfalse\t-1
                            3\t2024-06-15\tfalse\tfalse\t-1
                            """,
                    "select index, name, readOnly, isParquet, parquetFileSize from table_partitions('" + tableName + "')",
                    null,
                    false,
                    true
            );

            execute("ALTER TABLE " + tableName + " CONVERT PARTITION TO PARQUET WHERE timestamp = to_timestamp('2024-06-12', 'yyyy-MM-dd')");

            assertQueryNoLeakCheck(
                    replaceTimestampSuffix(
                            """
                                    index\tname\treadOnly\tisParquet\tisNonEmpty\tminTimestamp\tmaxTimestamp
                                    0\t2024-06-10\tfalse\tfalse\tfalse\t2024-06-10T00:00:00.000000Z\t2024-06-10T00:00:00.000000Z
                                    1\t2024-06-11\tfalse\tfalse\tfalse\t2024-06-11T00:00:00.000000Z\t2024-06-11T00:00:00.000000Z
                                    2\t2024-06-12\tfalse\ttrue\ttrue\t\t
                                    3\t2024-06-15\tfalse\tfalse\tfalse\t2024-06-15T00:00:00.000000Z\t2024-06-15T00:00:00.000000Z
                                    """,
                            timestampType.getTypeName()
                    ),
                    "select index, name, readOnly, isParquet, parquetFileSize   > 0 isNonEmpty, minTimestamp, maxTimestamp from table_partitions('" + tableName + "')",
                    null,
                    false,
                    true
            );

            assertPartitionDoesNotExist(tableName, "2024-06-10");
            assertPartitionDoesNotExist(tableName, "2024-06-11.0");
            assertPartitionExists(tableName, "2024-06-12.6");
            assertPartitionDoesNotExist(tableName, "2024-06-15.3");
        });
    }

    private void assertPartitionDoesNotExist(String tableName, String partition) {
        assertPartitionOnDisk0(tableName, false, partition);
    }

    private void assertPartitionExists(String tableName, String partition) {
        assertPartitionOnDisk0(tableName, true, partition);
    }

    private void assertPartitionOnDisk0(String tableName, boolean exists, String partition) {
        Path path = Path.getThreadLocal(configuration.getDbRoot());
        path.concat(engine.verifyTableName(tableName));
        path.concat(partition).concat(PARQUET_PARTITION_NAME);

        if (exists) {
            Assert.assertTrue(ff.exists(path.$()));
        } else {
            Assert.assertFalse(ff.exists(path.$()));
        }
    }

    private void createTable(String tableName, String... inserts) throws Exception {
        TableModel model = new TableModel(configuration, tableName, PartitionBy.DAY)
                .col("id", ColumnType.INT)
                .timestamp(timestampType.getTimestampType());
        AbstractCairoTest.create(model);
        for (int i = 0, n = inserts.length; i < n; i++) {
            execute(inserts[i]);
        }
    }

    private void createTableStr(String... inserts) throws Exception {
        TableModel model = new TableModel(configuration, "testConvertAllPartitionsToParquetAndBack", PartitionBy.DAY)
                .col("id", ColumnType.INT)
                .col("str", ColumnType.STRING)
                .timestamp(timestampType.getTimestampType());
        AbstractCairoTest.create(model);
        for (int i = 0, n = inserts.length; i < n; i++) {
            execute(inserts[i]);
        }
    }

    private void assertConvertAllPartitionsWithKeyword(String partitionKeyword) throws Exception {
        assertMemoryLeak(TestFilesFacadeImpl.INSTANCE, () -> {
            final String tableName = "testConvertAllPartitions_" + partitionKeyword;
            createTable(
                    tableName,
                    "insert into " + tableName + " values(1, '2024-06-10T00:00:00.000000Z')",
                    "insert into " + tableName + " values(2, '2024-06-11T00:00:00.000000Z')",
                    "insert into " + tableName + " values(3, '2024-06-12T00:00:00.000000Z')",
                    "insert into " + tableName + " values(4, '2024-06-12T00:00:01.000000Z')",
                    "insert into " + tableName + " values(5, '2024-06-15T00:00:00.000000Z')",
                    "insert into " + tableName + " values(6, '2024-06-12T00:00:02.000000Z')"
            );

            execute("alter table " + tableName + " convert " + partitionKeyword + " to parquet where timestamp > 0");

            assertPartitionExists(tableName, "2024-06-10.8");
            assertPartitionExists(tableName, "2024-06-11.6");
            assertPartitionExists(tableName, "2024-06-12.7");
            // last partition is not converted
        });
    }

    private void assertConvertListPartitionsWithKeyword(String partitionKeyword) throws Exception {
        assertMemoryLeak(TestFilesFacadeImpl.INSTANCE, () -> {
            final String tableName = "testConvertListPartitions_" + partitionKeyword;
            createTable(
                    tableName,
                    "insert into " + tableName + " values(1, '2024-06-10T00:00:00.000000Z')",
                    "insert into " + tableName + " values(2, '2024-06-11T00:00:00.000000Z')",
                    "insert into " + tableName + " values(3, '2024-06-12T00:00:00.000000Z')",
                    "insert into " + tableName + " values(4, '2024-06-12T00:00:01.000000Z')",
                    "insert into " + tableName + " values(5, '2024-06-15T00:00:00.000000Z')",
                    "insert into " + tableName + " values(6, '2024-06-12T00:00:02.000000Z')"
            );

            execute("alter table " + tableName + " convert " + partitionKeyword + " to parquet list '2024-06-10', '2024-06-11', '2024-06-12'");

            assertPartitionExists(tableName, "2024-06-10.6");
            assertPartitionExists(tableName, "2024-06-11.7");
            assertPartitionExists(tableName, "2024-06-12.8");
            assertPartitionDoesNotExist(tableName, "2024-06-15.3");
        });
    }
}
