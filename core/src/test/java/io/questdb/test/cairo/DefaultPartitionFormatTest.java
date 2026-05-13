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

package io.questdb.test.cairo;

import io.questdb.cairo.TableToken;
import io.questdb.cairo.TableUtils;
import io.questdb.cairo.sql.TableMetadata;
import io.questdb.griffin.SqlException;
import io.questdb.test.AbstractCairoTest;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

public class DefaultPartitionFormatTest extends AbstractCairoTest {

    @Test
    public void testAlterTableSetFormatInvalidValue() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE tango (ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
            try {
                execute("ALTER TABLE tango SET FORMAT BANANA");
                fail("Invalid format value accepted");
            } catch (SqlException e) {
                assertEquals("[29] 'parquet' or 'native' expected", e.getMessage());
            }
        });
    }

    @Test
    public void testAlterTableSetFormatNativeRoundTrip() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE tango (ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY FORMAT PARQUET WAL");
            assertDefaultPartitionFormat("tango", TableUtils.DEFAULT_PARTITION_FORMAT_PARQUET);

            execute("ALTER TABLE tango SET FORMAT NATIVE");
            drainWalQueue();
            assertDefaultPartitionFormat("tango", TableUtils.DEFAULT_PARTITION_FORMAT_NATIVE);
        });
    }

    @Test
    public void testAlterTableSetFormatParquet() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE tango (ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
            assertDefaultPartitionFormat("tango", TableUtils.DEFAULT_PARTITION_FORMAT_NATIVE);

            execute("ALTER TABLE tango SET FORMAT PARQUET");
            drainWalQueue();
            assertDefaultPartitionFormat("tango", TableUtils.DEFAULT_PARTITION_FORMAT_PARQUET);
        });
    }

    @Test
    public void testAlterTableSetFormatParquetRejectedOnNonPartitioned() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE tango (ts TIMESTAMP) TIMESTAMP(ts)");
            try {
                execute("ALTER TABLE tango SET FORMAT PARQUET");
                fail("FORMAT PARQUET should be rejected on non-partitioned table");
            } catch (SqlException e) {
                assertEquals("[29] FORMAT PARQUET is only supported on partitioned tables", e.getMessage());
            }
        });
    }

    @Test
    public void testAlterTableSetFormatParquetRejectedOnNonWal() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE tango (ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL");
            try {
                execute("ALTER TABLE tango SET FORMAT PARQUET");
                fail("FORMAT PARQUET should be rejected on non-WAL table");
            } catch (SqlException e) {
                assertEquals("[29] FORMAT PARQUET is only supported on WAL tables", e.getMessage());
            }
        });
    }

    @Test
    public void testCreateTableFormatNative() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE tango (ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY FORMAT NATIVE WAL");
            assertDefaultPartitionFormat("tango", TableUtils.DEFAULT_PARTITION_FORMAT_NATIVE);
        });
    }

    @Test
    public void testCreateTableFormatParquet() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE tango (ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY FORMAT PARQUET WAL");
            assertDefaultPartitionFormat("tango", TableUtils.DEFAULT_PARTITION_FORMAT_PARQUET);
        });
    }

    @Test
    public void testCreateTableFormatParquetRejectedOnNonWal() throws Exception {
        assertMemoryLeak(() -> {
            try {
                execute("CREATE TABLE tango (ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY FORMAT PARQUET BYPASS WAL");
                fail("FORMAT PARQUET should be rejected on non-WAL CREATE TABLE");
            } catch (SqlException e) {
                assertEquals("[72] FORMAT PARQUET is only supported on WAL tables", e.getMessage());
            }
        });
    }

    @Test
    public void testCreateTableInvalidFormatValue() throws Exception {
        assertMemoryLeak(() -> {
            try {
                execute("CREATE TABLE tango (ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY FORMAT BANANA WAL");
                fail("Invalid format value accepted");
            } catch (SqlException e) {
                assertEquals("[72] 'parquet' or 'native' expected", e.getMessage());
            }
        });
    }

    @Test
    public void testCreateTableNativeIsDefault() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE tango (ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
            assertDefaultPartitionFormat("tango", TableUtils.DEFAULT_PARTITION_FORMAT_NATIVE);
        });
    }

    @Test
    public void testCreateTableUnpartitionedIgnoresFormat() throws Exception {
        // FORMAT lives inside the PARTITION BY clause, so a non-partitioned
        // table never sees it. Confirm the table still creates and stays NATIVE.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE tango (ts TIMESTAMP) TIMESTAMP(ts)");
            assertDefaultPartitionFormat("tango", TableUtils.DEFAULT_PARTITION_FORMAT_NATIVE);
        });
    }

    /**
     * Failing test: documents that the storage-layer hookup is not yet
     * implemented. Once new partitions are emitted directly as parquet on a
     * FORMAT PARQUET table, this test will pass without modification.
     */
    @Test
    public void testNewPartitionLandsAsParquet() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE tango (ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY FORMAT PARQUET WAL");
            execute("INSERT INTO tango VALUES " +
                    "('2024-01-01T00:00:00.000000Z'), " +
                    "('2024-01-02T00:00:00.000000Z'), " +
                    "('2024-01-03T00:00:00.000000Z')");
            drainWalQueue();
            // Every partition must be parquet because the table is FORMAT PARQUET.
            assertSql("name\tisParquet\n" +
                            "2024-01-01\ttrue\n" +
                            "2024-01-02\ttrue\n" +
                            "2024-01-03\ttrue\n",
                    "SELECT name, isParquet FROM table_partitions('tango')");
        });
    }

    @Test
    public void testShowCreateTableEmitsFormatParquet() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE tango (ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY FORMAT PARQUET WAL");
            assertSql("""
                            ddl
                            CREATE TABLE 'tango' (\s
                            \tts TIMESTAMP
                            ) timestamp(ts) PARTITION BY DAY FORMAT PARQUET;
                            """,
                    "SHOW CREATE TABLE tango");
        });
    }

    @Test
    public void testShowCreateTableOmitsFormatNative() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE tango (ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
            assertSql("""
                            ddl
                            CREATE TABLE 'tango' (\s
                            \tts TIMESTAMP
                            ) timestamp(ts) PARTITION BY DAY;
                            """,
                    "SHOW CREATE TABLE tango");
        });
    }

    private void assertDefaultPartitionFormat(String tableName, int expected) {
        TableToken token = engine.verifyTableName(tableName);
        try (TableMetadata metadata = engine.getTableMetadata(token)) {
            assertEquals(expected, metadata.getDefaultPartitionFormat());
        }
    }
}
