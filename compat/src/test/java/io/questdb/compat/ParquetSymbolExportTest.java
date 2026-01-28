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

package io.questdb.compat;

import io.questdb.DefaultHttpClientConfiguration;
import io.questdb.ServerMain;
import io.questdb.cutlass.http.client.Fragment;
import io.questdb.cutlass.http.client.HttpClient;
import io.questdb.cutlass.http.client.HttpClientFactory;
import io.questdb.cutlass.http.client.Response;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.std.Numbers;
import io.questdb.std.Unsafe;
import org.apache.hadoop.conf.Configuration;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.metadata.BlockMetaData;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;
import org.apache.parquet.hadoop.util.HadoopInputFile;
import org.apache.parquet.io.InputFile;
import org.junit.Assert;
import org.junit.Test;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.util.List;

/**
 * Tests for Parquet export of SYMBOL columns.
 * <p>
 * Reproduces the scenario from <a href="https://github.com/questdb/questdb/issues/6692">...</a>
 * <p>
 * The bug manifests when:
 * 1. Multiple row groups are created (partial final row group)
 * 2. Symbol columns use bitpacked dictionary indices
 * 3. Strict parquet readers fail with:
 * - "Invalid number of indices: 0" on partial row groups (RLE encoding bug)
 * - "Column cannot have more than one dictionary" across row groups
 */
public class ParquetSymbolExportTest extends AbstractTest {
    private static final Log LOG = LogFactory.getLog(ParquetSymbolExportTest.class);

    /**
     * Tests symbol column with high cardinality (many unique values).
     */
    @Test
    public void testSymbolColumnHighCardinality() throws Exception {
        final int rowCount = 120001; // Creates 2 row groups

        try (final ServerMain serverMain = ServerMain.create(root)) {
            serverMain.start();

            serverMain.getEngine().execute(
                    "CREATE TABLE symbol_high_card_test (" +
                            "sym SYMBOL, " +
                            "ts TIMESTAMP" +
                            ") TIMESTAMP(ts) PARTITION BY DAY"
            );

            // Insert rows with high cardinality symbols (unique per row modulo 1000)
            LOG.info().$("Inserting ").$(rowCount).$(" rows with high cardinality symbols...").$();
            serverMain.getEngine().execute(
                    "INSERT INTO symbol_high_card_test " +
                            "SELECT " +
                            "'SYM_' || (x % 1000) as sym, " +
                            "timestamp_sequence('2024-01-01', 1000000) as ts " +
                            "FROM long_sequence(" + rowCount + ")"
            );

            // Wait for WAL to be applied
            serverMain.awaitTable("symbol_high_card_test");

            assertSql(serverMain.getEngine(), "SELECT count() FROM symbol_high_card_test", "count()\n" + rowCount + "\n");

            File parquetFile = new File(root, "symbol_high_card_test.parquet");
            exportToParquet(serverMain, "SELECT * FROM symbol_high_card_test", parquetFile);

            validateParquetFile(parquetFile, rowCount);

            serverMain.getEngine().execute("DROP TABLE symbol_high_card_test");
        }
    }

    /**
     * Tests that symbol columns can be exported and read back with multiple row groups.
     * Uses 250001 rows which creates 3 row groups (100k + 100k + 50001).
     */
    @Test
    public void testSymbolColumnMultipleRowGroups() throws Exception {
        final int rowCount = 250001; // Creates 3 row groups: 100k + 100k + 50001

        try (final ServerMain serverMain = ServerMain.create(root)) {
            serverMain.start();

            // Create table with symbol columns of different cardinalities
            // to test bitpacking behavior:
            // - sym_8: 8 values (3 bits)
            // - sym_3: 3 values (2 bits)
            // - sym_2: 2 values (1 bit)
            serverMain.getEngine().execute(
                    "CREATE TABLE symbol_test (" +
                            "sym_8 SYMBOL, " +
                            "sym_3 SYMBOL, " +
                            "sym_2 SYMBOL, " +
                            "value DOUBLE, " +
                            "ts TIMESTAMP" +
                            ") TIMESTAMP(ts) PARTITION BY DAY"
            );

            // Insert rows
            LOG.info().$("Inserting ").$(rowCount).$(" rows...").$();
            serverMain.getEngine().execute(
                    "INSERT INTO symbol_test " +
                            "SELECT " +
                            "rnd_symbol('A','B','C','D','E','F','G','H') as sym_8, " +
                            "rnd_symbol('X','Y','Z') as sym_3, " +
                            "rnd_symbol('ONE','TWO') as sym_2, " +
                            "rnd_double() as value, " +
                            "timestamp_sequence('2024-01-01', 1000000) as ts " +
                            "FROM long_sequence(" + rowCount + ")"
            );

            // Wait for WAL to be applied
            serverMain.awaitTable("symbol_test");

            // Verify row count
            assertSql(serverMain.getEngine(), "SELECT count() FROM symbol_test", "count()\n" + rowCount + "\n");

            // Export to Parquet via HTTP
            File parquetFile = new File(root, "symbol_test.parquet");
            exportToParquet(serverMain, "SELECT * FROM symbol_test", parquetFile);

            LOG.info().$("Parquet file size: ").$(parquetFile.length()).$(" bytes").$();

            // Read and validate the Parquet file
            validateParquetFile(parquetFile, rowCount);

            // Cleanup
            serverMain.getEngine().execute("DROP TABLE symbol_test");
        }
    }

    /**
     * Tests symbol column with exactly 1 distinct non-null value plus nulls.
     * This tests the edge case where max_key=0, which requires 0 bits (potential bug).
     */
    @Test
    public void testSymbolColumnOneDistinctValuePlusNulls() throws Exception {
        final int rowCount = 150001; // Creates 2 row groups

        try (final ServerMain serverMain = ServerMain.create(root)) {
            serverMain.start();

            serverMain.getEngine().execute(
                    "CREATE TABLE symbol_1val_test (" +
                            "sym SYMBOL, " +
                            "ts TIMESTAMP" +
                            ") TIMESTAMP(ts) PARTITION BY DAY"
            );

            // Insert rows with only 1 distinct symbol value plus nulls
            LOG.info().$("Inserting ").$(rowCount).$(" rows with 1 distinct symbol + nulls...").$();
            serverMain.getEngine().execute(
                    "INSERT INTO symbol_1val_test " +
                            "SELECT " +
                            "CASE WHEN x % 3 = 0 THEN NULL ELSE 'ONLY_ONE' END as sym, " +
                            "timestamp_sequence('2024-01-01', 1000000) as ts " +
                            "FROM long_sequence(" + rowCount + ")"
            );

            serverMain.awaitTable("symbol_1val_test");

            assertSql(serverMain.getEngine(), "SELECT count() FROM symbol_1val_test", "count()\n" + rowCount + "\n");

            File parquetFile = new File(root, "symbol_1val_test.parquet");
            exportToParquet(serverMain, "SELECT * FROM symbol_1val_test", parquetFile);

            validateParquetFile(parquetFile, rowCount);

            serverMain.getEngine().execute("DROP TABLE symbol_1val_test");
        }
    }

    /**
     * Tests symbol column with exactly 2 distinct non-null values plus nulls.
     * This tests the edge case where max_key=1, which requires 1 bit.
     */
    @Test
    public void testSymbolColumnTwoDistinctValuesPlusNulls() throws Exception {
        final int rowCount = 150001; // Creates 2 row groups

        try (final ServerMain serverMain = ServerMain.create(root)) {
            serverMain.start();

            serverMain.getEngine().execute(
                    "CREATE TABLE symbol_2val_test (" +
                            "sym SYMBOL, " +
                            "ts TIMESTAMP" +
                            ") TIMESTAMP(ts) PARTITION BY DAY"
            );

            // Insert rows with 2 distinct symbol values plus nulls
            LOG.info().$("Inserting ").$(rowCount).$(" rows with 2 distinct symbols + nulls...").$();
            serverMain.getEngine().execute(
                    "INSERT INTO symbol_2val_test " +
                            "SELECT " +
                            "CASE " +
                            "  WHEN x % 4 = 0 THEN NULL " +
                            "  WHEN x % 4 = 1 THEN 'FIRST' " +
                            "  ELSE 'SECOND' " +
                            "END as sym, " +
                            "timestamp_sequence('2024-01-01', 1000000) as ts " +
                            "FROM long_sequence(" + rowCount + ")"
            );

            serverMain.awaitTable("symbol_2val_test");

            assertSql(serverMain.getEngine(), "SELECT count() FROM symbol_2val_test", "count()\n" + rowCount + "\n");

            File parquetFile = new File(root, "symbol_2val_test.parquet");
            exportToParquet(serverMain, "SELECT * FROM symbol_2val_test", parquetFile);

            validateParquetFile(parquetFile, rowCount);

            serverMain.getEngine().execute("DROP TABLE symbol_2val_test");
        }
    }

    /**
     * Tests symbol column with exactly 3 distinct non-null values plus nulls.
     * This tests the edge case where max_key=2, which requires 2 bits.
     */
    @Test
    public void testSymbolColumnThreeDistinctValuesPlusNulls() throws Exception {
        final int rowCount = 150001; // Creates 2 row groups

        try (final ServerMain serverMain = ServerMain.create(root)) {
            serverMain.start();

            serverMain.getEngine().execute(
                    "CREATE TABLE symbol_3val_test (" +
                            "sym SYMBOL, " +
                            "ts TIMESTAMP" +
                            ") TIMESTAMP(ts) PARTITION BY DAY"
            );

            // Insert rows with 3 distinct symbol values plus nulls
            LOG.info().$("Inserting ").$(rowCount).$(" rows with 3 distinct symbols + nulls...").$();
            serverMain.getEngine().execute(
                    "INSERT INTO symbol_3val_test " +
                            "SELECT " +
                            "CASE " +
                            "  WHEN x % 5 = 0 THEN NULL " +
                            "  WHEN x % 5 = 1 THEN 'ALPHA' " +
                            "  WHEN x % 5 = 2 THEN 'BETA' " +
                            "  ELSE 'GAMMA' " +
                            "END as sym, " +
                            "timestamp_sequence('2024-01-01', 1000000) as ts " +
                            "FROM long_sequence(" + rowCount + ")"
            );

            serverMain.awaitTable("symbol_3val_test");

            assertSql(serverMain.getEngine(), "SELECT count() FROM symbol_3val_test", "count()\n" + rowCount + "\n");

            File parquetFile = new File(root, "symbol_3val_test.parquet");
            exportToParquet(serverMain, "SELECT * FROM symbol_3val_test", parquetFile);

            validateParquetFile(parquetFile, rowCount);

            serverMain.getEngine().execute("DROP TABLE symbol_3val_test");
        }
    }

    /**
     * Tests symbol column with column top in a single partition.
     * Column top is created by adding a symbol column after initial rows exist.
     * The initial rows will have NULL for the new symbol column.
     */
    @Test
    public void testSymbolColumnTopSinglePartition() throws Exception {
        final int initialRows = 5000;
        final int additionalRows = 145001; // Total 150001 rows, creates 2 row groups
        final int totalRows = initialRows + additionalRows;

        try (final ServerMain serverMain = ServerMain.create(root)) {
            serverMain.start();

            // Create table with only timestamp column initially
            serverMain.getEngine().execute(
                    "CREATE TABLE symbol_coltop_single (" +
                            "value INT, " +
                            "ts TIMESTAMP" +
                            ") TIMESTAMP(ts) PARTITION BY DAY"
            );

            // Insert initial rows (these will have NULL for the symbol column added later)
            LOG.info().$("Inserting ").$(initialRows).$(" initial rows...").$();
            serverMain.getEngine().execute(
                    "INSERT INTO symbol_coltop_single " +
                            "SELECT " +
                            "x as value, " +
                            "timestamp_sequence('2024-01-01', 1000000) as ts " +
                            "FROM long_sequence(" + initialRows + ")"
            );

            serverMain.awaitTable("symbol_coltop_single");

            // Add symbol column - creates column top for existing rows
            LOG.info().$("Adding symbol column (creates column top)...").$();
            serverMain.getEngine().execute(
                    "ALTER TABLE symbol_coltop_single ADD COLUMN sym SYMBOL"
            );

            // Insert additional rows with symbol values (same partition - timestamps continue)
            LOG.info().$("Inserting ").$(additionalRows).$(" rows with symbol values...").$();
            serverMain.getEngine().execute(
                    "INSERT INTO symbol_coltop_single (value, ts, sym) " +
                            "SELECT " +
                            "x + " + initialRows + " as value, " +
                            "timestamp_sequence('2024-01-01T00:00:05', 1000000) as ts, " +
                            "rnd_symbol('ALPHA', 'BETA', 'GAMMA') as sym " +
                            "FROM long_sequence(" + additionalRows + ")"
            );

            serverMain.awaitTable("symbol_coltop_single");

            assertSql(serverMain.getEngine(), "SELECT count() FROM symbol_coltop_single", "count()\n" + totalRows + "\n");

            // Verify column top rows have NULL symbols
            assertSql(serverMain.getEngine(),
                    "SELECT count() FROM symbol_coltop_single WHERE sym IS NULL",
                    "count()\n" + initialRows + "\n");

            File parquetFile = new File(root, "symbol_coltop_single.parquet");
            exportToParquet(serverMain, "SELECT * FROM symbol_coltop_single", parquetFile);

            validateParquetFile(parquetFile, totalRows);

            serverMain.getEngine().execute("DROP TABLE symbol_coltop_single");
        }
    }

    /**
     * Tests symbol column with column top across multiple partitions exported to the same row group.
     * Creates partitions small enough that multiple fit in one row group (100k rows).
     */
    @Test
    public void testSymbolColumnTopMultiplePartitionsSameRowGroup() throws Exception {
        final int rowsPerPartition = 10000;
        final int initialPartitions = 3; // 30k rows with NULL symbols
        final int additionalPartitions = 2; // 20k rows with symbols
        final int totalRows = rowsPerPartition * (initialPartitions + additionalPartitions); // 50k total, fits in 1 row group

        try (final ServerMain serverMain = ServerMain.create(root)) {
            serverMain.start();

            // Create table with only timestamp column initially
            serverMain.getEngine().execute(
                    "CREATE TABLE symbol_coltop_multi (" +
                            "value INT, " +
                            "ts TIMESTAMP" +
                            ") TIMESTAMP(ts) PARTITION BY DAY"
            );

            // Insert initial rows across multiple partitions (each day is a partition)
            LOG.info().$("Inserting ").$(rowsPerPartition * initialPartitions).$(" initial rows across ")
                    .$(initialPartitions).$(" partitions...").$();
            for (int p = 0; p < initialPartitions; p++) {
                String date = "2024-01-0" + (p + 1);
                serverMain.getEngine().execute(
                        "INSERT INTO symbol_coltop_multi " +
                                "SELECT " +
                                "x + " + (p * rowsPerPartition) + " as value, " +
                                "timestamp_sequence('" + date + "', 1000000) as ts " +
                                "FROM long_sequence(" + rowsPerPartition + ")"
                );
            }

            serverMain.awaitTable("symbol_coltop_multi");

            // Add symbol column - creates column top for all existing rows across all partitions
            LOG.info().$("Adding symbol column (creates column top across all partitions)...").$();
            serverMain.getEngine().execute(
                    "ALTER TABLE symbol_coltop_multi ADD COLUMN sym SYMBOL"
            );

            // Insert additional rows with symbol values in new partitions
            LOG.info().$("Inserting ").$(rowsPerPartition * additionalPartitions).$(" rows with symbols across ")
                    .$(additionalPartitions).$(" new partitions...").$();
            for (int p = 0; p < additionalPartitions; p++) {
                String date = "2024-01-0" + (initialPartitions + p + 1);
                serverMain.getEngine().execute(
                        "INSERT INTO symbol_coltop_multi (value, ts, sym) " +
                                "SELECT " +
                                "x + " + ((initialPartitions + p) * rowsPerPartition) + " as value, " +
                                "timestamp_sequence('" + date + "', 1000000) as ts, " +
                                "rnd_symbol('ONE', 'TWO', 'THREE') as sym " +
                                "FROM long_sequence(" + rowsPerPartition + ")"
                );
            }

            serverMain.awaitTable("symbol_coltop_multi");

            assertSql(serverMain.getEngine(), "SELECT count() FROM symbol_coltop_multi", "count()\n" + totalRows + "\n");

            // Verify column top rows have NULL symbols
            int expectedNulls = rowsPerPartition * initialPartitions;
            assertSql(serverMain.getEngine(),
                    "SELECT count() FROM symbol_coltop_multi WHERE sym IS NULL",
                    "count()\n" + expectedNulls + "\n");

            File parquetFile = new File(root, "symbol_coltop_multi.parquet");
            exportToParquet(serverMain, "SELECT * FROM symbol_coltop_multi", parquetFile);

            // Validate - all 50k rows should fit in a single row group
            validateParquetFileRowGroups(parquetFile, totalRows, 1);

            serverMain.getEngine().execute("DROP TABLE symbol_coltop_multi");
        }
    }

    /**
     * Tests symbol column with column top across multiple partitions spanning multiple row groups.
     * This test creates a complex scenario:
     * 1. Insert initial rows into partitions 2024-01-01, 2024-01-02, 2024-01-03
     * 2. Add symbol column (creates column top for all existing rows)
     * 3. Insert more rows into SAME partitions 2024-01-02, 2024-01-03 (column top + data in same partition)
     * 4. Insert into new partition 2024-01-04
     * 5. Insert into BACKDATED partition 2023-12-15 (before column top partitions but has symbol data)
     */
    @Test
    public void testSymbolColumnTopMultiplePartitionsMultipleRowGroups() throws Exception {
        final int rowsPerInsert = 25000;

        try (final ServerMain serverMain = ServerMain.create(root)) {
            serverMain.start();

            // Create table with only timestamp column initially
            serverMain.getEngine().execute(
                    "CREATE TABLE symbol_coltop_multi_rg (" +
                            "value INT, " +
                            "ts TIMESTAMP" +
                            ") TIMESTAMP(ts) PARTITION BY DAY"
            );

            // Step 1: Insert initial rows into 3 partitions (75k rows total, all will have NULL symbols)
            LOG.info().$("Step 1: Inserting initial rows into 2024-01-01, 2024-01-02, 2024-01-03...").$();
            serverMain.getEngine().execute(
                    "INSERT INTO symbol_coltop_multi_rg " +
                            "SELECT x as value, timestamp_sequence('2024-01-01', 1000000) as ts " +
                            "FROM long_sequence(" + rowsPerInsert + ")"
            );
            serverMain.getEngine().execute(
                    "INSERT INTO symbol_coltop_multi_rg " +
                            "SELECT x + " + rowsPerInsert + " as value, timestamp_sequence('2024-01-02', 1000000) as ts " +
                            "FROM long_sequence(" + rowsPerInsert + ")"
            );
            serverMain.getEngine().execute(
                    "INSERT INTO symbol_coltop_multi_rg " +
                            "SELECT x + " + (2 * rowsPerInsert) + " as value, timestamp_sequence('2024-01-03', 1000000) as ts " +
                            "FROM long_sequence(" + rowsPerInsert + ")"
            );
            serverMain.awaitTable("symbol_coltop_multi_rg");
            int colTopRows = 3 * rowsPerInsert; // 75k rows with NULL symbols

            // Step 2: Add symbol column - creates column top for all 75k existing rows
            //noinspection SuspiciousNameCombination
            LOG.info().$("Step 2: Adding symbol column (creates column top for ").$(colTopRows).$(" rows)...").$();
            serverMain.getEngine().execute(
                    "ALTER TABLE symbol_coltop_multi_rg ADD COLUMN sym SYMBOL"
            );

            // Step 3: Insert more rows into SAME partitions 2024-01-02 and 2024-01-03
            // This creates partitions with mixed data: column top (NULLs) + actual symbols
            // Use deterministic pattern: CASE WHEN x % 3 = 0 THEN 'X' WHEN x % 3 = 1 THEN 'Y' ELSE 'Z' END
            LOG.info().$("Step 3: Inserting rows with symbols into same partitions 2024-01-02, 2024-01-03...").$();
            serverMain.getEngine().execute(
                    "INSERT INTO symbol_coltop_multi_rg (value, ts, sym) " +
                            "SELECT x + " + (3 * rowsPerInsert) + " as value, " +
                            "timestamp_sequence('2024-01-02T10:00:00', 1000000) as ts, " +
                            "CASE WHEN x % 3 = 0 THEN 'X' WHEN x % 3 = 1 THEN 'Y' ELSE 'Z' END as sym " +
                            "FROM long_sequence(" + rowsPerInsert + ")"
            );
            serverMain.getEngine().execute(
                    "INSERT INTO symbol_coltop_multi_rg (value, ts, sym) " +
                            "SELECT x + " + (4 * rowsPerInsert) + " as value, " +
                            "timestamp_sequence('2024-01-03T10:00:00', 1000000) as ts, " +
                            "CASE WHEN x % 3 = 0 THEN 'X' WHEN x % 3 = 1 THEN 'Y' ELSE 'Z' END as sym " +
                            "FROM long_sequence(" + rowsPerInsert + ")"
            );
            serverMain.awaitTable("symbol_coltop_multi_rg");

            // Step 4: Insert into new partition 2024-01-04
            LOG.info().$("Step 4: Inserting rows into new partition 2024-01-04...").$();
            serverMain.getEngine().execute(
                    "INSERT INTO symbol_coltop_multi_rg (value, ts, sym) " +
                            "SELECT x + " + (5 * rowsPerInsert) + " as value, " +
                            "timestamp_sequence('2024-01-04', 1000000) as ts, " +
                            "CASE WHEN x % 3 = 0 THEN 'X' WHEN x % 3 = 1 THEN 'Y' ELSE 'Z' END as sym " +
                            "FROM long_sequence(" + rowsPerInsert + ")"
            );
            serverMain.awaitTable("symbol_coltop_multi_rg");

            // Step 5: Insert into BACKDATED partition 2023-12-15
            // This partition is chronologically before the column top partitions but has symbol data
            // Use pattern: CASE WHEN x % 3 = 0 THEN 'A' WHEN x % 3 = 1 THEN 'B' ELSE 'C' END
            LOG.info().$("Step 5: Inserting rows into backdated partition 2023-12-15...").$();
            serverMain.getEngine().execute(
                    "INSERT INTO symbol_coltop_multi_rg (value, ts, sym) " +
                            "SELECT x + " + (6 * rowsPerInsert) + " as value, " +
                            "timestamp_sequence('2023-12-15', 1000000) as ts, " +
                            "CASE WHEN x % 3 = 0 THEN 'A' WHEN x % 3 = 1 THEN 'B' ELSE 'C' END as sym " +
                            "FROM long_sequence(" + rowsPerInsert + ")"
            );
            serverMain.awaitTable("symbol_coltop_multi_rg");

            int totalRows = 7 * rowsPerInsert; // 175k rows total

            assertSql(serverMain.getEngine(), "SELECT count() FROM symbol_coltop_multi_rg", "count()\n" + totalRows + "\n");

            // Verify column top rows have NULL symbols (only the initial 75k rows)
            assertSql(serverMain.getEngine(),
                    "SELECT count() FROM symbol_coltop_multi_rg WHERE sym IS NULL",
                    "count()\n" + colTopRows + "\n");

            // Verify partition structure:
            // - 2023-12-15: 25k rows with symbols A/B/C (backdated), value = 6*25k + x
            // - 2024-01-01: 25k rows with NULL symbols (pure column top), value = x
            // - 2024-01-02: 25k NULL (value=25k+x) + 25k symbols X/Y/Z (value=3*25k+x) = 50k rows (mixed)
            // - 2024-01-03: 25k NULL (value=2*25k+x) + 25k symbols X/Y/Z (value=4*25k+x) = 50k rows (mixed)
            // - 2024-01-04: 25k rows with symbols X/Y/Z (value=5*25k+x)

            File parquetFile = new File(root, "symbol_coltop_multi_rg.parquet");
            exportToParquet(serverMain, "SELECT * FROM symbol_coltop_multi_rg", parquetFile);

            // Validate the parquet file structure and symbol values
            Configuration conf = new Configuration();
            org.apache.hadoop.fs.Path hadoopPath = new org.apache.hadoop.fs.Path(parquetFile.getAbsolutePath());
            InputFile inputFile = HadoopInputFile.fromPath(hadoopPath, conf);

            // First validate row count and row groups using ParquetFileReader
            try (ParquetFileReader fileReader = ParquetFileReader.open(inputFile)) {
                ParquetMetadata metadata = fileReader.getFooter();
                List<BlockMetaData> rowGroups = metadata.getBlocks();

                // Log the schema to help diagnose any issues
                org.apache.parquet.schema.MessageType schema = metadata.getFileMetaData().getSchema();
                LOG.info().$("Parquet schema: ").$(schema.toString()).$();

                LOG.info().$("Parquet file has ").$(rowGroups.size()).$(" row groups").$();

                long fileRowCount = 0;
                for (int i = 0; i < rowGroups.size(); i++) {
                    BlockMetaData rg = rowGroups.get(i);
                    LOG.info().$("  Row group ").$(i).$(": ").$(rg.getRowCount()).$(" rows").$();
                    fileRowCount += rg.getRowCount();

                    // Read the row group to validate it's not corrupt
                    fileReader.readRowGroup(i);
                }
                Assert.assertEquals("Total row count mismatch", totalRows, fileRowCount);
            }

            // Note: AvroParquetReader fails with "Empty name" because QuestDB exports
            // the Parquet schema with an empty root message name (see schema logged above).
            // This is a separate bug. For now, we validate using ParquetFileReader which
            // confirms the file structure is valid and readable.
            //
            // Once the empty schema name bug is fixed, add value validation using AvroParquetReader
            // to verify symbol values match expected pattern:
            // - value 1-75000: NULL (column top)
            // - value 75001-100000: X/Y/Z pattern
            // - value 100001-125000: X/Y/Z pattern
            // - value 125001-150000: X/Y/Z pattern
            // - value 150001-175000: A/B/C pattern (backdated)

            serverMain.getEngine().execute("DROP TABLE symbol_coltop_multi_rg");
        }
    }

    /**
     * Tests symbol columns with nulls across multiple row groups.
     */
    @Test
    public void testSymbolColumnWithNulls() throws Exception {
        final int rowCount = 150001; // Creates 2 row groups: 100k + 50001

        try (final ServerMain serverMain = ServerMain.create(root)) {
            serverMain.start();

            serverMain.getEngine().execute(
                    "CREATE TABLE symbol_nulls_test (" +
                            "sym SYMBOL, " +
                            "ts TIMESTAMP" +
                            ") TIMESTAMP(ts) PARTITION BY DAY"
            );

            // Insert rows with some nulls (using CASE to create nulls)
            LOG.info().$("Inserting ").$(rowCount).$(" rows with nulls...").$();
            serverMain.getEngine().execute(
                    "INSERT INTO symbol_nulls_test " +
                            "SELECT " +
                            "CASE WHEN x % 10 = 0 THEN NULL ELSE rnd_symbol('A','B','C') END as sym, " +
                            "timestamp_sequence('2024-01-01', 1000000) as ts " +
                            "FROM long_sequence(" + rowCount + ")"
            );

            // Wait for WAL to be applied
            serverMain.awaitTable("symbol_nulls_test");

            assertSql(serverMain.getEngine(), "SELECT count() FROM symbol_nulls_test", "count()\n" + rowCount + "\n");

            File parquetFile = new File(root, "symbol_nulls_test.parquet");
            exportToParquet(serverMain, "SELECT * FROM symbol_nulls_test", parquetFile);

            validateParquetFile(parquetFile, rowCount);

            serverMain.getEngine().execute("DROP TABLE symbol_nulls_test");
        }
    }

    private void exportToParquet(ServerMain serverMain, String query, File outputFile) throws IOException {
        String encodedQuery = URLEncoder.encode(query, StandardCharsets.UTF_8);
        String url = "/exp?fmt=parquet&query=" + encodedQuery;

        try (HttpClient httpClient = HttpClientFactory.newPlainTextInstance(new DefaultHttpClientConfiguration())) {
            HttpClient.Request request = httpClient.newRequest("localhost", serverMain.getHttpServerPort());
            request.GET().url(url);

            try (HttpClient.ResponseHeaders response = request.send(120_000)) { // 2 minute timeout
                response.await();

                int statusCode = Numbers.parseInt(response.getStatusCode().asAsciiCharSequence());
                if (statusCode != 200) {
                    throw new IOException("HTTP export failed with status " + statusCode);
                }

                // Read response body to file
                try (FileOutputStream fos = new FileOutputStream(outputFile)) {
                    Response body = response.getResponse();
                    Fragment fragment;
                    while ((fragment = body.recv()) != null) {
                        long lo = fragment.lo();
                        long hi = fragment.hi();
                        int len = (int) (hi - lo);
                        byte[] buf = new byte[len];
                        for (int i = 0; i < len; i++) {
                            buf[i] = Unsafe.getUnsafe().getByte(lo + i);
                        }
                        fos.write(buf);
                    }
                }
            }
        }

        if (!outputFile.exists() || outputFile.length() == 0) {
            throw new IOException("Parquet file was not created or is empty");
        }
    }

    private void validateParquetFile(File parquetFile, int expectedRowCount) throws IOException {
        Configuration conf = new Configuration();
        org.apache.hadoop.fs.Path hadoopPath = new org.apache.hadoop.fs.Path(parquetFile.getAbsolutePath());
        InputFile inputFile = HadoopInputFile.fromPath(hadoopPath, conf);

        // Validate file structure and read all row groups
        // This is where PyArrow would fail before the fix with:
        // - "Invalid number of indices: 0" (RLE encoding bug)
        // - "Column cannot have more than one dictionary" (multiple dict pages bug)
        try (ParquetFileReader reader = ParquetFileReader.open(inputFile)) {
            ParquetMetadata metadata = reader.getFooter();
            List<BlockMetaData> rowGroups = metadata.getBlocks();

            LOG.info().$("Parquet file has ").$(rowGroups.size()).$(" row groups").$();

            long totalRows = 0;
            for (int i = 0; i < rowGroups.size(); i++) {
                BlockMetaData rg = rowGroups.get(i);
                LOG.info().$("  Row group ").$(i).$(": ").$(rg.getRowCount()).$(" rows").$();
                totalRows += rg.getRowCount();

                // Read the row group to validate it's not corrupt
                // This exercises the dictionary and RLE decoding
                reader.readRowGroup(i);
            }

            Assert.assertEquals("Total row count mismatch", expectedRowCount, totalRows);
            Assert.assertTrue("Expected multiple row groups", rowGroups.size() > 1);

            LOG.info().$("Successfully validated all ").$(rowGroups.size()).$(" row groups with ")
                    .$(totalRows).$(" total rows").$();
        }
    }

    private void validateParquetFileRowGroups(File parquetFile, int expectedRowCount, int expectedRowGroups) throws IOException {
        Configuration conf = new Configuration();
        org.apache.hadoop.fs.Path hadoopPath = new org.apache.hadoop.fs.Path(parquetFile.getAbsolutePath());
        InputFile inputFile = HadoopInputFile.fromPath(hadoopPath, conf);

        try (ParquetFileReader reader = ParquetFileReader.open(inputFile)) {
            ParquetMetadata metadata = reader.getFooter();
            List<BlockMetaData> rowGroups = metadata.getBlocks();

            LOG.info().$("Parquet file has ").$(rowGroups.size()).$(" row groups").$();

            long totalRows = 0;
            for (int i = 0; i < rowGroups.size(); i++) {
                BlockMetaData rg = rowGroups.get(i);
                LOG.info().$("  Row group ").$(i).$(": ").$(rg.getRowCount()).$(" rows").$();
                totalRows += rg.getRowCount();

                // Read the row group to validate it's not corrupt
                reader.readRowGroup(i);
            }

            Assert.assertEquals("Total row count mismatch", expectedRowCount, totalRows);
            Assert.assertEquals("Row group count mismatch", expectedRowGroups, rowGroups.size());

            LOG.info().$("Successfully validated all ").$(rowGroups.size()).$(" row groups with ")
                    .$(totalRows).$(" total rows").$();
        }
    }
}
