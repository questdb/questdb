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
}
