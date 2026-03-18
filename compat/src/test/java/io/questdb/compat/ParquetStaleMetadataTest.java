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
import io.questdb.cairo.ColumnType;
import io.questdb.cutlass.http.client.Fragment;
import io.questdb.cutlass.http.client.HttpClient;
import io.questdb.cutlass.http.client.HttpClientFactory;
import io.questdb.cutlass.http.client.Response;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.std.Numbers;
import io.questdb.std.Unsafe;
import org.junit.Assert;
import org.junit.Test;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;

/**
 * Tests that QuestDB's read_parquet correctly handles Parquet files whose
 * embedded QDB metadata schema has a different number of columns than the
 * actual Parquet data. This happens when an external tool (e.g. DuckDB, Spark)
 * rewrites a QuestDB-exported Parquet file — dropping partition columns from
 * the data but preserving the original key-value metadata.
 */
public class ParquetStaleMetadataTest extends AbstractTest {
    private static final Log LOG = LogFactory.getLog(ParquetStaleMetadataTest.class);

    /**
     * Exports a 3-column table to parquet (QDB metadata has 3-column schema),
     * then replaces the QDB metadata with a 2-column schema (simulating an
     * external tool that added a column to the file data but kept the old
     * metadata). Verifies that read_parquet discards the mismatched metadata,
     * falls back to physical type inference, and returns all 3 data columns.
     */
    @Test
    public void testReadParquetWithStaleQdbMetadata() throws Exception {
        File importDir = new File(root, "import");
        Assert.assertTrue(importDir.mkdirs());

        Map<String, String> env = new HashMap<>();
        env.put("QDB_CAIRO_SQL_COPY_ROOT", importDir.getAbsolutePath());
        env.put("QDB_HTTP_BIND_TO", "0.0.0.0:0");
        env.put("QDB_HTTP_MIN_BIND_TO", "0.0.0.0:0");
        env.put("QDB_LINE_TCP_NET_BIND_TO", "0.0.0.0:0");
        env.put("QDB_PG_NET_BIND_TO", "0.0.0.0:0");

        try (final ServerMain serverMain = ServerMain.create(root, env)) {
            serverMain.start();

            // Create a 3-column table.
            serverMain.getEngine().execute("""
                    CREATE TABLE test_table (
                        ts TIMESTAMP,
                        category VARCHAR,
                        value DOUBLE
                    ) TIMESTAMP(ts) PARTITION BY DAY
                    """);
            serverMain.getEngine().execute("""
                    INSERT INTO test_table
                    SELECT
                        timestamp_sequence('2024-01-01', 1_000_000) AS ts,
                        'CAT-' || (x % 3) AS category,
                        x * 1.5 AS value
                    FROM long_sequence(10)
                    """);
            serverMain.awaitTable("test_table");

            // Export to parquet via HTTP. The file has 3 data columns and
            // 3-column QDB metadata (ts as designated timestamp, category
            // as varchar, value as double).
            File parquetFile = new File(importDir, "test.parquet");
            exportToParquet(serverMain, "SELECT * FROM test_table", parquetFile);

            // Inject stale QDB metadata: replace the 3-column schema with a
            // 2-column schema. This simulates a tool that added a column to
            // the Parquet data but preserved the old metadata. The schema
            // length mismatch (2 != 3) triggers the fix, which discards the
            // stale metadata and falls back to physical type inference.
            injectStaleMetadata(parquetFile);

            // Read the modified file. Without the fix, the decoder would use
            // the stale 2-column metadata for a 3-column file, causing type
            // mismatches (e.g. Double column decoded as Varchar).
            String jsonResult = executeQuery(
                    serverMain,
                    "SELECT * FROM read_parquet('test.parquet')"
            );
            LOG.info().$("read_parquet result: ").$(jsonResult).$();

            // The HTTP /exec endpoint returns JSON. Verify the response
            // contains the expected columns and data.
            Assert.assertTrue(
                    "result should contain column 'ts'",
                    jsonResult.contains("\"name\":\"ts\"")
            );
            Assert.assertTrue(
                    "result should contain column 'category'",
                    jsonResult.contains("\"name\":\"category\"")
            );
            Assert.assertTrue(
                    "result should contain column 'value'",
                    jsonResult.contains("\"name\":\"value\"")
            );
            // Verify data: first row has value = 1 * 1.5 = 1.5
            Assert.assertTrue(
                    "result should contain data value 1.5",
                    jsonResult.contains("1.5")
            );

            // Verify row count: the /exec JSON contains "count":N
            Assert.assertTrue(
                    "result should contain 10 rows",
                    jsonResult.contains("\"count\":10")
            );

            // Also verify the column count from the response metadata.
            // The JSON columns array should have exactly 3 entries.
            int colCount = countOccurrences(jsonResult, "\"name\":");
            Assert.assertEquals("Expected 3 columns", 3, colCount);

            serverMain.getEngine().execute("DROP TABLE test_table");
        }
    }

    /**
     * Replaces the 3-column QDB metadata in the file with a 2-column version,
     * padded to the same byte length. The Thrift framing is preserved because
     * the replacement is the same number of bytes.
     */
    private void injectStaleMetadata(File parquetFile) throws IOException {
        byte[] fileBytes = java.nio.file.Files.readAllBytes(parquetFile.toPath());
        String fileStr = new String(fileBytes, StandardCharsets.ISO_8859_1);

        // Find the QDB metadata JSON in the footer.
        String marker = "\"version\":1,\"schema\":[{";
        int markerIdx = fileStr.indexOf(marker);
        Assert.assertTrue(
                "QDB metadata not found in exported parquet file",
                markerIdx >= 0
        );

        // Walk backwards to the opening '{' of the JSON object.
        int jsonStart = markerIdx - 1;
        while (jsonStart > 0 && fileStr.charAt(jsonStart) != '{') {
            jsonStart--;
        }

        // Walk forwards with brace-depth tracking to find the closing '}'.
        int depth = 0;
        int jsonEnd = jsonStart;
        for (int i = jsonStart; i < fileStr.length(); i++) {
            char c = fileStr.charAt(i);
            if (c == '{') {
                depth++;
            } else if (c == '}') {
                depth--;
                if (depth == 0) {
                    jsonEnd = i + 1;
                    break;
                }
            }
        }
        Assert.assertTrue("Failed to find end of QDB metadata JSON", jsonEnd > jsonStart);

        String originalJson = fileStr.substring(jsonStart, jsonEnd);
        int originalLen = originalJson.length();
        LOG.info().$("Original QDB metadata (").$(originalLen).$(" bytes): ").$(originalJson).$();

        // Build a 2-column stale metadata (ts + value, dropping category).
        // ColumnType.TIMESTAMP = 8, designated timestamp flag = 1 << 17 = 131072
        // Designated ascending = 8 | 131072 = 131080
        // ColumnType.DOUBLE = 10
        String staleJson =
                "{\"version\":1,\"schema\":[" +
                        "{\"column_type\":" + (ColumnType.TIMESTAMP | (1 << 17)) + ",\"column_top\":0}," +
                        "{\"column_type\":" + ColumnType.DOUBLE + ",\"column_top\":0}" +
                        "]}";

        // Pad with whitespace before the closing "]}" to match the original length.
        // JSON allows arbitrary whitespace between tokens.
        Assert.assertTrue(
                "Stale metadata (" + staleJson.length() + " bytes) must not exceed original ("
                        + originalLen + " bytes)",
                staleJson.length() <= originalLen
        );

        StringBuilder padded = new StringBuilder(staleJson);
        // Insert spaces before the final "]}"
        int insertPos = padded.length() - 2;
        while (padded.length() < originalLen) {
            padded.insert(insertPos, ' ');
        }
        String paddedStale = padded.toString();
        Assert.assertEquals(
                "Padded stale metadata must be exactly the same length as original",
                originalLen, paddedStale.length()
        );

        LOG.info().$("Stale QDB metadata (").$(paddedStale.length()).$(" bytes): ").$(paddedStale).$();

        // Binary-replace the metadata in place. Because the byte length is
        // identical, the Thrift string framing and the Parquet footer length
        // remain valid.
        byte[] staleBytes = paddedStale.getBytes(StandardCharsets.ISO_8859_1);
        System.arraycopy(staleBytes, 0, fileBytes, jsonStart, staleBytes.length);
        java.nio.file.Files.write(parquetFile.toPath(), fileBytes);

        LOG.info().$("Injected stale 2-column QDB metadata into 3-column parquet file").$();
    }

    private void exportToParquet(ServerMain serverMain, String query, File outputFile) throws IOException {
        String encodedQuery = URLEncoder.encode(query, StandardCharsets.UTF_8);
        String url = "/exp?fmt=parquet&query=" + encodedQuery;

        try (HttpClient httpClient = HttpClientFactory.newPlainTextInstance(new DefaultHttpClientConfiguration())) {
            HttpClient.Request request = httpClient.newRequest("localhost", serverMain.getHttpServerPort());
            request.GET().url(url);

            try (HttpClient.ResponseHeaders response = request.send(120_000)) {
                response.await();

                int statusCode = Numbers.parseInt(response.getStatusCode().asAsciiCharSequence());
                if (statusCode != 200) {
                    throw new IOException("HTTP export failed with status " + statusCode);
                }

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

    private String executeQuery(ServerMain serverMain, String query) throws IOException {
        String encodedQuery = URLEncoder.encode(query, StandardCharsets.UTF_8);
        String url = "/exec?query=" + encodedQuery;

        try (HttpClient httpClient = HttpClientFactory.newPlainTextInstance(new DefaultHttpClientConfiguration())) {
            HttpClient.Request request = httpClient.newRequest("localhost", serverMain.getHttpServerPort());
            request.GET().url(url);

            try (HttpClient.ResponseHeaders response = request.send(120_000)) {
                response.await();

                int statusCode = Numbers.parseInt(response.getStatusCode().asAsciiCharSequence());
                StringBuilder sb = new StringBuilder();
                Response body = response.getResponse();
                Fragment fragment;
                while ((fragment = body.recv()) != null) {
                    long lo = fragment.lo();
                    long hi = fragment.hi();
                    for (long p = lo; p < hi; p++) {
                        sb.append((char) Unsafe.getUnsafe().getByte(p));
                    }
                }

                if (statusCode != 200) {
                    throw new IOException("Query failed with status " + statusCode + ": " + sb);
                }

                return sb.toString();
            }
        }
    }

    private static int countOccurrences(String haystack, String needle) {
        int count = 0;
        int idx = 0;
        while ((idx = haystack.indexOf(needle, idx)) != -1) {
            count++;
            idx += needle.length();
        }
        return count;
    }
}
