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

package io.questdb.test.tools;

import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.CairoEngine;
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.ParquetMetaFileReader;
import io.questdb.cairo.TableUtils;
import io.questdb.cairo.view.ViewCompilerJob;
import io.questdb.cairo.vm.MemoryCARWImpl;
import io.questdb.cutlass.text.CopyExportContext;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.engine.functions.constants.VarcharConstant;
import io.questdb.griffin.engine.table.ParquetRowGroupFilter;
import io.questdb.griffin.engine.table.PushdownFilterExtractor;
import io.questdb.griffin.engine.table.parquet.ParquetEncoding;
import io.questdb.griffin.engine.table.parquet.ParquetFileDecoder;
import io.questdb.griffin.engine.table.parquet.ParquetMetadataWriter;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.std.DirectLongList;
import io.questdb.std.Files;
import io.questdb.std.FilesFacade;
import io.questdb.std.MemoryTag;
import io.questdb.std.ObjList;
import io.questdb.std.Unsafe;
import io.questdb.std.str.Path;
import io.questdb.test.QueryAssertion;
import org.junit.Assert;

import java.util.function.Consumer;

public final class ParquetTestUtils {
    // the copy_export_log phases and statuses of an export that streams its rows and succeeds
    public static final String STREAMING_EXPORT_LOG = """
            phase\tstatus
            wait_to_run\tstarted
            wait_to_run\tfinished
            stream_sending_data\tstarted
            stream_sending_data\tfinished
            move_files\tstarted
            move_files\tfinished
            success\tfinished
            """;
    // the copy_export_log phases and statuses of an export that runs through a temp table and succeeds
    public static final String TEMP_TABLE_EXPORT_LOG = """
            phase\tstatus
            wait_to_run\tstarted
            wait_to_run\tfinished
            populating_data_to_temp_table\tstarted
            populating_data_to_temp_table\tfinished
            converting_partitions\tstarted
            converting_partitions\tfinished
            move_files\tstarted
            move_files\tfinished
            dropping_temp_table\tstarted
            dropping_temp_table\tfinished
            success\tfinished
            """;
    private static final Log LOG = LogFactory.getLog(ParquetTestUtils.class);
    // the rows of the tables that createTableToRetype() creates: two symbols alternate inside each of three days, and
    // other_ts holds NULL in two rows
    private static final String RETYPED_TABLE_ROWS = """
            ('a', 'p', 1, '2023-06-05T00:00:00.000000Z', '2024-01-01T00:00:00.000000Z'),
            ('b', 'q', 2, NULL, '2024-01-01T01:00:00.000000Z'),
            ('a', 'q', 3, '2023-06-03T00:00:00.000000Z', '2024-01-01T02:00:00.000000Z'),
            ('b', 'p', 4, '2023-06-02T00:00:00.000000Z', '2024-01-01T03:00:00.000000Z'),
            ('a', 'p', 5, '2023-06-09T00:00:00.000000Z', '2024-01-02T00:00:00.000000Z'),
            ('b', 'q', 6, NULL, '2024-01-02T01:00:00.000000Z'),
            ('a', 'q', 7, '2023-06-07T00:00:00.000000Z', '2024-01-02T02:00:00.000000Z'),
            ('b', 'p', 8, '2023-06-06T00:00:00.000000Z', '2024-01-02T03:00:00.000000Z'),
            ('a', 'p', 9, '2023-06-10T00:00:00.000000Z', '2024-01-03T00:00:00.000000Z'),
            ('b', 'q', 10, '2023-06-11T00:00:00.000000Z', '2024-01-03T01:00:00.000000Z')
            """;

    private ParquetTestUtils() {
    }

    /**
     * Asserts that both files have the same row groups and columns, and that each column of each row group lists
     * the same encodings in both.
     */
    public static void assertColumnEncodingsMatch(String expectedParquetFilePath, String actualParquetFilePath, FilesFacade ff) {
        withDecoder(expectedParquetFilePath, ff, expected -> withDecoder(actualParquetFilePath, ff, actual -> {
            final int rowGroupCount = expected.metadata().getRowGroupCount();
            Assert.assertTrue("expected parquet file to contain at least one row group", rowGroupCount > 0);
            Assert.assertEquals("row group count", rowGroupCount, actual.metadata().getRowGroupCount());
            final int columnCount = expected.metadata().getColumnCount();
            Assert.assertEquals("column count", columnCount, actual.metadata().getColumnCount());
            for (int rowGroupIndex = 0; rowGroupIndex < rowGroupCount; rowGroupIndex++) {
                for (int columnIndex = 0; columnIndex < columnCount; columnIndex++) {
                    for (int encodingId = ParquetEncoding.ENCODING_PLAIN; encodingId < ParquetEncoding.MAX_ENUM_INT; encodingId++) {
                        Assert.assertEquals(
                                ParquetEncoding.getEncodingName(encodingId) + " encoding in row group " + rowGroupIndex
                                        + ", column " + columnIndex,
                                expected.rowGroupColumnHasEncoding(rowGroupIndex, columnIndex, encodingId),
                                actual.rowGroupColumnHasEncoding(rowGroupIndex, columnIndex, encodingId)
                        );
                    }
                }
            }
        }));
    }

    /**
     * Asserts the maximum definition level of a column of the file: 0 for a REQUIRED column, which holds no NULL, and 1
     * for an OPTIONAL one. {@link ParquetMetadataWriter#generate} parses the footer of the file, independent of the
     * writer that wrote it, into a {@code _pm} file next to it, which the assertion reads and removes.
     */
    public static void assertColumnMaxDefLevel(String parquetFilePath, FilesFacade ff, int columnIndex, int expectedMaxDefLevel) {
        final ParquetMetaFileReader reader = new ParquetMetaFileReader();
        try (Path parquetPath = new Path(); Path metaPath = new Path()) {
            parquetPath.of(parquetFilePath).$();
            metaPath.of(parquetFilePath).put("_pm").$();
            final long parquetFileSize;
            final long parquetFd = TableUtils.openRO(ff, parquetPath.$(), LOG);
            try {
                parquetFileSize = ff.length(parquetFd);
                final long metaFd = ff.openRW(metaPath.$(), CairoConfiguration.O_NONE);
                Assert.assertTrue("could not create " + metaPath, metaFd > -1);
                try {
                    ParquetMetadataWriter.generate(
                            Unsafe.getNativeAllocator(MemoryTag.NATIVE_PARQUET_PARTITION_DECODER),
                            Files.toOsFd(parquetFd),
                            parquetFileSize,
                            Files.toOsFd(metaFd)
                    );
                } finally {
                    ff.close(metaFd);
                }
            } finally {
                ff.close(parquetFd);
            }
            final long address = ParquetMetaFileReader.openAndMapRO(ff, metaPath.$(), reader);
            Assert.assertNotEquals("could not map " + metaPath, 0, address);
            final long mapSize = reader.getFileSize();
            try {
                Assert.assertTrue("the footer must resolve", reader.resolveFooter(parquetFileSize));
                Assert.assertEquals(
                        "max definition level of column " + columnIndex,
                        expectedMaxDefLevel,
                        reader.getColumnMaxDefLevel(columnIndex)
                );
            } finally {
                reader.clear();
                ff.munmap(address, mapSize, MemoryTag.MMAP_PARQUET_METADATA_READER);
                ff.removeQuiet(metaPath.$());
            }
        }
    }

    /**
     * Asserts that the QuestDB metadata of the file gives each listed column the type {@code columnType}.
     */
    public static void assertColumnTypes(String parquetFilePath, FilesFacade ff, int columnType, int... columnIndexes) {
        withDecoder(parquetFilePath, ff, decoder -> {
            for (int columnIndex : columnIndexes) {
                Assert.assertEquals(
                        "type of column " + columnIndex,
                        ColumnType.nameOf(columnType),
                        ColumnType.nameOf(decoder.metadata().getColumnType(columnIndex))
                );
            }
        });
    }

    public static void assertColumnsDoNotUseDictionaryEncoding(String parquetFilePath, FilesFacade ff, int... columnIndexes) {
        assertEncoding(parquetFilePath, ff, ParquetEncoding.ENCODING_RLE_DICTIONARY, false, columnIndexes);
    }

    public static void assertColumnsDoNotUseEncoding(String parquetFilePath, FilesFacade ff, int encodingId, int... columnIndexes) {
        assertEncoding(parquetFilePath, ff, encodingId, false, columnIndexes);
    }

    public static void assertColumnsUseDictionaryEncoding(String parquetFilePath, FilesFacade ff, int... columnIndexes) {
        assertEncoding(parquetFilePath, ff, ParquetEncoding.ENCODING_RLE_DICTIONARY, true, columnIndexes);
    }

    public static void assertColumnsUseEncoding(String parquetFilePath, FilesFacade ff, int encodingId, int... columnIndexes) {
        assertEncoding(parquetFilePath, ff, encodingId, true, columnIndexes);
    }

    /**
     * Asserts that a file holding the columns of a table from {@link #createTableWithParquetSettingPerType}, in their
     * order, encodes each column with the encoding its PARQUET(...) setting names rather than with its type's default.
     * BOOLEAN and DOUBLE[] have no encoding other than their default and SYMBOL always uses a dictionary, so the file
     * cannot show their settings.
     */
    public static void assertColumnsUseParquetSettingPerType(String parquetFilePath, FilesFacade ff) {
        assertColumnsUseParquetSettingPerType(parquetFilePath, ff, true);
    }

    /**
     * Same as {@link #assertColumnsUseParquetSettingPerType(String, FilesFacade)}, for a file that holds the columns
     * without bin when {@code hasBinColumn} is false, so that each column after bin sits one position lower.
     */
    public static void assertColumnsUseParquetSettingPerType(String parquetFilePath, FilesFacade ff, boolean hasBinColumn) {
        final int shift = hasBinColumn ? 0 : 1;
        // b, sh, ch, l, dt, t, ip, g1, g2, g4 and g8 default to PLAIN
        assertColumnsUseEncoding(parquetFilePath, ff, ParquetEncoding.ENCODING_DELTA_BINARY_PACKED, 0, 1, 2, 4, 5, 6, 12, 13, 14, 15, 16);
        // i, tn, f, d, u, l256, the six decimals and bfe default to PLAIN, s and vc to DELTA_LENGTH_BYTE_ARRAY
        assertColumnsUseEncoding(parquetFilePath, ff, ParquetEncoding.ENCODING_RLE_DICTIONARY, 3, 7, 8, 9, 10, 11, 17, 18, 19, 20, 21, 22, 23, 24, 30 - shift);
        // the designated timestamp ts defaults to DELTA_BINARY_PACKED
        assertColumnsUseEncoding(parquetFilePath, ff, ParquetEncoding.ENCODING_PLAIN, 31 - shift);
        assertColumnsDoNotUseEncoding(parquetFilePath, ff, ParquetEncoding.ENCODING_DELTA_BINARY_PACKED, 31 - shift);
        if (hasBinColumn) {
            // bin defaults to DELTA_LENGTH_BYTE_ARRAY
            assertColumnsUseEncoding(parquetFilePath, ff, ParquetEncoding.ENCODING_PLAIN, 25);
            assertColumnsDoNotUseEncoding(parquetFilePath, ff, ParquetEncoding.ENCODING_DELTA_LENGTH_BYTE_ARRAY, 25);
        }
        // bf names no encoding, so it keeps the INT default, PLAIN
        assertColumnsDoNotUseEncoding(parquetFilePath, ff, ParquetEncoding.ENCODING_RLE_DICTIONARY, 29 - shift);
        assertColumnsDoNotUseEncoding(parquetFilePath, ff, ParquetEncoding.ENCODING_DELTA_BINARY_PACKED, 29 - shift);
    }

    // Asserts the phases and statuses that the export logged, in their order.
    public static void assertExportLog(CairoEngine engine, SqlExecutionContext executionContext, String exportId, String expectedPhases) throws Exception {
        new QueryAssertion(engine, executionContext, () -> {
        }, "SELECT phase, status FROM \"" + engine.getConfiguration().getSystemTableNamePrefix() + "copy_export_log\" WHERE id = '" + exportId + "'")
                .noLeakCheck()
                .returns(expectedPhases);
    }

    // Asserts that the export logged its success.
    public static void assertExportSucceeded(CairoEngine engine, SqlExecutionContext executionContext, String exportId) throws Exception {
        new QueryAssertion(engine, executionContext, () -> {
        }, "SELECT status FROM \"" + engine.getConfiguration().getSystemTableNamePrefix() + "copy_export_log\" WHERE id = '" + exportId + "' AND phase = 'success'")
                .noLeakCheck()
                .returns("status\nfinished\n");
    }

    /**
     * Waits until the active export has streamed rows to its file or client and returns its id, so that a test that
     * cancels the export or drops its connection afterwards stops it while it streams. Fails when the export streams
     * no rows.
     */
    public static long awaitExportStreamedRows(CairoEngine engine) throws Exception {
        final CopyExportContext copyExportContext = engine.getCopyExportContext();
        final CopyExportContext.ExportTaskData progress = new CopyExportContext.ExportTaskData();
        final long[] exportId = new long[1];
        TestUtils.assertEventually(() -> {
            exportId[0] = copyExportContext.getActiveExportId();
            Assert.assertTrue(
                    "the export must stream rows",
                    exportId[0] != CopyExportContext.INACTIVE_COPY_ID
                            && copyExportContext.getAndCopyEntry(exportId[0], progress)
                            && progress.getStreamingSendRowCount() > 0
            );
        });
        return exportId[0];
    }

    /**
     * Waits until no export is active: the export the test started has ended, whether it succeeded, failed or was
     * cancelled.
     */
    public static void awaitExportsDrained(CairoEngine engine) throws Exception {
        TestUtils.assertEventually(() -> Assert.assertEquals(
                "the export must end",
                CopyExportContext.INACTIVE_COPY_ID,
                engine.getCopyExportContext().getActiveExportId()
        ));
    }

    /**
     * Returns whether the min/max statistics and the bloom filter of a row group rule out {@code columnName = value}
     * for a SYMBOL column of the file, as the row group pruning of a table's Parquet partition asks.
     */
    public static boolean canSkipRowGroupOnSymbolEquals(
            String parquetFilePath,
            FilesFacade ff,
            int rowGroupIndex,
            String columnName,
            String value
    ) {
        final boolean[] canSkip = new boolean[1];
        withDecoder(parquetFilePath, ff, decoder -> {
            final ObjList<PushdownFilterExtractor.PushdownFilterCondition> conditions = new ObjList<>();
            try (
                    PushdownFilterExtractor.PushdownFilterCondition condition = new PushdownFilterExtractor.PushdownFilterCondition(columnName, -1, ColumnType.SYMBOL);
                    DirectLongList filterList = new DirectLongList(ParquetRowGroupFilter.LONGS_PER_FILTER, MemoryTag.NATIVE_PARQUET_PARTITION_DECODER, true);
                    MemoryCARWImpl filterValues = new MemoryCARWImpl(
                            ParquetRowGroupFilter.FILTER_BUFFER_PAGE_SIZE,
                            ParquetRowGroupFilter.FILTER_BUFFER_MAX_PAGES,
                            MemoryTag.NATIVE_PARQUET_PARTITION_DECODER
                    )
            ) {
                condition.addValueFunction(new VarcharConstant(value));
                conditions.add(condition);
                Assert.assertTrue(
                        "the filter must apply to the column",
                        ParquetRowGroupFilter.prepareFilterList(decoder.metadata(), conditions, filterList, filterValues, false)
                );
                canSkip[0] = decoder.canSkipRowGroup(rowGroupIndex, filterList, filterValues.getAddress() + filterValues.getAppendOffset());
            }
        });
        return canSkip[0];
    }

    public static void assertRowGroupCount(String parquetFilePath, FilesFacade ff, int expectedRowGroupCount) {
        withDecoder(parquetFilePath, ff, decoder -> Assert.assertEquals(
                "row group count",
                expectedRowGroupCount,
                decoder.metadata().getRowGroupCount()
        ));
    }

    /**
     * Creates the table (x INT, b BINARY, ts), partitioned by day, with four rows over three days, whose b holds random
     * bytes, and NULL in the second row. The WAL mode is as in createTableToRetype().
     */
    public static void createBinaryTableToRetype(
            CairoEngine engine,
            SqlExecutionContext executionContext,
            String tableName,
            String walMode
    ) throws SqlException {
        engine.execute("CREATE TABLE " + tableName + " (x INT, b BINARY, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY " + walMode, executionContext);
        engine.execute("INSERT INTO " + tableName + " VALUES " + """
                (1, rnd_bin(2, 4, 0), '2024-01-01T00:00:00.000000Z'),
                (2, NULL, '2024-01-01T12:00:00.000000Z'),
                (3, rnd_bin(2, 4, 0), '2024-01-02T00:00:00.000000Z'),
                (4, rnd_bin(2, 4, 0), '2024-01-03T00:00:00.000000Z')
                """, executionContext);
        TestUtils.drainWalQueue(engine);
    }

    /**
     * Creates the tables and views that the tests of exports of a retyped column read: r, whose first day goes to
     * Parquet before x turns from INT to LONG, so that the Parquet partition holds x in its old type and a read
     * converts it lazily; nr, which holds the same rows and turns x to LONG while every partition is native, so that
     * the table converts x eagerly and reads it as any LONG column; the views vr, vrf, vrl and vrv over r; and k, which
     * holds the symbols of s. The WAL mode is as in createTableToRetype(). dropRetypedTables() drops them all.
     */
    public static void createRetypedTables(CairoEngine engine, SqlExecutionContext executionContext, String walMode) throws SqlException {
        createTableToRetype(engine, executionContext, "r", walMode);
        createTableToRetype(engine, executionContext, "nr", walMode);
        retypeColumnX(engine, executionContext, "r", true);
        retypeColumnX(engine, executionContext, "nr", false);
        engine.execute("CREATE TABLE k (s SYMBOL)", executionContext);
        engine.execute("INSERT INTO k VALUES ('a'), ('b')", executionContext);
        engine.execute("CREATE VIEW vr AS (SELECT * FROM r)", executionContext);
        engine.execute("CREATE VIEW vrf AS (SELECT * FROM r WHERE x > 1)", executionContext);
        engine.execute("CREATE VIEW vrl AS (SELECT * FROM r LATEST ON ts PARTITION BY s)", executionContext);
        drainWalAndViewQueues(engine);
        engine.execute("CREATE VIEW vrv AS (SELECT * FROM vr)", executionContext);
        drainWalAndViewQueues(engine);
    }

    /**
     * Creates the table, partitioned by day, with the columns s SYMBOL INDEX, s2 SYMBOL, x INT, other_ts TIMESTAMP and
     * the designated timestamp ts, and inserts RETYPED_TABLE_ROWS. The WAL mode follows PARTITION BY DAY: WAL, BYPASS
     * WAL, or empty for the engine's default.
     */
    public static void createTableToRetype(
            CairoEngine engine,
            SqlExecutionContext executionContext,
            String tableName,
            String walMode
    ) throws SqlException {
        engine.execute("CREATE TABLE " + tableName + " (s SYMBOL INDEX, s2 SYMBOL, x INT, other_ts TIMESTAMP, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY " + walMode, executionContext);
        engine.execute("INSERT INTO " + tableName + " VALUES " + RETYPED_TABLE_ROWS, executionContext);
        TestUtils.drainWalQueue(engine);
    }

    /**
     * Creates a table with a column of every type the Parquet encoder writes, each with a PARQUET(...) setting its
     * type does not default to, and four rows: two that differ only in bin, which holds random bytes, and ts, a
     * different one, and one that holds NULL in every column whose type has a NULL. Column bf has only the
     * BLOOM_FILTER setting and holds 0 and 2.
     */
    public static void createTableWithParquetSettingPerType(
            CairoEngine engine,
            SqlExecutionContext executionContext,
            String tableName
    ) throws SqlException {
        engine.execute("CREATE TABLE " + tableName + " (" + """
                    b BYTE PARQUET(DELTA_BINARY_PACKED),
                    sh SHORT PARQUET(DELTA_BINARY_PACKED),
                    ch CHAR PARQUET(DELTA_BINARY_PACKED),
                    i INT PARQUET(RLE_DICTIONARY),
                    l LONG PARQUET(DELTA_BINARY_PACKED),
                    dt DATE PARQUET(DELTA_BINARY_PACKED),
                    t TIMESTAMP PARQUET(DELTA_BINARY_PACKED),
                    tn TIMESTAMP_NS PARQUET(RLE_DICTIONARY),
                    f FLOAT PARQUET(RLE_DICTIONARY),
                    d DOUBLE PARQUET(RLE_DICTIONARY),
                    u UUID PARQUET(RLE_DICTIONARY),
                    l256 LONG256 PARQUET(RLE_DICTIONARY),
                    ip IPV4 PARQUET(DELTA_BINARY_PACKED),
                    g1 GEOHASH(4b) PARQUET(DELTA_BINARY_PACKED),
                    g2 GEOHASH(2c) PARQUET(DELTA_BINARY_PACKED),
                    g4 GEOHASH(6c) PARQUET(DELTA_BINARY_PACKED),
                    g8 GEOHASH(12c) PARQUET(DELTA_BINARY_PACKED),
                    d8 DECIMAL(2,1) PARQUET(RLE_DICTIONARY),
                    d16 DECIMAL(4,2) PARQUET(RLE_DICTIONARY),
                    d32 DECIMAL(9,2) PARQUET(RLE_DICTIONARY),
                    d64 DECIMAL(18,3) PARQUET(RLE_DICTIONARY),
                    d128 DECIMAL(38,4) PARQUET(RLE_DICTIONARY),
                    d256 DECIMAL(76,10) PARQUET(RLE_DICTIONARY),
                    s STRING PARQUET(RLE_DICTIONARY),
                    vc VARCHAR PARQUET(RLE_DICTIONARY),
                    bin BINARY PARQUET(PLAIN),
                    sym SYMBOL PARQUET(RLE_DICTIONARY, ZSTD(3)),
                    bo BOOLEAN PARQUET(PLAIN),
                    arr DOUBLE[] PARQUET(PLAIN),
                    bf INT PARQUET(BLOOM_FILTER),
                    bfe INT PARQUET(RLE_DICTIONARY, BLOOM_FILTER),
                    ts TIMESTAMP PARQUET(PLAIN)
                ) TIMESTAMP(ts) PARTITION BY DAY""", executionContext);
        engine.execute("INSERT INTO " + tableName + " VALUES " + """
                (
                    1, 10, 'a', 100, 1000, '2024-01-01T00:00:00.000Z', '2024-01-01T00:00:00.000000Z',
                    '2024-01-01T00:00:00.000000001Z', 1.5, 2.5, '11111111-1111-1111-1111-111111111111', 0x01, '1.1.1.1',
                    ##0101, #u3, #u33dc0, #u33dc0cpke7v,
                    '1.5', '12.25', '1234.56', '123456.789', '12345678.9012', '1234567890.0123456789',
                    's1', 'v1', rnd_bin(2, 2, 0), 'sym1', true, ARRAY[1.0, 2.0], 0, 100, '2024-01-01T00:00:00.000000Z'
                ),
                (
                    1, 10, 'a', 100, 1000, '2024-01-01T00:00:00.000Z', '2024-01-01T00:00:00.000000Z',
                    '2024-01-01T00:00:00.000000001Z', 1.5, 2.5, '11111111-1111-1111-1111-111111111111', 0x01, '1.1.1.1',
                    ##0101, #u3, #u33dc0, #u33dc0cpke7v,
                    '1.5', '12.25', '1234.56', '123456.789', '12345678.9012', '1234567890.0123456789',
                    's1', 'v1', rnd_bin(2, 2, 0), 'sym1', true, ARRAY[1.0, 2.0], 0, 100, '2024-01-01T01:00:00.000000Z'
                ),
                (
                    2, 20, 'b', 200, 2000, '2024-01-02T00:00:00.000Z', '2024-01-02T00:00:00.000000Z',
                    '2024-01-02T00:00:00.000000002Z', 3.5, 4.5, '22222222-2222-2222-2222-222222222222', 0x02, '2.2.2.2',
                    ##1010, #s0, #s00000, #s00000000000,
                    '2.5', '22.25', '2234.56', '223456.789', '22345678.9012', '2234567890.0123456789',
                    's2', 'v2', rnd_bin(2, 2, 0), 'sym2', false, ARRAY[3.0], 2, 200, '2024-01-01T02:00:00.000000Z'
                ),
                (
                    0, 0, NULL, NULL, NULL, NULL, NULL,
                    NULL, NULL, NULL, NULL, NULL, NULL,
                    NULL, NULL, NULL, NULL,
                    NULL, NULL, NULL, NULL, NULL, NULL,
                    NULL, NULL, NULL, NULL, false, NULL, 2, NULL, '2024-01-01T03:00:00.000000Z'
                )""", executionContext);
    }

    // Drops the tables and views that createRetypedTables() creates.
    public static void dropRetypedTables(CairoEngine engine, SqlExecutionContext executionContext) throws SqlException {
        engine.execute("DROP VIEW vrv", executionContext);
        engine.execute("DROP VIEW vr", executionContext);
        engine.execute("DROP VIEW vrf", executionContext);
        engine.execute("DROP VIEW vrl", executionContext);
        engine.execute("DROP TABLE k", executionContext);
        engine.execute("DROP TABLE r", executionContext);
        engine.execute("DROP TABLE nr", executionContext);
        drainWalAndViewQueues(engine);
    }

    /**
     * Turns the INT column x of the table into LONG. With isFirstDayParquet the day 2024-01-01 goes to Parquet first,
     * so that the Parquet partition holds x in its old type and a read converts it lazily; without, every partition is
     * native and the table converts x eagerly.
     */
    public static void retypeColumnX(
            CairoEngine engine,
            SqlExecutionContext executionContext,
            String tableName,
            boolean isFirstDayParquet
    ) throws SqlException {
        if (isFirstDayParquet) {
            engine.execute("ALTER TABLE " + tableName + " CONVERT PARTITION TO PARQUET LIST '2024-01-01'", executionContext);
            TestUtils.drainWalQueue(engine);
        }
        engine.execute("ALTER TABLE " + tableName + " ALTER COLUMN x TYPE LONG", executionContext);
        TestUtils.drainWalQueue(engine);
    }

    private static void assertEncoding(String parquetFilePath, FilesFacade ff, int encodingId, boolean expectPresent, int... columnIndexes) {
        withDecoder(parquetFilePath, ff, decoder -> {
            final int rowGroupCount = decoder.metadata().getRowGroupCount();
            Assert.assertTrue("expected parquet file to contain at least one row group", rowGroupCount > 0);

            final CharSequence encodingName = ParquetEncoding.getEncodingName(encodingId);
            for (int rowGroupIndex = 0; rowGroupIndex < rowGroupCount; rowGroupIndex++) {
                for (int columnIndex : columnIndexes) {
                    Assert.assertTrue(
                            "row group " + rowGroupIndex + " is missing column " + columnIndex,
                            columnIndex >= 0 && columnIndex < decoder.metadata().getColumnCount()
                    );
                    Assert.assertEquals(
                            (expectPresent ? "expected " : "unexpected ")
                                    + encodingName + " encoding in row group " + rowGroupIndex
                                    + ", column " + columnIndex,
                            expectPresent,
                            decoder.rowGroupColumnHasEncoding(rowGroupIndex, columnIndex, encodingId)
                    );
                }
            }
        });
    }

    // applies the WAL of the tables and runs the view compiles that their DDL queued, as the server's jobs do
    private static void drainWalAndViewQueues(CairoEngine engine) {
        TestUtils.drainWalQueue(engine);
        try (ViewCompilerJob viewCompilerJob = new ViewCompilerJob(0, engine)) {
            viewCompilerJob.drain(0);
        }
    }

    private static void withDecoder(String parquetFilePath, FilesFacade ff, Consumer<ParquetFileDecoder> body) {
        long fd = -1;
        long addr = 0;
        long fileSize = 0;
        try (Path path = new Path(); ParquetFileDecoder decoder = new ParquetFileDecoder()) {
            path.of(parquetFilePath).$();
            fd = TableUtils.openRO(ff, path.$(), LOG);
            fileSize = ff.length(fd);
            addr = TableUtils.mapRO(ff, fd, fileSize, MemoryTag.MMAP_PARQUET_PARTITION_DECODER);
            decoder.of(addr, fileSize, MemoryTag.NATIVE_PARQUET_PARTITION_DECODER);
            body.accept(decoder);
        } finally {
            if (addr != 0) {
                ff.munmap(addr, fileSize, MemoryTag.MMAP_PARQUET_PARTITION_DECODER);
            }
            if (fd != -1) {
                ff.close(fd);
            }
        }
    }
}
