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

import io.questdb.cairo.CairoEngine;
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.TableUtils;
import io.questdb.cairo.vm.MemoryCARWImpl;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.engine.functions.constants.VarcharConstant;
import io.questdb.griffin.engine.table.ParquetRowGroupFilter;
import io.questdb.griffin.engine.table.PushdownFilterExtractor;
import io.questdb.griffin.engine.table.parquet.ParquetEncoding;
import io.questdb.griffin.engine.table.parquet.ParquetFileDecoder;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.std.DirectLongList;
import io.questdb.std.FilesFacade;
import io.questdb.std.MemoryTag;
import io.questdb.std.ObjList;
import io.questdb.std.str.Path;
import org.junit.Assert;

import java.util.function.Consumer;

public final class ParquetTestUtils {
    private static final Log LOG = LogFactory.getLog(ParquetTestUtils.class);

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
        // b, sh, ch, l, dt, t, ip, g1, g2, g4 and g8 default to PLAIN
        assertColumnsUseEncoding(parquetFilePath, ff, ParquetEncoding.ENCODING_DELTA_BINARY_PACKED, 0, 1, 2, 4, 5, 6, 12, 13, 14, 15, 16);
        // i, tn, f, d, u, l256, the six decimals and bfe default to PLAIN, s and vc to DELTA_LENGTH_BYTE_ARRAY
        assertColumnsUseEncoding(parquetFilePath, ff, ParquetEncoding.ENCODING_RLE_DICTIONARY, 3, 7, 8, 9, 10, 11, 17, 18, 19, 20, 21, 22, 23, 24, 30);
        // bin and the designated timestamp ts default to DELTA_LENGTH_BYTE_ARRAY and DELTA_BINARY_PACKED
        assertColumnsUseEncoding(parquetFilePath, ff, ParquetEncoding.ENCODING_PLAIN, 25, 31);
        assertColumnsDoNotUseEncoding(parquetFilePath, ff, ParquetEncoding.ENCODING_DELTA_LENGTH_BYTE_ARRAY, 25);
        assertColumnsDoNotUseEncoding(parquetFilePath, ff, ParquetEncoding.ENCODING_DELTA_BINARY_PACKED, 31);
        // bf names no encoding, so it keeps the INT default, PLAIN
        assertColumnsDoNotUseEncoding(parquetFilePath, ff, ParquetEncoding.ENCODING_RLE_DICTIONARY, 29);
        assertColumnsDoNotUseEncoding(parquetFilePath, ff, ParquetEncoding.ENCODING_DELTA_BINARY_PACKED, 29);
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
