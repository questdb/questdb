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

import io.questdb.cairo.TableUtils;
import io.questdb.griffin.engine.table.parquet.ParquetEncoding;
import io.questdb.griffin.engine.table.parquet.ParquetFileDecoder;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.std.FilesFacade;
import io.questdb.std.MemoryTag;
import io.questdb.std.str.Path;
import org.junit.Assert;

public final class ParquetTestUtils {
    private static final Log LOG = LogFactory.getLog(ParquetTestUtils.class);

    private ParquetTestUtils() {
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

    private static void assertEncoding(String parquetFilePath, FilesFacade ff, int encodingId, boolean expectPresent, int... columnIndexes) {
        long fd = -1;
        long addr = 0;
        long fileSize = 0;
        try (Path path = new Path(); ParquetFileDecoder decoder = new ParquetFileDecoder()) {
            path.of(parquetFilePath).$();
            fd = TableUtils.openRO(ff, path.$(), LOG);
            fileSize = ff.length(fd);
            addr = TableUtils.mapRO(ff, fd, fileSize, MemoryTag.MMAP_PARQUET_PARTITION_DECODER);
            decoder.of(addr, fileSize, MemoryTag.NATIVE_PARQUET_PARTITION_DECODER);

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
