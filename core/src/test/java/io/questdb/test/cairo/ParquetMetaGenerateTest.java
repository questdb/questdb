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

import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.ParquetMetaFileReader;
import io.questdb.cairo.PartitionBy;
import io.questdb.cairo.TableReader;
import io.questdb.cairo.TableToken;
import io.questdb.cairo.TableUtils;
import io.questdb.griffin.engine.table.parquet.ParquetMetadataWriter;
import io.questdb.std.Files;
import io.questdb.std.FilesFacade;
import io.questdb.std.MemoryTag;
import io.questdb.std.Unsafe;
import io.questdb.std.str.Path;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.std.TestFilesFacadeImpl;
import org.junit.Assert;
import org.junit.Test;

public class ParquetMetaGenerateTest extends AbstractCairoTest {

    @Test
    public void testGenerateFromParquetFile() throws Exception {
        assertMemoryLeak(TestFilesFacadeImpl.INSTANCE, () -> {
            execute("CREATE TABLE t (id INT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("INSERT INTO t VALUES(1, '2024-06-10T00:00:00.000000Z')");
            execute("INSERT INTO t VALUES(2, '2024-06-11T00:00:00.000000Z')");
            execute("ALTER TABLE t CONVERT PARTITION TO PARQUET LIST '2024-06-10'");

            final FilesFacade ff = configuration.getFilesFacade();
            final TableToken token = engine.verifyTableName("t");

            long partitionTs;
            long partitionNameTxn;
            try (TableReader reader = engine.getReader(token)) {
                partitionTs = reader.getTxFile().getPartitionTimestampByIndex(0);
                partitionNameTxn = reader.getTxFile().getPartitionNameTxn(0);
            }

            try (Path path = new Path().of(configuration.getDbRoot()).concat(token)) {
                int tablePathLen = path.size();

                // Open data.parquet for reading.
                TableUtils.setPathForParquetPartition(path, ColumnType.TIMESTAMP, PartitionBy.DAY, partitionTs, partitionNameTxn);
                long parquetFileSize = ff.length(path.$());
                Assert.assertTrue("parquet file should exist at " + path + " (size=" + parquetFileSize + ")", parquetFileSize > 0);
                long parquetFd = ff.openRO(path.$());
                Assert.assertTrue("parquet fd should be valid", parquetFd >= 0);

                // Create _pm for writing. Remove any existing file first.
                TableUtils.setPathForParquetPartitionMetadata(path.trimTo(tablePathLen), ColumnType.TIMESTAMP, PartitionBy.DAY, partitionTs, partitionNameTxn);
                ff.remove(path.$());
                long parquetMetaFd = ff.openRW(path.$(), CairoConfiguration.O_NONE);
                Assert.assertTrue("_pm fd should be valid", parquetMetaFd >= 0);

                long parquetMetaFileSize;
                try {
                    long parquetMetaAllocator = Unsafe.getNativeAllocator(MemoryTag.NATIVE_DEFAULT);
                    parquetMetaFileSize = ParquetMetadataWriter.generate(parquetMetaAllocator, Files.toOsFd(parquetFd), parquetFileSize, Files.toOsFd(parquetMetaFd));
                    Assert.assertTrue("_pm file size should be positive", parquetMetaFileSize > 0);
                } finally {
                    ff.close(parquetFd);
                    ff.close(parquetMetaFd);
                }

                // Read the _pm file back and verify. The on-disk file size and
                // the header's PARQUET_META_FILE_SIZE field must agree for a
                // freshly generated file.
                Assert.assertEquals(parquetMetaFileSize, ff.length(path.$()));
                Assert.assertEquals(parquetMetaFileSize, ParquetMetaFileReader.readParquetMetaFileSize(ff, path.$()));
                long parquetMetaAddr = TableUtils.mapRO(ff, path.$(), LOG, parquetMetaFileSize, MemoryTag.MMAP_DEFAULT);
                try {
                    ParquetMetaFileReader reader = new ParquetMetaFileReader();
                    reader.of(parquetMetaAddr, parquetMetaFileSize);
                    Assert.assertTrue(reader.resolveFooter(parquetFileSize));

                    Assert.assertEquals(2, reader.getColumnCount());
                    Assert.assertEquals(1, reader.getRowGroupCount());
                    Assert.assertEquals(parquetFileSize, reader.getParquetFileSize());
                    Assert.assertEquals(1, reader.getRowGroupSize(0));
                } finally {
                    ff.munmap(parquetMetaAddr, parquetMetaFileSize, MemoryTag.MMAP_DEFAULT);
                }
            }
        });
    }
}
