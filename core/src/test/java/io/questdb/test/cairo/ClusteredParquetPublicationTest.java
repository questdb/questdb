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

import io.questdb.PropertyKey;
import io.questdb.cairo.IndexMetaFileReader;
import io.questdb.cairo.ParquetMetaFileReader;
import io.questdb.cairo.PartitionBy;
import io.questdb.cairo.TableReader;
import io.questdb.cairo.TableToken;
import io.questdb.cairo.TableWriter;
import io.questdb.cairo.TableUtils;
import io.questdb.cairo.TxReader;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.std.FilesFacade;
import io.questdb.std.MemoryTag;
import io.questdb.std.str.Path;
import io.questdb.test.AbstractCairoTest;
import org.junit.Assert;
import org.junit.Test;

import java.util.List;
import java.util.stream.Collectors;

public class ClusteredParquetPublicationTest extends AbstractCairoTest {
    private static final Log LOG = LogFactory.getLog(ClusteredParquetPublicationTest.class);

    @Test
    public void testCompositeConversionPublishesOneDirectoryPerCell() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table c (ts timestamp, exchange symbol, k symbol, v int) " +
                    "timestamp(ts) partition by day, exchange order by k wal");
            execute("insert into c values " +
                    "('2024-01-01T00:00:00.000000Z', 'X', 'b', 1)," +
                    "('2024-01-01T00:00:01.000000Z', 'Y', 'a', 2)," +
                    "('2024-01-01T00:00:02.000000Z', 'X', 'a', 3)," +
                    "('2024-01-01T00:00:03.000000Z', 'Y', 'b', 4)");
            execute("insert into c values ('2024-01-02T00:00:00.000000Z', 'X', 'z', 5)");
            drainWalQueue();
            try (TableWriter writer = getWriter("c")) {
                writer.convertCompositePartitionToParquetForTest(
                        parseFloorPartialTimestamp("2024-01-01"),
                        null,
                        0.01
                );
            }

            final TableToken tableToken = engine.verifyTableName("c");
            final java.nio.file.Path tablePath = java.nio.file.Path.of(
                    root.toString(),
                    tableToken.getDirName()
            );
            final List<java.nio.file.Path> sidecars;
            try (java.util.stream.Stream<java.nio.file.Path> paths = java.nio.file.Files.walk(tablePath)) {
                sidecars = paths
                        .filter(p -> p.getFileName().toString().matches("data\\.parquet\\.\\d+\\._im"))
                        .collect(Collectors.toList());
            }
            Assert.assertEquals(2, sidecars.size());
            for (java.nio.file.Path sidecar : sidecars) {
                assertPublishedBinding(sidecar, 2);
            }
        });
    }

    @Test
    public void testConversionPublishesExactClusteredDirectoryToken() throws Exception {
        assertMemoryLeak(() -> {
            inputRoot = root;
            node1.setProperty(PropertyKey.CAIRO_POSTING_INDEX_PARQUET_PARTITION_FORMAT, "parquet");
            execute("create table x (k symbol index type posting include (v), v int, ts timestamp) " +
                    "timestamp(ts) partition by day order by k");
            execute("insert into x values " +
                    "('b', 1, '2024-01-01T00:00:00.000000Z')," +
                    "('a', 2, '2024-01-01T00:00:01.000000Z')," +
                    "(null, 3, '2024-01-01T00:00:02.000000Z')," +
                    "('b', 4, '2024-01-01T00:00:03.000000Z')," +
                    "('a', 5, '2024-01-01T00:00:04.000000Z')," +
                    "('c', 6, '2024-01-01T00:00:05.000000Z')");
            execute("insert into x values ('z', 7, '2024-01-02T00:00:00.000000Z')");
            execute("alter table x convert partition to parquet list '2024-01-01'");

            final FilesFacade ff = configuration.getFilesFacade();
            final TableToken tableToken = engine.verifyTableName("x");
            final String[] parquetRelativePath = new String[1];
            try (
                    Path path = new Path();
                    TableReader tableReader = engine.getReader(tableToken)
            ) {
                final TxReader tx = tableReader.getTxFile();
                final long partitionTimestamp = tx.getPartitionTimestampByIndex(0);
                final long partitionNameTxn = tx.getPartitionNameTxn(0);
                final long parquetFileSize = tx.getPartitionParquetFileSize(0);
                final int timestampType = tx.getTimestampType();
                final int clusterWriterIndex = tableReader.getMetadata().getPartitionSpec().getClusterColumn(0);

                path.of(configuration.getDbRoot()).concat(tableToken.getDirName());
                final int tablePathLen = path.size();
                TableUtils.setPathForParquetPartitionMetadata(
                        path.trimTo(tablePathLen),
                        timestampType,
                        PartitionBy.DAY,
                        partitionTimestamp,
                        partitionNameTxn
                );
                final ParquetMetaFileReader parquetMeta = new ParquetMetaFileReader();
                long parquetMetaAddr = 0;
                long parquetMetaSize = 0;
                long indexMetaAddr = 0;
                long indexMetaSize = 0;
                try {
                    parquetMetaAddr = ParquetMetaFileReader.openAndMapRO(ff, path.$(), parquetMeta);
                    parquetMetaSize = parquetMeta.getFileSize();
                    Assert.assertNotEquals(0, parquetMetaAddr);
                    Assert.assertTrue(parquetMeta.resolveFooter(parquetFileSize));
                    final long clusterTxn = parquetMeta.getClusteredDataTxn();
                    final long imFileSize = parquetMeta.getClusteredDataImFileSize();
                    indexMetaSize = imFileSize;
                    Assert.assertEquals(partitionNameTxn, clusterTxn);
                    Assert.assertTrue(imFileSize > 0);

                    TableUtils.setPathForParquetPartition(
                            path.trimTo(tablePathLen),
                            timestampType,
                            PartitionBy.DAY,
                            partitionTimestamp,
                            partitionNameTxn
                    );
                    path.parent();
                    parquetRelativePath[0] = java.nio.file.Path.of(root.toString()).relativize(
                            java.nio.file.Path.of(path.toString()).resolve(TableUtils.PARQUET_PARTITION_NAME)
                    ).toString();
                    TableUtils.clusteredDataMetadataFileName(path, clusterTxn);
                    Assert.assertEquals(imFileSize, ff.length(path.$()));
                    indexMetaAddr = TableUtils.mapRO(
                            ff,
                            path.$(),
                            LOG,
                            imFileSize,
                            MemoryTag.MMAP_PARQUET_METADATA_READER
                    );
                    final IndexMetaFileReader indexMeta = new IndexMetaFileReader();
                    indexMeta.ofAddressExact(indexMetaAddr, imFileSize, imFileSize);
                    indexMeta.validateClusteredDataBinding(parquetFileSize, clusterWriterIndex);
                    Assert.assertEquals(IndexMetaFileReader.IM_PAYLOAD_CLUSTERED_DATA, indexMeta.getPayloadKind());
                    Assert.assertEquals(5, indexMeta.getKeySpaceSize());
                    Assert.assertEquals(1, parquetMeta.getCoveringIndexCount());
                } finally {
                    parquetMeta.clear();
                    if (indexMetaAddr != 0) {
                        ff.munmap(indexMetaAddr, indexMetaSize, MemoryTag.MMAP_PARQUET_METADATA_READER);
                    }
                    if (parquetMetaAddr != 0) {
                        ff.munmap(parquetMetaAddr, parquetMetaSize, MemoryTag.MMAP_PARQUET_METADATA_READER);
                    }
                }
            }
            assertQuery("select coalesce(k, 'NULL') k, v from read_parquet('" + parquetRelativePath[0] + "')")
                    .expectSize()
                    .returns("k\tv\nNULL\t3\nb\t1\nb\t4\na\t2\na\t5\nc\t6\n");
            assertQuery("select * from x")
                    .fails(0, "clustered parquet partition requires clustered read mode");
        });
    }

    private void assertPublishedBinding(java.nio.file.Path sidecar, int clusterWriterIndex) {
        final FilesFacade ff = configuration.getFilesFacade();
        try (Path path = new Path()) {
            final java.nio.file.Path directory = sidecar.getParent();
            final long parquetFileSize = java.nio.file.Files.isRegularFile(directory.resolve(TableUtils.PARQUET_PARTITION_NAME))
                    ? directory.resolve(TableUtils.PARQUET_PARTITION_NAME).toFile().length()
                    : -1;
            final long imFileSize = sidecar.toFile().length();
            final String fileName = sidecar.getFileName().toString();
            final long clusterTxn = Long.parseLong(fileName.substring("data.parquet.".length(), fileName.length() - "._im".length()));
            final ParquetMetaFileReader parquetMeta = new ParquetMetaFileReader();
            long parquetMetaAddr = 0;
            long parquetMetaSize = 0;
            long indexMetaAddr = 0;
            try {
                path.of(directory.resolve(TableUtils.PARQUET_METADATA_FILE_NAME).toString());
                parquetMetaAddr = ParquetMetaFileReader.openAndMapRO(ff, path.$(), parquetMeta);
                parquetMetaSize = parquetMeta.getFileSize();
                Assert.assertTrue(parquetMeta.resolveFooter(parquetFileSize));
                Assert.assertEquals(clusterTxn, parquetMeta.getClusteredDataTxn());
                Assert.assertEquals(imFileSize, parquetMeta.getClusteredDataImFileSize());

                path.of(sidecar.toString());
                indexMetaAddr = TableUtils.mapRO(
                        ff,
                        path.$(),
                        LOG,
                        imFileSize,
                        MemoryTag.MMAP_PARQUET_METADATA_READER
                );
                final IndexMetaFileReader indexMeta = new IndexMetaFileReader();
                indexMeta.ofAddressExact(indexMetaAddr, imFileSize, imFileSize);
                indexMeta.validateClusteredDataBinding(parquetFileSize, clusterWriterIndex);
            } finally {
                parquetMeta.clear();
                if (indexMetaAddr != 0) {
                    ff.munmap(indexMetaAddr, imFileSize, MemoryTag.MMAP_PARQUET_METADATA_READER);
                }
                if (parquetMetaAddr != 0) {
                    ff.munmap(parquetMetaAddr, parquetMetaSize, MemoryTag.MMAP_PARQUET_METADATA_READER);
                }
            }
        }
    }
}
