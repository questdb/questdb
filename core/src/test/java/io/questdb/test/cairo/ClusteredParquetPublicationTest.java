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
import io.questdb.cairo.CairoException;
import io.questdb.cairo.IndexMetaFileReader;
import io.questdb.cairo.ParquetMetaFileReader;
import io.questdb.cairo.PartitionBy;
import io.questdb.cairo.PostingSealPurgeJob;
import io.questdb.cairo.PostingSealPurgeOperator;
import io.questdb.cairo.TableReader;
import io.questdb.cairo.TableToken;
import io.questdb.cairo.TableWriter;
import io.questdb.cairo.TableUtils;
import io.questdb.cairo.TxReader;
import io.questdb.cairo.TxnScoreboard;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.std.FilesFacade;
import io.questdb.std.MemoryTag;
import io.questdb.std.str.Path;
import io.questdb.tasks.PostingSealPurgeTask;
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
            assertQuery("select v, ts from c")
                    .timestamp("ts")
                    .expectSize()
                    .returns("v\tts\n"
                            + "1\t2024-01-01T00:00:00.000000Z\n"
                            + "2\t2024-01-01T00:00:01.000000Z\n"
                            + "3\t2024-01-01T00:00:02.000000Z\n"
                            + "4\t2024-01-01T00:00:03.000000Z\n"
                            + "5\t2024-01-02T00:00:00.000000Z\n");
        });
    }

    @Test
    public void testFailedClusteredGatherLeavesPartitionNativeAndNoArtifacts() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table f (k symbol, a double[], ts timestamp) timestamp(ts) partition by day order by k");
            execute("insert into f values "
                    + "('b', ARRAY[1.0, 2.0], '2024-01-01T00:00:00.000000Z'),"
                    + "('a', ARRAY[3.0, 4.0], '2024-01-01T00:00:01.000000Z'),"
                    + "('z', ARRAY[5.0], '2024-01-02T00:00:00.000000Z')");
            try {
                execute("alter table f convert partition to parquet list '2024-01-01'");
                Assert.fail("expected clustered ARRAY conversion to fail");
            } catch (CairoException ex) {
                Assert.assertTrue(ex.getFlyweightMessage().toString().contains(
                        "permutation-aware parquet encoding does not support ARRAY"
                ));
            }

            final TableToken token = engine.verifyTableName("f");
            try (TableReader reader = engine.getReader(token)) {
                Assert.assertFalse(reader.getTxFile().isPartitionParquet(0));
            }
            final java.nio.file.Path tablePath = java.nio.file.Path.of(root.toString(), token.getDirName());
            try (java.util.stream.Stream<java.nio.file.Path> paths = java.nio.file.Files.walk(tablePath)) {
                Assert.assertFalse(paths.anyMatch(p -> {
                    final String name = p.getFileName().toString();
                    return name.equals(TableUtils.PARQUET_PARTITION_NAME)
                            || name.equals(TableUtils.PARQUET_METADATA_FILE_NAME)
                            || name.matches("data\\.parquet\\.\\d+\\._im");
                }));
            }
        });
    }

    @Test
    public void testPinnedNativeReaderSurvivesClusteredPublication() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table p (k symbol, v int, ts timestamp) timestamp(ts) partition by day order by k");
            execute("insert into p values "
                    + "('b', 1, '2024-01-01T00:00:00.000000Z'),"
                    + "('a', 2, '2024-01-01T00:00:01.000000Z'),"
                    + "('b', 3, '2024-01-01T00:00:02.000000Z')");

            try (RecordCursorFactory factory = select("select v, ts from p");
                 RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                final Record record = cursor.getRecord();
                Assert.assertTrue(cursor.hasNext());
                Assert.assertEquals(1, record.getInt(0));

                execute("alter table p convert partition to parquet list '2024-01-01'");

                Assert.assertTrue(cursor.hasNext());
                Assert.assertEquals(2, record.getInt(0));
                Assert.assertTrue(cursor.hasNext());
                Assert.assertEquals(3, record.getInt(0));
                Assert.assertFalse(cursor.hasNext());
            }

            assertQuery("select v, ts from p")
                    .timestamp("ts")
                    .expectSize()
                    .returns("v\tts\n"
                            + "1\t2024-01-01T00:00:00.000000Z\n"
                            + "2\t2024-01-01T00:00:01.000000Z\n"
                            + "3\t2024-01-01T00:00:02.000000Z\n");
        });
    }

    @Test
    public void testClusteredCoveringIndexRequiresCoveredTimestamp() throws Exception {
        assertMemoryLeak(() -> {
            inputRoot = root;
            node1.setProperty(PropertyKey.CAIRO_POSTING_INDEX_PARQUET_PARTITION_FORMAT, "parquet");
            execute("create table safe (c symbol, s symbol index type posting include (v, ts), "
                    + "v int, extra long, ts timestamp) timestamp(ts) partition by day order by c");
            execute("insert into safe values "
                    + "('z', 'b', 1, 101, '2024-01-01T00:00:00.000000Z'),"
                    + "('a', 'a', 2, 102, '2024-01-01T00:00:01.000000Z'),"
                    + "('z', 'a', 3, 103, '2024-01-01T00:00:02.000000Z'),"
                    + "('a', 'a', 4, 104, '2024-01-01T00:00:03.000000Z'),"
                    + "('a', 'a', 5, 105, '2024-01-02T00:00:00.000000Z')");
            execute("alter table safe convert partition to parquet list '2024-01-01'");

            assertQuery("select v, ts from safe where s = 'a'")
                    .noLeakCheck()
                    .assertsPlanContaining("CoveringIndex");
            assertQuery("select v, ts from safe where s = 'a'")
                    .timestamp("ts")
                    .expectSize()
                    .returns("v\tts\n"
                            + "2\t2024-01-01T00:00:01.000000Z\n"
                            + "3\t2024-01-01T00:00:02.000000Z\n"
                            + "4\t2024-01-01T00:00:03.000000Z\n"
                            + "5\t2024-01-02T00:00:00.000000Z\n");
            final String intervalQuery = "select v, ts from safe where s = 'a' "
                    + "and ts between '2024-01-01T00:00:01.500000Z' and '2024-01-01T00:00:02.500000Z'";
            assertQuery(intervalQuery)
                    .noLeakCheck()
                    .assertsPlanContaining("CoveringIndex");
            assertQuery(intervalQuery)
                    .timestamp("ts")
                    .expectSize()
                    .returns("v\tts\n3\t2024-01-01T00:00:02.000000Z\n");
            assertQuery("select max(v) from safe where s in ('a', 'b')")
                    .noLeakCheck()
                    .assertsPlanContaining("Async Group By", "CoveringIndex");
            assertQuery("select max(v) from safe where s in ('a', 'b')")
                    .noRandomAccess()
                    .expectSize()
                    .returns("max\n5\n");
            assertQuery("select first(v), last(v) from safe where s in ('a', 'b')")
                    .noLeakCheck()
                    .assertsPlanContaining("CoveringIndex");
            assertQuery("select first(v), last(v) from safe where s in ('a', 'b')")
                    .noRandomAccess()
                    .expectSize()
                    .returns("first\tlast\n1\t5\n");

            // `extra` is not carried by the posting sidecar. The resealed row ids address
            // key-major data and provide no global timestamp contract, so codegen keeps the key
            // predicate as a residual over the timestamp-merged clustered-run scan.
            assertQuery("select extra, ts from safe where s = 'a'")
                    .noLeakCheck()
                    .assertsPlanNotContaining("CoveringIndex");
            assertQuery("select extra, ts from safe where s = 'a'")
                    .timestamp("ts")
                    .returns("extra\tts\n"
                            + "102\t2024-01-01T00:00:01.000000Z\n"
                            + "103\t2024-01-01T00:00:02.000000Z\n"
                            + "104\t2024-01-01T00:00:03.000000Z\n"
                            + "105\t2024-01-02T00:00:00.000000Z\n");

            execute("create table unsafe (c symbol, s symbol index type posting, v int, ts timestamp) "
                    + "timestamp(ts) partition by day order by c");
            execute("insert into unsafe values "
                    + "('z', 'b', 1, '2024-01-01T00:00:00.000000Z'),"
                    + "('a', 'a', 2, '2024-01-01T00:00:01.000000Z'),"
                    + "('z', 'a', 3, '2024-01-01T00:00:02.000000Z'),"
                    + "('a', 'a', 4, '2024-01-01T00:00:03.000000Z')");
            execute("alter table unsafe convert partition to parquet list '2024-01-01'");
            assertQuery("select v, ts from unsafe where s = 'a'")
                    .noLeakCheck()
                    .assertsPlanNotContaining("CoveringIndex");
            assertQuery("select v, ts from unsafe where s = 'a'")
                    .timestamp("ts")
                    .returns("v\tts\n"
                            + "2\t2024-01-01T00:00:01.000000Z\n"
                            + "3\t2024-01-01T00:00:02.000000Z\n"
                            + "4\t2024-01-01T00:00:03.000000Z\n");

            node1.setProperty(PropertyKey.CAIRO_POSTING_INDEX_PARQUET_PARTITION_FORMAT, "native");
            execute("create table native_cover (c symbol, s symbol index type posting include (v, ts), "
                    + "v int, ts timestamp) timestamp(ts) partition by day order by c");
            execute("insert into native_cover values "
                    + "('z', 'b', 1, '2024-01-01T00:00:00.000000Z'),"
                    + "('a', 'a', 2, '2024-01-01T00:00:01.000000Z'),"
                    + "('z', 'a', 3, '2024-01-01T00:00:02.000000Z')");
            execute("alter table native_cover convert partition to parquet list '2024-01-01'");
            assertQuery("select v, ts from native_cover where s = 'a'")
                    .noLeakCheck()
                    .assertsPlanNotContaining("CoveringIndex");
            assertQuery("select v, ts from native_cover where s = 'a'")
                    .timestamp("ts")
                    .returns("v\tts\n"
                            + "2\t2024-01-01T00:00:01.000000Z\n"
                            + "3\t2024-01-01T00:00:02.000000Z\n");
        });
    }

    @Test
    public void testClusteredCompositeO3RewritesEachCell() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table o3cc (exchange symbol, k symbol, n int, ts timestamp) "
                    + "timestamp(ts) partition by day, exchange order by k wal");
            execute("insert into o3cc values "
                    + "('X', 'b', 1, '2024-01-01T00:00:04.000000Z'),"
                    + "('X', 'a', 2, '2024-01-01T00:00:01.000000Z'),"
                    + "('Y', 'b', 3, '2024-01-01T00:00:06.000000Z'),"
                    + "('Y', 'a', 4, '2024-01-01T00:00:02.000000Z'),"
                    + "('X', 'z', 5, '2024-01-02T00:00:00.000000Z')");
            drainWalQueue();
            try (TableWriter writer = getWriter("o3cc")) {
                writer.convertCompositePartitionToParquetForTest(
                        parseFloorPartialTimestamp("2024-01-01"),
                        null,
                        0.01
                );
            }

            execute("insert into o3cc values "
                    + "('X', 'a', 6, '2024-01-01T00:00:03.000000Z'),"
                    + "('Y', 'c', 7, '2024-01-01T00:00:05.000000Z')");
            drainWalQueue();

            assertQuery("select exchange, k, n, ts from o3cc where ts in '2024-01-01' order by ts")
                    .timestamp("ts")
                    .returns("exchange\tk\tn\tts\n"
                            + "X\ta\t2\t2024-01-01T00:00:01.000000Z\n"
                            + "Y\ta\t4\t2024-01-01T00:00:02.000000Z\n"
                            + "X\ta\t6\t2024-01-01T00:00:03.000000Z\n"
                            + "X\tb\t1\t2024-01-01T00:00:04.000000Z\n"
                            + "Y\tc\t7\t2024-01-01T00:00:05.000000Z\n"
                            + "Y\tb\t3\t2024-01-01T00:00:06.000000Z\n");
            assertQuery("select k, n, ts from o3cc where exchange = 'X' and ts in '2024-01-01' order by ts")
                    .timestamp("ts")
                    .returns("k\tn\tts\n"
                            + "a\t2\t2024-01-01T00:00:01.000000Z\n"
                            + "a\t6\t2024-01-01T00:00:03.000000Z\n"
                            + "b\t1\t2024-01-01T00:00:04.000000Z\n");
            assertQuery("select k, n, ts from o3cc where exchange = 'Y' and ts in '2024-01-01' order by ts")
                    .timestamp("ts")
                    .returns("k\tn\tts\n"
                            + "a\t4\t2024-01-01T00:00:02.000000Z\n"
                            + "c\t7\t2024-01-01T00:00:05.000000Z\n"
                            + "b\t3\t2024-01-01T00:00:06.000000Z\n");
        });
    }

    @Test
    public void testClusteredO3RewriteSupportsTimestampDeduplication() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table o3d (k symbol, n int, ts timestamp) "
                    + "timestamp(ts) partition by day order by k wal dedup upsert keys(ts)");
            execute("insert into o3d values "
                    + "('a', 1, '2024-01-01T00:00:01.000000Z'),"
                    + "('b', 2, '2024-01-01T00:00:02.000000Z'),"
                    + "('z', 8, '2024-01-02T00:00:00.000000Z')");
            drainWalQueue();
            execute("alter table o3d convert partition to parquet list '2024-01-01'");
            drainWalQueue();

            execute("insert into o3d values "
                    + "('c', 9, '2024-01-01T00:00:01.000000Z'),"
                    + "('a', 3, '2024-01-01T00:00:03.000000Z')");
            drainWalQueue();

            assertQuery("select k, n, ts from o3d where ts in '2024-01-01' order by ts")
                    .timestamp("ts")
                    .returns("k\tn\tts\n"
                            + "c\t9\t2024-01-01T00:00:01.000000Z\n"
                            + "b\t2\t2024-01-01T00:00:02.000000Z\n"
                            + "a\t3\t2024-01-01T00:00:03.000000Z\n");
        });
    }

    @Test
    public void testClusteredO3RewriteRebuildsPermutationAndDirectory() throws Exception {
        node1.setProperty(PropertyKey.CAIRO_POSTING_INDEX_PARQUET_PARTITION_FORMAT, "parquet");
        assertMemoryLeak(() -> {
            execute("create table o3c (k symbol, p symbol index type posting include (n, ts), s string, v varchar, n int, ts timestamp) "
                    + "timestamp(ts) partition by day order by k");
            execute("insert into o3c values "
                    + "('b', 'x', 'old-b', 'vb', 1, '2024-01-01T00:00:04.000000Z'),"
                    + "('a', 'y', 'old-a0', 'va0', 2, '2024-01-01T00:00:01.000000Z'),"
                    + "('a', 'x', 'old-a1', 'va1', 3, '2024-01-01T00:00:06.000000Z'),"
                    + "('z', 'x', 'tail', 'vz', 4, '2024-01-02T00:00:00.000000Z')");
            execute("alter table o3c convert partition to parquet list '2024-01-01'");

            final TableToken token = engine.verifyTableName("o3c");
            final long oldClusterTxn;
            final long oldPartitionTimestamp;
            final long oldPartitionNameTxn;
            final int timestampType;
            final TableReader pinnedReader = engine.getReader(token);
            pinnedReader.setClusteredReadMode();
            pinnedReader.openPartition(0);
            oldClusterTxn = pinnedReader.getClusteredDataTxn(0);
            oldPartitionTimestamp = pinnedReader.getTxFile().getPartitionTimestampByIndex(0);
            oldPartitionNameTxn = pinnedReader.getTxFile().getPartitionNameTxn(0);
            timestampType = pinnedReader.getTxFile().getTimestampType();
            try {
                execute("insert into o3c values "
                        + "('a', 'x', 'late-a', 'late-va', 5, '2024-01-01T00:00:03.000000Z'),"
                        + "('c', 'y', 'new-c', 'new-vc', 6, '2024-01-01T00:00:02.000000Z'),"
                        + "('b', 'x', 'late-b', 'late-vb', 7, '2024-01-01T00:00:05.000000Z')");
                try (Path oldPath = new Path()) {
                    oldPath.of(configuration.getDbRoot()).concat(token.getDirName());
                    final int rootLen = oldPath.size();
                    TableUtils.setPathForParquetPartitionMetadata(
                            oldPath.trimTo(rootLen),
                            timestampType,
                            PartitionBy.DAY,
                            oldPartitionTimestamp,
                            oldPartitionNameTxn
                    );
                    oldPath.parent();
                    Assert.assertTrue(
                            "a reader-pinned clustered generation must remain on disk",
                            configuration.getFilesFacade().exists(
                                    TableUtils.clusteredDataMetadataFileName(oldPath, oldClusterTxn)
                            )
                    );
                }
            } finally {
                pinnedReader.close();
            }
            engine.releaseInactive();
            try (PostingSealPurgeJob purgeJob = new PostingSealPurgeJob(engine)) {
                for (int i = 0; i < 3; i++) {
                    purgeJob.run();
                }
            }
            try (Path oldPath = new Path()) {
                oldPath.of(configuration.getDbRoot()).concat(token.getDirName());
                final int rootLen = oldPath.size();
                TableUtils.setPathForParquetPartitionMetadata(
                        oldPath.trimTo(rootLen),
                        timestampType,
                        PartitionBy.DAY,
                        oldPartitionTimestamp,
                        oldPartitionNameTxn
                );
                oldPath.parent();
                Assert.assertFalse(
                        "the superseded clustered generation must be reclaimed after the reader releases it",
                        configuration.getFilesFacade().exists(
                                TableUtils.clusteredDataMetadataFileName(oldPath, oldClusterTxn)
                        )
                );
            }

            assertQuery("select k, s, v, n, ts from o3c where ts in '2024-01-01' order by ts")
                    .timestamp("ts")
                    .returns("k\ts\tv\tn\tts\n"
                            + "a\told-a0\tva0\t2\t2024-01-01T00:00:01.000000Z\n"
                            + "c\tnew-c\tnew-vc\t6\t2024-01-01T00:00:02.000000Z\n"
                            + "a\tlate-a\tlate-va\t5\t2024-01-01T00:00:03.000000Z\n"
                            + "b\told-b\tvb\t1\t2024-01-01T00:00:04.000000Z\n"
                            + "b\tlate-b\tlate-vb\t7\t2024-01-01T00:00:05.000000Z\n"
                            + "a\told-a1\tva1\t3\t2024-01-01T00:00:06.000000Z\n");
            assertQuery("select n, ts from o3c where p = 'x' and ts in '2024-01-01' order by ts")
                    .timestamp("ts")
                    .expectSize()
                    .returns("n\tts\n"
                            + "5\t2024-01-01T00:00:03.000000Z\n"
                            + "1\t2024-01-01T00:00:04.000000Z\n"
                            + "7\t2024-01-01T00:00:05.000000Z\n"
                            + "3\t2024-01-01T00:00:06.000000Z\n");

            try (TableReader reader = engine.getReader(token)) {
                reader.setClusteredReadMode();
                reader.openPartition(0);
                Assert.assertTrue(reader.isClusteredParquetPartition(0));
                Assert.assertNotEquals(oldClusterTxn, reader.getClusteredDataTxn(0));
                try (IndexMetaFileReader directory = new IndexMetaFileReader()) {
                    Assert.assertTrue(reader.openClusteredDataMetadata(0, directory));
                    directory.validateClusteredKeyDirectory();
                }
            }
        });
    }

    @Test
    public void testClusteredDirectoryPurgeIsReaderGatedAndProtectsLiveGeneration() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table purge_c (k symbol, v int, ts timestamp) "
                    + "timestamp(ts) partition by day order by k");
            execute("insert into purge_c values "
                    + "('b', 1, '2024-01-01T00:00:00.000000Z'),"
                    + "('a', 2, '2024-01-01T00:00:01.000000Z'),"
                    + "('z', 3, '2024-01-02T00:00:00.000000Z')");
            execute("alter table purge_c convert partition to parquet list '2024-01-01'");

            final TableToken token = engine.verifyTableName("purge_c");
            final FilesFacade ff = configuration.getFilesFacade();
            final TableReader reader = engine.getReader(token);
            try (PostingSealPurgeOperator purgeOperator = new PostingSealPurgeOperator(engine);
                 TxnScoreboard scoreboard = engine.getTxnScoreboard(token);
                 Path partitionPath = new Path()) {
                reader.setClusteredReadMode();
                reader.openPartition(0);
                final long partitionTimestamp = reader.getTxFile().getPartitionTimestampByIndex(0);
                final long partitionNameTxn = reader.getTxFile().getPartitionNameTxn(0);
                final long liveClusterTxn = reader.getClusteredDataTxn(0);
                final long pinnedTxn = reader.getTxn();
                final long heldScoreboardTxn = 1_000_000;
                final int timestampType = reader.getTxFile().getTimestampType();

                partitionPath.of(configuration.getDbRoot()).concat(token.getDirName());
                final int tablePathLen = partitionPath.size();
                TableUtils.setPathForParquetPartitionMetadata(
                        partitionPath.trimTo(tablePathLen),
                        timestampType,
                        PartitionBy.DAY,
                        partitionTimestamp,
                        partitionNameTxn
                );
                partitionPath.parent();
                final int partitionPathLen = partitionPath.size();
                reader.close();
                engine.releaseInactive();

                final PostingSealPurgeTask liveTask = new PostingSealPurgeTask();
                liveTask.of(
                        token,
                        "",
                        0,
                        liveClusterTxn,
                        PostingSealPurgeTask.ARTIFACT_FORM_CLUSTERED_DATA,
                        partitionTimestamp,
                        partitionNameTxn,
                        PartitionBy.DAY,
                        timestampType,
                        0,
                        pinnedTxn + 1
                );
                Assert.assertTrue(purgeOperator.purge(liveTask));
                Assert.assertTrue(ff.exists(TableUtils.clusteredDataMetadataFileName(
                        partitionPath.trimTo(partitionPathLen), liveClusterTxn
                )));

                final long orphanClusterTxn = liveClusterTxn + 1_000;
                final long orphanFd = TableUtils.openRW(
                        ff,
                        TableUtils.clusteredDataMetadataFileName(
                                partitionPath.trimTo(partitionPathLen), orphanClusterTxn
                        ),
                        LOG,
                        configuration.getWriterFileOpenOpts()
                );
                ff.close(orphanFd);
                final PostingSealPurgeTask orphanTask = new PostingSealPurgeTask();
                orphanTask.of(
                        token,
                        "",
                        0,
                        orphanClusterTxn,
                        PostingSealPurgeTask.ARTIFACT_FORM_CLUSTERED_DATA,
                        partitionTimestamp,
                        partitionNameTxn,
                        PartitionBy.DAY,
                        timestampType,
                        0,
                        heldScoreboardTxn + 1
                );
                Assert.assertTrue(scoreboard.acquireTxn(0, heldScoreboardTxn));
                try {
                    Assert.assertFalse(purgeOperator.purge(orphanTask));
                    Assert.assertTrue(ff.exists(TableUtils.clusteredDataMetadataFileName(
                            partitionPath.trimTo(partitionPathLen), orphanClusterTxn
                    )));
                } finally {
                    scoreboard.releaseTxn(0, heldScoreboardTxn);
                }
                Assert.assertTrue(purgeOperator.purge(orphanTask));
                Assert.assertFalse(ff.exists(TableUtils.clusteredDataMetadataFileName(
                        partitionPath.trimTo(partitionPathLen), orphanClusterTxn
                )));
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
            final String[] clusteredSidecarPath = new String[1];
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
                    clusteredSidecarPath[0] = path.toString();
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
            try (TableReader ordinaryReader = engine.getReader(tableToken)) {
                try {
                    ordinaryReader.openPartition(0);
                    Assert.fail("ordinary reader must not open clustered parquet");
                } catch (CairoException ex) {
                    Assert.assertTrue(ex.getFlyweightMessage().toString().contains(
                            "clustered parquet partition requires clustered read mode"
                    ));
                }
            }
            assertQuery("select coalesce(k, 'NULL') k, v from read_parquet('" + parquetRelativePath[0] + "')")
                    .expectSize()
                    .returns("k\tv\nNULL\t3\nb\t1\nb\t4\na\t2\na\t5\nc\t6\n");
            assertQuery("select v, ts from x")
                    .timestamp("ts")
                    .expectSize()
                    .returns("v\tts\n"
                            + "1\t2024-01-01T00:00:00.000000Z\n"
                            + "2\t2024-01-01T00:00:01.000000Z\n"
                            + "3\t2024-01-01T00:00:02.000000Z\n"
                            + "4\t2024-01-01T00:00:03.000000Z\n"
                            + "5\t2024-01-01T00:00:04.000000Z\n"
                            + "6\t2024-01-01T00:00:05.000000Z\n"
                            + "7\t2024-01-02T00:00:00.000000Z\n");
            assertQuery("select k, v from x")
                    .expectSize()
                    .returns("k\tv\n"
                            + "\t3\n"
                            + "b\t1\n"
                            + "b\t4\n"
                            + "a\t2\n"
                            + "a\t5\n"
                            + "c\t6\n"
                            + "z\t7\n");
            assertQuery("select v, ts from x order by ts desc")
                    .timestampDesc("ts")
                    .expectSize()
                    .returns("v\tts\n"
                            + "7\t2024-01-02T00:00:00.000000Z\n"
                            + "6\t2024-01-01T00:00:05.000000Z\n"
                            + "5\t2024-01-01T00:00:04.000000Z\n"
                            + "4\t2024-01-01T00:00:03.000000Z\n"
                            + "3\t2024-01-01T00:00:02.000000Z\n"
                            + "2\t2024-01-01T00:00:01.000000Z\n"
                            + "1\t2024-01-01T00:00:00.000000Z\n");
            assertQuery("select v, ts from x where ts between '2024-01-01T00:00:02.000000Z' and '2024-01-01T00:00:04.000000Z'")
                    .timestamp("ts")
                    .returns("v\tts\n"
                            + "3\t2024-01-01T00:00:02.000000Z\n"
                            + "4\t2024-01-01T00:00:03.000000Z\n"
                            + "5\t2024-01-01T00:00:04.000000Z\n");
            assertQuery("select v, ts from x where ts between '2024-01-01T00:00:02.000000Z' and '2024-01-01T00:00:04.000000Z' order by ts desc")
                    .timestampDesc("ts")
                    .returns("v\tts\n"
                            + "5\t2024-01-01T00:00:04.000000Z\n"
                            + "4\t2024-01-01T00:00:03.000000Z\n"
                            + "3\t2024-01-01T00:00:02.000000Z\n");
            assertQuery("select v, ts from x where k = 'a' order by ts")
                    .timestamp("ts")
                    .expectSize()
                    .returns("v\tts\n"
                            + "2\t2024-01-01T00:00:01.000000Z\n"
                            + "5\t2024-01-01T00:00:04.000000Z\n");
            assertQuery("select v, ts from x limit 4")
                    .timestamp("ts")
                    .expectSize()
                    .returns("v\tts\n"
                            + "1\t2024-01-01T00:00:00.000000Z\n"
                            + "2\t2024-01-01T00:00:01.000000Z\n"
                            + "3\t2024-01-01T00:00:02.000000Z\n"
                            + "4\t2024-01-01T00:00:03.000000Z\n");
            assertQuery("select count() from x")
                    .noRandomAccess()
                    .expectSize()
                    .returns("count\n7\n");
            assertQuery("select coalesce(k, 'NULL') k, first(v), last(v) from x order by k")
                    .expectSize()
                    .returns("k\tfirst\tlast\n"
                            + "NULL\t3\t3\n"
                            + "a\t2\t5\n"
                            + "b\t1\t4\n"
                            + "c\t6\t6\n"
                            + "z\t7\t7\n");
            assertQuery("select first(v), last(v), ts from x sample by 1d")
                    .timestamp("ts")
                    .expectSize()
                    .returns("first\tlast\tts\n"
                            + "1\t6\t2024-01-01T00:00:00.000000Z\n"
                            + "7\t7\t2024-01-02T00:00:00.000000Z\n");
            assertQuery("select coalesce(k, 'NULL') k, v, ts from x latest on ts partition by k order by k")
                    .expectSize()
                    .returns("k\tv\tts\n"
                            + "NULL\t3\t2024-01-01T00:00:02.000000Z\n"
                            + "a\t5\t2024-01-01T00:00:04.000000Z\n"
                            + "b\t4\t2024-01-01T00:00:03.000000Z\n"
                            + "c\t6\t2024-01-01T00:00:05.000000Z\n"
                            + "z\t7\t2024-01-02T00:00:00.000000Z\n");
            execute("create table q (k symbol, id int, ts timestamp) timestamp(ts) partition by day");
            execute("insert into q values "
                    + "('a', 10, '2024-01-01T00:00:02.500000Z'),"
                    + "('b', 11, '2024-01-01T00:00:04.500000Z'),"
                    + "('c', 12, '2024-01-02T00:00:00.500000Z')");
            assertQuery("select q.id, x.v from q asof join x on (k) order by id")
                    .expectSize()
                    .returns("id\tv\n10\t2\n11\t4\n12\t6\n");

            engine.releaseInactive();
            try (Path sidecar = new Path().of(clusteredSidecarPath[0])) {
                Assert.assertTrue(ff.exists(sidecar.$()));
                Assert.assertTrue(ff.removeQuiet(sidecar.$()));
            }
            assertQuery("select v, ts from x")
                    .fails(0, "clustered parquet directory size mismatch");
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
