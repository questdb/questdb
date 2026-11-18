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


package io.questdb.test.cairo.parquet;

import io.questdb.PropertyKey;
import io.questdb.cairo.CairoException;
import io.questdb.cairo.ParquetMetaFileReader;
import io.questdb.cairo.PartitionCompactionScanJob;
import io.questdb.cairo.TableReader;
import io.questdb.cairo.TableToken;
import io.questdb.cairo.TableUtils;
import io.questdb.cairo.TableWriter;
import io.questdb.cairo.TxReader;
import io.questdb.std.datetime.microtime.MicrosFormatUtils;
import io.questdb.std.str.Path;
import io.questdb.test.AbstractCairoTest;
import org.junit.Assert;
import org.junit.Test;

import java.io.File;

/**
 * {@link TableWriter#compactParquetPartition}: the writer-side rebuild of a parquet partition that is not compact, the
 * one that holds the writer for the copy.
 */
public class ParquetPartitionWriterCompactionTest extends AbstractCairoTest {
    private static final long DAY1 = parseTimestamp("2020-01-01T00:00:00.000000Z");
    private static final String LOOKUP_A = """
            s\tv\tts
            a\t1\t2020-01-01T01:00:00.000000Z
            a\t4\t2020-01-01T04:00:00.000000Z
            a\t7\t2020-01-01T07:00:00.000000Z
            a\t10\t2020-01-01T10:00:00.000000Z
            """;
    private static final String LOOKUP_B = """
            s\tv\tts
            b\t100\t2020-01-01T00:30:00.000000Z
            b\t2\t2020-01-01T02:00:00.000000Z
            b\t5\t2020-01-01T05:00:00.000000Z
            b\t8\t2020-01-01T08:00:00.000000Z
            b\t11\t2020-01-01T11:00:00.000000Z
            """;

    @Test
    public void testCompactPartitionIsLeftAlone() throws Exception {
        assertMemoryLeak(() -> {
            final TableToken token = createParquetTable("t", "");
            final long nameTxn = nameTxn(token);
            compactExpecting(token, false);
            Assert.assertEquals(nameTxn, nameTxn(token));
        });
    }

    @Test
    public void testDeadBytesOnlyKeepsIndexFiles() throws Exception {
        // Rows past the last one update the last row group in place, which leaves every index row where it was.
        assertMemoryLeak(() -> {
            final TableToken token = createParquetTable("t", " TYPE POSTING");
            execute("INSERT INTO t VALUES ('a', 200, '2020-01-01T13:00:00')");
            drainWalQueue();
            execute("INSERT INTO t VALUES ('a', 201, '2020-01-01T14:00:00')");
            drainWalQueue();
            Assert.assertFalse("precondition: no index row map", hasIndexRowMap(token));
            Assert.assertTrue("precondition: dead bytes", unusedBytes(token) > 0);
            final long nameTxn = nameTxn(token);
            compactExpecting(token, true);
            Assert.assertNotEquals(nameTxn, nameTxn(token));
            Assert.assertEquals(0, unusedBytes(token));
            assertQuery("SELECT s, v, ts FROM t WHERE ts IN '2020-01-01' AND s = 'a'")
                    .timestamp("ts")
                    .withPlanContaining("Index forward scan on: s")
                    .returns("""
                            s\tv\tts
                            a\t1\t2020-01-01T01:00:00.000000Z
                            a\t4\t2020-01-01T04:00:00.000000Z
                            a\t7\t2020-01-01T07:00:00.000000Z
                            a\t10\t2020-01-01T10:00:00.000000Z
                            a\t200\t2020-01-01T13:00:00.000000Z
                            a\t201\t2020-01-01T14:00:00.000000Z
                            """);
        });
    }

    @Test
    public void testDeltaActivePartitionIsLeftAlone() throws Exception {
        assertMemoryLeak(() -> {
            final TableToken token = createRowMapTable("t", "");
            final long nameTxn = nameTxn(token);
            try (TableWriter writer = engine.getWriter(token, "test")) {
                writer.getTxWriter().setPartitionDeltaActiveByTimestamp(DAY1);
                writer.bumpPartitionTableVersion();
                writer.commit();
                Assert.assertFalse(writer.compactParquetPartition(0));
            }
            Assert.assertEquals(nameTxn, nameTxn(token));
            Assert.assertTrue("the partition must keep its index row map", hasIndexRowMap(token));
        });
    }

    @Test
    public void testLeftoverDirectoryIsReplaced() throws Exception {
        // An earlier attempt at this txn died before its commit, and the writer stayed open.
        assertMemoryLeak(() -> {
            final TableToken token = createRowMapTable("t", "");
            try (TableWriter writer = engine.getWriter(token, "test"); Path path = new Path()) {
                path.of(configuration.getDbRoot()).concat(token);
                TableUtils.setPathForNativePartition(path, writer.getTimestampType(), writer.getPartitionBy(), DAY1, writer.getTxn());
                TableUtils.createDirsOrFail(configuration.getFilesFacade(), path, configuration.getMkDirMode());
                final File garbage = new File(path.toString(), "garbage");
                Assert.assertTrue(garbage.createNewFile());
                Assert.assertTrue(writer.compactParquetPartition(0));
                Assert.assertFalse("the leftover must be gone", garbage.exists());
            }
            assertLookups();
        });
    }

    @Test
    public void testOpenTransactionIsRefused() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (v INT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL");
            execute("INSERT INTO t VALUES (1, '2020-01-01'), (2, '2020-06-01')");
            final TableToken token = engine.verifyTableName("t");
            try (TableWriter writer = engine.getWriter(token, "test")) {
                final TableWriter.Row row = writer.newRow(parseTimestamp("2020-06-01T01:00:00.000000Z"));
                row.putInt(0, 3);
                row.append();
                try {
                    writer.compactParquetPartition(0);
                    Assert.fail("an open transaction must be refused");
                } catch (CairoException e) {
                    Assert.assertTrue(e.getMessage(), e.getMessage().contains("open transaction"));
                }
                writer.rollback();
            }
        });
    }

    @Test
    public void testPublishesAheadOfTheJobsStagedSwap() throws Exception {
        // The job has built off a reader and queued its swap when the writer rebuilds. The swap must be discarded,
        // staging directory included, and leave the writer's rebuild standing.
        assertMemoryLeak(() -> {
            final TableToken token = createRowMapTable("t", "");
            node1.setProperty(PropertyKey.CAIRO_PARTITION_COMPACTION_CHECK_INTERVAL, -1);
            final long newNameTxn;
            try (
                    PartitionCompactionScanJob job = new PartitionCompactionScanJob(engine);
                    TableWriter writer = engine.getWriter(token, "test")
            ) {
                Assert.assertTrue(engine.getPartitionCompactionRequestQueue().tryRequest(token, DAY1));
                Assert.assertTrue("the request must dispatch the rebuild", job.run());
                Assert.assertTrue("the job must have staged its build", stagingDirExists(token));
                Assert.assertTrue(writer.compactParquetPartition(0));
                newNameTxn = writer.getTxWriter().getPartitionNameTxn(0);
                writer.tick(true);
                Assert.assertEquals("the stale swap must not replace the rebuild", newNameTxn, writer.getTxWriter().getPartitionNameTxn(0));
            }
            Assert.assertFalse("the discarded swap must remove its staging directory", stagingDirExists(token));
            Assert.assertEquals(newNameTxn, nameTxn(token));
            assertLookups();
        });
    }

    @Test
    public void testRebuildsRowMapBitmap() throws Exception {
        assertRebuildsRowMap("");
    }

    @Test
    public void testRebuildsRowMapCovering() throws Exception {
        assertRebuildsRowMap(" TYPE POSTING INCLUDE (v)");
    }

    @Test
    public void testRebuildsRowMapPosting() throws Exception {
        assertRebuildsRowMap(" TYPE POSTING");
    }

    @Test
    public void testRemotePartitionIsLeftAlone() throws Exception {
        assertMemoryLeak(() -> {
            final TableToken token = createRowMapTable("t", "");
            final long nameTxn = nameTxn(token);
            try (TableWriter writer = engine.getWriter(token, "test")) {
                writer.getTxWriter().setPartitionRemoteByTimestamp(DAY1, true);
                writer.bumpPartitionTableVersion();
                writer.commit();
                Assert.assertFalse(writer.compactParquetPartition(0));
            }
            Assert.assertEquals(nameTxn, nameTxn(token));
        });
    }

    private static void compactExpecting(TableToken token, boolean expected) {
        try (TableWriter writer = engine.getWriter(token, "test")) {
            Assert.assertEquals(expected, writer.compactParquetPartition(0));
        }
        engine.releaseAllReaders();
    }

    private static boolean hasIndexRowMap(TableToken token) {
        try (TableReader reader = engine.getReader(token)) {
            reader.openPartition(0);
            return reader.getAndInitParquetPartitionDecoder(0).metadata().hasIndexRowMap();
        }
    }

    private static long nameTxn(TableToken token) {
        try (TableReader reader = engine.getReader(token)) {
            final TxReader tx = reader.getTxFile();
            return tx.getPartitionNameTxn(0);
        }
    }

    private static long parseTimestamp(String timestamp) {
        try {
            return MicrosFormatUtils.parseTimestamp(timestamp);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private static boolean stagingDirExists(TableToken token) {
        final String[] names = new File(configuration.getDbRoot().toString(), token.getDirName()).list();
        Assert.assertNotNull(names);
        for (String name : names) {
            if (name.contains(TableUtils.COMPACTING_DIR_MARKER)) {
                return true;
            }
        }
        return false;
    }

    private static long unusedBytes(TableToken token) {
        try (TableReader reader = engine.getReader(token)) {
            reader.openPartition(0);
            final ParquetMetaFileReader metadata = reader.getAndInitParquetPartitionDecoder(0).metadata();
            return metadata.getUnusedBytes();
        }
    }

    private void assertLookups() throws Exception {
        assertQuery("SELECT s, v, ts FROM t WHERE ts IN '2020-01-01' AND s = 'a'")
                .timestamp("ts")
                .withPlanContaining("Index forward scan on: s")
                .returns(LOOKUP_A);
        assertQuery("SELECT s, v, ts FROM t WHERE ts IN '2020-01-01' AND s = 'b'")
                .timestamp("ts")
                .withPlanContaining("Index forward scan on: s")
                .returns(LOOKUP_B);
    }

    private void assertRebuildsRowMap(String indexClause) throws Exception {
        assertMemoryLeak(() -> {
            final TableToken token = createRowMapTable("t", indexClause);
            final long nameTxn = nameTxn(token);
            compactExpecting(token, true);
            Assert.assertNotEquals("the rebuild lands in a new directory", nameTxn, nameTxn(token));
            Assert.assertFalse("the index row map must be gone", hasIndexRowMap(token));
            Assert.assertEquals("the dead bytes must be gone", 0, unusedBytes(token));
            final boolean isCovering = indexClause.contains("INCLUDE");
            for (String key : new String[]{"a", "b"}) {
                assertQuery("SELECT s, v, ts FROM t WHERE ts IN '2020-01-01' AND s = '" + key + "'")
                        .timestamp("ts")
                        .supportsRandomAccess(!isCovering)
                        .expectSize(isCovering)
                        .withPlanContaining(isCovering ? "CoveringIndex on: s" : "Index forward scan on: s")
                        .returns("a".equals(key) ? LOOKUP_A : LOOKUP_B);
            }
            // The rebuild is complete: a second call finds nothing to do.
            final long rebuiltNameTxn = nameTxn(token);
            compactExpecting(token, false);
            Assert.assertEquals(rebuiltNameTxn, nameTxn(token));
        });
    }

    private TableToken createParquetTable(String name, String indexClause) throws Exception {
        node1.setProperty(PropertyKey.CAIRO_PARTITION_ENCODER_PARQUET_ROW_GROUP_SIZE, 4);
        node1.setProperty(PropertyKey.CAIRO_PARTITION_ENCODER_PARQUET_O3_REWRITE_UNUSED_RATIO, "1.0");
        node1.setProperty(PropertyKey.CAIRO_PARTITION_ENCODER_PARQUET_O3_REWRITE_UNUSED_MAX_BYTES, Long.MAX_VALUE);
        execute("CREATE TABLE " + name + " (s SYMBOL, v INT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
        execute("ALTER TABLE " + name + " ALTER COLUMN s ADD INDEX" + indexClause);
        execute("INSERT INTO " + name + """
                 VALUES
                ('a', 1, '2020-01-01T01:00:00'), ('b', 2, '2020-01-01T02:00:00'),
                ('c', 3, '2020-01-01T03:00:00'), ('a', 4, '2020-01-01T04:00:00'),
                ('b', 5, '2020-01-01T05:00:00'), ('c', 6, '2020-01-01T06:00:00'),
                ('a', 7, '2020-01-01T07:00:00'), ('b', 8, '2020-01-01T08:00:00'),
                ('c', 9, '2020-01-01T09:00:00'), ('a', 10, '2020-01-01T10:00:00'),
                ('b', 11, '2020-01-01T11:00:00'), ('c', 12, '2020-01-01T12:00:00'),
                ('a', 99, '2020-06-01T00:00:00')
                """);
        drainWalQueue();
        execute("ALTER TABLE " + name + " CONVERT PARTITION TO PARQUET LIST '2020-01-01'");
        drainWalQueue();
        return engine.verifyTableName(name);
    }

    private TableToken createRowMapTable(String name, String indexClause) throws Exception {
        final TableToken token = createParquetTable(name, indexClause);
        execute("INSERT INTO " + name + " VALUES ('b', 100, '2020-01-01T00:30:00')");
        drainWalQueue();
        Assert.assertTrue("precondition: the late row must leave an index row map", hasIndexRowMap(token));
        return token;
    }
}
