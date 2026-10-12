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
import io.questdb.cairo.AttachDetachStatus;
import io.questdb.cairo.CairoException;
import io.questdb.cairo.CommitMode;
import io.questdb.cairo.TableUtils;
import io.questdb.cairo.TableWriter;
import io.questdb.cairo.wal.LocalDurabilityPolicy;
import io.questdb.cairo.wal.WalWriter;
import io.questdb.std.Os;
import io.questdb.std.str.LPSZ;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.std.TestFilesFacadeImpl;
import io.questdb.test.tools.TestUtils;
import org.junit.After;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Stream;

/**
 * Pins how many directory fsyncs each commit mode pays on the ingest and apply paths, per partition switch,
 * O3 commit into the last partition, WAL writer open, WAL segment roll and WAL column DDL that rolls pending
 * rows, the fsyncs of the metadata swap per schema change, and those of the files a schema change rewrites.
 * <p>
 * A directory fsync makes names durable, and a mode takes one only where it promises that what the name
 * points at survives a power loss:
 * <ul>
 *   <li>{@code TableWriter.openPartition} fsyncs the partition directory and the table directory under SYNC
 *   only. ADAPTIVE relies on its durable epoch and WAL replay, ASYNC and NOSYNC promise nothing.</li>
 *   <li>The WAL writer fsyncs a new segment's entry in {@code wal<N>} under SYNC and ADAPTIVE, and the table
 *   directory once per writer, when it has created {@code wal<N>}. ASYNC takes only the segment directory
 *   fsync of a newly opened segment.</li>
 *   <li>A schema change swaps {@code _meta} and fsyncs {@code _meta.swp}, {@code _todo_} and the table
 *   directory twice, under SYNC and ADAPTIVE only.</li>
 *   <li>A schema change that rewrites data (ALTER COLUMN TYPE, CONVERT PARTITION) fsyncs the files it writes
 *   under SYNC and ADAPTIVE only, except what ADAPTIVE rebuilds from the WAL; ASYNC takes only the parquet
 *   metadata and partition directory fsyncs. SYNC also fsyncs the directories that name the new
 *   files: each partition directory ALTER COLUMN TYPE writes into, and the partition directory CONVERT
 *   PARTITION creates, then the table directory.</li>
 *   <li>The switch of a native partition to its parquet file links the files into a new partition directory and
 *   fsyncs it, then the table directory, under SYNC only.</li>
 * </ul>
 * The crash tests that prove these are enough are in {@code DirectoryBarrierCrashTest}.
 */
@RunWith(Parameterized.class)
public class DirectoryBarrierCountTest extends AbstractCairoTest {
    private final String commitMode;
    private DirFsyncCountingFacade ff;

    public DirectoryBarrierCountTest(String commitMode) {
        this.commitMode = commitMode;
    }

    @Parameterized.Parameters(name = "{0}")
    public static Collection<Object[]> data() {
        return Arrays.asList(new Object[][]{{"nosync"}, {"async"}, {"sync"}, {"adaptive"}});
    }

    @Override
    @Before
    public void setUp() {
        super.setUp();
        // Windows cannot open a directory for fsync, so it takes none of these barriers in any mode.
        Assume.assumeFalse(Os.isWindows());
        node1.setProperty(PropertyKey.CAIRO_COMMIT_MODE, commitMode);
        // No durable epoch inside a measured window: an epoch fsyncs the table directory itself.
        node1.setProperty(PropertyKey.CAIRO_ADAPTIVE_EPOCH_INTERVAL, 3_600_000);
        ff = new DirFsyncCountingFacade();
    }

    @Override
    @After
    public void tearDown() throws Exception {
        engine.setLocalDurabilityPolicy(LocalDurabilityPolicy.ALWAYS_ON);
        super.tearDown();
    }

    @Test
    public void testNonWalO3IntoLastPartition() throws Exception {
        assertMemoryLeak(ff, () -> {
            execute("CREATE TABLE x (ts TIMESTAMP, v LONG) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL");
            execute("""
                    INSERT INTO x VALUES
                        ('2024-01-01T00:00:00.000000Z', 0),
                        ('2024-01-02T00:00:00.000000Z', 0),
                        ('2024-01-02T12:00:00.000000Z', 0)
                    """);
            final String tableDir = tableDir("x");
            ff.clear();
            for (int i = 1; i <= 5; i++) {
                // Each commit writes a new version of the last partition, which the writer then reopens.
                execute("INSERT INTO x VALUES ('2024-01-02T0" + i + ":00:00.000000Z', " + i + ")");
            }
            assertTableSide(5, tableDir);
        });
    }

    @Test
    public void testNonWalOneCommitAcrossPartitions() throws Exception {
        assertMemoryLeak(ff, () -> {
            execute("CREATE TABLE x (ts TIMESTAMP, v LONG) TIMESTAMP(ts) PARTITION BY HOUR BYPASS WAL");
            final String tableDir = tableDir("x");
            ff.clear();
            execute("""
                    INSERT INTO x
                    SELECT timestamp_sequence('2024-01-01T00:00:00.000000Z', 3_600_000_000L), x
                    FROM long_sequence(48)
                    """);
            assertTableSide(48, tableDir);
        });
    }

    @Test
    public void testNonWalPartitionSwitch() throws Exception {
        assertMemoryLeak(ff, () -> {
            execute("CREATE TABLE x (ts TIMESTAMP, v LONG) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL");
            execute("INSERT INTO x VALUES ('2024-01-01T00:00:00.000000Z', 0)");
            final String tableDir = tableDir("x");
            ff.clear();
            for (int i = 1; i <= 5; i++) {
                execute("INSERT INTO x VALUES ('2024-01-0" + (i + 1) + "T00:00:00.000000Z', " + i + ")");
            }
            assertTableSide(5, tableDir);

            // Appending to the open partition creates nothing, so no mode fsyncs a directory for it.
            ff.clear();
            for (int i = 1; i <= 5; i++) {
                execute("INSERT INTO x VALUES ('2024-01-06T0" + i + ":00:00.000000Z', " + i + ")");
            }
            assertTableSide(0, tableDir);
        });
    }

    @Test
    public void testOpenWriterFlushModeFollowsPolicy() throws Exception {
        assertMemoryLeak(ff, () -> {
            execute("CREATE TABLE w (ts TIMESTAMP, v LONG) TIMESTAMP(ts) PARTITION BY DAY WAL");
            try (TableWriter writer = getWriter("w")) {
                final int configured = CommitMode.fromString(commitMode);
                Assert.assertEquals(configured, writer.getEffectiveCommitMode());
                Assert.assertEquals(configured, writer.getFlushCommitMode());
                engine.setLocalDurabilityPolicy(LocalDurabilityPolicy.REPLICA_SKIP);
                Assert.assertEquals("role changes must not erase lifecycle truth", configured, writer.getEffectiveCommitMode());
                Assert.assertEquals(configured == CommitMode.ADAPTIVE ? CommitMode.NOSYNC : configured, writer.getFlushCommitMode());
                engine.setLocalDurabilityPolicy(LocalDurabilityPolicy.ALWAYS_ON);
                Assert.assertEquals(configured, writer.getFlushCommitMode());
            }
        });
    }

    @Test
    public void testReplicaParquetDetachAttach() throws Exception {
        assertMemoryLeak(ff, () -> {
            engine.setLocalDurabilityPolicy(LocalDurabilityPolicy.REPLICA_SKIP);
            execute("CREATE TABLE w (ts TIMESTAMP, v LONG) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("INSERT INTO w VALUES ('2024-01-01', 1), ('2024-01-02', 2)");
            execute("ALTER TABLE w CONVERT PARTITION TO PARQUET LIST '2024-01-01'");
            drainWalQueue();
            final String tableDir = tableDir("w");
            final boolean metadataSync = "sync".equals(commitMode) || "async".equals(commitMode);
            // DETACH's two durability passes and ATTACH's regenerated _pm must also use replica grade.
            try (TableWriter writer = getWriter("w")) {
                final long timestamp = writer.getTxWriter().getPartitionTimestampByIndex(0);
                ff.clear();
                Assert.assertEquals(AttachDetachStatus.OK, writer.detachPartition(timestamp));
                if (!metadataSync) {
                    Assert.assertEquals(ff.dump(), 0, ff.total());
                    Assert.assertEquals(ff.dump(), 0, ff.countOther());
                }
                final Path detached = Paths.get(tableDir, "2024-01-01" + TableUtils.DETACHED_DIR_MARKER);
                final Path attachable = Paths.get(tableDir, "2024-01-01" + configuration.getAttachPartitionSuffix());
                Files.move(detached, attachable);
                Files.delete(attachable.resolve(TableUtils.PARQUET_METADATA_FILE_NAME));
                ff.clear();
                Assert.assertEquals(AttachDetachStatus.OK, writer.attachPartition(timestamp));
                Assert.assertEquals(ff.dump(), metadataSync ? 1 : 0, ff.countOther());
                Assert.assertEquals(ff.dump(), metadataSync ? 1 : 0, ff.countChildren(tableDir));
            }
            assertQuery("SELECT sum(v) FROM w").noRandomAccess().expectSize().returns("sum\n3\n");
        });
    }

    @Test
    public void testReplicaStructuralFlushes() throws Exception {
        assertMemoryLeak(ff, () -> {
            engine.setLocalDurabilityPolicy(LocalDurabilityPolicy.REPLICA_SKIP);
            execute("CREATE TABLE w (ts TIMESTAMP, v LONG) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("INSERT INTO w VALUES ('2024-01-01', 1), ('2024-01-02', 2)");
            drainWalQueue();
            final String tableDir = tableDir("w");
            final boolean sync = "sync".equals(commitMode);
            final boolean metadataSync = sync || "async".equals(commitMode);
            ff.clear();
            execute("ALTER TABLE w ADD COLUMN c INT");
            drainWalQueue();
            Assert.assertEquals(ff.dump(), sync ? 1 : 0, ff.count(tableDir + "/_meta.swp"));
            Assert.assertEquals(ff.dump(), sync ? 2 : 0, ff.count(tableDir));
            ff.clear();
            execute("ALTER TABLE w ALTER COLUMN v TYPE DOUBLE");
            drainWalQueue();
            Assert.assertEquals(ff.dump(), sync ? 2 : 0, ff.countOther());
            Assert.assertEquals(ff.dump(), sync ? 2 : 0, ff.countChildren(tableDir));
            ff.clear();
            execute("ALTER TABLE w CONVERT PARTITION TO PARQUET LIST '2024-01-01'");
            drainWalQueue();
            Assert.assertEquals(ff.dump(), (sync ? 1 : 0) + (metadataSync ? 1 : 0), ff.countOther());
            Assert.assertEquals(ff.dump(), (sync ? 1 : 0) + (metadataSync ? 1 : 0), ff.countChildren(tableDir));
            ff.clear();
            execute("ALTER TABLE w CONVERT PARTITION TO NATIVE LIST '2024-01-01'");
            drainWalQueue();
            Assert.assertEquals(ff.dump(), sync ? 3 : 0, ff.countOther());
            Assert.assertEquals(ff.dump(), sync ? 2 : 0, ff.countChildren(tableDir));
            assertQuery("SELECT sum(v) FROM w").noRandomAccess().expectSize().returns("sum\n3.0\n");
        });
    }

    @Test
    public void testReplicaSwitchToParquet() throws Exception {
        assertSwitchToParquet(true);
    }

    @Test
    public void testSchemaChangeMetaSwap() throws Exception {
        assertMemoryLeak(ff, () -> {
            execute("CREATE TABLE x (ts TIMESTAMP, s SYMBOL, v LONG) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL");
            execute("INSERT INTO x VALUES ('2024-01-01T00:00:00.000000Z', 'a', 1)");
            execute("CREATE TABLE w (ts TIMESTAMP, s SYMBOL, v LONG) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("INSERT INTO w VALUES ('2024-01-01T00:00:00.000000Z', 'a', 1)");
            drainWalQueue();
            final String[] ddls = {
                    "ALTER TABLE x ADD COLUMN c INT",
                    "ALTER TABLE x RENAME COLUMN c TO d",
                    "ALTER TABLE x DROP COLUMN d",
                    "ALTER TABLE x ALTER COLUMN v TYPE DOUBLE",
                    "ALTER TABLE x ALTER COLUMN s ADD INDEX",
                    "ALTER TABLE x ALTER COLUMN s DROP INDEX",
                    "ALTER TABLE x ALTER COLUMN s SYMBOL CAPACITY 512",
                    "ALTER TABLE x ALTER COLUMN s NOCACHE",
                    "ALTER TABLE x SET PARAM maxUncommittedRows = 100",
                    "ALTER TABLE x SET PARAM o3MaxLag = 5s",
                    "ALTER TABLE x SET TTL 2 DAYS",
                    "ALTER TABLE w ADD COLUMN c INT",
                    "ALTER TABLE w DEDUP ENABLE UPSERT KEYS(ts)",
                    "ALTER TABLE w DEDUP DISABLE"
            };
            // Each schema change swaps _meta once. The swap fsyncs _meta.swp, _todo_ and the table directory
            // twice, in the modes that promise power-loss durability only.
            final int expected = isSyncOrAdaptive() ? 1 : 0;
            for (String ddl : ddls) {
                final String tableDir = tableDir(ddl.substring(12, 13));
                ff.clear();
                execute(ddl);
                drainWalQueue();
                final String message = ddl + ff.dump();
                Assert.assertEquals(message, 1, ff.countRenamesTo(tableDir + "/_meta"));
                Assert.assertEquals(message, expected, ff.count(tableDir + "/_meta.swp"));
                Assert.assertEquals(message, expected, ff.count(tableDir + "/_todo_"));
                Assert.assertEquals(message, 2 * expected, ff.count(tableDir));
            }

            // A swap that fails to publish _meta.swp restores _meta from _meta.prev, and fsyncs the table
            // directory for the restore instead of the publish, in the same modes.
            final String tableDir = tableDir("x");
            ff.clear();
            ff.failRenameTo = tableDir + "/_meta";
            try {
                execute("ALTER TABLE x ADD COLUMN e INT");
                Assert.fail("the _meta.swp rename must fail");
            } catch (CairoException e) {
                TestUtils.assertContains(e.getFlyweightMessage(), "could not rename");
            }
            final String message = "rollback" + ff.dump();
            Assert.assertEquals(message, 2, ff.countRenamesTo(tableDir + "/_meta"));
            Assert.assertEquals(message, expected, ff.count(tableDir + "/_meta.swp"));
            Assert.assertEquals(message, expected, ff.count(tableDir + "/_todo_"));
            Assert.assertEquals(message, 2 * expected, ff.count(tableDir));
            execute("ALTER TABLE x ADD COLUMN e INT");
            assertQuery("SELECT e FROM x").expectSize().returns("e\nnull\n");
        });
    }

    @Test
    public void testSchemaChangeDataFileFsyncs() throws Exception {
        assertMemoryLeak(ff, () -> {
            final boolean durable = isSyncOrAdaptive();
            final boolean isNoSync = "nosync".equals(commitMode);
            final boolean isSync = "sync".equals(commitMode);
            for (String table : new String[]{"x", "w"}) {
                execute("CREATE TABLE " + table + " (ts TIMESTAMP, s SYMBOL, v LONG) TIMESTAMP(ts) PARTITION BY DAY "
                        + ("x".equals(table) ? "BYPASS WAL" : "WAL"));
                execute("INSERT INTO " + table + " VALUES ('2024-01-01T00:00:00.000000Z', 'a', 1), "
                        + "('2024-01-02T00:00:00.000000Z', 'b', 2), ('2024-01-03T00:00:00.000000Z', 'c', 3)");
                drainWalQueue();
                // The apply job takes a save point for an adaptive WAL table, which fsyncs more files.
                final boolean exact = "x".equals(table) || !"adaptive".equals(commitMode);

                final String tableDir = tableDir(table);

                // The converted column file of each of the three partitions, under SYNC and ADAPTIVE; under SYNC
                // also the three partition directories. The table directory twice is the _meta swap.
                ff.clear();
                execute("ALTER TABLE " + table + " ALTER COLUMN v TYPE DOUBLE");
                drainWalQueue();
                if (exact) {
                    Assert.assertEquals("alter column type" + ff.dump(), durable ? 3 : 0, ff.countOther());
                    Assert.assertEquals("alter column type" + ff.dump(), isSync ? 3 : 0, ff.countChildren(tableDir));
                    Assert.assertEquals("alter column type" + ff.dump(), durable ? 2 : 0, ff.count(tableDir));
                }

                // data.parquet under SYNC and ADAPTIVE; _pm and the partition directory in every mode but NOSYNC;
                // under SYNC the partition directory again, after its index files, and the table directory.
                ff.clear();
                execute("ALTER TABLE " + table + " CONVERT PARTITION TO PARQUET LIST '2024-01-01'");
                drainWalQueue();
                if (exact) {
                    Assert.assertEquals("convert to parquet" + ff.dump(), (durable ? 1 : 0) + (isNoSync ? 0 : 1), ff.countOther());
                    Assert.assertEquals("convert to parquet" + ff.dump(), (isNoSync ? 0 : 1) + (isSync ? 1 : 0), ff.countChildren(tableDir));
                    Assert.assertEquals("convert to parquet" + ff.dump(), isSync ? 1 : 0, ff.count(tableDir));
                    Assert.assertEquals("convert to parquet" + ff.dump(), (isNoSync ? 0 : 1) + (isSync ? 2 : 0), ff.total());
                }

                // The three column files under SYNC only, ADAPTIVE replays the conversion; the partition directory
                // under SYNC and ADAPTIVE; under SYNC the partition directory again, after its index files, and
                // the table directory.
                ff.clear();
                execute("ALTER TABLE " + table + " CONVERT PARTITION TO NATIVE LIST '2024-01-01'");
                drainWalQueue();
                if (exact) {
                    Assert.assertEquals("convert to native" + ff.dump(), isSync ? 3 : 0, ff.countOther());
                    Assert.assertEquals("convert to native" + ff.dump(), (durable ? 1 : 0) + (isSync ? 1 : 0), ff.countChildren(tableDir));
                    Assert.assertEquals("convert to native" + ff.dump(), isSync ? 1 : 0, ff.count(tableDir));
                    Assert.assertEquals("convert to native" + ff.dump(), (durable ? 1 : 0) + (isSync ? 2 : 0), ff.total());
                }
            }
            assertQuery("SELECT sum(v) FROM w").noRandomAccess().expectSize().returns("sum\n6.0\n");
        });
    }

    @Test
    public void testSwitchToParquet() throws Exception {
        assertSwitchToParquet(false);
    }

    @Test
    public void testWalApplyPartitionSwitch() throws Exception {
        assertMemoryLeak(ff, () -> {
            execute("CREATE TABLE x (ts TIMESTAMP, v LONG) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("INSERT INTO x VALUES ('2024-01-01T00:00:00.000000Z', 0)");
            drainWalQueue();
            final String tableDir = tableDir("x");
            ff.clear();
            for (int i = 1; i <= 5; i++) {
                execute("INSERT INTO x VALUES ('2024-01-0" + (i + 1) + "T00:00:00.000000Z', " + i + ")");
                drainWalQueue();
            }
            // Apply creates each new partition through O3 and then reopens it as the last partition.
            assertTableSide(5, tableDir);
            assertQuery("SELECT count() FROM x").noRandomAccess().expectSize().returns("count\n6\n");
        });
    }

    @Test
    public void testWalColumnDdlRollsPendingRows() throws Exception {
        assertMemoryLeak(ff, () -> {
            execute("CREATE TABLE x (ts TIMESTAMP, v LONG) TIMESTAMP(ts) PARTITION BY DAY WAL");
            try (WalWriter writer = getWalWriter("x")) {
                final String walDir = tableDir("x") + "/" + writer.getWalName();
                appendRow(writer, 0, 1);
                writer.commit();
                appendRow(writer, 1, 2);
                execute("ALTER TABLE x ADD COLUMN c INT");
                ff.clear();
                // NO_TXN: the writer replays the ALTER, rolls the pending row into a new segment, then adds
                // the column's files to that segment.
                writer.commit();
                Assert.assertEquals("the pending row must have rolled to a new segment", 1, writer.getSegmentId());
                final boolean isDurable = isSyncOrAdaptive();
                Assert.assertEquals("wal directory fsyncs", isDurable ? 1 : 0, ff.count(walDir));
                Assert.assertEquals("segment directory fsyncs", isDurable ? 2 : 0, ff.count(walDir + "/1"));
                Assert.assertEquals("fsyncs of an empty directory", 0, ff.countEmpty());
            }
        });
    }

    @Test
    public void testWalWriterOpenAndSegmentRoll() throws Exception {
        node1.setProperty(PropertyKey.CAIRO_WAL_SEGMENT_ROLLOVER_ROW_COUNT, 3);
        assertMemoryLeak(ff, () -> {
            execute("CREATE TABLE x (ts TIMESTAMP, v LONG) TIMESTAMP(ts) PARTITION BY DAY WAL");
            final String tableDir = tableDir("x");
            ff.clear();
            try (WalWriter writer = getWalWriter("x")) {
                final String walDir = tableDir + "/" + writer.getWalName();
                final boolean isDurable = isSyncOrAdaptive();
                final int segmentDirFsyncs = "nosync".equals(commitMode) ? 0 : 1;
                Assert.assertEquals("open: table directory fsyncs", isDurable ? 1 : 0, ff.count(tableDir));
                Assert.assertEquals("open: wal directory fsyncs", isDurable ? 1 : 0, ff.count(walDir));
                Assert.assertEquals("open: segment directory fsyncs", segmentDirFsyncs, ff.count(walDir + "/0"));

                ff.clear();
                long v = 0;
                for (int commit = 0; commit < 4; commit++) {
                    for (int row = 0; row < 3; row++) {
                        appendRow(writer, v * 1_000_000L, v++);
                    }
                    writer.commit();
                }
                Assert.assertEquals("four commits of three rows must roll three times", 3, writer.getSegmentId());
                Assert.assertEquals("rolls: table directory fsyncs", 0, ff.count(tableDir));
                Assert.assertEquals("rolls: wal directory fsyncs", isDurable ? 3 : 0, ff.count(walDir));
                for (int segment = 1; segment <= 3; segment++) {
                    Assert.assertEquals("roll: segment directory fsyncs", segmentDirFsyncs, ff.count(walDir + "/" + segment));
                }
                Assert.assertEquals("rolls: all directory fsyncs", (isDurable ? 3 : 0) + 3 * segmentDirFsyncs, ff.total());
                Assert.assertEquals("fsyncs of an empty directory", 0, ff.countEmpty());
            }
        });
    }

    private static void appendRow(WalWriter writer, long ts, long v) {
        final TableWriter.Row row = writer.newRow(ts);
        row.putLong(1, v);
        row.append();
    }

    private void assertSwitchToParquet(boolean replica) throws Exception {
        assertMemoryLeak(ff, () -> {
            if (replica) {
                engine.setLocalDurabilityPolicy(LocalDurabilityPolicy.REPLICA_SKIP);
            }
            final boolean isSync = "sync".equals(commitMode);
            for (String table : new String[]{"x", "w"}) {
                execute("CREATE TABLE " + table + " (ts TIMESTAMP, s SYMBOL INDEX, v LONG) TIMESTAMP(ts) PARTITION BY DAY "
                        + ("x".equals(table) ? "BYPASS WAL" : "WAL"));
                execute("INSERT INTO " + table + " VALUES ('2024-01-01T00:00:00.000000Z', 'a', 1), "
                        + "('2024-01-02T00:00:00.000000Z', 'b', 2)");
                drainWalQueue();
                // The durable epoch an adaptive WAL table takes after the switch fsyncs more.
                final boolean exact = replica || "x".equals(table) || !"adaptive".equals(commitMode);

                final String tableDir = tableDir(table);
                // The cold storage job leaves data.parquet and _pm in the native partition; the switch links them.
                final Path partitionDir;
                try (Stream<Path> dirs = Files.list(Paths.get(tableDir))) {
                    partitionDir = dirs.filter(p -> p.getFileName().toString().startsWith("2024-01-01")).findFirst().orElseThrow();
                }
                Files.createFile(partitionDir.resolve(TableUtils.PARQUET_PARTITION_NAME));
                Files.createFile(partitionDir.resolve(TableUtils.PARQUET_METADATA_FILE_NAME));

                // Under SYNC only, the new partition directory, once the parquet and index files are linked into
                // it, then the table directory.
                ff.clear();
                try (TableWriter writer = getWriter(table)) {
                    final long partitionTimestamp = writer.getTxWriter().getPartitionTimestampByIndex(0);
                    Assert.assertTrue(writer.markPartitionParquetReady(partitionTimestamp));
                    Assert.assertEquals(TableWriter.SWITCH_OK, writer.switchNativePartitionWithParquet(partitionTimestamp, 0));
                }
                if (exact) {
                    Assert.assertEquals("switch to parquet" + ff.dump(), 0, ff.countOther());
                    Assert.assertEquals("switch to parquet" + ff.dump(), isSync ? 1 : 0, ff.countChildren(tableDir));
                    Assert.assertEquals("switch to parquet" + ff.dump(), isSync ? 1 : 0, ff.count(tableDir));
                    Assert.assertEquals("switch to parquet" + ff.dump(), isSync ? 2 : 0, ff.total());
                    Assert.assertEquals("switch to parquet" + ff.dump(), 0, ff.countEmpty());
                }
            }
        });
    }

    /**
     * Asserts the partition-directory and table-directory fsyncs of the measured window: one of each per
     * partition that {@code openPartition} opened under SYNC, none in any other mode.
     */
    private void assertTableSide(int syncCount, String tableDir) {
        final int expected = "sync".equals(commitMode) ? syncCount : 0;
        Assert.assertEquals("partition directory fsyncs" + ff.dump(), expected, ff.countChildren(tableDir));
        Assert.assertEquals("table directory fsyncs" + ff.dump(), expected, ff.count(tableDir));
        Assert.assertEquals("all directory fsyncs" + ff.dump(), 2 * expected, ff.total());
        // A partition directory fsynced before its column files exist persists an empty directory.
        Assert.assertEquals("fsyncs of an empty directory", 0, ff.countEmpty());
    }

    private boolean isSyncOrAdaptive() {
        return "sync".equals(commitMode) || "adaptive".equals(commitMode);
    }

    private String tableDir(String tableName) {
        return Paths.get(root, engine.verifyTableName(tableName).getDirName()).toString();
    }

    /**
     * Counts fsyncs of directories by path. A directory fsync opens the directory with
     * {@code openRONoCache}, so that is where the facade learns which descriptors are directories. Also counts
     * fsyncs of the {@code _meta.swp} and {@code _todo_} files of a metadata swap, which it opens with
     * {@code openRWNoCache}, the fsyncs of every other file, and renames by target path.
     */
    private static class DirFsyncCountingFacade extends TestFilesFacadeImpl {
        private final Map<String, Integer> counts = new HashMap<>();
        private final Map<Long, String> dirFds = new HashMap<>();
        private final Map<Long, String> fileFds = new HashMap<>();
        private final Map<String, Integer> renameCounts = new HashMap<>();
        private int emptyCount;
        private volatile String failRenameTo;
        private int otherCount;

        @Override
        public synchronized boolean close(long fd) {
            dirFds.remove(fd);
            fileFds.remove(fd);
            return super.close(fd);
        }

        @Override
        public void fsync(long fd) {
            record(fd, false);
            super.fsync(fd);
        }

        @Override
        public void fsyncAndClose(long fd) {
            record(fd, true);
            super.fsyncAndClose(fd);
        }

        @Override
        public long openRONoCache(LPSZ name) {
            final long fd = super.openRONoCache(name);
            if (fd > -1) {
                final String path = toPathString(name);
                if (Files.isDirectory(Paths.get(path))) {
                    synchronized (this) {
                        dirFds.put(fd, path);
                    }
                }
            }
            return fd;
        }

        @Override
        public long openRWNoCache(LPSZ name, int opts) {
            final long fd = super.openRWNoCache(name, opts);
            if (fd > -1) {
                final String path = toPathString(name);
                if (path.endsWith("/_meta.swp") || path.endsWith("/_todo_")) {
                    synchronized (this) {
                        fileFds.put(fd, path);
                    }
                }
            }
            return fd;
        }

        @Override
        public int rename(LPSZ from, LPSZ to) {
            final String path = toPathString(to);
            synchronized (this) {
                renameCounts.merge(path, 1, Integer::sum);
            }
            if (path.equals(failRenameTo)) {
                failRenameTo = null;
                return -1;
            }
            return super.rename(from, to);
        }

        private static String toPathString(LPSZ name) {
            final StringBuilder sb = new StringBuilder(name.size());
            for (int i = 0, n = name.size(); i < n; i++) {
                sb.append((char) (name.byteAt(i) & 0xFF));
            }
            int len = sb.length();
            while (len > 1 && sb.charAt(len - 1) == '/') {
                len--;
            }
            return Paths.get(sb.substring(0, len)).toString();
        }

        synchronized void clear() {
            counts.clear();
            renameCounts.clear();
            emptyCount = 0;
            otherCount = 0;
        }

        synchronized int count(String dir) {
            return counts.getOrDefault(dir, 0);
        }

        /**
         * fsyncs of the directories directly inside {@code dir} whose name starts with a digit: partitions,
         * as opposed to {@code wal<N>}, {@code txn_seq} and the like.
         */
        synchronized int countChildren(String dir) {
            int total = 0;
            for (Map.Entry<String, Integer> e : counts.entrySet()) {
                final String path = e.getKey();
                if (path.startsWith(dir + "/")) {
                    final String name = path.substring(dir.length() + 1);
                    if (name.indexOf('/') < 0 && Character.isDigit(name.charAt(0))) {
                        total += e.getValue();
                    }
                }
            }
            return total;
        }

        synchronized int countEmpty() {
            return emptyCount;
        }

        /**
         * fsyncs of files other than {@code _meta.swp} and {@code _todo_}: column files, parquet data and
         * metadata files.
         */
        synchronized int countOther() {
            return otherCount;
        }

        synchronized int countRenamesTo(String path) {
            return renameCounts.getOrDefault(path, 0);
        }

        synchronized String dump() {
            return " fsyncs=" + counts + ", renames=" + renameCounts;
        }

        synchronized int total() {
            int total = 0;
            for (int c : counts.values()) {
                total += c;
            }
            return total;
        }

        private synchronized void record(long fd, boolean isClosing) {
            final String file = isClosing ? fileFds.remove(fd) : fileFds.get(fd);
            if (file != null) {
                counts.merge(file, 1, Integer::sum);
                return;
            }
            final String dir = isClosing ? dirFds.remove(fd) : dirFds.get(fd);
            if (dir == null) {
                otherCount++;
            } else {
                counts.merge(dir, 1, Integer::sum);
                try (Stream<Path> entries = Files.list(Paths.get(dir))) {
                    if (entries.findAny().isEmpty()) {
                        emptyCount++;
                    }
                } catch (IOException e) {
                    throw new UncheckedIOException(e);
                }
            }
        }
    }
}
