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

package io.questdb.test.cairo.fuzz;

import io.questdb.PropertyKey;
import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.CairoException;
import io.questdb.cairo.ColumnVersionReader;
import io.questdb.cairo.O3PartitionJob;
import io.questdb.cairo.TableReader;
import io.questdb.cairo.TableReaderMetadata;
import io.questdb.cairo.TableToken;
import io.questdb.cairo.TableUtils;
import io.questdb.cairo.TxReader;
import io.questdb.cairo.idx.PostingIndexChainHeader;
import io.questdb.cairo.idx.PostingIndexUtils;
import io.questdb.cairo.vm.Vm;
import io.questdb.cairo.vm.api.MemoryCMARW;
import io.questdb.cairo.vm.api.MemoryCMR;
import io.questdb.griffin.SqlException;
import io.questdb.std.Chars;
import io.questdb.std.Files;
import io.questdb.std.FilesFacade;
import io.questdb.std.FindVisitor;
import io.questdb.std.MemoryTag;
import io.questdb.std.Misc;
import io.questdb.std.Numbers;
import io.questdb.std.ObjList;
import io.questdb.std.Os;
import io.questdb.std.Rnd;
import io.questdb.std.Unsafe;
import io.questdb.std.str.LPSZ;
import io.questdb.std.str.Path;
import io.questdb.std.str.StringSink;
import io.questdb.std.str.Utf8s;
import io.questdb.test.fuzz.FuzzTransaction;
import io.questdb.test.std.TestFilesFacadeImpl;
import io.questdb.test.tools.TestUtils;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.File;
import java.text.Normalizer;
import java.util.Arrays;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

public class CheckpointFuzzTest extends AbstractFuzzTest {
    private static Path triggerFilePath;

    @BeforeClass
    public static void setUpStatic() throws Exception {
        AbstractFuzzTest.setUpStatic();
        triggerFilePath = new Path();
    }

    @AfterClass
    public static void tearDownStatic() {
        triggerFilePath = Misc.free(triggerFilePath);
        AbstractFuzzTest.tearDownStatic();
    }

    @Before
    public void setUp() {
        super.setUp();
        triggerFilePath.of(engine.getConfiguration().getDbRoot()).parent().concat(TableUtils.RESTORE_FROM_CHECKPOINT_TRIGGER_FILE_NAME);
    }

    @Test
    public void testCheckpointCopyKeepsCommittedPostingIndexListedDecomposed() throws Exception {
        // The file system lists the column files of "prix_é" and "größe" decomposed, as HFS+ does, and as APFS does
        // with a copy taken through HFS+. The restore must still keep the committed indexes of prix_é and s, and reset
        // the chain that an ADD INDEX of größe run after the checkpoint left, and the key file u.pk of no column.
        Assume.assumeFalse(Os.isWindows());
        assertMemoryLeak(new DecomposingFilesFacade(), () -> {
            execute("""
                    CREATE TABLE x (
                        s SYMBOL INDEX TYPE POSTING,
                        "prix_é" SYMBOL INDEX TYPE POSTING INCLUDE (v),
                        "größe" SYMBOL,
                        v LONG,
                        ts TIMESTAMP
                    ) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL
                    """);
            execute("""
                    INSERT INTO x VALUES
                    ('p', 'a', 'k', 1, '2024-01-01T00:00'), ('q', 'b', 'm', 2, '2024-01-01T12:00'),
                    ('p', 'a', 'k', 3, '2024-01-02T00:00'), ('q', 'b', 'm', 4, '2024-01-02T12:00')
                    """);
            final TableToken tableToken = engine.verifyTableName("x");
            final long committedSealTxn = readPostingSealTxn(tableToken, "2024-01-01", "prix_é");
            final long committedAsciiSealTxn = readPostingSealTxn(tableToken, "2024-01-01", "s");
            checkpointCreate(false, true);
            final String[] committedFiles = checkpointCopyFiles(tableToken, "2024-01-01", "prix_é.p");
            // Model a copy taken after ADD INDEX built the index of größe, which the checkpoint does not hold.
            execute("ALTER TABLE x ALTER COLUMN \"größe\" ADD INDEX TYPE POSTING");
            engine.releaseAllWriters();
            linkToCheckpointCopy(tableToken, "2024-01-01", "größe.p");
            try (Path path = new Path()) {
                writeFile(path.of(configuration.getDbRoot()).put(TableUtils.CHECKPOINT_META_FILE_NAME)
                        .concat(tableToken).concat("2024-01-01").concat("u.pk"));
            }
            checkpointRecover();

            assertPartitionFiles(tableToken, "2024-01-01", "prix_é.p", Arrays.toString(committedFiles));
            Assert.assertEquals(committedSealTxn, readPostingSealTxn(tableToken, "2024-01-01", "prix_é"));
            Assert.assertEquals(committedAsciiSealTxn, readPostingSealTxn(tableToken, "2024-01-01", "s"));
            assertPartitionFiles(tableToken, "2024-01-01", "größe.p", "[größe.pk]");
            try (Path path = new Path()) {
                path.of(configuration.getDbRoot()).concat(tableToken).concat("2024-01-01").concat("u.pk");
                Assert.assertEquals(-1, emptyPostingChainGenerationCounter(path));
            }
            assertQuery("SELECT v, ts FROM x WHERE \"prix_é\" = 'a'")
                    .noLeakCheck()
                    .noRandomAccess()
                    .expectSize()
                    .timestamp("ts")
                    .withPlanContaining("CoveringIndex on: prix_é")
                    .returns("""
                            v\tts
                            1\t2024-01-01T00:00:00.000000Z
                            3\t2024-01-02T00:00:00.000000Z
                            """);
            assertQuery("SELECT v, ts FROM x WHERE s = 'q'")
                    .noLeakCheck()
                    .timestamp("ts")
                    .withPlanContaining("Index forward scan on: s")
                    .returns("""
                            v\tts
                            2\t2024-01-01T12:00:00.000000Z
                            4\t2024-01-02T12:00:00.000000Z
                            """);
            execute("ALTER TABLE x ALTER COLUMN \"größe\" ADD INDEX TYPE POSTING");
            assertQuery("SELECT v, ts FROM x WHERE \"größe\" = 'm'")
                    .noLeakCheck()
                    .timestamp("ts")
                    .withPlanContaining("Index forward scan on: größe")
                    .returns("""
                            v\tts
                            2\t2024-01-01T12:00:00.000000Z
                            4\t2024-01-02T12:00:00.000000Z
                            """);
        });
    }

    @Test
    public void testCheckpointCopyKeepsCommittedPostingIndexOfNonWalTable() throws Exception {
        assertCheckpointCopyKeepsCommittedPostingIndex("BYPASS WAL");
    }

    @Test
    public void testCheckpointCopyKeepsCommittedPostingIndexOfWalTable() throws Exception {
        assertCheckpointCopyKeepsCommittedPostingIndex("WAL");
    }

    @Test
    public void testCheckpointCopyKeepsPostingIndexOfColumnVersionOfNonWalTable() throws Exception {
        assertCheckpointCopyKeepsPostingIndexOfColumnVersion("BYPASS WAL", true);
    }

    @Test
    public void testCheckpointCopyKeepsPostingIndexOfColumnVersionOfWalTable() throws Exception {
        assertCheckpointCopyKeepsPostingIndexOfColumnVersion("WAL", true);
    }

    @Test
    public void testCheckpointCopyKeepsPostingIndexOfSplitPartitionOfNonWalTable() throws Exception {
        assertCheckpointCopyKeepsPostingIndexOfSplitPartition("BYPASS WAL");
    }

    @Test
    public void testCheckpointCopyKeepsPostingIndexOfSplitPartitionOfWalTable() throws Exception {
        assertCheckpointCopyKeepsPostingIndexOfSplitPartition("WAL");
    }

    @Test
    public void testCheckpointCopyKeepsRebuiltPostingIndexOfColumnVersionOfNonWalTable() throws Exception {
        assertCheckpointCopyKeepsPostingIndexOfColumnVersion("BYPASS WAL", false);
    }

    @Test
    public void testCheckpointCopyKeepsRebuiltPostingIndexOfColumnVersionOfWalTable() throws Exception {
        assertCheckpointCopyKeepsPostingIndexOfColumnVersion("WAL", false);
    }

    @Test
    public void testCheckpointCopyRebuildsIncompleteBitmapIndex() throws Exception {
        Assume.assumeFalse(Os.isWindows());
        setProperty(PropertyKey.CAIRO_DEFAULT_SYMBOL_INDEX_TYPE, "bitmap");
        assertMemoryLeak(() -> {
            execute("create table x (s symbol index, ts timestamp) timestamp(ts) partition by day wal");
            execute("insert into x select 'a', '2024-01-01'::timestamp from long_sequence(100)");
            drainWalQueue();
            final TableToken tableToken = engine.verifyTableName("x");
            checkpointCreate(false, false);

            // Model a value file copied before the corresponding key file's size update.
            final FilesFacade ff = configuration.getFilesFacade();
            try (Path path = new Path()) {
                path.of(configuration.getDbRoot()).put(TableUtils.CHECKPOINT_META_FILE_NAME)
                        .concat(tableToken).concat("2024-01-01").concat("s.v");
                final long fd = ff.openRW(path.$(), configuration.getWriterFileOpenOpts());
                Assert.assertTrue(fd > -1);
                try {
                    Assert.assertTrue(ff.truncate(fd, 0));
                } finally {
                    ff.close(fd);
                }
            }

            checkpointRecover();
            execute("insert into x values ('b', '2024-01-01T01:00')");
            drainWalQueue();
            Assert.assertFalse("table suspended", engine.getTableSequencerAPI().isSuspended(tableToken));
            assertQuery("select count() from x where s = 'a'")
                    .noLeakCheck().noRandomAccess().expectSize().returns("count\n100\n");
            assertQuery("select count() from x where s = 'b'")
                    .noLeakCheck().noRandomAccess().expectSize().returns("count\n1\n");
        });
    }

    @Test
    public void testCheckpointCopyRebuildsIncompletePostingFilesAddedInFlight() throws Exception {
        Assume.assumeFalse(Os.isWindows());
        assertMemoryLeak(() -> {
            execute("CREATE TABLE x (s SYMBOL, v LONG, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("""
                    INSERT INTO x
                    SELECT CASE WHEN x % 2 = 0 THEN 'a'::STRING ELSE NULL END, x, timestamp_sequence('2024-01-01', 21_600_000_000L)
                    FROM long_sequence(20)
                    """);
            drainWalQueue();
            final TableToken tableToken = engine.verifyTableName("x");
            execute("ALTER TABLE x ALTER COLUMN s ADD INDEX TYPE POSTING INCLUDE (v)");
            checkpointCreate(false, true);
            drainWalQueue();
            engine.releaseAllWriters();
            for (int day = 1; day <= 5; day++) {
                linkToCheckpointCopy(tableToken, "2024-01-0" + day, "s.p");
            }
            final long sealTxn = readPostingSealTxn(tableToken, "2024-01-04");
            final FilesFacade ff = configuration.getFilesFacade();
            try (Path path = new Path()) {
                path.of(configuration.getDbRoot()).put(TableUtils.CHECKPOINT_META_FILE_NAME).concat(tableToken);
                final int tableLen = path.size();
                Assert.assertTrue(ff.removeQuiet(PostingIndexUtils.coverInfoFileName(
                        path.concat("2024-01-01"), "s", -1)));
                for (int day = 2; day <= 3; day++) {
                    path.trimTo(tableLen).concat("2024-01-0" + day);
                    if (day == 2) {
                        PostingIndexUtils.coverDataFileName(path, "s", 0, -1, -1, sealTxn);
                    } else {
                        PostingIndexUtils.valueFileName(path, "s", -1, sealTxn);
                    }
                    final long fd = ff.openRW(path.$(), configuration.getWriterFileOpenOpts());
                    Assert.assertTrue(fd >= 0);
                    try {
                        Assert.assertTrue(ff.truncate(fd, 1));
                    } finally {
                        ff.close(fd);
                    }
                }
                // The seal sizes a sealed file before it writes it: a copy that read the value and cover files of
                // 2024-01-04 in between holds them at full length, without the postings and covered values.
                path.trimTo(tableLen).concat("2024-01-04");
                final int partitionLen = path.size();
                zeroFile(PostingIndexUtils.valueFileName(path, "s", -1, sealTxn));
                zeroFile(PostingIndexUtils.coverDataFileName(path.trimTo(partitionLen), "s", 0, -1, -1, sealTxn));
            }
            checkpointRecover();
            // The restored metadata does not index s: the restore resets every chain the copy holds of it.
            for (int day = 1; day <= 5; day++) {
                assertPartitionFiles(tableToken, "2024-01-0" + day, "s.p", "[s.pk]");
            }
            drainWalQueue();
            Assert.assertFalse("table suspended", engine.getTableSequencerAPI().isSuspended(tableToken));
            for (int day = 1; day <= 4; day++) {
                Assert.assertTrue("rebuild partition " + day, readPostingSealTxn(tableToken, "2024-01-0" + day) > sealTxn);
            }
            assertQuery("SELECT v FROM x WHERE s = 'a'")
                    .noLeakCheck().noRandomAccess().expectSize().withPlanContaining("CoveringIndex")
                    .returns("""
                            v
                            2
                            4
                            6
                            8
                            10
                            12
                            14
                            16
                            18
                            20
                            """);
            assertQuery("SELECT v FROM x WHERE s IS NULL")
                    .noLeakCheck().noRandomAccess().expectSize()
                    .returns("""
                            v
                            1
                            3
                            5
                            7
                            9
                            11
                            13
                            15
                            17
                            19
                            """);
        });
    }

    @Test
    public void testCheckpointCopyRebuildsPostingIndexAddedInFlight() throws Exception {
        Assume.assumeFalse(Os.isWindows());
        assertMemoryLeak(() -> {
            execute("CREATE TABLE x (s SYMBOL, v LONG, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
            // 3 partitions, 'a' on every even row.
            execute("""
                    INSERT INTO x
                    SELECT CASE WHEN x % 2 = 0 THEN 'a' ELSE 'b' END, x, timestamp_sequence('2024-01-01', 21_600_000_000L)
                    FROM long_sequence(12)
                    """);
            drainWalQueue();
            final TableToken tableToken = engine.verifyTableName("x");
            // The checkpoint holds the table as it was before the ADD INDEX, which the restore replays.
            execute("ALTER TABLE x ALTER COLUMN s ADD INDEX TYPE POSTING INCLUDE (v)");
            checkpointCreate(false, true);
            drainWalQueue();

            // Model a copy taken while the ADD INDEX ran. It linked every index file of 2024-01-02, which the
            // ADD INDEX had finished, but only the .pk of 2024-01-01 and 2024-01-03: the seal published the
            // .pk in place after the copy linked it, and created the .pv after the copy listed the partition.
            linkToCheckpointCopy(tableToken, "2024-01-01", "s.pk");
            linkToCheckpointCopy(tableToken, "2024-01-02", "s.p");
            linkToCheckpointCopy(tableToken, "2024-01-03", "s.pk");
            final long completeSealTxn = readPostingSealTxn(tableToken, "2024-01-02");

            checkpointRecover();
            // The restored metadata does not index s: the restore resets every chain the copy holds of it, the
            // complete one included, and the replayed ADD INDEX builds them all again.
            for (int day = 1; day <= 3; day++) {
                assertPartitionFiles(tableToken, "2024-01-0" + day, "s.p", "[s.pk]");
            }
            drainWalQueue();
            Assert.assertFalse("table suspended", engine.getTableSequencerAPI().isSuspended(tableToken));
            for (int day = 1; day <= 3; day++) {
                Assert.assertTrue("rebuild partition " + day, readPostingSealTxn(tableToken, "2024-01-0" + day) > completeSealTxn);
            }
            assertQuery("SELECT v, ts FROM x WHERE s = 'a'")
                    .noLeakCheck()
                    .noRandomAccess()
                    .expectSize()
                    .timestamp("ts")
                    .withPlanContaining("CoveringIndex")
                    .returns("""
                            v\tts
                            2\t2024-01-01T06:00:00.000000Z
                            4\t2024-01-01T18:00:00.000000Z
                            6\t2024-01-02T06:00:00.000000Z
                            8\t2024-01-02T18:00:00.000000Z
                            10\t2024-01-03T06:00:00.000000Z
                            12\t2024-01-03T18:00:00.000000Z
                            """);
        });
    }

    @Test
    public void testCheckpointCopyRebuildsPostingIndexAfterCommit() throws Exception {
        Assume.assumeFalse(Os.isWindows());
        assertMemoryLeak(() -> {
            execute("CREATE TABLE x (s SYMBOL, v LONG, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL");
            execute("""
                    INSERT INTO x
                    SELECT CASE WHEN x % 2 = 0 THEN 'a' ELSE 'b' END, x, timestamp_sequence('2024-01-01', 21_600_000_000L)
                    FROM long_sequence(12)
                    """);
            final TableToken tableToken = engine.verifyTableName("x");
            checkpointCreate(false, true);
            // Model a copy taken while the ADD INDEX ran on this non-WAL table: it holds the sealed .pk of
            // 2024-01-01, but not the .pv that the .pk names.
            execute("ALTER TABLE x ALTER COLUMN s ADD INDEX TYPE POSTING INCLUDE (v)");
            engine.releaseAllWriters();
            linkToCheckpointCopy(tableToken, "2024-01-01", "s.pk");
            checkpointRecover();

            // The restored table is not indexed. The commit moves it to the txn the leftover entry carries, so the
            // entry no longer looks abandoned when the ADD INDEX runs again.
            execute("INSERT INTO x VALUES ('a', 13, '2024-01-03T23:00')");
            execute("ALTER TABLE x ALTER COLUMN s ADD INDEX TYPE POSTING INCLUDE (v)");
            assertQuery("SELECT v, ts FROM x WHERE s = 'a'")
                    .noLeakCheck()
                    .noRandomAccess()
                    .expectSize()
                    .timestamp("ts")
                    .withPlanContaining("CoveringIndex")
                    .returns("""
                            v\tts
                            2\t2024-01-01T06:00:00.000000Z
                            4\t2024-01-01T18:00:00.000000Z
                            6\t2024-01-02T06:00:00.000000Z
                            8\t2024-01-02T18:00:00.000000Z
                            10\t2024-01-03T06:00:00.000000Z
                            12\t2024-01-03T18:00:00.000000Z
                            13\t2024-01-03T23:00:00.000000Z
                            """);
        });
    }

    @Test
    public void testCheckpointCopyRebuildsPostingIndexAfterDivergentCommit() throws Exception {
        assertCheckpointCopyRebuildsPostingIndexAfterDivergentCommit("BYPASS WAL", false);
    }

    @Test
    public void testCheckpointCopyRebuildsPostingIndexOfAddedColumnOfNonWalTable() throws Exception {
        assertCheckpointCopyRebuildsPostingIndexOfAddedColumn("BYPASS WAL");
    }

    @Test
    public void testCheckpointCopyRebuildsPostingIndexOfAddedColumnOfWalTable() throws Exception {
        assertCheckpointCopyRebuildsPostingIndexOfAddedColumn("WAL");
    }

    @Test
    public void testCheckpointCopyRebuildsPostingIndexOfUpdatedColumnOfNonWalTable() throws Exception {
        assertCheckpointCopyRebuildsPostingIndexOfUpdatedColumn("BYPASS WAL");
    }

    @Test
    public void testCheckpointCopyRebuildsPostingIndexOfUpdatedColumnOfWalTable() throws Exception {
        assertCheckpointCopyRebuildsPostingIndexOfUpdatedColumn("WAL");
    }

    @Test
    public void testCheckpointCopyRebuildsPostingIndexOfVersionedPartitionAfterDivergentCommit() throws Exception {
        assertCheckpointCopyRebuildsPostingIndexAfterDivergentCommit("BYPASS WAL", true);
    }

    @Test
    public void testCheckpointCopyRebuildsPostingIndexOfVersionedPartitionOfWalTableAfterDivergentCommit() throws Exception {
        assertCheckpointCopyRebuildsPostingIndexAfterDivergentCommit("WAL", true);
    }

    @Test
    public void testCheckpointCopyRebuildsPostingIndexOfWalTableAfterDivergentCommit() throws Exception {
        assertCheckpointCopyRebuildsPostingIndexAfterDivergentCommit("WAL", false);
    }

    @Test
    public void testCheckpointCopyRebuildsPostingIndexOnCompositePartition() throws Exception {
        Assume.assumeFalse(Os.isWindows());
        setProperty(PropertyKey.CAIRO_O3_PARTITION_MERGE_APPEND_ENABLED, "true");
        setProperty(PropertyKey.CAIRO_O3_PARTITION_SPLIT_MIN_SIZE, 1);
        setProperty(PropertyKey.CAIRO_PARTITION_COMPACTION_AVG_ROWS_PIECE_LIM, 1);
        setProperty(PropertyKey.CAIRO_PARTITION_COMPACTION_DEAD_MIN_SIZE, "1G");
        assertMemoryLeak(() -> {
            execute("CREATE TABLE x (s SYMBOL, v LONG, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL DEDUP UPSERT KEYS(ts)");
            execute("""
                    INSERT INTO x
                    SELECT CASE WHEN x % 2 = 0 THEN 'a' ELSE 'b' END, x, timestamp_sequence('2024-01-01', 21_600_000_000L)
                    FROM long_sequence(12)
                    """);
            drainWalQueue();
            execute("INSERT INTO x VALUES ('a', 99, '2024-01-02T06:00')");
            drainWalQueue();
            try (TableReader reader = getReader("x")) {
                Assert.assertTrue(reader.getTxFile().isPartitionComposite(1));
                Assert.assertTrue("index covers physical rows, including dead rows",
                        reader.getPartitionPhysicalRowCount(1) > reader.getPartitionRowCountFromMetadata(1));
            }
            final TableToken tableToken = engine.verifyTableName("x");
            execute("ALTER TABLE x ALTER COLUMN s ADD INDEX TYPE POSTING INCLUDE (v)");
            checkpointCreate(false, true);
            drainWalQueue();
            linkToCheckpointCopy(tableToken, "2024-01-02", "s.p");
            final long sealTxn = readPostingSealTxn(tableToken, "2024-01-02");
            checkpointRecover();
            assertPartitionFiles(tableToken, "2024-01-02", "s.p", "[s.pk]");
            drainWalQueue();
            Assert.assertFalse("table suspended", engine.getTableSequencerAPI().isSuspended(tableToken));
            Assert.assertTrue("rebuild the index over the full physical extent",
                    readPostingSealTxn(tableToken, "2024-01-02") > sealTxn);
            assertQuery("SELECT v, ts FROM x WHERE s = 'a'")
                    .noLeakCheck().noRandomAccess().expectSize().timestamp("ts").withPlanContaining("CoveringIndex")
                    .returns("""
                            v\tts
                            2\t2024-01-01T06:00:00.000000Z
                            4\t2024-01-01T18:00:00.000000Z
                            99\t2024-01-02T06:00:00.000000Z
                            8\t2024-01-02T18:00:00.000000Z
                            10\t2024-01-03T06:00:00.000000Z
                            12\t2024-01-03T18:00:00.000000Z
                            """);
        });
    }

    @Test
    public void testCheckpointCopyResetsOnlyPostingKeyFiles() throws Exception {
        Assume.assumeFalse(Os.isWindows());
        // The listings of the restored 2024-01-01 report its files as a file system without entry types does, and an
        // entry "\xff.pk" as well: a name a file system that takes any bytes can hold, and no column can have, as it
        // is not UTF-8. Taken for a key file, it would reset ".pk".
        final AtomicLong invalidUtf8Name = new AtomicLong();
        final FilesFacade ff = new TestFilesFacadeImpl() {
            @Override
            public void iterateDir(LPSZ path, FindVisitor func) {
                final long name = invalidUtf8Name.get();
                if (name != 0 && Utf8s.endsWithAscii(path, "2024-01-01")) {
                    super.iterateDir(path, (pUtf8NameZ, type) -> func.onFind(pUtf8NameZ, type == Files.DT_FILE ? Files.DT_UNKNOWN : type));
                    func.onFind(name, Files.DT_UNKNOWN);
                } else {
                    super.iterateDir(path, func);
                }
            }
        };
        assertMemoryLeak(ff, () -> {
            execute("CREATE TABLE x (s SYMBOL, v LONG, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL");
            execute("INSERT INTO x VALUES ('a', 1, '2024-01-01'), ('b', 2, '2024-01-01T01:00')");
            execute("ALTER TABLE x ALTER COLUMN s ADD INDEX TYPE POSTING");
            final TableToken tableToken = engine.verifyTableName("x");
            final long committedSealTxn = readPostingSealTxn(tableToken, "2024-01-01");
            checkpointCreate(false, true);
            // Files in the copy of 2024-01-01 that the restore must tell apart: posting key files of columns the
            // restored metadata does not hold, one under a multi-byte name with a value file of its chain, and
            // files and a directory whose names only resemble a key file's. Taken for a key file of s at a column
            // name txn of -1 or below, one of them would reset the committed index of s.
            final String[] keyFiles = {"u.pk", "u.pk.7", "ц.pk.3"};
            final String[] otherFiles = {".pk", "s.p", "s.pkx", "s.pk.", "s.pk.x1", "s.pk.-2", "s.pv", "s.xk"};
            try (Path path = new Path()) {
                path.of(configuration.getDbRoot()).put(TableUtils.CHECKPOINT_META_FILE_NAME).concat(tableToken).concat("2024-01-01");
                final int partitionLen = path.size();
                for (String name : keyFiles) {
                    writeFile(path.trimTo(partitionLen).concat(name));
                }
                for (String name : otherFiles) {
                    writeFile(path.trimTo(partitionLen).concat(name));
                }
                writeFile(path.trimTo(partitionLen).concat("ц.pv.3.5"));
                Assert.assertEquals(0, ff.mkdir(path.trimTo(partitionLen).concat("d.pk").$(), configuration.getMkDirMode()));
                // A symbolic link named like a key file, whose target is gone: there is no chain to reset.
                try (Path target = new Path()) {
                    target.of(path.trimTo(partitionLen)).concat("missing");
                    Assert.assertEquals(0, ff.softLink(target.$(), path.trimTo(partitionLen).concat("u4.pk").$()));
                }
            }
            final long name = Unsafe.malloc(5, MemoryTag.NATIVE_DEFAULT);
            try {
                Unsafe.putByte(name, (byte) 0xff);
                Unsafe.putByte(name + 1, (byte) '.');
                Unsafe.putByte(name + 2, (byte) 'p');
                Unsafe.putByte(name + 3, (byte) 'k');
                Unsafe.putByte(name + 4, (byte) 0);
                invalidUtf8Name.set(name);
                checkpointRecover();
            } finally {
                invalidUtf8Name.set(0);
                Unsafe.free(name, 5, MemoryTag.NATIVE_DEFAULT);
            }

            try (Path path = new Path()) {
                path.of(configuration.getDbRoot()).concat(tableToken).concat("2024-01-01");
                final int partitionLen = path.size();
                // The key files hold empty chains, past every sealTxn of theirs: the value file of the multi-byte
                // name counted, and went with its chain.
                Assert.assertEquals(-1, emptyPostingChainGenerationCounter(path.trimTo(partitionLen).concat("u.pk")));
                Assert.assertEquals(-1, emptyPostingChainGenerationCounter(path.trimTo(partitionLen).concat("u.pk.7")));
                Assert.assertEquals(5, emptyPostingChainGenerationCounter(path.trimTo(partitionLen).concat("ц.pk.3")));
                Assert.assertFalse(ff.exists(path.trimTo(partitionLen).concat("ц.pv.3.5").$()));
                for (String otherFile : otherFiles) {
                    Assert.assertEquals(otherFile, Long.BYTES, ff.length(path.trimTo(partitionLen).concat(otherFile).$()));
                }
                Assert.assertTrue(ff.isDirOrSoftLinkDir(path.trimTo(partitionLen).concat("d.pk").$()));
                Assert.assertTrue(ff.isSoftLink(path.trimTo(partitionLen).concat("u4.pk").$()));
                Assert.assertFalse(ff.exists(path.trimTo(partitionLen).concat("u4.pk").$()));
            }
            Assert.assertEquals(committedSealTxn, readPostingSealTxn(tableToken, "2024-01-01"));
            assertQuery("SELECT v FROM x WHERE s = 'b'")
                    .noLeakCheck()
                    .withPlanContaining("Index forward scan on: s")
                    .returns("""
                            v
                            2
                            """);
        });
    }

    @Test
    public void testCheckpointCopyResetsPostingFilesOfNonPartitionedTable() throws Exception {
        Assume.assumeFalse(Os.isWindows());
        assertMemoryLeak(() -> {
            execute("CREATE TABLE x (s SYMBOL, v LONG)");
            execute("INSERT INTO x VALUES ('a', 1), ('b', 2), ('a', 3)");
            final TableToken tableToken = engine.verifyTableName("x");
            checkpointCreate(false, true);
            // Model a copy taken after ADD INDEX built the index of this non-WAL table.
            execute("ALTER TABLE x ALTER COLUMN s ADD INDEX TYPE POSTING INCLUDE (v)");
            engine.releaseAllWriters();
            linkToCheckpointCopy(tableToken, "default", "s.p");
            checkpointRecover();

            // An empty chain, and none of the value and cover files the build wrote.
            assertPartitionFiles(tableToken, "default", "s.p", "[s.pk]");
            execute("ALTER TABLE x ALTER COLUMN s ADD INDEX TYPE POSTING INCLUDE (v)");
            assertQuery("SELECT v FROM x WHERE s = 'a'")
                    .noLeakCheck()
                    .noRandomAccess()
                    .expectSize()
                    .withPlanContaining("CoveringIndex on: s")
                    .returns("""
                            v
                            1
                            3
                            """);
        });
    }

    @Test
    public void testCheckpointCopyRestoreFailsWhenPostingResetFails() throws Exception {
        Assume.assumeFalse(Os.isWindows());
        // The restore cannot remove the key file of the chain it resets in 2024-01-02. It fails rather than leave a
        // chain that a later ADD INDEX could take for finished, and keeps the checkpoint for the next start.
        final AtomicBoolean isFailing = new AtomicBoolean();
        final FilesFacade ff = new TestFilesFacadeImpl() {
            @Override
            public boolean removeQuiet(LPSZ name) {
                if (isFailing.get() && Utf8s.endsWithAscii(name, "2024-01-02" + Files.SEPARATOR + "s.pk")) {
                    return false;
                }
                return super.removeQuiet(name);
            }
        };
        assertMemoryLeak(ff, () -> {
            execute("CREATE TABLE x (s SYMBOL, v LONG, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL");
            execute("""
                    INSERT INTO x VALUES
                    ('a', 1, '2024-01-01'), ('b', 2, '2024-01-01T12:00'),
                    ('a', 3, '2024-01-02'), ('b', 4, '2024-01-02T12:00'),
                    ('a', 5, '2024-01-03'), ('b', 6, '2024-01-03T12:00')
                    """);
            final TableToken tableToken = engine.verifyTableName("x");
            checkpointCreate(false, true);
            // Model a copy taken after ADD INDEX built the index of this non-WAL table.
            execute("ALTER TABLE x ALTER COLUMN s ADD INDEX TYPE POSTING INCLUDE (v)");
            engine.releaseAllWriters();
            for (int day = 1; day <= 3; day++) {
                linkToCheckpointCopy(tableToken, "2024-01-0" + day, "s.p");
            }
            isFailing.set(true);
            try {
                checkpointRecover();
                Assert.fail("the restore must fail");
            } catch (CairoException e) {
                TestUtils.assertContains(e.getFlyweightMessage(), "could not remove posting index key file");
                TestUtils.assertContains(e.getFlyweightMessage(), "2024-01-02");
            } finally {
                isFailing.set(false);
            }

            // The trigger file and the checkpoint stay: the next start restores it again, and completes.
            Assert.assertTrue(ff.exists(triggerFilePath.$()));
            engine.checkpointRecover();
            engine.getTableSequencerAPI().releaseAll();
            engine.reloadTableNames();
            for (int day = 1; day <= 3; day++) {
                assertPartitionFiles(tableToken, "2024-01-0" + day, "s.p", "[s.pk]");
            }
            execute("ALTER TABLE x ALTER COLUMN s ADD INDEX TYPE POSTING INCLUDE (v)");
            assertQuery("SELECT v, ts FROM x WHERE s = 'a'")
                    .noLeakCheck()
                    .noRandomAccess()
                    .expectSize()
                    .timestamp("ts")
                    .withPlanContaining("CoveringIndex on: s")
                    .returns("""
                            v\tts
                            1\t2024-01-01T00:00:00.000000Z
                            3\t2024-01-02T00:00:00.000000Z
                            5\t2024-01-03T00:00:00.000000Z
                            """);
        });
    }

    @Test
    public void testCheckpointEjectedWalApply() throws Exception {
        Rnd rnd = generateRandom(LOG);
        runEjectedWalApplyBody(rnd);
    }

    @Test
    public void testCheckpointFrequentTableDrop() throws Exception {
        Rnd rnd = generateRandom(LOG);
        fuzzer.setFuzzProbabilities(
                0,
                0,
                0,
                0,
                0,
                0,
                0,
                0,
                0.1,
                0.5,
                0.0,
                0,
                1,
                0.0,
                0.01,
                0
        );

        fuzzer.setFuzzCounts(
                rnd.nextBoolean(),
                rnd.nextInt(2_000_000),
                rnd.nextInt(1000),
                rnd.nextInt(3),
                rnd.nextInt(5),
                rnd.nextInt(1000),
                rnd.nextInt(1_000_000),
                5 + rnd.nextInt(10)
        );

        setFuzzProperties(1, getRndO3PartitionSplit(rnd), getRndO3PartitionSplitMaxCount(rnd), 10 * Numbers.SIZE_1MB, 3, getRndWalApplyBlockMaxRows(rnd));
        runFuzzWithCheckpoint(rnd);
    }

    @Test
    public void testCheckpointFullFuzz() throws Exception {
        Rnd rnd = generateRandom(LOG);
        fullFuzz(rnd);
        setFuzzProperties(rnd.nextLong(50), getRndO3PartitionSplit(rnd), getRndO3PartitionSplitMaxCount(rnd), 10 * Numbers.SIZE_1MB, 3, getRndWalApplyBlockMaxRows(rnd));
        runFuzzWithCheckpoint(rnd);
    }

    @Test
    public void testIndexesMatchScansDetectsLostEmptyStringKey() throws Exception {
        // The empty string is a key of its own, which prints the way NULL does: the check must hold that key against a
        // scan as well.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE x (s SYMBOL INDEX TYPE POSTING, v LONG, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL");
            execute("""
                    INSERT INTO x VALUES
                    ('a', 1, '2024-01-01'), (NULL, 2, '2024-01-01T12:00'),
                    ('', 3, '2024-01-02'), ('', 4, '2024-01-02T12:00')
                    """);
            engine.releaseAllWriters();
            fuzzer.assertIndexesMatchScans("x");
            // Readers map the index files, and Windows cannot remove a mapped file
            engine.releaseAllReaders();

            // 2024-01-02 holds only the empty string: the index loses that key and no other
            final TableToken tableToken = engine.verifyTableName("x");
            try (Path path = new Path(); MemoryCMARW keyMem = Vm.getCMARWInstance()) {
                path.of(configuration.getDbRoot()).concat(tableToken).concat("2024-01-02");
                Assert.assertTrue(PostingIndexUtils.resetIndex(configuration.getFilesFacade(), path, path.size(), "s", -1, keyMem));
            }
            final AssertionError e = Assert.assertThrows(AssertionError.class, () -> fuzzer.assertIndexesMatchScans("x"));
            TestUtils.assertContains(e.getMessage(), "Actual cursor does not have record at 0");
        });
    }

    @Test
    public void testIndexesMatchScansDetectsLostPostingKeys() throws Exception {
        // The check the fuzz runs after a restore must see the keys a posting index lost, so it cannot take them from
        // that index.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE x (s SYMBOL INDEX TYPE POSTING, v LONG, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL");
            execute("INSERT INTO x VALUES ('a', 1, '2024-01-01'), ('b', 2, '2024-01-01T12:00'), ('a', 3, '2024-01-02')");
            engine.releaseAllWriters();
            fuzzer.assertIndexesMatchScans("x");
            // Readers map the index files, and Windows cannot remove a mapped file
            engine.releaseAllReaders();

            final TableToken tableToken = engine.verifyTableName("x");
            try (Path path = new Path(); MemoryCMARW keyMem = Vm.getCMARWInstance()) {
                for (String partitionName : new String[]{"2024-01-01", "2024-01-02"}) {
                    path.of(configuration.getDbRoot()).concat(tableToken).concat(partitionName);
                    Assert.assertTrue(PostingIndexUtils.resetIndex(configuration.getFilesFacade(), path, path.size(), "s", -1, keyMem));
                }
            }
            final AssertionError e = Assert.assertThrows(AssertionError.class, () -> fuzzer.assertIndexesMatchScans("x"));
            TestUtils.assertContains(e.getMessage(), "Actual cursor does not have record at 0");
        });
    }

    private static void assertNotSuspended(TableToken tableToken) {
        Assert.assertFalse("table suspended", tableToken.isWal() && engine.getTableSequencerAPI().isSuspended(tableToken));
    }

    // Every partition of table x in which the column holds rows keeps a sealed chain in the key file of the column
    // version _cv gives the partition.
    private static void assertPostingIndexChains(String columnName) {
        try (TableReader reader = getReader("x"); Path path = new Path()) {
            final TableReaderMetadata metadata = reader.getMetadata();
            final int writerIndex = metadata.getWriterIndex(metadata.getColumnIndex(columnName));
            final ColumnVersionReader columnVersionReader = reader.getColumnVersionReader();
            final TxReader txFile = reader.getTxFile();
            for (int i = 0, n = txFile.getPartitionCount(); i < n; i++) {
                final long partitionTimestamp = txFile.getPartitionTimestampByIndex(i);
                final long columnTop = columnVersionReader.getColumnTop(partitionTimestamp, writerIndex);
                if (columnTop < 0 || columnTop >= txFile.getPartitionSize(i)) {
                    continue;
                }
                path.of(configuration.getDbRoot()).concat(reader.getTableToken());
                TableUtils.setPathForNativePartition(path, metadata.getTimestampType(), metadata.getPartitionBy(), partitionTimestamp, txFile.getPartitionNameTxn(i));
                PostingIndexUtils.keyFileName(path, columnName, columnVersionReader.getColumnNameTxn(partitionTimestamp, writerIndex));
                Assert.assertTrue(path.toString(), PostingIndexUtils.readSealTxnFromKeyFile(configuration.getFilesFacade(), path.$()) > -1);
            }
        }
    }

    private static String[] checkpointCopyFiles(TableToken tableToken, String partitionName, String fileNamePrefix) {
        final File copyRoot = new File(configuration.getDbRoot() + TableUtils.CHECKPOINT_META_FILE_NAME);
        return listFiles(new File(new File(copyRoot, tableToken.getDirName()), partitionName), fileNamePrefix);
    }

    // The column name txn _cv gives the column of table x, then each partition.
    private static long[] columnNameTxns(String columnName) {
        try (TableReader reader = getReader("x")) {
            final int writerIndex = reader.getMetadata().getWriterIndex(reader.getMetadata().getColumnIndex(columnName));
            final ColumnVersionReader columnVersionReader = reader.getColumnVersionReader();
            final long[] columnNameTxns = new long[reader.getPartitionCount() + 1];
            columnNameTxns[0] = columnVersionReader.getDefaultColumnNameTxn(writerIndex);
            for (int i = 0, n = reader.getPartitionCount(); i < n; i++) {
                columnNameTxns[i + 1] = columnVersionReader.getColumnNameTxn(reader.getPartitionTimestampByIndex(i), writerIndex);
            }
            return columnNameTxns;
        }
    }

    private static void createTriggerFile() {
        Files.touch(triggerFilePath.$());
    }

    // Returns the generation counter of the empty chain the key file holds: the first entry of the chain takes the
    // next sealTxn.
    private static long emptyPostingChainGenerationCounter(Path keyFile) {
        final FilesFacade ff = configuration.getFilesFacade();
        final long length = ff.length(keyFile.$());
        Assert.assertTrue(keyFile.toString(), length >= PostingIndexUtils.KEY_FILE_RESERVED);
        try (MemoryCMR mem = Vm.getCMRInstance()) {
            mem.of(ff, keyFile.$(), ff.getMapPageSize(), length, MemoryTag.MMAP_DEFAULT);
            final PostingIndexChainHeader.Snapshot header = new PostingIndexChainHeader.Snapshot();
            Assert.assertTrue(keyFile.toString(), PostingIndexChainHeader.readUnderSeqlock(mem, header));
            Assert.assertEquals(keyFile.toString(), PostingIndexUtils.V2_NO_HEAD, header.headEntryOffset);
            Assert.assertEquals(keyFile.toString(), 0, header.entryCount);
            return header.generationCounter;
        }
    }

    private static String[] listFiles(File dir, String fileNamePrefix) {
        final String[] files = dir.list((_, name) -> name.startsWith(fileNamePrefix));
        Assert.assertNotNull(files);
        Arrays.sort(files);
        return files;
    }

    // The directory name of a partition of table x, with the name txn suffix _txn gives it.
    private static String partitionDirName(int partitionIndex) {
        try (TableReader reader = getReader("x")) {
            final StringSink sink = new StringSink();
            TableUtils.setSinkForNativePartition(
                    sink,
                    reader.getMetadata().getTimestampType(),
                    reader.getPartitionedBy(),
                    reader.getPartitionTimestampByIndex(partitionIndex),
                    reader.getTxFile().getPartitionNameTxn(partitionIndex)
            );
            return sink.toString();
        }
    }

    // Writes a long of -1, which no posting index file starts with, to a file of the checkpoint copy.
    private static void writeFile(Path file) {
        final FilesFacade ff = configuration.getFilesFacade();
        final long fd = ff.openRW(file.$(), configuration.getWriterFileOpenOpts());
        Assert.assertTrue(fd > -1);
        final long buf = Unsafe.malloc(Long.BYTES, MemoryTag.NATIVE_DEFAULT);
        try {
            Unsafe.putLong(buf, -1L);
            Assert.assertEquals(Long.BYTES, ff.write(fd, buf, Long.BYTES, 0));
        } finally {
            Unsafe.free(buf, Long.BYTES, MemoryTag.NATIVE_DEFAULT);
            ff.close(fd);
        }
    }

    // Overwrites a file of the checkpoint copy with zeros, and keeps its length.
    private static void zeroFile(LPSZ file) {
        final FilesFacade ff = configuration.getFilesFacade();
        final long fd = ff.openRW(file, configuration.getWriterFileOpenOpts());
        Assert.assertTrue(fd > -1);
        try {
            final long length = ff.length(fd);
            Assert.assertTrue(length > 0);
            final long buf = Unsafe.calloc(length, MemoryTag.NATIVE_DEFAULT);
            try {
                Assert.assertEquals(length, ff.write(fd, buf, length, 0));
            } finally {
                Unsafe.free(buf, length, MemoryTag.NATIVE_DEFAULT);
            }
        } finally {
            ff.close(fd);
        }
    }

    private void assertCheckpointCopyKeepsCommittedPostingIndex(String walClause) throws Exception {
        Assume.assumeFalse(Os.isWindows());
        assertMemoryLeak(() -> {
            execute("CREATE TABLE x (s SYMBOL, t SYMBOL, b SYMBOL, v LONG, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY " + walClause);
            execute("""
                    INSERT INTO x VALUES
                    ('p', 'q', 'k', 1, '2024-01-01T00:00'), ('r', 'q', 'm', 2, '2024-01-01T12:00'),
                    ('p', 'u', 'k', 3, '2024-01-02T00:00'), ('r', 'u', 'm', 4, '2024-01-02T12:00')
                    """);
            drainWalQueue();
            execute("ALTER TABLE x ALTER COLUMN s ADD INDEX TYPE POSTING INCLUDE (v)");
            drainWalQueue();
            execute("ALTER TABLE x ALTER COLUMN b ADD INDEX TYPE BITMAP");
            drainWalQueue();
            final TableToken tableToken = engine.verifyTableName("x");
            final long committedSealTxn = readPostingSealTxn(tableToken, "2024-01-01");
            checkpointCreate(false, true);
            final String[] committedFiles = checkpointCopyFiles(tableToken, "2024-01-01", "s.p");
            // Model a copy taken after the table moved on: ADD INDEX built t's index, and an UPDATE moved s of
            // 2024-01-01 to a later column version, which it indexed. The checkpoint has neither, and the restore
            // replays neither.
            execute("ALTER TABLE x ALTER COLUMN t ADD INDEX TYPE POSTING");
            drainWalQueue();
            execute("UPDATE x SET s = 'r' WHERE v = 1");
            drainWalQueue();
            final long laterColumnNameTxn;
            try (TableReader reader = getReader("x")) {
                final int writerIndex = reader.getMetadata().getWriterIndex(reader.getMetadata().getColumnIndex("s"));
                laterColumnNameTxn = reader.getColumnVersionReader().getColumnNameTxn(reader.getPartitionTimestampByIndex(0), writerIndex);
            }
            Assert.assertTrue(laterColumnNameTxn > -1);
            engine.releaseAllWriters();
            linkToCheckpointCopy(tableToken, "2024-01-01", "t.p");
            linkToCheckpointCopy(tableToken, "2024-01-01", "s.p");
            // b has a BITMAP index, so no posting chain under its name is one the metadata commits.
            try (Path path = new Path()) {
                writeFile(path.of(configuration.getDbRoot()).put(TableUtils.CHECKPOINT_META_FILE_NAME)
                        .concat(tableToken).concat("2024-01-01").concat("b.pk"));
            }
            checkpointRecover();

            // The restore resets what the copy holds of t's index, of the later version of s and of the chain of b,
            // and leaves the index s had at the checkpoint.
            assertPartitionFiles(tableToken, "2024-01-01", "t.p", "[t.pk]");
            final String laterKeyFile = "s.pk." + laterColumnNameTxn;
            final String[] expectedFiles = Arrays.copyOf(committedFiles, committedFiles.length + 1);
            expectedFiles[committedFiles.length] = laterKeyFile;
            Arrays.sort(expectedFiles);
            assertPartitionFiles(tableToken, "2024-01-01", "s.p", Arrays.toString(expectedFiles));
            Assert.assertEquals(committedSealTxn, readPostingSealTxn(tableToken, "2024-01-01"));
            try (Path path = new Path()) {
                path.of(configuration.getDbRoot()).concat(tableToken).concat("2024-01-01");
                final int partitionLen = path.size();
                Assert.assertTrue(emptyPostingChainGenerationCounter(path.concat(laterKeyFile)) > -1);
                Assert.assertEquals(-1, emptyPostingChainGenerationCounter(path.trimTo(partitionLen).concat("b.pk")));
            }
            drainWalQueue();
            assertNotSuspended(tableToken);
            assertQuery("SELECT v, ts FROM x WHERE s = 'p'")
                    .noLeakCheck()
                    .noRandomAccess()
                    .expectSize()
                    .timestamp("ts")
                    .withPlanContaining("CoveringIndex on: s")
                    .returns("""
                            v\tts
                            1\t2024-01-01T00:00:00.000000Z
                            3\t2024-01-02T00:00:00.000000Z
                            """);
            assertQuery("SELECT v, ts FROM x WHERE b = 'k'")
                    .noLeakCheck()
                    .timestamp("ts")
                    .withPlanContaining("Index forward scan on: b")
                    .returns("""
                            v\tts
                            1\t2024-01-01T00:00:00.000000Z
                            3\t2024-01-02T00:00:00.000000Z
                            """);
            execute("ALTER TABLE x ALTER COLUMN t ADD INDEX TYPE POSTING");
            drainWalQueue();
            assertNotSuspended(tableToken);
            assertQuery("SELECT v, ts FROM x WHERE t = 'q'")
                    .noLeakCheck()
                    .timestamp("ts")
                    .withPlanContaining("Index forward scan on: t")
                    .returns("""
                            v\tts
                            1\t2024-01-01T00:00:00.000000Z
                            2\t2024-01-01T12:00:00.000000Z
                            """);
        });
    }

    // The restore keeps the committed POSTING indexes of column versions that are not a column's first: a BITMAP index
    // moved to POSTING (DROP INDEX gives the column a version of its own in every partition), a column an UPDATE
    // rewrote in one partition, a column added after the table had rows, and a column ALTER COLUMN TYPE converted to an
    // indexed SYMBOL. A full copy rebuilds the indexes first.
    private void assertCheckpointCopyKeepsPostingIndexOfColumnVersion(String walClause, boolean isHardLinkCopy) throws Exception {
        Assume.assumeFalse(Os.isWindows());
        assertMemoryLeak(() -> {
            execute("CREATE TABLE x (junk LONG, a SYMBOL INDEX TYPE BITMAP, b SYMBOL INDEX TYPE POSTING, d VARCHAR, v LONG, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY " + walClause);
            execute("INSERT INTO x VALUES (0, 'p', 'q', 'g', 1, '2024-01-01T00:00'), (0, 'r', 'u', 'h', 2, '2024-01-01T12:00')");
            drainWalQueue();
            execute("ALTER TABLE x ADD COLUMN c SYMBOL INDEX TYPE POSTING");
            drainWalQueue();
            execute("""
                    INSERT INTO x (junk, a, b, d, v, ts, c) VALUES
                    (0, 'p', 'q', 'g', 3, '2024-01-02T00:00', 'k'), (0, 'r', 'u', 'h', 4, '2024-01-02T12:00', 'm'),
                    (0, 'p', 'q', 'g', 5, '2024-01-03T00:00', 'k'), (0, 'r', 'u', 'h', 6, '2024-01-03T12:00', 'm')
                    """);
            drainWalQueue();
            execute("ALTER TABLE x ALTER COLUMN a DROP INDEX");
            drainWalQueue();
            execute("ALTER TABLE x ALTER COLUMN a ADD INDEX TYPE POSTING");
            drainWalQueue();
            execute("UPDATE x SET b = 'u' WHERE v = 1");
            drainWalQueue();
            execute("ALTER TABLE x ALTER COLUMN d TYPE SYMBOL INDEX TYPE POSTING");
            drainWalQueue();
            execute("ALTER TABLE x DROP COLUMN junk");
            drainWalQueue();
            final TableToken tableToken = engine.verifyTableName("x");
            // The column name txn _cv gives the column, then each partition.
            final long[] a = columnNameTxns("a");
            final long[] b = columnNameTxns("b");
            final long[] c = columnNameTxns("c");
            final long[] d = columnNameTxns("d");
            Assert.assertEquals(-1, a[0]);
            for (int i = 1; i < a.length; i++) {
                Assert.assertTrue(Arrays.toString(a), a[i] > -1);
            }
            Assert.assertTrue(Arrays.toString(b), b[0] == -1 && b[1] > -1 && b[2] == -1 && b[3] == -1);
            Assert.assertTrue(Arrays.toString(c), c[0] > -1);
            for (long columnNameTxn : d) {
                Assert.assertTrue(Arrays.toString(d), columnNameTxn > -1);
            }
            // Dropping junk leaves each column a metadata index below its writer index, and the conversion gives d the
            // highest writer index: looked up by metadata index, _cv gives another column's version.
            try (TableReader reader = getReader("x")) {
                final TableReaderMetadata metadata = reader.getMetadata();
                for (String column : new String[]{"a", "b", "c", "d"}) {
                    final int columnIndex = metadata.getColumnIndex(column);
                    Assert.assertNotEquals(column, columnIndex, metadata.getWriterIndex(columnIndex));
                }
            }
            checkpointCreate(false, isHardLinkCopy);
            checkpointRecover();
            drainWalQueue();
            assertNotSuspended(tableToken);

            for (String column : new String[]{"a", "b", "c", "d"}) {
                assertPostingIndexChains(column);
            }
            assertQuery("SELECT v, ts FROM x WHERE a = 'p'")
                    .noLeakCheck()
                    .timestamp("ts")
                    .withPlanContaining("Index forward scan on: a")
                    .returns("""
                            v\tts
                            1\t2024-01-01T00:00:00.000000Z
                            3\t2024-01-02T00:00:00.000000Z
                            5\t2024-01-03T00:00:00.000000Z
                            """);
            assertQuery("SELECT v, ts FROM x WHERE b = 'u'")
                    .noLeakCheck()
                    .timestamp("ts")
                    .withPlanContaining("Index forward scan on: b")
                    .returns("""
                            v\tts
                            1\t2024-01-01T00:00:00.000000Z
                            2\t2024-01-01T12:00:00.000000Z
                            4\t2024-01-02T12:00:00.000000Z
                            6\t2024-01-03T12:00:00.000000Z
                            """);
            assertQuery("SELECT v, ts FROM x WHERE c = 'k'")
                    .noLeakCheck()
                    .timestamp("ts")
                    .withPlanContaining("Index forward scan on: c")
                    .returns("""
                            v\tts
                            3\t2024-01-02T00:00:00.000000Z
                            5\t2024-01-03T00:00:00.000000Z
                            """);
            assertQuery("SELECT v, ts FROM x WHERE d = 'g'")
                    .noLeakCheck()
                    .timestamp("ts")
                    .withPlanContaining("Index forward scan on: d")
                    .returns("""
                            v\tts
                            1\t2024-01-01T00:00:00.000000Z
                            3\t2024-01-02T00:00:00.000000Z
                            5\t2024-01-03T00:00:00.000000Z
                            """);
        });
    }

    // An out-of-order commit into the middle of a partition splits its tail off into a directory of its own, as
    // out-of-order data does on a server, where merge-append is off. The split's partition timestamp is not the
    // partition's floor, and an UPDATE gives the split a column version of its own: the restore keeps the chain of the
    // version _cv names for the split's own timestamp.
    private void assertCheckpointCopyKeepsPostingIndexOfSplitPartition(String walClause) throws Exception {
        Assume.assumeFalse(Os.isWindows());
        setProperty(PropertyKey.CAIRO_O3_PARTITION_MERGE_APPEND_ENABLED, "false");
        setProperty(PropertyKey.CAIRO_O3_PARTITION_SPLIT_MIN_SIZE, 1);
        assertMemoryLeak(() -> {
            execute("CREATE TABLE x (s SYMBOL INDEX TYPE POSTING INCLUDE (v), v LONG, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY " + walClause);
            // 00:00 to 07:00, every hour, 'a' on every even row
            execute("""
                    INSERT INTO x
                    SELECT CASE WHEN x % 2 = 0 THEN 'a' ELSE 'b' END, x, timestamp_sequence('2024-01-01', 3_600_000_000L)
                    FROM long_sequence(8)
                    """);
            drainWalQueue();
            execute("INSERT INTO x VALUES ('a', 9, '2024-01-01T06:30')");
            drainWalQueue();
            execute("UPDATE x SET s = 'c' WHERE v = 9");
            drainWalQueue();
            try (TableReader reader = getReader("x")) {
                final TxReader txFile = reader.getTxFile();
                Assert.assertEquals(2, txFile.getPartitionCount());
                final long splitTimestamp = txFile.getPartitionTimestampByIndex(1);
                Assert.assertEquals(txFile.getPartitionTimestampByIndex(0), txFile.getLogicalPartitionTimestamp(splitTimestamp));
                Assert.assertNotEquals(txFile.getPartitionTimestampByIndex(0), splitTimestamp);
            }
            // The column name txn _cv gives s, then 2024-01-01, then the split.
            final long[] columnNameTxns = columnNameTxns("s");
            Assert.assertTrue(Arrays.toString(columnNameTxns), columnNameTxns[2] > -1 && columnNameTxns[2] != columnNameTxns[1]);
            final TableToken tableToken = engine.verifyTableName("x");
            final String splitPartitionName = partitionDirName(1);
            final long sealTxn = readPostingSealTxn(tableToken, splitPartitionName, "s", columnNameTxns[2]);
            checkpointCreate(false, true);
            checkpointRecover();
            drainWalQueue();
            assertNotSuspended(tableToken);

            Assert.assertEquals(sealTxn, readPostingSealTxn(tableToken, splitPartitionName, "s", columnNameTxns[2]));
            assertQuery("SELECT v, ts FROM x WHERE s = 'a'")
                    .noLeakCheck()
                    .noRandomAccess()
                    .expectSize()
                    .timestamp("ts")
                    .withPlanContaining("CoveringIndex on: s")
                    .returns("""
                            v\tts
                            2\t2024-01-01T01:00:00.000000Z
                            4\t2024-01-01T03:00:00.000000Z
                            6\t2024-01-01T05:00:00.000000Z
                            8\t2024-01-01T07:00:00.000000Z
                            """);
            assertQuery("SELECT v, ts FROM x WHERE s = 'c'")
                    .noLeakCheck()
                    .noRandomAccess()
                    .expectSize()
                    .timestamp("ts")
                    .withPlanContaining("CoveringIndex on: s")
                    .returns("""
                            v\tts
                            9\t2024-01-01T06:30:00.000000Z
                            """);
        });
    }

    // isPartitionVersioned: an out-of-order commit rewrites 2024-01-01 into a directory whose name carries a partition
    // name txn before the checkpoint, as out-of-order data does by default. The test setup turns merge-append, which
    // writes a composite piece instead, on, so the variant turns it off.
    private void assertCheckpointCopyRebuildsPostingIndexAfterDivergentCommit(String walClause, boolean isPartitionVersioned) throws Exception {
        Assume.assumeFalse(Os.isWindows());
        if (isPartitionVersioned) {
            setProperty(PropertyKey.CAIRO_O3_PARTITION_MERGE_APPEND_ENABLED, "false");
        }
        assertMemoryLeak(() -> {
            execute("CREATE TABLE x (s SYMBOL, v LONG, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY " + walClause);
            if (isPartitionVersioned) {
                execute("INSERT INTO x VALUES ('x', 1, '2024-01-01T00:00'), ('x', 3, '2024-01-01T02:00'), ('y', 4, '2024-01-01T03:00')");
                drainWalQueue();
                execute("INSERT INTO x VALUES ('y', 2, '2024-01-01T01:00')");
            } else {
                execute("""
                        INSERT INTO x VALUES
                        ('x', 1, '2024-01-01T00:00'), ('y', 2, '2024-01-01T01:00'),
                        ('x', 3, '2024-01-01T02:00'), ('y', 4, '2024-01-01T03:00')
                        """);
            }
            drainWalQueue();
            final String partitionDir = partitionDirName(0);
            Assert.assertEquals(partitionDir, isPartitionVersioned, !"2024-01-01".equals(partitionDir));
            final TableToken tableToken = engine.verifyTableName("x");
            checkpointCreate(false, true);
            // Model a copy taken after the table moved on: one commit appended to 2024-01-01 and opened 2024-01-02,
            // then ADD INDEX indexed 2024-01-01 with the rows that commit wrote.
            execute("""
                    INSERT INTO x VALUES
                    ('x', 11, '2024-01-01T04:00'), ('y', 12, '2024-01-01T05:00'),
                    ('x', 13, '2024-01-01T06:00'), ('y', 14, '2024-01-01T07:00'),
                    ('x', 15, '2024-01-02')
                    """);
            drainWalQueue();
            Assert.assertEquals(partitionDir, partitionDirName(0));
            execute("ALTER TABLE x ALTER COLUMN s ADD INDEX TYPE POSTING INCLUDE (v)");
            drainWalQueue();
            engine.releaseAllWriters();
            linkToCheckpointCopy(tableToken, partitionDir, "s.p");
            checkpointRecover();
            // The restored metadata does not index s: the restore resets the chain the copy holds of it.
            assertPartitionFiles(tableToken, partitionDir, "s.p", "[s.pk]");
            drainWalQueue();

            // The restore replays nothing that came after the checkpoint. The next commit writes other rows, up to
            // the same count and at the same txn, as the commit the index in the copy was built over.
            execute("""
                    INSERT INTO x VALUES
                    ('y', 21, '2024-01-01T04:00'), ('x', 22, '2024-01-01T05:00'),
                    ('y', 23, '2024-01-01T06:00'), ('x', 24, '2024-01-01T07:00'),
                    ('x', 25, '2024-01-02')
                    """);
            drainWalQueue();
            Assert.assertEquals(partitionDir, partitionDirName(0));
            execute("ALTER TABLE x ALTER COLUMN s ADD INDEX TYPE POSTING INCLUDE (v)");
            drainWalQueue();
            assertNotSuspended(tableToken);
            assertQuery("SELECT v, ts FROM x WHERE s = 'x'")
                    .noLeakCheck()
                    .noRandomAccess()
                    .expectSize()
                    .timestamp("ts")
                    .withPlanContaining("CoveringIndex")
                    .returns("""
                            v\tts
                            1\t2024-01-01T00:00:00.000000Z
                            3\t2024-01-01T02:00:00.000000Z
                            22\t2024-01-01T05:00:00.000000Z
                            24\t2024-01-01T07:00:00.000000Z
                            25\t2024-01-02T00:00:00.000000Z
                            """);
            assertQuery("SELECT v, ts FROM x WHERE s = 'y'")
                    .noLeakCheck()
                    .noRandomAccess()
                    .expectSize()
                    .timestamp("ts")
                    .withPlanContaining("CoveringIndex")
                    .returns("""
                            v\tts
                            2\t2024-01-01T01:00:00.000000Z
                            4\t2024-01-01T03:00:00.000000Z
                            21\t2024-01-01T04:00:00.000000Z
                            23\t2024-01-01T06:00:00.000000Z
                            """);
        });
    }

    private void assertCheckpointCopyRebuildsPostingIndexOfAddedColumn(String walClause) throws Exception {
        Assume.assumeFalse(Os.isWindows());
        assertMemoryLeak(() -> {
            execute("CREATE TABLE x (s SYMBOL, v LONG, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY " + walClause);
            execute("""
                    INSERT INTO x VALUES
                    ('x', 1, '2024-01-01T00:00'), ('y', 2, '2024-01-01T01:00'),
                    ('x', 3, '2024-01-01T02:00'), ('y', 4, '2024-01-01T03:00')
                    """);
            drainWalQueue();
            final TableToken tableToken = engine.verifyTableName("x");
            checkpointCreate(false, true);
            // Model a copy taken after the table added column c, filled it in 2024-01-01 with a commit that also
            // opened 2024-01-02, and indexed it.
            execute("ALTER TABLE x ADD COLUMN c SYMBOL");
            drainWalQueue();
            execute("""
                    INSERT INTO x (s, v, ts, c) VALUES
                    ('x', 11, '2024-01-01T04:00', 'a'), ('y', 12, '2024-01-01T05:00', 'a'),
                    ('x', 13, '2024-01-01T06:00', 'b'), ('y', 14, '2024-01-01T07:00', 'b'),
                    ('x', 15, '2024-01-02', 'a')
                    """);
            drainWalQueue();
            execute("ALTER TABLE x ALTER COLUMN c ADD INDEX TYPE POSTING INCLUDE (v)");
            drainWalQueue();
            engine.releaseAllWriters();
            final String keyFile = postingKeyFile(tableToken, "2024-01-01", "c");
            linkToCheckpointCopy(tableToken, "2024-01-01", "c.p");
            checkpointRecover();
            // The restored metadata has no column c: the restore resets the chain the copy holds of its index.
            assertPartitionFiles(tableToken, "2024-01-01", "c.p", "[" + keyFile + "]");
            drainWalQueue();

            // The same statements with other values add c under the same column name txn, and bring 2024-01-01 to
            // the txn and row count the index in the copy was built for.
            execute("ALTER TABLE x ADD COLUMN c SYMBOL");
            drainWalQueue();
            execute("""
                    INSERT INTO x (s, v, ts, c) VALUES
                    ('y', 21, '2024-01-01T04:00', 'a'), ('x', 22, '2024-01-01T05:00', 'b'),
                    ('y', 23, '2024-01-01T06:00', 'a'), ('x', 24, '2024-01-01T07:00', 'b'),
                    ('x', 25, '2024-01-02', 'a')
                    """);
            drainWalQueue();
            execute("ALTER TABLE x ALTER COLUMN c ADD INDEX TYPE POSTING INCLUDE (v)");
            drainWalQueue();
            assertNotSuspended(tableToken);
            assertQuery("SELECT v, ts FROM x WHERE c = 'a'")
                    .noLeakCheck()
                    .noRandomAccess()
                    .expectSize()
                    .timestamp("ts")
                    .withPlanContaining("CoveringIndex")
                    .returns("""
                            v\tts
                            21\t2024-01-01T04:00:00.000000Z
                            23\t2024-01-01T06:00:00.000000Z
                            25\t2024-01-02T00:00:00.000000Z
                            """);
            assertQuery("SELECT v, ts FROM x WHERE c = 'b'")
                    .noLeakCheck()
                    .noRandomAccess()
                    .expectSize()
                    .timestamp("ts")
                    .withPlanContaining("CoveringIndex")
                    .returns("""
                            v\tts
                            22\t2024-01-01T05:00:00.000000Z
                            24\t2024-01-01T07:00:00.000000Z
                            """);
        });
    }

    private void assertCheckpointCopyRebuildsPostingIndexOfUpdatedColumn(String walClause) throws Exception {
        Assume.assumeFalse(Os.isWindows());
        assertMemoryLeak(() -> {
            execute("CREATE TABLE x (s SYMBOL, v LONG, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY " + walClause);
            execute("""
                    INSERT INTO x VALUES
                    ('x', 1, '2024-01-01T00:00'), ('y', 2, '2024-01-01T01:00'),
                    ('x', 3, '2024-01-01T02:00'), ('y', 4, '2024-01-01T03:00'),
                    ('x', 5, '2024-01-02')
                    """);
            drainWalQueue();
            final TableToken tableToken = engine.verifyTableName("x");
            checkpointCreate(false, true);
            // Model a copy taken after an UPDATE wrote s of 2024-01-01 under a new column version, which ADD INDEX
            // then indexed.
            execute("UPDATE x SET s = 'x' WHERE v = 2");
            drainWalQueue();
            execute("ALTER TABLE x ALTER COLUMN s ADD INDEX TYPE POSTING INCLUDE (v)");
            drainWalQueue();
            engine.releaseAllWriters();
            final String keyFile = postingKeyFile(tableToken, "2024-01-01", "s");
            linkToCheckpointCopy(tableToken, "2024-01-01", "s.p");
            checkpointRecover();
            // The restored _cv names the version of s from before the UPDATE: the restore resets the chain of the
            // later one.
            assertPartitionFiles(tableToken, "2024-01-01", "s.p", "[" + keyFile + "]");
            drainWalQueue();

            // The same UPDATE with another value writes s of 2024-01-01 under that version again, at the txn and row
            // count the index in the copy was built for.
            execute("UPDATE x SET s = 'y' WHERE v = 1");
            drainWalQueue();
            execute("ALTER TABLE x ALTER COLUMN s ADD INDEX TYPE POSTING INCLUDE (v)");
            drainWalQueue();
            assertNotSuspended(tableToken);
            assertQuery("SELECT v, ts FROM x WHERE s = 'x'")
                    .noLeakCheck()
                    .noRandomAccess()
                    .expectSize()
                    .timestamp("ts")
                    .withPlanContaining("CoveringIndex")
                    .returns("""
                            v\tts
                            3\t2024-01-01T02:00:00.000000Z
                            5\t2024-01-02T00:00:00.000000Z
                            """);
            assertQuery("SELECT v, ts FROM x WHERE s = 'y'")
                    .noLeakCheck()
                    .noRandomAccess()
                    .expectSize()
                    .timestamp("ts")
                    .withPlanContaining("CoveringIndex")
                    .returns("""
                            v\tts
                            1\t2024-01-01T00:00:00.000000Z
                            2\t2024-01-01T01:00:00.000000Z
                            4\t2024-01-01T03:00:00.000000Z
                            """);
        });
    }

    private void assertPartitionFiles(TableToken tableToken, String partitionName, String fileNamePrefix, String expected) {
        Assert.assertEquals(expected, Arrays.toString(partitionFiles(tableToken, partitionName, fileNamePrefix)));
    }

    private void checkpointCreate(boolean legacy, boolean hardLinkCopy) throws SqlException {
        // A file-by-file copy can capture an index header after its value file. Rebuild those indexes on restore.
        setProperty(PropertyKey.CAIRO_CHECKPOINT_RECOVERY_REBUILD_COLUMN_INDEXES, Boolean.toString(!hardLinkCopy));
        LOG.info().$("creating checkpoint").$();

        if (legacy) {
            execute("snapshot prepare");
        } else {
            execute("checkpoint create");
        }
        CairoConfiguration conf = engine.getConfiguration();

        FilesFacade ff = conf.getFilesFacade();
        Path snapshotPath = Path.getThreadLocal(conf.getDbRoot()).put(TableUtils.CHECKPOINT_META_FILE_NAME);
        Path rootPath = Path.getThreadLocal2(conf.getDbRoot());

        ff.mkdirs(snapshotPath, conf.getMkDirMode());

        if (hardLinkCopy) {
            LOG.info().$("hard linking data to the checkpoint [from=").$(rootPath).$(", to=").$(snapshotPath).$();
            hardLinkCopyRecursiveIgnoreErrors(ff, rootPath, snapshotPath, conf.getMkDirMode());
        } else {
            LOG.info().$("copying data to the checkpoint [from=").$(rootPath).$(", to=").$(snapshotPath).$();
            copyRecursiveIgnoreErrors(ff, rootPath, snapshotPath, conf.getMkDirMode());
        }

        if (legacy) {
            execute("snapshot complete");
        } else {
            execute("checkpoint release");
        }
    }

    private void checkpointRecover() {
        LOG.info().$("begin checkpoint restore").$();
        engine.releaseInactive();

        CairoConfiguration conf = engine.getConfiguration();
        FilesFacade ff = conf.getFilesFacade();
        Path snapshotPath = Path.getThreadLocal(conf.getDbRoot()).put(TableUtils.CHECKPOINT_META_FILE_NAME).slash();
        Path rootPath = Path.getThreadLocal2(conf.getDbRoot()).slash();

        ff.rmdir(rootPath);
        ff.rename(snapshotPath.$(), rootPath.$());

        LOG.info().$("recovering from the checkpoint").$();
        createTriggerFile();
        // a restore restarts the server, which drops the column purges queued before it
        engine.getMessageBus().getColumnPurgeSubSeq().clear();
        engine.checkpointRecover();
        engine.getTableSequencerAPI().releaseAll();
        engine.reloadTableNames();
    }

    private void copyRecursiveIgnoreErrors(FilesFacade ff, Path src, Path dst, int dirMode) {
        int dstLen = dst.size();
        int srcLen = src.size();
        int len = src.size();
        long p = ff.findFirst(src.$());

        if (!ff.exists(dst.$()) && -1 == ff.mkdir(dst.$(), dirMode)) {
            LOG.info().$("failed to copy, cannot create dst dir ").$(src).$(" to ").$(dst)
                    .$(", errno: ").$(ff.errno()).$();
        }

        if (p > 0) {
            try {
                int res;
                do {
                    long name = ff.findName(p);
                    if (Files.notDots(name)) {
                        int type = ff.findType(p);
                        src.trimTo(len);
                        src.concat(name);
                        dst.concat(name);
                        if (type == Files.DT_FILE) {
                            res = Files.copy(src.$(), dst.$());
                            if (res != 0) {
                                LOG.info().$("failed to copy ").$(src).$(" to ").$(dst)
                                        .$(", errno: ").$(ff.errno()).$();
                            }
                        } else {
                            ff.mkdir(dst.$(), dirMode);
                            copyRecursiveIgnoreErrors(ff, src, dst, dirMode);
                        }
                        src.trimTo(srcLen);
                        dst.trimTo(dstLen);
                    }
                } while (ff.findNext(p) > 0);
            } finally {
                ff.findClose(p);
                src.trimTo(srcLen);
                dst.trimTo(dstLen);
            }
        }
    }

    private void fullFuzz(Rnd rnd) {
        // Parquet partitions and covering indexes: a restore that rebuilds the indexes rebuilds theirs too.
        final boolean withParquet = rnd.nextBoolean();
        fuzzer.setFuzzProbabilities(
                0.5 * rnd.nextDouble(),
                rnd.nextDouble(),
                rnd.nextDouble(),
                0.5 * rnd.nextDouble(),
                rnd.nextDouble(),
                rnd.nextDouble(),
                rnd.nextDouble(),
                rnd.nextDouble(),
                rnd.nextDouble(),
                0.01,
                0.0,
                withParquet ? 0.05 : 0.0,       // partitionToParquetProb
                withParquet ? 0.05 : 0.0,       // partitionToNativeProb
                0.1 * rnd.nextDouble(),
                rnd.nextDouble(),
                0.0,
                withParquet ? 0.0 : 0.01,       // replaceInsertProb, unsupported on parquet partitions
                0,
                0.0,                            // queryProb
                withParquet ? 0.05 : 0.0,       // setParquetEncodingProb
                withParquet ? 0.05 : 0.0,       // setTableFormatProb
                0.1                             // addCoveringIndexProb
        );

        fuzzer.setFuzzCounts(
                rnd.nextBoolean(),
                rnd.nextInt(2_000_000),
                rnd.nextInt(1000),
                rnd.nextInt(3),
                rnd.nextInt(5),
                rnd.nextInt(1000),
                rnd.nextInt(1_000_000),
                5 + rnd.nextInt(10)
        );
    }

    private String getTestTableName() {
        return testName.getMethodName().replace('[', '_').replace(']', '_');
    }

    private void hardLinkCopyRecursiveIgnoreErrors(FilesFacade ff, Path src, Path dst, int dirMode) {
        int dstLen = dst.size();
        int srcLen = src.size();
        int len = src.size();
        long p = ff.findFirst(src.$());

        if (!ff.exists(dst.$()) && -1 == ff.mkdir(dst.$(), dirMode)) {
            LOG.info().$("failed to copy, cannot create dst dir ").$(src).$(" to ").$(dst)
                    .$(", errno: ").$(ff.errno()).$();
        }

        if (p > 0) {
            try {
                int res;
                do {
                    long name = ff.findName(p);
                    if (Files.notDots(name)) {
                        int type = ff.findType(p);
                        src.trimTo(len);
                        src.concat(name);
                        dst.concat(name);
                        if (type == Files.DT_FILE) {
                            res = Files.hardLink(src.$(), dst.$());
                            if (res != 0) {
                                LOG.info().$("failed to copy ").$(src).$(" to ").$(dst)
                                        .$(", errno: ").$(ff.errno()).$();
                            }
                        } else {
                            ff.mkdir(dst.$(), dirMode);
                            hardLinkCopyRecursiveIgnoreErrors(ff, src, dst, dirMode);
                        }
                        src.trimTo(srcLen);
                        dst.trimTo(dstLen);
                    }
                } while (ff.findNext(p) > 0);
            } finally {
                ff.findClose(p);
                src.trimTo(srcLen);
                dst.trimTo(dstLen);
            }
        }
    }

    // Hard-links the files of a partition that start with the prefix, and the checkpoint copy does not hold yet, into
    // the copy, as a copy taken later would hold them.
    private void linkToCheckpointCopy(TableToken tableToken, String partitionName, String fileNamePrefix) {
        final FilesFacade ff = configuration.getFilesFacade();
        int linkCount = 0;
        try (Path src = new Path(); Path dst = new Path()) {
            for (String file : partitionFiles(tableToken, partitionName, fileNamePrefix)) {
                dst.of(configuration.getDbRoot()).put(TableUtils.CHECKPOINT_META_FILE_NAME)
                        .concat(tableToken).concat(partitionName).concat(file);
                if (!ff.exists(dst.$())) {
                    src.of(configuration.getDbRoot()).concat(tableToken).concat(partitionName).concat(file);
                    Assert.assertEquals(0, ff.hardLink(src.$(), dst.$()));
                    linkCount++;
                }
            }
        }
        Assert.assertTrue(linkCount > 0);
    }

    private String[] partitionFiles(TableToken tableToken, String partitionName, String fileNamePrefix) {
        return listFiles(new File(new File(configuration.getDbRoot(), tableToken.getDirName()), partitionName), fileNamePrefix);
    }

    private String postingKeyFile(TableToken tableToken, String partitionName, String columnName) {
        final String[] files = partitionFiles(tableToken, partitionName, columnName + ".pk");
        Assert.assertEquals(Arrays.toString(files), 1, files.length);
        return files[0];
    }

    private long readPostingSealTxn(TableToken tableToken, String partitionName) {
        return readPostingSealTxn(tableToken, partitionName, "s");
    }

    private long readPostingSealTxn(TableToken tableToken, String partitionName, String columnName) {
        return readPostingSealTxn(tableToken, partitionName, columnName, -1);
    }

    private long readPostingSealTxn(TableToken tableToken, String partitionName, String columnName, long columnNameTxn) {
        try (Path path = new Path()) {
            path.of(configuration.getDbRoot()).concat(tableToken).concat(partitionName);
            final long sealTxn = PostingIndexUtils.readSealTxnFromKeyFile(configuration.getFilesFacade(),
                    PostingIndexUtils.keyFileName(path, columnName, columnNameTxn));
            Assert.assertTrue(path.toString(), sealTxn >= 0);
            return sealTxn;
        }
    }

    private void runEjectedWalApplyBody(Rnd rnd) throws Exception {
        fuzzer.setFuzzProbabilities(
                0,
                0,
                0,
                0,
                0,
                0,
                0,
                0,
                1,
                0.5,
                0.0,
                0,
                0,
                0.5,
                0.01,
                0
        );

        fuzzer.setFuzzCounts(
                rnd.nextBoolean(),
                rnd.nextInt(2_000_000),
                rnd.nextInt(1000),
                rnd.nextInt(3),
                rnd.nextInt(5),
                rnd.nextInt(1000),
                rnd.nextInt(1_000_000),
                5 + rnd.nextInt(10)
        );

        setFuzzProperties(
                1,
                getRndO3PartitionSplit(rnd),
                getRndO3PartitionSplitMaxCount(rnd),
                10 * Numbers.SIZE_1MB,
                3,
                getRndWalApplyBlockMaxRows(rnd)
        );
        runFuzzWithCheckpoint(rnd);
    }

    protected void runFuzzWithCheckpoint(Rnd rnd) throws Exception {
        // Snapshot is not supported on Windows.
        Assume.assumeFalse(Os.isWindows());
        boolean testHardLinkCheckpoint = rnd.nextBoolean();

        assertMemoryLeak(() -> {
            final boolean isMergeAppendEnabled = rnd.nextBoolean();
            node1.setProperty(PropertyKey.CAIRO_O3_PARTITION_MERGE_APPEND_ENABLED, isMergeAppendEnabled);
            LOG.info().$("checkpoint merge-append [enabled=").$(isMergeAppendEnabled).I$();
            if (testHardLinkCheckpoint) {
                node1.setProperty(PropertyKey.CAIRO_O3_PARTITION_SPLIT_MIN_SIZE, "100G");
            }
            // A backup restore always rebuilds the indexes; a checkpoint restore does only when asked to.
            final boolean isIndexRebuilt = rnd.nextBoolean();
            LOG.info().$("checkpoint recovery rebuilds column indexes [rebuild=").$(isIndexRebuilt).I$();
            node1.setProperty(PropertyKey.CAIRO_CHECKPOINT_RECOVERY_REBUILD_COLUMN_INDEXES, isIndexRebuilt);

            String tableNameNonWal = getTestTableName() + "_non_wal";
            String tableNameWal = getTestTableName();
            TableToken walTable = fuzzer.createInitialTableWal(tableNameWal, fuzzer.initialRowCount);
            ObjList<FuzzTransaction> transactions = fuzzer.generateTransactions(tableNameWal, rnd);

            fuzzer.createInitialTableNonWal(tableNameNonWal, transactions);
            if (rnd.nextBoolean()) {
                drainWalQueue();
            }

            try {
                // The fuzzer can generate a single transaction; Rnd.nextInt(0) divides by
                // zero, so a one-transaction run takes the checkpoint after its only
                // transaction and leaves the (already optional) after-set empty.
                int snapshotIndex = transactions.size() > 1 ? 1 + rnd.nextInt(transactions.size() - 1) : 1;

                ObjList<FuzzTransaction> beforeSnapshot = new ObjList<>();
                beforeSnapshot.addAll(transactions, 0, snapshotIndex);
                ObjList<FuzzTransaction> afterSnapshot = new ObjList<>();
                afterSnapshot.addAll(transactions, snapshotIndex, transactions.size());

                fuzzer.applyToWal(beforeSnapshot, tableNameWal, rnd.nextInt(2) + 1, rnd);

                AtomicReference<Throwable> ex = new AtomicReference<>();
                Thread asyncWalApply = new Thread(() -> {
                    try {
                        drainWalQueue();
                    } catch (Throwable th) {
                        ex.set(th);
                    } finally {
                        // drainWalQueue() runs O3PartitionJob inline on this thread, outside any
                        // WorkerPool - so the O3PartitionJob.COMPOSITE_CONTEXT/PARQUET_MERGE_CONTEXT
                        // CarrierLocal slots it may have populated (e.g. a composite-partition merge's
                        // _geometry scratch buffer) never see WorkerPool's worker-halt cleanup. This
                        // thread is single-use, so free them here, the same way a WorkerPool worker
                        // would on halt.
                        Misc.free(O3PartitionJob.THREAD_LOCAL_CLEANER);
                        Path.clearThreadLocals();
                    }
                });
                asyncWalApply.start();

                Os.sleep(rnd.nextLong(snapshotIndex * 50L));
                // Make snapshot here
                checkpointCreate((rnd.nextInt() >> 30) == 1, testHardLinkCheckpoint);

                asyncWalApply.join();

                if (ex.get() != null) {
                    throw new RuntimeException(ex.get());
                }

                // Restore snapshot here
                checkpointRecover();
                // Hold the indexes as the restore left them against scans that read no index: the WAL apply below
                // and the transactions after the checkpoint rewrite the partitions they touch, which rebuilds their
                // indexes, and the final comparison reads both tables through their indexes.
                fuzzer.assertIndexesMatchScans(tableNameWal);
                fuzzer.assertIndexesMatchScans(tableNameNonWal);
                engine.notifyWalTxnRepublisher(engine.verifyTableName(tableNameWal));
                drainWalQueue();
                fuzzer.assertIndexesMatchScans(tableNameWal);
                fuzzer.assertRandomIndexes(tableNameWal, tableNameWal, rnd);
                if (afterSnapshot.size() > 0) {
                    fuzzer.applyWal(afterSnapshot, tableNameWal, rnd.nextInt(2) + 1, rnd);
                } else {
                    drainWalQueue();
                }

                Assert.assertFalse("table suspended", engine.getTableSequencerAPI().isSuspended(walTable));

                // Write same data to non-wal table
                fuzzer.applyNonWal(transactions, tableNameNonWal, rnd);

                String limit = "";
                TestUtils.assertSqlCursors(engine, sqlExecutionContext, tableNameNonWal + limit, tableNameWal + limit, LOG);
                fuzzer.assertRandomIndexes(tableNameNonWal, tableNameWal, rnd);
            } finally {
                Misc.freeObjListAndClear(transactions);
            }
        });
    }

    // Lists a name decomposed, an accented letter as the base letter and a combining mark, and finds a file under
    // either form of its name, as HFS+ does.
    private static class DecomposingFilesFacade extends TestFilesFacadeImpl {
        @Override
        public boolean exists(LPSZ path) {
            if (Utf8s.isAscii(path)) {
                return super.exists(path);
            }
            try (Path composed = compose(path)) {
                return super.exists(composed.$());
            }
        }

        @Override
        public void iterateDir(LPSZ path, FindVisitor func) {
            final StringSink name = new StringSink();
            try (Path decomposed = new Path()) {
                super.iterateDir(path, (pUtf8NameZ, type) -> {
                    name.clear();
                    Utf8s.utf8ToUtf16Z(pUtf8NameZ, name);
                    if (Chars.isAscii(name)) {
                        func.onFind(pUtf8NameZ, type);
                    } else {
                        func.onFind(decomposed.of(Normalizer.normalize(name, Normalizer.Form.NFD)).$().ptr(), type);
                    }
                });
            }
        }

        @Override
        public long length(LPSZ name) {
            if (Utf8s.isAscii(name)) {
                return super.length(name);
            }
            try (Path composed = compose(name)) {
                return super.length(composed.$());
            }
        }

        @Override
        public long openRO(LPSZ name) {
            if (Utf8s.isAscii(name)) {
                return super.openRO(name);
            }
            try (Path composed = compose(name)) {
                return super.openRO(composed.$());
            }
        }

        @Override
        public long openRW(LPSZ name, int opts) {
            if (Utf8s.isAscii(name)) {
                return super.openRW(name, opts);
            }
            try (Path composed = compose(name)) {
                return super.openRW(composed.$(), opts);
            }
        }

        @Override
        public boolean removeQuiet(LPSZ name) {
            if (Utf8s.isAscii(name)) {
                return super.removeQuiet(name);
            }
            try (Path composed = compose(name)) {
                return super.removeQuiet(composed.$());
            }
        }

        private static Path compose(LPSZ path) {
            return new Path().of(Normalizer.normalize(Utf8s.stringFromUtf8Bytes(path), Normalizer.Form.NFC));
        }
    }
}
