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

package io.questdb.test.cairo.crash;

import io.questdb.PropertyKey;
import io.questdb.cairo.CommitMode;
import io.questdb.cairo.O3PartitionPurgeJob;
import io.questdb.cairo.PostingSealPurgeJob;
import io.questdb.cairo.RecoveryCoordinator;
import io.questdb.cairo.TableToken;
import io.questdb.cairo.TableWriter;
import io.questdb.mp.SCSequence;
import io.questdb.std.FilesFacade;
import io.questdb.std.Os;
import io.questdb.std.str.LPSZ;
import io.questdb.std.str.Path;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.regex.Pattern;
import java.util.stream.Stream;

/**
 * A power loss at every flush of an operation that seals a POSTING index, in each commit mode but ADAPTIVE
 * ({@code AdaptivePostingResealCrashTest} covers that one), with and without INCLUDE columns: the seal at a partition
 * switch, the reseal of older partitions by a WAL commit (in place, and into a rewritten partition), ADD INDEX,
 * REINDEX, the seal at the generation cap of the last partition, and the rollback of the index when the writer opens
 * a partition. Every one of those seals takes the ordered flush: the new value and cover files reach the disk before
 * the {@code .pk} entry that names them, and the {@code .pk} before the purge may delete the files they supersede. The
 * rewrite of an older partition rebuilds its index into a new value file instead; without INCLUDE that rebuild seals
 * nothing and flushes as on master: under NOSYNC and ASYNC not at all.
 * <p>
 * After each power loss and restart, the table holds the rows of an outcome the operation allows (a WAL commit is in
 * the durable WAL, so recovery applies it), the index finds the rows a scan finds, with the same INCLUDE values, before
 * and after the writer opens, and the table takes the next commit, after which each partition holds one generation of
 * the index. One more power loss comes right after the operation returned and the purge deleted the files it
 * superseded: SYNC must keep the operation; in the other modes the kernel is assumed to have written back every file
 * but the index files of the sealed partitions, so those hold only what the seal flushed, unless a later commit of the
 * operation wrote that index too, or the operation rebuilt that index without a seal. NOSYNC and ASYNC may lose
 * commits, never the index, except after a rebuild without a seal: the purge does not wait for the rebuilt index to be
 * durable before it deletes the generation it superseded, so a power loss after the purge, with the rebuilt index
 * files not written back, leaves an index that names deleted files. That is a limit of the purge, not of the seal.
 * <p>
 * Directory entries follow ext4 and xfs: a file's fsync makes every directory change before it durable. Under strict
 * POSIX a SYNC commit into a new partition loses its directory, a limit of every SYNC commit, not of the seal.
 * <p>
 * PR runs take SYNC without INCLUDE and NOSYNC with INCLUDE only, and cut the power at the {@code .pk} flush of each
 * seal, where the purge may start to delete the files the seal superseded; {@code -Dquestdb.fuzz.nightly=true} takes
 * every mode and cuts the power at every flush.
 */
@RunWith(Parameterized.class)
public class PostingSealPowerLossCrashTest extends AbstractAdaptiveCrashTest {
    private static final long DAY = 86_400_000_000L;
    private static final Pattern INDEX_FILE = Pattern.compile("k\\.(pk|pv\\..*|pc\\d+\\..*|pci.*)");
    private static final boolean IS_NIGHTLY = Boolean.getBoolean("questdb.fuzz.nightly");
    private static final int KEYS = 4;
    private static final Pattern KEY_FILE = Pattern.compile(".*[/\\\\]k\\.pk");
    // The flushes of the new _meta of a metadata swap, as withoutTable() writes them.
    private static final Pattern META_SWAP_FLUSH = Pattern.compile("\\S+ <table>[/\\\\]_meta\\.swp");
    // A flush of an index file or a syncfs, as withoutTable() writes them.
    private static final Pattern SEAL_FLUSH = Pattern.compile(
            "syncfs .*|\\S+ .*[/\\\\]k\\.(pk|pv\\..*|pc\\d+\\..*|pci.*)"
    );
    // The default cairo.posting.seal.gen.threshold: a partition switch or an out-of-order append seals an index of more
    // generations.
    private static final int SEAL_THRESHOLD = 16;
    // The value file a seal writes; the first value file of a partition, k.pv.0, holds the generations of commits.
    private static final Pattern SEAL_VALUE_FILE = Pattern.compile(".*[/\\\\]k\\.pv\\.[1-9][0-9]*");
    // The flushes of _txn, as withoutTable() writes them.
    private static final Pattern TXN_FLUSH = Pattern.compile("\\S+ <table>[/\\\\]_txn");
    // The value files of a seal the operation created.
    private final Set<String> sealValueFiles = new HashSet<>();
    private final String commitMode;
    private final boolean isCovering;
    private volatile boolean isRecordingSealValueFiles;

    public PostingSealPowerLossCrashTest(String commitMode, boolean isCovering) {
        this.commitMode = commitMode;
        this.isCovering = isCovering;
    }

    @Parameterized.Parameters(name = "{0} INCLUDE={1}")
    public static Collection<Object[]> data() {
        if (!IS_NIGHTLY) {
            return Arrays.asList(new Object[][]{{"sync", false}, {"nosync", true}});
        }
        return Arrays.asList(new Object[][]{
                {"sync", false}, {"sync", true},
                {"nosync", false}, {"nosync", true},
                {"async", false}, {"async", true}
        });
    }

    @Override
    protected CrashFaultFilesFacade newCrashFacade() {
        return new CrashFaultFilesFacade() {
            @Override
            public long openCleanRW(LPSZ name, long size) {
                recordSealValueFile(name);
                return super.openCleanRW(name, size);
            }

            @Override
            public long openRW(LPSZ name, int opts) {
                recordSealValueFile(name);
                return super.openRW(name, opts);
            }

            private void recordSealValueFile(LPSZ name) {
                if (isRecordingSealValueFiles && !exists(name)) {
                    final String path = name.asAsciiCharSequence().toString();
                    if (SEAL_VALUE_FILE.matcher(path).matches()) {
                        sealValueFiles.add(path);
                    }
                }
            }
        };
    }

    @Override
    public void setUp() {
        super.setUp();
        setProperty(PropertyKey.CAIRO_COMMIT_MODE, commitMode);
        // The production default: an out-of-order commit appends to or rewrites the partition and reseals its index.
        setProperty(PropertyKey.CAIRO_O3_PARTITION_MERGE_APPEND_ENABLED, "false");
        // Retry a held purge on every job run.
        setProperty(PropertyKey.CAIRO_SQL_COLUMN_PURGE_RETRY_DELAY, 1);
        setProperty(PropertyKey.CAIRO_SQL_COLUMN_PURGE_RETRY_DELAY_LIMIT, 1);
    }

    // A fast WAL commit into the last partition seals it inline once it reaches the generation cap.
    @Test
    public void testGenerationCapSeal() throws Exception {
        // One generation per commit; the cap is 128.
        final List<long[]> r0 = rowsInDay(0, 127);
        final List<long[]> r1 = List.of(row(200_000_000L, 127));
        sweep(new Workload(true, r0, r1, true) {
            @Override
            void run(String table) {
                drainWalQueue();
            }

            @Override
            void setUp(String table) throws Exception {
                createTable(table, true, true, false);
                insertEachAndApply(table, r0);
                execute(insertSql(table, r1));
            }
        });
    }

    // ALTER TABLE ADD INDEX on a non-WAL table builds and seals the index of every partition.
    @Test
    public void testIndexBuild() throws Exception {
        final List<long[]> r0 = rowsOverDays(0, 24, 4);
        sweep(new Workload(false, r0, List.of(), false) {
            @Override
            void run(String table) throws Exception {
                execute(addIndexSql(table));
            }

            @Override
            void setUp(String table) throws Exception {
                createTable(table, false, false, false);
                execute(insertSql(table, r0));
            }
        });
    }

    // ALTER TABLE ADD INDEX on a WAL table builds and seals the index of every partition when the WAL applies it.
    @Test
    public void testIndexBuildWal() throws Exception {
        final List<long[]> r0 = rowsOverDays(0, 24, 4);
        sweep(new Workload(true, r0, List.of(), true) {
            @Override
            void run(String table) {
                drainWalQueue();
            }

            @Override
            void setUp(String table) throws Exception {
                createTable(table, false, true, false);
                execute(insertSql(table, r0));
                drainWalQueue();
                execute(addIndexSql(table));
            }
        });
    }

    // A WAL commit with rows after the last row of each partition appends to the older partitions in place and reseals
    // their index there: a covering index at once, a plain one once it holds more generations than the seal threshold.
    @Test
    public void testOlderPartitionReseal() throws Exception {
        // One commit per row, SEAL_THRESHOLD generations in each day.
        final List<long[]> r0 = new ArrayList<>();
        for (int d = 0; d < 3; d++) {
            for (int i = 0; i < SEAL_THRESHOLD; i++) {
                r0.add(row(d * DAY + (i + 1) * 1_000_000L, r0.size()));
            }
        }
        final List<long[]> r1 = new ArrayList<>();
        for (int d = 0; d < 3; d++) {
            for (int i = 0; i < 4; i++) {
                r1.add(row(d * DAY + 3_600_000_000L + i * 1_000_000L, r0.size() + r1.size()));
            }
        }
        sweep(new Workload(true, r0, r1, true) {
            @Override
            void run(String table) {
                drainWalQueue();
            }

            @Override
            void setUp(String table) throws Exception {
                createTable(table, true, true, true);
                insertEachAndApply(table, r0);
                execute(insertSql(table, r1));
            }
        });
    }

    // A WAL commit with rows between the rows of each partition rewrites the older partitions and rebuilds their index
    // in the new partition version.
    @Test
    public void testOlderPartitionRewrite() throws Exception {
        final List<long[]> r0 = rowsOverDays(0, 24, 3);
        final List<long[]> r1 = new ArrayList<>();
        for (int d = 0; d < 3; d++) {
            for (int i = 0; i < 4; i++) {
                r1.add(row(d * DAY + (2 * i + 1) * 500_000L, 24 + 4 * d + i));
            }
        }
        final Workload workload = new Workload(true, r0, r1, true) {
            @Override
            void run(String table) {
                drainWalQueue();
            }

            @Override
            void setUp(String table) throws Exception {
                createTable(table, true, true, false);
                execute(insertSql(table, r0));
                drainWalQueue();
                execute(insertSql(table, r1));
            }
        };
        // A covering index seals after the rebuild, and that seal flushes the rebuilt index.
        workload.isIndexRebuiltWithoutSeal = !isCovering;
        sweep(workload);
    }

    // A non-WAL commit into the next partition seals the index of the last one, once it holds more generations than
    // the seal threshold.
    @Test
    public void testPartitionSwitchSeal() throws Exception {
        final List<long[]> r0 = rowsInDay(0, SEAL_THRESHOLD + 4);
        final List<long[]> r1 = List.of(row(100_000_000L, 20), row(DAY + 1_000_000L, 21));
        sweep(new Workload(false, r0, r1, false) {
            @Override
            void run(String table) throws Exception {
                execute(insertSql(table, r1));
            }

            @Override
            void setUp(String table) throws Exception {
                createTable(table, true, false, false);
                // One commit per row: a generation each.
                for (int i = 0, n = r0.size(); i < n; i++) {
                    execute(insertSql(table, r0.subList(i, i + 1)));
                }
            }
        });
    }

    // A WAL commit into the next partition seals the covering index of the last one.
    @Test
    public void testPartitionSwitchSealWal() throws Exception {
        Assume.assumeTrue("only a covering index seals at a WAL partition switch", isCovering);
        final List<long[]> r0 = rowsInDay(0, SEAL_THRESHOLD + 4);
        final List<long[]> r1 = List.of(row(DAY + 1_000_000L, 20));
        sweep(new Workload(true, r0, r1, true) {
            @Override
            void run(String table) {
                drainWalQueue();
            }

            @Override
            void setUp(String table) throws Exception {
                createTable(table, true, true, false);
                insertEachAndApply(table, r0);
                execute(insertSql(table, r1));
            }
        });
    }

    // REINDEX rebuilds the index of every partition outside any commit. Only the power loss after REINDEX and the
    // purge: REINDEX deletes the old index files before it writes the new ones, so a power loss inside it leaves a key
    // file too short to open, a limit of REINDEX that this test does not cover.
    @Test
    public void testReindex() throws Exception {
        final List<long[]> r0 = rowsOverDays(0, 24, 4);
        final Workload workload = new Workload(false, r0, List.of(), false) {
            @Override
            void run(String table) throws Exception {
                execute("REINDEX TABLE " + table + " COLUMN k LOCK EXCLUSIVE");
            }

            @Override
            void setUp(String table) throws Exception {
                createTable(table, true, false, false);
                execute(insertSql(table, r0));
            }
        };
        workload.isPowerLossInsideSkipped = true;
        sweep(workload);
    }

    // A power loss right after the seal at the generation cap, stamped with the committed txn, leaves an entry that
    // names the rows of the commit that never landed. The restart opens the writer, which rolls the index of the
    // partition back to its committed rows, into a new value file, and then applies the commit again.
    @Test
    public void testRollbackAtPartitionOpen() throws Exception {
        final List<long[]> r0 = rowsInDay(0, 127);
        final List<long[]> r1 = List.of(row(200_000_000L, 127));
        final Workload workload = new Workload(true, r0, r1, true) {
            // The flush of the operation after which the setup cuts the power: the .pk flush of the seal.
            private int pkFlushOfSeal;

            @Override
            void run(String table) {
                new RecoveryCoordinator(engine).recover();
                engine.notifyWalTxnRepublisher(engine.verifyTableName(table));
                drainWalQueue();
            }

            @Override
            void setUp(String table) throws Exception {
                if (pkFlushOfSeal == 0) {
                    pkFlushOfSeal = findPkFlushOfSeal(table + "_probe");
                }
                final TableToken token = applyUpToCap(table);
                final Set<Long> fds = new HashSet<>(crashFf.noCacheOpenFdsSnapshot());
                markDurableBaseline();
                crashFf.armCrashAt(crashFf.durabilityOpCount() + pkFlushOfSeal);
                // The directory model of the sweep (ext4, xfs): off Linux the seal flushes each file, and only the
                // journal makes the entry of its new value file durable.
                crashFf.setNamespaceJournaled(true);
                try {
                    drainWalQueue();
                } catch (CrashSimulationError ignore) {
                    // the power loss
                }
                crashFf.setNamespaceJournaled(false);
                Assert.assertFalse("the power loss after the seal did not come", crashFf.isCrashArmed());
                engine.releaseAllReaders();
                engine.releaseAllWriters();
                engine.releaseCrashOrphanedWriters();
                discardPurgeQueue();
                simulateRestart(new TableToken[]{token});
                reclaimLingeringNonCacheFds(fds);
            }

            // The commits up to the generation cap applied, and the one that reaches it in the WAL.
            private TableToken applyUpToCap(String table) throws Exception {
                createTable(table, true, true, false);
                insertEachAndApply(table, r0);
                execute(insertSql(table, r1));
                engine.releaseAllWalWriters();
                return engine.verifyTableName(table);
            }

            private int findPkFlushOfSeal(String table) throws Exception {
                applyUpToCap(table);
                final int opsBefore = crashFf.durabilityOpCount();
                drainWalQueue();
                final List<String> log = crashFf.durabilityOpLog();
                final List<Integer> sealKeyFileFlushes = sealKeyFileFlushes(log, opsBefore);
                Assert.assertFalse(
                        "the commit at the generation cap sealed nothing: " + log.subList(opsBefore, log.size()),
                        sealKeyFileFlushes.isEmpty()
                );
                execute("DROP TABLE " + table);
                drainWalQueue();
                drainPurgeJob();
                return sealKeyFileFlushes.get(0) - opsBefore + 1;
            }
        };
        workload.isSetUpCrashed = true;
        // The commit applied again after the rollback adds its row to the rolled back index as a plain commit, which
        // NOSYNC and ASYNC do not flush, as any commit there.
        workload.isSealedIndexCommittedAfterSeal = true;
        sweep(workload);
    }

    // Every partition directory holds one value file of the index, and every cover file belongs to it.
    private static void assertOneGeneration(String tableDir, String where) {
        final Map<String, List<String>> byPartition = new TreeMap<>();
        for (String file : listFiles(tableDir, Pattern.compile("k\\.(pv|pc\\d+)\\..*"))) {
            final java.nio.file.Path p = Paths.get(file);
            byPartition.computeIfAbsent(p.getParent().getFileName().toString(), k -> new ArrayList<>()).add(p.getFileName().toString());
        }
        Assert.assertFalse(where + ": no index files", byPartition.isEmpty());
        for (Map.Entry<String, List<String>> e : byPartition.entrySet()) {
            final List<String> names = e.getValue();
            String sealTxn = null;
            for (String name : names) {
                if (name.startsWith("k.pv.")) {
                    Assert.assertNull(where + ": more than one value file in " + e.getKey() + ": " + names, sealTxn);
                    sealTxn = name.substring(name.lastIndexOf('.'));
                }
            }
            Assert.assertNotNull(where + ": no value file in " + e.getKey() + ": " + names, sealTxn);
            for (String name : names) {
                Assert.assertTrue(where + ": a cover file of another generation in " + e.getKey() + ": " + names,
                        name.endsWith(sealTxn));
            }
        }
    }

    private static void discardPurgeQueue() {
        final SCSequence subSeq = engine.getMessageBus().getPostingSealPurgeSubSeq();
        while (true) {
            final long cursor = subSeq.next();
            if (cursor == -2) {
                continue;
            }
            if (cursor < 0) {
                break;
            }
            subSeq.done(cursor);
        }
    }

    // A fresh process starts the purge log table with a new scoreboard.
    private static void evictPurgeLogScoreboard() {
        final TableToken logToken = engine.getTableTokenIfExists("sys.posting_seal_purge_log");
        if (logToken != null) {
            engine.releaseAllReaders();
            engine.releaseAllWriters();
            engine.getTxnScoreboardPool().remove(logToken);
        }
    }

    // Makes every directory entry under the table durable, as a journal commit would.
    private static void fsyncTableDirs(String tableDir) {
        final FilesFacade ff = engine.getConfiguration().getFilesFacade();
        try (Stream<java.nio.file.Path> paths = Files.walk(Paths.get(tableDir), 1)) {
            paths.filter(Files::isDirectory).forEach(dir -> {
                final long fd = ff.openRONoCache(Path.getThreadLocal(dir.toString()).$());
                Assert.assertTrue(fd > -1);
                ff.fsyncAndClose(fd);
            });
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private static void insertEachAndApply(String table, List<long[]> rows) throws Exception {
        for (int i = 0, n = rows.size(); i < n; i++) {
            execute(insertSql(table, rows.subList(i, i + 1)));
            drainWalQueue();
        }
    }

    private static String insertSql(String table, List<long[]> rows) {
        final StringBuilder sql = new StringBuilder("INSERT INTO ").append(table).append(" VALUES ");
        for (int i = 0, n = rows.size(); i < n; i++) {
            final long[] r = rows.get(i);
            sql.append(i == 0 ? "" : ", ").append('(').append(r[0]).append("::TIMESTAMP, 'k").append(r[1])
                    .append("', ").append(r[2]).append(", 'x").append(r[2]).append("')");
        }
        return sql.toString();
    }

    private static List<String> listFiles(String tableDir, Pattern pattern) {
        final List<String> files = new ArrayList<>();
        try (Stream<java.nio.file.Path> paths = Files.walk(Paths.get(tableDir))) {
            paths.filter(p -> pattern.matcher(p.getFileName().toString()).matches()).forEach(p -> files.add(p.toString()));
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        return files;
    }

    // The flushes, [from, to), of a metadata swap that no power loss may cut. A metadata swap is not atomic with
    // _txn: a power loss between its first rename and the _txn flush leaves _meta and _txn out of step, with no _meta,
    // or with the new _meta and the old _txn (under NOSYNC the swap even clears its _todo_ entry before _txn is
    // durable). A limit of the metadata swap, not of the seal: the range runs from the last flush of _meta.swp to the
    // next flush of _txn, or to the end, and holds no flush of a seal. Empty when the operation swaps no metadata.
    private static int[] metaSwapFlushes(List<String> flushes) {
        int from = -1;
        for (int i = 0, n = flushes.size(); i < n; i++) {
            if (META_SWAP_FLUSH.matcher(flushes.get(i)).matches()) {
                from = i;
            }
        }
        if (from < 0) {
            return new int[]{0, 0};
        }
        int to = from + 1;
        while (to < flushes.size() && !TXN_FLUSH.matcher(flushes.get(to)).matches()) {
            Assert.assertFalse("a seal flush inside the metadata swap: " + flushes,
                    SEAL_FLUSH.matcher(flushes.get(to)).matches());
            to++;
        }
        return new int[]{from, to};
    }

    // The name of the partition a file is in, without its version: 1970-01-01 for 1970-01-01.3/k.pv.4.
    private static String partitionName(String file) {
        final String dir = Paths.get(file).getParent().getFileName().toString();
        final int dot = dir.indexOf('.');
        return dot > -1 ? dir.substring(0, dot) : dir;
    }

    // The names of the partitions of the table, without their versions.
    private static Set<String> partitions(String tableDir) {
        final Set<String> partitions = new HashSet<>();
        try (Stream<java.nio.file.Path> paths = Files.list(Paths.get(tableDir))) {
            paths.filter(p -> Files.isDirectory(p) && Character.isDigit(p.getFileName().toString().charAt(0)))
                    .forEach(p -> partitions.add(partitionName(p.resolve("x").toString())));
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        return partitions;
    }

    // A row of key v % KEYS and value v.
    private static long[] row(long ts, long v) {
        return new long[]{ts, v % KEYS, v};
    }

    // count rows in day 0, one second apart, from value 0
    private static List<long[]> rowsInDay(int from, int count) {
        final List<long[]> rows = new ArrayList<>();
        for (int i = 0; i < count; i++) {
            rows.add(row((i + 1) * 1_000_000L, from + i));
        }
        return rows;
    }

    // count rows over `days` day partitions, from value `from`, one second apart within each day
    private static List<long[]> rowsOverDays(int from, int count, int days) {
        final List<long[]> rows = new ArrayList<>();
        for (int i = 0; i < count; i++) {
            rows.add(row((i % days) * DAY + (i / days + 1) * 1_000_000L, from + i));
        }
        return rows;
    }

    private static void runPurgeJobs() throws Exception {
        engine.releaseAllReaders();
        try (
                PostingSealPurgeJob job = new PostingSealPurgeJob(engine);
                O3PartitionPurgeJob partitionPurge = new O3PartitionPurgeJob(engine, 1)
        ) {
            for (int i = 0; i < 4; i++) {
                Os.sleep(1);
                job.run();
            }
            //noinspection StatementWithEmptyBody
            while (partitionPurge.run()) {
            }
        }
    }

    // The indexes in the flush log, from `from` on, of the .pk flushes of seals: the first .pk flush after the flush of
    // a seal's new files, a syncfs where syncfs is filesystem-wide (Linux), a flush of each file, the value file among
    // them, elsewhere.
    private static List<Integer> sealKeyFileFlushes(List<String> log, int from) {
        final List<Integer> flushes = new ArrayList<>();
        boolean isSealFileFlushed = false;
        for (int i = from, n = log.size(); i < n; i++) {
            final String flush = log.get(i);
            if (KEY_FILE.matcher(flush).matches()) {
                if (isSealFileFlushed) {
                    flushes.add(i);
                }
                isSealFileFlushed = false;
            } else if (flush.contains(" syncfs ") || SEAL_VALUE_FILE.matcher(flush).matches()) {
                isSealFileFlushed = true;
            }
        }
        return flushes;
    }

    // The flush with the table's directory name taken out, the same in every pass.
    private static String withoutTable(String flush, String dirName) {
        return flush.substring(flush.indexOf(' ') + 1).replace(dirName, "<table>");
    }

    private String addIndexSql(String table) {
        return "ALTER TABLE " + table + " ALTER COLUMN k ADD INDEX TYPE POSTING" + (isCovering ? " INCLUDE (v, s)" : "");
    }

    // The table holds the rows of one of the outcomes, and for every key the index finds the rows a scan finds.
    private void assertRecovered(String table, TableToken token, List<List<long[]>> outcomes, String where) throws Exception {
        Assert.assertFalse(where + ": the table is suspended", anyTableSuspended(token));
        final long[][] stats = new long[outcomes.size()][];
        final StringBuilder expected = new StringBuilder();
        for (int i = 0, n = outcomes.size(); i < n; i++) {
            stats[i] = stats(outcomes.get(i));
            expected.append(stats[i][0]).append('/').append(stats[i][1]).append(' ');
        }
        printSql("SELECT count(), sum(v) FROM " + table);
        final String actual = sink.toString();
        int outcome = -1;
        for (int i = 0, n = outcomes.size(); i < n; i++) {
            if (actual.equals("count\tsum\n" + stats[i][0] + "\t" + (stats[i][0] == 0 ? "null" : stats[i][1]) + "\n")) {
                outcome = i;
                break;
            }
        }
        Assert.assertTrue(where + ": rows " + actual + " are none of the outcomes " + expected, outcome > -1);
        for (int k = 0; k < KEYS; k++) {
            final long count = stats[outcome][2 + 2 * k];
            final long sum = stats[outcome][3 + 2 * k];
            assertQuery("SELECT count(), sum(v) FROM " + table + " WHERE k = 'k" + k + "'")
                    .noLeakCheck()
                    .noRandomAccess()
                    .expectSize()
                    .returns("count\tsum\n" + count + "\t" + (count == 0 ? "null" : sum) + "\n");
            assertSqlCursors(
                    "SELECT ts, k, v, s FROM " + table + " WHERE k::STRING = 'k" + k + "'",
                    "SELECT ts, k, v, s FROM " + table + " WHERE k = 'k" + k + "'"
            );
        }
    }

    private void createTable(String table, boolean isIndexed, boolean isWal, boolean isDedup) throws Exception {
        execute("CREATE TABLE " + table + " (ts TIMESTAMP, k SYMBOL"
                + (isIndexed ? " INDEX TYPE POSTING" + (isCovering ? " INCLUDE (v, s)" : "") : "")
                + ", v LONG, s VARCHAR) TIMESTAMP(ts) PARTITION BY DAY " + (isWal ? "WAL" : "BYPASS WAL")
                + (isDedup ? " DEDUP UPSERT KEYS(ts, k)" : ""));
    }

    // Every file of the table but the index files of the sealed partitions is durable, as the kernel may have written
    // them back.
    private void markWrittenBackExceptSealedIndexes(String tableDir, Set<String> sealedPartitions) {
        try (Stream<java.nio.file.Path> paths = Files.walk(Paths.get(tableDir))) {
            paths.filter(Files::isRegularFile).forEach(p -> {
                if (!INDEX_FILE.matcher(p.getFileName().toString()).matches()
                        || !sealedPartitions.contains(partitionName(p.toString()))) {
                    crashFf.markFileDurable(p.toString());
                }
            });
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    // The restart after a power loss, as a fresh process runs it. The process closed every file but those open
    // before the operation.
    private void restart(TableToken token, boolean isWal, Set<Long> fds) {
        simulateRestart(new TableToken[]{token});
        reclaimLingeringNonCacheFds(fds);
        new RecoveryCoordinator(engine).recover();
        if (isWal) {
            engine.notifyWalTxnRepublisher(token);
            drainWalQueue();
        }
        evictPurgeLogScoreboard();
    }

    // Switches the instance mode and checks that the configuration reports it.
    private void setCommitMode(String mode) {
        node1.setProperty(PropertyKey.CAIRO_COMMIT_MODE, mode);
        Assert.assertEquals(mode, CommitMode.toString(engine.getConfiguration().getCommitMode()));
    }

    // {count, sum(v)} of the rows, then {count, sum(v)} of each key.
    private long[] stats(List<long[]> rows) {
        final long[] stats = new long[2 + 2 * KEYS];
        for (int i = 0, n = rows.size(); i < n; i++) {
            final long[] r = rows.get(i);
            stats[0]++;
            stats[1] += r[2];
            stats[2 + 2 * (int) r[1]]++;
            stats[3 + 2 * (int) r[1]] += r[2];
        }
        return stats;
    }

    // The first pass runs the operation to its end, records its flushes, and cuts the power after the purge. Each
    // later pass cuts it at one of those flushes, on a fresh table.
    private void sweep(Workload workload) throws Exception {
        runWithCrashFacade(() -> {
            // The purge log table exists before the first power loss, as on a server that ran its purge job before.
            try (PostingSealPurgeJob ignore = new PostingSealPurgeJob(engine)) {
                Assert.assertNotNull(ignore);
            }
            List<String> flushes = null;
            List<Integer> crashPoints = null;
            for (int pass = 0; crashPoints == null || pass <= crashPoints.size(); pass++) {
                final int crashAt = pass == 0 ? 0 : crashPoints.get(pass - 1);
                final String table = "t" + pass;
                // The setup flushes nothing: the baseline makes all of it durable.
                setCommitMode("nosync");
                workload.setUp(table);
                final TableToken token = engine.verifyTableName(table);
                final String tableDir = Paths.get(root, token.getDirName()).toString();
                engine.releaseAllReaders();
                engine.releaseAllWriters();
                engine.releaseAllWalWriters();
                setCommitMode(commitMode);
                markDurableBaseline();
                final Set<Long> fds = new HashSet<>(crashFf.noCacheOpenFdsSnapshot());
                final int opsBefore = crashFf.durabilityOpCount();
                crashFf.setNamespaceJournaled(true);
                sealValueFiles.clear();
                isRecordingSealValueFiles = true;
                if (crashAt > 0) {
                    crashFf.armCrashAt(opsBefore + crashAt);
                }
                try {
                    workload.run(table);
                } catch (Throwable th) {
                    if (crashAt == 0 || crashFf.isCrashArmed()) {
                        throw th;
                    }
                    // the power loss
                }
                isRecordingSealValueFiles = false;
                final String where;
                if (crashAt == 0) {
                    final List<String> log = crashFf.durabilityOpLog();
                    flushes = new ArrayList<>();
                    crashPoints = new ArrayList<>();
                    for (int i = opsBefore, n = log.size(); i < n; i++) {
                        flushes.add(withoutTable(log.get(i), token.getDirName()));
                    }
                    final int[] metaSwap = metaSwapFlushes(flushes);
                    if (IS_NIGHTLY && !workload.isPowerLossInsideSkipped) {
                        for (int i = 0, n = flushes.size(); i < n; i++) {
                            if (i < metaSwap[0] || i >= metaSwap[1]) {
                                crashPoints.add(i + 1);
                            }
                        }
                    }
                    if (!IS_NIGHTLY && !workload.isPowerLossInsideSkipped) {
                        for (int i : sealKeyFileFlushes(log, opsBefore)) {
                            crashPoints.add(i - opsBefore + 1);
                        }
                    }
                    where = "power loss after the operation returned and the purge ran";
                    // The partitions whose index the operation sealed.
                    final Set<String> sealed = new HashSet<>();
                    for (String file : sealValueFiles) {
                        sealed.add(partitionName(file));
                    }
                    Assert.assertFalse("the operation sealed nothing, flushes " + flushes, sealed.isEmpty());
                    runPurgeJobs();
                    fsyncTableDirs(tableDir);
                    if (!"sync".equals(commitMode)) {
                        markWrittenBackExceptSealedIndexes(
                                tableDir,
                                workload.isSealedIndexCommittedAfterSeal || workload.isIndexRebuiltWithoutSeal
                                        ? Set.of()
                                        : sealed
                        );
                    }
                    crashFf.powerLoss();
                } else {
                    Assert.assertFalse("the power loss at flush " + crashAt + " did not come", crashFf.isCrashArmed());
                    final String flush = withoutTable(crashFf.durabilityOpLog().get(opsBefore + crashAt - 1), token.getDirName());
                    where = "power loss at flush " + crashAt + " (" + flush + ")";
                    Assert.assertEquals(where + ": the flushes of the operation changed", flushes.get(crashAt - 1), flush);
                }
                crashFf.setNamespaceJournaled(false);
                engine.releaseAllReaders();
                engine.releaseAllWriters();
                engine.releaseCrashOrphanedWriters();
                discardPurgeQueue();
                restart(token, workload.isWal, fds);
                try {
                    final List<List<long[]>> outcomes = crashAt == 0 ? workload.returnedOutcomes() : workload.outcomes();
                    assertRecovered(table, token, outcomes, where);
                    // The writer opens, with its recovery.
                    try (TableWriter writer = getWriter(token)) {
                        Assert.assertNotNull(writer);
                    }
                    assertRecovered(table, token, outcomes, where + ", writer opened");
                    // The next commit, a row at the end of each partition, and the purge leave one generation in each.
                    final List<long[]> next = new ArrayList<>();
                    for (String partition : partitions(tableDir)) {
                        final long day = java.time.LocalDate.parse(partition).toEpochDay();
                        next.add(row(day * DAY + 80_000_000_000L, 1000 + next.size()));
                    }
                    execute(insertSql(table, next));
                    if (workload.isWal) {
                        drainWalQueue();
                    }
                    runPurgeJobs();
                    final List<List<long[]>> nextOutcomes = new ArrayList<>();
                    for (int i = 0, n = outcomes.size(); i < n; i++) {
                        final List<long[]> rows = new ArrayList<>(outcomes.get(i));
                        rows.addAll(next);
                        nextOutcomes.add(rows);
                    }
                    assertRecovered(table, token, nextOutcomes, where + ", next commit");
                    if (crashAt == 0 && !workload.isSetUpCrashed) {
                        // A power loss before the purge of the generations it superseded, or inside a seal, leaves
                        // their files behind: the purge queue lives in memory, and only ADAPTIVE recovery removes the
                        // generations no chain entry names.
                        assertOneGeneration(tableDir, where + ", next commit");
                    }
                } catch (Throwable th) {
                    throw new AssertionError(th.getMessage() + "\nflushes of the operation: " + flushes, th);
                }
                execute("DROP TABLE " + table);
                if (workload.isWal) {
                    drainWalQueue();
                }
                drainPurgeJob();
            }
        });
    }

    private abstract static class Workload {
        // The operation rebuilds the index of the partitions it writes without a seal, and under NOSYNC and ASYNC that
        // rebuild takes no flush: the power loss after the purge assumes the kernel wrote the rebuilt index files back,
        // as the purge does not wait for them to be durable.
        boolean isIndexRebuiltWithoutSeal;
        // The power loss after the operation returned and the purge ran is the only one.
        boolean isPowerLossInsideSkipped;
        // R1 is in the durable WAL before the operation, so recovery applies it.
        final boolean isR1Durable;
        // A plain commit of the operation wrote the index of the sealed partitions after the seal, so the kernel may
        // have written those index files back as any other file.
        boolean isSealedIndexCommittedAfterSeal;
        // The setup ends with a power loss.
        boolean isSetUpCrashed;
        final boolean isWal;
        final List<long[]> r0;
        final List<long[]> r1;

        Workload(boolean isWal, List<long[]> r0, List<long[]> r1, boolean isR1Durable) {
            this.isWal = isWal;
            this.r0 = r0;
            this.r1 = r1;
            this.isR1Durable = isR1Durable;
        }

        // The row sets a power loss inside the operation may leave.
        List<List<long[]>> outcomes() {
            final List<long[]> all = new ArrayList<>(r0);
            all.addAll(r1);
            return isR1Durable || r1.isEmpty() ? List.of(all) : List.of(r0, all);
        }

        // The row set after the operation returned.
        List<List<long[]>> returnedOutcomes() {
            final List<long[]> all = new ArrayList<>(r0);
            all.addAll(r1);
            return List.of(all);
        }

        abstract void run(String table) throws Exception;

        abstract void setUp(String table) throws Exception;
    }
}
