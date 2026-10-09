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
import io.questdb.cairo.TableReader;
import io.questdb.cairo.TableToken;
import io.questdb.cairo.TableWriter;
import io.questdb.mp.SCSequence;
import io.questdb.std.FilesFacade;
import io.questdb.std.Os;
import io.questdb.std.str.Path;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.regex.Pattern;
import java.util.stream.Stream;

/**
 * A posting-index reseal under ADAPTIVE takes no inline save point, so a superseded {@code .pv} the last save
 * point still references outlives the commit that replaced it until the next periodic save point. A crash before
 * that save point recovers from the last one, whose index still reads that {@code .pv} and its covers. The pin
 * of that save point must keep the purge job from removing them, even where the removal would have reached the
 * disk. The purge rows a crash leaves open are replayed by the purge job of every later start until one start
 * finishes them all, in whatever mode the instance then runs, and must not keep readers out of the recovered table.
 */
public class AdaptivePostingResealCrashTest extends AbstractAdaptiveCrashTest {
    private static final long DAY = 86_400_000_000L;
    private static final long DAY1 = 1_704_067_200_000_000L; // 2024-01-01T00:00:00Z
    private static final String EPOCH_METRIC = "questdb_wal_adaptive_epoch_advances_total";
    private static final long HOUR = 3_600_000_000L;
    private static final int PASSES = 4;
    private static final int ROWS_PER_PASS = 10;
    private static final Pattern SEALED_FILE = Pattern.compile("s\\.(pv|pc\\d+)\\..*");
    // The sealed posting files of the covering column s and the plain column k of the three-day table.
    private static final Pattern SEALED_FILE_S_K = Pattern.compile("[sk]\\.(pv|pc\\d+)\\..*");
    private static final int SYMBOLS = 4;

    @Test
    public void testCrashBeforeNextSavePointRecoversFromLastSavePoint() throws Exception {
        crashBeforeNextSavePoint(true);
    }

    @Test
    public void testKillBeforeNextSavePointRecoversFromLastSavePoint() throws Exception {
        crashBeforeNextSavePoint(false);
    }

    @Test
    public void testKillsLeaveOneGenerationInEveryPartition() throws Exception {
        assertOneGenerationInEveryPartitionAfterCrashes(false);
    }

    @Test
    public void testKillsLeaveOnePostingGeneration() throws Exception {
        assertOnePostingGenerationAfterCrashes(false);
    }

    // A power loss at every durability operation of the apply of a pass that reseals two older partitions and the
    // active one in place and ends with a save point, after two such passes since the last save point. Recovery reads
    // every generation the restored txn needs, and after the next save point every partition holds one generation of
    // each POSTING column.
    @Test
    public void testPowerLossAtEveryOpOfOlderPartitionResealLeavesOneGeneration() throws Exception {
        setUpAdaptive();
        runWithCrashFacade(() -> {
            int swept = 0;
            int savePointKept = 0;
            for (int op = 1; ; op++) {
                final String table = "w" + op;
                final TableToken token = createThreeDayTable(table);
                final String tableDir = Paths.get(root, token.getDirName()).toString();
                markDurableBaseline();
                try (
                        PostingSealPurgeJob job = new PostingSealPurgeJob(engine);
                        O3PartitionPurgeJob partitionPurge = new O3PartitionPurgeJob(engine, 1)
                ) {
                    for (int pass = 1; pass <= 2; pass++) {
                        execute(threeDaySql(table, pass));
                        drainWalQueue();
                        runPurgeJobs(job, partitionPurge);
                        fsyncTableDirs(tableDir);
                    }
                }
                // The commit is in the WAL; the sweep covers the apply that reseals and the save point after it.
                execute(threeDaySql(table, 3));
                final Set<Long> fds = new HashSet<>(crashFf.noCacheOpenFdsSnapshot());
                crashFf.armCrashAt(crashFf.durabilityOpCount() + op);
                node1.setProperty(PropertyKey.CAIRO_ADAPTIVE_EPOCH_MAX_ROWS, 1);
                try {
                    drainWalQueue();
                } catch (CrashSimulationError ignore) {
                    // the power loss
                } finally {
                    node1.setProperty(PropertyKey.CAIRO_ADAPTIVE_EPOCH_MAX_ROWS, Long.MAX_VALUE);
                }
                if (crashFf.isCrashArmed()) {
                    // The pass made fewer operations: every one is swept.
                    crashFf.armCrashAt(0);
                    break;
                }
                swept++;
                final String where = "power loss at op " + op;
                engine.releaseAllReaders();
                engine.releaseAllWriters();
                engine.releaseCrashOrphanedWriters();
                reclaimLingeringNonCacheFds(fds);
                discardPurgeQueue();
                final long epochsBeforeRecovery = TestUtils.getMetricValue(engine, EPOCH_METRIC);
                recoverAfterCrash(new TableToken[]{token});
                // The pass's save point was lost when recovery applies its WAL again, which takes a new one.
                final boolean savePointLost = TestUtils.getMetricValue(engine, EPOCH_METRIC) > epochsBeforeRecovery;
                evictPurgeLogScoreboard();
                Assert.assertFalse(where + ": recovery suspended the table", anyTableSuspended(token));
                final long rows;
                try (TableReader reader = getReader(token)) {
                    rows = reader.size();
                }
                // The crashed pass is in the WAL, so recovery applies it again.
                final int lastPass = (int) (rows / ROWS_PER_PASS) - 1;
                Assert.assertEquals(where + ": rows", 4 * ROWS_PER_PASS, rows);
                assertThreeDayRowsThroughIndex(table, lastPass);
                savePointPass(table, lastPass + 1);
                try (
                        PostingSealPurgeJob job = new PostingSealPurgeJob(engine);
                        O3PartitionPurgeJob partitionPurge = new O3PartitionPurgeJob(engine, 1)
                ) {
                    runPurgeJobs(job, partitionPurge);
                }
                assertThreeDayRowsThroughIndex(table, lastPass + 1);
                if (savePointLost) {
                    assertOneGenerationPerPartitionAndColumn(tableDir, where);
                } else {
                    // The pass's save point was durable, so there was nothing to roll back. The generation the pass
                    // superseded waits on a purge log row, and the purge log is not a WAL table, so ADAPTIVE does
                    // not make that row durable: another leak, not this one.
                    savePointKept++;
                }
                // A smaller database keeps the next power loss quick.
                execute("DROP TABLE " + table);
                drainWalQueue();
                drainPurgeJob();
            }
            Assert.assertTrue("operations swept: " + swept + ", save point kept: " + savePointKept, swept - savePointKept > 3);
        });
    }

    @Test
    public void testPowerLossesLeaveOneGenerationInEveryPartition() throws Exception {
        assertOneGenerationInEveryPartitionAfterCrashes(true);
    }

    @Test
    public void testPowerLossesLeaveOnePostingGeneration() throws Exception {
        assertOnePostingGenerationAfterCrashes(true);
    }

    // The instance runs each mode after the kill (ADAPTIVE is the control), for two starts with no write between.
    // The first start recovers and pins the table, and the pin holds the rows that end above the save point; the WAL
    // apply in another mode leaves adaptive and drops the pin. The held rows stay in the purge log, so the second
    // start replays them again, now with no pin, also when that second start runs in ADAPTIVE.
    @Test
    public void testPurgeLogReplayAfterKillKeepsReadersOpen() throws Exception {
        setUpAdaptive();
        runWithCrashFacade(() -> {
            final StringBuilder failures = new StringBuilder();
            // The modes of the two starts after the kill.
            for (String[] modes : new String[][]{
                    {"adaptive", "adaptive"},
                    {"nosync", "nosync"},
                    {"async", "async"},
                    {"sync", "sync"},
                    {"nosync", "adaptive"}
            }) {
                setCommitMode("adaptive");
                final String name = modes[0] + ", then " + modes[1];
                final String table = "t_" + modes[0] + "_" + modes[1];
                execute("CREATE TABLE " + table + " (ts TIMESTAMP, s SYMBOL INDEX TYPE POSTING INCLUDE (v), v LONG) "
                        + "TIMESTAMP(ts) PARTITION BY DAY WAL DEDUP UPSERT KEYS(ts, s)");
                execute("INSERT INTO " + table + " VALUES " + row(DAY1 + 10 * HOUR, 0) + ", " + row(DAY1 + DAY + 10 * HOUR, 1));
                // The first apply takes the save point the kill recovers from.
                drainWalQueue();
                final TableToken token = engine.verifyTableName(table);
                try (PostingSealPurgeJob job = new PostingSealPurgeJob(engine)) {
                    runPurgeJob(job);
                    // Each in-place commit reseals its partition. The first reseal of each day supersedes a .pv the
                    // save point names, so the pin holds its purge row open.
                    insertAndApply(table, row(DAY1 + 11 * HOUR, 2), job);
                    insertAndApply(table, row(DAY1 + DAY + 11 * HOUR, 3), job);
                    // A reader up to the kill holds open the purges of the next .pv of each day: windows above the
                    // save point.
                    try (TableReader ignore = getReader(token)) {
                        insertAndApply(table, row(DAY1 + 12 * HOUR, 4), job);
                        insertAndApply(table, row(DAY1 + 13 * HOUR, 5), job);
                        insertAndApply(table, row(DAY1 + DAY + 12 * HOUR, 6), job);
                        Assert.assertEquals("purge rows open at the kill", 4, job.getOutstandingPurgeTasks());
                    }
                }
                kill(token);
                // Recovery follows the table's enrolment, not the instance mode, so the mode can change after it.
                setCommitMode(modes[0]);
                for (int start = 1; start <= 2; start++) {
                    if (start == 2) {
                        setCommitMode(modes[1]);
                        kill(token);
                        // Only the control stays adaptive through the first start, so only it is pinned again.
                        final boolean control = "adaptive".equals(modes[0]);
                        Assert.assertEquals(name + ": pinned at start 2", control,
                                engine.getTableSequencerAPI().getTxnTracker(token).getPinnedEpochTxn() > -1);
                    }
                    Assert.assertEquals(name, CommitMode.fromString(modes[start - 1]), engine.getConfiguration().getCommitMode());
                    // A start builds the purge job, which replays the open rows of its log, before any WAL apply.
                    try (PostingSealPurgeJob job = new PostingSealPurgeJob(engine)) {
                        engine.notifyWalTxnRepublisher(token);
                        drainWalQueue();
                        runPurgeJob(job);
                        // The job stays up, as on a server, and keeps its writer of the purge log.
                        assertQuery("SELECT count(), sum(v) FROM " + table).noLeakCheck().noRandomAccess().expectSize()
                                .returns("count\tsum\n7\t21\n");
                        for (int k = 0; k < SYMBOLS; k++) {
                            long count = 0;
                            long sum = 0;
                            for (int v = k; v < 7; v += SYMBOLS) {
                                count++;
                                sum += v;
                            }
                            assertQuery("SELECT count(), sum(v) FROM " + table + " WHERE s = '" + (char) ('a' + k) + "'")
                                    .noLeakCheck()
                                    .noRandomAccess()
                                    .expectSize()
                                    .returns("count\tsum\n" + count + "\t" + sum + "\n");
                        }
                    } catch (Throwable th) {
                        failures.append(name).append(", start ").append(start).append(": ").append(th.getMessage()).append('\n');
                        break;
                    }
                }
            }
            Assert.assertEquals("", failures.toString());
        });
    }

    // An adaptive kill after in-place reseals of both days. The next start runs in NOSYNC: it recovers, opens and closes
    // the writer, and dies before the WAL apply commits anything. One more start follows. Every start reads the
    // save point's rows through the index, and the apply then reads every row.
    @Test
    public void testWriterOpenedWithoutApplyThenRestartKeepsSavePointFiles() throws Exception {
        setUpAdaptive();
        runWithCrashFacade(() -> {
            execute("CREATE TABLE t (ts TIMESTAMP, s SYMBOL INDEX TYPE POSTING INCLUDE (v), v LONG) "
                    + "TIMESTAMP(ts) PARTITION BY DAY WAL DEDUP UPSERT KEYS(ts, s)");
            execute("INSERT INTO t VALUES " + row(DAY1 + 10 * HOUR, 0) + ", " + row(DAY1 + DAY + 10 * HOUR, 1));
            // The first apply takes the save point the kill recovers from.
            drainWalQueue();
            final TableToken token = engine.verifyTableName("t");
            try (PostingSealPurgeJob job = new PostingSealPurgeJob(engine)) {
                runPurgeJob(job);
                insertAndApply("t", row(DAY1 + 11 * HOUR, 2), job);
                insertAndApply("t", row(DAY1 + DAY + 11 * HOUR, 3), job);
                try (TableReader ignore = getReader(token)) {
                    insertAndApply("t", row(DAY1 + 12 * HOUR, 4), job);
                    insertAndApply("t", row(DAY1 + 13 * HOUR, 5), job);
                    insertAndApply("t", row(DAY1 + DAY + 12 * HOUR, 6), job);
                }
            }
            setCommitMode("nosync");
            kill(token);
            try (PostingSealPurgeJob ignore = new PostingSealPurgeJob(engine)) {
                assertSavePointRowsThroughIndex();
                try (TableWriter ignore2 = getWriter(token)) {
                    Assert.assertNotNull(ignore2);
                }
                assertSavePointRowsThroughIndex();
            }
            // The process dies before the apply commits anything.
            kill(token);
            try (PostingSealPurgeJob job = new PostingSealPurgeJob(engine)) {
                assertSavePointRowsThroughIndex();
                engine.notifyWalTxnRepublisher(token);
                drainWalQueue();
                runPurgeJob(job);
                assertQuery("SELECT count(), sum(v) FROM t").noLeakCheck().noRandomAccess().expectSize()
                        .returns("count\tsum\n7\t21\n");
                for (int k = 0; k < SYMBOLS; k++) {
                    long count = 0;
                    long sum = 0;
                    for (int v = k; v < 7; v += SYMBOLS) {
                        count++;
                        sum += v;
                    }
                    assertQuery("SELECT count(), sum(v) FROM t WHERE s = '" + (char) ('a' + k) + "'")
                            .noLeakCheck()
                            .noRandomAccess()
                            .expectSize()
                            .returns("count\tsum\n" + count + "\t" + sum + "\n");
                }
            }
        });
    }

    // Every partition directory holds the save point generations of each POSTING column of the three-day table.
    private static void assertOneGenerationPerPartitionAndColumn(String tableDir, String where) {
        final Map<String, List<String>> byPartitionAndColumn = new TreeMap<>();
        final List<String> files = sealedFiles(tableDir, SEALED_FILE_S_K);
        for (int i = 0, n = files.size(); i < n; i++) {
            final java.nio.file.Path file = Paths.get(files.get(i));
            final String name = file.getFileName().toString();
            final String key = file.getParent().getFileName() + "/" + name.substring(0, name.indexOf('.'));
            byPartitionAndColumn.computeIfAbsent(key, k -> new ArrayList<>()).add(name);
        }
        Assert.assertEquals(where + ": partitions and columns with sealed files " + byPartitionAndColumn.keySet(), 6, byPartitionAndColumn.size());
        for (Map.Entry<String, List<String>> e : byPartitionAndColumn.entrySet()) {
            assertSavePointGenerations(where, e.getKey(), e.getValue());
        }
    }

    // Every partition directory holds the save point generations of column s.
    private static void assertOnePostingGenerationPerPartition(String tableDir, String where) {
        final Map<String, List<String>> byPartition = new TreeMap<>();
        final List<String> files = sealedFiles(tableDir);
        for (int i = 0, n = files.size(); i < n; i++) {
            final java.nio.file.Path file = Paths.get(files.get(i));
            byPartition.computeIfAbsent(file.getParent().getFileName().toString(), k -> new ArrayList<>()).add(file.getFileName().toString());
        }
        Assert.assertFalse(where + ": no sealed files", byPartition.isEmpty());
        for (Map.Entry<String, List<String>> e : byPartition.entrySet()) {
            assertSavePointGenerations(where, e.getKey(), e.getValue());
        }
    }

    // At most two generations: the one the restored save point names, and the one the older save point names, which
    // recovery falls back to if the restored one is damaged and so stays until the next save point. Every cover file
    // belongs to one of them.
    private static void assertSavePointGenerations(String where, String key, List<String> names) {
        final List<String> sealTxns = new ArrayList<>();
        for (String name : names) {
            if (name.contains(".pv.")) {
                sealTxns.add(name.substring(name.lastIndexOf('.')));
            }
        }
        Assert.assertFalse(where + ": no .pv in " + key + ": " + names, sealTxns.isEmpty());
        Assert.assertTrue(where + ": more than two .pv in " + key + ": " + names, sealTxns.size() <= 2);
        for (String name : names) {
            Assert.assertTrue(where + ": a cover of another generation in " + key + ": " + names, sealTxns.contains(name.substring(name.lastIndexOf('.'))));
        }
    }

    // A killed process loses its in-memory purge queue, including what a writer published at close.
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

    // Makes every removal so far durable, as a journal commit before a power loss would.
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

    private static void insertAndApply(String table, String values, PostingSealPurgeJob job) throws Exception {
        execute("INSERT INTO " + table + " VALUES " + values);
        drainWalQueue();
        runPurgeJob(job);
    }

    // One WAL commit of ROWS_PER_PASS rows in day 1, from 10:00 + 10 minutes per pass, after every earlier row.
    private static String passSql(int pass) {
        final long step = HOUR / 60;
        final long start = DAY1 + 10L * HOUR + pass * ROWS_PER_PASS * step;
        final StringBuilder sql = new StringBuilder("INSERT INTO t VALUES ");
        for (int i = 0; i < ROWS_PER_PASS; i++) {
            final long v = (long) pass * ROWS_PER_PASS + i;
            sql.append(i == 0 ? "" : ",").append('(').append(start + i * step).append("::timestamp, '")
                    .append((char) ('a' + v % SYMBOLS)).append("', ").append(v).append(')');
        }
        return sql.toString();
    }

    // A row of symbol v % SYMBOLS and value v.
    private static String row(long ts, long v) {
        return "(" + ts + "::timestamp, '" + (char) ('a' + v % SYMBOLS) + "', " + v + ")";
    }

    private static void runPurgeJob(PostingSealPurgeJob job) {
        for (int i = 0; i < 4; i++) {
            Os.sleep(1);
            job.run();
        }
    }

    private static void runPurgeJobs(PostingSealPurgeJob job, O3PartitionPurgeJob partitionPurge) {
        runPurgeJob(job);
        //noinspection StatementWithEmptyBody
        while (partitionPurge.run()) {
        }
    }

    // The sealed posting files of column s: every .pv and every cover file.
    private static List<String> sealedFiles(String tableDir) {
        return sealedFiles(tableDir, SEALED_FILE);
    }

    private static List<String> sealedFiles(String tableDir, Pattern sealedFile) {
        final List<String> files = new ArrayList<>();
        try (Stream<java.nio.file.Path> paths = Files.walk(Paths.get(tableDir))) {
            paths.filter(p -> sealedFile.matcher(p.getFileName().toString()).matches()).forEach(p -> files.add(p.toString()));
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        return files;
    }

    private static void setUpAdaptive() {
        setProperty(PropertyKey.CAIRO_COMMIT_MODE, "adaptive");
        // No periodic save point inside a test.
        setProperty(PropertyKey.CAIRO_ADAPTIVE_EPOCH_INTERVAL, 1_000_000_000L);
        setProperty(PropertyKey.CAIRO_ADAPTIVE_EPOCH_MAX_ROWS, Long.MAX_VALUE);
        // A crash takes no save point at writer close.
        setProperty(PropertyKey.CAIRO_ADAPTIVE_EPOCH_FLUSH_ON_CLOSE, "false");
        // Retry a held purge on every job run, so the job would remove the file as soon as nothing held it.
        setProperty(PropertyKey.CAIRO_SQL_COLUMN_PURGE_RETRY_DELAY, 1);
        setProperty(PropertyKey.CAIRO_SQL_COLUMN_PURGE_RETRY_DELAY_LIMIT, 1);
    }

    // One WAL commit of ROWS_PER_PASS rows over three days, each after every earlier row of its day: with DEDUP, the two
    // older partitions and the active one take the rows in place and reseal both POSTING columns there.
    private static String threeDaySql(String table, int pass) {
        final StringBuilder sql = new StringBuilder("INSERT INTO ").append(table).append(" VALUES ");
        for (int i = 0; i < ROWS_PER_PASS; i++) {
            final long v = (long) pass * ROWS_PER_PASS + i;
            final long ts = DAY1 + (i % 3) * DAY + 10L * HOUR + pass * 600_000_000L + i * 30_000_000L;
            sql.append(i == 0 ? "" : ", ").append('(').append(ts).append("::timestamp, '")
                    .append((char) ('a' + v % SYMBOLS)).append("', '").append((char) ('p' + v % 3)).append("', ")
                    .append(v).append(')');
        }
        return sql.toString();
    }

    // Three crashes, each after passes that reseal two older partitions and the active one in place, for a covering and
    // a plain POSTING column. After each recovery, one more commit, the next save point and a restart's purge jobs
    // leave one generation of each column in every partition.
    private void assertOneGenerationInEveryPartitionAfterCrashes(boolean powerLoss) throws Exception {
        setUpAdaptive();
        runWithCrashFacade(() -> {
            final TableToken token = createThreeDayTable("w");
            final String tableDir = Paths.get(root, token.getDirName()).toString();
            int pass = 0;
            for (int crash = 1; crash <= 3; crash++) {
                if (powerLoss) {
                    markDurableBaseline();
                }
                try (
                        PostingSealPurgeJob job = new PostingSealPurgeJob(engine);
                        O3PartitionPurgeJob partitionPurge = new O3PartitionPurgeJob(engine, 1)
                ) {
                    for (int i = 0; i < PASSES; i++) {
                        execute(threeDaySql("w", ++pass));
                        drainWalQueue();
                        runPurgeJobs(job, partitionPurge);
                        fsyncTableDirs(tableDir);
                    }
                }
                if (powerLoss) {
                    engine.releaseAllReaders();
                    engine.releaseAllWriters();
                    engine.releaseCrashOrphanedWriters();
                    discardPurgeQueue();
                    recoverAfterCrash(new TableToken[]{token});
                } else {
                    kill(token);
                    engine.notifyWalTxnRepublisher(token);
                    drainWalQueue();
                }
                evictPurgeLogScoreboard();
                Assert.assertFalse("recovery suspended the table", anyTableSuspended(token));
                assertThreeDayRowsThroughIndex("w", pass);
                savePointPass("w", ++pass);
                try (
                        PostingSealPurgeJob job = new PostingSealPurgeJob(engine);
                        O3PartitionPurgeJob partitionPurge = new O3PartitionPurgeJob(engine, 1)
                ) {
                    runPurgeJobs(job, partitionPurge);
                }
                assertThreeDayRowsThroughIndex("w", pass);
                assertOneGenerationPerPartitionAndColumn(tableDir, (powerLoss ? "power loss " : "kill ") + crash);
            }
        });
    }

    // Three crashes, each after in-place reseals since the last save point. After each recovery, one more commit, the
    // next save point and a restart's purge jobs leave one posting generation in the partition.
    private void assertOnePostingGenerationAfterCrashes(boolean powerLoss) throws Exception {
        setUpAdaptive();
        runWithCrashFacade(() -> {
            execute("CREATE TABLE t (ts TIMESTAMP, s SYMBOL INDEX TYPE POSTING INCLUDE (v), v LONG) TIMESTAMP(ts) "
                    + "PARTITION BY DAY WAL DEDUP UPSERT KEYS(ts, s)");
            execute(passSql(0));
            drainWalQueue();
            final TableToken token = engine.verifyTableName("t");
            final String tableDir = Paths.get(root, token.getDirName()).toString();
            try (PostingSealPurgeJob job = new PostingSealPurgeJob(engine)) {
                runPurgeJob(job);
            }
            int pass = 0;
            for (int crash = 1; crash <= 3; crash++) {
                if (powerLoss) {
                    markDurableBaseline();
                }
                try (
                        PostingSealPurgeJob job = new PostingSealPurgeJob(engine);
                        O3PartitionPurgeJob partitionPurge = new O3PartitionPurgeJob(engine, 1)
                ) {
                    for (int i = 0; i < PASSES; i++) {
                        execute(passSql(++pass));
                        drainWalQueue();
                        runPurgeJobs(job, partitionPurge);
                        fsyncTableDirs(tableDir);
                    }
                }
                if (powerLoss) {
                    engine.releaseAllReaders();
                    engine.releaseAllWriters();
                    engine.releaseCrashOrphanedWriters();
                    discardPurgeQueue();
                    recoverAfterCrash(new TableToken[]{token});
                } else {
                    kill(token);
                    engine.notifyWalTxnRepublisher(token);
                    drainWalQueue();
                }
                evictPurgeLogScoreboard();
                Assert.assertFalse("recovery suspended the table", anyTableSuspended(token));
                assertRowsThroughIndex(pass);

                node1.setProperty(PropertyKey.CAIRO_ADAPTIVE_EPOCH_MAX_ROWS, 1);
                execute(passSql(++pass));
                drainWalQueue();
                node1.setProperty(PropertyKey.CAIRO_ADAPTIVE_EPOCH_MAX_ROWS, Long.MAX_VALUE);
                try (
                        PostingSealPurgeJob job = new PostingSealPurgeJob(engine);
                        O3PartitionPurgeJob partitionPurge = new O3PartitionPurgeJob(engine, 1)
                ) {
                    runPurgeJobs(job, partitionPurge);
                }
                assertRowsThroughIndex(pass);
                assertOnePostingGenerationPerPartition(tableDir, "crash " + crash);
            }
        });
    }

    // The count and sum of every symbol through the index, against the rows of passes 0 to lastPass.
    private void assertRowsThroughIndex(int lastPass) throws Exception {
        final long[] count = new long[SYMBOLS];
        final long[] sum = new long[SYMBOLS];
        for (int pass = 0; pass <= lastPass; pass++) {
            for (int i = 0; i < ROWS_PER_PASS; i++) {
                final long v = (long) pass * ROWS_PER_PASS + i;
                count[(int) (v % SYMBOLS)]++;
                sum[(int) (v % SYMBOLS)] += v;
            }
        }
        for (int k = 0; k < SYMBOLS; k++) {
            // No leak check: it clears the engine's queues, which would lose the purge tasks the replay published.
            assertQuery("SELECT count(), sum(v) FROM t WHERE s = '" + (char) ('a' + k) + "'")
                    .noLeakCheck()
                    .noRandomAccess()
                    .expectSize()
                    .returns("count\tsum\n" + count[k] + "\t" + sum[k] + "\n");
        }
    }

    // The save point's rows through the index: one row in each day, v = 0 for symbol a and v = 1 for symbol b.
    private void assertSavePointRowsThroughIndex() throws Exception {
        for (int k = 0; k < 2; k++) {
            assertQuery("SELECT count(), sum(v) FROM t WHERE s = '" + (char) ('a' + k) + "'")
                    .noLeakCheck()
                    .noRandomAccess()
                    .expectSize()
                    .returns("count\tsum\n1\t" + k + "\n");
        }
    }

    // The count and sum of every symbol of both POSTING columns through their index, against the rows of passes 0 to
    // lastPass of the three-day table.
    private void assertThreeDayRowsThroughIndex(String table, int lastPass) throws Exception {
        final long[] count = new long[SYMBOLS + 3];
        final long[] sum = new long[SYMBOLS + 3];
        for (int pass = 0; pass <= lastPass; pass++) {
            for (int i = 0; i < ROWS_PER_PASS; i++) {
                final long v = (long) pass * ROWS_PER_PASS + i;
                count[(int) (v % SYMBOLS)]++;
                sum[(int) (v % SYMBOLS)] += v;
                count[SYMBOLS + (int) (v % 3)]++;
                sum[SYMBOLS + (int) (v % 3)] += v;
            }
        }
        for (int k = 0; k < SYMBOLS + 3; k++) {
            final String filter = k < SYMBOLS ? "s = '" + (char) ('a' + k) + "'" : "k = '" + (char) ('p' + k - SYMBOLS) + "'";
            assertQuery("SELECT count(), sum(v) FROM " + table + " WHERE " + filter)
                    .noLeakCheck()
                    .noRandomAccess()
                    .expectSize()
                    .returns("count\tsum\n" + count[k] + "\t" + sum[k] + "\n");
        }
    }

    // A power loss or a kill after four in-place reseals since the save point. Recovery reads the save point's
    // .pv; the next start's purge job replays the rows the crash left open without keeping readers out; after the
    // next save point the partition holds one posting generation.
    private void crashBeforeNextSavePoint(boolean powerLoss) throws Exception {
        setUpAdaptive();
        runWithCrashFacade(() -> {
            // DEDUP sends every commit through the O3 path, which appends to the partition in place and reseals
            // its covering index there, so each commit supersedes the .pv and covers the previous one sealed.
            execute("CREATE TABLE t (ts TIMESTAMP, s SYMBOL INDEX TYPE POSTING INCLUDE (v), v LONG) TIMESTAMP(ts) "
                    + "PARTITION BY DAY WAL DEDUP UPSERT KEYS(ts, s)");
            execute(passSql(0));
            // The first apply takes the save point the crash recovers from.
            drainWalQueue();
            final TableToken token = engine.verifyTableName("t");
            final String tableDir = Paths.get(root, token.getDirName()).toString();

            final List<String> savePointFiles;
            final boolean[] kept;
            final int held;
            try (PostingSealPurgeJob job = new PostingSealPurgeJob(engine)) {
                // Files superseded inside the first commit are not the save point's; they go at once.
                runPurgeJob(job);
                Assert.assertEquals(0, job.getOutstandingPurgeTasks());
                savePointFiles = sealedFiles(tableDir);
                Assert.assertFalse("the save point has no .pv", savePointFiles.isEmpty());
                kept = new boolean[savePointFiles.size()];

                if (powerLoss) {
                    markDurableBaseline();
                }
                final long epochs = TestUtils.getMetricValue(engine, EPOCH_METRIC);
                int pass = 1;
                for (; pass <= PASSES / 2; pass++) {
                    execute(passSql(pass));
                    drainWalQueue();
                    runPurgeJob(job);
                }
                // A reader up to the crash holds the purge of the next .pv open: a window above the save point.
                try (TableReader ignore = getReader(token)) {
                    for (; pass <= PASSES; pass++) {
                        execute(passSql(pass));
                        drainWalQueue();
                        runPurgeJob(job);
                    }
                    held = job.getOutstandingPurgeTasks();
                }
                Assert.assertEquals("save point after the first apply", epochs, TestUtils.getMetricValue(engine, EPOCH_METRIC));

                final FilesFacade ff = engine.getConfiguration().getFilesFacade();
                for (int i = 0, n = savePointFiles.size(); i < n; i++) {
                    kept[i] = ff.exists(Path.getThreadLocal(savePointFiles.get(i)).$());
                }
                // Make every removal durable, as a journal commit before the power loss would.
                fsyncTableDirs(tableDir);
            }
            // The reseal of the save point's files waits for the pin, the one the reader held waits for the reader,
            // the others went at once.
            Assert.assertEquals(2, held);
            for (int i = 0; i < kept.length; i++) {
                Assert.assertTrue("removed before the next save point: " + savePointFiles.get(i), kept[i]);
            }

            if (powerLoss) {
                engine.releaseAllReaders();
                engine.releaseAllWriters();
                engine.releaseCrashOrphanedWriters();
                discardPurgeQueue();
                recoverAfterCrash(new TableToken[]{token});
                evictPurgeLogScoreboard();
            } else {
                kill(token);
            }
            // The next start's purge job replays the open rows of its log and stays up, as on a server.
            try (PostingSealPurgeJob job = new PostingSealPurgeJob(engine)) {
                if (!powerLoss) {
                    engine.notifyWalTxnRepublisher(token);
                    drainWalQueue();
                }
                Assert.assertFalse("recovery suspended the table", anyTableSuspended(token));
                runPurgeJob(job);
                assertRowsThroughIndex(PASSES);

                // The next save point, then the purge jobs of another start.
                node1.setProperty(PropertyKey.CAIRO_ADAPTIVE_EPOCH_MAX_ROWS, 1);
                execute(passSql(PASSES + 1));
                drainWalQueue();
                node1.setProperty(PropertyKey.CAIRO_ADAPTIVE_EPOCH_MAX_ROWS, Long.MAX_VALUE);
                runPurgeJob(job);
            }
            try (
                    PostingSealPurgeJob job = new PostingSealPurgeJob(engine);
                    O3PartitionPurgeJob partitionPurge = new O3PartitionPurgeJob(engine, 1)
            ) {
                runPurgeJobs(job, partitionPurge);
            }
            assertRowsThroughIndex(PASSES + 1);
            assertOnePostingGenerationPerPartition(tableDir, powerLoss ? "power loss" : "kill");
        });
    }

    // A table with a covering and a plain POSTING column over three days, and its first save point.
    private TableToken createThreeDayTable(String table) throws Exception {
        execute("CREATE TABLE " + table + " (ts TIMESTAMP, s SYMBOL INDEX TYPE POSTING INCLUDE (v), k SYMBOL INDEX TYPE POSTING, "
                + "v LONG) TIMESTAMP(ts) PARTITION BY DAY WAL DEDUP UPSERT KEYS(ts, s)");
        execute(threeDaySql(table, 0));
        // The first apply takes the save point.
        drainWalQueue();
        try (PostingSealPurgeJob job = new PostingSealPurgeJob(engine)) {
            runPurgeJob(job);
        }
        return engine.verifyTableName(table);
    }

    // A kill keeps every byte written but loses the process: its purge queue, scoreboards and sequencer state.
    // The restart recovers every adaptive table before it builds any writer job.
    private void kill(TableToken token) {
        engine.releaseAllReaders();
        engine.releaseAllWriters();
        engine.releaseCrashOrphanedWalWriters();
        engine.releaseAllWalWriters();
        discardPurgeQueue();
        engine.getTxnScoreboardPool().remove(token);
        engine.getTableSequencerAPI().resetForReboot(token);
        new RecoveryCoordinator(engine).recover();
        evictPurgeLogScoreboard();
    }

    // One more pass over the three days, applied with a save point.
    private void savePointPass(String table, int pass) throws Exception {
        node1.setProperty(PropertyKey.CAIRO_ADAPTIVE_EPOCH_MAX_ROWS, 1);
        try {
            execute(threeDaySql(table, pass));
            drainWalQueue();
        } finally {
            node1.setProperty(PropertyKey.CAIRO_ADAPTIVE_EPOCH_MAX_ROWS, Long.MAX_VALUE);
        }
    }

    // Switches the instance mode and checks that the configuration reports it.
    private void setCommitMode(String mode) {
        node1.setProperty(PropertyKey.CAIRO_COMMIT_MODE, mode);
        Assert.assertEquals(mode, CommitMode.toString(engine.getConfiguration().getCommitMode()));
    }
}
