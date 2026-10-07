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
import io.questdb.cairo.O3PartitionPurgeJob;
import io.questdb.cairo.PostingSealPurgeJob;
import io.questdb.cairo.RecoveryCoordinator;
import io.questdb.cairo.TableReader;
import io.questdb.cairo.TableToken;
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
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.regex.Pattern;
import java.util.stream.Stream;

/**
 * A posting-index reseal under ADAPTIVE takes no inline save point, so a superseded {@code .pv} the last save
 * point still references outlives the commit that replaced it until the next periodic save point. A crash before
 * that save point recovers from the last one, whose index still reads that {@code .pv} and its covers. The pin
 * of that save point must keep the purge job from removing them, even where the removal would have reached the
 * disk. The purge rows a crash leaves open are replayed by the next start's purge job, and must not keep readers
 * out of the recovered table.
 */
public class AdaptivePostingResealCrashTest extends AbstractAdaptiveCrashTest {
    private static final long DAY = 86_400_000_000L;
    private static final long DAY1 = 1_704_067_200_000_000L; // 2024-01-01T00:00:00Z
    private static final String EPOCH_METRIC = "questdb_wal_adaptive_epoch_advances_total";
    private static final long HOUR = 3_600_000_000L;
    private static final int PASSES = 4;
    private static final int ROWS_PER_PASS = 10;
    private static final Pattern SEALED_FILE = Pattern.compile("s\\.(pv|pc\\d+)\\..*");
    private static final int SYMBOLS = 4;

    @Test
    public void testCrashBeforeNextSavePointRecoversFromLastSavePoint() throws Exception {
        setProperty(PropertyKey.CAIRO_COMMIT_MODE, "adaptive");
        setProperty(PropertyKey.CAIRO_ADAPTIVE_EPOCH_INTERVAL, 1_000_000_000L);
        setProperty(PropertyKey.CAIRO_ADAPTIVE_EPOCH_MAX_ROWS, Long.MAX_VALUE);
        // A power loss takes no save point at writer close, so the crash below must not get one either.
        setProperty(PropertyKey.CAIRO_ADAPTIVE_EPOCH_FLUSH_ON_CLOSE, "false");
        // Retry a held purge on every job run, so the job would remove the file as soon as nothing held it.
        setProperty(PropertyKey.CAIRO_SQL_COLUMN_PURGE_RETRY_DELAY, 1);
        setProperty(PropertyKey.CAIRO_SQL_COLUMN_PURGE_RETRY_DELAY_LIMIT, 1);
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

                markDurableBaseline();
                final long epochs = TestUtils.getMetricValue(engine, EPOCH_METRIC);
                for (int pass = 1; pass <= PASSES; pass++) {
                    execute(passSql(pass));
                    drainWalQueue();
                    runPurgeJob(job);
                }
                Assert.assertEquals("save point after the first apply", epochs, TestUtils.getMetricValue(engine, EPOCH_METRIC));
                held = job.getOutstandingPurgeTasks();

                final FilesFacade ff = engine.getConfiguration().getFilesFacade();
                for (int i = 0, n = savePointFiles.size(); i < n; i++) {
                    final String file = savePointFiles.get(i);
                    kept[i] = ff.exists(Path.getThreadLocal(file).$());
                    // Make a removal durable, as a journal commit before the power loss would.
                    final long dirFd = ff.openRONoCache(Path.getThreadLocal(Paths.get(file).getParent().toString()).$());
                    Assert.assertTrue(dirFd > -1);
                    ff.fsyncAndClose(dirFd);
                }
            }

            recoverAfterCrash(new TableToken[]{token});
            Assert.assertFalse("recovery suspended the table", anyTableSuspended(token));
            final int rows = ROWS_PER_PASS * (PASSES + 1);
            assertQuery("SELECT count() FROM t").noRandomAccess().expectSize().returns("count\n" + rows + "\n");
            for (int k = 0; k < SYMBOLS; k++) {
                long sum = 0;
                for (int v = k; v < rows; v += SYMBOLS) {
                    sum += v;
                }
                assertQuery("SELECT count(), sum(v) FROM t WHERE s = '" + (char) ('a' + k) + "'")
                        .noRandomAccess()
                        .expectSize()
                        .returns("count\tsum\n" + (rows + SYMBOLS - 1 - k) / SYMBOLS + "\t" + sum + "\n");
            }
            // Only the reseal of the save point's files waits; the later ones went at once.
            Assert.assertEquals(1, held);
            for (int i = 0; i < kept.length; i++) {
                Assert.assertTrue("removed before the next save point: " + savePointFiles.get(i), kept[i]);
            }
        });
    }

    @Test
    public void testKillsLeaveOnePostingGeneration() throws Exception {
        assertOnePostingGenerationAfterCrashes(false);
    }

    @Test
    public void testPowerLossesLeaveOnePostingGeneration() throws Exception {
        assertOnePostingGenerationAfterCrashes(true);
    }

    @Test
    public void testPurgeLogReplayAfterKillKeepsReadersOpen() throws Exception {
        setUpAdaptive();
        runWithCrashFacade(() -> {
            execute("CREATE TABLE t (ts TIMESTAMP, s SYMBOL INDEX TYPE POSTING INCLUDE (v), v LONG) TIMESTAMP(ts) "
                    + "PARTITION BY DAY WAL DEDUP UPSERT KEYS(ts, s)");
            execute("INSERT INTO t VALUES " + row(DAY1 + 10 * HOUR, 0) + ", " + row(DAY1 + DAY + 10 * HOUR, 1));
            // The first apply takes the save point the kill recovers from.
            drainWalQueue();
            final TableToken token = engine.verifyTableName("t");
            try (PostingSealPurgeJob job = new PostingSealPurgeJob(engine)) {
                runPurgeJob(job);
                // Each in-place commit reseals its partition. The first reseal of each day supersedes a .pv the
                // save point names, so the pin holds its purge row open.
                insertAndApply(row(DAY1 + 11 * HOUR, 2), job);
                insertAndApply(row(DAY1 + DAY + 11 * HOUR, 3), job);
                // A reader up to the kill holds open the purges of the next .pv of each day: windows above the
                // save point.
                try (TableReader ignore = getReader(token)) {
                    insertAndApply(row(DAY1 + 12 * HOUR, 4), job);
                    insertAndApply(row(DAY1 + 13 * HOUR, 5), job);
                    insertAndApply(row(DAY1 + DAY + 12 * HOUR, 6), job);
                    Assert.assertEquals("purge rows open at the kill", 4, job.getOutstandingPurgeTasks());
                }
            }
            kill(token);
            // A restart builds the purge job, which replays the open rows of its log, before any WAL apply.
            try (PostingSealPurgeJob job = new PostingSealPurgeJob(engine)) {
                engine.notifyWalTxnRepublisher(token);
                drainWalQueue();
                runPurgeJob(job);
                // The job stays up, as on a server, and keeps its writer of the purge log.
                assertQuery("SELECT count(), sum(v) FROM t").noLeakCheck().noRandomAccess().expectSize().returns("count\tsum\n7\t21\n");
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

    // Every partition directory holds one .pv of column s, and every cover file of s belongs to it.
    private static void assertOnePostingGenerationPerPartition(String tableDir, String where) {
        final Map<String, List<String>> byPartition = new TreeMap<>();
        final List<String> files = sealedFiles(tableDir);
        for (int i = 0, n = files.size(); i < n; i++) {
            final java.nio.file.Path file = Paths.get(files.get(i));
            byPartition.computeIfAbsent(file.getParent().getFileName().toString(), k -> new ArrayList<>()).add(file.getFileName().toString());
        }
        Assert.assertFalse(where + ": no sealed files", byPartition.isEmpty());
        for (Map.Entry<String, List<String>> e : byPartition.entrySet()) {
            String sealTxn = null;
            for (String name : e.getValue()) {
                if (name.startsWith("s.pv.")) {
                    Assert.assertNull(where + ": more than one .pv in " + e.getKey() + ": " + e.getValue(), sealTxn);
                    sealTxn = name.substring(name.lastIndexOf('.'));
                }
            }
            Assert.assertNotNull(where + ": no .pv in " + e.getKey() + ": " + e.getValue(), sealTxn);
            for (String name : e.getValue()) {
                Assert.assertTrue(where + ": a cover of another generation in " + e.getKey() + ": " + e.getValue(), name.endsWith(sealTxn));
            }
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

    private static void insertAndApply(String values, PostingSealPurgeJob job) throws Exception {
        execute("INSERT INTO t VALUES " + values);
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
        final List<String> files = new ArrayList<>();
        try (Stream<java.nio.file.Path> paths = Files.walk(Paths.get(tableDir))) {
            paths.filter(p -> SEALED_FILE.matcher(p.getFileName().toString()).matches()).forEach(p -> files.add(p.toString()));
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        return files;
    }

    private static void setUpAdaptive() {
        setProperty(PropertyKey.CAIRO_COMMIT_MODE, "adaptive");
        setProperty(PropertyKey.CAIRO_ADAPTIVE_EPOCH_INTERVAL, 1_000_000_000L);
        setProperty(PropertyKey.CAIRO_ADAPTIVE_EPOCH_MAX_ROWS, Long.MAX_VALUE);
        // A crash takes no save point at writer close.
        setProperty(PropertyKey.CAIRO_ADAPTIVE_EPOCH_FLUSH_ON_CLOSE, "false");
        // Retry a held purge on every job run, so the job would remove the file as soon as nothing held it.
        setProperty(PropertyKey.CAIRO_SQL_COLUMN_PURGE_RETRY_DELAY, 1);
        setProperty(PropertyKey.CAIRO_SQL_COLUMN_PURGE_RETRY_DELAY_LIMIT, 1);
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
}
