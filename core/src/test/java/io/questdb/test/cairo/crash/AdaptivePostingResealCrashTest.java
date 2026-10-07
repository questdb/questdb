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
import io.questdb.cairo.PostingSealPurgeJob;
import io.questdb.cairo.TableToken;
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
import java.util.regex.Pattern;
import java.util.stream.Stream;

/**
 * A posting-index reseal under ADAPTIVE takes no inline save point, so a superseded {@code .pv} the last save
 * point still references outlives the commit that replaced it until the next periodic save point. A crash before
 * that save point recovers from the last one, whose index still reads that {@code .pv} and its covers. The pin
 * of that save point must keep the purge job from removing them, even where the removal would have reached the
 * disk.
 */
public class AdaptivePostingResealCrashTest extends AbstractAdaptiveCrashTest {
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

    // One WAL commit of ROWS_PER_PASS rows in day 1, from hour 10 + pass, after every earlier row.
    private static String passSql(int pass) {
        final long start = DAY1 + (10L + pass) * HOUR;
        final long step = HOUR / ROWS_PER_PASS;
        final StringBuilder sql = new StringBuilder("INSERT INTO t VALUES ");
        for (int i = 0; i < ROWS_PER_PASS; i++) {
            final long v = (long) pass * ROWS_PER_PASS + i;
            sql.append(i == 0 ? "" : ",").append('(').append(start + i * step).append("::timestamp, '")
                    .append((char) ('a' + v % SYMBOLS)).append("', ").append(v).append(')');
        }
        return sql.toString();
    }

    private static void runPurgeJob(PostingSealPurgeJob job) {
        for (int i = 0; i < 4; i++) {
            Os.sleep(1);
            job.run();
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
}
