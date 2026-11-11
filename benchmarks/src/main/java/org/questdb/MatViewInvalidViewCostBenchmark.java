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

package org.questdb;

import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.CairoEngine;
import io.questdb.cairo.DefaultCairoConfiguration;
import io.questdb.cairo.TableToken;
import io.questdb.cairo.mv.MatViewRefreshJob;
import io.questdb.cairo.mv.MatViewTimerJob;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.cairo.wal.ApplyWal2TableJob;
import io.questdb.cairo.wal.CheckWalTransactionsJob;
import io.questdb.cairo.wal.WalPurgeJob;
import io.questdb.griffin.SqlCompiler;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.SqlExecutionContextImpl;
import io.questdb.log.LogFactory;
import io.questdb.std.datetime.MicrosecondClock;

import java.io.File;
import java.io.IOException;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;

/**
 * Measures what an invalid IMMEDIATE materialized view costs while its base table stays busy: the view WAL
 * transactions it writes and the base table WAL it keeps from purging. An invalid view keeps a backlog of the base
 * changes it misses, which a timer brings up to date once per {@code cairo.mat.view.refresh.intervals.update.period}
 * (15 s by default) with one view WAL transaction, and the WAL purge keeps the base WAL from the backlog's position on.
 * A build without the backlog writes nothing for an invalid view and keeps no base WAL for it.
 * <p>
 * The harness simulates an hour on a clock it moves by one second per step. Each step commits
 * {@link #ROWS_PER_COMMIT} rows to the base table, every commit in a WAL segment of its own, applies the WAL, runs the
 * refresh and timer jobs, then the WAL purge, and counts the base WAL segments left on disk: the base WAL that the view
 * keeps from purging, plus the segment the base table's WAL writer has open. It reports the view WAL transactions
 * written over the hour and the base WAL segments the purges left, for an invalid view and, as the reference, a valid
 * one, which refreshes after every base commit.
 * <p>
 * A standalone harness with a {@code main()}, not a JMH benchmark; run it with
 * {@code java -cp benchmarks/target/benchmarks.jar org.questdb.MatViewInvalidViewCostBenchmark}.
 */
public class MatViewInvalidViewCostBenchmark {
    // the WAL purge runs after every commit, so what it leaves is the base WAL that the views keep from purging
    private static final long PURGE_INTERVAL_SECONDS = 1;
    private static final int ROWS_PER_COMMIT = 10;
    private static final long SECOND_MICROS = 1_000_000L;
    private static final long SIMULATED_SECONDS = 3_600;
    // 2024-01-01T00:00:00Z
    private static final long START_MICROS = 1_704_067_200_000_000L;

    public static void main(String[] args) throws Exception {
        System.out.printf("%-10s %22s %22s %22s %16s%n", "view", "view WAL txns per hour", "max base WAL segments", "avg base WAL segments", "base commits");
        runScenario(false);
        runScenario(true);
        LogFactory.haltInstance();
    }

    private static long countBaseWalSegments(CairoConfiguration configuration, TableToken baseToken) {
        long segments = 0;
        final File[] walDirs = new File(configuration.getDbRoot(), baseToken.getDirName()).listFiles((dir, name) -> name.startsWith("wal"));
        if (walDirs != null) {
            for (File walDir : walDirs) {
                final File[] segmentDirs = walDir.listFiles(f -> f.isDirectory() && f.getName().chars().allMatch(Character::isDigit));
                if (segmentDirs != null) {
                    segments += segmentDirs.length;
                }
            }
        }
        return segments;
    }

    private static void deleteRecursively(Path dir) throws IOException {
        if (!Files.exists(dir)) {
            return;
        }
        Files.walkFileTree(dir, new SimpleFileVisitor<>() {
            @Override
            public FileVisitResult postVisitDirectory(Path d, IOException exc) throws IOException {
                Files.delete(d);
                return FileVisitResult.CONTINUE;
            }

            @Override
            public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) throws IOException {
                Files.delete(file);
                return FileVisitResult.CONTINUE;
            }
        });
    }

    private static void drainWal(CairoEngine engine, ApplyWal2TableJob walApplyJob) {
        //noinspection StatementWithEmptyBody
        while (walApplyJob.run()) ;
        if (new CheckWalTransactionsJob(engine).run()) {
            //noinspection StatementWithEmptyBody
            while (walApplyJob.run()) ;
        }
    }

    private static void insertCommit(CairoEngine engine, SqlExecutionContext context, long nowMicros) throws SqlException {
        final StringBuilder sql = new StringBuilder("INSERT INTO trades VALUES ");
        for (int i = 0; i < ROWS_PER_COMMIT; i++) {
            if (i > 0) {
                sql.append(", ");
            }
            sql.append("('s").append(i % 4).append("', ").append(100 + i).append(".5, ").append(nowMicros + i).append("::timestamp)");
        }
        engine.execute(sql, context);
    }

    // Simulates the hour on the engine, which is loaded, and prints one line of results.
    private static void measure(CairoEngine engine, CairoConfiguration configuration, ManualClock clock, boolean isInvalid) throws Exception {
        try (
                SqlExecutionContext context = new SqlExecutionContextImpl(engine, 1).with(
                        configuration.getFactoryProvider().getSecurityContextFactory().getRootContext(),
                        null,
                        null,
                        -1,
                        null
                );
                SqlCompiler compiler = engine.getSqlCompiler();
                ApplyWal2TableJob walApplyJob = new ApplyWal2TableJob(engine, 0);
                MatViewRefreshJob refreshJob = new MatViewRefreshJob(0, engine, 1)
        ) {
            final MatViewTimerJob timerJob = new MatViewTimerJob(engine);
            engine.execute("CREATE TABLE trades (sym SYMBOL, price DOUBLE, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL", context);
            engine.execute("CREATE MATERIALIZED VIEW mv AS (SELECT ts, sym, last(price) AS price FROM trades SAMPLE BY 1m) PARTITION BY DAY", context);
            insertCommit(engine, context, clock.ticks);
            runJobs(engine, walApplyJob, refreshJob, timerJob);
            final TableToken baseToken = engine.verifyTableName("trades");
            final TableToken viewToken = engine.verifyTableName("mv");
            if (isInvalid) {
                engine.getMatViewStateStore().enqueueInvalidate(viewToken, "benchmark");
                runJobs(engine, walApplyJob, refreshJob, timerJob);
                if (!engine.getMatViewStateStore().getViewState(viewToken).isInvalid()) {
                    throw new IllegalStateException("the view did not go invalid");
                }
            }
            final String viewTxnSql = "SELECT sequencerTxn FROM wal_tables() WHERE name = 'mv'";
            final long viewTxnBefore = readLong(compiler, context, viewTxnSql);

            long maxSegments = 0;
            long totalSegments = 0;
            long purges = 0;
            for (long second = 1; second <= SIMULATED_SECONDS; second++) {
                clock.ticks += SECOND_MICROS;
                insertCommit(engine, context, clock.ticks);
                runJobs(engine, walApplyJob, refreshJob, timerJob);
                if (second % PURGE_INTERVAL_SECONDS == 0) {
                    // a new job each time: the job throttles its own runs to the purge interval
                    try (WalPurgeJob purgeJob = new WalPurgeJob(engine)) {
                        purgeJob.drain(0);
                    }
                    final long segments = countBaseWalSegments(configuration, baseToken);
                    maxSegments = Math.max(maxSegments, segments);
                    totalSegments += segments;
                    purges++;
                }
            }
            if (isInvalid != engine.getMatViewStateStore().getViewState(viewToken).isInvalid()) {
                throw new IllegalStateException("the view changed its validity");
            }
            final long viewTxns = readLong(compiler, context, viewTxnSql) - viewTxnBefore;
            System.out.printf("%-10s %22d %22d %22.1f %16d%n", isInvalid ? "invalid" : "valid", viewTxns, maxSegments, (double) totalSegments / purges, SIMULATED_SECONDS);
        }
    }

    private static long readLong(SqlCompiler compiler, SqlExecutionContext context, String sql) throws SqlException {
        try (
                RecordCursorFactory factory = compiler.compile(sql, context).getRecordCursorFactory();
                RecordCursor cursor = factory.getCursor(context)
        ) {
            return cursor.hasNext() ? cursor.getRecord().getLong(0) : -1;
        }
    }

    private static void runJobs(CairoEngine engine, ApplyWal2TableJob walApplyJob, MatViewRefreshJob refreshJob, MatViewTimerJob timerJob) {
        drainWal(engine, walApplyJob);
        //noinspection StatementWithEmptyBody
        while (refreshJob.run()) ;
        //noinspection StatementWithEmptyBody
        while (timerJob.run()) ;
        //noinspection StatementWithEmptyBody
        while (refreshJob.run()) ;
        drainWal(engine, walApplyJob);
    }

    private static void runScenario(boolean isInvalid) throws Exception {
        final Path dbRoot = Files.createTempDirectory("mvInvalidCost-");
        try {
            final ManualClock clock = new ManualClock(START_MICROS);
            final CairoConfiguration configuration = new DefaultCairoConfiguration(dbRoot.toString()) {
                @Override
                public MicrosecondClock getMicrosecondClock() {
                    return clock;
                }

                @Override
                public long getWalSegmentRolloverRowCount() {
                    // a WAL segment per commit, so the purge can remove each commit's WAL on its own
                    return ROWS_PER_COMMIT;
                }
            };
            try (CairoEngine engine = new CairoEngine(configuration)) {
                // swaps the no-op mat view state store for the real one, which the jobs below take
                engine.load();
                measure(engine, configuration, clock, isInvalid);
            }
        } finally {
            deleteRecursively(dbRoot);
        }
    }

    private static final class ManualClock implements MicrosecondClock {
        private long ticks;

        private ManualClock(long ticks) {
            this.ticks = ticks;
        }

        @Override
        public long getTicks() {
            return ticks;
        }
    }
}
