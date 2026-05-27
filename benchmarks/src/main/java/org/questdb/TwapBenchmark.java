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

import io.questdb.client.Sender;
import io.questdb.std.Rnd;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import java.util.Properties;
import java.util.TimeZone;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/**
 * End-to-end benchmark for the parallel {@code twap()} aggregate, comparing
 * QuestDB before and after the per-key state was widened from 3 to 7 longs
 * to carry per-frame batch descriptors. The benchmark drives a running
 * QuestDB and reports two outputs per scenario:
 *
 * <ul>
 *   <li><b>Perf</b>: wall-clock time of the TWAP query (mean / median /
 *       max over a small fixed sample).</li>
 *   <li><b>Memory peak</b>: NATIVE_FAST_MAP (per-key map storage) and
 *       NATIVE_GROUP_BY_FUNCTION (entry + descriptor buffers) bytes
 *       captured by a probe thread that polls {@code memory_metrics()}
 *       from a second connection while the query is in flight.</li>
 * </ul>
 *
 * <h2>Why these scenarios</h2>
 *
 * The PR changes two things:
 * <ul>
 *   <li>Per-key state grows by 32 bytes (4 longs). Worst case is high
 *       group cardinality with few observations per key, where the
 *       per-key constant dominates over per-observation storage.</li>
 *   <li>The merge step (compactInPlace at read time, compactInto under
 *       sharded reduction) switches from O(N log R) element-wise
 *       pairwise mergesort to whole-batch permutation. The win grows
 *       with batch count R and entries per batch. R only becomes large
 *       when a slot's buffer receives page frames in non-monotonic order;
 *       within one query that mostly happens at very heavy obs/group with
 *       slot contention, and across queries it happens deterministically
 *       under cross-query work stealing -- the {@code concurrent}
 *       scenario forces this.</li>
 * </ul>
 *
 * <h2>How to run</h2>
 *
 * <ol>
 *   <li>Start a QuestDB server (default config) on localhost. Expects PG
 *       wire on 8812 and HTTP on 9000.</li>
 *   <li>{@code mvn -pl benchmarks -am -Plocal-client package -DskipTests}</li>
 *   <li>{@code java -cp benchmarks/target/benchmarks.jar org.questdb.TwapBenchmark}</li>
 *   <li>Switch branches, restart QuestDB, re-run with {@code -Dskip.populate=true}
 *       to reuse the table. Compare the two runs' TSV output.</li>
 * </ol>
 *
 * <h2>System properties</h2>
 *
 * <ul>
 *   <li>{@code -Dskip.populate=true}: reuse an existing {@code twap_bench}
 *       table. Default is to drop + recreate + ingest.</li>
 *   <li>{@code -Dconcurrent.threads=8}: thread count for the concurrent
 *       scenario. Set higher than worker count to force slot contention.</li>
 *   <li>{@code -Dmeasure.iters=5} / {@code -Dwarmup.iters=2}: control
 *       per-scenario iteration counts.</li>
 *   <li>{@code -Drows=10000000}: total row count. Larger gives more page
 *       frames per slot, more contention.</li>
 * </ul>
 */
public class TwapBenchmark {

    private static final int CONCURRENT_THREADS = intProp("concurrent.threads", 8);
    private static final String HOST = "localhost";
    private static final int HTTP_PORT = 9000;
    // 1M distinct keys -- 10 observations/key at 10M rows. Memory stressor.
    private static final int KEY_HIGH = 1_000_000;
    // 16 distinct keys -- many observations/key, drives the merge step.
    private static final int KEY_LOW = 16;
    // 10K distinct keys -- balanced case.
    private static final int KEY_MED = 10_000;
    private static final int MEASUREMENT_ITERATIONS = intProp("measure.iters", 5);
    // Repetitions during the memory probe phase, so the probe captures
    // a stable peak even for fast queries.
    private static final int MEMORY_PROBE_REPS = intProp("mem.probe.reps", 5);
    private static final int N_ROWS = intProp("rows", 10_000_000);
    private static final int PG_PORT = 8812;
    private static final String TABLE = "twap_bench";
    private static final int WARMUP_ITERATIONS = intProp("warmup.iters", 2);

    public static void main(String[] args) throws Exception {
        TimeZone.setDefault(TimeZone.getTimeZone("UTC"));

        try (Connection conn = pg()) {
            // Auto-skip population when the table already holds the
            // expected row count. Force re-population with
            // -Dforce.populate=true.
            boolean forcePopulate = Boolean.parseBoolean(System.getProperty("force.populate", "false"));
            long existing = forcePopulate ? -1 : existingRowCount(conn);
            if (forcePopulate || existing != N_ROWS) {
                if (existing >= 0) {
                    System.out.printf("Table %s has %,d rows, expected %,d -- repopulating%n",
                            TABLE, existing, N_ROWS);
                }
                recreateTable(conn);
                ingest();
                waitForWalApply(conn);
            } else {
                System.out.printf("Table %s already has %,d rows -- skipping ingestion%n", TABLE, existing);
            }

            String sqlHigh = "SELECT key_high, twap(price, ts) FROM " + TABLE;
            String sqlMed = "SELECT key_med, twap(price, ts) FROM " + TABLE;
            String sqlLow = "SELECT key_low, twap(price, ts) FROM " + TABLE;

            List<Scenario> results = new ArrayList<>();
            // High cardinality first: stresses the per-key state growth.
            // Few obs/key means the merge step is cheap, so any perf
            // overhead the new layout adds shows up cleanly here.
//            results.add(runScenario("1M groups, ~10 obs/group (memory-stress)", sqlHigh, 1));
            // Medium: the typical analytical query shape.
//            results.add(runScenario("10K groups, ~1K obs/group", sqlMed, 1));
            // Low cardinality, many obs/group: maximizes single-query merge work.
//            results.add(runScenario("16 groups, ~625K obs/group (merge-heavy)", sqlLow, 1));
            // Concurrent low cardinality: forces cross-query work stealing.
            // The same slot's buffer sees frames from multiple queries
            // interleaved, generating gaps that materialize as descriptor
            // buffers and -- on the pre-PR code -- expensive element-wise
            // merges across many runs.
            results.add(runScenario(
                    "16 groups x " + CONCURRENT_THREADS + " concurrent (cross-query stealing)",
                    sqlLow, CONCURRENT_THREADS));

            printHumanTable(results);
            printTsv(results);
        }
    }

    private static long elapsedQuery(Connection c, String sql) throws Exception {
        long start = System.nanoTime();
        executeAndDrain(c, sql);
        return System.nanoTime() - start;
    }

    private static int executeAndDrain(Connection c, String sql) throws Exception {
        try (Statement st = c.createStatement(); ResultSet rs = st.executeQuery(sql)) {
            int n = 0;
            while (rs.next()) {
                rs.getDouble(2);
                n++;
            }
            return n;
        }
    }

    // Returns the table's current row count, or -1 if the table is missing.
    // Tolerates a transient WAL backlog: re-reads up to a small budget if
    // the count is below N_ROWS, so a freshly populated table from a prior
    // run isn't misclassified as stale just because WAL apply lags.
    private static long existingRowCount(Connection c) {
        try (Statement st = c.createStatement()) {
            long last = -1;
            for (int attempt = 0; attempt < 5; attempt++) {
                try (ResultSet rs = st.executeQuery("SELECT count() FROM " + TABLE)) {
                    if (rs.next()) {
                        last = rs.getLong(1);
                        if (last >= N_ROWS) {
                            return last;
                        }
                    }
                } catch (Exception missing) {
                    // Table doesn't exist or another transient error.
                    return -1;
                }
                Thread.sleep(200);
            }
            return last;
        } catch (Exception e) {
            return -1;
        }
    }

    private static String formatBytes(long bytes) {
        if (bytes < 1024) return bytes + " B";
        if (bytes < 1024 * 1024) return String.format(Locale.ROOT, "%.1f KB", bytes / 1024.0);
        if (bytes < 1024L * 1024 * 1024) return String.format(Locale.ROOT, "%.1f MB", bytes / (1024.0 * 1024));
        return String.format(Locale.ROOT, "%.2f GB", bytes / (1024.0 * 1024 * 1024));
    }

    private static String formatMillis(long nanos) {
        return String.format(Locale.ROOT, "%,d ms", nanos / 1_000_000);
    }

    private static void ingest() throws Exception {
        System.out.printf("Ingesting %,d rows via ILP (key_low=%d, key_med=%d, key_high=%d distinct)...%n",
                N_ROWS, KEY_LOW, KEY_MED, KEY_HIGH);
        long start = System.currentTimeMillis();
        Rnd rnd = new Rnd(0xC0FFEE, 0xDECAF);
        // Use 1-microsecond steps. With 10M rows that spans 10 seconds of
        // wall time. Plenty of TWAP intervals.
        try (Sender sender = Sender.fromConfig("ws::addr=" + HOST + ":" + HTTP_PORT + ";")) {
            for (int i = 0; i < N_ROWS; i++) {
                sender.table(TABLE)
                        .longColumn("key_low", i % KEY_LOW)
                        .longColumn("key_med", i % KEY_MED)
                        .longColumn("key_high", i % KEY_HIGH)
                        .doubleColumn("price", 100.0 + rnd.nextDouble())
                        .at(1_700_000_000_000_000L + (long) i, ChronoUnit.MICROS);
            }
            sender.flush();
        }
        long elapsed = System.currentTimeMillis() - start;
        System.out.printf("Ingested in %,d ms (%,.0f rows/sec)%n", elapsed, N_ROWS * 1000.0 / Math.max(1, elapsed));
    }

    private static int intProp(String key, int dflt) {
        return Integer.parseInt(System.getProperty(key, Integer.toString(dflt)));
    }

    private static MemoryPeak measureMemoryPeak(String sql, int concurrency) throws Exception {
        try (Connection probe = pg()) {
            MemoryProbe probeThread = new MemoryProbe(probe);
            probeThread.start();
            // Let the probe collect a steady baseline before timing begins.
            Thread.sleep(50);
            probeThread.fixBaseline();
            // Run the query multiple times so the probe (~1ms cadence) has
            // many chances to sample the peak even for sub-10ms queries.
            // The cursor closes and the allocator frees its chunks between
            // iterations, so each iteration is an independent peak.
            for (int rep = 0; rep < MEMORY_PROBE_REPS; rep++) {
                if (concurrency == 1) {
                    try (Connection q = pg()) {
                        executeAndDrain(q, sql);
                    }
                } else {
                    ExecutorService pool = Executors.newFixedThreadPool(concurrency);
                    CountDownLatch done = new CountDownLatch(concurrency);
                    for (int i = 0; i < concurrency; i++) {
                        pool.submit(() -> {
                            try (Connection q = pg()) {
                                executeAndDrain(q, sql);
                            } catch (Exception ex) {
                                ex.printStackTrace();
                            } finally {
                                done.countDown();
                            }
                        });
                    }
                    done.await();
                    pool.shutdown();
                }
            }
            probeThread.requestStop();
            probeThread.join();
            return probeThread.peak();
        }
    }

    private static long percentile(long[] sortedNanos, double p) {
        int idx = (int) Math.min(sortedNanos.length - 1, Math.floor(p * sortedNanos.length));
        return sortedNanos[idx];
    }

    private static Connection pg() throws Exception {
        Properties p = new Properties();
        p.setProperty("user", "admin");
        p.setProperty("password", "quest");
        p.setProperty("sslmode", "disable");
        return DriverManager.getConnection(String.format("jdbc:postgresql://%s:%d/qdb", HOST, PG_PORT), p);
    }

    private static void printHumanTable(List<Scenario> results) {
        System.out.println();
        System.out.println("=== Results ===");
        System.out.printf("%-48s %9s %12s %12s %16s %20s%n",
                "scenario", "mean", "p50", "p90", "NATIVE_FAST_MAP", "NATIVE_GROUP_BY_FN");
        System.out.println(repeat('-', 48 + 9 + 12 * 2 + 16 + 20 + 6));
        for (Scenario r : results) {
            System.out.printf("%-48s %9s %12s %12s %16s %20s%n",
                    r.label,
                    formatMillis(r.meanNanos()),
                    formatMillis(r.p50Nanos()),
                    formatMillis(r.p90Nanos()),
                    formatBytes(r.peak.fastMap),
                    formatBytes(r.peak.groupByFn));
        }
    }

    private static void printTsv(List<Scenario> results) {
        System.out.println();
        System.out.println("=== TSV (paste into diff/spreadsheet) ===");
        System.out.println("scenario\tconcurrency\tmean_ms\tp50_ms\tp90_ms\tmax_ms\tfast_map_bytes\tgroup_by_fn_bytes");
        for (Scenario r : results) {
            System.out.printf(Locale.ROOT, "%s\t%d\t%d\t%d\t%d\t%d\t%d\t%d%n",
                    r.label,
                    r.concurrency,
                    r.meanNanos() / 1_000_000,
                    r.p50Nanos() / 1_000_000,
                    r.p90Nanos() / 1_000_000,
                    r.maxNanos() / 1_000_000,
                    r.peak.fastMap,
                    r.peak.groupByFn);
        }
    }

    private static void recreateTable(Connection c) throws Exception {
        try (Statement st = c.createStatement()) {
            st.execute("DROP TABLE IF EXISTS " + TABLE);
            st.execute("CREATE TABLE " + TABLE + " (" +
                    "key_low LONG, " +
                    "key_med LONG, " +
                    "key_high LONG, " +
                    "price DOUBLE, " +
                    "ts TIMESTAMP" +
                    ") TIMESTAMP(ts) PARTITION BY DAY WAL");
        }
        System.out.println("Recreated table " + TABLE);
    }

    private static String repeat(char c, int n) {
        char[] arr = new char[n];
        Arrays.fill(arr, c);
        return new String(arr);
    }

    private static long runConcurrent(String sql, int concurrency) throws Exception {
        ExecutorService pool = Executors.newFixedThreadPool(concurrency);
        CountDownLatch start = new CountDownLatch(1);
        CountDownLatch done = new CountDownLatch(concurrency);
        long[] perQuery = new long[concurrency];
        for (int t = 0; t < concurrency; t++) {
            final int tt = t;
            pool.submit(() -> {
                try (Connection c = pg()) {
                    start.await();
                    perQuery[tt] = elapsedQuery(c, sql);
                } catch (Exception e) {
                    e.printStackTrace();
                } finally {
                    done.countDown();
                }
            });
        }
        long batchStart = System.nanoTime();
        start.countDown();
        done.await();
        long batchEnd = System.nanoTime();
        pool.shutdown();
        // Use wall-clock batch duration -- that's the latency a user
        // sees when issuing K parallel queries.
        return batchEnd - batchStart;
    }

    private static Scenario runScenario(String label, String sql, int concurrency) throws Exception {
        System.out.println();
        System.out.println("Running scenario: " + label);
        // Warm up the executor and JIT and let any caches warm up.
        for (int i = 0; i < WARMUP_ITERATIONS; i++) {
            if (concurrency == 1) {
                try (Connection c = pg()) {
                    elapsedQuery(c, sql);
                }
            } else {
                // Single warmup of the parallel-load case is enough; we
                // care about the steady state.
                MemoryPeak ignored = measureMemoryPeak(sql, concurrency);
            }
        }

        // Timing phase: no memory probe to avoid the polling thread
        // disturbing the measurement.
        long[] elapsed = new long[MEASUREMENT_ITERATIONS];
        if (concurrency == 1) {
            try (Connection c = pg()) {
                for (int i = 0; i < MEASUREMENT_ITERATIONS; i++) {
                    elapsed[i] = elapsedQuery(c, sql);
                }
            }
        } else {
            // For concurrent scenarios, one iteration = one batch of K
            // simultaneous queries. The reported time is the maximum
            // per-query latency in the batch -- that's the wall-clock
            // until the batch is done, and what a user would feel as
            // "the query was slow".
            for (int i = 0; i < MEASUREMENT_ITERATIONS; i++) {
                elapsed[i] = runConcurrent(sql, concurrency);
            }
        }
        Arrays.sort(elapsed);

        // Memory probe: separate run with the probe thread active.
        // Captures the peak NATIVE_FAST_MAP and NATIVE_GROUP_BY_FUNCTION
        // delta from idle baseline.
        MemoryPeak peak = measureMemoryPeak(sql, concurrency);

        Scenario sc = new Scenario();
        sc.label = label;
        sc.concurrency = concurrency;
        sc.elapsedNanos = elapsed;
        sc.peak = peak;
        return sc;
    }

    private static void waitForWalApply(Connection c) throws Exception {
        try (Statement st = c.createStatement()) {
            for (int attempt = 0; attempt < 600; attempt++) {
                try (ResultSet rs = st.executeQuery("SELECT count() FROM " + TABLE)) {
                    if (rs.next() && rs.getLong(1) >= N_ROWS) {
                        return;
                    }
                }
                Thread.sleep(200);
            }
            throw new AssertionError("timed out waiting for WAL apply to reach " + N_ROWS + " rows");
        }
    }

    private static final class MemoryPeak {
        long fastMap;
        long groupByFn;
        long totalUsed;
    }

    private static final class MemoryProbe extends Thread {
        private final Connection conn;
        private long baseFM;
        private long baseGB;
        private long baseTotal;
        private long maxFM;
        private long maxGB;
        private long maxTotal;
        private volatile boolean stop;

        MemoryProbe(Connection conn) throws Exception {
            setDaemon(true);
            setName("twap-memory-probe");
            this.conn = conn;
            long[] mm = readMetrics();
            baseFM = mm[0];
            baseGB = mm[1];
            baseTotal = mm[2];
        }

        @Override
        public void run() {
            try {
                while (!stop) {
                    long[] mm = readMetrics();
                    long fm = mm[0] - baseFM;
                    long gb = mm[1] - baseGB;
                    long total = mm[2] - baseTotal;
                    if (fm > maxFM) maxFM = fm;
                    if (gb > maxGB) maxGB = gb;
                    if (total > maxTotal) maxTotal = total;
                    Thread.sleep(1);
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            } catch (Exception e) {
                // Likely the connection was closed under our feet
                // during teardown. Stop silently.
            }
        }

        private long[] readMetrics() throws Exception {
            long fm = -1, gb = -1, total = -1;
            try (PreparedStatement ps = conn.prepareStatement(
                    "SELECT memory_tag, bytes FROM memory_metrics() " +
                            "WHERE memory_tag IN ('NATIVE_FAST_MAP', 'NATIVE_GROUP_BY_FUNCTION', 'TOTAL_USED')");
                 ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    String tag = rs.getString(1);
                    long bytes = rs.getLong(2);
                    if ("NATIVE_FAST_MAP".equals(tag)) fm = bytes;
                    else if ("NATIVE_GROUP_BY_FUNCTION".equals(tag)) gb = bytes;
                    else if ("TOTAL_USED".equals(tag)) total = bytes;
                }
            }
            return new long[]{fm, gb, total};
        }

        void fixBaseline() throws Exception {
            long[] mm = readMetrics();
            baseFM = mm[0];
            baseGB = mm[1];
            baseTotal = mm[2];
        }

        MemoryPeak peak() {
            MemoryPeak p = new MemoryPeak();
            p.fastMap = maxFM;
            p.groupByFn = maxGB;
            p.totalUsed = maxTotal;
            return p;
        }

        void requestStop() {
            stop = true;
        }
    }

    private static final class Scenario {
        int concurrency;
        long[] elapsedNanos;
        String label;
        MemoryPeak peak;

        long maxNanos() {
            return elapsedNanos[elapsedNanos.length - 1];
        }

        long meanNanos() {
            long sum = 0;
            for (long e : elapsedNanos) sum += e;
            return sum / elapsedNanos.length;
        }

        long p50Nanos() {
            return percentile(elapsedNanos, 0.5);
        }

        long p90Nanos() {
            return percentile(elapsedNanos, 0.9);
        }
    }
}
