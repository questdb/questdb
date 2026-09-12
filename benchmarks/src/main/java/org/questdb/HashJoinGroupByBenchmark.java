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

import io.questdb.cairo.CairoEngine;
import io.questdb.cairo.DefaultCairoConfiguration;
import io.questdb.cairo.security.AllowAllSecurityContext;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.SqlExecutionContextImpl;
import io.questdb.griffin.TextPlanSink;
import io.questdb.griffin.engine.table.AsyncHashJoinGroupByRecordCursorFactory;
import io.questdb.griffin.engine.table.HashJoinGroupByMetrics;
import io.questdb.mp.WorkerPool;
import io.questdb.mp.WorkerPoolConfiguration;
import io.questdb.mp.WorkerPoolUtils;
import io.questdb.std.MemoryTag;
import io.questdb.std.Os;
import io.questdb.std.Unsafe;

import java.lang.management.ManagementFactory;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.locks.LockSupport;

/** Seeded, end-to-end RFC 130 baseline and same-build comparison runner. */
public class HashJoinGroupByBenchmark {
    public static final String SQL = """
            SELECT p.country, year(r.reading_ts) AS yr, month(r.reading_ts) AS mo,
                   sum(r.energy_kwh) AS total_energy_kwh,
                   avg(r.irradiance_wm2) AS avg_irradiance,
                   sum(r.energy_kwh) / nullif(sum(p.installed_kwp), 0) AS specific_yield_kwh_kwp
            FROM fact_solar_readings r
            INNER JOIN dim_plant p ON r.plant_id = p.plant_id
            WHERE r.reading_ts >= '2020-01-01' AND r.reading_ts < '2025-01-01'
              AND p.country IN ('ES','IT')
            GROUP BY p.country, year(r.reading_ts), month(r.reading_ts)
            ORDER BY p.country, yr, mo
            """;

    public static void main(String[] args) throws Exception {
        Map<String, String> options = options(args);
        long rows = number(options, "rows", 100_000_000, 1, 1_000_000_000);
        int plants = (int) number(options, "plants", 100_000, 100, 10_000_000);
        int selectedPercent = (int) number(options, "selected-percent", 10, 0, 100);
        int fanout = (int) number(options, "fanout", 1, 1, 1000);
        long seed = number(options, "seed", 130, 1, Integer.MAX_VALUE);
        int workers = (int) number(options, "workers", 4, 1, 64);
        int warmups = (int) number(options, "warmups", 3, 1, 100);
        int runs = (int) number(options, "runs", 10, 10, 1000);
        int repetitions = (int) number(options, "repetitions", 2, 1, 100);
        CandidateCompiler candidate = options.containsKey("candidate-compiler")
                ? (CandidateCompiler) Class.forName(options.get("candidate-compiler")).getConstructor().newInstance() : null;
        Os.init();
        // A fresh directory prevents accidentally overwriting a developer's database.
        Path root = Files.createTempDirectory("hash-join-group-by-");
        System.out.println("# data_directory=" + root + " (retained for inspection)");
        System.out.printf(Locale.ROOT, "# revision=%s java=%s vm=%s os=%s arch=%s processors=%d heap_max=%d vm_args=%s%n",
                options.getOrDefault("revision", "unspecified"), System.getProperty("java.version"), System.getProperty("java.vm.name"),
                System.getProperty("os.name"), System.getProperty("os.arch"), Runtime.getRuntime().availableProcessors(),
                Runtime.getRuntime().maxMemory(), ManagementFactory.getRuntimeMXBean().getInputArguments());
        System.out.printf(Locale.ROOT, "# rows=%d plants=%d selected_percent=%d fanout=%d seed=%d workers=%d warmups=%d runs=%d repetitions=%d storage=warm-native%n",
                rows, plants, selectedPercent, fanout, seed, workers, warmups, runs, repetitions);
        System.out.println("# SQL\n" + SQL);
        WorkerPool pool = new WorkerPool(new WorkerPoolConfiguration() {
            @Override
            public String getPoolName() {
                return "hash-join-group-by-benchmark";
            }

            @Override
            public int getWorkerCount() {
                return workers;
            }
        });
        try (CairoEngine engine = new CairoEngine(new DefaultCairoConfiguration(root.toString()))) {
            engine.load();
            WorkerPoolUtils.setupQueryJobs(pool, engine);
            pool.start();
            try (SqlExecutionContextImpl context = new SqlExecutionContextImpl(engine, workers) {
                @Override
                public boolean shouldLogSql() {
                    return false;
                }
            }.with(AllowAllSecurityContext.INSTANCE, null, null, -1, null)) {
                generate(engine, context, rows, plants, selectedPercent, fanout, seed);
                try (
                        RecordCursorFactory baseline = engine.select(SQL, context);
                        RecordCursorFactory fused = candidate == null ? null : candidate.compile(engine, context, SQL)
                ) {
                    printPlan("baseline", baseline, context, false);
                    if (fused != null) {
                        printPlan("candidate", fused, context, true);
                    }
                    RecordCursorFactory[] factories = fused == null ? new RecordCursorFactory[]{baseline} : new RecordCursorFactory[]{baseline, fused};
                    List<ResultRow> expected = null;
                    System.out.println("arm,repetition,run,elapsed_ns,groups,sampled_native_peak_delta_bytes,retained_native_delta_bytes,build_rows,build_keys,build_bytes,scanned_rows,matched_pairs,null_extended_rows,surviving_rows,merge_cardinality,build_ns,init_ns,probe_ns,merge_ns");
                    for (int repetition = 0; repetition < repetitions; repetition++) {
                        List<List<Long>> elapsed = new ArrayList<>();
                        for (int arm = 0; arm < factories.length; arm++) {
                            elapsed.add(new ArrayList<>());
                        }
                        for (int run = -warmups; run < runs; run++) {
                            for (int turn = 0; turn < factories.length; turn++) {
                                int arm = ((run + warmups + repetition) & 1) == 0 ? turn : factories.length - turn - 1;
                                Sample sample = execute(factories[arm], context);
                                if (expected == null) {
                                    expected = sample.result;
                                    System.out.println("# result=" + expected);
                                } else {
                                    assertResults(expected, sample.result);
                                }
                                if (run >= 0) {
                                    elapsed.get(arm).add(sample.nanos);
                                    System.out.printf(Locale.ROOT, "%s,%d,%d,%d,%d,%d,%d,%s%n", arm == 0 ? "baseline" : "candidate",
                                            repetition, run, sample.nanos, sample.result.size(), sample.peak, sample.retained, sample.metrics);
                                }
                            }
                        }
                        double baselineMedian = 0;
                        for (int arm = 0; arm < factories.length; arm++) {
                            long[] times = elapsed.get(arm).stream().mapToLong(Long::longValue).sorted().toArray();
                            double median = (times[(times.length - 1) / 2] + (double) times[times.length / 2]) / 2;
                            if (arm == 0) {
                                baselineMedian = median;
                            }
                            System.out.printf(Locale.ROOT, "# %s repetition=%d median_ms=%.3f min_ms=%.3f max_ms=%.3f %s%n",
                                    arm == 0 ? "baseline" : "candidate", repetition, median / 1e6, times[0] / 1e6,
                                    times[times.length - 1] / 1e6, arm == 0 ? "" : String.format(Locale.ROOT, "speedup=%.3f", baselineMedian / median));
                        }
                    }
                }
            } finally {
                pool.halt();
            }
        }
    }

    private static void assertResults(List<ResultRow> expected, List<ResultRow> actual) {
        if (expected.size() != actual.size()) {
            throw new IllegalStateException("group count differs: " + expected.size() + " vs " + actual.size());
        }
        for (int i = 0; i < expected.size(); i++) {
            ResultRow a = expected.get(i);
            ResultRow b = actual.get(i);
            if (!a.country.equals(b.country) || a.year != b.year || a.month != b.month
                    || !equal(a.energy, b.energy) || !equal(a.irradiance, b.irradiance) || !equal(a.yield, b.yield)) {
                throw new IllegalStateException("result differs at " + i + ": " + a + " vs " + b);
            }
        }
    }

    private static boolean equal(double a, double b) {
        return Double.doubleToLongBits(a) == Double.doubleToLongBits(b)
                || (Double.isFinite(a) && Double.isFinite(b) && Math.abs(a - b) <= 1e-10 * Math.max(1, Math.abs(a)));
    }

    private static Sample execute(RecordCursorFactory factory, SqlExecutionContext context) throws Exception {
        List<ResultRow> result = new ArrayList<>(120);
        // Start before acquisition: eager build/sort work inside getCursor must be measured too.
        try (NativeSampler sampler = new NativeSampler()) {
            long start = System.nanoTime();
            try (RecordCursor cursor = factory.getCursor(context)) {
                Record record = cursor.getRecord();
                while (cursor.hasNext()) {
                    result.add(new ResultRow(record.getSymA(0).toString(), record.getInt(1), record.getInt(2),
                            record.getDouble(3), record.getDouble(4), record.getDouble(5)));
                }
                long nanos = System.nanoTime() - start;
                long retained = nativeBytes() - sampler.initial;
                sampler.sample();
                return new Sample(nanos, result, sampler.peak - sampler.initial, retained, metrics(factory));
            }
        }
    }

    private static String metrics(RecordCursorFactory factory) {
        while (factory != null && !(factory instanceof AsyncHashJoinGroupByRecordCursorFactory)) {
            factory = factory.getBaseFactory();
        }
        if (factory == null) {
            return ",,,,,,,,,,,";
        }
        HashJoinGroupByMetrics m = ((AsyncHashJoinGroupByRecordCursorFactory) factory).getMetrics();
        return m.getBuildRows() + "," + m.getBuildKeys() + "," + m.getBuildBytes() + "," + m.getScannedRows()
                + "," + m.getMatchedPairs() + "," + m.getNullExtendedRows() + "," + m.getSurvivingRows()
                + "," + m.getMergeCardinality() + "," + m.getBuildNanos() + "," + m.getInitNanos()
                + "," + m.getProbeNanos() + "," + m.getMergeNanos();
    }

    private static void generate(CairoEngine engine, SqlExecutionContext context, long rows, int plants, int selectedPercent, int fanout, long seed) throws Exception {
        String dimension = "create table dim_plant as (select cast((x-1)/" + fanout + " as int) plant_id, "
                + "cast(case when ((x-1)/" + fanout + ")%100 < " + selectedPercent
                + " then case when ((x-1)/" + fanout + ")%2=0 then 'ES' else 'IT' end else 'DE' end as symbol) country, "
                + "rnd_int(1, 800, 0)/8.0 installed_kwp from long_sequence(" + ((long) plants * fanout) + "," + seed + "," + (seed + 1) + "))";
        long step = 157_852_800_000_000L / rows; // 2020-01-01 through 2025-01-01, including both leap days.
        String fact = "create table fact_solar_readings as (select cast(rnd_long(0," + (plants - 1) + ",0) as int) plant_id, "
                + "timestamp_sequence('2020-01-01'," + step + ") reading_ts, "
                + "rnd_int(0,1023,0)/1024.0 energy_kwh, rnd_int(0,4095,0)/4.0 irradiance_wm2 "
                + "from long_sequence(" + rows + "," + (seed + 2) + "," + (seed + 3) + ")) timestamp(reading_ts) partition by month bypass wal";
        System.out.println("# generator\n" + dimension + ";\n" + fact + ";");
        engine.execute(dimension, context);
        engine.execute(fact, context);
    }

    private static long nativeBytes() {
        long bytes = 0;
        for (int tag = MemoryTag.NATIVE_DEFAULT; tag < MemoryTag.SIZE; tag++) {
            bytes += Unsafe.getMemUsedByTag(tag);
        }
        return bytes;
    }

    private static long number(Map<String, String> options, String name, long defaultValue, long min, long max) {
        long value = Long.parseLong(options.getOrDefault(name, Long.toString(defaultValue)));
        if (value < min || value > max) {
            throw new IllegalArgumentException(name + " must be in [" + min + ", " + max + "]");
        }
        return value;
    }

    private static Map<String, String> options(String[] args) {
        Map<String, String> result = new HashMap<>();
        List<String> names = Arrays.asList("rows", "plants", "selected-percent", "fanout", "seed", "workers", "warmups", "runs", "repetitions", "revision", "candidate-compiler");
        for (String arg : args) {
            int eq = arg.indexOf('=');
            if (!arg.startsWith("--") || eq < 3 || !names.contains(arg.substring(2, eq))
                    || result.put(arg.substring(2, eq), arg.substring(eq + 1)) != null) {
                throw new IllegalArgumentException("unknown or duplicate option: " + arg);
            }
        }
        return result;
    }

    private static void printPlan(String name, RecordCursorFactory factory, SqlExecutionContext context, boolean fused) {
        TextPlanSink sink = new TextPlanSink();
        sink.of(factory, context);
        StringBuilder plan = new StringBuilder();
        for (int i = 1; i <= sink.getLineCount(); i++) {
            plan.append(sink.getLine(i)).append('\n');
        }
        if (plan.toString().contains("Async Hash Join Group By") != fused
                || (!fused && !plan.toString().contains("Hash Join Light"))) {
            throw new IllegalStateException("unexpected " + name + " plan:\n" + plan);
        }
        System.out.println("# " + name + " plan\n" + plan);
    }

    /**
     * Phase 1 adapter: compile the original SQL with fused selection enabled in this same build.
     * The runner owns the returned factory; the adapter borrows the engine and context. It must
     * restore any temporary planner setting before returning (also when compilation throws).
     */
    public interface CandidateCompiler {
        RecordCursorFactory compile(CairoEngine engine, SqlExecutionContext context, String sql) throws Exception;
    }

    /** Enables the experimental planner only while compiling the candidate arm. */
    public static final class PlannerCandidateCompiler implements CandidateCompiler {
        @Override
        public RecordCursorFactory compile(CairoEngine engine, SqlExecutionContext context, String sql) throws Exception {
            boolean enabled = context.isParallelHashJoinGroupByEnabled();
            context.setParallelHashJoinGroupByEnabled(true);
            try {
                return engine.select(sql, context);
            } finally {
                context.setParallelHashJoinGroupByEnabled(enabled);
            }
        }
    }

    private static final class NativeSampler extends Thread implements AutoCloseable {
        private final long initial = nativeBytes();
        private volatile long peak = initial;
        private volatile boolean running = true;

        private NativeSampler() {
            super("hash-join-group-by-native-sampler");
            setDaemon(true);
            start();
        }

        @Override
        public void close() throws InterruptedException {
            running = false;
            join();
        }

        @Override
        public void run() {
            while (running) {
                sample();
                LockSupport.parkNanos(1_000_000);
            }
        }

        private synchronized void sample() {
            peak = Math.max(peak, nativeBytes());
        }
    }

    private record ResultRow(String country, int year, int month, double energy, double irradiance, double yield) {
    }

    private record Sample(long nanos, List<ResultRow> result, long peak, long retained, String metrics) {
    }
}
