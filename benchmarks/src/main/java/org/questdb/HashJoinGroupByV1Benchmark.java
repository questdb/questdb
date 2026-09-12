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
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.DefaultCairoConfiguration;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.cairo.sql.RecordMetadata;
import io.questdb.griffin.TextPlanSink;
import io.questdb.mp.WorkerPool;
import io.questdb.mp.WorkerPoolConfiguration;
import io.questdb.mp.WorkerPoolUtils;
import io.questdb.std.Os;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HexFormat;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.Callable;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static org.questdb.HashJoinGroupByBenchmark.equal;
import static org.questdb.HashJoinGroupByBenchmark.metrics;
import static org.questdb.HashJoinGroupByBenchmark.number;
import static org.questdb.HashJoinGroupByBenchmark.printPlan;

/** Completed-V1 diagnostics. The unchanged primary SQL/data gate lives in HashJoinGroupByBenchmark. */
public class HashJoinGroupByV1Benchmark {
    private static final List<String> OPTIONS = Arrays.asList(
            "rows", "plants", "selected-keys", "source-rows", "fanout", "key-domain", "hot-percent",
            "workers", "concurrency", "warmups", "runs", "repetitions", "join", "groups", "post-filter",
            "probe-storage", "build-storage", "cold-helper", "memory-limit", "revision", "interval"
    );

    public static void main(String[] args) throws Exception {
        Map<String, String> options = new HashMap<>();
        for (String arg : args) {
            int eq = arg.indexOf('=');
            if (!arg.startsWith("--") || eq < 3 || !OPTIONS.contains(arg.substring(2, eq))
                    || options.put(arg.substring(2, eq), arg.substring(eq + 1)) != null) {
                throw new IllegalArgumentException("unknown or duplicate option: " + arg);
            }
        }
        long rows = number(options, "rows", 10_000_000, 1, 1_000_000_000);
        int plants = (int) number(options, "plants", 100_000, 1, 10_000_000);
        int selected = (int) number(options, "selected-keys", Math.min(plants, 10_000), 0, plants);
        int fanout = (int) number(options, "fanout", 1, 1, 100_000);
        long sourceRows = number(options, "source-rows", (long) plants * fanout, (long) plants * fanout, 100_000_000);
        int domain = (int) number(options, "key-domain", plants, 1, Integer.MAX_VALUE);
        int hotPercent = (int) number(options, "hot-percent", 0, 0, 100);
        int workers = (int) number(options, "workers", 4, 1, 64);
        int concurrency = (int) number(options, "concurrency", 1, 1, 16);
        int warmups = (int) number(options, "warmups", 3, 1, 100);
        int runs = (int) number(options, "runs", 10, 10, 1000);
        int repetitions = (int) number(options, "repetitions", 2, 1, 100);
        long memoryLimit = number(options, "memory-limit", 0, 0, Long.MAX_VALUE);
        String join = choice(options, "join", "inner", "inner", "left", "right", "inner-swapped", "left-swapped", "right-swapped");
        String groups = choice(options, "groups", "low", "low", "scalar", "high");
        String filter = choice(options, "post-filter", "none", "none", "selective", "null-accepting", "reject-all");
        String probeStorage = choice(options, "probe-storage", "native", "native", "mixed", "parquet");
        String buildStorage = choice(options, "build-storage", "native", "native", "mixed", "parquet");
        String interval = choice(options, "interval", "full", "full", "hour");
        if (options.containsKey("cold-helper") && concurrency != 1) {
            throw new IllegalArgumentException("cold measurements require concurrency=1");
        }
        String sql = sql(join, groups, filter, interval);
        Os.init();
        Path root = Files.createTempDirectory("hash-join-v1-");
        System.out.println("# data_directory=" + root + " (retained for inspection)");
        System.out.println("# options=" + options + " defaults: rows=10000000 plants=100000 selected-keys=min(plants,10000)"
                + " source-rows=plants*fanout fanout=1 key-domain=plants hot-percent=0 workers=4 concurrency=1"
                + " warmups=3 runs=10 repetitions=2 join=inner groups=low post-filter=none"
                + " probe-storage=native build-storage=native memory-limit=0 interval=full seed=130");
        System.out.println("# SQL\n" + sql);
        WorkerPool pool = new WorkerPool(new WorkerPoolConfiguration() {
            @Override
            public String getPoolName() {
                return "hash-join-v1-benchmark";
            }

            @Override
            public int getWorkerCount() {
                return workers;
            }
        });
        ExecutorService owners = Executors.newFixedThreadPool(concurrency);
        try (CairoEngine engine = new CairoEngine(new DefaultCairoConfiguration(root.toString()) {
            @Override
            public long getQueryMemoryLimitBytes() {
                return memoryLimit;
            }
        })) {
            engine.load();
            WorkerPoolUtils.setupQueryJobs(pool, engine);
            pool.start();
            HashJoinGroupByBenchmark.BenchmarkContext[] contexts = new HashJoinGroupByBenchmark.BenchmarkContext[concurrency];
            RecordCursorFactory[][] factories = new RecordCursorFactory[2][concurrency];
            try {
                for (int owner = 0; owner < concurrency; owner++) {
                    contexts[owner] = new HashJoinGroupByBenchmark.BenchmarkContext(engine, workers);
                }
                generate(engine, contexts[0], rows, plants, selected, sourceRows, fanout, domain, hotPercent);
                boolean swapped = join.endsWith("swapped");
                convert(engine, contexts[0], swapped ? "dim_plant" : "fact_solar_readings", probeStorage);
                convert(engine, contexts[0], swapped ? "fact_solar_readings" : "dim_plant", buildStorage);
                for (int owner = 0; owner < concurrency; owner++) {
                    factories[0][owner] = engine.select(sql, contexts[owner]);
                    factories[1][owner] = new HashJoinGroupByBenchmark.PlannerCandidateCompiler().compile(engine, contexts[owner], sql);
                    assertMetadata(factories[0][owner].getMetadata(), factories[1][owner].getMetadata());
                }
                printPlan("baseline", factories[0][0], contexts[0], false);
                printPlan("candidate", factories[1][0], contexts[0], true);
                TextPlanSink sink = new TextPlanSink();
                sink.of(factories[1][0], contexts[0]);
                StringBuilder plan = new StringBuilder();
                for (int i = 1; i <= sink.getLineCount(); i++) {
                    plan.append(sink.getLine(i)).append('\n');
                }
                String planText = plan.toString();
                int probeStart = planText.indexOf("Probe\n");
                int buildStart = planText.indexOf("Build\n");
                if (probeStart < 0 || buildStart <= probeStart
                        || !planText.substring(probeStart, buildStart).contains("on: " + (swapped ? "dim_plant" : "fact_solar_readings"))
                        || !planText.substring(buildStart).contains("on: " + (swapped ? "fact_solar_readings" : "dim_plant"))) {
                    throw new IllegalStateException("wrong physical inputs: " + plan);
                }
                if (!planText.contains("logicalJoinType: " + join.split("-")[0])
                        || !plan.toString().contains("inputSwapped: " + join.startsWith("right"))
                        || plan.toString().contains("aggregation: scalar") != groups.equals("scalar")) {
                    throw new IllegalStateException("wrong join orientation or aggregation mode: " + plan);
                }
                List<List<Object>> expected = null;
                long checks = 0;
                System.out.println("arm,repetition,run,owner,elapsed_ns,groups,batch_ns,sampled_batch_native_peak_delta_bytes,"
                        + "sampled_query_peak_bytes,retained_query_bytes,build_rows,build_keys,build_bytes,scanned_rows,matched_pairs,"
                        + "null_extended_rows,surviving_rows,merge_cardinality,build_ns,init_ns,probe_ns,merge_ns");
                for (int repetition = 0; repetition < repetitions; repetition++) {
                    List<List<Long>> times = Arrays.asList(new ArrayList<>(), new ArrayList<>());
                    List<List<Long>> batchTimes = Arrays.asList(new ArrayList<>(), new ArrayList<>());
                    for (int run = -warmups; run < runs; run++) {
                        for (int turn = 0; turn < 2; turn++) {
                            int arm = ((run + warmups + repetition) & 1) == 0 ? turn : 1 - turn;
                            if (options.containsKey("cold-helper") && run >= 0) {
                                engine.releaseAllReaders();
                                engine.releaseAllWriters();
                                Process process = new ProcessBuilder("python3", options.get("cold-helper"), root.toString()).inheritIO().start();
                                if (process.waitFor() != 0) {
                                    throw new IllegalStateException("cold file eviction/residency verification failed");
                                }
                            }
                            Batch batch = execute(owners, factories[arm], contexts);
                            for (int owner = 0; owner < concurrency; owner++) {
                                Sample sample = batch.samples.get(owner);
                                if (expected == null) {
                                    expected = sample.result;
                                    MessageDigest digest = MessageDigest.getInstance("SHA-256");
                                    for (List<Object> row : expected) {
                                        digest.update((row.toString() + "\n").getBytes(StandardCharsets.UTF_8));
                                    }
                                    System.out.println("# reference_groups=" + expected.size() + " sha256=" + HexFormat.of().formatHex(digest.digest())
                                            + " preview=" + expected.subList(0, Math.min(3, expected.size())));
                                } else {
                                    assertResults(expected, sample.result);
                                    checks++;
                                }
                                if (run >= 0) {
                                    times.get(arm).add(sample.nanos);
                                    System.out.printf(Locale.ROOT, "%s,%d,%d,%d,%d,%d,%d,%d,%d,%d,%s%n",
                                            arm == 0 ? "baseline" : "candidate", repetition, run, owner, sample.nanos, sample.result.size(),
                                            batch.nanos, batch.peak, batch.queryPeaks[owner], sample.retained, sample.metrics);
                                }
                            }
                            if (run >= 0) {
                                batchTimes.get(arm).add(batch.nanos);
                            }
                        }
                    }
                    double baselineMedian = median(times.get(0));
                    for (int arm = 0; arm < 2; arm++) {
                        long[] sorted = times.get(arm).stream().mapToLong(Long::longValue).sorted().toArray();
                        double median = median(times.get(arm));
                        System.out.printf(Locale.ROOT, "# %s repetition=%d median_ms=%.3f min_ms=%.3f max_ms=%.3f speedup=%.3f batch_median_ms=%.3f queries_per_second=%.3f%n",
                                arm == 0 ? "baseline" : "candidate", repetition, median / 1e6, sorted[0] / 1e6,
                                sorted[sorted.length - 1] / 1e6, baselineMedian / median,
                                median(batchTimes.get(arm)) / 1e6, concurrency * 1e9 / median(batchTimes.get(arm)));
                    }
                }
                System.out.println("# result_checks=" + checks);
            } finally {
                owners.shutdown();
                if (!owners.awaitTermination(5, TimeUnit.MINUTES)) {
                    throw new IllegalStateException("benchmark owners did not drain");
                }
                for (RecordCursorFactory[] arm : factories) {
                    for (RecordCursorFactory factory : arm) {
                        if (factory != null) {
                            factory.close();
                        }
                    }
                }
                for (HashJoinGroupByBenchmark.BenchmarkContext context : contexts) {
                    if (context != null) {
                        context.close();
                    }
                }
                pool.halt();
            }
        } finally {
            owners.shutdownNow();
        }
    }

    private static void assertMetadata(RecordMetadata expected, RecordMetadata actual) {
        if (expected.getColumnCount() != actual.getColumnCount()) {
            throw new IllegalStateException("column count differs");
        }
        for (int col = 0; col < expected.getColumnCount(); col++) {
            if (expected.getColumnType(col) != actual.getColumnType(col)
                    || !expected.getColumnName(col).equals(actual.getColumnName(col))) {
                throw new IllegalStateException("column metadata differs at " + col);
            }
        }
    }

    private static void assertResults(List<List<Object>> expected, List<List<Object>> actual) {
        if (expected.size() != actual.size()) {
            throw new IllegalStateException("group count differs: " + expected.size() + " vs " + actual.size());
        }
        for (int row = 0; row < expected.size(); row++) {
            for (int col = 0; col < expected.get(row).size(); col++) {
                Object a = expected.get(row).get(col);
                Object b = actual.get(row).get(col);
                if (!(a instanceof Double && b instanceof Double ? equal((double) a, (double) b) : Objects.equals(a, b))) {
                    throw new IllegalStateException("result differs at " + row + "," + col + ": " + a + " vs " + b);
                }
            }
        }
    }

    private static String choice(Map<String, String> options, String name, String defaultValue, String... allowed) {
        String value = options.getOrDefault(name, defaultValue);
        if (!Arrays.asList(allowed).contains(value)) {
            throw new IllegalArgumentException(name + " must be one of " + Arrays.toString(allowed));
        }
        return value;
    }

    private static void convert(CairoEngine engine, HashJoinGroupByBenchmark.BenchmarkContext context, String table, String storage) throws Exception {
        if (!storage.equals("native")) {
            String sql = "alter table " + table + " convert partition to parquet where reading_ts < '"
                    + (storage.equals("mixed") ? "2022-07-01" : "2025-01-01") + "'";
            System.out.println("# conversion=" + sql);
            engine.execute(sql, context);
        }
    }

    private static Batch execute(ExecutorService owners, RecordCursorFactory[] factories, HashJoinGroupByBenchmark.BenchmarkContext[] contexts) throws Exception {
        CyclicBarrier start = new CyclicBarrier(factories.length);
        try (HashJoinGroupByBenchmark.NativeSampler sampler = new HashJoinGroupByBenchmark.NativeSampler(contexts)) {
            List<Callable<Sample>> tasks = new ArrayList<>();
            for (int owner = 0; owner < factories.length; owner++) {
                final int index = owner;
                tasks.add(() -> {
                    try {
                        start.await(30, TimeUnit.SECONDS);
                        List<List<Object>> result = new ArrayList<>();
                        RecordCursorFactory factory = factories[index];
                        long begin = System.nanoTime();
                        try (RecordCursor cursor = factory.getCursor(contexts[index])) {
                            Record record = cursor.getRecord();
                            RecordMetadata metadata = factory.getMetadata();
                            while (cursor.hasNext()) {
                                List<Object> row = new ArrayList<>(metadata.getColumnCount());
                                for (int col = 0; col < metadata.getColumnCount(); col++) {
                                    row.add(switch (ColumnType.tagOf(metadata.getColumnType(col))) {
                                        case ColumnType.INT -> record.getInt(col);
                                        case ColumnType.LONG -> record.getLong(col);
                                        case ColumnType.DOUBLE -> record.getDouble(col);
                                        case ColumnType.SYMBOL -> record.getSymA(col) == null ? null : record.getSymA(col).toString();
                                        default -> throw new IllegalStateException("unsupported result type: " + metadata.getColumnType(col));
                                    });
                                }
                                result.add(row);
                            }
                            long nanos = System.nanoTime() - begin;
                            long retained = contexts[index].getMemoryTracker().getUsed();
                            sampler.sample();
                            return new Sample(nanos, result, retained, metrics(factory));
                        }
                    } finally {
                        io.questdb.std.str.Path.clearThreadLocals();
                    }
                });
            }
            long begin = System.nanoTime();
            // invokeAll drains every owner even when an individual execution fails.
            List<Future<Sample>> futures = owners.invokeAll(tasks);
            long nanos = System.nanoTime() - begin;
            List<Sample> samples = new ArrayList<>();
            for (Future<Sample> future : futures) {
                samples.add(future.get());
            }
            sampler.sample();
            return new Batch(nanos, sampler.peak - sampler.initial, sampler.queryPeaks.clone(), samples);
        }
    }

    private static void generate(CairoEngine engine, HashJoinGroupByBenchmark.BenchmarkContext context, long rows, int plants,
                                 int selected, long sourceRows, int fanout, int domain, int hotPercent) throws Exception {
        String dimension = "create table dim_plant as (select cast((x-1)/" + fanout + " as int) plant_id, "
                + "cast(case when (x-1)/" + fanout + " < " + selected + " then case when ((x-1)/" + fanout
                + ")%2=0 then 'ES' else 'IT' end else 'DE' end as symbol) country, "
                + "rnd_int(1,800,0)/8.0 installed_kwp, timestamp_sequence('2020-01-01'," + (157_852_800_000_000L / sourceRows)
                + ") reading_ts from long_sequence(" + sourceRows + ",130,131)) timestamp(reading_ts) partition by month bypass wal";
        String key = hotPercent == 0 ? "rnd_long(0," + (domain - 1) + ",0)"
                : "case when x%100 < " + hotPercent + " then 0 else rnd_long(0," + (domain - 1) + ",0) end";
        String fact = "create table fact_solar_readings as (select cast(" + key + " as int) plant_id, "
                + "timestamp_sequence('2020-01-01'," + (157_852_800_000_000L / rows) + ") reading_ts, "
                + "rnd_int(0,1023,0)/1024.0 energy_kwh, rnd_int(0,4095,0)/4.0 irradiance_wm2 "
                + "from long_sequence(" + rows + ",132,133)) timestamp(reading_ts) partition by month bypass wal";
        System.out.println("# generator\n" + dimension + ";\n" + fact + ";");
        engine.execute(dimension, context);
        engine.execute(fact, context);
    }

    private static double median(List<Long> values) {
        long[] sorted = values.stream().mapToLong(Long::longValue).sorted().toArray();
        return (sorted[(sorted.length - 1) / 2] + (double) sorted[sorted.length / 2]) / 2;
    }

    private static String sql(String join, String groups, String filter, String interval) {
        String keys = switch (groups) {
            case "scalar" -> "";
            case "high" -> "r.plant_id, year(r.reading_ts) yr, month(r.reading_ts) mo, ";
            default -> "p.country, year(r.reading_ts) yr, month(r.reading_ts) mo, ";
        };
        String r = "fact_solar_readings r";
        // Keep a build-only filter for outer semantics and a filtered preserved input when swapped.
        String p = "(select * from dim_plant where country in ('ES','IT')) p";
        String inputs = switch (join) {
            case "left" -> r + " left join " + p;
            case "right" -> p + " right join " + r;
            case "inner-swapped" -> p + " inner join " + r;
            case "left-swapped" -> p + " left join " + r;
            case "right-swapped" -> r + " right join " + p;
            default -> r + " inner join " + p;
        };
        String where = join.endsWith("swapped") ? "p.reading_ts" : "r.reading_ts";
        where = " where " + where + " >= '2020-01-01' and " + where + " < '"
                + (interval.equals("hour") ? "2020-01-01T01:00:00" : "2025-01-01") + "'";
        where += switch (filter) {
            case "selective" -> " and p.installed_kwp > 90.0";
            case "null-accepting" -> " and (p.installed_kwp > 90.0 or p.installed_kwp is null)";
            case "reject-all" -> " and p.installed_kwp > 100.0";
            default -> "";
        };
        return "select " + keys + "count(*) joined_rows, count(p.installed_kwp) non_null_capacity, "
                + "sum(r.energy_kwh) energy, avg(r.irradiance_wm2) irradiance, "
                + "sum(r.energy_kwh) / nullif(sum(p.installed_kwp),0) specific_yield from "
                + inputs + " on r.plant_id=p.plant_id" + where
                + (groups.equals("scalar") ? "" : " group by " + (groups.equals("high") ? "r.plant_id" : "p.country")
                + ", year(r.reading_ts), month(r.reading_ts) order by " + (groups.equals("high") ? "r.plant_id" : "p.country") + ", yr, mo");
    }

    private record Sample(long nanos, List<List<Object>> result, long retained, String metrics) {
    }

    private record Batch(long nanos, long peak, long[] queryPeaks, List<Sample> samples) {
    }
}
