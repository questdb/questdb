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
import io.questdb.cairo.SymbolMapReaderImpl;
import io.questdb.cairo.TableReader;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.griffin.SqlExecutionContextImpl;
import io.questdb.mp.WorkerPool;
import io.questdb.mp.WorkerPoolUtils;
import io.questdb.std.str.DirectString;
import io.questdb.std.str.Utf8StringSink;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Comparator;
import java.util.Locale;
import java.util.function.Consumer;

/**
 * Query-level companion to SymbolMapCacheBenchmark. Run in fresh JVMs against
 * baseline and candidate classes. Arguments: CACHE/NOCACHE, warmups, repetitions,
 * batches, query workers, symbol count and cache budget in bytes. DDL, compilation and filling
 * are outside timed batches.
 * The moving hotspot uses at least 10,000 warmups.
 * JSON measures cursor consumption and symbol escaping, without HTTP transport.
 */
public class SymbolMapCacheQueryBenchmark {
    public static void main(String[] args) throws Exception {
        String cacheMode = args.length > 0 ? args[0] : "CACHE";
        if (!cacheMode.equals("CACHE") && !cacheMode.equals("NOCACHE")) {
            throw new IllegalArgumentException("expected CACHE or NOCACHE");
        }
        int warmups = args.length > 1 ? Integer.parseInt(args[1]) : 100;
        int repetitions = args.length > 2 ? Integer.parseInt(args[2]) : 50;
        int batches = args.length > 3 ? Integer.parseInt(args[3]) : 7;
        int workerCount = args.length > 4 ? Integer.parseInt(args[4]) : 1;
        int symbolCount = args.length > 5 ? Integer.parseInt(args[5]) : 64;
        long budget = args.length > 6 ? Long.parseLong(args[6]) : 256 * 1024;
        if (symbolCount < 2 || symbolCount > 1_048_576 || budget < 0) {
            throw new IllegalArgumentException("symbol count must be in [2, 1048576] and budget must be non-negative");
        }
        int rows = Math.max(100_000, symbolCount);
        int symbolCapacity = Math.max(256, symbolCount);
        if (warmups < 0 || repetitions < 1 || batches < 1 || workerCount < 1) {
            throw new IllegalArgumentException("warmups must be non-negative; repetitions, batches and workers must be positive");
        }
        System.out.printf(Locale.ROOT, "SYMBOL_QUERY config cache=%s warmups=%d repetitions=%d batches=%d workers=%d symbols=%d budget=%d%n",
                cacheMode, warmups, repetitions, batches, workerCount, symbolCount, budget);
        Path root = Files.createTempDirectory("symbol-cache-query-");
        DefaultCairoConfiguration configuration = new DefaultCairoConfiguration(root.toString());
        try (
                CairoEngine engine = new CairoEngine(configuration);
                SqlExecutionContextImpl context = new SqlExecutionContextImpl(engine, workerCount)
        ) {
            boolean hasSharedCache = SymbolMapCacheBenchmarkData.setBudget(engine, budget);
            context.with(configuration.getFactoryProvider().getSecurityContextFactory().getRootContext(), null, null, -1, null);
            WorkerPool workers = new WorkerPool(() -> workerCount);
            WorkerPoolUtils.setupQueryJobs(workers, engine, true);
            workers.start();
            try {
                engine.execute("create table data (s symbol capacity " + symbolCapacity + " " + cacheMode + ", ts timestamp) timestamp(ts)", context);
                engine.execute("insert into data select 'symbol' || (x % " + symbolCount + ")::string, x::timestamp from long_sequence(" + rows + ")", context);
                engine.execute("create table dims (s symbol capacity " + symbolCapacity + " " + cacheMode + ", n long)", context);
                engine.execute("insert into dims select 'symbol' || (" + symbolCount + " - x)::string, x from long_sequence(" + symbolCount + ")", context);
                measure(engine, context, "json", "select s from data", 0, warmups, repetitions, batches, null);
                measure(engine, context, "like", "select count() from data where s like '%1%'", 1, warmups, repetitions, batches, null);
                measure(engine, context, "join", "select sum(d.n) from data t join dims d on t.s = d.s", 1, warmups, repetitions, batches, null);
                measure(engine, context, "parallel", "select s, count() from data where length(s) > 6", 2, warmups, repetitions, batches, null);

                if (hasSharedCache && cacheMode.equals("CACHE") && symbolCount > 4096) {
                    try (TableReader reader = engine.getReader("data")) {
                        int cached = SymbolMapCacheBenchmarkData.cacheSize((SymbolMapReaderImpl) reader.getSymbolMapReader(0));
                        boolean isFull = budget >= 256L * 1024 * 1024;
                        if ((isFull && cached != symbolCount) || (!isFull && (cached < 4096 || cached >= symbolCount))) {
                            throw new AssertionError("unexpected high-cardinality cache state: " + cached);
                        }
                        System.out.printf(Locale.ROOT, "SYMBOL_QUERY cached=%d symbols=%d budget=%d%n", cached, symbolCount, budget);
                    }
                }

                // Avoid automatic capacity rebuilds changing the dictionary version during the append.
                engine.execute("create table hot (s symbol capacity 65536 " + cacheMode + ", ts timestamp) timestamp(ts)", context);
                engine.execute("insert into hot select cast(x as string), x::timestamp from long_sequence(20000)", context);
                // Keep the old map alive while appending a disjoint set of hot keys.
                try (TableReader pinned = engine.getReader("hot")) {
                    SymbolMapReaderImpl symbolReader = (SymbolMapReaderImpl) pinned.getSymbolMapReader(0);
                    for (int i = 0; i < 20000; i++) {
                        symbolReader.valueOf(i);
                    }
                    Consumer<String> cacheCheck = null;
                    if (hasSharedCache && cacheMode.equals("CACHE")) {
                        Object map = cacheMap(symbolReader);
                        int cachedBefore = SymbolMapCacheBenchmarkData.cacheSize(symbolReader);
                        long bytesBefore = (long) map.getClass().getMethod("allocatedBytes").invoke(map);
                        boolean isAmple = budget >= 256L * 1024 * 1024;
                        cacheCheck = phase -> checkMovingCache(engine, symbolReader, map, cachedBefore, bytesBefore, isAmple, phase);
                        cacheCheck.accept("prepared");
                    }
                    engine.execute("insert into hot select cast(x + 20000 as string), (x + 20000)::timestamp from long_sequence(20000)", context);
                    measure(engine, context, "moving", "select s from hot order by ts desc limit 1000", 0, Math.max(10_000, warmups), repetitions, batches, cacheCheck);
                }
            } finally {
                workers.halt();
            }
        } finally {
            io.questdb.std.str.Path.clearThreadLocals();
            try (var paths = Files.walk(root)) {
                for (Path path : paths.sorted(Comparator.reverseOrder()).toList()) {
                    Files.delete(path);
                }
            }
        }
    }

    private static Object cacheMap(SymbolMapReaderImpl reader) throws ReflectiveOperationException {
        var field = SymbolMapReaderImpl.class.getDeclaredField("cache");
        field.setAccessible(true);
        return field.get(reader);
    }

    private static void checkMovingCache(
            CairoEngine engine,
            SymbolMapReaderImpl pinned,
            Object map,
            int cachedBefore,
            long bytesBefore,
            boolean isAmple,
            String phase
    ) {
        try (TableReader reader = engine.getReader("hot")) {
            SymbolMapReaderImpl current = (SymbolMapReaderImpl) reader.getSymbolMapReader(0);
            if (pinned.getSymbolCapacity() != 65536 || current.getSymbolCapacity() != 65536) {
                throw new AssertionError("moving symbol dictionary capacity changed");
            }
            if (cacheMap(pinned) != map || cacheMap(current) != map) {
                throw new AssertionError("moving query reader does not share the pinned cache");
            }
            boolean isPrepared = phase.equals("prepared");
            int low = (int) pinned.getClass().getMethod("getLowCacheSize").invoke(pinned);
            int cached = low + pinned.getCacheSize();
            boolean isFilling = (boolean) map.getClass().getMethod("isFilling").invoke(map);
            int stopReason = (int) map.getClass().getMethod("getStopReason").invoke(map);
            long bytes = (long) map.getClass().getMethod("allocatedBytes").invoke(map);
            if (low != 4096 || current.getSymbolCount() != (isPrepared ? 20000 : 40000)) {
                throw new AssertionError("unexpected moving symbol dictionary state");
            }
            if (isAmple) {
                if (cached != (isPrepared ? 20000 : 21000) || !isFilling || stopReason != 0) {
                    throw new AssertionError("moving cache did not retain the old and new hotspots: " + cached);
                }
            } else if (cachedBefore < 4096 || cachedBefore >= 20000 || cached != cachedBefore || bytes != bytesBefore
                    || isFilling || stopReason != map.getClass().getField("STOP_BUDGET").getInt(null)) {
                throw new AssertionError("moving cache did not remain stopped at its budget: " + cached);
            }
            // The pinned reader still has 20K symbols; probe its shared map without admitting values.
            var get = map.getClass().getMethod("get", int.class, DirectString.class);
            DirectString view = new DirectString();
            boolean isHitExpected = isAmple && !isPrepared;
            if ((get.invoke(map, 39000, view) != null) != isHitExpected
                    || (get.invoke(map, 39999, view) != null) != isHitExpected) {
                throw new AssertionError("unexpected moving hotspot boundary cache state");
            }
            Object pool = engine.getClass().getMethod("getSymbolMapCachePool").invoke(engine);
            Object budget = pool.getClass().getMethod("getBudget").invoke(pool);
            long budgetUsed = (long) budget.getClass().getMethod("getUsedBytes").invoke(budget);
            System.out.printf(Locale.ROOT, "SYMBOL_QUERY moving_cache phase=%s cached=%d low=%d filling=%b stopReason=%d bytes=%d budgetUsed=%d%n",
                    phase, cached, low, isFilling, stopReason, bytes, budgetUsed);
        } catch (ReflectiveOperationException e) {
            throw new AssertionError("cannot inspect the moving cache", e);
        }
    }

    private static void measure(
            CairoEngine engine,
            SqlExecutionContextImpl context,
            String name,
            String sql,
            int resultType,
            int warmups,
            int repetitions,
            int batches,
            Consumer<String> cacheCheck
    ) throws Exception {
        Utf8StringSink json = new Utf8StringSink();
        try (RecordCursorFactory factory = engine.select(sql, context)) {
            long expected = consume(factory, context, json, resultType);
            for (int i = 0; i < warmups; i++) {
                check(expected, consume(factory, context, json, resultType));
            }
            if (cacheCheck != null) {
                cacheCheck.accept("warmed");
            }
            double[] samples = new double[batches];
            for (int batch = 0; batch < batches; batch++) {
                long start = System.nanoTime();
                for (int i = 0; i < repetitions; i++) {
                    check(expected, consume(factory, context, json, resultType));
                }
                samples[batch] = (System.nanoTime() - start) / (repetitions * 1000.0);
            }
            if (cacheCheck != null) {
                cacheCheck.accept("measured");
            }
            System.out.printf(Locale.ROOT, "SYMBOL_QUERY %s samples_us=%s checksum=%d%n", name, Arrays.toString(samples), expected);
            Arrays.sort(samples);
            double q1 = percentile(samples, 0.25);
            double q3 = percentile(samples, 0.75);
            System.out.printf(Locale.ROOT, "SYMBOL_QUERY %s median_us=%.3f q1_us=%.3f q3_us=%.3f iqr_us=%.3f%n",
                    name, percentile(samples, 0.5), q1, q3, q3 - q1);
        }
    }

    private static double percentile(double[] sorted, double fraction) {
        double position = fraction * (sorted.length - 1);
        int lo = (int) position;
        int hi = Math.min(lo + 1, sorted.length - 1);
        return sorted[lo] + (sorted[hi] - sorted[lo]) * (position - lo);
    }

    private static void check(long expected, long actual) {
        if (expected != actual) {
            throw new AssertionError("query result changed: " + expected + " != " + actual);
        }
    }

    private static long consume(RecordCursorFactory factory, SqlExecutionContextImpl context, Utf8StringSink json, int resultType) throws Exception {
        long checksum = 0;
        try (RecordCursor cursor = factory.getCursor(context)) {
            Record record = cursor.getRecord();
            while (cursor.hasNext()) {
                if (resultType == 0) {
                    json.clear();
                    json.putQuote().escapeJsonStr(record.getSymA(0)).putQuote();
                    checksum += json.size();
                } else if (resultType == 1) {
                    checksum += record.getLong(0);
                } else {
                    checksum += record.getSymA(0).length() + record.getLong(1);
                }
            }
        }
        return checksum;
    }
}
