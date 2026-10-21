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
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.griffin.SqlExecutionContextImpl;
import io.questdb.mp.WorkerPool;
import io.questdb.mp.WorkerPoolUtils;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Comparator;
import java.util.Locale;

/**
 * Measures first and repeated LIKE scans; arguments: symbol count, ample/stopped.
 */
public class SymbolMapCacheLikeBenchmark {
    private static final int BATCHES = 9;
    private static final int CACHE_WARMUPS = 3;
    private static final int FIRST_SCANS = 3;
    private static final int PREFILL_KEYS = 16_384;
    private static final int WARMUPS = 100;

    public static void main(String[] args) throws Exception {
        int symbolCount = Integer.parseInt(args[0]);
        if (symbolCount != 64 && symbolCount != 65_536 && symbolCount != 1_048_576) {
            throw new IllegalArgumentException("expected 64, 65536 or 1048576 symbols");
        }
        if (!args[1].equals("ample") && !args[1].equals("stopped")) {
            throw new IllegalArgumentException("expected ample or stopped");
        }
        boolean isStopped = args[1].equals("stopped");
        long budget = isStopped ? (symbolCount <= 4096 ? 0 : 256 * 1024) : 256L * 1024 * 1024;
        int rows = Math.max(100_000, symbolCount);
        int repetitions = symbolCount == 64 ? 50 : 5;
        long expected = expectedCount(rows, symbolCount);
        Path root = Files.createTempDirectory("symbol-cache-like-");
        DefaultCairoConfiguration configuration = new DefaultCairoConfiguration(root.toString());
        try (
                CairoEngine engine = new CairoEngine(configuration);
                SqlExecutionContextImpl context = new SqlExecutionContextImpl(engine, 1)
        ) {
            boolean hasSharedCache = SymbolMapCacheBenchmarkData.setBudget(engine, budget);
            context.with(configuration.getFactoryProvider().getSecurityContextFactory().getRootContext(), null, null, -1, null);
            WorkerPool workers = new WorkerPool(() -> 1);
            WorkerPoolUtils.setupQueryJobs(workers, engine, true);
            workers.start();
            try {
                create(engine, context, "training", symbolCount, rows);
                try (RecordCursorFactory factory = engine.select("SELECT count() FROM training WHERE s LIKE '%1%'", context)) {
                    for (int i = 0; i < WARMUPS; i++) {
                        consume(factory, context, expected);
                    }
                }
                if (!engine.releaseAllReaders()) {
                    throw new AssertionError("training readers remain borrowed");
                }
                engine.execute("DROP TABLE training", context);
                create(engine, context, "data", symbolCount, rows);
                double[] first = new double[FIRST_SCANS];
                double[] repeated = new double[BATCHES];
                System.out.printf(Locale.ROOT,
                        "SYMBOL_LIKE config symbols=%d scenario=%s budget=%d shared=%b rows=%d warmups=%d cacheWarmups=%d firstScans=%d repetitions=%d batches=%d%n",
                        symbolCount, args[1], budget, hasSharedCache, rows, WARMUPS, CACHE_WARMUPS, FIRST_SCANS, repetitions, BATCHES);
                for (int trial = 0; trial < CACHE_WARMUPS + FIRST_SCANS; trial++) {
                    if (!engine.releaseAllReaders()) {
                        throw new AssertionError("measured readers remain borrowed");
                    }
                    if (hasSharedCache && getBudgetUsed(engine) != 0) {
                        throw new AssertionError("previous cache allocations were not released");
                    }
                    try (RecordCursorFactory factory = engine.select("SELECT count() FROM data WHERE s LIKE '%1%'", context)) {
                        TableReader prepared;
                        int initial;
                        int before;
                        boolean isFillingBefore;
                        int stopReason = 0;
                        long allocatedBefore = 0;
                        long allocatedAfter = 0;
                        try (TableReader reader = engine.getReader("data")) {
                            prepared = reader;
                            SymbolMapReaderImpl symbols = (SymbolMapReaderImpl) reader.getSymbolMapReader(0);
                            initial = SymbolMapCacheBenchmarkData.cacheSize(symbols);
                            if (initial != (hasSharedCache ? Math.min(4096, symbolCount) : 0)) {
                                throw new AssertionError("target cache was not reset: " + initial);
                            }
                            if (isStopped && symbolCount > 4096) {
                                for (int key = 0; key < PREFILL_KEYS; key++) {
                                    symbols.valueOf(key);
                                }
                            }
                            before = SymbolMapCacheBenchmarkData.cacheSize(symbols);
                            isFillingBefore = false;
                            if (hasSharedCache) {
                                Object map = getMap(symbols);
                                isFillingBefore = (boolean) map.getClass().getMethod("isFilling").invoke(map);
                                stopReason = (int) map.getClass().getMethod("getStopReason").invoke(map);
                                allocatedBefore = (long) map.getClass().getMethod("allocatedBytes").invoke(map);
                                if (isStopped && symbolCount > 4096 && stopReason != map.getClass().getField("STOP_BUDGET").getInt(null)) {
                                    throw new AssertionError("expected a budget stop: " + stopReason);
                                }
                            }
                            if (hasSharedCache && isStopped && symbolCount > 4096
                                    && (isFillingBefore || before <= 4096 || before >= symbolCount)) {
                                throw new AssertionError("expected a partially populated stopped map: " + before);
                            }
                        }
                        long start = System.nanoTime();
                        consume(factory, context, expected);
                        if (trial >= CACHE_WARMUPS) {
                            first[trial - CACHE_WARMUPS] = (System.nanoTime() - start) / 1000.0;
                        }
                        int after;
                        try (TableReader reader = engine.getReader("data")) {
                            if (reader != prepared) {
                                throw new AssertionError("LIKE did not reuse the prepared reader");
                            }
                            SymbolMapReaderImpl symbols = (SymbolMapReaderImpl) reader.getSymbolMapReader(0);
                            after = SymbolMapCacheBenchmarkData.cacheSize(symbols);
                            if (hasSharedCache) {
                                Object map = getMap(symbols);
                                allocatedAfter = (long) map.getClass().getMethod("allocatedBytes").invoke(map);
                                if (isStopped && allocatedAfter != allocatedBefore) {
                                    throw new AssertionError("stopped scan allocated map memory");
                                }
                            }
                        }
                        int wanted = hasSharedCache && isStopped ? before : symbolCount;
                        if (after != wanted) {
                            throw new AssertionError("unexpected first-scan cache size: " + after + " != " + wanted);
                        }
                        if (trial == CACHE_WARMUPS + FIRST_SCANS - 1) {
                            for (int i = 0; i < WARMUPS; i++) {
                                consume(factory, context, expected);
                            }
                            for (int batch = 0; batch < BATCHES; batch++) {
                                start = System.nanoTime();
                                for (int i = 0; i < repetitions; i++) {
                                    consume(factory, context, expected);
                                }
                                repeated[batch] = (System.nanoTime() - start) / (repetitions * 1000.0);
                            }
                            try (TableReader reader = engine.getReader("data")) {
                                if (reader != prepared || SymbolMapCacheBenchmarkData.cacheSize((SymbolMapReaderImpl) reader.getSymbolMapReader(0)) != after) {
                                    throw new AssertionError("repeated scans changed the reader or cache size");
                                }
                                if (hasSharedCache) {
                                    Object map = getMap((SymbolMapReaderImpl) reader.getSymbolMapReader(0));
                                    if ((long) map.getClass().getMethod("allocatedBytes").invoke(map) != allocatedAfter) {
                                        throw new AssertionError("repeated scans allocated map memory");
                                    }
                                }
                            }
                        }
                        System.out.printf(Locale.ROOT,
                                "SYMBOL_LIKE cache warmup=%b trial=%d initial=%d before=%d after=%d low=%d fillingBefore=%b stopReason=%d bytesBefore=%d bytesAfter=%d budgetUsed=%d lowOnly=%b%n",
                                trial < CACHE_WARMUPS, trial, initial, before, after, hasSharedCache ? Math.min(4096, symbolCount) : 0,
                                isFillingBefore, stopReason, allocatedBefore, allocatedAfter, hasSharedCache ? getBudgetUsed(engine) : -1, symbolCount <= 4096);
                    }
                }
                System.out.printf(Locale.ROOT, "SYMBOL_LIKE first samples_us=%s checksum=%d%n", Arrays.toString(first), expected);
                System.out.printf(Locale.ROOT, "SYMBOL_LIKE repeated samples_us=%s checksum=%d%n", Arrays.toString(repeated), expected);
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

    private static void consume(RecordCursorFactory factory, SqlExecutionContextImpl context, long expected) throws Exception {
        try (RecordCursor cursor = factory.getCursor(context)) {
            if (!cursor.hasNext() || cursor.getRecord().getLong(0) != expected || cursor.hasNext()) {
                throw new AssertionError("LIKE result changed");
            }
        }
    }

    private static void create(CairoEngine engine, SqlExecutionContextImpl context, String table, int symbols, int rows) throws Exception {
        engine.execute("CREATE TABLE " + table + " (s SYMBOL CAPACITY " + Math.max(256, symbols) + " CACHE)", context);
        engine.execute("INSERT INTO " + table + " SELECT 'symbol' || (x % " + symbols + ")::string FROM long_sequence(" + rows + ")", context);
    }

    private static long expectedCount(int rows, int symbols) {
        long count = 0;
        for (int row = 1; row <= rows; row++) {
            for (int value = row % symbols; value > 0; value /= 10) {
                if (value % 10 == 1) {
                    count++;
                    break;
                }
            }
        }
        return count;
    }

    private static long getBudgetUsed(CairoEngine engine) throws Exception {
        Object pool = engine.getClass().getMethod("getSymbolMapCachePool").invoke(engine);
        Object budget = pool.getClass().getMethod("getBudget").invoke(pool);
        return (long) budget.getClass().getMethod("getUsedBytes").invoke(budget);
    }

    private static Object getMap(SymbolMapReaderImpl reader) throws Exception {
        var field = SymbolMapReaderImpl.class.getDeclaredField("cache");
        field.setAccessible(true);
        return field.get(reader);
    }
}
