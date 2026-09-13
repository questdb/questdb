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

import io.questdb.MessageBus;
import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.CairoEngine;
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.DefaultCairoConfiguration;
import io.questdb.cairo.TableReader;
import io.questdb.cairo.sql.PartitionFormat;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.TextPlanSink;
import io.questdb.log.Log;
import io.questdb.mp.WorkerPool;
import io.questdb.mp.WorkerPoolConfiguration;
import io.questdb.mp.WorkerPoolUtils;
import io.questdb.std.FilesFacade;
import io.questdb.std.MemoryTracker;

import java.lang.instrument.Instrumentation;
import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayDeque;
import java.util.IdentityHashMap;
import java.util.Map;
import java.util.TreeMap;

/** Exact reachable heap sizing of the fused factory, with explicit shared-service boundaries. */
public class HashJoinGroupByHeapBenchmark {
    private static final int WORKERS = 4;
    private static final Map<String, Long> heapBaselines = new TreeMap<>();
    private static Instrumentation instrumentation;

    public static void premain(String args, Instrumentation agent) {
        instrumentation = agent;
    }

    public static void main(String[] args) throws Exception {
        if (instrumentation == null) {
            throw new IllegalStateException("run with the heap-size javaagent; see the off-heap guide");
        }
        final String storage = args.length > 0 ? args[0] : "native";
        final String mode = args.length > 1 ? args[1] : "owner";
        if (!storage.equals("native") && !storage.equals("mixed") && !storage.equals("parquet")) {
            throw new IllegalArgumentException("storage must be native, mixed or parquet");
        }
        if (!mode.equals("owner") && !mode.equals("sharded") && !mode.equals("scalar")) {
            throw new IllegalArgumentException("mode must be owner, sharded or scalar");
        }
        final Path root = Files.createTempDirectory("hash-join-heap-");
        WorkerPool pool = new WorkerPool(new WorkerPoolConfiguration() {
            @Override
            public int getWorkerCount() {
                return WORKERS;
            }
        });
        try (CairoEngine engine = new CairoEngine(new DefaultCairoConfiguration(root.toString()) {
            @Override
            public int getGroupByShardingThreshold() {
                return mode.equals("owner") ? Integer.MAX_VALUE : 1;
            }

            @Override
            public int getPartitionEncoderParquetRowGroupSize() {
                return 64;
            }
        })) {
            engine.load();
            WorkerPoolUtils.setupQueryJobs(pool, engine);
            pool.start();
            try (HashJoinGroupByBenchmark.BenchmarkContext context = new HashJoinGroupByBenchmark.BenchmarkContext(engine, WORKERS)) {
                context.changePageFrameSizes(64, 64);
                context.setJitMode(2);
                // Fixed shape, worker count, two partitions and frame size. Cardinalities grow
                // without warming a data-sized Java pool; each factory executes twice below.
                for (String table : new String[]{"r", "p"}) {
                    engine.execute("create table " + table + " (id int, g int, s symbol capacity 16 cache, d double, ts timestamp) timestamp(ts) partition by day", context);
                }
                final String sql = "select " + (mode.equals("scalar") ? "" : "r.g, p.s, ")
                        + "sum(r.d), avg(p.d), count(*) from r left join (p where ts < '1970-01-03') p "
                        + "on r.id=p.id and p.s like 's%' where r.ts < '1970-01-03' and r.s ilike 'S%' and r.d > 0";
                System.out.println("storage,mode,dimension,cardinality,phase,heap_bytes,heap_objects,largest_array_bytes,query_native_bytes");
                for (String dimension : new String[]{"rows", "keys", "symbols_groups", "fanout"}) {
                    for (int cardinality : new int[]{1024, 8192, 65_536, 262_144}) {
                        for (String table : new String[]{"r", "p"}) {
                            engine.execute("truncate table " + table, context);
                            final int rows = dimension.equals("rows") && table.equals("p") ? 64
                                    : dimension.equals("fanout") && table.equals("r") ? 1 : cardinality;
                            final int keys = dimension.equals("rows") ? 64 : dimension.equals("fanout") ? 1 : cardinality;
                            final int groups = dimension.equals("symbols_groups") ? cardinality : 16;
                            final int symbols = dimension.equals("symbols_groups") ? cardinality : 16;
                            engine.execute("insert into " + table + " select (x%" + keys + ")::int, (x%" + groups
                                    + ")::int, ('s'||(x%" + symbols + "))::symbol, 1.0, (case when x <= "
                                    + Math.max(1, rows / 2) + " then 0 else 86_400_000_000 end)::timestamp from long_sequence(" + rows + ")", context);
                            // Keep the active partition outside the scan so all selected
                            // partitions can be converted, including non-WAL tables.
                            engine.execute("insert into " + table + " values (-1,-1,'active',0.0,'1970-01-03')", context);
                            if (!storage.equals("native")) {
                                engine.execute("alter table " + table + " convert partition to parquet"
                                        + (storage.equals("mixed") ? " where ts < '1970-01-02'" : " where ts >= '1970-01-01'"), context);
                            }
                        }
                        for (String table : new String[]{"r", "p"}) {
                            try (TableReader reader = engine.getReader(table)) {
                                for (int partition = 0; partition < reader.getPartitionCount(); partition++) {
                                    long timestamp = reader.getPartitionTimestampByIndex(partition);
                                    if (timestamp >= 172_800_000_000L) {
                                        continue;
                                    }
                                    byte expected = storage.equals("native") || (storage.equals("mixed") && timestamp >= 86_400_000_000L)
                                            ? PartitionFormat.NATIVE : PartitionFormat.PARQUET;
                                    if (reader.getPartitionFormatFromMetadata(partition) != expected) {
                                        throw new AssertionError("storage mismatch for " + table + " partition " + partition);
                                    }
                                }
                            }
                        }
                        // Truncation/storage conversion can invalidate table metadata. Compile
                        // after it, then measure both a fresh execution and reuse of that factory.
                        context.setParallelHashJoinGroupByEnabled(true);
                        long[] actual = null;
                        try (RecordCursorFactory factory = engine.select(sql, context)) {
                            TextPlanSink plan = new TextPlanSink();
                            plan.of(factory, context);
                            boolean isFused = false;
                            for (int i = 1; i <= plan.getLineCount(); i++) {
                                String line = plan.getLine(i).toString();
                                isFused |= line.contains("Async Hash Join Group By");
                                System.out.println("# " + line);
                            }
                            if (!isFused) {
                                throw new AssertionError("fused selection required");
                            }
                            for (int execution = 1; execution <= 2; execution++) {
                                try (RecordCursor cursor = factory.getCursor(context)) {
                                    if (!cursor.hasNext()) {
                                        throw new AssertionError("expected output");
                                    }
                                    snapshot(factory, context, storage, mode, dimension, cardinality, "live" + execution);
                                    long[] result = consume(cursor, factory, true);
                                    if (actual != null && !java.util.Arrays.equals(actual, result)) {
                                        throw new AssertionError("reuse checksum mismatch");
                                    }
                                    actual = result;
                                    snapshot(factory, context, storage, mode, dimension, cardinality, "output" + execution);
                                }
                                snapshot(factory, context, storage, mode, dimension, cardinality, "closed" + execution);
                                if (context.sampledMemory() != 0) {
                                    throw new AssertionError("native query memory retained after close");
                                }
                            }
                        }
                        // Baseline and harness allocation are outside the measured graph.
                        context.setParallelHashJoinGroupByEnabled(false);
                        try (RecordCursorFactory baseline = engine.select(sql, context);
                             RecordCursor cursor = baseline.getCursor(context)) {
                            long[] expected = consume(cursor, baseline, false);
                            if (!java.util.Arrays.equals(expected, actual)) {
                                throw new AssertionError("result checksum mismatch: " + java.util.Arrays.toString(actual)
                                        + " vs " + java.util.Arrays.toString(expected));
                            }
                        }
                        // Do not let the ordinary baseline's source SYMBOL caches contaminate
                        // a later candidate sample through the shared reader pool.
                        engine.releaseAllReaders();
                    }
                }
            } finally {
                pool.halt();
            }
        }
        System.out.println("# data_root=" + root);
    }

    private static long[] consume(RecordCursor cursor, RecordCursorFactory factory, boolean isFirstReady) {
        long count = 0;
        long sum = 0;
        long xor = 0;
        while (isFirstReady || cursor.hasNext()) {
            isFirstReady = false;
            Record record = cursor.getRecord();
            long hash = 1;
            for (int i = 0; i < factory.getMetadata().getColumnCount(); i++) {
                long value;
                switch (ColumnType.tagOf(factory.getMetadata().getColumnType(i))) {
                    case ColumnType.INT: value = record.getInt(i); break;
                    case ColumnType.LONG: value = record.getLong(i); break;
                    case ColumnType.DOUBLE: value = Double.doubleToLongBits(record.getDouble(i)); break;
                    case ColumnType.SYMBOL:
                        CharSequence symbol = record.getSymA(i);
                        value = 0;
                        if (symbol != null) {
                            for (int c = 0; c < symbol.length(); c++) {
                                value = value * 31 + symbol.charAt(c);
                            }
                        }
                        break;
                    default: throw new AssertionError("unexpected result type");
                }
                hash = hash * 1_000_003 + value;
            }
            count++;
            sum += hash;
            xor ^= hash;
        }
        return new long[]{count, sum, xor};
    }

    private static void snapshot(Object factory, HashJoinGroupByBenchmark.BenchmarkContext context,
                                 String storage, String mode, String dimension, int cardinality, String phase) throws Exception {
        Graph graph = new Graph();
        graph.visit(factory);
        String boundary = dimension + ":" + phase;
        Long baseline = heapBaselines.putIfAbsent(boundary, graph.bytes);
        if (baseline != null && graph.bytes > baseline + 65_536) {
            throw new AssertionError("retained heap grew by more than 64 KiB at fixed shape: " + boundary
                    + " [baseline=" + baseline + ", bytes=" + graph.bytes + "]");
        }
        if (graph.largestArray > 65_536) {
            throw new AssertionError("large execution heap array: " + graph.largestArray);
        }
        System.out.printf("%s,%s,%s,%d,%s,%d,%d,%d,%d%n", storage, mode, dimension, cardinality, phase,
                graph.bytes, graph.seen.size(), graph.largestArray, context.sampledMemory());
        // Retain class totals so changes to the boundary or a growing helper are inspectable.
        for (Map.Entry<String, long[]> entry : graph.classes.entrySet()) {
            System.out.printf("# class,%s,%s,%s,%d,%s,%s,%d,%d%n", storage, mode, dimension, cardinality,
                    phase, entry.getKey(), entry.getValue()[0], entry.getValue()[1]);
        }
    }

    private static class Graph {
        private final Map<String, long[]> classes = new TreeMap<>();
        private final ArrayDeque<Object> pending = new ArrayDeque<>();
        private final IdentityHashMap<Object, Boolean> seen = new IdentityHashMap<>();
        private long bytes;
        private long largestArray;

        private void add(Object value) {
            if (value != null) {
                pending.push(value);
            }
        }

        private void visit(Object root) throws Exception {
            add(root);
            while (!pending.isEmpty()) {
                Object value = pending.pop();
                Class<?> type = value.getClass();
                // Borrowed infrastructure is not owned by the factory. Table-reader SYMBOL
                // caches are deliberately included below, even though the reader is shared.
                if (value instanceof CairoEngine || value instanceof CairoConfiguration
                        || value instanceof SqlExecutionContext || value instanceof MessageBus
                        || value instanceof FilesFacade || value instanceof Log || value instanceof MemoryTracker
                        || value instanceof Thread || value instanceof Class<?> || value instanceof ClassLoader
                        || type.isEnum() || type.getName().startsWith("java.lang.invoke.")) {
                    continue;
                }
                if (seen.put(value, Boolean.TRUE) != null) {
                    continue;
                }
                long size = instrumentation.getObjectSize(value);
                bytes += size;
                long[] totals = classes.computeIfAbsent(type.getName(), ignored -> new long[2]);
                totals[0]++;
                totals[1] += size;
                if (value instanceof TableReader reader) {
                    // Count the reader shell and source dictionaries, excluding shared
                    // partition/mmap/pool metadata, whose ownership predates this operator.
                    for (int i = 0; i < reader.getMetadata().getColumnCount(); i++) {
                        if (ColumnType.isSymbol(reader.getMetadata().getColumnType(i))) {
                            add(reader.getSymbolMapReader(i));
                        }
                    }
                } else if (type.isArray()) {
                    largestArray = Math.max(largestArray, size);
                    if (value instanceof Object[] array) {
                        for (Object element : array) {
                            add(element);
                        }
                    }
                } else if (!(value instanceof java.lang.ref.Reference<?>)) {
                    // Reference referents are not retained strongly. Cleaner queue links
                    // belong to the JVM, not to the factory's direct-buffer shell.
                    for (Class<?> current = type; current != null; current = current.getSuperclass()) {
                        for (Field field : current.getDeclaredFields()) {
                            if (!Modifier.isStatic(field.getModifiers()) && !field.getType().isPrimitive()) {
                                if (!field.trySetAccessible()) {
                                    throw new IllegalStateException("cannot inspect " + field);
                                }
                                add(field.get(value));
                            }
                        }
                    }
                }
            }
        }
    }
}
