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

import com.sun.management.ThreadMXBean;
import io.questdb.cairo.*;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.griffin.TextPlanSink;
import io.questdb.mp.*;
import java.lang.management.ManagementFactory;
import java.lang.management.ThreadInfo;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;

/** Allocation-free result consumer and whole-query owner/worker allocation windows. */
public final class HashJoinGroupByAllocationBenchmark {
    private static final ThreadMXBean BEAN = (ThreadMXBean) ManagementFactory.getThreadMXBean();
    private static final int WORKERS = 4;
    private static final long[] THREAD_IDS = new long[WORKERS + 2];
    private static final String[] THREAD_NAMES = new String[WORKERS + 2];
    private static final long[][] BYTES = new long[5][WORKERS + 2];
    private static int threadCount;
    private static final long[] RESULT = new long[3];

    public static void main(String[] args) throws Exception {
        String storage = args.length > 0 ? args[0] : "native";
        String mode = args.length > 1 ? args[1] : "owner";
        boolean sites = args.length > 2 && args[2].equals("sites");
        int rows = args.length > 3 ? Integer.parseInt(args[3]) : 8192;
        boolean concurrent = args.length > 4 && args[4].equals("2");
        String join = args.length > 5 ? args[5] : "left";
        boolean converted = args.length > 6 && args[6].equals("converted");
        if (!storage.equals("native") && !storage.equals("mixed") && !storage.equals("parquet")) throw new IllegalArgumentException("storage");
        if (!mode.equals("owner") && !mode.equals("sharded") && !mode.equals("scalar")) throw new IllegalArgumentException("mode");
        if (!join.equals("left") && !join.equals("inner") && !join.equals("right")) throw new IllegalArgumentException("join");
        if (rows < 8192) throw new IllegalArgumentException("rows must exceed bounded setup");
        Path root = Files.createTempDirectory("hash-join-allocation-");
        BEAN.setThreadAllocatedMemoryEnabled(true);
        WorkerPool pool = new WorkerPool(new WorkerPoolConfiguration() {
            @Override public int getWorkerCount() { return WORKERS; }
            @Override public String getPoolName() { return "alloc_worker"; }
        });
        try (CairoEngine engine = new CairoEngine(new DefaultCairoConfiguration(root.toString()) {
            @Override public int getGroupByShardingThreshold() { return mode.equals("owner") ? Integer.MAX_VALUE : 1; }
            @Override public int getPartitionEncoderParquetRowGroupSize() { return 64; }
            @Override public boolean isGroupByPresizeEnabled() { return false; }
        })) {
            engine.load();
            WorkerPoolUtils.setupQueryJobs(pool, engine);
            pool.start();
            try (HashJoinGroupByBenchmark.BenchmarkContext context = new HashJoinGroupByBenchmark.BenchmarkContext(engine, WORKERS)) {
                context.with(new io.questdb.griffin.engine.functions.bind.BindVariableServiceImpl(engine.getConfiguration()));
                context.changePageFrameSizes(64, 64);
                context.setJitMode(2);
                for (String table : new String[]{"r", "p"}) {
                    engine.execute("create table " + table + " (id int, l long, s symbol capacity 16 cache, d double, ts timestamp) timestamp(ts) partition by day", context);
                }
                load(engine, context, "native", 128);
                context.getBindVariableService().setInt("key_limit", 512);
                context.getBindVariableService().setInt("key_floor", 1);
                context.setParallelHashJoinGroupByEnabled(true);
                String sql = "select " + (mode.equals("scalar") ? "" : "r.s, p.s, year(r.ts), month(r.ts), ")
                        + "sum(r.d), avg(p.d), sum(p.d), avg(r.d), count(*), count(r.id), count(p.id), count(r.l), count(p.l), count(r.d), count(p.d), count(r.s), count(p.s) from r left join (p where ts < '1970-01-03' and id < :key_limit and id >= :key_floor) p "
                        + "on r.id=p.id and p.s like 's%' where r.ts < '1970-01-03' and r.s ilike 'S%' and r.d > 0 and r.id < :key_limit and r.id >= :key_floor";
                if (join.equals("inner")) sql = sql.replace("r left join", "r join");
                if (join.equals("right")) sql = sql.replace("r left join (p where ts < '1970-01-03' and id < :key_limit and id >= :key_floor) p",
                        "(p where ts < '1970-01-03' and id < :key_limit and id >= :key_floor) p right join r");
                // Warm code and fixed expression/slot controls with a small input only.
                try (RecordCursorFactory warm = engine.select(sql, context)) {
                    for (int i = 0; i < 100; i++) execute(warm, context);
                }
                if (converted) for (String table : new String[]{"r", "p"}) engine.execute("alter table " + table + " alter column d type long", context);
                load(engine, context, storage, 2 * rows + 1);
                if (converted) for (String table : new String[]{"r", "p"}) engine.execute("alter table " + table + " alter column d type double", context);
                // Warm growth and merge machine code in a disposable factory. Its
                // native build/maps are released; a fresh factory is measured below.
                context.getBindVariableService().setInt("key_limit", rows / 2 + 1);
                try (RecordCursorFactory warmGrowth = engine.select(sql, context)) {
                    for (int i = 0; i < 16; i++) execute(warmGrowth, context);
                }
                engine.releaseAllReaders();
                context.getBindVariableService().setInt("key_limit", 512);
                context.getBindVariableService().setInt("key_floor", 1);
                AllocationAgent.reset();
                AllocationAgent.profile = false;
                AllocationAgent.enabled = true;
                RecordCursorFactory factory;
                long compilerStart = BEAN.getCurrentThreadAllocatedBytes();
                try { factory = engine.select(sql, context); } finally { AllocationAgent.enabled = false; }
                long compilerBytes = BEAN.getCurrentThreadAllocatedBytes() - compilerStart;
                System.out.println("COMPILER_BYTES," + compilerBytes);
                AllocationAgent.profile = sites;
                if (sites) AllocationAgent.report("compiler");
                try (factory; Peer peer = concurrent ? new Peer(engine, sql) : null) {
                    if (peer != null) peer.start();
                    TextPlanSink plan = new TextPlanSink();
                    plan.of(factory, context);
                    boolean fused = false;
                    for (int i = 1; i <= plan.getLineCount(); i++) {
                        String line = plan.getLine(i).toString();
                        fused |= line.contains("Async Hash Join Group By");
                        System.out.println("PLAN," + line);
                    }
                    if (!fused) throw new AssertionError("fused selection required");
                    threads(concurrent);
                    long[] measuredResult = null;
                    for (int run = 0; run < 4; run++) {
                        if (run == 1) {
                            // Cover the fixed 4 x 32 shared filter queue cells and every
                            // bounded decoder/slot shell. The build remains limited to
                            // 1,022 rows; this never warms a data-sized Java pool.
                            snapshot(0);
                            warmSymbolViews(engine, concurrent ? 2 : 1);
                            warmLocalFilterTasks(engine, factory, context, peer);
                            for (int setup = 0; setup < 64; setup++) {
                                if (peer != null) peer.request++;
                                execute(factory, context);
                                if (peer != null) peer.await();
                            }
                            // Exercise the same bounded controls at peak native
                            // occupancy. The first key range is disjoint from the
                            // measured range; dictionaries/maps close every time.
                            context.getBindVariableService().setInt("key_limit", rows / 2 + 1);
                            if (peer != null) peer.context.getBindVariableService().setInt("key_limit", rows / 2 + 1);
                            for (int setup = 0; setup < 32; setup++) {
                                if (peer != null) peer.request++;
                                execute(factory, context);
                                if (peer != null) peer.await();
                            }
                            snapshot(4);
                            for (int thread = 0; thread < threadCount; thread++) System.out.println("BOUNDED_SETUP_BYTES," + THREAD_NAMES[thread] + "," + (BYTES[4][thread] - BYTES[0][thread]));
                            context.getBindVariableService().setInt("key_floor", rows / 2 + 1);
                            context.getBindVariableService().setInt("key_limit", rows + 1);
                            if (peer != null) {
                                peer.context.getBindVariableService().setInt("key_floor", rows / 2 + 1);
                                peer.context.getBindVariableService().setInt("key_limit", rows + 1);
                            }
                        }
                        jdk.jfr.Recording recording = null;
                        if (run > 0 && args.length > 7) {
                            recording = new jdk.jfr.Recording();
                            recording.enable("jdk.ObjectAllocationOutsideTLAB").withStackTrace();
                            recording.start();
                        }
                        String phase = run == 0 ? "setup1024" : "execution" + run;
                        AllocationAgent.reset();
                        AllocationAgent.enabled = true;
                        snapshot(0);
                        if (peer != null) peer.request++;
                        RecordCursor cursor = factory.getCursor(context);
                        io.questdb.std.MemoryTracker tracker = context.getMemoryTracker();
                        snapshot(1);
                        boolean ready = cursor.hasNext();
                        snapshot(2);
                        consume(cursor, factory, ready, RESULT);
                        snapshot(3);
                        cursor.close();
                        if (peer != null) peer.await();
                        snapshot(4);
                        AllocationAgent.enabled = false;
                        if (recording != null) {
                            recording.stop();
                            recording.dump(Path.of(args[7] + "." + run + ".jfr"));
                            recording.close();
                        }
                        if (run > 0) {
                            if (measuredResult == null) measuredResult = RESULT.clone();
                            else if (!Arrays.equals(measuredResult, RESULT)) throw new AssertionError("reuse mismatch");
                        }
                        if (!sites && run > 0) {
                            for (int thread = 0; thread < threadCount; thread++) {
                                long bytes = BYTES[4][thread] - BYTES[0][thread];
                                long frameworkBytes = AllocationAgent.frameworkBytes(THREAD_IDS[thread]);
                                if (bytes != frameworkBytes) { AllocationAgent.report("diagnostic"); throw new AssertionError("unexplained allocation: " + THREAD_NAMES[thread] + " bytes=" + bytes + " framework=" + frameworkBytes); }
                            }
                        }
                        for (int thread = 0; thread < threadCount; thread++) System.out.println("ATTRIBUTION," + phase + "," + THREAD_NAMES[thread] + "," + AllocationAgent.frameworkBytes(THREAD_IDS[thread]));
                        if (peer != null && !Arrays.equals(RESULT, peer.result)) throw new AssertionError("peer mismatch");
                        if (tracker.getUsed() != 0) throw new AssertionError("native memory retained");
                        for (int stage = 1; stage < 5; stage++) for (int thread = 0; thread < threadCount; thread++) {
                            System.out.println("BYTES," + storage + "," + mode + "," + rows + "," + phase + "," + stage
                                    + "," + THREAD_NAMES[thread] + "," + (BYTES[stage][thread] - BYTES[stage - 1][thread]));
                        }
                        System.out.println("RESULT," + storage + "," + mode + "," + rows + "," + phase + "," + Arrays.toString(RESULT));
                        if (sites) AllocationAgent.report(phase);
                    }
                    long[] actual = RESULT.clone();
                    // Deliberate reporting allocations have their own window. The breaker
                    // interrupts a live build; the same factory must work afterward.
                    io.questdb.cairo.sql.SqlExecutionCircuitBreaker original = context.getCircuitBreaker();
                    FailingBreaker failureBreaker = new FailingBreaker(engine, context);
                    context.with(failureBreaker);
                    AllocationAgent.reset();
                    AllocationAgent.enabled = true;
                    snapshot(0);
                    boolean cancelled = false;
                    try { execute(factory, context); }
                    catch (CairoException failure) { cancelled = failure.isInterruption(); }
                    snapshot(4);
                    AllocationAgent.enabled = false;
                    if (!cancelled || failureBreaker.tracker == null || failureBreaker.tracker.getUsed() != 0) throw new AssertionError("failed cancellation cleanup");
                    context.with(original);
                    for (int thread = 0; thread < threadCount; thread++) System.out.println("FAILURE_BYTES," + THREAD_NAMES[thread] + "," + (BYTES[4][thread] - BYTES[0][thread]));
                    if (sites) AllocationAgent.report("cancellation");
                    execute(factory, context);
                    if (!Arrays.equals(RESULT, actual)) throw new AssertionError("post-cancellation reuse mismatch");
                    context.setParallelHashJoinGroupByEnabled(false);
                    try (RecordCursorFactory baseline = engine.select(sql, context)) { execute(baseline, context); }
                    if (!Arrays.equals(RESULT, actual)) throw new AssertionError("ordinary result mismatch");
                    System.out.println("PASS," + storage + "," + mode + "," + rows);
                }
            } finally { pool.halt(); }
        }
        System.out.println("ROOT," + root);
    }

    private static void load(CairoEngine engine, HashJoinGroupByBenchmark.BenchmarkContext context, String storage, int rows) throws Exception {
        for (String table : new String[]{"r", "p"}) {
            engine.execute("truncate table " + table, context);
            engine.execute("insert into " + table + " select (x/2)::int, x, ('s'||x)::symbol, 1.0, (case when x <= "
                    + rows / 2 + " then 0 else 86_400_000_000 end)::timestamp from long_sequence(" + rows + ")", context);
            engine.execute("insert into " + table + " values (-1,-1,'active',0.0,'1970-01-03')", context);
            if (!storage.equals("native")) engine.execute("alter table " + table + " convert partition to parquet"
                    + (storage.equals("mixed") ? " where ts < '1970-01-02'" : " where ts < '1970-01-03'"), context);
        }
    }

    private static void warmLocalFilterTasks(CairoEngine engine, RecordCursorFactory factory,
                                             HashJoinGroupByBenchmark.BenchmarkContext context, Peer peer) throws Exception {
        // Each filter has one lazy owner-local task, including bounded decoder
        // shells. Normal warmup may never fill its queue when workers keep up.
        // Pin each shard's collector during one small-result setup per owner:
        // after one queue cycle, remaining frames must use the local task.
        // The bind still admits only 1,022 rows; no measured symbols are copied.
        io.questdb.MessageBus bus = engine.getMessageBus();
        SCSequence[] gates = new SCSequence[bus.getPageFrameReduceShardCount()];
        try {
            for (int shard = 0; shard < gates.length; shard++) {
                gates[shard] = new SCSequence();
                bus.getPageFrameCollectFanOut(shard).and(gates[shard]);
            }
            execute(factory, context);
            if (peer != null) {
                peer.request++;
                peer.await();
            }
        } finally {
            for (int shard = 0; shard < gates.length; shard++) {
                if (gates[shard] != null) bus.getPageFrameCollectFanOut(shard).remove(gates[shard]);
            }
        }
    }

    private static void warmSymbolViews(CairoEngine engine, int owners) {
        // Reader interchange can leave both factories' previous expression views
        // attached to one dictionary until their next init. Cover that fixed
        // overlap directly instead of relying on the warmup's scheduling order.
        // This SQL has at most four source SYMBOL references per owner/worker
        // slot (predicate, grouping, COUNT and record lookup), plus one borrowed
        // build-copy view per owner. No dictionary values are read or cached.
        int viewCount = owners * (4 * (WORKERS + 1) + 1);
        TableReader[] readers = new TableReader[owners];
        io.questdb.cairo.sql.SymbolTable[] views = new io.questdb.cairo.sql.SymbolTable[viewCount];
        for (String table : new String[]{"r", "p"}) {
            try {
                for (int owner = 0; owner < owners; owner++) readers[owner] = engine.getReader(table);
                for (TableReader reader : readers) {
                    try {
                        for (int view = 0; view < viewCount; view++) views[view] = reader.newSymbolTable(2);
                    } finally {
                        for (int view = 0; view < viewCount; view++) views[view] = io.questdb.std.Misc.freeIfCloseable(views[view]);
                    }
                }
            } finally {
                for (int owner = 0; owner < owners; owner++) readers[owner] = io.questdb.std.Misc.free(readers[owner]);
            }
        }
        System.out.println("SYMBOL_VIEW_SETUP," + owners + "," + viewCount);
    }

    private static void threads(boolean concurrent) {
        THREAD_IDS[0] = Thread.currentThread().threadId();
        THREAD_NAMES[0] = "owner";
        int count = 1;
        for (ThreadInfo info : BEAN.getThreadInfo(BEAN.getAllThreadIds())) {
            if (info != null && (info.getThreadName().startsWith("alloc_worker_") || info.getThreadName().equals("alloc_peer"))) {
                THREAD_IDS[count] = info.getThreadId();
                THREAD_NAMES[count++] = info.getThreadName();
            }
        }
        if (count != WORKERS + (concurrent ? 2 : 1)) throw new AssertionError("missing workers");
        threadCount = count;
        for (int i = 0; i < 5; i++) snapshot(i);
    }

    private static void snapshot(int stage) {
        for (int i = 0; i < threadCount; i++) BYTES[stage][i] = BEAN.getThreadAllocatedBytes(THREAD_IDS[i]);
    }

    private static void execute(RecordCursorFactory factory, HashJoinGroupByBenchmark.BenchmarkContext context) throws Exception {
        execute(factory, context, RESULT);
    }

    private static void execute(RecordCursorFactory factory, HashJoinGroupByBenchmark.BenchmarkContext context, long[] result) throws Exception {
        io.questdb.std.MemoryTracker tracker;
        try (RecordCursor cursor = factory.getCursor(context)) {
            tracker = context.getMemoryTracker();
            consume(cursor, factory, false, result);
        }
        if (tracker.getUsed() != 0) throw new AssertionError("native memory retained");
    }

    private static void consume(RecordCursor cursor, RecordCursorFactory factory, boolean ready, long[] result) {
        long count = 0, sum = 0, xor = 0;
        Record record = cursor.getRecord();
        while (ready || cursor.hasNext()) {
            ready = false;
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
                        if (symbol != null) for (int c = 0; c < symbol.length(); c++) value = value * 31 + symbol.charAt(c);
                        break;
                    default: throw new AssertionError("unexpected result type");
                }
                hash = hash * 1_000_003 + value;
            }
            count++; sum += hash; xor ^= hash;
        }
        result[0] = count; result[1] = sum; result[2] = xor;
    }
    private static final class FailingBreaker extends io.questdb.cairo.sql.AtomicBooleanCircuitBreaker {
        private final HashJoinGroupByBenchmark.BenchmarkContext context;
        private io.questdb.std.MemoryTracker tracker;
        private int remaining = 100;

        private FailingBreaker(CairoEngine engine, HashJoinGroupByBenchmark.BenchmarkContext context) {
            super(engine);
            this.context = context;
        }

        @Override public void statefulThrowExceptionIfTripped() { statefulThrowExceptionIfTrippedNoThrottle(); }

        @Override public void statefulThrowExceptionIfTrippedNoThrottle() {
            if (--remaining == 0) {
                tracker = context.getMemoryTracker();
                throw CairoException.queryCancelled(-1);
            }
        }
    }

    private static final class Peer extends Thread implements AutoCloseable {
        private final HashJoinGroupByBenchmark.BenchmarkContext context;
        private final RecordCursorFactory factory;
        private final long[] result = new long[3];
        private volatile int request;
        private volatile int completed;
        private volatile boolean stopped;
        private volatile Throwable failure;

        private Peer(CairoEngine engine, String sql) throws Exception {
            super("alloc_peer");
            context = new HashJoinGroupByBenchmark.BenchmarkContext(engine, WORKERS);
            context.with(new io.questdb.griffin.engine.functions.bind.BindVariableServiceImpl(engine.getConfiguration()));
            context.getBindVariableService().setInt("key_limit", 512);
            context.getBindVariableService().setInt("key_floor", 1);
            context.changePageFrameSizes(64, 64);
            context.setJitMode(2);
            context.setParallelHashJoinGroupByEnabled(true);
            long before = BEAN.getCurrentThreadAllocatedBytes();
            factory = engine.select(sql, context);
            long bytes = BEAN.getCurrentThreadAllocatedBytes() - before;
            System.out.println("PEER_COMPILER_BYTES," + bytes);
        }

        @Override public void run() {
            while (!stopped) {
                if (request != completed) {
                    try { execute(factory, context, result); }
                    catch (Throwable th) { failure = th; }
                    completed = request;
                } else {
                    Thread.onSpinWait();
                }
            }
        }

        private void await() {
            while (completed != request) Thread.onSpinWait();
            if (failure != null) throw new AssertionError("peer failed", failure);
        }

        @Override public void close() throws InterruptedException {
            stopped = true;
            join();
            factory.close();
            context.close();
        }
    }

}
