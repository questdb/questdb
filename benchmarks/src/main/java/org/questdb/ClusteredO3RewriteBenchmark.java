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
 *******************************************************************************/

package org.questdb;

import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.CairoEngine;
import io.questdb.cairo.DefaultCairoConfiguration;
import io.questdb.cairo.idx.PostingIndexUtils;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.SqlExecutionContextImpl;
import io.questdb.log.LogFactory;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.options.CommandLineOptions;
import org.openjdk.jmh.runner.options.Options;

import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Measures clustered parquet O3 replacement in two deliberately separate cases:
 * <ul>
 *     <li>{@link #clusteredRewriteDataOnly(DataOnlyState)} rewrites the data parquet and clustered
 *     directory with no secondary index.</li>
 *     <li>{@link #clusteredRewriteWithFullIndexReseal(IndexedState)} performs the same rewrite while
 *     fully resealing a covered posting index. The delta between the two results is the index-reseal
 *     cost; neither result is presented as a performance claim.</li>
 * </ul>
 * <p>
 * Invocation setup creates and clusters a fresh partition and is outside the measured interval.
 * The benchmark invocation is one O3 INSERT whose timestamps fall inside that clustered partition;
 * writer commit therefore performs the complete replacement. Invocation teardown drops the table.
 * <p>
 * Tunables: {@code clustered.o3.bench.rows} (default 100,000),
 * {@code clustered.o3.bench.late.rows} (default 10,000), and
 * {@code clustered.o3.bench.keys} (default 64).
 * <p>
 * Build and run a short report:
 * <pre>
 * mvn -pl benchmarks -am package -DskipTests
 * java --add-exports=java.base/jdk.internal.vm=ALL-UNNAMED \
 *      --add-opens=java.base/java.lang=ALL-UNNAMED \
 *      --sun-misc-unsafe-memory-access=allow --enable-native-access=ALL-UNNAMED \
 *      -Dclustered.o3.bench.rows=100000 -Dclustered.o3.bench.late.rows=10000 \
 *      -cp benchmarks/target/benchmarks.jar org.questdb.ClusteredO3RewriteBenchmark \
 *      -wi 1 -i 3
 * </pre>
 */
@BenchmarkMode(Mode.SingleShotTime)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@Warmup(iterations = 1)
@Measurement(iterations = 3)
@Fork(0)
public class ClusteredO3RewriteBenchmark {
    private static final int KEYS = Integer.getInteger("clustered.o3.bench.keys", 64);
    private static final long LATE_ROWS = Long.getLong("clustered.o3.bench.late.rows", 10_000L);
    private static final long ROWS = Long.getLong("clustered.o3.bench.rows", 100_000L);
    private static final String ROOT = System.getProperty("java.io.tmpdir")
            + java.io.File.separator
            + "clustered-o3-rewrite-bench";
    private static final AtomicLong TABLE_ID = new AtomicLong();
    private static final CairoConfiguration CONFIGURATION = new DefaultCairoConfiguration(ROOT) {
        @Override
        public byte getPostingIndexParquetPartitionFormat() {
            return PostingIndexUtils.PARQUET_INDEX_FORMAT_PARQUET;
        }
    };
    private static CairoEngine engine;
    private static SqlExecutionContext executionContext;

    public static void main(String[] args) throws Exception {
        Files.createDirectories(Paths.get(ROOT));
        ensureEngine();
        try {
            final String[] jmhArgs = new String[args.length + 1];
            jmhArgs[0] = ClusteredO3RewriteBenchmark.class.getSimpleName();
            System.arraycopy(args, 0, jmhArgs, 1, args.length);
            final Options options = new CommandLineOptions(jmhArgs);
            new Runner(options).run();
        } finally {
            if (engine != null) {
                engine.close();
            }
            LogFactory.haltInstance();
        }
    }

    @Benchmark
    public void clusteredRewriteDataOnly(DataOnlyState state) throws Exception {
        engine.execute(state.getLateInsertSql(), executionContext);
    }

    @Benchmark
    public void clusteredRewriteWithFullIndexReseal(IndexedState state) throws Exception {
        engine.execute(state.getLateInsertSql(), executionContext);
    }

    private static synchronized void ensureEngine() {
        if (engine == null) {
            engine = new CairoEngine(CONFIGURATION);
            executionContext = new SqlExecutionContextImpl(engine, 1).with(
                    CONFIGURATION.getFactoryProvider().getSecurityContextFactory().getRootContext(),
                    null,
                    null,
                    -1,
                    null
            );
        }
    }

    public abstract static class RewriteState {
        private String lateInsertSql;
        private String tableName;

        protected abstract boolean indexed();

        final String getLateInsertSql() {
            return lateInsertSql;
        }

        @Setup(Level.Invocation)
        public void setUp() throws Exception {
            ensureEngine();
            tableName = "clustered_o3_" + TABLE_ID.incrementAndGet();
            final String columns = indexed()
                    ? "(k symbol, p symbol index type posting include (n, ts), n long, ts timestamp)"
                    : "(k symbol, p symbol, n long, ts timestamp)";
            engine.execute(
                    "create table " + tableName + " " + columns
                            + " timestamp(ts) partition by day order by k",
                    executionContext
            );
            final String initialProjection = "select ('K' || (x % " + KEYS + "))::symbol,"
                    + " ('P' || (x % 16))::symbol, x,"
                    + " ('2024-01-01'::timestamp + x * 500L)::timestamp";
            engine.execute(
                    "insert into " + tableName + " " + initialProjection + " from long_sequence(" + ROWS + ")",
                    executionContext
            );
            engine.execute(
                    "insert into " + tableName
                            + " values ('TAIL', 'P0', 0, '2024-01-02T00:00:00.000000Z')",
                    executionContext
            );
            engine.execute(
                    "alter table " + tableName + " convert partition to parquet list '2024-01-01'",
                    executionContext
            );
            lateInsertSql = "insert into " + tableName
                    + " select ('L' || (x % 8))::symbol, ('P' || (x % 16))::symbol, -x,"
                    + " ('2024-01-01'::timestamp + x * 750L)::timestamp"
                    + " from long_sequence(" + LATE_ROWS + ")";
        }

        @TearDown(Level.Invocation)
        public void tearDown() throws Exception {
            if (tableName != null) {
                engine.execute("drop table if exists " + tableName, executionContext);
                engine.releaseInactive();
                tableName = null;
            }
        }
    }

    @State(Scope.Benchmark)
    public static class DataOnlyState extends RewriteState {
        @Override
        protected boolean indexed() {
            return false;
        }
    }

    @State(Scope.Benchmark)
    public static class IndexedState extends RewriteState {
        @Override
        protected boolean indexed() {
            return true;
        }
    }
}
