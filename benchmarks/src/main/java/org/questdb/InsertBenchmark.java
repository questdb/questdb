package org.questdb;

import org.openjdk.jmh.annotations.*;
import org.questdb.test.tools.TestUtils;

import java.util.concurrent.TimeUnit;

@State(Scope.Thread)
@Warmup(iterations = 5, time = 1)
@Measurement(iterations = 5, time = 1)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
public class InsertBenchmark {
    private final static int BATCH_SIZE = 10000;
    private final static int NUM_INSERTS = 1000000;

    @State(Scope.Benchmark)
    public static class BenchmarkState {
        private final static String TABLE_NAME = "test";
        private final static String CREATE_TABLE_SQL = "create table test (timestamp timestamp, value double)";
        private final static String INSERT_SQL = "insert into test values (?, ?)";

        private final CairoEngine engine = new CairoEngine(new DefaultCairoConfiguration("target/bench", TestUtils.SMAP_DEFAULT));
        private final SqlCompiler compiler = new SqlCompiler(engine);
        private final SqlExecutionContextImpl sqlExecutionContext = new SqlExecutionContextImpl(engine, 1);
        private final CompiledQuery query = compiler.compile(INSERT_SQL, sqlExecutionContext);

        @Setup(Level.Iteration)
        public void setUp() {
            engine.reset();
            engine.createTable(CREATE_TABLE_SQL, sqlExecutionContext);
        }

        @TearDown(Level.Iteration)
        public void tearDown() {
            engine.releaseAllReaders();
            engine.releaseAllWriters();
        }
    }

    @Benchmark
    public void testInsert(BenchmarkState state) {
        try (TableWriter writer = state.engine.getWriter(state.sqlExecutionContext.getCairoSecurityContext(), "test")) {
            writer.reset();
            long timestamp = System.currentTimeMillis();
            for (int i = 0; i < NUM_INSERTS; i++) {
                writer.append(timestamp + i, i);
                if (i % BATCH_SIZE == 0) {
                    writer.commit();
                }
            }
            writer.commit();
        }
    }
}
