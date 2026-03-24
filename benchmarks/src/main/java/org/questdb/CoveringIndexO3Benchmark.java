package org.questdb;

import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.CairoEngine;
import io.questdb.cairo.DefaultCairoConfiguration;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.griffin.SqlCompilerImpl;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContextImpl;
import io.questdb.log.LogFactory;
import org.openjdk.jmh.annotations.*;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;

import java.util.concurrent.TimeUnit;

@State(Scope.Benchmark)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
public class CoveringIndexO3Benchmark {

    private static final int KEY_COUNT = 50;
    private static final int ROWS_PER_PARTITION = 5_000; // ~100 rows/key
    private static final int PARTITIONS = 10;
    private static final long TOTAL_ROWS = (long) ROWS_PER_PARTITION * PARTITIONS;
    private static final CairoConfiguration configuration = new DefaultCairoConfiguration(System.getProperty("java.io.tmpdir"));

    @Param({"covering_where", "covering_latest", "covering_in", "distinct", "non_covering"})
    public String queryType;

    private SqlCompilerImpl compiler;
    private SqlExecutionContextImpl ctx;
    private CairoEngine engine;
    private RecordCursorFactory factory;

    public static void main(String[] args) throws RunnerException {
        try (CairoEngine engine = new CairoEngine(configuration)) {
            SqlExecutionContextImpl ctx = new SqlExecutionContextImpl(engine, 1)
                    .with(configuration.getFactoryProvider().getSecurityContextFactory().getRootContext(),
                            null, null, -1, null);
            try {
                // Build rnd_symbol args: 'K0','K1',...,'K49'
                StringBuilder symArgs = new StringBuilder();
                for (int k = 0; k < KEY_COUNT; k++) {
                    if (k > 0) symArgs.append(',');
                    symArgs.append("'K").append(k).append("'");
                }

                // Create table with in-order data (partitions 2-10)
                long inOrderRows = (long) ROWS_PER_PARTITION * (PARTITIONS - 1);
                engine.execute(
                        "CREATE TABLE IF NOT EXISTS o3bench AS ("
                                + " SELECT dateadd('s', x::INT, '2024-01-02')::TIMESTAMP ts,"
                                + "   rnd_symbol(" + symArgs + ") sym,"
                                + "   rnd_double() price, rnd_int() qty"
                                + " FROM long_sequence(" + inOrderRows + ")"
                                + ") TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL", ctx);

                engine.execute("ALTER TABLE o3bench ALTER COLUMN sym ADD INDEX TYPE POSTING INCLUDE (price, qty)", ctx);
                engine.releaseAllWriters();

                // O3 insert: day 1 (before all existing data)
                engine.execute(
                        "INSERT INTO o3bench SELECT dateadd('s', x::INT, '2024-01-01')::TIMESTAMP ts,"
                                + "   rnd_symbol(" + symArgs + ") sym,"
                                + "   rnd_double() price, rnd_int() qty"
                                + " FROM long_sequence(" + ROWS_PER_PARTITION + ")", ctx);

                engine.releaseAllWriters();
                System.out.printf("Data: %d keys x %d rows/partition x %d partitions = %,d rows%n",
                        KEY_COUNT, ROWS_PER_PARTITION, PARTITIONS, TOTAL_ROWS);
            } catch (SqlException e) {
                e.printStackTrace();
            }
        }

        Options opt = new OptionsBuilder()
                .include(CoveringIndexO3Benchmark.class.getSimpleName())
                .warmupIterations(5)
                .measurementIterations(10)
                .forks(1)
                .build();
        new Runner(opt).run();
        LogFactory.haltInstance();
    }

    @Setup(Level.Iteration)
    public void setup() throws Exception {
        engine = new CairoEngine(configuration);
        ctx = new SqlExecutionContextImpl(engine, 1).with(
                configuration.getFactoryProvider().getSecurityContextFactory().getRootContext(),
                null, null, -1, null);
        compiler = new SqlCompilerImpl(engine);
        String sql = switch (queryType) {
            case "covering_where" -> "SELECT price, qty FROM o3bench WHERE sym = 'K0'";
            case "covering_latest" -> "SELECT price, qty FROM o3bench WHERE sym = 'K0' LATEST ON ts PARTITION BY sym";
            case "covering_in" -> "SELECT price FROM o3bench WHERE sym IN ('K0', 'K1', 'K2')";
            case "distinct" -> "SELECT DISTINCT sym FROM o3bench";
            case "non_covering" -> "SELECT * FROM o3bench WHERE sym = 'K0'";
            default -> throw new IllegalStateException("Unknown: " + queryType);
        };
        factory = compiler.compile(sql, ctx).getRecordCursorFactory();
    }

    @TearDown(Level.Iteration)
    public void tearDown() {
        factory.close();
        compiler.close();
        engine.close();
    }

    @Benchmark
    public long query() throws SqlException {
        long count = 0;
        try (RecordCursor cursor = factory.getCursor(ctx)) {
            while (cursor.hasNext()) {
                count++;
            }
        }
        return count;
    }
}
