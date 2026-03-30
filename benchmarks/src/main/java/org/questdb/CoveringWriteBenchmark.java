package org.questdb;

import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.CairoEngine;
import io.questdb.cairo.DefaultCairoConfiguration;
import io.questdb.cairo.O3PartitionJob;
import io.questdb.griffin.SqlCompilerImpl;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContextImpl;
import io.questdb.log.LogFactory;
import io.questdb.std.MemoryTag;
import io.questdb.std.Misc;
import io.questdb.std.Unsafe;
import org.openjdk.jmh.annotations.*;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;

import java.io.File;
import java.io.IOException;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Measures INSERT performance with and without a covering index.
 * Each iteration inserts BATCH_SIZE rows into a fresh table.
 */
@State(Scope.Benchmark)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@Warmup(iterations = 3, time = 1)
@Measurement(iterations = 5, time = 1)
@Fork(value = 1, jvmArgs = {"-Xmx4g"})
public class CoveringWriteBenchmark {

    private static final int BATCH_SIZE = 100_000;
    private static final int KEY_COUNT = 50;

    @Param({
            "no_index",
            "bitmap_index",
            "posting_index",
            "posting_covering_double",
            "posting_covering_varchar"
    })
    public String tableType;

    private CairoConfiguration configuration;
    private Path tmpDir;
    private int iteration;

    public static void main(String[] args) throws RunnerException {
        Options opt = new OptionsBuilder()
                .include(CoveringWriteBenchmark.class.getSimpleName())
                .build();
        new Runner(opt).run();
        LogFactory.haltInstance();
    }

    @Benchmark
    public long insert() throws SqlException, IOException {
        iteration++;
        String tableName = "bench_" + iteration;
        Path dbDir = Files.createTempDirectory(tmpDir, "iter");
        CairoConfiguration cfg = new DefaultCairoConfiguration(dbDir.toString());

        try (CairoEngine engine = new CairoEngine(cfg)) {
            SqlExecutionContextImpl ctx = new SqlExecutionContextImpl(engine, 1)
                    .with(cfg.getFactoryProvider().getSecurityContextFactory().getRootContext(),
                            null, null, -1, null);
            // Suppress logging in forked JVM
            LogFactory.haltInstance();

            StringBuilder symArgs = new StringBuilder();
            for (int k = 0; k < KEY_COUNT; k++) {
                if (k > 0) symArgs.append(',');
                symArgs.append("'K").append(k).append("'");
            }

            String ddl = switch (tableType) {
                case "no_index" -> """
                        CREATE TABLE %s (
                            ts TIMESTAMP, sym SYMBOL, price DOUBLE, name VARCHAR
                        ) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL
                        """.formatted(tableName);
                case "bitmap_index" -> """
                        CREATE TABLE %s (
                            ts TIMESTAMP, sym SYMBOL INDEX, price DOUBLE, name VARCHAR
                        ) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL
                        """.formatted(tableName);
                case "posting_index" -> """
                        CREATE TABLE %s (
                            ts TIMESTAMP, sym SYMBOL INDEX TYPE POSTING, price DOUBLE, name VARCHAR
                        ) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL
                        """.formatted(tableName);
                case "posting_covering_double" -> """
                        CREATE TABLE %s (
                            ts TIMESTAMP, sym SYMBOL INDEX TYPE POSTING INCLUDE (price), price DOUBLE, name VARCHAR
                        ) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL
                        """.formatted(tableName);
                case "posting_covering_varchar" -> """
                        CREATE TABLE %s (
                            ts TIMESTAMP, sym SYMBOL INDEX TYPE POSTING INCLUDE (price, name), price DOUBLE, name VARCHAR
                        ) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL
                        """.formatted(tableName);
                default -> throw new IllegalStateException("Unknown: " + tableType);
            };

            engine.execute(ddl, ctx);
            engine.execute(
                    "INSERT INTO " + tableName
                            + " SELECT dateadd('s', x::INT, '2024-01-01')::TIMESTAMP,"
                            + "   rnd_symbol(" + symArgs + "),"
                            + "   rnd_double() * 100,"
                            + "   rnd_str('order_alpha_2024','order_beta_2024','order_gamma_2024','order_delta_2024')"
                            + " FROM long_sequence(" + BATCH_SIZE + ")", ctx);
            engine.releaseAllWriters();
        }
        // Measure table size
        long size = dirSize(dbDir);
        deleteDir(dbDir.toFile());
        return size;
    }

    @Setup(Level.Trial)
    public void setup() throws IOException {
        tmpDir = Files.createTempDirectory("covering-write-bench");
        iteration = 0;
        LogFactory.haltInstance();
    }

    @TearDown(Level.Trial)
    public void teardown() {
        deleteDir(tmpDir.toFile());
    }

    private static long dirSize(Path dir) throws IOException {
        AtomicLong size = new AtomicLong();
        Files.walkFileTree(dir, new SimpleFileVisitor<>() {
            @Override
            public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) {
                size.addAndGet(attrs.size());
                return FileVisitResult.CONTINUE;
            }
        });
        return size.get();
    }

    private static void deleteDir(File dir) {
        File[] files = dir.listFiles();
        if (files != null) {
            for (File f : files) {
                if (f.isDirectory()) {
                    deleteDir(f);
                } else {
                    f.delete();
                }
            }
        }
        dir.delete();
    }
}
