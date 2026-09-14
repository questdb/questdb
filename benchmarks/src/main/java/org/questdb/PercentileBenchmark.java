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
 ******************************************************************************/

package org.questdb;

import io.questdb.cairo.CairoEngine;
import io.questdb.cairo.DefaultCairoConfiguration;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.griffin.SqlCompilerImpl;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.SqlExecutionContextImpl;
import io.questdb.log.LogFactory;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.util.Arrays;

/**
 * Standalone benchmark for percentile functions.
 * Measures in-process execution time for percentile_disc, percentile_cont,
 * and approx_percentile with various configurations.
 * <p>
 * Usage: java -cp ... org.questdb.PercentileBenchmark [rows]
 * Default: 1_000_000 and 10_000_000 rows
 */
public class PercentileBenchmark {

    private static final int WARMUP_ITERATIONS = 3;
    private static final int MEASUREMENT_ITERATIONS = 7;

    private static final String[] QUERY_LABELS = {
            "percentile_disc(val, 0.95)",
            "percentile_cont(val, 0.95)",
            "approx_percentile(val, 0.95)",
            "percentile_disc(val, 0.95) GROUP BY",
            "approx_percentile(val, 0.95) GROUP BY",
            "percentile_disc(val, ARRAY[5 quantiles])",
            "approx_percentile(val, ARRAY[5 quantiles])",
    };

    private static final String[] QUERIES = {
            "SELECT percentile_disc(val, 0.95) FROM t",
            "SELECT percentile_cont(val, 0.95) FROM t",
            "SELECT approx_percentile(val, 0.95) FROM t",
            "SELECT grp, percentile_disc(val, 0.95) FROM t GROUP BY grp",
            "SELECT grp, approx_percentile(val, 0.95) FROM t GROUP BY grp",
            "SELECT percentile_disc(val, ARRAY[0.25, 0.5, 0.75, 0.95, 0.99]) FROM t",
            "SELECT approx_percentile(val, ARRAY[0.25, 0.5, 0.75, 0.95, 0.99]) FROM t",
    };

    public static void main(String[] args) throws IOException {
        System.out.println("QuestDB Percentile Benchmark");
        System.out.println("============================");
        System.out.println("Warmup: " + WARMUP_ITERATIONS + " iterations, Measurement: " + MEASUREMENT_ITERATIONS + " iterations");
        System.out.println();

        int[] sizes;
        if (args.length > 0) {
            sizes = new int[args.length];
            for (int i = 0; i < args.length; i++) {
                sizes[i] = Integer.parseInt(args[i].replace("_", ""));
            }
        } else {
            sizes = new int[]{1_000_000, 10_000_000};
        }

        for (int size : sizes) {
            runBenchmarkSuite(size);
        }

        LogFactory.haltInstance();
    }

    private static long benchmarkQuery(CairoEngine engine, SqlExecutionContext ctx, String query) throws SqlException {
        try (SqlCompilerImpl compiler = new SqlCompilerImpl(engine)) {
            try (RecordCursorFactory factory = compiler.compile(query, ctx).getRecordCursorFactory()) {
                long start = System.nanoTime();
                try (RecordCursor cursor = factory.getCursor(ctx)) {
                    while (cursor.hasNext()) {
                        // consume all rows
                    }
                }
                return System.nanoTime() - start;
            }
        }
    }

    private static File createTempDir() throws IOException {
        Path tmpDir = java.nio.file.Files.createTempDirectory("questdb-bench-");
        File dir = tmpDir.toFile();
        dir.deleteOnExit();
        return dir;
    }

    private static void deleteDir(File dir) {
        if (dir.exists()) {
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

    private static double medianMs(long[] nanos) {
        long[] sorted = nanos.clone();
        Arrays.sort(sorted);
        int mid = sorted.length / 2;
        long medianNanos = sorted.length % 2 == 0
                ? (sorted[mid - 1] + sorted[mid]) / 2
                : sorted[mid];
        return medianNanos / 1_000_000.0;
    }

    private static void runBenchmarkSuite(int numRows) throws IOException {
        String formattedSize = String.format("%,d", numRows);
        System.out.println("Dataset: " + formattedSize + " rows");
        System.out.println("-".repeat(75));
        System.out.printf("%-50s %12s%n", "Query", "Median(ms)");
        System.out.println("-".repeat(75));

        File tmpDir = createTempDir();
        try {
            DefaultCairoConfiguration config = new DefaultCairoConfiguration(tmpDir.getAbsolutePath());
            try (CairoEngine engine = new CairoEngine(config)) {
                SqlExecutionContext ctx = new SqlExecutionContextImpl(engine, 1)
                        .with(
                                config.getFactoryProvider().getSecurityContextFactory().getRootContext(),
                                null,
                                null,
                                -1,
                                null
                        );

                // Create table with random data
                String createSql = "CREATE TABLE t AS (" +
                        "SELECT rnd_double() * 1000 AS val, " +
                        "rnd_symbol('a','b','c','d','e','f','g','h','i','j') AS grp " +
                        "FROM long_sequence(" + numRows + ")" +
                        ")";

                System.out.print("Creating table... ");
                long createStart = System.nanoTime();
                engine.execute(createSql, ctx);
                System.out.printf("done (%.1f ms)%n%n", (System.nanoTime() - createStart) / 1_000_000.0);

                // Run each query benchmark
                for (int q = 0; q < QUERIES.length; q++) {
                    long[] measurements = new long[MEASUREMENT_ITERATIONS];

                    // Warmup
                    for (int i = 0; i < WARMUP_ITERATIONS; i++) {
                        benchmarkQuery(engine, ctx, QUERIES[q]);
                    }

                    // Measure
                    for (int i = 0; i < MEASUREMENT_ITERATIONS; i++) {
                        measurements[i] = benchmarkQuery(engine, ctx, QUERIES[q]);
                    }

                    double median = medianMs(measurements);
                    System.out.printf("%-50s %12.1f%n", QUERY_LABELS[q], median);
                }
            }
        } catch (SqlException e) {
            System.err.println("SQL error: " + e.getMessage());
            e.printStackTrace();
        } finally {
            deleteDir(tmpDir);
        }
        System.out.println();
    }
}
