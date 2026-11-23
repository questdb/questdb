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
import io.questdb.griffin.SqlCompiler;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContextImpl;
import io.questdb.std.Misc;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Warmup;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Comparator;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

/**
 * Runs queries whose filter stays with a SELECT above a scan and names an output of that SELECT, over native and
 * parquet partitions of a wide table. Which columns such a scan reads depends on how the optimiser passes the names of
 * the filter down: the column the output reads, a column named like the output, one column for the rows, or every
 * column.
 */
@State(Scope.Benchmark)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@Warmup(iterations = 3, time = 1)
@Measurement(iterations = 5, time = 1)
@Fork(value = 1, jvmArgsAppend = {
        "-ea",
        "-Xms2g",
        "-Xmx2g",
        "-XX:+UseParallelGC",
        "--sun-misc-unsafe-memory-access=allow",
        "--enable-native-access=ALL-UNNAMED",
        "--add-exports=java.base/jdk.internal.vm=ALL-UNNAMED"
})
public class FilterOnOutputScanBenchmark {
    private static final int DAY_COUNT = 4;
    private static final int EXTRA_COLUMN_COUNT = 24;
    private static final int ROWS_PER_DAY = 250_000;
    @Param({"NATIVE", "PARQUET"})
    public String partitions;
    @Param({
            // a filter on an output the SELECT computes, named like a column of the table the SELECT does not read
            "SELECT count() FROM (SELECT ts, c0 * 10 b FROM x) WHERE b > 25",
            // a filter on a renamed output above LATEST ON, named like a column the SELECT does not read
            "SELECT count() FROM (SELECT ts, k, c0 AS b FROM x LATEST ON ts PARTITION BY k) WHERE b > 0",
            // a filter on a constant output named like a column of the table
            "SELECT count() FROM (SELECT k, 'c' b FROM (SELECT * FROM x)) WHERE b = 'c'",
            // a filter on a constant output named like no column of the table
            "SELECT count() FROM (SELECT k, 'c' z FROM (SELECT * FROM x)) WHERE z = 'c'",
            // a filter on a constant output named like a column, over a sub-query whose first column is a VARCHAR
            "SELECT count() FROM (SELECT 'c' b FROM (SELECT v, b, c0 FROM x)) WHERE b = 'c'"
    })
    public String query;
    private SqlCompiler compiler;
    private SqlExecutionContextImpl context;
    private CairoEngine engine;
    private RecordCursorFactory factory;
    private Path root;

    @Benchmark
    public long run() throws SqlException {
        long rowCount = 0;
        try (RecordCursor cursor = factory.getCursor(context)) {
            while (cursor.hasNext()) {
                rowCount++;
            }
        }
        return rowCount;
    }

    @Setup(Level.Trial)
    public void setup() throws Exception {
        root = Files.createTempDirectory("filter-on-output-scan-");
        final DefaultCairoConfiguration configuration = new DefaultCairoConfiguration(root.toString());
        engine = new CairoEngine(configuration);
        context = new SqlExecutionContextImpl(engine, 1).with(
                configuration.getFactoryProvider().getSecurityContextFactory().getRootContext(),
                null,
                null,
                -1,
                null
        );
        compiler = engine.getSqlCompiler();
        final StringBuilder ddl = new StringBuilder("CREATE TABLE x (ts TIMESTAMP, v VARCHAR, k SYMBOL, b LONG");
        final StringBuilder insert = new StringBuilder("INSERT INTO x SELECT timestamp_sequence('2024-01-01', ")
                .append(86_400_000_000L / ROWS_PER_DAY)
                .append("), rnd_varchar(8, 24, 0), rnd_symbol(100, 4, 8, 0), rnd_long()");
        for (int i = 0; i < EXTRA_COLUMN_COUNT; i++) {
            ddl.append(", c").append(i).append(" LONG");
            insert.append(", rnd_long()");
        }
        ddl.append(") TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL");
        insert.append(" FROM long_sequence(").append((long) DAY_COUNT * ROWS_PER_DAY).append(')');
        engine.execute(ddl, context);
        engine.execute(insert, context);
        if ("PARQUET".equals(partitions)) {
            // every partition but the last, which stays native as the active one
            engine.execute("ALTER TABLE x CONVERT PARTITION TO PARQUET WHERE ts < '2024-01-0" + DAY_COUNT + "'", context);
        }
        factory = compiler.compile(query, context).getRecordCursorFactory();
    }

    @TearDown(Level.Trial)
    public void tearDown() throws IOException {
        factory = Misc.free(factory);
        compiler = Misc.free(compiler);
        context = Misc.free(context);
        engine = Misc.free(engine);
        io.questdb.std.str.Path.clearThreadLocals();
        if (root != null) {
            try (Stream<Path> paths = Files.walk(root)) {
                for (Path path : paths.sorted(Comparator.reverseOrder()).toList()) {
                    Files.delete(path);
                }
            }
        }
    }
}
