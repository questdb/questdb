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

import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.CairoEngine;
import io.questdb.cairo.DefaultCairoConfiguration;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.griffin.SqlCompiler;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContextImpl;
import io.questdb.griffin.engine.functions.bind.BindVariableServiceImpl;
import io.questdb.log.LogFactory;
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
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Comparator;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

/**
 * Compile time of long chains of joins over empty tables, as generated SQL writes them. Each operation
 * compiles the query text and closes the factory without opening a cursor, so it covers the parser,
 * the optimiser's join ordering and code generation. The shapes:
 * <ul>
 *     <li>{@code cross_full}: t0 CROSS JOIN t1 FULL JOIN t2 ON t2.k = t1.k CROSS JOIN t3 FULL JOIN t4 ...</li>
 *     <li>{@code cross_right_t0}: t0 CROSS JOIN t1 RIGHT JOIN t2 ON t2.k = t0.k CROSS JOIN t3 RIGHT JOIN t4 ON t4.k = t0.k ...</li>
 *     <li>{@code cross_inner_full}: t0 CROSS JOIN t1 JOIN t2 ON t2.k = t1.k FULL JOIN t3 ON t3.k = t2.k CROSS JOIN t4 ...</li>
 *     <li>{@code cross_full_non_equi}: t0 CROSS JOIN t1 FULL JOIN t2 ON t2.k &gt; t1.k CROSS JOIN t3 ...</li>
 *     <li>{@code full}: t0 FULL JOIN t1 ON t1.k = t0.k FULL JOIN t2 ON t2.k = t1.k ..., no CROSS join</li>
 * </ul>
 * The class uses only APIs that predate it, so it runs unchanged against a base commit.
 */
@State(Scope.Thread)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@Warmup(iterations = 3, time = 2)
@Measurement(iterations = 5, time = 2)
@Fork(value = 1, jvmArgsAppend = {
        "-Xms2g",
        "-Xmx2g",
        "-XX:+UseParallelGC",
        "--sun-misc-unsafe-memory-access=allow",
        "--enable-native-access=ALL-UNNAMED",
        "--add-opens=java.base/java.lang=ALL-UNNAMED",
        "--add-opens=java.base/java.lang.reflect=ALL-UNNAMED",
        "--add-opens=java.base/java.nio=ALL-UNNAMED",
        "--add-opens=java.base/java.time.zone=ALL-UNNAMED",
        "--add-exports=java.base/jdk.internal.vm=ALL-UNNAMED"
})
public class JoinChainCompileBenchmark {
    @Param({"cross_full", "cross_right_t0", "cross_inner_full", "cross_full_non_equi", "full"})
    public String shape;
    @Param({"320", "1280"})
    public int tables;
    private SqlCompiler compiler;
    private SqlExecutionContextImpl ctx;
    private CairoEngine engine;
    private Path root;
    private String sql;

    public static void main(String[] args) throws RunnerException {
        Options opt = new OptionsBuilder()
                .include(JoinChainCompileBenchmark.class.getSimpleName())
                .build();
        new Runner(opt).run();
        LogFactory.haltInstance();
    }

    @Benchmark
    public int compile() throws SqlException {
        try (RecordCursorFactory factory = compiler.compile(sql, ctx).getRecordCursorFactory()) {
            return factory.getMetadata().getColumnCount();
        }
    }

    @Setup(Level.Trial)
    public void setup() throws Exception {
        root = Files.createTempDirectory("join-chain-compile-bench-");
        final CairoConfiguration configuration = new DefaultCairoConfiguration(root.toString());
        engine = new CairoEngine(configuration);
        ctx = new SqlExecutionContextImpl(engine, 1).with(
                configuration.getFactoryProvider().getSecurityContextFactory().getRootContext(),
                new BindVariableServiceImpl(configuration),
                null,
                -1,
                null
        );
        compiler = engine.getSqlCompiler();
        for (int i = 0; i < tables; i++) {
            engine.execute("CREATE TABLE t" + i + " (k INT)", ctx);
        }
        final StringBuilder sb = new StringBuilder("SELECT count(*) FROM t0");
        for (int i = 1; i < tables; i++) {
            switch (shape) {
                case "cross_full" -> appendCrossOrOuter(sb, i, i % 2 == 1, "FULL", "=", i - 1);
                case "cross_right_t0" -> appendCrossOrOuter(sb, i, i % 2 == 1, "RIGHT", "=", 0);
                case "cross_full_non_equi" -> appendCrossOrOuter(sb, i, i % 2 == 1, "FULL", ">", i - 1);
                case "cross_inner_full" -> {
                    switch (i % 3) {
                        case 1 -> sb.append(" CROSS JOIN t").append(i);
                        case 2 -> sb.append(" JOIN t").append(i).append(" ON t").append(i).append(".k = t").append(i - 1).append(".k");
                        default -> appendCrossOrOuter(sb, i, false, "FULL", "=", i - 1);
                    }
                }
                case "full" -> appendCrossOrOuter(sb, i, false, "FULL", "=", i - 1);
                default -> throw new IllegalArgumentException("unknown shape: " + shape);
            }
        }
        sql = sb.toString();
    }

    @TearDown(Level.Trial)
    public void tearDown() throws IOException {
        compiler.close();
        engine.close();
        try (Stream<Path> paths = Files.walk(root)) {
            paths.sorted(Comparator.reverseOrder()).forEach(path -> path.toFile().delete());
        }
    }

    private static void appendCrossOrOuter(StringBuilder sb, int table, boolean isCross, String outerJoin, String op, int keyTable) {
        if (isCross) {
            sb.append(" CROSS JOIN t").append(table);
        } else {
            sb.append(' ').append(outerJoin).append(" JOIN t").append(table)
                    .append(" ON t").append(table).append(".k ").append(op).append(" t").append(keyTable).append(".k");
        }
    }
}
