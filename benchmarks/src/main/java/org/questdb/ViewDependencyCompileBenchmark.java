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
import io.questdb.griffin.SqlCompiler;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContextImpl;
import io.questdb.griffin.engine.functions.bind.BindVariableServiceImpl;
import io.questdb.griffin.model.ExecutionModel;
import io.questdb.std.LowerCaseCharSequenceHashSet;
import io.questdb.std.LowerCaseCharSequenceObjHashMap;
import io.questdb.std.Misc;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
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

/** Compiles view definitions with independent wildcard holders, without timing definition-file writes. */
@State(Scope.Thread)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@Warmup(iterations = 3, time = 1)
@Measurement(iterations = 5, time = 1)
@Fork(value = 2, jvmArgsAppend = {
        "-ea",
        "-Xms2g",
        "-Xmx2g",
        "-XX:+UseParallelGC",
        "--sun-misc-unsafe-memory-access=allow",
        "--enable-native-access=ALL-UNNAMED",
        "--add-exports=java.base/jdk.internal.vm=ALL-UNNAMED"
})
public class ViewDependencyCompileBenchmark {
    @Param({"1", "64", "1024"})
    public int holderCount;
    @Param({"false", "true"})
    public boolean isSameTable;
    private final LowerCaseCharSequenceObjHashMap<LowerCaseCharSequenceHashSet> dependencies = new LowerCaseCharSequenceObjHashMap<>();
    private SqlCompiler compiler;
    private SqlExecutionContextImpl context;
    private CairoEngine engine;
    private Path root;
    private String sql;

    @Benchmark
    public ExecutionModel compile() throws SqlException {
        dependencies.clear();
        return compiler.generateViewExecutionModel(sql, context, dependencies);
    }

    @Setup
    public void setup() throws Exception {
        root = Files.createTempDirectory("view-dependency-compile-");
        final DefaultCairoConfiguration configuration = new DefaultCairoConfiguration(root.toString());
        engine = new CairoEngine(configuration);
        engine.load();
        context = new SqlExecutionContextImpl(engine, 1).with(
                configuration.getFactoryProvider().getSecurityContextFactory().getRootContext(),
                new BindVariableServiceImpl(configuration),
                null,
                -1,
                null
        );
        compiler = engine.getSqlCompiler();
        for (int i = 0, n = isSameTable ? 1 : holderCount; i < n; i++) {
            engine.execute("CREATE TABLE w" + i + " (a LONG)", context);
        }
        sql = wildcardHolders(0, holderCount);
    }

    @TearDown
    public void tearDown() throws IOException {
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

    private String wildcardHolders(int lo, int hi) {
        if (hi - lo == 1) {
            return "SELECT count() FROM (SELECT * FROM w" + (isSameTable ? 0 : lo) + ")";
        }
        final int mid = (lo + hi) / 2;
        return "SELECT * FROM (" + wildcardHolders(lo, mid)
                + ") UNION ALL SELECT * FROM (" + wildcardHolders(mid, hi) + ")";
    }
}
