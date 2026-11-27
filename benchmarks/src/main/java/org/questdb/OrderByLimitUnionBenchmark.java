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
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.griffin.SqlCompiler;
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
import org.openjdk.jmh.infra.Blackhole;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Comparator;
import java.util.concurrent.TimeUnit;

@State(Scope.Benchmark)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@Warmup(iterations = 5, time = 1)
@Measurement(iterations = 5, time = 1)
@Fork(2)
public class OrderByLimitUnionBenchmark {
    @Param({"10", "1000"})
    public int limit;
    @Param({"1000000"})
    public int rowCount;
    @Param({"long_sorted", "long_reverse", "long_random", "varchar_sorted", "varchar_reverse", "varchar_random", "computed_varchar_reverse"})
    public String shape;
    private SqlCompiler compiler;
    private SqlExecutionContextImpl context;
    private CairoEngine engine;
    private RecordCursorFactory factory;
    private boolean isVarchar;
    private Path root;

    @Benchmark
    public void topK(Blackhole blackhole) throws Exception {
        try (RecordCursor cursor = factory.getCursor(context)) {
            final Record record = cursor.getRecord();
            while (cursor.hasNext()) {
                if (isVarchar) {
                    blackhole.consume(record.getVarcharA(0));
                } else {
                    blackhole.consume(record.getLong(0));
                }
            }
        }
    }

    @Setup(Level.Trial)
    public void setup() throws Exception {
        root = Files.createTempDirectory("order-by-limit-union");
        final CairoConfiguration configuration = new DefaultCairoConfiguration(root.toString());
        engine = new CairoEngine(configuration);
        context = new SqlExecutionContextImpl(engine, 1)
                .with(configuration.getFactoryProvider().getSecurityContextFactory().getRootContext());
        compiler = engine.getSqlCompiler();
        engine.execute("create table t as (select x l, -x sorted, (x * 7919) % 100003 rnd, "
                + "('sensor_' || (10_000_000 - x))::varchar vsorted, ('sensor_' || (1_000_000 + x))::varchar vreverse, "
                + "('sensor_' || ((x * 7919) % 100003))::varchar vrnd from long_sequence(" + rowCount + "))", context);
        final String column = switch (shape) {
            case "long_sorted" -> "sorted";
            case "long_reverse" -> "l";
            case "long_random" -> "rnd";
            case "varchar_sorted" -> "vsorted";
            case "varchar_reverse" -> "vreverse";
            case "varchar_random" -> "vrnd";
            case "computed_varchar_reverse" -> "('sensor_' || (1_000_000 + l))::varchar";
            default -> throw new IllegalArgumentException("unknown shape: " + shape);
        };
        isVarchar = shape.contains("varchar");
        factory = compiler.compile("select " + column + " k from (select * from t where l <= " + rowCount / 2
                + " union all select * from t where l > " + rowCount / 2 + ") order by k desc limit " + limit, context).getRecordCursorFactory();
    }

    @TearDown(Level.Trial)
    public void tearDown() throws Exception {
        factory = Misc.free(factory);
        compiler = Misc.free(compiler);
        context = Misc.free(context);
        engine = Misc.free(engine);
        try (var paths = Files.walk(root)) {
            for (Path path : paths.sorted(Comparator.reverseOrder()).toList()) {
                Files.deleteIfExists(path);
            }
        }
    }
}
