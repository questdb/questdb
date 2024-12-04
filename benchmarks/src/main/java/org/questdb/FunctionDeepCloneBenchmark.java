/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2024 QuestDB
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
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.DefaultCairoConfiguration;
import io.questdb.cairo.CairoEngine;
import io.questdb.cairo.GenericRecordMetadata;
import io.questdb.cairo.TableColumnMetadata;
import io.questdb.cairo.sql.Function;
import io.questdb.griffin.FunctionFactory;
import io.questdb.griffin.FunctionFactoryCache;
import io.questdb.griffin.FunctionParser;
import io.questdb.griffin.SqlCompiler;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContextImpl;
import io.questdb.griffin.engine.functions.FunctionCloneFactory;
import io.questdb.griffin.engine.functions.date.ToStrDateFunctionFactory;
import io.questdb.griffin.engine.functions.groupby.CountDistinctStringGroupByFunctionFactory;
import io.questdb.griffin.model.ExpressionNode;
import io.questdb.griffin.model.QueryModel;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.concurrent.TimeUnit;

@State(Scope.Thread)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
public class FunctionDeepCloneBenchmark {
    private static final CairoConfiguration configuration =
            new DefaultCairoConfiguration(System.getProperty("java.io.tmpdir"));
    private static final ArrayList<FunctionFactory> functions = new ArrayList<>(Arrays.asList(new ToStrDateFunctionFactory(), new CountDistinctStringGroupByFunctionFactory()));
    private static final FunctionParser functionParser = new FunctionParser(configuration, new FunctionFactoryCache(configuration, functions));
    private static SqlExecutionContextImpl ctx;
    private static final GenericRecordMetadata metadata = new GenericRecordMetadata();
    private static ExpressionNode node;
    private static Function function;

    public static void main(String[] args) throws RunnerException {
        Options opt = new OptionsBuilder()
                .include(FunctionDeepCloneBenchmark.class.getSimpleName())
                .warmupIterations(1)
                .measurementIterations(2)
                .forks(1)
                .build();
        new Runner(opt).run();
    }

    @Setup(Level.Trial)
    public void setup() throws SqlException {
        CairoEngine engine = new CairoEngine(configuration);
        ctx = new SqlExecutionContextImpl(engine, 1);
        QueryModel queryModel = QueryModel.FACTORY.newInstance();
        metadata.add(new TableColumnMetadata("a", ColumnType.DATE));

        try (SqlCompiler compiler = engine.getSqlCompiler()) {
            node = compiler.testParseExpression("count_distinct(to_str(a, 'EE, dd-MMM-yyyy hh:mm:ss'))", queryModel);
            function = functionParser.parseFunction(node, metadata, ctx);
        }
    }

    @Benchmark
    public Function deepClone() {
        return FunctionCloneFactory.deepCloneFunction(function);
    }

    @Benchmark
    public Function functionParser() throws SqlException {
        return functionParser.parseFunction(node, metadata, ctx);
    }
}
