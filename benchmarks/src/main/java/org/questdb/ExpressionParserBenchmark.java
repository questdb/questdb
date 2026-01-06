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

import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.CairoEngine;
import io.questdb.cairo.DefaultCairoConfiguration;
import io.questdb.griffin.ExpressionParserListener;
import io.questdb.griffin.SqlCompiler;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.model.ExpressionNode;
import io.questdb.log.LogFactory;
import io.questdb.std.str.StringSink;
import org.openjdk.jmh.annotations.*;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;

import java.util.concurrent.TimeUnit;

@State(Scope.Thread)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
public class ExpressionParserBenchmark {

    private static final CairoConfiguration configuration = new DefaultCairoConfiguration(System.getProperty("java.io.tmpdir"));

    @Param({
            "a + b",
            "a + b * c / 2",
            "a + b * c(x, y) / 2",
            "a = 1 and b = 2 or c = 3",
            "case when a > 0 then 'positive' when a < 0 then 'negative' else 'zero' end",
            "a in (1, 2, 3, 4, 5)",
            "cast(a as double) + cast(b as long)",
            "a between 1 and 10 and b like '%test%'",
            "coalesce(a, b, c, d, 0)",
            "sum(a) over (partition by b order by c rows between unbounded preceding and current row)"
    })
    public String expression;

    private CairoEngine engine;
    private SqlCompiler compiler;
    private final RpnBuilder rpnBuilder = new RpnBuilder();

    public static void main(String[] args) throws RunnerException {
        Options opt = new OptionsBuilder()
                .include(ExpressionParserBenchmark.class.getSimpleName())
                .warmupIterations(3)
                .measurementIterations(5)
                .forks(1)
                .build();

        new Runner(opt).run();

        LogFactory.haltInstance();
    }

    @Setup(Level.Trial)
    public void setup() {
        engine = new CairoEngine(configuration);
        compiler = engine.getSqlCompiler();
    }

    @TearDown(Level.Trial)
    public void tearDown() {
        compiler.close();
        engine.close();
    }

    @Benchmark
    public CharSequence testParseExpression() throws SqlException {
        rpnBuilder.reset();
        compiler.testParseExpression(expression, rpnBuilder);
        return rpnBuilder.rpn();
    }

    public static class RpnBuilder implements ExpressionParserListener {
        private final StringSink sink = new StringSink();

        @Override
        public void onNode(ExpressionNode node) {
            if (node.queryModel != null) {
                sink.put('(').put(node.queryModel).put(')').put(' ');
            } else {
                sink.put(node.token).put(' ');
            }
        }

        public void reset() {
            sink.clear();
        }

        public final CharSequence rpn() {
            if (!sink.isEmpty() && sink.charAt(sink.length() - 1) == ' ') {
                sink.clear(sink.length() - 1);
            }
            return sink;
        }
    }
}
