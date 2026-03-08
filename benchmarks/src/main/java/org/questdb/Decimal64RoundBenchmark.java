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

import io.questdb.std.Decimal64;
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
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;

import java.math.BigDecimal;
import java.math.MathContext;
import java.math.RoundingMode;
import java.util.concurrent.TimeUnit;

@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@Warmup(iterations = 5, time = 100, timeUnit = TimeUnit.MILLISECONDS)
@Measurement(iterations = 5, time = 100, timeUnit = TimeUnit.MILLISECONDS)
@Fork(1)
@State(Scope.Benchmark)
public class Decimal64RoundBenchmark {

    private BigDecimal bigDecimalValue;
    private Decimal64 decimal64Result;
    private Decimal64 decimal64Value;
    private RoundingMode javaRoundingMode;
    @SuppressWarnings("unused")
    @Param({"HALF_UP", "HALF_DOWN", "HALF_EVEN", "UP", "DOWN", "CEILING", "FLOOR"})
    private String roundingMode;
    @SuppressWarnings("unused")
    @Param({"ROUND_DOWN", "ROUND_UP", "ROUND_HALFWAY", "ROUND_PRECISION", "ROUND_TRAILING_ZEROS", "ROUND_NEGATIVE"})
    private String scenario;
    private int targetScale;

    public static void main(String[] args) throws RunnerException {
        Options opt = new OptionsBuilder()
                .include(Decimal64RoundBenchmark.class.getSimpleName())
                .warmupIterations(5)
                .measurementIterations(10)
                .forks(1)
                .build();

        new Runner(opt).run();
    }

    @Benchmark
    public BigDecimal bigDecimalRound() {
        return bigDecimalValue.setScale(targetScale, javaRoundingMode);
    }

    @Benchmark
    public BigDecimal bigDecimalRoundWithContext() {
        MathContext mc = new MathContext(targetScale + 3, javaRoundingMode);
        return bigDecimalValue.round(mc);
    }

    @Benchmark
    public Decimal64 decimal64Round() {
        decimal64Result.copyFrom(decimal64Value);
        decimal64Result.round(targetScale, javaRoundingMode);
        return decimal64Result;
    }

    @Setup
    public void setup() {
        decimal64Result = new Decimal64();
        javaRoundingMode = RoundingMode.valueOf(roundingMode);

        switch (scenario) {
            case "ROUND_DOWN":
                // Value that needs rounding down: 123.456789 -> 123.457 (scale 3)
                decimal64Value = Decimal64.fromLong(123456789L, 6);
                bigDecimalValue = new BigDecimal("123.456789");
                targetScale = 3;
                break;

            case "ROUND_UP":
                // Value that needs rounding up: 123.456789 -> 123.46 (scale 2)
                decimal64Value = Decimal64.fromLong(123456789L, 6);
                bigDecimalValue = new BigDecimal("123.456789");
                targetScale = 2;
                break;

            case "ROUND_HALFWAY":
                // Halfway case: 123.4565 -> depends on rounding mode
                decimal64Value = Decimal64.fromLong(1234565L, 4);
                bigDecimalValue = new BigDecimal("123.4565");
                targetScale = 3;
                break;

            case "ROUND_PRECISION":
                // High precision rounding: PI to 10 decimal places
                decimal64Value = Decimal64.fromLong(31415926535898L, 13);
                bigDecimalValue = new BigDecimal("3.1415926535898");
                targetScale = 10;
                break;

            case "ROUND_TRAILING_ZEROS":
                // Value with trailing zeros: 123.450000 -> 123.45
                decimal64Value = Decimal64.fromLong(123450000L, 6);
                bigDecimalValue = new BigDecimal("123.450000");
                targetScale = 2;
                break;

            case "ROUND_NEGATIVE":
                // Negative value rounding: -123.456789 -> -123.46
                decimal64Value = Decimal64.fromLong(-123456789L, 6);
                bigDecimalValue = new BigDecimal("-123.456789");
                targetScale = 2;
                break;
        }
    }
}