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
public class Decimal64MultiplyBenchmark {

    private BigDecimal bigDecimalFactor1;
    private BigDecimal bigDecimalFactor2;
    private Decimal64 decimal64Factor1;
    private Decimal64 decimal64Factor2;
    private Decimal64 decimal64Result;
    private MathContext mathContext;
    @SuppressWarnings("unused")
    @Param({"SIMPLE", "LARGE_NUMBERS", "SMALL_NUMBERS", "HIGH_PRECISION", "POWER_OF_10", "MIXED_SCALE"})
    private String scenario;


    public static void main(String[] args) throws RunnerException {
        Options opt = new OptionsBuilder()
                .include(Decimal64MultiplyBenchmark.class.getSimpleName())
                .warmupIterations(5)
                .measurementIterations(10)
                .forks(1)
                .build();

        new Runner(opt).run();
    }

    @Benchmark
    public BigDecimal bigDecimalMultiply() {
        return bigDecimalFactor1.multiply(bigDecimalFactor2);
    }

    @Benchmark
    public BigDecimal bigDecimalMultiplyWithContext() {
        return bigDecimalFactor1.multiply(bigDecimalFactor2, mathContext);
    }

    @Benchmark
    public Decimal64 decimal64Multiply() {
        decimal64Result.copyFrom(decimal64Factor1);
        decimal64Result.multiply(decimal64Factor2);
        return decimal64Result;
    }

    @Setup
    public void setup() {
        decimal64Result = new Decimal64();
        mathContext = new MathContext(18, RoundingMode.HALF_UP);


        switch (scenario) {
            case "SIMPLE":
                // Simple multiplication: 123.456 * 7.89
                decimal64Factor1 = Decimal64.fromLong(123456L, 3);
                decimal64Factor2 = Decimal64.fromLong(789L, 2);
                bigDecimalFactor1 = new BigDecimal("123.456");
                bigDecimalFactor2 = new BigDecimal("7.89");
                break;

            case "LARGE_NUMBERS":
                // Large number multiplication: 123456.789 * 9876.543
                decimal64Factor1 = Decimal64.fromLong(123456789L, 3);
                decimal64Factor2 = Decimal64.fromLong(9876543L, 3);
                bigDecimalFactor1 = new BigDecimal("123456.789");
                bigDecimalFactor2 = new BigDecimal("9876.543");
                break;

            case "SMALL_NUMBERS":
                // Small number multiplication: 0.00123 * 0.00456
                decimal64Factor1 = Decimal64.fromLong(123L, 5);
                decimal64Factor2 = Decimal64.fromLong(456L, 5);
                bigDecimalFactor1 = new BigDecimal("0.00123");
                bigDecimalFactor2 = new BigDecimal("0.00456");
                break;

            case "HIGH_PRECISION":
                // High precision: PI * E with many decimal places
                decimal64Factor1 = Decimal64.fromLong(31415926536L, 10);
                decimal64Factor2 = Decimal64.fromLong(27182818285L, 10);
                bigDecimalFactor1 = new BigDecimal("3.1415926536");
                bigDecimalFactor2 = new BigDecimal("2.7182818285");
                break;

            case "POWER_OF_10":
                // Multiplication by power of 10: 123.456 * 100
                decimal64Factor1 = Decimal64.fromLong(123456L, 3);
                decimal64Factor2 = Decimal64.fromLong(100L, 0);
                bigDecimalFactor1 = new BigDecimal("123.456");
                bigDecimalFactor2 = new BigDecimal("100");
                break;

            case "MIXED_SCALE":
                // Mixed scales: 1234567.89 * 0.001234
                decimal64Factor1 = Decimal64.fromLong(123456789L, 2);
                decimal64Factor2 = Decimal64.fromLong(1234L, 6);
                bigDecimalFactor1 = new BigDecimal("1234567.89");
                bigDecimalFactor2 = new BigDecimal("0.001234");
                break;
        }
    }
}