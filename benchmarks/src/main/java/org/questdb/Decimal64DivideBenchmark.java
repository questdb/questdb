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
public class Decimal64DivideBenchmark {

    private BigDecimal bigDecimalDividend;
    private BigDecimal bigDecimalDivisor;
    private Decimal64 decimal64Dividend;
    private Decimal64 decimal64Divisor;
    private Decimal64 decimal64Result;
    private MathContext mathContext;
    @SuppressWarnings("unused")
    @Param({"SIMPLE", "LARGE_NUMBERS", "SMALL_NUMBERS", "HIGH_PRECISION", "POWER_OF_10", "MIXED_SCALE"})
    private String scenario;


    public static void main(String[] args) throws RunnerException {
        Options opt = new OptionsBuilder()
                .include(Decimal64DivideBenchmark.class.getSimpleName())
                .warmupIterations(5)
                .measurementIterations(10)
                .forks(1)
                .build();

        new Runner(opt).run();
    }

    @Benchmark
    public BigDecimal bigDecimalDivide() {
        return bigDecimalDividend.divide(bigDecimalDivisor, 6, RoundingMode.HALF_UP);
    }

    @Benchmark
    public BigDecimal bigDecimalDivideWithContext() {
        return bigDecimalDividend.divide(bigDecimalDivisor, mathContext);
    }

    @Benchmark
    public Decimal64 decimal64Divide() {
        decimal64Result.copyFrom(decimal64Dividend);
        decimal64Result.divide(decimal64Divisor, 6, RoundingMode.HALF_UP);
        return decimal64Result;
    }

    @Setup
    public void setup() {
        decimal64Result = new Decimal64();
        mathContext = new MathContext(18, RoundingMode.HALF_UP);


        switch (scenario) {
            case "SIMPLE":
                // Simple division: 789.123 / 12.34
                decimal64Dividend = Decimal64.fromLong(789123L, 3);
                decimal64Divisor = Decimal64.fromLong(1234L, 2);
                bigDecimalDividend = new BigDecimal("789.123");
                bigDecimalDivisor = new BigDecimal("12.34");
                break;

            case "LARGE_NUMBERS":
                // Large number division: 12345678.123 / 987.654
                decimal64Dividend = Decimal64.fromLong(12345678123L, 3);
                decimal64Divisor = Decimal64.fromLong(987654L, 3);
                bigDecimalDividend = new BigDecimal("12345678.123");
                bigDecimalDivisor = new BigDecimal("987.654");
                break;

            case "SMALL_NUMBERS":
                // Small number division: 0.00789 / 0.00123
                decimal64Dividend = Decimal64.fromLong(789L, 5);
                decimal64Divisor = Decimal64.fromLong(123L, 5);
                bigDecimalDividend = new BigDecimal("0.00789");
                bigDecimalDivisor = new BigDecimal("0.00123");
                break;

            case "HIGH_PRECISION":
                // High precision: PI / E with many decimal places
                decimal64Dividend = Decimal64.fromLong(31415926536L, 10);
                decimal64Divisor = Decimal64.fromLong(27182818285L, 10);
                bigDecimalDividend = new BigDecimal("3.1415926536");
                bigDecimalDivisor = new BigDecimal("2.7182818285");
                break;

            case "POWER_OF_10":
                // Division by power of 10: 123.456 / 100
                decimal64Dividend = Decimal64.fromLong(123456L, 3);
                decimal64Divisor = Decimal64.fromLong(100L, 0);
                bigDecimalDividend = new BigDecimal("123.456");
                bigDecimalDivisor = new BigDecimal("100");
                break;

            case "MIXED_SCALE":
                // Mixed scales: 1234567.89 / 0.001234
                decimal64Dividend = Decimal64.fromLong(123456789L, 2);
                decimal64Divisor = Decimal64.fromLong(1234L, 6);
                bigDecimalDividend = new BigDecimal("1234567.89");
                bigDecimalDivisor = new BigDecimal("0.001234");
                break;
        }
    }
}