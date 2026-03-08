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
public class Decimal64ModuloBenchmark {

    private BigDecimal bigDecimalDividend;
    private BigDecimal bigDecimalDivisor;
    private Decimal64 decimal64Dividend;
    private Decimal64 decimal64Divisor;
    private Decimal64 decimal64Result;
    private MathContext mathContext;
    @SuppressWarnings("unused")
    @Param({"SIMPLE", "LARGE_NUMBERS", "SMALL_NUMBERS", "DIFFERENT_SCALES", "NEAR_DIVISOR", "FRACTIONAL"})
    private String scenario;


    public static void main(String[] args) throws RunnerException {
        Options opt = new OptionsBuilder()
                .include(Decimal64ModuloBenchmark.class.getSimpleName())
                .warmupIterations(5)
                .measurementIterations(10)
                .forks(1)
                .build();

        new Runner(opt).run();
    }

    @Benchmark
    public BigDecimal bigDecimalModulo() {
        return bigDecimalDividend.remainder(bigDecimalDivisor);
    }

    @Benchmark
    public BigDecimal bigDecimalModuloWithContext() {
        return bigDecimalDividend.remainder(bigDecimalDivisor, mathContext);
    }

    @Benchmark
    public Decimal64 decimal64Modulo() {
        decimal64Result.copyFrom(decimal64Dividend);
        decimal64Result.modulo(decimal64Divisor);
        return decimal64Result;
    }

    @Setup
    public void setup() {
        decimal64Result = new Decimal64();
        mathContext = new MathContext(18, RoundingMode.HALF_UP);


        switch (scenario) {
            case "SIMPLE":
                // Simple modulo: 789.123 % 12.34
                decimal64Dividend = Decimal64.fromLong(789123L, 3);
                decimal64Divisor = Decimal64.fromLong(1234L, 2);
                bigDecimalDividend = new BigDecimal("789.123");
                bigDecimalDivisor = new BigDecimal("12.34");
                break;

            case "LARGE_NUMBERS":
                // Large number modulo: 12345678.123 % 987.654
                decimal64Dividend = Decimal64.fromLong(12345678123L, 3);
                decimal64Divisor = Decimal64.fromLong(987654L, 3);
                bigDecimalDividend = new BigDecimal("12345678.123");
                bigDecimalDivisor = new BigDecimal("987.654");
                break;

            case "SMALL_NUMBERS":
                // Small number modulo: 0.00789 % 0.00123
                decimal64Dividend = Decimal64.fromLong(789L, 5);
                decimal64Divisor = Decimal64.fromLong(123L, 5);
                bigDecimalDividend = new BigDecimal("0.00789");
                bigDecimalDivisor = new BigDecimal("0.00123");
                break;

            case "DIFFERENT_SCALES":
                // Different scales: 123.456 % 7.8901234
                decimal64Dividend = Decimal64.fromLong(123456L, 3);
                decimal64Divisor = Decimal64.fromLong(78901234L, 7);
                bigDecimalDividend = new BigDecimal("123.456");
                bigDecimalDivisor = new BigDecimal("7.8901234");
                break;

            case "NEAR_DIVISOR":
                // Dividend near divisor: 123.456 % 123.123
                decimal64Dividend = Decimal64.fromLong(123456L, 3);
                decimal64Divisor = Decimal64.fromLong(123123L, 3);
                bigDecimalDividend = new BigDecimal("123.456");
                bigDecimalDivisor = new BigDecimal("123.123");
                break;

            case "FRACTIONAL":
                // Fractional modulo: 3.14159 % 2.71828
                decimal64Dividend = Decimal64.fromLong(314159L, 5);
                decimal64Divisor = Decimal64.fromLong(271828L, 5);
                bigDecimalDividend = new BigDecimal("3.14159");
                bigDecimalDivisor = new BigDecimal("2.71828");
                break;
        }
    }
}