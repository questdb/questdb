/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2025 QuestDB
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

import io.questdb.std.Decimal128;
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
@Warmup(iterations = 5, time = 1)
@Measurement(iterations = 5, time = 1)
@Fork(1)
@State(Scope.Benchmark)
public class Decimal128DivideBenchmark {

    // BigDecimal instances
    private BigDecimal bigDecimalDividend;
    private BigDecimal bigDecimalDivisor;
    // Decimal128 instances
    private Decimal128 decimal128Dividend;
    private Decimal128 decimal128Divisor;
    private Decimal128 decimal128Result;
    // Add at the class level:
    private Decimal128 largeDividend128;
    private MathContext mathContext;
    // Test data for different scenarios
    @SuppressWarnings("unused")
    @Param({"SIMPLE", "LARGE_DIVIDEND", "LARGE_DIVISOR", "HIGH_PRECISION", "POWER_OF_10"})
    private String scenario;

    public static void main(String[] args) throws RunnerException {
        Options opt = new OptionsBuilder()
                .include(Decimal128DivideBenchmark.class.getSimpleName())
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
    public Decimal128 decimal128Divide() {
        decimal128Result.copyFrom(decimal128Dividend);
        decimal128Result.divide(decimal128Divisor, 6, RoundingMode.HALF_UP);
        return decimal128Result;
    }

    @Benchmark
    public void decimal128Divide128By64() {
        // This scenario uses a 128-bit dividend divided by a 64-bit divisor
        decimal128Result.copyFrom(largeDividend128);
        decimal128Result.divide(decimal128Divisor, 6, RoundingMode.HALF_UP);
    }

    @Setup
    public void setup() {
        decimal128Result = new Decimal128();
        largeDividend128 = new Decimal128();
        largeDividend128.of(123456789L, 987654321098765432L, 6);
        mathContext = new MathContext(16, RoundingMode.HALF_UP);

        // Setup test data based on scenario
        switch (scenario) {
            case "SIMPLE":
                // Simple division: 123.456 / 7.89
                decimal128Dividend = Decimal128.fromDouble(123.456, 3);
                decimal128Divisor = Decimal128.fromDouble(7.89, 2);
                bigDecimalDividend = new BigDecimal("123.456");
                bigDecimalDivisor = new BigDecimal("7.89");
                break;

            case "LARGE_DIVIDEND":
                // Large dividend: 987654321.123456789 / 123.456
                decimal128Dividend = Decimal128.fromDouble(987654321.123456789, 9);
                decimal128Divisor = Decimal128.fromDouble(123.456, 3);
                bigDecimalDividend = new BigDecimal("987654321.123456789");
                bigDecimalDivisor = new BigDecimal("123.456");
                break;

            case "LARGE_DIVISOR":
                // Large divisor: 123.456 / 987654321.123456789
                decimal128Dividend = Decimal128.fromDouble(123.456, 3);
                decimal128Divisor = Decimal128.fromDouble(987654321.123456789, 9);
                bigDecimalDividend = new BigDecimal("123.456");
                bigDecimalDivisor = new BigDecimal("987654321.123456789");
                break;

            case "HIGH_PRECISION":
                // High precision: PI / E with many decimal places
                decimal128Dividend = Decimal128.fromDouble(3.141592653589793, 15);
                decimal128Divisor = Decimal128.fromDouble(2.718281828459045, 15);
                bigDecimalDividend = new BigDecimal("3.141592653589793");
                bigDecimalDivisor = new BigDecimal("2.718281828459045");
                break;

            case "POWER_OF_10":
                // Division by power of 10: 123456.789 / 100
                decimal128Dividend = Decimal128.fromDouble(123456.789, 3);
                decimal128Divisor = Decimal128.fromDouble(100.0, 0);
                bigDecimalDividend = new BigDecimal("123456.789");
                bigDecimalDivisor = new BigDecimal("100");
                break;
        }
    }
}