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

import io.questdb.std.Decimal160;
import org.openjdk.jmh.annotations.*;
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
public class Decimal160DivideBenchmark {

    // Test data for different scenarios
    @Param({"SIMPLE", "LARGE_DIVIDEND", "LARGE_DIVISOR", "HIGH_PRECISION", "POWER_OF_10"})
    private String scenario;

    // Decimal160 instances
    private Decimal160 decimal160Dividend;
    private Decimal160 decimal160Divisor;
    private Decimal160 decimal160Result;

    // BigDecimal instances
    private BigDecimal bigDecimalDividend;
    private BigDecimal bigDecimalDivisor;
    private MathContext mathContext;

    @Setup
    public void setup() {
        // Initialize result containers
        decimal160Result = new Decimal160();
        mathContext = new MathContext(16, RoundingMode.HALF_UP);

        // Setup test data based on scenario
        switch (scenario) {
            case "SIMPLE":
                // Simple division: 123.456 / 7.89
                decimal160Dividend = Decimal160.fromDouble(123.456, 3);
                decimal160Divisor = Decimal160.fromDouble(7.89, 2);
                bigDecimalDividend = new BigDecimal("123.456");
                bigDecimalDivisor = new BigDecimal("7.89");
                break;

            case "LARGE_DIVIDEND":
                // Large dividend: 987654321.123456789 / 123.456
                decimal160Dividend = Decimal160.fromDouble(987654321.123456789, 9);
                decimal160Divisor = Decimal160.fromDouble(123.456, 3);
                bigDecimalDividend = new BigDecimal("987654321.123456789");
                bigDecimalDivisor = new BigDecimal("123.456");
                break;

            case "LARGE_DIVISOR":
                // Large divisor: 123.456 / 987654321.123456789
                decimal160Dividend = Decimal160.fromDouble(123.456, 3);
                decimal160Divisor = Decimal160.fromDouble(987654321.123456789, 9);
                bigDecimalDividend = new BigDecimal("123.456");
                bigDecimalDivisor = new BigDecimal("987654321.123456789");
                break;

            case "HIGH_PRECISION":
                // High precision: PI / E with many decimal places
                decimal160Dividend = Decimal160.fromDouble(3.141592653589793, 15);
                decimal160Divisor = Decimal160.fromDouble(2.718281828459045, 15);
                bigDecimalDividend = new BigDecimal("3.141592653589793");
                bigDecimalDivisor = new BigDecimal("2.718281828459045");
                break;

            case "POWER_OF_10":
                // Division by power of 10: 123456.789 / 100
                decimal160Dividend = Decimal160.fromDouble(123456.789, 3);
                decimal160Divisor = Decimal160.fromDouble(100.0, 0);
                bigDecimalDividend = new BigDecimal("123456.789");
                bigDecimalDivisor = new BigDecimal("100");
                break;
        }
    }

    @Benchmark
    public Decimal160 decimal160Divide() {
        decimal160Result.copyFrom(decimal160Dividend);
        decimal160Result.divide(decimal160Divisor, 6, RoundingMode.HALF_UP);
        return decimal160Result;
    }

    @Benchmark
    public BigDecimal bigDecimalDivide() {
        return bigDecimalDividend.divide(bigDecimalDivisor, 6, RoundingMode.HALF_UP);
    }

    @Benchmark
    public BigDecimal bigDecimalDivideWithContext() {
        return bigDecimalDividend.divide(bigDecimalDivisor, mathContext);
    }

    // Benchmark that tests 128-bit by 64-bit division specifically
    @Benchmark
    public void decimal160Divide128By64() {
        // This scenario uses a 128-bit dividend divided by a 64-bit divisor
        Decimal160 largeDividend = new Decimal160();
        largeDividend.set(123456789L, 987654321098765432L, 6);
        
        decimal160Result.copyFrom(largeDividend);
        decimal160Result.divide(decimal160Divisor, 6, RoundingMode.HALF_UP);
    }

    public static void main(String[] args) throws RunnerException {
        Options opt = new OptionsBuilder()
                .include(Decimal160DivideBenchmark.class.getSimpleName())
                .warmupIterations(5)
                .measurementIterations(10)
                .forks(1)
                .build();

        new Runner(opt).run();
    }
}