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
public class Decimal128MultiplyBenchmark {

    private BigDecimal bigDecimalFactor1;
    private BigDecimal bigDecimalFactor2;
    private Decimal128 decimal128Factor1;
    private Decimal128 decimal128Factor2;
    private Decimal128 decimal128Result;
    private MathContext mathContext;
    @SuppressWarnings("unused")
    @Param({"SIMPLE", "LARGE_NUMBERS", "SMALL_NUMBERS", "HIGH_PRECISION", "POWER_OF_10", "MIXED_SCALE", "MIXED_128_64", "PURE_64_BIT"})
    private String scenario;


    public static void main(String[] args) throws RunnerException {
        Options opt = new OptionsBuilder()
                .include(Decimal128MultiplyBenchmark.class.getSimpleName())
                .warmupIterations(5)
                .measurementIterations(10)
                .forks(1)
                .build();

        new Runner(opt).run();
    }

    @Benchmark
    public BigDecimal bigDecimalMultiply() {
        return bigDecimalFactor1.multiply(bigDecimalFactor2).setScale(6, RoundingMode.HALF_UP);
    }

    @Benchmark
    public BigDecimal bigDecimalMultiplyWithContext() {
        return bigDecimalFactor1.multiply(bigDecimalFactor2, mathContext);
    }

    @Benchmark
    public Decimal128 decimal128Multiply() {
        decimal128Result.copyFrom(decimal128Factor1);
        decimal128Result.multiply(decimal128Factor2);
        decimal128Result.round(6, RoundingMode.HALF_UP);
        return decimal128Result;
    }


    @Setup
    public void setup() {
        decimal128Result = new Decimal128();
        mathContext = new MathContext(16, RoundingMode.HALF_UP);


        switch (scenario) {
            case "SIMPLE":
                // Simple multiplication: 123.456 * 7.89
                decimal128Factor1 = Decimal128.fromDouble(123.456, 3);
                decimal128Factor2 = Decimal128.fromDouble(7.89, 2);
                bigDecimalFactor1 = new BigDecimal("123.456");
                bigDecimalFactor2 = new BigDecimal("7.89");
                break;

            case "LARGE_NUMBERS":
                // Large number multiplication: 987654321.123 * 123456789.456
                decimal128Factor1 = Decimal128.fromDouble(987654321.123, 3);
                decimal128Factor2 = Decimal128.fromDouble(123456789.456, 3);
                bigDecimalFactor1 = new BigDecimal("987654321.123");
                bigDecimalFactor2 = new BigDecimal("123456789.456");
                break;

            case "SMALL_NUMBERS":
                // Small number multiplication: 0.00123 * 0.00456
                decimal128Factor1 = Decimal128.fromDouble(0.00123, 5);
                decimal128Factor2 = Decimal128.fromDouble(0.00456, 5);
                bigDecimalFactor1 = new BigDecimal("0.00123");
                bigDecimalFactor2 = new BigDecimal("0.00456");
                break;

            case "HIGH_PRECISION":
                // High precision: PI * E with many decimal places
                decimal128Factor1 = Decimal128.fromDouble(3.141592653589793, 15);
                decimal128Factor2 = Decimal128.fromDouble(2.718281828459045, 15);
                bigDecimalFactor1 = new BigDecimal("3.141592653589793");
                bigDecimalFactor2 = new BigDecimal("2.718281828459045");
                break;

            case "POWER_OF_10":
                // Multiplication by power of 10: 123.456 * 100
                decimal128Factor1 = Decimal128.fromDouble(123.456, 3);
                decimal128Factor2 = Decimal128.fromDouble(100.0, 0);
                bigDecimalFactor1 = new BigDecimal("123.456");
                bigDecimalFactor2 = new BigDecimal("100");
                break;

            case "MIXED_SCALE":
                // Mixed scales: 1234567.89 * 0.001234
                decimal128Factor1 = Decimal128.fromDouble(1234567.89, 2);
                decimal128Factor2 = Decimal128.fromDouble(0.001234, 6);
                bigDecimalFactor1 = new BigDecimal("1234567.89");
                bigDecimalFactor2 = new BigDecimal("0.001234");
                break;

            case "MIXED_128_64":
                // Multiplication of 128-bit by 64-bit value: large 128-bit * normal 64-bit
                decimal128Factor1 = new Decimal128();
                decimal128Factor1.of(123456789L, 987654321098765432L, 6);
                decimal128Factor2 = Decimal128.fromDouble(7.89, 2);
                bigDecimalFactor1 = new BigDecimal("123456789987654321098.765432");
                bigDecimalFactor2 = new BigDecimal("7.89");
                break;

            case "PURE_64_BIT":
                // Multiplication of two 64-bit values within Decimal128
                decimal128Factor1 = Decimal128.fromDouble(123456.789, 3);
                decimal128Factor2 = Decimal128.fromDouble(987.654, 3);
                bigDecimalFactor1 = new BigDecimal("123456.789");
                bigDecimalFactor2 = new BigDecimal("987.654");
                break;
        }
    }
}