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

import io.questdb.std.Decimal256;
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
public class Decimal256MultiplyBenchmark {

    private BigDecimal bigDecimalMultiplicand;
    private BigDecimal bigDecimalMultiplier;
    private Decimal256 decimal256Multiplicand;
    private Decimal256 decimal256Multiplier;
    private Decimal256 decimal256Result;
    private MathContext mathContext;
    @SuppressWarnings("unused")
    @Param({"SIMPLE", "LARGE_NUMBERS", "SMALL_NUMBERS", "DIFFERENT_SCALES", "PRECISION_INTENSIVE", "INTEGER_MULTIPLICATION", "MIXED_256_128", "PURE_128_BIT", "VERY_LARGE"})
    private String scenario;

    public static void main(String[] args) throws RunnerException {
        Options opt = new OptionsBuilder()
                .include(Decimal256MultiplyBenchmark.class.getSimpleName())
                .warmupIterations(5)
                .measurementIterations(10)
                .forks(1)
                .build();

        new Runner(opt).run();
    }

    @Benchmark
    public BigDecimal bigDecimalMultiply() {
        return bigDecimalMultiplicand.multiply(bigDecimalMultiplier);
    }

    @Benchmark
    public BigDecimal bigDecimalMultiplyWithContext() {
        return bigDecimalMultiplicand.multiply(bigDecimalMultiplier, mathContext);
    }

    @Benchmark
    public Decimal256 decimal256Multiply() {
        decimal256Result.copyFrom(decimal256Multiplicand);
        decimal256Result.multiply(decimal256Multiplier);
        return decimal256Result;
    }


    @Setup
    public void setup() {
        decimal256Result = new Decimal256();
        mathContext = new MathContext(32, RoundingMode.HALF_UP);


        switch (scenario) {
            case "SIMPLE":
                // Simple multiplication: 12.34 * 5.6
                decimal256Multiplicand = Decimal256.fromDouble(12.34, 2);
                decimal256Multiplier = Decimal256.fromDouble(5.6, 1);
                bigDecimalMultiplicand = new BigDecimal("12.34");
                bigDecimalMultiplier = new BigDecimal("5.6");
                break;

            case "LARGE_NUMBERS":
                // Large number multiplication: 123456.789 * 987654.321
                decimal256Multiplicand = Decimal256.fromDouble(123456.789, 3);
                decimal256Multiplier = Decimal256.fromDouble(987654.321, 3);
                bigDecimalMultiplicand = new BigDecimal("123456.789");
                bigDecimalMultiplier = new BigDecimal("987654.321");
                break;

            case "SMALL_NUMBERS":
                // Small number multiplication: 0.00123 * 0.00456
                decimal256Multiplicand = Decimal256.fromDouble(0.00123, 5);
                decimal256Multiplier = Decimal256.fromDouble(0.00456, 5);
                bigDecimalMultiplicand = new BigDecimal("0.00123");
                bigDecimalMultiplier = new BigDecimal("0.00456");
                break;

            case "DIFFERENT_SCALES":
                // Different scales: 123.456 * 7.8901234
                decimal256Multiplicand = Decimal256.fromDouble(123.456, 3);
                decimal256Multiplier = Decimal256.fromDouble(7.8901234, 7);
                bigDecimalMultiplicand = new BigDecimal("123.456");
                bigDecimalMultiplier = new BigDecimal("7.8901234");
                break;

            case "PRECISION_INTENSIVE":
                // High precision multiplication
                decimal256Multiplicand = Decimal256.fromBigDecimal(new BigDecimal("123456789012345.123456789"));
                decimal256Multiplier = Decimal256.fromBigDecimal(new BigDecimal("987654321098.987654321"));
                bigDecimalMultiplicand = new BigDecimal("123456789012345.123456789");
                bigDecimalMultiplier = new BigDecimal("987654321098.987654321");
                break;

            case "INTEGER_MULTIPLICATION":
                // Integer multiplication: 123456 * 789012
                decimal256Multiplicand = Decimal256.fromLong(123456, 0);
                decimal256Multiplier = Decimal256.fromLong(789012, 0);
                bigDecimalMultiplicand = new BigDecimal("123456");
                bigDecimalMultiplier = new BigDecimal("789012");
                break;

            case "MIXED_256_128":
                // Multiplication of 256-bit and 128-bit values: large 256-bit * normal 128-bit
                decimal256Multiplicand = new Decimal256();
                decimal256Multiplicand.ofLong(123456789098765432L, 6);
                decimal256Multiplier = Decimal256.fromDouble(7.89, 2);
                bigDecimalMultiplicand = new BigDecimal("123456789098.765432");
                bigDecimalMultiplier = new BigDecimal("7.89");
                break;

            case "PURE_128_BIT":
                // Multiplication of two 128-bit values within Decimal256
                decimal256Multiplicand = Decimal256.fromDouble(12345.678, 3);
                decimal256Multiplier = Decimal256.fromDouble(9876.543, 3);
                bigDecimalMultiplicand = new BigDecimal("12345.678");
                bigDecimalMultiplier = new BigDecimal("9876.543");
                break;

            case "VERY_LARGE":
                // Multiplication requiring very high precision
                decimal256Multiplicand = Decimal256.fromBigDecimal(new BigDecimal("123456789012345678901234.123456789"));
                decimal256Multiplier = Decimal256.fromBigDecimal(new BigDecimal("987654321098765432.987654321"));
                bigDecimalMultiplicand = new BigDecimal("123456789012345678901234.123456789");
                bigDecimalMultiplier = new BigDecimal("987654321098765432.987654321");
                break;
        }
    }
}