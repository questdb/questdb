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
    @Param({"SIMPLE", "LARGE_NUMBERS", "SMALL_NUMBERS", "DIFFERENT_SCALES", "PRECISION_INTENSIVE", "INTEGER_MULTIPLICATION"})
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

    @Benchmark
    public void decimal256Multiply256By128() {
        // Test multiplication of 256-bit and 128-bit values
        Decimal256 largeMultiplicand = new Decimal256();
        largeMultiplicand.setFromLong(123456789098765432L, 6);

        decimal256Result.copyFrom(largeMultiplicand);
        decimal256Result.multiply(decimal256Multiplier);
    }

    @Benchmark
    public void decimal256Multiply128Bit() {
        // Test multiplication of two 128-bit values
        Decimal256 val1 = Decimal256.fromDouble(12345.678, 3);
        Decimal256 val2 = Decimal256.fromDouble(9876.543, 3);

        decimal256Result.copyFrom(val1);
        decimal256Result.multiply(val2);
    }

    @Benchmark
    public void decimal256MultiplyLargePrecision() {
        // Test multiplication requiring high precision
        Decimal256 val1 = Decimal256.fromBigDecimal(new BigDecimal("123456789012345678901234.123456789"));
        Decimal256 val2 = Decimal256.fromBigDecimal(new BigDecimal("987654321098765432.987654321"));

        decimal256Result.copyFrom(val1);
        decimal256Result.multiply(val2);
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
        }
    }
}