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
public class Decimal256SubtractBenchmark {

    private BigDecimal bigDecimalMinuend;
    private BigDecimal bigDecimalSubtrahend;
    private Decimal256 decimal256Minuend;
    private Decimal256 decimal256Subtrahend;
    private Decimal256 decimal256Result;
    private MathContext mathContext;
    @Param({"SIMPLE", "LARGE_NUMBERS", "SMALL_NUMBERS", "DIFFERENT_SCALES", "NEAR_ZERO", "NEGATIVE_SUBTRACTION"})
    private String scenario;

    public static void main(String[] args) throws RunnerException {
        Options opt = new OptionsBuilder()
                .include(Decimal256SubtractBenchmark.class.getSimpleName())
                .warmupIterations(5)
                .measurementIterations(10)
                .forks(1)
                .build();

        new Runner(opt).run();
    }

    @Benchmark
    public BigDecimal bigDecimalSubtract() {
        return bigDecimalMinuend.subtract(bigDecimalSubtrahend);
    }

    @Benchmark
    public BigDecimal bigDecimalSubtractWithContext() {
        return bigDecimalMinuend.subtract(bigDecimalSubtrahend, mathContext);
    }

    @Benchmark
    public Decimal256 decimal256Subtract() {
        decimal256Result.copyFrom(decimal256Minuend);
        decimal256Result.subtract(decimal256Subtrahend);
        return decimal256Result;
    }

    @Benchmark
    public void decimal256Subtract256By128() {
        // Test subtraction of 256-bit and 128-bit values
        Decimal256 largeMinuend = new Decimal256();
        largeMinuend.setFromLong(123456789098765432L, 6);

        decimal256Result.copyFrom(largeMinuend);
        decimal256Result.subtract(decimal256Subtrahend);
    }

    @Benchmark
    public void decimal256Subtract128Bit() {
        // Test subtraction of two 128-bit values
        Decimal256 val1 = Decimal256.fromDouble(987654.321, 3);
        Decimal256 val2 = Decimal256.fromDouble(123456.789, 3);

        decimal256Result.copyFrom(val1);
        decimal256Result.subtract(val2);
    }

    @Benchmark
    public void decimal256SubtractNearZero() {
        // Test subtraction that results in near-zero: 123.456 - 123.455
        Decimal256 val1 = Decimal256.fromDouble(123.456, 3);
        Decimal256 val2 = Decimal256.fromDouble(123.455, 3);

        decimal256Result.copyFrom(val1);
        decimal256Result.subtract(val2);
    }

    @Benchmark
    public void decimal256SubtractNegative() {
        // Test subtraction with negative result: 123.456 - 789.123
        Decimal256 val1 = Decimal256.fromDouble(123.456, 3);
        Decimal256 val2 = Decimal256.fromDouble(789.123, 3);

        decimal256Result.copyFrom(val1);
        decimal256Result.subtract(val2);
    }

    @Setup
    public void setup() {
        decimal256Result = new Decimal256();
        mathContext = new MathContext(32, RoundingMode.HALF_UP);

        switch (scenario) {
            case "SIMPLE":
                // Simple subtraction: 789.123 - 123.456
                decimal256Minuend = Decimal256.fromDouble(789.123, 3);
                decimal256Subtrahend = Decimal256.fromDouble(123.456, 3);
                bigDecimalMinuend = new BigDecimal("789.123");
                bigDecimalSubtrahend = new BigDecimal("123.456");
                break;

            case "LARGE_NUMBERS":
                // Large number subtraction: 987654321.123 - 123456789.456
                decimal256Minuend = Decimal256.fromDouble(987654321.123, 3);
                decimal256Subtrahend = Decimal256.fromDouble(123456789.456, 3);
                bigDecimalMinuend = new BigDecimal("987654321.123");
                bigDecimalSubtrahend = new BigDecimal("123456789.456");
                break;

            case "SMALL_NUMBERS":
                // Small number subtraction: 0.00456 - 0.00123
                decimal256Minuend = Decimal256.fromDouble(0.00456, 5);
                decimal256Subtrahend = Decimal256.fromDouble(0.00123, 5);
                bigDecimalMinuend = new BigDecimal("0.00456");
                bigDecimalSubtrahend = new BigDecimal("0.00123");
                break;

            case "DIFFERENT_SCALES":
                // Different scales: 123.456 - 7.8901234
                decimal256Minuend = Decimal256.fromDouble(123.456, 3);
                decimal256Subtrahend = Decimal256.fromDouble(7.8901234, 7);
                bigDecimalMinuend = new BigDecimal("123.456");
                bigDecimalSubtrahend = new BigDecimal("7.8901234");
                break;

            case "NEAR_ZERO":
                // Near zero result: 123.456 - 123.455
                decimal256Minuend = Decimal256.fromDouble(123.456, 3);
                decimal256Subtrahend = Decimal256.fromDouble(123.455, 3);
                bigDecimalMinuend = new BigDecimal("123.456");
                bigDecimalSubtrahend = new BigDecimal("123.455");
                break;

            case "NEGATIVE_SUBTRACTION":
                // Negative subtraction: -123.456 - 789.123
                decimal256Minuend = Decimal256.fromDouble(-123.456, 3);
                decimal256Subtrahend = Decimal256.fromDouble(789.123, 3);
                bigDecimalMinuend = new BigDecimal("-123.456");
                bigDecimalSubtrahend = new BigDecimal("789.123");
                break;
        }
    }
}