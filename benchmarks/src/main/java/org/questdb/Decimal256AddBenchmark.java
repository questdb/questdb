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
public class Decimal256AddBenchmark {

    private BigDecimal bigDecimalAddend1;
    private BigDecimal bigDecimalAddend2;
    private Decimal256 decimal256Addend1;
    private Decimal256 decimal256Addend2;
    private Decimal256 decimal256Result;
    private MathContext mathContext;
    @SuppressWarnings("unused")
    @Param({"SIMPLE", "LARGE_NUMBERS", "SMALL_NUMBERS", "DIFFERENT_SCALES", "SAME_MAGNITUDE", "NEGATIVE_POSITIVE", "NEAR_CANCEL", "MIXED_256_128", "PURE_128_BIT", "VERY_LARGE"})
    private String scenario;

    public static void main(String[] args) throws RunnerException {
        Options opt = new OptionsBuilder()
                .include(Decimal256AddBenchmark.class.getSimpleName())
                .warmupIterations(5)
                .measurementIterations(10)
                .forks(1)
                .build();

        new Runner(opt).run();
    }

    @Benchmark
    public BigDecimal bigDecimalAdd() {
        return bigDecimalAddend1.add(bigDecimalAddend2);
    }

    @Benchmark
    public BigDecimal bigDecimalAddWithContext() {
        return bigDecimalAddend1.add(bigDecimalAddend2, mathContext);
    }

    @Benchmark
    public Decimal256 decimal256Add() {
        decimal256Result.copyFrom(decimal256Addend1);
        decimal256Result.add(decimal256Addend2);
        return decimal256Result;
    }


    @Setup
    public void setup() {
        decimal256Result = new Decimal256();
        mathContext = new MathContext(32, RoundingMode.HALF_UP);


        switch (scenario) {
            case "SIMPLE":
                // Simple addition: 123.456 + 789.123
                decimal256Addend1 = Decimal256.fromDouble(123.456, 3);
                decimal256Addend2 = Decimal256.fromDouble(789.123, 3);
                bigDecimalAddend1 = new BigDecimal("123.456");
                bigDecimalAddend2 = new BigDecimal("789.123");
                break;

            case "LARGE_NUMBERS":
                // Large number addition: 987654321.123 + 123456789.456
                decimal256Addend1 = Decimal256.fromDouble(987654321.123, 3);
                decimal256Addend2 = Decimal256.fromDouble(123456789.456, 3);
                bigDecimalAddend1 = new BigDecimal("987654321.123");
                bigDecimalAddend2 = new BigDecimal("123456789.456");
                break;

            case "SMALL_NUMBERS":
                // Small number addition: 0.00123 + 0.00456
                decimal256Addend1 = Decimal256.fromDouble(0.00123, 5);
                decimal256Addend2 = Decimal256.fromDouble(0.00456, 5);
                bigDecimalAddend1 = new BigDecimal("0.00123");
                bigDecimalAddend2 = new BigDecimal("0.00456");
                break;

            case "DIFFERENT_SCALES":
                // Different scales: 123.456 + 7.8901234
                decimal256Addend1 = Decimal256.fromDouble(123.456, 3);
                decimal256Addend2 = Decimal256.fromDouble(7.8901234, 7);
                bigDecimalAddend1 = new BigDecimal("123.456");
                bigDecimalAddend2 = new BigDecimal("7.8901234");
                break;

            case "SAME_MAGNITUDE":
                // Same magnitude: 123.456 + 123.789
                decimal256Addend1 = Decimal256.fromDouble(123.456, 3);
                decimal256Addend2 = Decimal256.fromDouble(123.789, 3);
                bigDecimalAddend1 = new BigDecimal("123.456");
                bigDecimalAddend2 = new BigDecimal("123.789");
                break;

            case "NEGATIVE_POSITIVE":
                // Negative + positive: -123.456 + 789.123
                decimal256Addend1 = Decimal256.fromDouble(-123.456, 3);
                decimal256Addend2 = Decimal256.fromDouble(789.123, 3);
                bigDecimalAddend1 = new BigDecimal("-123.456");
                bigDecimalAddend2 = new BigDecimal("789.123");
                break;

            case "NEAR_CANCEL":
                // Addition that nearly cancels out: 123.456 + (-123.455) = 0.001
                decimal256Addend1 = Decimal256.fromDouble(123.456, 3);
                decimal256Addend2 = Decimal256.fromDouble(-123.455, 3);
                bigDecimalAddend1 = new BigDecimal("123.456");
                bigDecimalAddend2 = new BigDecimal("-123.455");
                break;

            case "MIXED_256_128":
                // Addition of 256-bit and 128-bit values: large 256-bit + normal 128-bit
                decimal256Addend1 = new Decimal256();
                decimal256Addend1.ofLong(123456789098765432L, 6);
                decimal256Addend2 = Decimal256.fromDouble(789.123, 3);
                bigDecimalAddend1 = new BigDecimal("123456789098.765432");
                bigDecimalAddend2 = new BigDecimal("789.123");
                break;

            case "PURE_128_BIT":
                // Addition of two 128-bit values within Decimal256
                decimal256Addend1 = Decimal256.fromDouble(123456.789, 3);
                decimal256Addend2 = Decimal256.fromDouble(987654.321, 3);
                bigDecimalAddend1 = new BigDecimal("123456.789");
                bigDecimalAddend2 = new BigDecimal("987654.321");
                break;

            case "VERY_LARGE":
                // Addition with very large numbers that benefit from 256-bit precision
                decimal256Addend1 = Decimal256.fromBigDecimal(new BigDecimal("123456789012345678901234567890.123456789"));
                decimal256Addend2 = Decimal256.fromBigDecimal(new BigDecimal("987654321098765432109876543210.987654321"));
                bigDecimalAddend1 = new BigDecimal("123456789012345678901234567890.123456789");
                bigDecimalAddend2 = new BigDecimal("987654321098765432109876543210.987654321");
                break;
        }
    }
}