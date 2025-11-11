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
public class Decimal128SubtractBenchmark {

    private BigDecimal bigDecimalMinuend;
    private BigDecimal bigDecimalSubtrahend;
    private Decimal128 decimal128Minuend;
    private Decimal128 decimal128Result;
    private Decimal128 decimal128Subtrahend;
    private MathContext mathContext;
    @SuppressWarnings("unused")
    @Param({"SIMPLE", "LARGE_NUMBERS", "SMALL_NUMBERS", "DIFFERENT_SCALES", "NEAR_EQUAL", "SIGN_CHANGE", "MIXED_128_64", "PURE_64_BIT", "NEAR_ZERO", "NEGATIVE"})
    private String scenario;

    public static void main(String[] args) throws RunnerException {
        Options opt = new OptionsBuilder()
                .include(Decimal128SubtractBenchmark.class.getSimpleName())
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
    public Decimal128 decimal128Subtract() {
        decimal128Result.copyFrom(decimal128Minuend);
        decimal128Result.subtract(decimal128Subtrahend);
        return decimal128Result;
    }


    @Setup
    public void setup() {
        decimal128Result = new Decimal128();
        mathContext = new MathContext(16, RoundingMode.HALF_UP);


        switch (scenario) {
            case "SIMPLE":
                // Simple subtraction: 789.123 - 123.456
                decimal128Minuend = Decimal128.fromDouble(789.123, 3);
                decimal128Subtrahend = Decimal128.fromDouble(123.456, 3);
                bigDecimalMinuend = new BigDecimal("789.123");
                bigDecimalSubtrahend = new BigDecimal("123.456");
                break;

            case "LARGE_NUMBERS":
                // Large number subtraction: 987654321.123 - 123456789.456
                decimal128Minuend = Decimal128.fromDouble(987654321.123, 3);
                decimal128Subtrahend = Decimal128.fromDouble(123456789.456, 3);
                bigDecimalMinuend = new BigDecimal("987654321.123");
                bigDecimalSubtrahend = new BigDecimal("123456789.456");
                break;

            case "SMALL_NUMBERS":
                // Small number subtraction: 0.00789 - 0.00123
                decimal128Minuend = Decimal128.fromDouble(0.00789, 5);
                decimal128Subtrahend = Decimal128.fromDouble(0.00123, 5);
                bigDecimalMinuend = new BigDecimal("0.00789");
                bigDecimalSubtrahend = new BigDecimal("0.00123");
                break;

            case "DIFFERENT_SCALES":
                // Different scales: 123.456 - 7.8901234
                decimal128Minuend = Decimal128.fromDouble(123.456, 3);
                decimal128Subtrahend = Decimal128.fromDouble(7.8901234, 7);
                bigDecimalMinuend = new BigDecimal("123.456");
                bigDecimalSubtrahend = new BigDecimal("7.8901234");
                break;

            case "NEAR_EQUAL":
                // Near equal subtraction: 123.456789 - 123.456788
                decimal128Minuend = Decimal128.fromDouble(123.456789, 6);
                decimal128Subtrahend = Decimal128.fromDouble(123.456788, 6);
                bigDecimalMinuend = new BigDecimal("123.456789");
                bigDecimalSubtrahend = new BigDecimal("123.456788");
                break;

            case "SIGN_CHANGE":
                // Subtraction causing sign change: 123.456 - 789.123
                decimal128Minuend = Decimal128.fromDouble(123.456, 3);
                decimal128Subtrahend = Decimal128.fromDouble(789.123, 3);
                bigDecimalMinuend = new BigDecimal("123.456");
                bigDecimalSubtrahend = new BigDecimal("789.123");
                break;

            case "MIXED_128_64":
                // Subtraction of 128-bit minus 64-bit value: large 128-bit - normal 64-bit
                decimal128Minuend = new Decimal128();
                decimal128Minuend.of(123456789L, 987654321098765432L, 6);
                decimal128Subtrahend = Decimal128.fromDouble(789.123, 3);
                bigDecimalMinuend = new BigDecimal("123456789987654321098.765432");
                bigDecimalSubtrahend = new BigDecimal("789.123");
                break;

            case "PURE_64_BIT":
                // Subtraction of two 64-bit values within Decimal128
                decimal128Minuend = Decimal128.fromDouble(987654.321, 3);
                decimal128Subtrahend = Decimal128.fromDouble(123456.789, 3);
                bigDecimalMinuend = new BigDecimal("987654.321");
                bigDecimalSubtrahend = new BigDecimal("123456.789");
                break;

            case "NEAR_ZERO":
                // Subtraction resulting in near-zero: 123.456001 - 123.456000
                decimal128Minuend = Decimal128.fromDouble(123.456001, 6);
                decimal128Subtrahend = Decimal128.fromDouble(123.456000, 6);
                bigDecimalMinuend = new BigDecimal("123.456001");
                bigDecimalSubtrahend = new BigDecimal("123.456000");
                break;

            case "NEGATIVE":
                // Subtraction with negative values: -123.456 - 789.123
                decimal128Minuend = Decimal128.fromDouble(-123.456, 3);
                decimal128Subtrahend = Decimal128.fromDouble(789.123, 3);
                bigDecimalMinuend = new BigDecimal("-123.456");
                bigDecimalSubtrahend = new BigDecimal("789.123");
                break;
        }
    }
}