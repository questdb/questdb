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
public class Decimal128SubtractBenchmark {

    @Param({"SIMPLE", "LARGE_NUMBERS", "SMALL_NUMBERS", "DIFFERENT_SCALES", "NEAR_EQUAL", "SIGN_CHANGE"})
    private String scenario;

    private Decimal128 decimal128Minuend;
    private Decimal128 decimal128Subtrahend;
    private Decimal128 decimal128Result;

    private BigDecimal bigDecimalMinuend;
    private BigDecimal bigDecimalSubtrahend;
    private MathContext mathContext;

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
        }
    }

    @Benchmark
    public Decimal128 decimal128Subtract() {
        decimal128Result.copyFrom(decimal128Minuend);
        decimal128Result.subtract(decimal128Subtrahend);
        return decimal128Result;
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
    public void decimal128Subtract64Bit() {
        // Test subtraction of two 64-bit values
        Decimal128 val1 = Decimal128.fromDouble(987654.321, 3);
        Decimal128 val2 = Decimal128.fromDouble(123456.789, 3);
        
        decimal128Result.copyFrom(val1);
        decimal128Result.subtract(val2);
    }

    @Benchmark
    public void decimal128Subtract128By64() {
        // Test subtraction of 128-bit minus 64-bit value
        Decimal128 largeMinuend = new Decimal128();
        largeMinuend.set(123456789L, 987654321098765432L, 6);
        
        decimal128Result.copyFrom(largeMinuend);
        decimal128Result.subtract(decimal128Subtrahend);
    }

    @Benchmark
    public void decimal128SubtractNearZero() {
        // Test subtraction resulting in near-zero: 123.456001 - 123.456000
        Decimal128 val1 = Decimal128.fromDouble(123.456001, 6);
        Decimal128 val2 = Decimal128.fromDouble(123.456000, 6);
        
        decimal128Result.copyFrom(val1);
        decimal128Result.subtract(val2);
    }

    @Benchmark
    public void decimal128SubtractNegative() {
        // Test subtraction with negative values: -123.456 - 789.123
        Decimal128 val1 = Decimal128.fromDouble(-123.456, 3);
        Decimal128 val2 = Decimal128.fromDouble(789.123, 3);
        
        decimal128Result.copyFrom(val1);
        decimal128Result.subtract(val2);
    }

    public static void main(String[] args) throws RunnerException {
        Options opt = new OptionsBuilder()
                .include(Decimal128SubtractBenchmark.class.getSimpleName())
                .warmupIterations(5)
                .measurementIterations(10)
                .forks(1)
                .build();

        new Runner(opt).run();
    }
}