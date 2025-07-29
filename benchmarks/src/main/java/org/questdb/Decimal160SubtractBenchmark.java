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
public class Decimal160SubtractBenchmark {

    @Param({"SIMPLE", "LARGE_NUMBERS", "SMALL_NUMBERS", "DIFFERENT_SCALES", "NEAR_EQUAL", "SIGN_CHANGE"})
    private String scenario;

    private Decimal160 decimal160Minuend;
    private Decimal160 decimal160Subtrahend;
    private Decimal160 decimal160Result;

    private BigDecimal bigDecimalMinuend;
    private BigDecimal bigDecimalSubtrahend;
    private MathContext mathContext;

    @Setup
    public void setup() {
        decimal160Result = new Decimal160();
        mathContext = new MathContext(16, RoundingMode.HALF_UP);

        switch (scenario) {
            case "SIMPLE":
                // Simple subtraction: 789.123 - 123.456
                decimal160Minuend = Decimal160.fromDouble(789.123, 3);
                decimal160Subtrahend = Decimal160.fromDouble(123.456, 3);
                bigDecimalMinuend = new BigDecimal("789.123");
                bigDecimalSubtrahend = new BigDecimal("123.456");
                break;

            case "LARGE_NUMBERS":
                // Large number subtraction: 987654321.123 - 123456789.456
                decimal160Minuend = Decimal160.fromDouble(987654321.123, 3);
                decimal160Subtrahend = Decimal160.fromDouble(123456789.456, 3);
                bigDecimalMinuend = new BigDecimal("987654321.123");
                bigDecimalSubtrahend = new BigDecimal("123456789.456");
                break;

            case "SMALL_NUMBERS":
                // Small number subtraction: 0.00789 - 0.00123
                decimal160Minuend = Decimal160.fromDouble(0.00789, 5);
                decimal160Subtrahend = Decimal160.fromDouble(0.00123, 5);
                bigDecimalMinuend = new BigDecimal("0.00789");
                bigDecimalSubtrahend = new BigDecimal("0.00123");
                break;

            case "DIFFERENT_SCALES":
                // Different scales: 123.456 - 7.8901234
                decimal160Minuend = Decimal160.fromDouble(123.456, 3);
                decimal160Subtrahend = Decimal160.fromDouble(7.8901234, 7);
                bigDecimalMinuend = new BigDecimal("123.456");
                bigDecimalSubtrahend = new BigDecimal("7.8901234");
                break;

            case "NEAR_EQUAL":
                // Near equal subtraction: 123.456789 - 123.456788
                decimal160Minuend = Decimal160.fromDouble(123.456789, 6);
                decimal160Subtrahend = Decimal160.fromDouble(123.456788, 6);
                bigDecimalMinuend = new BigDecimal("123.456789");
                bigDecimalSubtrahend = new BigDecimal("123.456788");
                break;

            case "SIGN_CHANGE":
                // Subtraction causing sign change: 123.456 - 789.123
                decimal160Minuend = Decimal160.fromDouble(123.456, 3);
                decimal160Subtrahend = Decimal160.fromDouble(789.123, 3);
                bigDecimalMinuend = new BigDecimal("123.456");
                bigDecimalSubtrahend = new BigDecimal("789.123");
                break;
        }
    }

    @Benchmark
    public Decimal160 decimal160Subtract() {
        decimal160Result.copyFrom(decimal160Minuend);
        decimal160Result.subtract(decimal160Subtrahend);
        return decimal160Result;
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
    public void decimal160Subtract64Bit() {
        // Test subtraction of two 64-bit values
        Decimal160 val1 = Decimal160.fromDouble(987654.321, 3);
        Decimal160 val2 = Decimal160.fromDouble(123456.789, 3);
        
        decimal160Result.copyFrom(val1);
        decimal160Result.subtract(val2);
    }

    @Benchmark
    public void decimal160Subtract128By64() {
        // Test subtraction of 128-bit minus 64-bit value
        Decimal160 largeMinuend = new Decimal160();
        largeMinuend.set(123456789L, 987654321098765432L, 6);
        
        decimal160Result.copyFrom(largeMinuend);
        decimal160Result.subtract(decimal160Subtrahend);
    }

    @Benchmark
    public void decimal160SubtractNearZero() {
        // Test subtraction resulting in near-zero: 123.456001 - 123.456000
        Decimal160 val1 = Decimal160.fromDouble(123.456001, 6);
        Decimal160 val2 = Decimal160.fromDouble(123.456000, 6);
        
        decimal160Result.copyFrom(val1);
        decimal160Result.subtract(val2);
    }

    @Benchmark
    public void decimal160SubtractNegative() {
        // Test subtraction with negative values: -123.456 - 789.123
        Decimal160 val1 = Decimal160.fromDouble(-123.456, 3);
        Decimal160 val2 = Decimal160.fromDouble(789.123, 3);
        
        decimal160Result.copyFrom(val1);
        decimal160Result.subtract(val2);
    }

    public static void main(String[] args) throws RunnerException {
        Options opt = new OptionsBuilder()
                .include(Decimal160SubtractBenchmark.class.getSimpleName())
                .warmupIterations(5)
                .measurementIterations(10)
                .forks(1)
                .build();

        new Runner(opt).run();
    }
}