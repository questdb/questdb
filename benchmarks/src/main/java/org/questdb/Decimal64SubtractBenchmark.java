/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2026 QuestDB
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

import io.questdb.std.Decimal64;
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
public class Decimal64SubtractBenchmark {

    private BigDecimal bigDecimalMinuend;
    private BigDecimal bigDecimalSubtrahend;
    private Decimal64 decimal64Minuend;
    private Decimal64 decimal64Result;
    private Decimal64 decimal64Subtrahend;
    private MathContext mathContext;
    @SuppressWarnings("unused")
    @Param({"SIMPLE", "LARGE_NUMBERS", "SMALL_NUMBERS", "DIFFERENT_SCALES", "NEAR_EQUAL", "SIGN_CHANGE"})
    private String scenario;


    public static void main(String[] args) throws RunnerException {
        Options opt = new OptionsBuilder()
                .include(Decimal64SubtractBenchmark.class.getSimpleName())
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
    public Decimal64 decimal64Subtract() {
        decimal64Result.copyFrom(decimal64Minuend);
        decimal64Result.subtract(decimal64Subtrahend);
        return decimal64Result;
    }

    @Setup
    public void setup() {
        decimal64Result = new Decimal64();
        mathContext = new MathContext(18, RoundingMode.HALF_UP);


        switch (scenario) {
            case "SIMPLE":
                // Simple subtraction: 789.123 - 123.456
                decimal64Minuend = Decimal64.fromLong(789123L, 3);
                decimal64Subtrahend = Decimal64.fromLong(123456L, 3);
                bigDecimalMinuend = new BigDecimal("789.123");
                bigDecimalSubtrahend = new BigDecimal("123.456");
                break;

            case "LARGE_NUMBERS":
                // Large number subtraction: 12345678.123 - 1234567.456
                decimal64Minuend = Decimal64.fromLong(12345678123L, 3);
                decimal64Subtrahend = Decimal64.fromLong(1234567456L, 3);
                bigDecimalMinuend = new BigDecimal("12345678.123");
                bigDecimalSubtrahend = new BigDecimal("1234567.456");
                break;

            case "SMALL_NUMBERS":
                // Small number subtraction: 0.00789 - 0.00123
                decimal64Minuend = Decimal64.fromLong(789L, 5);
                decimal64Subtrahend = Decimal64.fromLong(123L, 5);
                bigDecimalMinuend = new BigDecimal("0.00789");
                bigDecimalSubtrahend = new BigDecimal("0.00123");
                break;

            case "DIFFERENT_SCALES":
                // Different scales: 123.456 - 7.8901234
                decimal64Minuend = Decimal64.fromLong(123456L, 3);
                decimal64Subtrahend = Decimal64.fromLong(78901234L, 7);
                bigDecimalMinuend = new BigDecimal("123.456");
                bigDecimalSubtrahend = new BigDecimal("7.8901234");
                break;

            case "NEAR_EQUAL":
                // Near equal subtraction: 123.456789 - 123.456788
                decimal64Minuend = Decimal64.fromLong(123456789L, 6);
                decimal64Subtrahend = Decimal64.fromLong(123456788L, 6);
                bigDecimalMinuend = new BigDecimal("123.456789");
                bigDecimalSubtrahend = new BigDecimal("123.456788");
                break;

            case "SIGN_CHANGE":
                // Subtraction causing sign change: 123.456 - 789.123
                decimal64Minuend = Decimal64.fromLong(123456L, 3);
                decimal64Subtrahend = Decimal64.fromLong(789123L, 3);
                bigDecimalMinuend = new BigDecimal("123.456");
                bigDecimalSubtrahend = new BigDecimal("789.123");
                break;
        }
    }
}