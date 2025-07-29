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
public class Decimal160MultiplyBenchmark {

    @Param({"SIMPLE", "LARGE_NUMBERS", "SMALL_NUMBERS", "HIGH_PRECISION", "POWER_OF_10", "MIXED_SCALE"})
    private String scenario;

    private Decimal160 decimal160Factor1;
    private Decimal160 decimal160Factor2;
    private Decimal160 decimal160Result;

    private BigDecimal bigDecimalFactor1;
    private BigDecimal bigDecimalFactor2;
    private MathContext mathContext;

    @Setup
    public void setup() {
        decimal160Result = new Decimal160();
        mathContext = new MathContext(16, RoundingMode.HALF_UP);

        switch (scenario) {
            case "SIMPLE":
                // Simple multiplication: 123.456 * 7.89
                decimal160Factor1 = Decimal160.fromDouble(123.456, 3);
                decimal160Factor2 = Decimal160.fromDouble(7.89, 2);
                bigDecimalFactor1 = new BigDecimal("123.456");
                bigDecimalFactor2 = new BigDecimal("7.89");
                break;

            case "LARGE_NUMBERS":
                // Large number multiplication: 987654321.123 * 123456789.456
                decimal160Factor1 = Decimal160.fromDouble(987654321.123, 3);
                decimal160Factor2 = Decimal160.fromDouble(123456789.456, 3);
                bigDecimalFactor1 = new BigDecimal("987654321.123");
                bigDecimalFactor2 = new BigDecimal("123456789.456");
                break;

            case "SMALL_NUMBERS":
                // Small number multiplication: 0.00123 * 0.00456
                decimal160Factor1 = Decimal160.fromDouble(0.00123, 5);
                decimal160Factor2 = Decimal160.fromDouble(0.00456, 5);
                bigDecimalFactor1 = new BigDecimal("0.00123");
                bigDecimalFactor2 = new BigDecimal("0.00456");
                break;

            case "HIGH_PRECISION":
                // High precision: PI * E with many decimal places
                decimal160Factor1 = Decimal160.fromDouble(3.141592653589793, 15);
                decimal160Factor2 = Decimal160.fromDouble(2.718281828459045, 15);
                bigDecimalFactor1 = new BigDecimal("3.141592653589793");
                bigDecimalFactor2 = new BigDecimal("2.718281828459045");
                break;

            case "POWER_OF_10":
                // Multiplication by power of 10: 123.456 * 100
                decimal160Factor1 = Decimal160.fromDouble(123.456, 3);
                decimal160Factor2 = Decimal160.fromDouble(100.0, 0);
                bigDecimalFactor1 = new BigDecimal("123.456");
                bigDecimalFactor2 = new BigDecimal("100");
                break;

            case "MIXED_SCALE":
                // Mixed scales: 1234567.89 * 0.001234
                decimal160Factor1 = Decimal160.fromDouble(1234567.89, 2);
                decimal160Factor2 = Decimal160.fromDouble(0.001234, 6);
                bigDecimalFactor1 = new BigDecimal("1234567.89");
                bigDecimalFactor2 = new BigDecimal("0.001234");
                break;
        }
    }

    @Benchmark
    public Decimal160 decimal160Multiply() {
        decimal160Result.copyFrom(decimal160Factor1);
        decimal160Result.multiply(decimal160Factor2);
        decimal160Result.round(6, RoundingMode.HALF_UP);
        return decimal160Result;
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
    public void decimal160Multiply64Bit() {
        // Test multiplication of two 64-bit values
        Decimal160 val1 = Decimal160.fromDouble(123456.789, 3);
        Decimal160 val2 = Decimal160.fromDouble(987.654, 3);
        
        decimal160Result.copyFrom(val1);
        decimal160Result.multiply(val2);
        decimal160Result.round(6, RoundingMode.HALF_UP);
    }

    @Benchmark
    public void decimal160Multiply128By64() {
        // Test multiplication of 128-bit by 64-bit value
        Decimal160 largeFactor = new Decimal160();
        largeFactor.set(123456789L, 987654321098765432L, 6);
        
        decimal160Result.copyFrom(largeFactor);
        decimal160Result.multiply(decimal160Factor2);
        decimal160Result.round(6, RoundingMode.HALF_UP);
    }

    public static void main(String[] args) throws RunnerException {
        Options opt = new OptionsBuilder()
                .include(Decimal160MultiplyBenchmark.class.getSimpleName())
                .warmupIterations(5)
                .measurementIterations(10)
                .forks(1)
                .build();

        new Runner(opt).run();
    }
}