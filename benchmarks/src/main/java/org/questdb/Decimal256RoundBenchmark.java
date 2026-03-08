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
public class Decimal256RoundBenchmark {

    private BigDecimal bigDecimalValue;
    private Decimal256 decimal256Result;
    private Decimal256 decimal256Value;
    private RoundingMode javaRoundingMode;
    @SuppressWarnings("unused")
    @Param({"HALF_UP", "HALF_DOWN", "HALF_EVEN", "UP", "DOWN", "CEILING", "FLOOR"})
    private String roundingMode;
    @SuppressWarnings("unused")
    @Param({"SIMPLE", "LARGE_NUMBER", "SMALL_NUMBER", "HIGH_PRECISION", "SCALE_INCREASE", "SCALE_DECREASE", "VERY_LARGE", "VERY_SMALL", "MULTIPLE_ROUNDS", "TO_INTEGER", "TO_TENS"})
    private String scenario;
    private int targetScale;

    public static void main(String[] args) throws RunnerException {
        Options opt = new OptionsBuilder()
                .include(Decimal256RoundBenchmark.class.getSimpleName())
                .warmupIterations(5)
                .measurementIterations(10)
                .forks(1)
                .build();

        new Runner(opt).run();
    }

    @Benchmark
    public BigDecimal bigDecimalRound() {
        return bigDecimalValue.setScale(targetScale, javaRoundingMode);
    }

    @Benchmark
    public BigDecimal bigDecimalRoundWithContext() {
        MathContext mc = new MathContext(targetScale + 3, javaRoundingMode);
        return bigDecimalValue.round(mc);
    }

    @Benchmark
    public Decimal256 decimal256Round() {
        decimal256Result.copyFrom(decimal256Value);

        if ("MULTIPLE_ROUNDS".equals(scenario)) {
            // Multiple consecutive rounding operations
            decimal256Result.round(targetScale, javaRoundingMode);
            decimal256Result.round(targetScale - 2, javaRoundingMode);
            decimal256Result.round(targetScale - 4, javaRoundingMode);
        } else {
            decimal256Result.round(targetScale, javaRoundingMode);
        }

        return decimal256Result;
    }

    @Setup
    public void setup() {
        decimal256Result = new Decimal256();
        javaRoundingMode = RoundingMode.valueOf(roundingMode);

        switch (scenario) {
            case "SIMPLE":
                // Simple rounding: 123.456789 -> 5 decimal places
                decimal256Value = Decimal256.fromDouble(123.456789, 6);
                bigDecimalValue = new BigDecimal("123.456789");
                targetScale = 5;
                break;

            case "LARGE_NUMBER":
                // Large number rounding
                decimal256Value = Decimal256.fromBigDecimal(new BigDecimal("987654321012345.123456789"));
                bigDecimalValue = new BigDecimal("987654321012345.123456789");
                targetScale = 6;
                break;

            case "SMALL_NUMBER":
                // Small number rounding
                decimal256Value = Decimal256.fromDouble(0.123456789, 9);
                bigDecimalValue = new BigDecimal("0.123456789");
                targetScale = 6;
                break;

            case "HIGH_PRECISION":
                // High precision rounding
                decimal256Value = Decimal256.fromBigDecimal(new BigDecimal("123456789.123456789012345678901234567890"));
                bigDecimalValue = new BigDecimal("123456789.123456789012345678901234567890");
                targetScale = 15;
                break;

            case "SCALE_INCREASE":
                // Scale increase (adding zeros)
                decimal256Value = Decimal256.fromDouble(123.45, 2);
                bigDecimalValue = new BigDecimal("123.45");
                targetScale = 5;
                break;

            case "SCALE_DECREASE":
                // Scale decrease (removing digits)
                decimal256Value = Decimal256.fromDouble(123.456789, 6);
                bigDecimalValue = new BigDecimal("123.456789");
                targetScale = 3;
                break;

            case "VERY_LARGE":
                // Very large number rounding: large 256-bit value
                decimal256Value = Decimal256.fromBigDecimal(new BigDecimal("123456789012345678901234.123456789"));
                bigDecimalValue = new BigDecimal("123456789012345678901234.123456789");
                targetScale = 6;
                break;

            case "VERY_SMALL":
                // Very small number rounding: 0.000000123456789
                decimal256Value = Decimal256.fromBigDecimal(new BigDecimal("0.000000123456789"));
                bigDecimalValue = new BigDecimal("0.000000123456789");
                targetScale = 10;
                break;

            case "MULTIPLE_ROUNDS":
                // Multiple consecutive rounding operations: start with high precision
                decimal256Value = Decimal256.fromDouble(123.456789012345, 12);
                bigDecimalValue = new BigDecimal("123.456789012345");
                targetScale = 8; // Will be rounded multiple times in benchmark
                break;

            case "TO_INTEGER":
                // Round to integer (scale 0): 123.456 -> 123
                decimal256Value = Decimal256.fromDouble(123.456, 3);
                bigDecimalValue = new BigDecimal("123.456");
                targetScale = 0;
                break;

            case "TO_TENS":
                // Round to tens place (scale -1): 123.456 -> 120
                decimal256Value = Decimal256.fromDouble(123.456, 3);
                bigDecimalValue = new BigDecimal("123.456");
                targetScale = -1;
                break;
        }
    }
}