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
public class Decimal256RoundBenchmark {

    private BigDecimal bigDecimalValue;
    private Decimal256 decimal256Value;
    private Decimal256 decimal256Result;
    private MathContext mathContext;
    @Param({"SIMPLE", "LARGE_NUMBER", "SMALL_NUMBER", "HIGH_PRECISION", "SCALE_INCREASE", "SCALE_DECREASE"})
    private String scenario;

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
        return bigDecimalValue.setScale(5, RoundingMode.HALF_UP);
    }

    @Benchmark
    public BigDecimal bigDecimalRoundWithContext() {
        return bigDecimalValue.round(mathContext);
    }

    @Benchmark
    public Decimal256 decimal256Round() {
        decimal256Result.copyFrom(decimal256Value);
        decimal256Result.round(5, RoundingMode.HALF_UP);
        return decimal256Result;
    }

    @Benchmark
    public void decimal256RoundLargeNumber() {
        // Test rounding of very large numbers
        Decimal256 largeValue = Decimal256.fromBigDecimal(new BigDecimal("123456789012345678901234.123456789"));
        largeValue.round(10, RoundingMode.HALF_UP);
    }

    @Benchmark
    public void decimal256RoundSmallNumber() {
        // Test rounding of very small numbers
        Decimal256 smallValue = Decimal256.fromBigDecimal(new BigDecimal("0.000000123456789"));
        smallValue.round(8, RoundingMode.HALF_UP);
    }

    @Benchmark
    public void decimal256RoundMultiple() {
        // Test multiple rounding operations
        Decimal256 value = Decimal256.fromDouble(123.456789, 6);
        value.round(4, RoundingMode.HALF_UP);
        value.round(2, RoundingMode.HALF_UP);
        value.round(0, RoundingMode.HALF_UP);
    }

    @Benchmark
    public void decimal256RoundToTens() {
        // Test rounding to tens place (scale -1)
        Decimal256 value = Decimal256.fromDouble(12345.67, 2);
        value.round(0, RoundingMode.HALF_UP);
    }

    @Benchmark
    public void decimal256RoundToZero() {
        // Test rounding to zero decimal places
        Decimal256 value = Decimal256.fromDouble(123.456, 3);
        value.round(0, RoundingMode.HALF_UP);
    }

    @Setup
    public void setup() {
        decimal256Result = new Decimal256();
        mathContext = new MathContext(20, RoundingMode.HALF_UP);

        switch (scenario) {
            case "SIMPLE":
                // Simple rounding: 123.456789 -> 5 decimal places
                decimal256Value = Decimal256.fromDouble(123.456789, 6);
                bigDecimalValue = new BigDecimal("123.456789");
                break;

            case "LARGE_NUMBER":
                // Large number rounding
                decimal256Value = Decimal256.fromBigDecimal(new BigDecimal("987654321012345.123456789"));
                bigDecimalValue = new BigDecimal("987654321012345.123456789");
                break;

            case "SMALL_NUMBER":
                // Small number rounding
                decimal256Value = Decimal256.fromDouble(0.123456789, 9);
                bigDecimalValue = new BigDecimal("0.123456789");
                break;

            case "HIGH_PRECISION":
                // High precision rounding
                decimal256Value = Decimal256.fromBigDecimal(new BigDecimal("123456789.123456789012345678901234567890"));
                bigDecimalValue = new BigDecimal("123456789.123456789012345678901234567890");
                break;

            case "SCALE_INCREASE":
                // Scale increase (adding zeros)
                decimal256Value = Decimal256.fromDouble(123.45, 2);
                bigDecimalValue = new BigDecimal("123.45");
                break;

            case "SCALE_DECREASE":
                // Scale decrease (removing digits)
                decimal256Value = Decimal256.fromDouble(123.456789, 6);
                bigDecimalValue = new BigDecimal("123.456789");
                break;
        }
    }
}