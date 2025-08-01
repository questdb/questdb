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
public class Decimal160RoundBenchmark {

    @Param({"ROUND_DOWN", "ROUND_UP", "ROUND_HALFWAY", "ROUND_PRECISION", "ROUND_TRAILING_ZEROS", "ROUND_NEGATIVE"})
    private String scenario;

    @Param({"HALF_UP", "HALF_DOWN", "HALF_EVEN", "UP", "DOWN", "CEILING", "FLOOR"})
    private String roundingMode;

    private Decimal160 decimal160Value;
    private Decimal160 decimal160Result;
    private BigDecimal bigDecimalValue;
    private RoundingMode javaRoundingMode;
    private int targetScale;

    @Setup
    public void setup() {
        decimal160Result = new Decimal160();
        javaRoundingMode = RoundingMode.valueOf(roundingMode);
        
        switch (scenario) {
            case "ROUND_DOWN":
                // Value that needs rounding down: 123.456789 -> 123.457 (scale 3)
                decimal160Value = Decimal160.fromDouble(123.456789, 6);
                bigDecimalValue = new BigDecimal("123.456789");
                targetScale = 3;
                break;

            case "ROUND_UP":
                // Value that needs rounding up: 123.456789 -> 123.46 (scale 2)
                decimal160Value = Decimal160.fromDouble(123.456789, 6);
                bigDecimalValue = new BigDecimal("123.456789");
                targetScale = 2;
                break;

            case "ROUND_HALFWAY":
                // Halfway case: 123.4565 -> depends on rounding mode
                decimal160Value = Decimal160.fromDouble(123.4565, 4);
                bigDecimalValue = new BigDecimal("123.4565");
                targetScale = 3;
                break;

            case "ROUND_PRECISION":
                // High precision rounding: PI to 10 decimal places
                decimal160Value = Decimal160.fromDouble(3.141592653589793, 15);
                bigDecimalValue = new BigDecimal("3.141592653589793");
                targetScale = 10;
                break;

            case "ROUND_TRAILING_ZEROS":
                // Value with trailing zeros: 123.450000 -> 123.45
                decimal160Value = Decimal160.fromDouble(123.450000, 6);
                bigDecimalValue = new BigDecimal("123.450000");
                targetScale = 2;
                break;

            case "ROUND_NEGATIVE":
                // Negative value rounding: -123.456789 -> -123.46
                decimal160Value = Decimal160.fromDouble(-123.456789, 6);
                bigDecimalValue = new BigDecimal("-123.456789");
                targetScale = 2;
                break;
        }
    }

    @Benchmark
    public Decimal160 decimal160Round() {
        decimal160Result.copyFrom(decimal160Value);
        decimal160Result.round(targetScale, javaRoundingMode);
        return decimal160Result;
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
    public void decimal160RoundToZero() {
        // Round to integer (scale 0)
        decimal160Result.copyFrom(decimal160Value);
        decimal160Result.round(0, javaRoundingMode);
    }

    @Benchmark
    public void decimal160RoundToTens() {
        // Round to tens place (scale -1)
        decimal160Result.copyFrom(decimal160Value);
        decimal160Result.round(-1, javaRoundingMode);
    }

    @Benchmark
    public void decimal160RoundMultiple() {
        // Test multiple consecutive rounding operations
        decimal160Result.copyFrom(decimal160Value);
        decimal160Result.round(targetScale, javaRoundingMode);
        decimal160Result.round(targetScale - 1, javaRoundingMode);
        decimal160Result.round(targetScale - 2, javaRoundingMode);
    }

    @Benchmark
    public void decimal160RoundLargeNumber() {
        // Test rounding of large numbers
        Decimal160 largeNumber = new Decimal160();
        largeNumber.set(123456789L, 987654321098765432L, 9);
        
        decimal160Result.copyFrom(largeNumber);
        decimal160Result.round(6, javaRoundingMode);
    }

    @Benchmark
    public void decimal160RoundSmallNumber() {
        // Test rounding of very small numbers
        Decimal160 smallNumber = Decimal160.fromDouble(0.000000123456789, 15);
        
        decimal160Result.copyFrom(smallNumber);
        decimal160Result.round(10, javaRoundingMode);
    }

    public static void main(String[] args) throws RunnerException {
        Options opt = new OptionsBuilder()
                .include(Decimal160RoundBenchmark.class.getSimpleName())
                .warmupIterations(5)
                .measurementIterations(10)
                .forks(1)
                .build();

        new Runner(opt).run();
    }
}