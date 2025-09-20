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
public class Decimal256DivideBenchmark {

    private BigDecimal bigDecimalDividend;
    private BigDecimal bigDecimalDivisor;
    private Decimal256 decimal256Dividend;
    private Decimal256 decimal256Divisor;
    private Decimal256 decimal256Result;
    private MathContext mathContext;
    @SuppressWarnings("unused")
    @Param({"SIMPLE", "LARGE_NUMBERS", "HIGH_PRECISION", "DIFFERENT_SCALES", "REPEATING_DECIMAL", "INTEGER_DIVISION", "MIXED_256_128", "PURE_128_BIT", "SMALL_DIVISOR"})
    private String scenario;

    public static void main(String[] args) throws RunnerException {
        Options opt = new OptionsBuilder()
                .include(Decimal256DivideBenchmark.class.getSimpleName())
                .warmupIterations(5)
                .measurementIterations(10)
                .forks(1)
                .build();

        new Runner(opt).run();
    }

    @Benchmark
    public BigDecimal bigDecimalDivide() {
        return bigDecimalDividend.divide(bigDecimalDivisor, 10, RoundingMode.HALF_UP);
    }

    @Benchmark
    public BigDecimal bigDecimalDivideWithContext() {
        return bigDecimalDividend.divide(bigDecimalDivisor, mathContext);
    }

    @Benchmark
    public Decimal256 decimal256Divide() {
        decimal256Result.copyFrom(decimal256Dividend);
        decimal256Result.divide(decimal256Divisor, 10, RoundingMode.HALF_UP);
        return decimal256Result;
    }

    @Setup
    public void setup() {
        decimal256Result = new Decimal256();
        mathContext = new MathContext(32, RoundingMode.HALF_UP);

        switch (scenario) {
            case "SIMPLE":
                // Simple division: 100.0 / 4.0
                decimal256Dividend = Decimal256.fromDouble(100.0, 1);
                decimal256Divisor = Decimal256.fromDouble(4.0, 1);
                bigDecimalDividend = new BigDecimal("100.0");
                bigDecimalDivisor = new BigDecimal("4.0");
                break;

            case "LARGE_NUMBERS":
                // Large number division: 987654321.123 / 123.456
                decimal256Dividend = Decimal256.fromDouble(987654321.123, 3);
                decimal256Divisor = Decimal256.fromDouble(123.456, 3);
                bigDecimalDividend = new BigDecimal("987654321.123");
                bigDecimalDivisor = new BigDecimal("123.456");
                break;

            case "HIGH_PRECISION":
                // High precision division
                decimal256Dividend = Decimal256.fromBigDecimal(new BigDecimal("123456789012345.123456789"));
                decimal256Divisor = Decimal256.fromBigDecimal(new BigDecimal("987.654321098765"));
                bigDecimalDividend = new BigDecimal("123456789012345.123456789");
                bigDecimalDivisor = new BigDecimal("987.654321098765");
                break;

            case "DIFFERENT_SCALES":
                // Different scales: 123.456 / 7.89
                decimal256Dividend = Decimal256.fromDouble(123.456, 3);
                decimal256Divisor = Decimal256.fromDouble(7.89, 2);
                bigDecimalDividend = new BigDecimal("123.456");
                bigDecimalDivisor = new BigDecimal("7.89");
                break;

            case "REPEATING_DECIMAL":
                // Division that results in repeating decimal: 10 / 3
                decimal256Dividend = Decimal256.fromLong(10, 0);
                decimal256Divisor = Decimal256.fromLong(3, 0);
                bigDecimalDividend = new BigDecimal("10");
                bigDecimalDivisor = new BigDecimal("3");
                break;

            case "INTEGER_DIVISION":
                // Integer division: 1000000 / 7
                decimal256Dividend = Decimal256.fromLong(1000000, 0);
                decimal256Divisor = Decimal256.fromLong(7, 0);
                bigDecimalDividend = new BigDecimal("1000000");
                bigDecimalDivisor = new BigDecimal("7");
                break;

            case "MIXED_256_128":
                // Division of 256-bit by 128-bit value: large 256-bit / normal 128-bit
                decimal256Dividend = new Decimal256();
                decimal256Dividend.ofLong(123456789098765432L, 6);
                decimal256Divisor = Decimal256.fromDouble(123.456, 3);
                bigDecimalDividend = new BigDecimal("123456789098.765432");
                bigDecimalDivisor = new BigDecimal("123.456");
                break;

            case "PURE_128_BIT":
                // Division of two 128-bit values within Decimal256
                decimal256Dividend = Decimal256.fromDouble(987654.321, 3);
                decimal256Divisor = Decimal256.fromDouble(123.456, 3);
                bigDecimalDividend = new BigDecimal("987654.321");
                bigDecimalDivisor = new BigDecimal("123.456");
                break;

            case "SMALL_DIVISOR":
                // Division by small divisor: 123456.789 / 0.001
                decimal256Dividend = Decimal256.fromDouble(123456.789, 3);
                decimal256Divisor = Decimal256.fromDouble(0.001, 3);
                bigDecimalDividend = new BigDecimal("123456.789");
                bigDecimalDivisor = new BigDecimal("0.001");
                break;
        }
    }
}