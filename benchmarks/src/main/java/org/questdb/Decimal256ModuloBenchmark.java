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
public class Decimal256ModuloBenchmark {

    private BigDecimal bigDecimalDividend;
    private BigDecimal bigDecimalDivisor;
    private Decimal256 decimal256Dividend;
    private Decimal256 decimal256Divisor;
    private Decimal256 decimal256Result;
    private MathContext mathContext;
    @SuppressWarnings("unused")
    @Param({"SIMPLE", "LARGE_NUMBERS", "SMALL_NUMBERS", "DIFFERENT_SCALES", "NEAR_DIVISOR", "FRACTIONAL", "MIXED_256_128", "PURE_128_BIT", "HIGH_PRECISION", "INTEGER_MODULO"})
    private String scenario;

    public static void main(String[] args) throws RunnerException {
        Options opt = new OptionsBuilder()
                .include(Decimal256ModuloBenchmark.class.getSimpleName())
                .warmupIterations(5)
                .measurementIterations(10)
                .forks(1)
                .build();

        new Runner(opt).run();
    }

    @Benchmark
    public BigDecimal bigDecimalModulo() {
        return bigDecimalDividend.remainder(bigDecimalDivisor);
    }

    @Benchmark
    public BigDecimal bigDecimalModuloWithContext() {
        return bigDecimalDividend.remainder(bigDecimalDivisor, mathContext);
    }

    @Benchmark
    public Decimal256 decimal256Modulo() {
        decimal256Result.copyFrom(decimal256Dividend);
        decimal256Result.modulo(decimal256Divisor);
        return decimal256Result;
    }

    @Setup
    public void setup() {
        decimal256Result = new Decimal256();
        mathContext = new MathContext(32, RoundingMode.HALF_UP);

        switch (scenario) {
            case "SIMPLE":
                // Simple modulo: 789.123 % 12.34
                decimal256Dividend = Decimal256.fromDouble(789.123, 3);
                decimal256Divisor = Decimal256.fromDouble(12.34, 2);
                bigDecimalDividend = new BigDecimal("789.123");
                bigDecimalDivisor = new BigDecimal("12.34");
                break;

            case "LARGE_NUMBERS":
                // Large number modulo: 987654321.123 % 123456.789
                decimal256Dividend = Decimal256.fromDouble(987654321.123, 3);
                decimal256Divisor = Decimal256.fromDouble(123456.789, 3);
                bigDecimalDividend = new BigDecimal("987654321.123");
                bigDecimalDivisor = new BigDecimal("123456.789");
                break;

            case "SMALL_NUMBERS":
                // Small number modulo: 0.00789 % 0.00123
                decimal256Dividend = Decimal256.fromDouble(0.00789, 5);
                decimal256Divisor = Decimal256.fromDouble(0.00123, 5);
                bigDecimalDividend = new BigDecimal("0.00789");
                bigDecimalDivisor = new BigDecimal("0.00123");
                break;

            case "DIFFERENT_SCALES":
                // Different scales: 123.456 % 7.8901234
                decimal256Dividend = Decimal256.fromDouble(123.456, 3);
                decimal256Divisor = Decimal256.fromDouble(7.8901234, 7);
                bigDecimalDividend = new BigDecimal("123.456");
                bigDecimalDivisor = new BigDecimal("7.8901234");
                break;

            case "NEAR_DIVISOR":
                // Dividend near divisor: 123.456 % 123.123
                decimal256Dividend = Decimal256.fromDouble(123.456, 3);
                decimal256Divisor = Decimal256.fromDouble(123.123, 3);
                bigDecimalDividend = new BigDecimal("123.456");
                bigDecimalDivisor = new BigDecimal("123.123");
                break;

            case "FRACTIONAL":
                // Fractional modulo: 3.14159 % 2.71828
                decimal256Dividend = Decimal256.fromDouble(3.14159, 5);
                decimal256Divisor = Decimal256.fromDouble(2.71828, 5);
                bigDecimalDividend = new BigDecimal("3.14159");
                bigDecimalDivisor = new BigDecimal("2.71828");
                break;

            case "MIXED_256_128":
                // Modulo of 256-bit by 128-bit value: large 256-bit % normal 128-bit
                decimal256Dividend = new Decimal256();
                decimal256Dividend.ofLong(123456789098765432L, 6);
                decimal256Divisor = Decimal256.fromDouble(12345.678, 3);
                bigDecimalDividend = new BigDecimal("123456789098.765432");
                bigDecimalDivisor = new BigDecimal("12345.678");
                break;

            case "PURE_128_BIT":
                // Modulo of two 128-bit values within Decimal256
                decimal256Dividend = Decimal256.fromDouble(987654.321, 3);
                decimal256Divisor = Decimal256.fromDouble(123.456, 3);
                bigDecimalDividend = new BigDecimal("987654.321");
                bigDecimalDivisor = new BigDecimal("123.456");
                break;

            case "HIGH_PRECISION":
                // High precision modulo with very large numbers
                decimal256Dividend = Decimal256.fromBigDecimal(new BigDecimal("123456789012345678901234.123456789"));
                decimal256Divisor = Decimal256.fromBigDecimal(new BigDecimal("987654321098.765432"));
                bigDecimalDividend = new BigDecimal("123456789012345678901234.123456789");
                bigDecimalDivisor = new BigDecimal("987654321098.765432");
                break;

            case "INTEGER_MODULO":
                // Integer modulo: 1000000 % 7
                decimal256Dividend = Decimal256.fromLong(1000000, 0);
                decimal256Divisor = Decimal256.fromLong(7, 0);
                bigDecimalDividend = new BigDecimal("1000000");
                bigDecimalDivisor = new BigDecimal("7");
                break;
        }
    }
}