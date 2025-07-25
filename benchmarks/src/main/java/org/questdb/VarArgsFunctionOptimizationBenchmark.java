/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2024 QuestDB
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

import java.util.concurrent.TimeUnit;

import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;

/**
 * Benchmark comparing loop-based vs loop-unrolled implementations of var-args functions.
 * Tests greatest(), least(), coalesce() function patterns with 2-5 arguments.
 * This benchmark demonstrates the performance benefits of loop unrolling for short argument lists.
 */
@State(Scope.Benchmark)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@Warmup(iterations = 5, time = 1, timeUnit = TimeUnit.SECONDS)
@Measurement(iterations = 10, time = 1, timeUnit = TimeUnit.SECONDS)
@Fork(3)
public class VarArgsFunctionOptimizationBenchmark {
    
    // Test data arrays for direct calculations
    private final double[] data = {10.5, 20.3, 15.7, 8.2, 30.1};
    private final long[] longData = {105L, 203L, 157L, 82L, 301L};
    
    // GREATEST function benchmarks
    @Benchmark
    public double greatestLoopBased2() {
        double max = Double.NEGATIVE_INFINITY;
        for (int i = 0; i < 2; i++) {
            double val = data[i];
            if (!Double.isNaN(val)) {
                max = Math.max(max, val);
            }
        }
        return max;
    }
    
    @Benchmark
    public double greatestUnrolled2() {
        double v1 = data[0];
        double v2 = data[1];
        
        if (Double.isNaN(v1)) {
            return Double.isNaN(v2) ? Double.NaN : v2;
        }
        if (Double.isNaN(v2)) {
            return v1;
        }
        return Math.max(v1, v2);
    }
    
    @Benchmark
    public double greatestLoopBased3() {
        double max = Double.NEGATIVE_INFINITY;
        for (int i = 0; i < 3; i++) {
            double val = data[i];
            if (!Double.isNaN(val)) {
                max = Math.max(max, val);
            }
        }
        return max;
    }
    
    @Benchmark
    public double greatestUnrolled3() {
        double v1 = data[0];
        double v2 = data[1];
        double v3 = data[2];
        
        double max = Double.NEGATIVE_INFINITY;
        boolean hasValidValue = false;
        
        if (!Double.isNaN(v1)) {
            max = v1;
            hasValidValue = true;
        }
        if (!Double.isNaN(v2)) {
            max = hasValidValue ? Math.max(max, v2) : v2;
            hasValidValue = true;
        }
        if (!Double.isNaN(v3)) {
            max = hasValidValue ? Math.max(max, v3) : v3;
            hasValidValue = true;
        }
        
        return hasValidValue ? max : Double.NaN;
    }
    
    @Benchmark
    public double greatestLoopBased4() {
        double max = Double.NEGATIVE_INFINITY;
        for (int i = 0; i < 4; i++) {
            double val = data[i];
            if (!Double.isNaN(val)) {
                max = Math.max(max, val);
            }
        }
        return max;
    }
    
    @Benchmark
    public double greatestUnrolled4() {
        double v1 = data[0];
        double v2 = data[1];
        double v3 = data[2];
        double v4 = data[3];
        
        double max = Double.NEGATIVE_INFINITY;
        boolean hasValidValue = false;
        
        if (!Double.isNaN(v1)) {
            max = v1;
            hasValidValue = true;
        }
        if (!Double.isNaN(v2)) {
            max = hasValidValue ? Math.max(max, v2) : v2;
            hasValidValue = true;
        }
        if (!Double.isNaN(v3)) {
            max = hasValidValue ? Math.max(max, v3) : v3;
            hasValidValue = true;
        }
        if (!Double.isNaN(v4)) {
            max = hasValidValue ? Math.max(max, v4) : v4;
            hasValidValue = true;
        }
        
        return hasValidValue ? max : Double.NaN;
    }
    
    @Benchmark
    public double greatestLoopBased5() {
        double max = Double.NEGATIVE_INFINITY;
        for (int i = 0; i < 5; i++) {
            double val = data[i];
            if (!Double.isNaN(val)) {
                max = Math.max(max, val);
            }
        }
        return max;
    }
    
    @Benchmark
    public double greatestUnrolled5() {
        double v1 = data[0];
        double v2 = data[1];
        double v3 = data[2];
        double v4 = data[3];
        double v5 = data[4];
        
        double max = Double.NEGATIVE_INFINITY;
        boolean hasValidValue = false;
        
        if (!Double.isNaN(v1)) {
            max = v1;
            hasValidValue = true;
        }
        if (!Double.isNaN(v2)) {
            max = hasValidValue ? Math.max(max, v2) : v2;
            hasValidValue = true;
        }
        if (!Double.isNaN(v3)) {
            max = hasValidValue ? Math.max(max, v3) : v3;
            hasValidValue = true;
        }
        if (!Double.isNaN(v4)) {
            max = hasValidValue ? Math.max(max, v4) : v4;
            hasValidValue = true;
        }
        if (!Double.isNaN(v5)) {
            max = hasValidValue ? Math.max(max, v5) : v5;
            hasValidValue = true;
        }
        
        return hasValidValue ? max : Double.NaN;
    }
    
    // LEAST function benchmarks
    @Benchmark
    public double leastLoopBased2() {
        double result = Double.MAX_VALUE;
        for (int i = 0; i < 2; i++) {
            double val = data[i];
            if (val != val) return val; // NaN
            result = Math.min(result, val);
        }
        return result;
    }
    
    @Benchmark
    public double leastUnrolled2() {
        double val1 = data[0];
        double val2 = data[1];
        
        if (val1 != val1) return val1; // NaN
        if (val2 != val2) return val2; // NaN
        
        return Math.min(val1, val2);
    }
    
    @Benchmark
    public double leastLoopBased3() {
        double result = Double.MAX_VALUE;
        for (int i = 0; i < 3; i++) {
            double val = data[i];
            if (val != val) return val; // NaN
            result = Math.min(result, val);
        }
        return result;
    }
    
    @Benchmark
    public double leastUnrolled3() {
        double val1 = data[0];
        double val2 = data[1];
        double val3 = data[2];
        
        if (val1 != val1) return val1; // NaN
        if (val2 != val2) return val2; // NaN
        if (val3 != val3) return val3; // NaN
        
        return Math.min(Math.min(val1, val2), val3);
    }
    
    @Benchmark
    public double leastLoopBased4() {
        double result = Double.MAX_VALUE;
        for (int i = 0; i < 4; i++) {
            double val = data[i];
            if (val != val) return val; // NaN
            result = Math.min(result, val);
        }
        return result;
    }
    
    @Benchmark
    public double leastUnrolled4() {
        double val1 = data[0];
        double val2 = data[1];
        double val3 = data[2];
        double val4 = data[3];
        
        if (val1 != val1) return val1; // NaN
        if (val2 != val2) return val2; // NaN
        if (val3 != val3) return val3; // NaN
        if (val4 != val4) return val4; // NaN
        
        return Math.min(Math.min(val1, val2), Math.min(val3, val4));
    }
    
    @Benchmark
    public double leastLoopBased5() {
        double result = Double.MAX_VALUE;
        for (int i = 0; i < 5; i++) {
            double val = data[i];
            if (val != val) return val; // NaN
            result = Math.min(result, val);
        }
        return result;
    }
    
    @Benchmark
    public double leastUnrolled5() {
        double val1 = data[0];
        double val2 = data[1];
        double val3 = data[2];
        double val4 = data[3];
        double val5 = data[4];
        
        if (val1 != val1) return val1; // NaN
        if (val2 != val2) return val2; // NaN
        if (val3 != val3) return val3; // NaN
        if (val4 != val4) return val4; // NaN
        if (val5 != val5) return val5; // NaN
        
        return Math.min(Math.min(Math.min(val1, val2), Math.min(val3, val4)), val5);
    }

    // COALESCE function benchmarks
    @Benchmark
    public double coalesceLoopBased2() {
        for (int i = 0; i < 2; i++) {
            double value = data[i];
            if (value == value) { // Not NaN
                return value;
            }
        }
        return Double.NaN;
    }
    
    @Benchmark
    public double coalesceUnrolled2() {
        double value = data[0];
        if (value == value) return value;  // Not NaN
        return data[1];
    }
    
    @Benchmark
    public double coalesceLoopBased3() {
        for (int i = 0; i < 3; i++) {
            double value = data[i];
            if (value == value) { // Not NaN
                return value;
            }
        }
        return Double.NaN;
    }
    
    @Benchmark
    public double coalesceUnrolled3() {
        double value = data[0];
        if (value == value) return value;  // Not NaN
        
        value = data[1];
        if (value == value) return value;  // Not NaN
        
        return data[2];
    }
    
    @Benchmark
    public double coalesceLoopBased4() {
        for (int i = 0; i < 4; i++) {
            double value = data[i];
            if (value == value) { // Not NaN
                return value;
            }
        }
        return Double.NaN;
    }
    
    @Benchmark
    public double coalesceUnrolled4() {
        double value = data[0];
        if (value == value) return value;  // Not NaN
        
        value = data[1];
        if (value == value) return value;  // Not NaN
        
        value = data[2];
        if (value == value) return value;  // Not NaN
        
        return data[3];
    }
    
    @Benchmark
    public double coalesceLoopBased5() {
        for (int i = 0; i < 5; i++) {
            double value = data[i];
            if (value == value) { // Not NaN
                return value;
            }
        }
        return Double.NaN;
    }
    
    @Benchmark
    public double coalesceUnrolled5() {
        double value = data[0];
        if (value == value) return value;  // Not NaN
        
        value = data[1];
        if (value == value) return value;  // Not NaN
        
        value = data[2];
        if (value == value) return value;  // Not NaN
        
        value = data[3];
        if (value == value) return value;  // Not NaN
        
        return data[4];
    }

    // LONG functions to test with integer types
    @Benchmark
    public long longGreatestLoopBased3() {
        long max = Long.MIN_VALUE;
        for (int i = 0; i < 3; i++) {
            long val = longData[i];
            if (val != Long.MIN_VALUE) { // Not null value
                max = Math.max(max, val);
            }
        }
        return max;
    }
    
    @Benchmark
    public long longGreatestUnrolled3() {
        long v1 = longData[0];
        long v2 = longData[1];
        long v3 = longData[2];
        
        long max = Long.MIN_VALUE;
        boolean hasValidValue = false;
        
        if (v1 != Long.MIN_VALUE) {
            max = v1;
            hasValidValue = true;
        }
        if (v2 != Long.MIN_VALUE) {
            max = hasValidValue ? Math.max(max, v2) : v2;
            hasValidValue = true;
        }
        if (v3 != Long.MIN_VALUE) {
            max = hasValidValue ? Math.max(max, v3) : v3;
            hasValidValue = true;
        }
        
        return hasValidValue ? max : Long.MIN_VALUE;
    }

    public static void main(String[] args) throws RunnerException {
        Options opt = new OptionsBuilder()
                .include(VarArgsFunctionOptimizationBenchmark.class.getSimpleName())
                .build();
        new Runner(opt).run();
    }
}
