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

package io.questdb.test.std;

import io.questdb.std.Decimal128;
import io.questdb.std.MutableDecimal128;
import org.junit.Test;

/**
 * Performance benchmarks comparing object allocation vs sink-based approaches
 * for Decimal128 operations.
 * 
 * Run with: mvn test -Dtest=Decimal128PerformanceTest
 */
public class Decimal128PerformanceTest {

    @Test
    public void benchmarkAdditionPerformance() {
        System.out.println("\n=== Addition Performance Benchmark ===");
        
        final int warmupIterations = 10000;
        final int testIterations = 100000;
        
        // Warmup
        runAdditionWithAllocation(warmupIterations);
        runAdditionWithSink(warmupIterations);
        
        // Force GC before measurement
        System.gc();
        System.runFinalization();
        
        // Measure with allocation
        long allocStartTime = System.nanoTime();
        long allocStartMem = getUsedMemory();
        
        Decimal128 allocResult = runAdditionWithAllocation(testIterations);
        
        long allocEndTime = System.nanoTime();
        long allocEndMem = getUsedMemory();
        
        // Force GC between tests
        System.gc();
        System.runFinalization();
        
        // Measure with sink
        long sinkStartTime = System.nanoTime();
        long sinkStartMem = getUsedMemory();
        
        Decimal128 sinkResult = runAdditionWithSink(testIterations);
        
        long sinkEndTime = System.nanoTime();
        long sinkEndMem = getUsedMemory();
        
        // Calculate results
        long allocTime = allocEndTime - allocStartTime;
        long sinkTime = sinkEndTime - sinkStartTime;
        long allocMemUsed = Math.max(0, allocEndMem - allocStartMem);
        long sinkMemUsed = Math.max(0, sinkEndMem - sinkStartMem);
        
        // Verify both approaches produce same result
        assert allocResult.equals(sinkResult) : "Results don't match!";
        
        // Print results
        System.out.println("Iterations: " + testIterations);
        System.out.println("\nObject Allocation Approach:");
        System.out.println("  Time: " + (allocTime / 1_000_000) + " ms");
        System.out.println("  Memory allocated: " + (allocMemUsed / 1024) + " KB");
        System.out.println("  Time per operation: " + (allocTime / testIterations) + " ns");
        
        System.out.println("\nSink-based Approach:");
        System.out.println("  Time: " + (sinkTime / 1_000_000) + " ms");
        System.out.println("  Memory allocated: " + (sinkMemUsed / 1024) + " KB");
        System.out.println("  Time per operation: " + (sinkTime / testIterations) + " ns");
        
        System.out.println("\nImprovement:");
        System.out.println("  Time reduction: " + String.format("%.1f%%", 100.0 * (allocTime - sinkTime) / allocTime));
        System.out.println("  Memory reduction: " + String.format("%.1f%%", 100.0 * (allocMemUsed - sinkMemUsed) / Math.max(1, allocMemUsed)));
    }

    @Test
    public void benchmarkComplexCalculation() {
        System.out.println("\n=== Complex Calculation Performance Benchmark ===");
        
        final int iterations = 10000;
        
        // Warmup
        runComplexCalculationWithAllocation(100);
        runComplexCalculationWithSink(100);
        
        // Force GC
        System.gc();
        System.runFinalization();
        
        // Measure with allocation
        long allocStartTime = System.nanoTime();
        long allocStartMem = getUsedMemory();
        
        Decimal128 allocResult = runComplexCalculationWithAllocation(iterations);
        
        long allocEndTime = System.nanoTime();
        long allocEndMem = getUsedMemory();
        
        // Force GC
        System.gc();
        System.runFinalization();
        
        // Measure with sink
        long sinkStartTime = System.nanoTime();
        long sinkStartMem = getUsedMemory();
        
        Decimal128 sinkResult = runComplexCalculationWithSink(iterations);
        
        long sinkEndTime = System.nanoTime();
        long sinkEndMem = getUsedMemory();
        
        // Calculate results
        long allocTime = allocEndTime - allocStartTime;
        long sinkTime = sinkEndTime - sinkStartTime;
        long allocMemUsed = Math.max(0, allocEndMem - allocStartMem);
        long sinkMemUsed = Math.max(0, sinkEndMem - sinkStartMem);
        
        // Print results
        System.out.println("Iterations: " + iterations);
        System.out.println("\nObject Allocation Approach:");
        System.out.println("  Time: " + (allocTime / 1_000_000) + " ms");
        System.out.println("  Memory allocated: " + (allocMemUsed / 1024) + " KB");
        
        System.out.println("\nSink-based Approach:");
        System.out.println("  Time: " + (sinkTime / 1_000_000) + " ms");
        System.out.println("  Memory allocated: " + (sinkMemUsed / 1024) + " KB");
        
        System.out.println("\nImprovement:");
        System.out.println("  Time reduction: " + String.format("%.1f%%", 100.0 * (allocTime - sinkTime) / allocTime));
        System.out.println("  Memory reduction: " + String.format("%.1f%%", 100.0 * (allocMemUsed - sinkMemUsed) / Math.max(1, allocMemUsed)));
    }

    private Decimal128 runAdditionWithAllocation(int iterations) {
        Decimal128 sum = Decimal128.fromLong(0, 2);
        Decimal128 increment = Decimal128.fromLong(1, 2);
        
        for (int i = 0; i < iterations; i++) {
            sum = sum.add(increment);
        }
        
        return sum;
    }

    private Decimal128 runAdditionWithSink(int iterations) {
        MutableDecimal128 sum = new MutableDecimal128();
        sum.setFromLong(0, 2);
        Decimal128 increment = Decimal128.fromLong(1, 2);
        
        for (int i = 0; i < iterations; i++) {
            // Truly allocation-free - no toDecimal128() call
            sum.addDecimal128(increment);
        }
        
        return sum.toDecimal128();
    }

    private Decimal128 runComplexCalculationWithAllocation(int iterations) {
        // Simulate a financial calculation: compound interest
        // A = P(1 + r/n)^(nt)
        Decimal128 principal = Decimal128.fromDouble(1000.00, 2);
        Decimal128 rate = Decimal128.fromDouble(0.05, 4);  // 5% annual rate
        Decimal128 n = Decimal128.fromDouble(12.0, 1);     // Monthly compounding
        
        Decimal128 amount = principal;
        
        for (int i = 0; i < iterations; i++) {
            // amount = amount * (1 + rate/n)
            Decimal128 ratePerPeriod = rate.divide(n, 6);
            Decimal128 onePlusRate = Decimal128.fromDouble(1.0, 6).add(ratePerPeriod);
            amount = amount.multiply(onePlusRate);
        }
        
        return amount;
    }

    private Decimal128 runComplexCalculationWithSink(int iterations) {
        // Same calculation but using sinks
        Decimal128 principal = Decimal128.fromDouble(1000.00, 2);
        Decimal128 rate = Decimal128.fromDouble(0.05, 4);
        Decimal128 n = Decimal128.fromDouble(12.0, 1);
        
        MutableDecimal128 amount = new MutableDecimal128();
        amount.copyFrom(principal);
        
        MutableDecimal128 tempSink1 = new MutableDecimal128();
        MutableDecimal128 tempSink2 = new MutableDecimal128();
        
        // Pre-calculate rate/n to avoid allocations in loop
        rate.divideTo(n, 6, tempSink1);
        Decimal128 ratePerPeriod = tempSink1.toDecimal128();
        
        // Pre-calculate 1 + rate/n
        Decimal128 one = Decimal128.fromDouble(1.0, 6);
        one.addTo(ratePerPeriod, tempSink1);
        Decimal128 onePlusRate = tempSink1.toDecimal128();
        
        for (int i = 0; i < iterations; i++) {
            // Multiply amount by (1 + rate/n) using truly allocation-free operation
            amount.multiplyDecimal128(onePlusRate);
        }
        
        return amount.toDecimal128();
    }

    private long getUsedMemory() {
        Runtime runtime = Runtime.getRuntime();
        return runtime.totalMemory() - runtime.freeMemory();
    }
}