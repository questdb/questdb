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
 ******************************************************************************/

package org.questdb;

import io.questdb.cairo.ColumnType;
import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.Record;
import io.questdb.griffin.engine.functions.DoubleFunction;
import io.questdb.griffin.engine.functions.UnaryFunction;
import io.questdb.griffin.udf.ScalarUDF;
import io.questdb.griffin.udf.UDFType;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Level;
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

import java.util.concurrent.TimeUnit;

/**
 * Benchmark comparing performance of traditional Griffin functions vs simplified UDF API.
 *
 * This measures the overhead introduced by the UDF wrapper:
 * - Boxing/unboxing of primitive types (Double vs double)
 * - Extra method call indirection through the UDF interface
 * - Null checks in extractInput
 */
@State(Scope.Thread)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
public class UDFOverheadBenchmark {

    @Param({"1000", "10000", "100000"})
    public int rowCount;

    // Test data - array of random doubles
    private double[] testData;
    private int currentRow;

    // Traditional Griffin function - direct implementation
    private TraditionalSquareFunction traditionalFunction;

    // UDF-wrapped function - uses simplified API pattern
    private UDFSquareFunction udfFunction;

    // Mock record that returns values from testData array
    private MockRecord mockRecord;

    // Mock argument function that reads from record
    private MockDoubleArgFunction argFunction;

    public static void main(String[] args) throws RunnerException {
        Options opt = new OptionsBuilder()
                .include(UDFOverheadBenchmark.class.getSimpleName())
                .forks(0)  // Run in same JVM to avoid classpath issues
                .warmupIterations(3)
                .measurementIterations(5)
                .build();
        new Runner(opt).run();
    }

    @Setup(Level.Iteration)
    public void setup() {
        // Generate test data
        testData = new double[rowCount];
        java.util.Random rnd = new java.util.Random(42);
        for (int i = 0; i < rowCount; i++) {
            testData[i] = rnd.nextDouble() * 100;
        }
        currentRow = 0;

        // Setup mock record and arg function
        mockRecord = new MockRecord();
        argFunction = new MockDoubleArgFunction();

        // Setup functions
        traditionalFunction = new TraditionalSquareFunction(argFunction);
        udfFunction = new UDFSquareFunction(argFunction, x -> x * x);
    }

    @Benchmark
    public double testTraditionalGriffinFunction() {
        double sum = 0;
        for (currentRow = 0; currentRow < rowCount; currentRow++) {
            sum += traditionalFunction.getDouble(mockRecord);
        }
        return sum;
    }

    @Benchmark
    public double testUDFWrappedFunction() {
        double sum = 0;
        for (currentRow = 0; currentRow < rowCount; currentRow++) {
            sum += udfFunction.getDouble(mockRecord);
        }
        return sum;
    }

    @Benchmark
    public double testDirectComputation() {
        // Baseline: direct computation without any function abstraction
        double sum = 0;
        for (int i = 0; i < rowCount; i++) {
            double x = testData[i];
            sum += x * x;
        }
        return sum;
    }

    // ==========================================
    // Traditional Griffin Function Implementation
    // ==========================================

    /**
     * Traditional implementation - extends DoubleFunction directly.
     * This is the pattern used by built-in QuestDB functions.
     */
    private static class TraditionalSquareFunction extends DoubleFunction implements UnaryFunction {
        private final Function arg;

        TraditionalSquareFunction(Function arg) {
            this.arg = arg;
        }

        @Override
        public Function getArg() {
            return arg;
        }

        @Override
        public double getDouble(Record rec) {
            double x = arg.getDouble(rec);
            return x * x;
        }

        @Override
        public String getName() {
            return "traditional_square";
        }
    }

    // ==========================================
    // UDF-Wrapped Function Implementation
    // ==========================================

    /**
     * UDF-wrapped implementation - mimics the ScalarUDFFactory wrapper.
     * This includes the overhead of boxing/unboxing and null checks.
     */
    private class UDFSquareFunction extends DoubleFunction implements UnaryFunction {
        private final Function arg;
        private final ScalarUDF<Double, Double> udf;

        UDFSquareFunction(Function arg, ScalarUDF<Double, Double> udf) {
            this.arg = arg;
            this.udf = udf;
        }

        @Override
        public Function getArg() {
            return arg;
        }

        @Override
        public double getDouble(Record rec) {
            // This mimics the extractInput + compute pattern from ScalarUDFFactory
            Double input = extractInput(arg, rec);
            Double result = udf.compute(input);
            return result == null ? Double.NaN : result;
        }

        @SuppressWarnings("unchecked")
        private Double extractInput(Function arg, Record rec) {
            // Mimics ScalarUDFFactory.extractInput for Double type
            double v = arg.getDouble(rec);
            return Double.isNaN(v) ? null : v;
        }

        @Override
        public String getName() {
            return "udf_square";
        }
    }

    // ==========================================
    // Mock Classes for Testing
    // ==========================================

    /**
     * Mock record that provides access to test data.
     */
    private class MockRecord implements Record {
        @Override
        public double getDouble(int col) {
            return testData[currentRow];
        }

        // Implement other Record methods as no-ops for benchmark purposes
        @Override public boolean getBool(int col) { return false; }
        @Override public byte getByte(int col) { return 0; }
        @Override public char getChar(int col) { return 0; }
        @Override public short getShort(int col) { return 0; }
        @Override public int getInt(int col) { return 0; }
        @Override public long getLong(int col) { return 0; }
        @Override public float getFloat(int col) { return 0; }
        @Override public CharSequence getStrA(int col) { return null; }
        @Override public CharSequence getStrB(int col) { return null; }
        @Override public int getStrLen(int col) { return 0; }
    }

    /**
     * Mock function that reads a double from record column 0.
     */
    private static class MockDoubleArgFunction extends DoubleFunction {
        @Override
        public double getDouble(Record rec) {
            return rec.getDouble(0);
        }

        @Override
        public String getName() {
            return "mock_arg";
        }
    }
}
