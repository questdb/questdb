/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Example QuestDB Plugin - Simplified UDF API Examples
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 ******************************************************************************/

package io.questdb.plugin.example;

import io.questdb.cairo.CairoConfiguration;
import io.questdb.griffin.FunctionFactory;
import io.questdb.griffin.udf.AggregateUDF;
import io.questdb.griffin.udf.Date;
import io.questdb.griffin.udf.PluginFunctions;
import io.questdb.griffin.udf.PluginLifecycle;
import io.questdb.griffin.udf.Timestamp;
import io.questdb.griffin.udf.UDFRegistry;
import io.questdb.std.ObjList;

import java.time.ZoneOffset;

/**
 * Examples of using the simplified UDF API.
 * <p>
 * This demonstrates how easy it is to create QuestDB functions using the
 * simplified API compared to the full Griffin FunctionFactory interface.
 * <p>
 * <h2>Comparison - Old API vs New API</h2>
 * <p>
 * <b>Old API (28 lines for abs function):</b>
 * <pre>{@code
 * public class AbsIntFunctionFactory implements FunctionFactory {
 *     @Override
 *     public String getSignature() { return "abs(I)"; }
 *
 *     @Override
 *     public Function newInstance(int position, ObjList<Function> args,
 *             IntList argPositions, CairoConfiguration configuration,
 *             SqlExecutionContext sqlExecutionContext) {
 *         return new AbsIntFunction(args.getQuick(0));
 *     }
 *
 *     private static class AbsIntFunction extends IntFunction implements UnaryFunction {
 *         private final Function arg;
 *         public AbsIntFunction(Function arg) { this.arg = arg; }
 *         @Override public Function getArg() { return arg; }
 *         @Override public int getInt(Record rec) { return Math.abs(arg.getInt(rec)); }
 *         @Override public String getName() { return "abs"; }
 *     }
 * }
 * }</pre>
 * <p>
 * <b>New API (1 line!):</b>
 * <pre>{@code
 * FunctionFactory abs = UDFRegistry.scalar("abs", Integer.class, Integer.class, Math::abs);
 * }</pre>
 */
@PluginFunctions(
        description = "Simplified UDF examples: math, string, and aggregate functions",
        version = "1.0.0",
        author = "QuestDB",
        license = "Apache-2.0"
)
public class SimpleUDFExamples implements PluginLifecycle {

    // Track whether onLoad was called (for demonstration)
    private static boolean initialized = false;

    @Override
    public void onLoad(CairoConfiguration configuration) {
        initialized = true;
        // Example: could initialize external resources, connections, etc.
        System.out.println("SimpleUDFExamples plugin loaded");
    }

    @Override
    public void onUnload() {
        initialized = false;
        // Example: could clean up resources, close connections, etc.
        System.out.println("SimpleUDFExamples plugin unloaded");
    }

    /**
     * Get all example functions using the simplified API.
     *
     * @return list of function factories
     */
    public static ObjList<FunctionFactory> getFunctions() {
        return UDFRegistry.functions(
                // ============================================
                // SCALAR FUNCTIONS (one-liners!)
                // ============================================

                // Math: square a number
                UDFRegistry.scalar("simple_square", Double.class, Double.class,
                        x -> x * x),

                // Math: cube a number
                UDFRegistry.scalar("simple_cube", Double.class, Double.class,
                        x -> x * x * x),

                // Math: absolute value
                UDFRegistry.scalar("simple_abs", Double.class, Double.class,
                        Math::abs),

                // Math: ceiling
                UDFRegistry.scalar("simple_ceil", Double.class, Double.class,
                        Math::ceil),

                // Math: floor
                UDFRegistry.scalar("simple_floor", Double.class, Double.class,
                        Math::floor),

                // Math: natural log
                UDFRegistry.scalar("simple_ln", Double.class, Double.class,
                        Math::log),

                // Math: square root
                UDFRegistry.scalar("simple_sqrt", Double.class, Double.class,
                        Math::sqrt),

                // String: reverse
                UDFRegistry.scalar("simple_reverse", String.class, String.class,
                        s -> s == null ? null : new StringBuilder(s).reverse().toString()),

                // String: to uppercase
                UDFRegistry.scalar("simple_upper", String.class, String.class,
                        s -> s == null ? null : s.toUpperCase()),

                // String: to lowercase
                UDFRegistry.scalar("simple_lower", String.class, String.class,
                        s -> s == null ? null : s.toLowerCase()),

                // String: length as integer
                UDFRegistry.scalar("simple_len", String.class, Integer.class,
                        s -> s == null ? null : s.length()),

                // String: trim whitespace
                UDFRegistry.scalar("simple_trim", String.class, String.class,
                        s -> s == null ? null : s.trim()),

                // Boolean: negate
                UDFRegistry.scalar("simple_not", Boolean.class, Boolean.class,
                        b -> b == null ? null : !b),

                // ============================================
                // TIMESTAMP FUNCTIONS
                // ============================================

                // Extract hour from timestamp (0-23)
                UDFRegistry.scalar("simple_hour", Timestamp.class, Integer.class,
                        ts -> ts == null ? null : ts.toInstant().atZone(ZoneOffset.UTC).getHour()),

                // Extract day of month from timestamp (1-31)
                UDFRegistry.scalar("simple_day", Timestamp.class, Integer.class,
                        ts -> ts == null ? null : ts.toInstant().atZone(ZoneOffset.UTC).getDayOfMonth()),

                // Extract year from timestamp
                UDFRegistry.scalar("simple_year", Timestamp.class, Integer.class,
                        ts -> ts == null ? null : ts.toInstant().atZone(ZoneOffset.UTC).getYear()),

                // Add days to timestamp
                UDFRegistry.binary("simple_add_days", Timestamp.class, Integer.class, Timestamp.class,
                        (ts, days) -> {
                            if (ts == null || days == null) return null;
                            return new Timestamp(ts.getMicros() + days * 24L * 60L * 60L * 1_000_000L);
                        }),

                // ============================================
                // DATE FUNCTIONS
                // ============================================

                // Extract year from date
                UDFRegistry.scalar("simple_date_year", Date.class, Integer.class,
                        d -> d == null ? null : d.toLocalDate().getYear()),

                // Extract month from date (1-12)
                UDFRegistry.scalar("simple_date_month", Date.class, Integer.class,
                        d -> d == null ? null : d.toLocalDate().getMonthValue()),

                // Extract day of month from date (1-31)
                UDFRegistry.scalar("simple_date_day", Date.class, Integer.class,
                        d -> d == null ? null : d.toLocalDate().getDayOfMonth()),

                // Add months to date
                UDFRegistry.binary("simple_date_add_months", Date.class, Integer.class, Date.class,
                        (d, months) -> {
                            if (d == null || months == null) return null;
                            return Date.fromLocalDate(d.toLocalDate().plusMonths(months));
                        }),

                // ============================================
                // BINARY FUNCTIONS (two arguments)
                // ============================================

                // Math: power (base^exponent)
                UDFRegistry.binary("simple_power", Double.class, Double.class, Double.class,
                        (base, exp) -> Math.pow(base, exp)),

                // Math: modulo
                UDFRegistry.binary("simple_mod", Long.class, Long.class, Long.class,
                        (a, b) -> b == 0 ? null : a % b),

                // String: concatenate with separator
                UDFRegistry.binary("simple_concat", String.class, String.class, String.class,
                        (a, b) -> (a == null ? "" : a) + (b == null ? "" : b)),

                // String: left pad
                UDFRegistry.binary("simple_lpad", String.class, Integer.class, String.class,
                        (s, len) -> {
                            if (s == null || len == null) return null;
                            if (s.length() >= len) return s;
                            return " ".repeat(len - s.length()) + s;
                        }),

                // ============================================
                // AGGREGATE FUNCTIONS (GROUP BY)
                // ============================================

                // Sum - simple aggregate
                UDFRegistry.aggregate("simple_sum", Double.class, Double.class,
                        SumAggregate::new),

                // Count non-null - simple aggregate
                UDFRegistry.aggregate("simple_count_notnull", Double.class, Long.class,
                        CountNotNullAggregate::new),

                // Average - stateful aggregate
                UDFRegistry.aggregate("simple_avg", Double.class, Double.class,
                        AvgAggregate::new),

                // Min - simple aggregate
                UDFRegistry.aggregate("simple_min", Double.class, Double.class,
                        MinAggregate::new),

                // Max - simple aggregate
                UDFRegistry.aggregate("simple_max", Double.class, Double.class,
                        MaxAggregate::new),

                // ============================================
                // VARIADIC FUNCTIONS (N arguments)
                // ============================================

                // Return maximum of N values
                UDFRegistry.varargs("simple_max_of", Double.class, Double.class,
                        args -> args.stream()
                                .filter(d -> d != null)
                                .max(Double::compare)
                                .orElse(null)),

                // Return minimum of N values
                UDFRegistry.varargs("simple_min_of", Double.class, Double.class,
                        args -> args.stream()
                                .filter(d -> d != null)
                                .min(Double::compare)
                                .orElse(null)),

                // Concatenate N strings
                UDFRegistry.varargs("simple_concat_all", String.class, String.class,
                        args -> {
                            StringBuilder sb = new StringBuilder();
                            for (String s : args) {
                                if (s != null) {
                                    sb.append(s);
                                }
                            }
                            return sb.length() > 0 ? sb.toString() : null;
                        }),

                // Return first non-null value (like coalesce)
                UDFRegistry.varargs("simple_coalesce", Double.class, Double.class,
                        args -> args.stream()
                                .filter(d -> d != null)
                                .findFirst()
                                .orElse(null)),

                // ============================================
                // ERROR HANDLING TEST FUNCTIONS
                // ============================================

                // Function that throws on negative input (for testing error handling)
                UDFRegistry.scalar("simple_throw_on_negative", Double.class, Double.class,
                        x -> {
                            if (x != null && x < 0) {
                                throw new IllegalArgumentException("Negative value not allowed: " + x);
                            }
                            return x;
                        }),

                // Function that throws on null (for testing error handling)
                UDFRegistry.scalar("simple_require_nonnull", String.class, String.class,
                        s -> {
                            if (s == null) {
                                throw new NullPointerException("Value must not be null");
                            }
                            return s.toUpperCase();
                        })
        );
    }

    // ============================================
    // AGGREGATE IMPLEMENTATIONS
    // ============================================

    /**
     * Simple sum aggregate.
     */
    public static class SumAggregate implements AggregateUDF<Double, Double> {
        private double sum = 0;

        @Override
        public void accumulate(Double value) {
            if (value != null) {
                sum += value;
            }
        }

        @Override
        public Double result() {
            return sum;
        }

        @Override
        public void reset() {
            sum = 0;
        }
    }

    /**
     * Count non-null values.
     */
    public static class CountNotNullAggregate implements AggregateUDF<Double, Long> {
        private long count = 0;

        @Override
        public void accumulate(Double value) {
            if (value != null) {
                count++;
            }
        }

        @Override
        public Long result() {
            return count;
        }

        @Override
        public void reset() {
            count = 0;
        }
    }

    /**
     * Average aggregate with running sum and count.
     */
    public static class AvgAggregate implements AggregateUDF<Double, Double> {
        private double sum = 0;
        private long count = 0;

        @Override
        public void accumulate(Double value) {
            if (value != null) {
                sum += value;
                count++;
            }
        }

        @Override
        public Double result() {
            return count == 0 ? Double.NaN : sum / count;
        }

        @Override
        public void reset() {
            sum = 0;
            count = 0;
        }
    }

    /**
     * Minimum aggregate.
     */
    public static class MinAggregate implements AggregateUDF<Double, Double> {
        private double min = Double.MAX_VALUE;
        private boolean hasValue = false;

        @Override
        public void accumulate(Double value) {
            if (value != null) {
                if (!hasValue || value < min) {
                    min = value;
                    hasValue = true;
                }
            }
        }

        @Override
        public Double result() {
            return hasValue ? min : Double.NaN;
        }

        @Override
        public void reset() {
            min = Double.MAX_VALUE;
            hasValue = false;
        }
    }

    /**
     * Maximum aggregate.
     */
    public static class MaxAggregate implements AggregateUDF<Double, Double> {
        private double max = Double.MIN_VALUE;
        private boolean hasValue = false;

        @Override
        public void accumulate(Double value) {
            if (value != null) {
                if (!hasValue || value > max) {
                    max = value;
                    hasValue = true;
                }
            }
        }

        @Override
        public Double result() {
            return hasValue ? max : Double.NaN;
        }

        @Override
        public void reset() {
            max = Double.MIN_VALUE;
            hasValue = false;
        }
    }
}
