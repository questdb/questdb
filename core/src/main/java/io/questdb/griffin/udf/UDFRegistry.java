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

package io.questdb.griffin.udf;

import io.questdb.griffin.FunctionFactory;
import io.questdb.std.ObjList;

import java.util.function.Supplier;

/**
 * Registry for creating simplified UDF function factories.
 * <p>
 * This class provides a fluent API for creating QuestDB plugin functions
 * with minimal boilerplate. Instead of implementing the full
 * {@link FunctionFactory} interface, you can use simple lambdas.
 * <p>
 * <h2>Scalar Functions (row-by-row)</h2>
 * <pre>{@code
 * // Simple lambda - square a number
 * FunctionFactory square = UDFRegistry.scalar("my_square", Double.class, Double.class,
 *     x -> x * x);
 *
 * // String manipulation
 * FunctionFactory upper = UDFRegistry.scalar("my_upper", String.class, String.class,
 *     s -> s == null ? null : s.toUpperCase());
 *
 * // Binary function - power
 * FunctionFactory power = UDFRegistry.binary("my_power",
 *     Double.class, Double.class, Double.class,
 *     (base, exp) -> Math.pow(base, exp));
 * }</pre>
 * <p>
 * <h2>Aggregate Functions (GROUP BY)</h2>
 * <pre>{@code
 * // Sum aggregate
 * FunctionFactory sum = UDFRegistry.aggregate("my_sum", Double.class, Double.class,
 *     () -> new AggregateUDF<Double, Double>() {
 *         private double sum = 0;
 *
 *         public void accumulate(Double value) {
 *             if (value != null) sum += value;
 *         }
 *
 *         public Double result() { return sum; }
 *
 *         public void reset() { sum = 0; }
 *     });
 * }</pre>
 * <p>
 * <h2>Plugin Usage</h2>
 * In your plugin, expose the function factories:
 * <pre>{@code
 * public class MyPlugin {
 *     public static List<FunctionFactory> getFunctions() {
 *         return Arrays.asList(
 *             UDFRegistry.scalar("my_square", Double.class, Double.class, x -> x * x),
 *             UDFRegistry.scalar("my_reverse", String.class, String.class,
 *                 s -> new StringBuilder(s).reverse().toString())
 *         );
 *     }
 * }
 * }</pre>
 */
public final class UDFRegistry {

    private UDFRegistry() {
        // utility class
    }

    /**
     * Create a scalar (row-by-row) function factory with a single input.
     * <p>
     * Example:
     * <pre>{@code
     * FunctionFactory abs = UDFRegistry.scalar("my_abs", Double.class, Double.class,
     *     x -> Math.abs(x));
     * }</pre>
     *
     * @param name       function name (used in SQL)
     * @param inputType  input Java type (Double, Long, Integer, String, Boolean)
     * @param outputType output Java type
     * @param udf        the function implementation
     * @param <I>        input type
     * @param <O>        output type
     * @return a FunctionFactory that can be registered with QuestDB
     */
    public static <I, O> FunctionFactory scalar(
            String name,
            Class<I> inputType,
            Class<O> outputType,
            ScalarUDF<I, O> udf
    ) {
        return new ScalarUDFFactory<>(name, inputType, outputType, udf);
    }

    /**
     * Create a binary (two-input) scalar function factory.
     * <p>
     * Example:
     * <pre>{@code
     * FunctionFactory power = UDFRegistry.binary("my_power",
     *     Double.class, Double.class, Double.class,
     *     (base, exp) -> Math.pow(base, exp));
     * }</pre>
     *
     * @param name        function name
     * @param inputType1  first input Java type
     * @param inputType2  second input Java type
     * @param outputType  output Java type
     * @param udf         the function implementation
     * @param <I1>        first input type
     * @param <I2>        second input type
     * @param <O>         output type
     * @return a FunctionFactory that can be registered with QuestDB
     */
    public static <I1, I2, O> FunctionFactory binary(
            String name,
            Class<I1> inputType1,
            Class<I2> inputType2,
            Class<O> outputType,
            BinaryScalarUDF<I1, I2, O> udf
    ) {
        return new BinaryScalarUDFFactory<>(name, inputType1, inputType2, outputType, udf);
    }

    /**
     * Create an aggregate (GROUP BY) function factory.
     * <p>
     * The supplier is called to create a fresh aggregate instance for each group.
     * <p>
     * Example:
     * <pre>{@code
     * FunctionFactory sum = UDFRegistry.aggregate("my_sum", Double.class, Double.class,
     *     () -> new AggregateUDF<Double, Double>() {
     *         private double sum = 0;
     *         public void accumulate(Double v) { if (v != null) sum += v; }
     *         public Double result() { return sum; }
     *         public void reset() { sum = 0; }
     *     });
     * }</pre>
     *
     * @param name       function name
     * @param inputType  input Java type
     * @param outputType output Java type
     * @param supplier   supplies new aggregate instances
     * @param <I>        input type
     * @param <O>        output type
     * @return a FunctionFactory that can be registered with QuestDB
     */
    public static <I, O> FunctionFactory aggregate(
            String name,
            Class<I> inputType,
            Class<O> outputType,
            Supplier<AggregateUDF<I, O>> supplier
    ) {
        return new AggregateUDFFactory<>(name, inputType, outputType, supplier);
    }

    /**
     * Create a variadic (N-ary) scalar function factory that accepts any number of arguments.
     * <p>
     * All arguments must be of the same type. The function receives a list of all values.
     * <p>
     * Example:
     * <pre>{@code
     * // Return the maximum of any number of values
     * FunctionFactory maxOf = UDFRegistry.varargs("my_max", Double.class, Double.class,
     *     args -> args.stream().filter(Objects::nonNull).max(Double::compare).orElse(null));
     *
     * // Concatenate any number of strings
     * FunctionFactory concatAll = UDFRegistry.varargs("concat_all", String.class, String.class,
     *     args -> args.stream().filter(Objects::nonNull).collect(Collectors.joining()));
     * }</pre>
     *
     * @param name       function name (used in SQL)
     * @param inputType  input Java type for all arguments
     * @param outputType output Java type
     * @param udf        the function implementation
     * @param <I>        input element type
     * @param <O>        output type
     * @return a FunctionFactory that can be registered with QuestDB
     */
    public static <I, O> FunctionFactory varargs(
            String name,
            Class<I> inputType,
            Class<O> outputType,
            VarargsScalarUDF<I, O> udf
    ) {
        return new VarargsScalarUDFFactory<>(name, inputType, outputType, udf);
    }

    /**
     * Collect multiple function factories into a list.
     * <p>
     * Useful for plugin implementations:
     * <pre>{@code
     * public static ObjList<FunctionFactory> getPluginFunctions() {
     *     return UDFRegistry.functions(
     *         UDFRegistry.scalar("f1", Double.class, Double.class, x -> x * 2),
     *         UDFRegistry.scalar("f2", String.class, String.class, String::toUpperCase)
     *     );
     * }
     * }</pre>
     *
     * @param factories the function factories
     * @return an ObjList containing all factories
     */
    public static ObjList<FunctionFactory> functions(FunctionFactory... factories) {
        ObjList<FunctionFactory> list = new ObjList<>(factories.length);
        for (FunctionFactory factory : factories) {
            list.add(factory);
        }
        return list;
    }
}
