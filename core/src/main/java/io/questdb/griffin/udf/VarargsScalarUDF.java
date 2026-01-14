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

import java.util.List;

/**
 * A simplified functional interface for variadic (N-ary) scalar UDFs.
 * <p>
 * This interface allows creating SQL functions that accept a variable number
 * of arguments of the same type.
 * <p>
 * Example usage:
 * <pre>{@code
 * // Create a function that returns the maximum of any number of values
 * FunctionFactory maxOf = UDFRegistry.varargs("my_max", Double.class, Double.class,
 *     args -> args.stream().filter(Objects::nonNull).max(Double::compare).orElse(null)
 * );
 *
 * // Create a function that concatenates any number of strings
 * FunctionFactory concatAll = UDFRegistry.varargs("concat_all", String.class, String.class,
 *     args -> args.stream().filter(Objects::nonNull).collect(Collectors.joining())
 * );
 * }</pre>
 *
 * @param <I> the input element type (all arguments must be of this type)
 * @param <O> the output type
 * @see UDFRegistry#varargs(String, Class, Class, VarargsScalarUDF)
 */
@FunctionalInterface
public interface VarargsScalarUDF<I, O> {
    /**
     * Compute the result from the given list of input values.
     *
     * @param inputs list of input values, may contain nulls
     * @return the computed result, or null if the result should be null
     */
    O compute(List<I> inputs);
}
