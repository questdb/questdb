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

/**
 * Simplified interface for scalar (row-by-row) user-defined functions.
 * <p>
 * Scalar functions take input values and produce a single output value per row.
 * This interface provides a much simpler API compared to the full Griffin
 * {@link io.questdb.griffin.FunctionFactory} interface.
 * <p>
 * Example usage with lambda:
 * <pre>{@code
 * // Square a number
 * ScalarUDF<Double, Double> square = x -> x * x;
 *
 * // Reverse a string
 * ScalarUDF<String, String> reverse = s -> new StringBuilder(s).reverse().toString();
 * }</pre>
 * <p>
 * Register using {@link UDFRegistry}:
 * <pre>{@code
 * UDFRegistry.scalar("my_square", Double.class, Double.class, x -> x * x);
 * }</pre>
 *
 * @param <I> Input type (Double, Long, Integer, String, Boolean)
 * @param <O> Output type (Double, Long, Integer, String, Boolean)
 */
@FunctionalInterface
public interface ScalarUDF<I, O> {

    /**
     * Compute the function result for the given input.
     *
     * @param input the input value (may be null)
     * @return the computed result (may be null)
     */
    O compute(I input);
}
