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
 * Simplified interface for binary (two-argument) scalar user-defined functions.
 * <p>
 * Example usage:
 * <pre>{@code
 * // Add two numbers
 * BinaryScalarUDF<Double, Double, Double> add = (a, b) -> a + b;
 *
 * // Concatenate with separator
 * BinaryScalarUDF<String, String, String> concat = (a, b) -> a + ", " + b;
 * }</pre>
 *
 * @param <I1> First input type
 * @param <I2> Second input type
 * @param <O>  Output type
 */
@FunctionalInterface
public interface BinaryScalarUDF<I1, I2, O> {

    /**
     * Compute the function result for the given inputs.
     *
     * @param input1 the first input value (may be null)
     * @param input2 the second input value (may be null)
     * @return the computed result (may be null)
     */
    O compute(I1 input1, I2 input2);
}
