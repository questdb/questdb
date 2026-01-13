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
 * Simplified interface for aggregate (GROUP BY) user-defined functions.
 * <p>
 * Aggregate functions accumulate values across multiple rows to produce
 * a single result per group. Unlike the complex Griffin GroupByFunction,
 * this interface handles state management automatically.
 * <p>
 * Example - Sum function:
 * <pre>{@code
 * public class SumUDF implements AggregateUDF<Double, Double> {
 *     private double sum = 0;
 *
 *     @Override
 *     public void accumulate(Double value) {
 *         if (value != null) {
 *             sum += value;
 *         }
 *     }
 *
 *     @Override
 *     public Double result() {
 *         return sum;
 *     }
 *
 *     @Override
 *     public void reset() {
 *         sum = 0;
 *     }
 * }
 * }</pre>
 * <p>
 * Example - Kahan summation (compensated):
 * <pre>{@code
 * public class KahanSumUDF implements AggregateUDF<Double, Double> {
 *     private double sum = 0;
 *     private double compensation = 0;
 *
 *     @Override
 *     public void accumulate(Double value) {
 *         if (value != null) {
 *             double y = value - compensation;
 *             double t = sum + y;
 *             compensation = (t - sum) - y;
 *             sum = t;
 *         }
 *     }
 *
 *     @Override
 *     public Double result() {
 *         return sum;
 *     }
 *
 *     @Override
 *     public void reset() {
 *         sum = 0;
 *         compensation = 0;
 *     }
 * }
 * }</pre>
 *
 * @param <I> Input type per row (Double, Long, Integer, String, Boolean)
 * @param <O> Output/result type (Double, Long, Integer, String, Boolean)
 */
public interface AggregateUDF<I, O> {

    /**
     * Accumulate a value from a row into the aggregate state.
     * Called once per row in the group.
     *
     * @param value the value to accumulate (may be null)
     */
    void accumulate(I value);

    /**
     * Get the final result after all rows have been accumulated.
     *
     * @return the aggregate result
     */
    O result();

    /**
     * Reset the aggregate state for a new group.
     * Called before processing each new group.
     */
    void reset();

    /**
     * Optional: Merge another aggregate's state into this one.
     * Used for parallel execution. Default implementation throws
     * UnsupportedOperationException (disabling parallelism).
     *
     * @param other the other aggregate to merge
     */
    default void merge(AggregateUDF<I, O> other) {
        throw new UnsupportedOperationException("Parallel execution not supported");
    }

    /**
     * Returns true if this aggregate supports parallel execution via merge().
     *
     * @return true if merge() is implemented
     */
    default boolean supportsParallelism() {
        return false;
    }

    /**
     * Create a new instance for parallel execution.
     * Each thread gets its own instance.
     *
     * @return a new instance of this aggregate
     */
    default AggregateUDF<I, O> copy() {
        throw new UnsupportedOperationException("Parallel execution not supported");
    }
}
