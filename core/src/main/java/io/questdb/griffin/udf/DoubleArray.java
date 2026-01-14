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

import io.questdb.cairo.arr.ArrayView;

import java.util.Iterator;
import java.util.NoSuchElementException;

/**
 * Wrapper class for QuestDB double arrays in UDFs.
 * <p>
 * This class provides a simple interface for working with 1D double arrays
 * in the simplified UDF API. It wraps QuestDB's {@link ArrayView} for
 * user-friendly access.
 * <p>
 * Example usage:
 * <pre>{@code
 * // Create a UDF that sums an array
 * FunctionFactory arraySum = UDFRegistry.scalar(
 *     "my_array_sum",
 *     DoubleArray.class,
 *     Double.class,
 *     arr -> {
 *         if (arr == null || arr.isEmpty()) return 0.0;
 *         double sum = 0;
 *         for (double v : arr) {
 *             sum += v;
 *         }
 *         return sum;
 *     }
 * );
 * }</pre>
 */
public final class DoubleArray implements Iterable<Double> {

    private final ArrayView arrayView;

    /**
     * Creates a DoubleArray from an ArrayView.
     *
     * @param arrayView the underlying array view
     */
    public DoubleArray(ArrayView arrayView) {
        this.arrayView = arrayView;
    }

    /**
     * Returns the number of elements in this array.
     *
     * @return the length of the array
     */
    public int length() {
        if (arrayView == null || arrayView.isNull()) {
            return 0;
        }
        return arrayView.getCardinality();
    }

    /**
     * Returns true if this array is empty or null.
     *
     * @return true if empty or null
     */
    public boolean isEmpty() {
        return arrayView == null || arrayView.isNull() || arrayView.isEmpty();
    }

    /**
     * Returns the value at the specified index.
     *
     * @param index the index (0-based)
     * @return the value at the index
     * @throws IndexOutOfBoundsException if index is out of range
     */
    public double get(int index) {
        if (index < 0 || index >= length()) {
            throw new IndexOutOfBoundsException("Index: " + index + ", Length: " + length());
        }
        return arrayView.getDouble(index);
    }

    /**
     * Returns the underlying ArrayView.
     *
     * @return the ArrayView, may be null
     */
    public ArrayView getArrayView() {
        return arrayView;
    }

    /**
     * Copies the array contents to a Java double array.
     *
     * @return a new double array with the contents
     */
    public double[] toArray() {
        int len = length();
        double[] result = new double[len];
        for (int i = 0; i < len; i++) {
            result[i] = arrayView.getDouble(i);
        }
        return result;
    }

    /**
     * Returns the sum of all elements.
     *
     * @return the sum
     */
    public double sum() {
        double sum = 0;
        int len = length();
        for (int i = 0; i < len; i++) {
            sum += arrayView.getDouble(i);
        }
        return sum;
    }

    /**
     * Returns the average of all elements.
     *
     * @return the average, or NaN if empty
     */
    public double avg() {
        int len = length();
        if (len == 0) {
            return Double.NaN;
        }
        return sum() / len;
    }

    /**
     * Returns the minimum value in the array.
     *
     * @return the minimum, or NaN if empty
     */
    public double min() {
        int len = length();
        if (len == 0) {
            return Double.NaN;
        }
        double min = arrayView.getDouble(0);
        for (int i = 1; i < len; i++) {
            double v = arrayView.getDouble(i);
            if (v < min) {
                min = v;
            }
        }
        return min;
    }

    /**
     * Returns the maximum value in the array.
     *
     * @return the maximum, or NaN if empty
     */
    public double max() {
        int len = length();
        if (len == 0) {
            return Double.NaN;
        }
        double max = arrayView.getDouble(0);
        for (int i = 1; i < len; i++) {
            double v = arrayView.getDouble(i);
            if (v > max) {
                max = v;
            }
        }
        return max;
    }

    @Override
    public Iterator<Double> iterator() {
        return new Iterator<>() {
            private int pos = 0;
            private final int len = length();

            @Override
            public boolean hasNext() {
                return pos < len;
            }

            @Override
            public Double next() {
                if (!hasNext()) {
                    throw new NoSuchElementException();
                }
                return arrayView.getDouble(pos++);
            }
        };
    }

    @Override
    public String toString() {
        if (isEmpty()) {
            return "[]";
        }
        StringBuilder sb = new StringBuilder("[");
        int len = length();
        for (int i = 0; i < len; i++) {
            if (i > 0) {
                sb.append(", ");
            }
            sb.append(arrayView.getDouble(i));
        }
        sb.append("]");
        return sb.toString();
    }
}
