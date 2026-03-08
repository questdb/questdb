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
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 *
 ******************************************************************************/

package io.questdb.griffin.engine.groupby.hyperloglog;

import io.questdb.griffin.engine.groupby.GroupByAllocator;
import io.questdb.std.Unsafe;
import org.jetbrains.annotations.TestOnly;

/**
 * This is an implementation of HyperLogLog++ described in the paper
 * <a href="http://static.googleusercontent.com/media/research.google.com/fr//pubs/archive/40671.pdf">'HyperLogLog in
 * Practice: Algorithmic Engineering of a State of The Art Cardinality Estimation Algorithm'</a>.
 * <p>
 * This class assumes that specific HyperLogLog representations reserve the first byte in their memory layouts for
 * information about their type and the subsequent 8 bytes for cached cardinality.
 */
public class HyperLogLog {
    public static final int DEFAULT_PRECISION = 14;
    public static final int MAX_PRECISION = 18;
    public static final int MIN_PRECISION = 4;
    private static final long CACHED_CARDINALITY_OFFSET = Byte.BYTES;
    private static final long CARDINALITY_NULL_VALUE = -1;
    private static final byte DENSE = 0;
    private static final byte SPARSE = 1;
    private final HyperLogLogDenseRepresentation dense;
    private final int precision;
    private final HyperLogLogSparseRepresentation sparse;

    private long ptr;

    public HyperLogLog(int precision) {
        assert precision >= MIN_PRECISION && precision <= MAX_PRECISION : "Precision must be within the range of 4 to 18, inclusive.";
        this.dense = new HyperLogLogDenseRepresentation(precision);
        this.sparse = new HyperLogLogSparseRepresentation(precision);
        this.precision = precision;
    }

    public static long merge(HyperLogLog first, HyperLogLog second) {
        byte firstType = first.getType();
        byte secondType = second.getType();

        if (firstType == DENSE && secondType == DENSE) {
            return mergeDenseWithDense(first, second);
        } else if (firstType == SPARSE && secondType == SPARSE) {
            if (second.sparse.size() < first.sparse.size()) {
                return mergeSparseWithSparse(second, first);
            } else {
                return mergeSparseWithSparse(first, second);
            }
        } else if (firstType == SPARSE && secondType == DENSE) {
            return mergeSparseWithDense(first, second);
        } else if (firstType == DENSE && secondType == SPARSE) {
            return mergeSparseWithDense(second, first);
        }
        throw new IllegalStateException("Unexpected combination of HyperLogLog types.");
    }

    /**
     * Adds the provided hash to this instance of HyperLogLog and, if it is
     * computationally inexpensive, calculates the current cardinality.
     * Combining these two operations in one method allows us to take advantage of
     * the data that is already in the CPU cache and avoid cache misses while
     * performing deferred cardinality computation using the
     * {@link HyperLogLog#computeCardinality()} method.
     *
     * @param hash hash to be added to this instance of HyperLogLog
     * @return estimated cardinality or -1
     */
    public long addAndComputeCardinalityFast(long hash) {
        if (getType() == SPARSE) {
            sparse.add(hash);
            ptr = sparse.ptr();
            if (sparse.isFull()) {
                convertToDense();
                setCachedCardinality(CARDINALITY_NULL_VALUE);
                return -1;
            }
            long cardinality = sparse.computeCardinality();
            setCachedCardinality(cardinality);
            return cardinality;
        } else {
            dense.add(hash);
            setCachedCardinality(CARDINALITY_NULL_VALUE);
            return -1;
        }
    }

    public long computeCardinality() {
        long cardinality = getCachedCardinality();
        if (cardinality != CARDINALITY_NULL_VALUE) {
            return cardinality;
        }
        if (getType() == SPARSE) {
            cardinality = sparse.computeCardinality();
        } else {
            cardinality = dense.computeCardinality();
        }
        setCachedCardinality(cardinality);
        return cardinality;
    }

    @TestOnly
    public boolean isSparse() {
        return getType() == SPARSE;
    }

    public HyperLogLog of(long ptr) {
        if (ptr == 0) {
            if (HyperLogLogSparseRepresentation.calculateSparseSetMaxSize(precision) > 0) {
                this.ptr = sparse.of(0).ptr();
                setType(SPARSE);
            } else {
                this.ptr = dense.of(0).ptr();
                setType(DENSE);
            }
            setCachedCardinality(CARDINALITY_NULL_VALUE);
        } else {
            this.ptr = ptr;
            if (getType() == SPARSE) {
                sparse.of(ptr);
            } else {
                dense.of(ptr);
            }
        }
        return this;
    }

    public long ptr() {
        return ptr;
    }

    public void resetPtr() {
        ptr = 0;
    }

    public void setAllocator(GroupByAllocator allocator) {
        sparse.setAllocator(allocator);
        dense.setAllocator(allocator);
    }

    private static long mergeDenseWithDense(HyperLogLog src, HyperLogLog dst) {
        src.dense.copyTo(dst.dense);
        dst.of(dst.dense.ptr());
        dst.setCachedCardinality(CARDINALITY_NULL_VALUE);
        return dst.ptr();
    }

    private static long mergeSparseWithDense(HyperLogLog src, HyperLogLog dst) {
        src.sparse.copyTo(dst.dense);
        dst.of(dst.dense.ptr());
        dst.setCachedCardinality(CARDINALITY_NULL_VALUE);
        return dst.ptr();
    }

    private static long mergeSparseWithSparse(HyperLogLog src, HyperLogLog dst) {
        src.sparse.copyTo(dst.sparse);
        dst.of(dst.sparse.ptr());
        if (dst.sparse.isFull()) {
            dst.convertToDense();
        }
        dst.setCachedCardinality(CARDINALITY_NULL_VALUE);
        return dst.ptr();
    }

    private void convertToDense() {
        sparse.convertToDense(dense);
        this.ptr = dense.ptr();
        setType(DENSE);
    }

    private long getCachedCardinality() {
        return Unsafe.getUnsafe().getLong(ptr + CACHED_CARDINALITY_OFFSET);
    }

    private byte getType() {
        return Unsafe.getUnsafe().getByte(ptr);
    }

    private void setCachedCardinality(long estimate) {
        Unsafe.getUnsafe().putLong(ptr + CACHED_CARDINALITY_OFFSET, estimate);
    }

    private void setType(byte type) {
        Unsafe.getUnsafe().putByte(ptr, type);
    }
}
