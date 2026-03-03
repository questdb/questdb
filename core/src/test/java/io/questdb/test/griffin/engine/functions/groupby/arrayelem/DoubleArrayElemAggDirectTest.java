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

package io.questdb.test.griffin.engine.functions.groupby.arrayelem;

import io.questdb.cairo.ArrayColumnTypes;
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.arr.ArrayView;
import io.questdb.cairo.arr.DirectArray;
import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.Record;
import io.questdb.griffin.engine.functions.GroupByFunction;
import io.questdb.griffin.engine.functions.groupby.DoubleArrayElemAvgGroupByFunctionFactory;
import io.questdb.griffin.engine.functions.groupby.DoubleArrayElemMaxGroupByFunctionFactory;
import io.questdb.griffin.engine.functions.groupby.DoubleArrayElemMinGroupByFunctionFactory;
import io.questdb.griffin.engine.functions.groupby.DoubleArrayElemSumGroupByFunctionFactory;
import io.questdb.griffin.engine.groupby.FastGroupByAllocator;
import io.questdb.griffin.engine.groupby.GroupByAllocator;
import io.questdb.griffin.engine.groupby.SimpleMapValue;
import io.questdb.std.IntList;
import io.questdb.std.Misc;
import io.questdb.std.ObjList;
import io.questdb.test.AbstractCairoTest;
import org.junit.Assert;
import org.junit.Test;

/**
 * Direct (non-SQL) tests for array element aggregate GROUP BY functions.
 * Exercises overallocation cap, large arrays, shape growth, and parallel merge
 * by instantiating functions directly and feeding them programmatic arrays.
 */
public class DoubleArrayElemAggDirectTest extends AbstractCairoTest {

    // ======================== overallocation cap ========================

    @Test
    public void testAvg2dMiddleDimGrowth() throws Exception {
        // Same shape growth as sum, verify counts are correct
        assertMemoryLeak(() -> {
            try (Harness h = createAvgHarness(2)) {
                h.computeFirst(buildArray2D(2, 3, (r, c) -> 10.0));
                h.computeNext(buildArray2D(2, 5, (r, c) -> 20.0));

                ArrayView result = h.getResult();
                Assert.assertEquals(10, result.getCardinality());
                // Positions that had both rows: avg = (10+20)/2 = 15
                // Positions that had only second row: avg = 20/1 = 20
                for (int r = 0; r < 2; r++) {
                    for (int c = 0; c < 5; c++) {
                        double expected = c < 3 ? 15.0 : 20.0;
                        Assert.assertEquals(
                                "r=" + r + ",c=" + c,
                                expected, result.getDouble(r * 5 + c), 1e-10
                        );
                    }
                }
            }
        });
    }

    @Test
    public void testAvgLargeArray() throws Exception {
        assertMemoryLeak(() -> {
            try (Harness h = createAvgHarness(1)) {
                h.computeFirst(buildArray1D(2000, i -> 10.0));
                h.computeNext(buildArray1D(2000, i -> 20.0));
                h.computeNext(buildArray1D(2000, i -> 30.0));

                ArrayView result = h.getResult();
                Assert.assertEquals(2000, result.getCardinality());
                for (int i = 0; i < 2000; i++) {
                    Assert.assertEquals(20.0, result.getDouble(i), 1e-10);
                }
            }
        });
    }

    @Test
    public void testAvgLargeWithNaN() throws Exception {
        // Some positions NaN in first row, finite in second — avg should reflect count=1
        assertMemoryLeak(() -> {
            try (Harness h = createAvgHarness(1)) {
                h.computeFirst(buildArray1D(1500, i -> i % 3 == 0 ? Double.NaN : 10.0));
                h.computeNext(buildArray1D(1500, i -> 20.0));

                ArrayView result = h.getResult();
                Assert.assertEquals(1500, result.getCardinality());
                for (int i = 0; i < 1500; i++) {
                    if (i % 3 == 0) {
                        // Only second row contributed: avg = 20.0
                        Assert.assertEquals(20.0, result.getDouble(i), 1e-10);
                    } else {
                        // Both rows: (10 + 20) / 2 = 15.0
                        Assert.assertEquals(15.0, result.getDouble(i), 1e-10);
                    }
                }
            }
        });
    }

    // ======================== avg with large arrays ========================

    @Test
    public void testAvgParallelMerge2dRemap() throws Exception {
        // Merge two partitions with incompatible 2D shapes → remap
        assertMemoryLeak(() -> {
            try (Harness h1 = createAvgHarness(2);
                 Harness h2 = createAvgHarness(2)) {
                // Partition 1: (3,2)
                h1.computeFirst(buildArray2D(3, 2, (r, c) -> 6.0));
                // Partition 2: (2,4)
                h2.computeFirst(buildArray2D(2, 4, (r, c) -> 12.0));

                h1.merge(h2);

                ArrayView result = h1.getResult();
                // Merged shape: (3,4) = 12 elements
                Assert.assertEquals(12, result.getCardinality());
                Assert.assertEquals(3, result.getDimLen(0));
                Assert.assertEquals(4, result.getDimLen(1));

                // Positions that had both: avg = (6+12)/2 = 9
                // Position from partition 1 only: avg = 6
                // Position from partition 2 only: avg = 12
                for (int row = 0; row < 3; row++) {
                    for (int col = 0; col < 4; col++) {
                        double expected = getExpected(row, col);
                        double actual = result.getDouble(row * 4 + col);
                        if (Double.isNaN(expected)) {
                            Assert.assertTrue("row=" + row + ",col=" + col + " expected NaN", Double.isNaN(actual));
                        } else {
                            Assert.assertEquals("row=" + row + ",col=" + col, expected, actual, 1e-10);
                        }
                    }
                }
            }
        });
    }

    @Test
    public void testMax2dMiddleDimGrowth() throws Exception {
        // Shape (2,3) then (2,5): inner dim grows → remap, max picks larger value per position
        assertMemoryLeak(() -> {
            try (Harness h = createMaxHarness(2)) {
                h.computeFirst(buildArray2D(2, 3, (r, c) -> 100.0 + r * 3 + c));
                h.computeNext(buildArray2D(2, 5, (r, c) -> (double) (r * 5 + c)));

                ArrayView result = h.getResult();
                Assert.assertEquals(10, result.getCardinality());
                Assert.assertEquals(2, result.getDimLen(0));
                Assert.assertEquals(5, result.getDimLen(1));
                // Row 0: first=[100,101,102], second=[0,1,2,3,4] → max=[100,101,102,3,4]
                Assert.assertEquals(100.0, result.getDouble(0), 1e-10);
                Assert.assertEquals(101.0, result.getDouble(1), 1e-10);
                Assert.assertEquals(102.0, result.getDouble(2), 1e-10);
                Assert.assertEquals(3.0, result.getDouble(3), 1e-10);
                Assert.assertEquals(4.0, result.getDouble(4), 1e-10);
                // Row 1: first=[103,104,105], second=[5,6,7,8,9] → max=[103,104,105,8,9]
                Assert.assertEquals(103.0, result.getDouble(5), 1e-10);
                Assert.assertEquals(104.0, result.getDouble(6), 1e-10);
                Assert.assertEquals(105.0, result.getDouble(7), 1e-10);
                Assert.assertEquals(8.0, result.getDouble(8), 1e-10);
                Assert.assertEquals(9.0, result.getDouble(9), 1e-10);
            }
        });
    }

    @Test
    public void testMaxLargeArray() throws Exception {
        assertMemoryLeak(() -> {
            try (Harness h = createMaxHarness(1)) {
                h.computeFirst(buildArray1D(1500, i -> (double) i));
                h.computeNext(buildArray1D(1500, i -> (double) (i + 100)));

                ArrayView result = h.getResult();
                Assert.assertEquals(1500, result.getCardinality());
                for (int i = 0; i < 1500; i++) {
                    Assert.assertEquals((i + 100), result.getDouble(i), 1e-10);
                }
            }
        });
    }

    @Test
    public void testMin2dMiddleDimGrowth() throws Exception {
        // Shape (2,3) then (2,5): inner dim grows → remap, min picks smaller value per position
        assertMemoryLeak(() -> {
            try (Harness h = createMinHarness(2)) {
                h.computeFirst(buildArray2D(2, 3, (r, c) -> 100.0 + r * 3 + c));
                h.computeNext(buildArray2D(2, 5, (r, c) -> (double) (r * 5 + c)));

                ArrayView result = h.getResult();
                Assert.assertEquals(10, result.getCardinality());
                Assert.assertEquals(2, result.getDimLen(0));
                Assert.assertEquals(5, result.getDimLen(1));
                // Row 0: first=[100,101,102], second=[0,1,2,3,4] → min=[0,1,2,3,4]
                Assert.assertEquals(0.0, result.getDouble(0), 1e-10);
                Assert.assertEquals(1.0, result.getDouble(1), 1e-10);
                Assert.assertEquals(2.0, result.getDouble(2), 1e-10);
                Assert.assertEquals(3.0, result.getDouble(3), 1e-10);
                Assert.assertEquals(4.0, result.getDouble(4), 1e-10);
                // Row 1: first=[103,104,105], second=[5,6,7,8,9] → min=[5,6,7,8,9]
                Assert.assertEquals(5.0, result.getDouble(5), 1e-10);
                Assert.assertEquals(6.0, result.getDouble(6), 1e-10);
                Assert.assertEquals(7.0, result.getDouble(7), 1e-10);
                Assert.assertEquals(8.0, result.getDouble(8), 1e-10);
                Assert.assertEquals(9.0, result.getDouble(9), 1e-10);
            }
        });
    }

    // ======================== min/max large arrays ========================

    @Test
    public void testMinLargeArray() throws Exception {
        assertMemoryLeak(() -> {
            try (Harness h = createMinHarness(1)) {
                h.computeFirst(buildArray1D(1500, i -> (double) (i + 100)));
                h.computeNext(buildArray1D(1500, i -> (double) i));

                ArrayView result = h.getResult();
                Assert.assertEquals(1500, result.getCardinality());
                for (int i = 0; i < 1500; i++) {
                    Assert.assertEquals(i, result.getDouble(i), 1e-10);
                }
            }
        });
    }

    @Test
    public void testSum2dMiddleDimGrowth() throws Exception {
        // Shape (2,3) then (2,5): inner dim grows while outer stays same → remap needed
        assertMemoryLeak(() -> {
            try (Harness h = createSumHarness(2)) {
                // (2,3): values 1..6
                h.computeFirst(buildArray2D(2, 3, (r, c) -> (double) (r * 3 + c + 1)));
                // (2,5): values 10..19
                h.computeNext(buildArray2D(2, 5, (r, c) -> (double) (r * 5 + c + 10)));

                ArrayView result = h.getResult();
                // Result shape: (2,5) = 10 elements
                Assert.assertEquals(10, result.getCardinality());
                Assert.assertEquals(2, result.getDimLen(0));
                Assert.assertEquals(5, result.getDimLen(1));

                // Row 0 of (2,3): [1,2,3] mapped to positions [0,1,2] of (2,5)
                // Row 0 of (2,5): [10,11,12,13,14]
                // Merged row 0: [11,13,15,13,14]
                Assert.assertEquals(11.0, result.getDouble(0), 1e-10); // 1+10
                Assert.assertEquals(13.0, result.getDouble(1), 1e-10); // 2+11
                Assert.assertEquals(15.0, result.getDouble(2), 1e-10); // 3+12
                Assert.assertEquals(13.0, result.getDouble(3), 1e-10); // NaN→13
                Assert.assertEquals(14.0, result.getDouble(4), 1e-10); // NaN→14

                // Row 1 of (2,3): [4,5,6] mapped to positions [5,6,7] of (2,5)
                // Row 1 of (2,5): [15,16,17,18,19]
                Assert.assertEquals(19.0, result.getDouble(5), 1e-10); // 4+15
                Assert.assertEquals(21.0, result.getDouble(6), 1e-10); // 5+16
                Assert.assertEquals(23.0, result.getDouble(7), 1e-10); // 6+17
                Assert.assertEquals(18.0, result.getDouble(8), 1e-10); // NaN→18
                Assert.assertEquals(19.0, result.getDouble(9), 1e-10); // NaN→19
            }
        });
    }

    @Test
    public void testSumGrowthCrossingCapBoundary() throws Exception {
        // Start below cap (512), grow above cap (1500).
        assertMemoryLeak(() -> {
            try (Harness h = createSumHarness(1)) {
                h.computeFirst(buildArray1D(512, i -> 1.0));
                h.computeNext(buildArray1D(1500, i -> 2.0));

                ArrayView result = h.getResult();
                Assert.assertEquals(1500, result.getCardinality());
                for (int i = 0; i < 512; i++) {
                    Assert.assertEquals(3.0, result.getDouble(i), 1e-10);
                }
                for (int i = 512; i < 1500; i++) {
                    Assert.assertEquals(2.0, result.getDouble(i), 1e-10);
                }
            }
        });
    }

    // ======================== 2D middle-dimension growth ========================

    @Test
    public void testSumKahanPrecisionLargeArray() throws Exception {
        // Accumulate many rows of small values on top of a large base — Kahan should preserve precision
        assertMemoryLeak(() -> {
            try (Harness h = createSumHarness(1)) {
                int size = 1200;
                h.computeFirst(buildArray1D(size, i -> 1e15));
                for (int row = 0; row < 1000; row++) {
                    h.computeNext(buildArray1D(size, i -> 1.1));
                }

                ArrayView result = h.getResult();
                Assert.assertEquals(size, result.getCardinality());
                double expected = 1e15 + 1000 * 1.1;
                for (int i = 0; i < size; i++) {
                    Assert.assertEquals(expected, result.getDouble(i), 1e-5);
                }
            }
        });
    }

    @Test
    public void testSumNullThenLargeArray() throws Exception {
        assertMemoryLeak(() -> {
            try (Harness h = createSumHarness(1)) {
                h.computeFirst(null);
                h.computeNext(buildArray1D(1500, i -> (double) i));

                ArrayView result = h.getResult();
                Assert.assertEquals(1500, result.getCardinality());
                for (int i = 0; i < 1500; i++) {
                    Assert.assertEquals(i, result.getDouble(i), 1e-10);
                }
            }
        });
    }

    // ======================== parallel merge with large arrays ========================

    @Test
    public void testSumOverallocCapAboveThreshold() throws Exception {
        // 1200 elements: above OVERALLOC_CAP (1024), exact capacity, no overallocation.
        // Growth to 1300 must realloc since there's no slack.
        assertMemoryLeak(() -> {
            try (Harness h = createSumHarness(1)) {
                h.computeFirst(buildArray1D(1200, i -> 1.0));
                h.computeNext(buildArray1D(1300, i -> 2.0));

                ArrayView result = h.getResult();
                Assert.assertEquals(1300, result.getCardinality());
                // First 1200 positions: 1.0 + 2.0 = 3.0
                for (int i = 0; i < 1200; i++) {
                    Assert.assertEquals(3.0, result.getDouble(i), 1e-10);
                }
                // Positions 1200..1299: only second row = 2.0
                for (int i = 1200; i < 1300; i++) {
                    Assert.assertEquals(2.0, result.getDouble(i), 1e-10);
                }
            }
        });
    }

    @Test
    public void testSumOverallocCapBelowThreshold() throws Exception {
        // 800 elements: below OVERALLOC_CAP (1024), should overallocate.
        // After computeFirst, grow to 900: should fit in-place (no realloc) thanks to overalloc.
        assertMemoryLeak(() -> {
            try (Harness h = createSumHarness(1)) {
                h.computeFirst(buildArray1D(800, i -> (double) i));
                // Grow to 900 — should fit in the overallocated buffer
                h.computeNext(buildArray1D(900, i -> (double) i));

                ArrayView result = h.getResult();
                Assert.assertEquals(900, result.getCardinality());
                // Positions 0..799 got both rows summed; positions 800..899 got only the second row
                for (int i = 0; i < 800; i++) {
                    Assert.assertEquals(2.0 * i, result.getDouble(i), 1e-10);
                }
                for (int i = 800; i < 900; i++) {
                    Assert.assertEquals(i, result.getDouble(i), 1e-10);
                }
            }
        });
    }

    // ======================== null / empty edge cases ========================

    @Test
    public void testSumParallelMergeLargeArrays() throws Exception {
        assertMemoryLeak(() -> {
            try (Harness h1 = createSumHarness(1);
                 Harness h2 = createSumHarness(1)) {
                // Partition 1: 1500-element array
                h1.computeFirst(buildArray1D(1500, i -> 1.0));
                h1.computeNext(buildArray1D(1500, i -> 2.0));

                // Partition 2: 1200-element array (smaller)
                h2.computeFirst(buildArray1D(1200, i -> 10.0));

                // Merge partition 2 into partition 1
                h1.merge(h2);

                ArrayView result = h1.getResult();
                Assert.assertEquals(1500, result.getCardinality());
                // First 1200: 1+2+10 = 13
                for (int i = 0; i < 1200; i++) {
                    Assert.assertEquals(13.0, result.getDouble(i), 1e-10);
                }
                // 1200..1499: 1+2 = 3
                for (int i = 1200; i < 1500; i++) {
                    Assert.assertEquals(3.0, result.getDouble(i), 1e-10);
                }
            }
        });
    }

    // ======================== Kahan precision with large accumulation ========================

    private static DirectArray buildArray1D(int size, DoubleIndexFunction filler) {
        DirectArray array = new DirectArray();
        array.setType(ColumnType.encodeArrayType(ColumnType.DOUBLE, 1));
        array.getShape().set(0, size);
        array.applyShape();
        for (int i = 0; i < size; i++) {
            array.putDouble(i, filler.apply(i));
        }
        return array;
    }

    // ======================== helpers ========================

    private static DirectArray buildArray2D(int rows, int cols, Double2DFunction filler) {
        DirectArray array = new DirectArray();
        array.setType(ColumnType.encodeArrayType(ColumnType.DOUBLE, 2));
        array.getShape().set(0, rows);
        array.getShape().set(1, cols);
        array.applyShape();
        for (int r = 0; r < rows; r++) {
            for (int c = 0; c < cols; c++) {
                array.putDouble(r * cols + c, filler.apply(r, c));
            }
        }
        return array;
    }

    private static double getExpected(int row, int col) {
        boolean inP1 = col < 2; // partition 1 shape: (3,2) — all rows in range
        boolean inP2 = row < 2; // partition 2 shape: (2,4) — all cols in range
        double expected;
        if (inP1 && inP2) {
            expected = 9.0; // both partitions: (6+12)/2
        } else if (inP1) {
            expected = 6.0; // partition 1 only
        } else if (inP2) {
            expected = 12.0; // partition 2 only
        } else {
            expected = Double.NaN; // neither partition
        }
        return expected;
    }

    private Harness createAvgHarness(int nDims) throws Exception {
        return createHarness(new DoubleArrayElemAvgGroupByFunctionFactory(), nDims);
    }

    private Harness createHarness(io.questdb.griffin.FunctionFactory factory, int nDims) throws Exception {
        int arrayType = ColumnType.encodeArrayType(ColumnType.DOUBLE, nDims);
        StubArrayFunction stub = new StubArrayFunction(arrayType);
        ObjList<Function> args = new ObjList<>();
        args.add(stub);
        IntList argPositions = new IntList();
        argPositions.add(0);
        Function func = factory.newInstance(0, args, argPositions, configuration, null);
        return new Harness((GroupByFunction) func, stub);
    }

    private Harness createMaxHarness(int nDims) throws Exception {
        return createHarness(new DoubleArrayElemMaxGroupByFunctionFactory(), nDims);
    }

    private Harness createMinHarness(int nDims) throws Exception {
        return createHarness(new DoubleArrayElemMinGroupByFunctionFactory(), nDims);
    }

    private Harness createSumHarness(int nDims) throws Exception {
        return createHarness(new DoubleArrayElemSumGroupByFunctionFactory(), nDims);
    }

    @FunctionalInterface
    private interface Double2DFunction {
        double apply(int row, int col);
    }

    @FunctionalInterface
    private interface DoubleIndexFunction {
        double apply(int index);
    }

    /**
     * Wraps a GroupByFunction with its allocator, MapValues, and stub arg for direct testing.
     */
    private static class Harness implements AutoCloseable {
        private final GroupByAllocator allocator;
        private final GroupByFunction func;
        private final SimpleMapValue mapValue;
        private final StubArrayFunction stub;

        Harness(GroupByFunction func, StubArrayFunction stub) {
            this.func = func;
            this.stub = stub;
            this.allocator = new FastGroupByAllocator(8192, 4 * 1024 * 1024);
            func.setAllocator(allocator);
            ArrayColumnTypes types = new ArrayColumnTypes();
            func.initValueTypes(types);
            this.mapValue = new SimpleMapValue(types.getColumnCount());
            // Start in null state
            func.setNull(mapValue);
        }

        @Override
        public void close() {
            Misc.free(func);
            mapValue.close();
            allocator.close();
        }

        void computeFirst(DirectArray array) {
            stub.setCurrent(array);
            func.computeFirst(mapValue, null, 0);
            if (array != null) {
                array.close();
            }
        }

        void computeNext(DirectArray array) {
            stub.setCurrent(array);
            func.computeNext(mapValue, null, 0);
            if (array != null) {
                array.close();
            }
        }

        ArrayView getResult() {
            return func.getArray(mapValue);
        }

        void merge(Harness src) {
            func.merge(mapValue, src.mapValue);
        }
    }

    /**
     * Stub Function that returns whatever ArrayView is set on it.
     */
    private static class StubArrayFunction extends io.questdb.cairo.sql.ArrayFunction {

        private ArrayView current;

        StubArrayFunction(int arrayType) {
            this.type = arrayType;
        }

        @Override
        public ArrayView getArray(Record rec) {
            return current;
        }

        void setCurrent(ArrayView array) {
            this.current = array;
        }
    }
}
