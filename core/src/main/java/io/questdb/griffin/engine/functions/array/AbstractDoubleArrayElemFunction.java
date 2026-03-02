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

package io.questdb.griffin.engine.functions.array;

import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.arr.ArrayView;
import io.questdb.cairo.arr.DirectArray;
import io.questdb.cairo.sql.ArrayFunction;
import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.SymbolTableSource;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.engine.functions.MultiArgFunction;
import io.questdb.griffin.engine.functions.constants.ArrayConstant;
import io.questdb.std.DoubleList;
import io.questdb.std.IntList;
import io.questdb.std.Misc;
import io.questdb.std.Numbers;
import io.questdb.std.ObjList;

/**
 * Base class for element-wise variadic array aggregation functions
 * ({@code array_elem_max}, {@code array_elem_min}, {@code array_elem_sum},
 * {@code array_elem_avg}).
 * <p>
 * Handles nD shape resolution, NaN-fill, coordinate iteration, and the
 * accumulate-or-seed pattern. Subclasses only provide {@link #accumulate} for the
 * element-wise operation. Sum and avg use {@link #kahanAccumulate} for
 * compensated summation. The avg variant additionally overrides
 * {@link #beforeAccumulation} and {@link #postProcess} for per-position
 * count tracking and final division.
 * <p>
 * When all non-null inputs are vanilla row-major and share inner dimensions with
 * the output, uses a flat fast path that avoids coordinate arithmetic entirely.
 * Otherwise falls back to a coordinate-based nD path that handles arbitrary
 * strides (transpose, slicing, broadcasting) and mismatched shapes.
 */
public abstract class AbstractDoubleArrayElemFunction extends ArrayFunction implements MultiArgFunction {

    /**
     * The variadic DOUBLE[] arguments.
     */
    protected final ObjList<Function> args;
    /**
     * Reusable output array, reshaped per call.
     */
    protected final DirectArray arrayOut;
    /**
     * Cached array views from the current row, populated by {@link #scanInputs}.
     * Null entries represent NULL/empty inputs.
     */
    private final ObjList<ArrayView> cachedViews = new ObjList<>();
    /**
     * Scratch: coordinate vector for nD iteration (size nDims).
     */
    protected int[] coords;
    /**
     * Scratch: shape of the current input being accumulated (size nDims).
     */
    protected int[] inputShape;
    /**
     * Scratch: per-dimension max across all inputs (size nDims).
     */
    protected int[] maxShape;
    /**
     * Number of array dimensions; 0 until resolved from weak dims.
     */
    protected int nDims;
    /**
     * Scratch: row-major strides for the output shape (size nDims).
     */
    protected int[] outStrides;

    protected AbstractDoubleArrayElemFunction(CairoConfiguration configuration, ObjList<Function> args, int resolvedDims) {
        this.args = args;
        this.nDims = resolvedDims;
        if (resolvedDims > 0) {
            this.type = ColumnType.encodeArrayType(ColumnType.DOUBLE, resolvedDims);
            this.maxShape = new int[resolvedDims];
            this.coords = new int[resolvedDims];
            this.inputShape = new int[resolvedDims];
            this.outStrides = new int[resolvedDims];
        } else {
            this.type = ColumnType.encodeArrayTypeWithWeakDims(ColumnType.DOUBLE, true);
        }
        this.arrayOut = new DirectArray(configuration);
        this.arrayOut.setType(type);
    }

    @Override
    public ObjList<Function> args() {
        return args;
    }

    @Override
    public void close() {
        MultiArgFunction.super.close();
        Misc.free(arrayOut);
    }

    @Override
    public ArrayView getArray(Record rec) {
        boolean canUseFlatPath = scanInputs(rec);

        int totalFlatLen = 1;
        for (int d = 0; d < nDims; d++) {
            arrayOut.setDimLen(d, maxShape[d]);
            totalFlatLen *= maxShape[d];
        }
        if (totalFlatLen == 0) {
            return ArrayConstant.NULL;
        }
        arrayOut.applyShape();
        for (int i = 0; i < totalFlatLen; i++) {
            arrayOut.putDouble(i, Double.NaN);
        }
        beforeAccumulation(totalFlatLen);

        if (canUseFlatPath) {
            accumulateFlat();
        } else {
            accumulateCoords();
        }

        postProcess(totalFlatLen);
        return arrayOut;
    }

    @Override
    public void init(SymbolTableSource symbolTableSource, SqlExecutionContext executionContext) throws SqlException {
        MultiArgFunction.super.init(symbolTableSource, executionContext);
        int resolvedDims = 0;
        for (int i = 0, n = args.size(); i < n; i++) {
            int d = ColumnType.decodeWeakArrayDimensionality(args.getQuick(i).getType());
            if (d > 0) {
                resolvedDims = d;
                break;
            }
        }
        if (resolvedDims > 0) {
            this.nDims = resolvedDims;
            this.type = ColumnType.encodeArrayType(ColumnType.DOUBLE, resolvedDims);
            this.arrayOut.setType(type);
            this.maxShape = new int[resolvedDims];
            this.coords = new int[resolvedDims];
            this.inputShape = new int[resolvedDims];
            this.outStrides = new int[resolvedDims];
        }
    }

    @Override
    public boolean isThreadSafe() {
        return false;
    }

    /**
     * Coordinate-based nD path: for each cached input, iterates that input's
     * coordinates and maps each to the output via row-major strides.
     * Uses {@code arr.getStride(d)} for the input flat index, handling
     * non-vanilla strides (transpose, slicing).
     */
    private void accumulateCoords() {
        ArrayView.computeRowMajorStrides(maxShape, outStrides);
        for (int a = 0, n = cachedViews.size(); a < n; a++) {
            ArrayView arr = cachedViews.getQuick(a);
            if (arr == null) {
                continue;
            }
            for (int d = 0; d < nDims; d++) {
                inputShape[d] = arr.getDimLen(d);
                coords[d] = 0;
            }
            do {
                int inputFi = 0;
                for (int d = 0; d < nDims; d++) {
                    inputFi += coords[d] * arr.getStride(d);
                }
                double val = arr.getDouble(inputFi);
                if (Numbers.isFinite(val)) {
                    int outFi = ArrayView.coordsToFlatIndex(coords, outStrides);
                    accumulate(outFi, val);
                }
            } while (ArrayView.incrementCoords(coords, inputShape));
        }
    }

    /**
     * Flat fast path: all non-null inputs are vanilla row-major with inner dims
     * matching the output, so flat index {@code i} in the input maps to the same
     * logical coordinate as flat index {@code i} in the output.
     */
    private void accumulateFlat() {
        for (int a = 0, n = cachedViews.size(); a < n; a++) {
            ArrayView arr = cachedViews.getQuick(a);
            if (arr == null) {
                continue;
            }
            int len = arr.getFlatViewLength();
            for (int i = 0; i < len; i++) {
                double val = arr.getDouble(i);
                if (Numbers.isFinite(val)) {
                    accumulate(i, val);
                }
            }
        }
    }

    /**
     * Evaluates all arguments once, caching views in {@link #cachedViews}.
     * Populates {@link #maxShape} with the per-dimension max across non-null inputs.
     * Checks whether the flat fast path is usable: all non-null inputs must be
     * vanilla row-major with inner dimensions (d &ge; 1) matching the output's.
     *
     * @return true if the flat path can be used, false if coordinate path is needed
     */
    private boolean scanInputs(Record rec) {
        for (int d = 0; d < nDims; d++) {
            maxShape[d] = 0;
        }
        int n = args.size();
        cachedViews.clear();
        cachedViews.setPos(n);
        boolean canUseFlatPath = true;
        for (int i = 0; i < n; i++) {
            ArrayView a = args.getQuick(i).getArray(rec);
            if (a == null || a.isNull()) {
                cachedViews.setQuick(i, null);
                continue;
            }
            cachedViews.setQuick(i, a);
            for (int d = 0; d < nDims; d++) {
                maxShape[d] = Math.max(maxShape[d], a.getDimLen(d));
            }
            if (canUseFlatPath && !a.isVanilla()) {
                canUseFlatPath = false;
            }
        }
        if (canUseFlatPath) {
            for (int i = 0; i < n; i++) {
                ArrayView a = cachedViews.getQuick(i);
                if (a == null) {
                    continue;
                }
                for (int d = 1; d < nDims; d++) {
                    if (a.getDimLen(d) != maxShape[d]) {
                        return false;
                    }
                }
            }
        }
        return canUseFlatPath;
    }

    /**
     * Validates that all arguments are DOUBLE[] with consistent dimensionality.
     *
     * @return resolved dimensionality (0 if all args have weak dims)
     */
    static int validateArgsAndResolveDims(
            int position,
            ObjList<Function> args,
            IntList argPositions,
            String funcName
    ) throws SqlException {
        if (args == null || args.size() < 2) {
            throw SqlException.$(position, funcName + " requires at least 2 arguments");
        }
        int resolvedDims = 0;
        for (int i = 0, n = args.size(); i < n; i++) {
            int t = args.getQuick(i).getType();
            if (!ColumnType.isArray(t) || ColumnType.decodeArrayElementType(t) != ColumnType.DOUBLE) {
                throw SqlException.$(argPositions.getQuick(i), "expected DOUBLE[] argument");
            }
            int d = ColumnType.decodeWeakArrayDimensionality(t);
            if (d > 0) {
                if (resolvedDims > 0 && resolvedDims != d) {
                    throw SqlException.$(position, "dimension mismatch");
                }
                resolvedDims = d;
            }
        }
        return resolvedDims;
    }

    /**
     * Accumulates a single finite value at the given output position.
     * Called once per finite input element during the accumulation loop.
     *
     * @param outIndex flat index in the output array
     * @param val      the finite input value to accumulate
     */
    protected abstract void accumulate(int outIndex, double val);

    /**
     * Called before the accumulation loop. Subclasses use this to initialize per-position state.
     *
     * @param totalFlatLen total number of flat positions in the output
     */
    protected void beforeAccumulation(int totalFlatLen) {
    }

    /**
     * Kahan compensated accumulation. Seeds the position on first value,
     * otherwise adds with compensation to reduce floating-point error.
     *
     * @param outIndex     flat index in the output array
     * @param val          the finite input value to accumulate
     * @param compensation list holding per-position Kahan compensation values
     */
    protected void kahanAccumulate(int outIndex, double val, DoubleList compensation) {
        double cur = arrayOut.getDouble(outIndex);
        if (Numbers.isFinite(cur)) {
            double c = compensation.getQuick(outIndex);
            double y = val - c;
            double t = cur + y;
            compensation.setQuick(outIndex, (t - cur) - y);
            arrayOut.putDouble(outIndex, t);
        } else {
            arrayOut.putDouble(outIndex, val);
        }
    }

    /**
     * Called after all inputs are accumulated. Avg uses this to divide sums by counts.
     *
     * @param totalFlatLen total number of flat positions in the output
     */
    protected void postProcess(int totalFlatLen) {
    }
}
