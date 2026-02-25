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
 * accumulate-or-seed pattern. Subclasses only provide {@link #combine} for the
 * element-wise operation. The avg variant additionally overrides
 * {@link #beforeAccumulation}, {@link #onAccumulate}, and {@link #postProcess}
 * for per-position count tracking and final division.
 * <p>
 * When all non-null inputs are vanilla row-major and share inner dimensions with
 * the output, uses a flat fast path that avoids coordinate arithmetic entirely.
 * Otherwise falls back to a coordinate-based nD path that handles arbitrary
 * strides (transpose, slicing, broadcasting) and mismatched shapes.
 */
public abstract class AbstractDoubleArrayElemFunction extends ArrayFunction implements MultiArgFunction {

    /** Reusable output array, reshaped per call. */
    protected final DirectArray arrayOut;
    /** The variadic DOUBLE[] arguments. */
    protected final ObjList<Function> args;
    /** Scratch: coordinate vector for nD iteration (size nDims). */
    protected int[] coords;
    /** Scratch: shape of the current input being accumulated (size nDims). */
    protected int[] inputShape;
    /** Scratch: per-dimension max across all inputs (size nDims). */
    protected int[] maxShape;
    /** Number of array dimensions; 0 until resolved from weak dims. */
    protected int nDims;
    /** Scratch: row-major strides for the output shape (size nDims). */
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
            accumulateFlat(rec);
        } else {
            accumulateCoords(rec);
        }

        postProcess(totalFlatLen);
        return arrayOut;
    }

    @Override
    public void init(SymbolTableSource symbolTableSource, SqlExecutionContext executionContext) throws SqlException {
        MultiArgFunction.super.init(symbolTableSource, executionContext);
        int resolvedDims = 0;
        for (int i = 0, n = args.size(); i < n; i++) {
            int d = ColumnType.decodeArrayDimensionality(args.getQuick(i).getType());
            if (d > 0) {
                resolvedDims = d;
                break;
            }
        }
        this.nDims = resolvedDims;
        this.type = ColumnType.encodeArrayType(ColumnType.DOUBLE, resolvedDims);
        this.arrayOut.setType(type);
        this.maxShape = new int[resolvedDims];
        this.coords = new int[resolvedDims];
        this.inputShape = new int[resolvedDims];
        this.outStrides = new int[resolvedDims];
    }

    @Override
    public boolean isThreadSafe() {
        return false;
    }

    /** Called before the accumulation loop. Avg uses this to initialize per-position counts. */
    protected void beforeAccumulation(int totalFlatLen) {
    }

    /** The element-wise aggregation operation (e.g. {@code Math.max}, {@code Math.min}, {@code +}). */
    protected abstract double combine(double cur, double val);

    /** Called after each finite value is accumulated. Avg uses this to increment per-position counts. */
    protected void onAccumulate(int outFlatIndex) {
    }

    /** Called after all inputs are accumulated. Avg uses this to divide sums by counts. */
    protected void postProcess(int totalFlatLen) {
    }

    /**
     * Scans all inputs, populating {@link #maxShape} with the per-dimension max
     * across non-null inputs. Also checks whether the flat fast path is usable:
     * all non-null inputs must be vanilla row-major with inner dimensions (d &ge; 1)
     * matching the output's. If no non-null inputs exist, maxShape remains all-zeros.
     *
     * @return true if the flat path can be used, false if coordinate path is needed
     */
    private boolean scanInputs(Record rec) {
        for (int d = 0; d < nDims; d++) {
            maxShape[d] = 0;
        }
        boolean canUseFlatPath = true;
        for (int i = 0, n = args.size(); i < n; i++) {
            ArrayView a = args.getQuick(i).getArray(rec);
            if (a == null || a.isNull()) {
                continue;
            }
            for (int d = 0; d < nDims; d++) {
                maxShape[d] = Math.max(maxShape[d], a.getDimLen(d));
            }
            if (canUseFlatPath && !a.isVanilla()) {
                canUseFlatPath = false;
            }
        }
        if (canUseFlatPath) {
            for (int i = 0, n = args.size(); i < n; i++) {
                ArrayView a = args.getQuick(i).getArray(rec);
                if (a == null || a.isNull()) {
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
     * Flat fast path: all non-null inputs are vanilla row-major with inner dims
     * matching the output, so flat index {@code i} in the input maps to the same
     * logical coordinate as flat index {@code i} in the output. No coordinate
     * arithmetic, no bounds check per dimension — just a bare linear scan.
     */
    private void accumulateFlat(Record rec) {
        for (int a = 0, n = args.size(); a < n; a++) {
            ArrayView arr = args.getQuick(a).getArray(rec);
            if (arr == null || arr.isNull()) {
                continue;
            }
            int len = arr.getFlatViewLength();
            for (int i = 0; i < len; i++) {
                double val = arr.getDouble(i);
                if (Numbers.isFinite(val)) {
                    double cur = arrayOut.getDouble(i);
                    if (Numbers.isFinite(cur)) {
                        arrayOut.putDouble(i, combine(cur, val));
                    } else {
                        arrayOut.putDouble(i, val);
                    }
                    onAccumulate(i);
                }
            }
        }
    }

    /**
     * Coordinate-based nD path: for each input, iterates that input's coordinates
     * and maps each to the output via row-major strides. Uses {@code arr.getStride(d)}
     * for the input flat index, handling non-vanilla strides (transpose, slicing).
     * No bounds checking needed — the output shape is the per-dimension max, so
     * every input coordinate is always in bounds for the output.
     */
    private void accumulateCoords(Record rec) {
        ArrayView.computeRowMajorStrides(maxShape, outStrides);
        for (int a = 0, n = args.size(); a < n; a++) {
            ArrayView arr = args.getQuick(a).getArray(rec);
            if (arr == null || arr.isNull()) {
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
                    double cur = arrayOut.getDouble(outFi);
                    if (Numbers.isFinite(cur)) {
                        arrayOut.putDouble(outFi, combine(cur, val));
                    } else {
                        arrayOut.putDouble(outFi, val);
                    }
                    onAccumulate(outFi);
                }
            } while (ArrayView.incrementCoords(coords, inputShape));
        }
    }
}
