/*+*****************************************************************************
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

package io.questdb.griffin.engine.groupby;

import io.questdb.cairo.CairoException;
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.RecordMetadata;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlKeywords;
import io.questdb.griffin.engine.functions.GroupByFunction;
import io.questdb.griffin.engine.functions.PerWorkerFunctionList;
import io.questdb.griffin.engine.functions.columns.ArrayColumn;
import io.questdb.griffin.engine.functions.columns.BinColumn;
import io.questdb.griffin.engine.functions.columns.BooleanColumn;
import io.questdb.griffin.engine.functions.columns.ByteColumn;
import io.questdb.griffin.engine.functions.columns.CharColumn;
import io.questdb.griffin.engine.functions.columns.ColumnFunction;
import io.questdb.griffin.engine.functions.columns.DateColumn;
import io.questdb.griffin.engine.functions.columns.DecimalColumn;
import io.questdb.griffin.engine.functions.columns.DoubleColumn;
import io.questdb.griffin.engine.functions.columns.FloatColumn;
import io.questdb.griffin.engine.functions.columns.GeoByteColumn;
import io.questdb.griffin.engine.functions.columns.GeoIntColumn;
import io.questdb.griffin.engine.functions.columns.GeoLongColumn;
import io.questdb.griffin.engine.functions.columns.GeoShortColumn;
import io.questdb.griffin.engine.functions.columns.IPv4Column;
import io.questdb.griffin.engine.functions.columns.IntColumn;
import io.questdb.griffin.engine.functions.columns.IntervalColumn;
import io.questdb.griffin.engine.functions.columns.Long128Column;
import io.questdb.griffin.engine.functions.columns.Long256Column;
import io.questdb.griffin.engine.functions.columns.LongColumn;
import io.questdb.griffin.engine.functions.columns.ShortColumn;
import io.questdb.griffin.engine.functions.columns.StrColumn;
import io.questdb.griffin.engine.functions.columns.TimestampColumn;
import io.questdb.griffin.engine.functions.columns.UuidColumn;
import io.questdb.griffin.engine.functions.columns.VarcharColumn;
import io.questdb.griffin.engine.functions.groupby.SparklineGroupByFunction;
import io.questdb.griffin.engine.functions.groupby.TwapGroupByFunction;
import io.questdb.std.Misc;
import io.questdb.std.ObjList;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;


public class GroupByUtils {

    public static Function createColumnFunction(
            @Nullable RecordMetadata metadata,
            int keyColumnIndex,
            int type,
            int index
    ) {
        final Function func;
        switch (ColumnType.tagOf(type)) {
            case ColumnType.BOOLEAN:
                func = BooleanColumn.newInstance(keyColumnIndex - 1);
                break;
            case ColumnType.BYTE:
                func = ByteColumn.newInstance(keyColumnIndex - 1);
                break;
            case ColumnType.SHORT:
                func = ShortColumn.newInstance(keyColumnIndex - 1);
                break;
            case ColumnType.CHAR:
                func = new CharColumn(keyColumnIndex - 1);
                break;
            case ColumnType.INT:
                func = IntColumn.newInstance(keyColumnIndex - 1);
                break;
            case ColumnType.IPv4:
                func = new IPv4Column(keyColumnIndex - 1);
                break;
            case ColumnType.LONG:
                func = LongColumn.newInstance(keyColumnIndex - 1);
                break;
            case ColumnType.FLOAT:
                func = FloatColumn.newInstance(keyColumnIndex - 1);
                break;
            case ColumnType.DOUBLE:
                func = DoubleColumn.newInstance(keyColumnIndex - 1);
                break;
            case ColumnType.STRING:
                func = new StrColumn(keyColumnIndex - 1);
                break;
            case ColumnType.VARCHAR:
                func = new VarcharColumn(keyColumnIndex - 1);
                break;
            case ColumnType.SYMBOL:
                if (metadata != null) {
                    // must be a column key
                    func = new MapSymbolColumn(keyColumnIndex - 1, index, metadata.isSymbolTableStatic(index));
                } else {
                    // must be a function key, so we treat symbols as strings
                    func = new StrColumn(keyColumnIndex - 1);
                }
                break;
            case ColumnType.DATE:
                func = DateColumn.newInstance(keyColumnIndex - 1);
                break;
            case ColumnType.TIMESTAMP:
                func = TimestampColumn.newInstance(keyColumnIndex - 1, type);
                break;
            case ColumnType.LONG256:
                func = Long256Column.newInstance(keyColumnIndex - 1);
                break;
            case ColumnType.GEOBYTE:
                func = GeoByteColumn.newInstance(keyColumnIndex - 1, type);
                break;
            case ColumnType.GEOSHORT:
                func = GeoShortColumn.newInstance(keyColumnIndex - 1, type);
                break;
            case ColumnType.GEOINT:
                func = GeoIntColumn.newInstance(keyColumnIndex - 1, type);
                break;
            case ColumnType.GEOLONG:
                func = GeoLongColumn.newInstance(keyColumnIndex - 1, type);
                break;
            case ColumnType.LONG128:
                func = Long128Column.newInstance(keyColumnIndex - 1);
                break;
            case ColumnType.UUID:
                func = UuidColumn.newInstance(keyColumnIndex - 1);
                break;
            case ColumnType.INTERVAL:
                func = IntervalColumn.newInstance(keyColumnIndex - 1, type);
                break;
            case ColumnType.ARRAY:
                func = new ArrayColumn(keyColumnIndex - 1, type);
                break;
            case ColumnType.DECIMAL8:
            case ColumnType.DECIMAL16:
            case ColumnType.DECIMAL32:
            case ColumnType.DECIMAL64:
            case ColumnType.DECIMAL128:
            case ColumnType.DECIMAL256:
                func = DecimalColumn.newInstance(keyColumnIndex - 1, type);
                break;
            default:
                func = BinColumn.newInstance(keyColumnIndex - 1);
                break;
        }
        return func;
    }

    /**
     * Returns the page-frame column index when {@code arg} is a direct
     * {@link ColumnFunction} reference whose native storage matches
     * {@code expectedType}, allowing the batched GROUP BY fast path to read
     * values straight from page-frame memory. Returns -1 otherwise, signalling
     * that the caller must fall back to {@code arg.getXxx(record)}.
     * <p>
     * The type check guards against silent type reinterpretation: a function
     * like {@code avg(long_col)} keeps the raw {@code LongColumn} as its arg
     * (no explicit cast, because {@link io.questdb.griffin.engine.functions.LongFunction#getDouble}
     * already widens), but reading that column's 8-byte storage as a
     * {@code double} would produce meaningless denormal values.
     */
    public static int directArgColumnIndex(Function arg, int expectedType) {
        if (arg instanceof ColumnFunction cf && arg.getType() == expectedType) {
            return cf.getColumnIndex();
        }
        return -1;
    }

    /**
     * Variant of {@link #directArgColumnIndex} that matches by column type tag
     * rather than by full type. Useful for parameterised types such as geohashes
     * whose full {@code arg.getType()} value packs storage bits into the upper
     * half and so never equals a bare tag like {@link ColumnType#GEOBYTE}.
     */
    public static int directArgColumnIndexByTag(Function arg, int expectedTag) {
        if (arg instanceof ColumnFunction cf && ColumnType.tagOf(arg.getType()) == expectedTag) {
            return cf.getColumnIndex();
        }
        return -1;
    }

    /**
     * Builds a borrowed list of the non-group-by entries of {@code recordFunctions}. Cursors
     * call this once at construction, so every subsequent cached execution initializes the
     * non-group-by functions with a plain Theta(V) walk instead of re-classifying all
     * P record functions with instanceof checks. Ownership stays with {@code recordFunctions}:
     * callers must never close the returned list's entries.
     */
    public static ObjList<Function> extractNonGroupByFunctions(ObjList<Function> recordFunctions) {
        final ObjList<Function> nonGroupByFunctions = new ObjList<>(recordFunctions.size());
        for (int i = 0, n = recordFunctions.size(); i < n; i++) {
            final Function function = recordFunctions.getQuick(i);
            if (!(function instanceof GroupByFunction)) {
                nonGroupByFunctions.add(function);
            }
        }
        return nonGroupByFunctions;
    }

    /**
     * Frees the projection functions produced by {@link #assembleGroupByFunctions} exactly once
     * when generation fails after assembly, in Theta(outer + inner) time and constant space by
     * walking the producer's positional correspondence. The first assembly loop adds each parsed
     * Function to both lists, so paired slots share references; the timestamp column appends null
     * to outer and nothing to inner, so a null outer slot consumes no inner slot; the key-rewrite
     * loop may replace an outer entry with a column-ref Function, leaving the original parsed
     * Function reachable only through its paired inner slot; a mid-assembly failure can leave
     * the last non-null outer entry without an inner counterpart, never the reverse. Every
     * non-null outer entry is freed, and a paired inner entry is freed only when it is not the
     * same reference. Closing the same Function twice would underflow allocator counters, so
     * callers must not additionally free the group-by function list - its entries are aliased in
     * the outer list and are already closed by this call.
     * <p>
     * The walk is best-effort: a throwing close() does not stop it. Every function still sees
     * exactly one close attempt, both lists end up cleared, and the first failure rethrows once
     * the walk completes, with later failures attached to it as suppressed. Callers already
     * holding a primary exception must catch the rethrown failure and suppress it themselves.
     */
    public static void freeAssembledProjectionFunctions(
            @Nullable ObjList<Function> outerProjectionFunctions,
            @Nullable ObjList<Function> innerProjectionFunctions
    ) {
        if (outerProjectionFunctions == null) {
            if (innerProjectionFunctions != null) {
                final Throwable failure = Misc.freeObjListBestEffort(null, innerProjectionFunctions);
                innerProjectionFunctions.clear();
                CairoException.rethrowCleanupFailure(failure);
            }
            return;
        }
        final int innerSize = innerProjectionFunctions != null ? innerProjectionFunctions.size() : 0;
        Throwable failure = null;
        int j = 0;
        for (int i = 0, n = outerProjectionFunctions.size(); i < n; i++) {
            final Function outerFunc = outerProjectionFunctions.getQuick(i);
            if (outerFunc == null) {
                // timestamp placeholder: assembly added no inner counterpart
                continue;
            }
            failure = Misc.freeBestEffort(failure, outerFunc);
            if (j < innerSize) {
                final Function innerFunc = innerProjectionFunctions.getQuick(j);
                if (innerFunc != outerFunc) {
                    // the key rewrite replaced the outer entry; the parsed original is
                    // reachable only through this inner slot
                    failure = Misc.freeBestEffort(failure, innerFunc);
                }
                j++;
            }
        }
        // Full assembly pairs every inner entry; a mid-assembly failure leaves at most the last
        // non-null outer entry unpaired. Either way the walk must consume the whole inner list.
        assert j == innerSize;
        if (innerProjectionFunctions != null) {
            innerProjectionFunctions.clear();
        }
        outerProjectionFunctions.clear();
        CairoException.rethrowCleanupFailure(failure);
    }

    /**
     * Variant of {@link #freeAssembledProjectionFunctions(ObjList, ObjList)} for cleanup paths
     * that already hold a primary exception: it attaches any failure from the best-effort walk
     * to the primary as suppressed instead of letting the failure propagate and mask it.
     */
    public static void freeAssembledProjectionFunctions(
            @Nullable ObjList<Function> outerProjectionFunctions,
            @Nullable ObjList<Function> innerProjectionFunctions,
            @NotNull Throwable primary
    ) {
        try {
            freeAssembledProjectionFunctions(outerProjectionFunctions, innerProjectionFunctions);
        } catch (Throwable th) {
            if (th != primary) {
                primary.addSuppressed(th);
            }
        }
    }

    public static CharSequence getUnsupportedSampleByFill(GroupByFunction function, CharSequence token) {
        final int flags = function.getSampleByFlags();
        assert (flags & GroupByFunction.SAMPLE_BY_FILL_NONE) != 0 :
                "aggregate must support FILL(NONE): " + function.getClass().getName();
        if (SqlKeywords.isNullKeyword(token)) {
            return (flags & GroupByFunction.SAMPLE_BY_FILL_NULL) == 0 ? "NULL" : null;
        }
        if (SqlKeywords.isPrevKeyword(token)) {
            return (flags & GroupByFunction.SAMPLE_BY_FILL_PREVIOUS) == 0 ? "PREV" : null;
        }
        if (SqlKeywords.isLinearKeyword(token)) {
            return (flags & GroupByFunction.SAMPLE_BY_FILL_LINEAR) == 0 ? "LINEAR" : null;
        }
        return !SqlKeywords.isNoneKeyword(token) && (flags & GroupByFunction.SAMPLE_BY_FILL_VALUE) == 0 ? "VALUE" : null;
    }

    public static SqlException invalidSampleByFillValue(CharSequence fillToken, int fillPosition) {
        return SqlException.position(fillPosition).put("invalid fill value: ").put(fillToken);
    }

    public static boolean isEarlyExitSupported(ObjList<GroupByFunction> functions) {
        for (int i = 0, n = functions.size(); i < n; i++) {
            if (!functions.getQuick(i).isEarlyExitSupported()) {
                return false;
            }
        }
        return true;
    }

    public static boolean isParallelismSupported(ObjList<GroupByFunction> functions) {
        for (int i = 0, n = functions.size(); i < n; i++) {
            if (!functions.getQuick(i).supportsParallelism()) {
                return false;
            }
        }
        return true;
    }

    public static void setAllocator(ObjList<GroupByFunction> functions, GroupByAllocator allocator) {
        if (functions instanceof PerWorkerFunctionList<?> perWorkerFunctions) {
            // The list tracks worker-owned clones positionally, so iterate the owned bits
            // directly instead of scanning every retained function: the common fully borrowed
            // per-worker list costs a single probe instead of one probe per function.
            for (int i = perWorkerFunctions.nextOwned(0); i > -1; i = perWorkerFunctions.nextOwned(i + 1)) {
                functions.getQuick(i).setAllocator(allocator);
            }
        } else {
            for (int i = 0, n = functions.size(); i < n; i++) {
                functions.getQuick(i).setAllocator(allocator);
            }
        }
    }

    public static void toTop(ObjList<? extends Function> args) {
        PerWorkerFunctionList.toTop(args);
    }

    public static void validateTimestampOrder(
            GroupByFunction function,
            int timestampIndex,
            boolean isBaseTimestampAscending,
            int position
    ) throws SqlException {
        if (function instanceof TwapGroupByFunction twap) {
            twap.validateTimestampArg(timestampIndex, isBaseTimestampAscending, position);
        } else if (function instanceof SparklineGroupByFunction sparkline) {
            sparkline.validateScanDirection(isBaseTimestampAscending, position);
        }
    }

}
