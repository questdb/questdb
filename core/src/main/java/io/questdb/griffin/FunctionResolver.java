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

package io.questdb.griffin;

import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.ImplicitCastException;
import io.questdb.cairo.MillisTimestampDriver;
import io.questdb.cairo.arr.ArrayView;
import io.questdb.cairo.arr.FunctionArray;
import io.questdb.cairo.sql.BindVariableService;
import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.RecordMetadata;
import io.questdb.griffin.engine.functions.GroupByFunction;
import io.questdb.griffin.engine.functions.RuntimeConstFunction;
import io.questdb.griffin.engine.functions.bind.IndexedParameterLinkFunction;
import io.questdb.griffin.engine.functions.bind.NamedParameterLinkFunction;
import io.questdb.griffin.engine.functions.bool.BooleanSubQueryFunction;
import io.questdb.griffin.engine.functions.cast.CastByteToDecimalFunctionFactory;
import io.questdb.griffin.engine.functions.cast.CastCharToSymbolFunctionFactory;
import io.questdb.griffin.engine.functions.cast.CastGeoHashToGeoHashFunctionFactory;
import io.questdb.griffin.engine.functions.cast.CastIPv4ToStrFunctionFactory;
import io.questdb.griffin.engine.functions.cast.CastIPv4ToVarcharFunctionFactory;
import io.questdb.griffin.engine.functions.cast.CastIntToDecimalFunctionFactory;
import io.questdb.griffin.engine.functions.cast.CastIntervalToStrFunctionFactory;
import io.questdb.griffin.engine.functions.cast.CastLongToDecimalFunctionFactory;
import io.questdb.griffin.engine.functions.cast.CastShortToDecimalFunctionFactory;
import io.questdb.griffin.engine.functions.cast.CastStrToDecimalFunctionFactory;
import io.questdb.griffin.engine.functions.cast.CastStrToDoubleArrayFunctionFactory;
import io.questdb.griffin.engine.functions.cast.CastStrToGeoHashFunctionFactory;
import io.questdb.griffin.engine.functions.cast.CastStrToTimestampFunctionFactory;
import io.questdb.griffin.engine.functions.cast.CastStrToUuidFunctionFactory;
import io.questdb.griffin.engine.functions.cast.CastUuidToStrFunctionFactory;
import io.questdb.griffin.engine.functions.cast.CastUuidToVarcharFunctionFactory;
import io.questdb.griffin.engine.functions.cast.CastVarcharToDecimalFunctionFactory;
import io.questdb.griffin.engine.functions.cast.CastVarcharToGeoHashFunctionFactory;
import io.questdb.griffin.engine.functions.cast.CastVarcharToTimestampFunctionFactory;
import io.questdb.griffin.engine.functions.cast.CastVarcharToUuidFunctionFactory;
import io.questdb.griffin.engine.functions.columns.ArrayColumn;
import io.questdb.griffin.engine.functions.columns.BinColumn;
import io.questdb.griffin.engine.functions.columns.BooleanColumn;
import io.questdb.griffin.engine.functions.columns.ByteColumn;
import io.questdb.griffin.engine.functions.columns.CharColumn;
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
import io.questdb.griffin.engine.functions.columns.RecordColumn;
import io.questdb.griffin.engine.functions.columns.ShortColumn;
import io.questdb.griffin.engine.functions.columns.StrColumn;
import io.questdb.griffin.engine.functions.columns.SymbolColumn;
import io.questdb.griffin.engine.functions.columns.TimestampColumn;
import io.questdb.griffin.engine.functions.columns.UuidColumn;
import io.questdb.griffin.engine.functions.columns.VarcharColumn;
import io.questdb.griffin.engine.functions.constants.ArrayConstant;
import io.questdb.griffin.engine.functions.constants.BooleanConstant;
import io.questdb.griffin.engine.functions.constants.ByteConstant;
import io.questdb.griffin.engine.functions.constants.CharConstant;
import io.questdb.griffin.engine.functions.constants.CharTypeConstant;
import io.questdb.griffin.engine.functions.constants.ConstantFunction;
import io.questdb.griffin.engine.functions.constants.Constants;
import io.questdb.griffin.engine.functions.constants.DateConstant;
import io.questdb.griffin.engine.functions.constants.Decimal128Constant;
import io.questdb.griffin.engine.functions.constants.Decimal16Constant;
import io.questdb.griffin.engine.functions.constants.Decimal256Constant;
import io.questdb.griffin.engine.functions.constants.Decimal32Constant;
import io.questdb.griffin.engine.functions.constants.Decimal64Constant;
import io.questdb.griffin.engine.functions.constants.Decimal8Constant;
import io.questdb.griffin.engine.functions.constants.DecimalTypeConstant;
import io.questdb.griffin.engine.functions.constants.DoubleConstant;
import io.questdb.griffin.engine.functions.constants.FloatConstant;
import io.questdb.griffin.engine.functions.constants.GeoByteConstant;
import io.questdb.griffin.engine.functions.constants.GeoHashTypeConstant;
import io.questdb.griffin.engine.functions.constants.GeoIntConstant;
import io.questdb.griffin.engine.functions.constants.GeoLongConstant;
import io.questdb.griffin.engine.functions.constants.GeoShortConstant;
import io.questdb.griffin.engine.functions.constants.IPv4Constant;
import io.questdb.griffin.engine.functions.constants.IntConstant;
import io.questdb.griffin.engine.functions.constants.Long256Constant;
import io.questdb.griffin.engine.functions.constants.LongConstant;
import io.questdb.griffin.engine.functions.constants.NullConstant;
import io.questdb.griffin.engine.functions.constants.ShortConstant;
import io.questdb.griffin.engine.functions.constants.StrConstant;
import io.questdb.griffin.engine.functions.constants.SymbolConstant;
import io.questdb.griffin.engine.functions.constants.TimestampConstant;
import io.questdb.griffin.engine.functions.constants.UuidConstant;
import io.questdb.griffin.engine.functions.constants.VarcharConstant;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.std.Chars;
import io.questdb.std.Decimal128;
import io.questdb.std.Decimal256;
import io.questdb.std.Decimals;
import io.questdb.std.GenericLexer;
import io.questdb.std.IntList;
import io.questdb.std.Long256Impl;
import io.questdb.std.Misc;
import io.questdb.std.Mutable;
import io.questdb.std.Numbers;
import io.questdb.std.NumericException;
import io.questdb.std.ObjList;
import io.questdb.std.Transient;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import static io.questdb.griffin.SqlKeywords.*;

/**
 * Resolves and constructs functions for every consumer that builds them: literal constants, bind variables and
 * calls. A call resolves in phases over its already-built arguments - {@link #resolveCast}, then
 * {@link #selectOverload}, then {@link #coerceArguments} - and is then constructed by {@link #createFunction} or
 * admitted unconstructed by {@link #admitUnconstructed}. Arguments are functions: constants, columns, and the
 * placeholders of calls left unconstructed. Every phase but {@link #resolveCast} releases the arguments when it
 * fails.
 */
public class FunctionResolver implements Mutable {
    private static final boolean ASSERTIONS_ENABLED = FunctionResolver.class.desiredAssertionStatus();
    private static final Log LOG = LogFactory.getLog(FunctionResolver.class);
    private final IntList argTraits = new IntList();
    private final IntList argTypes = new IntList();
    private final CairoConfiguration configuration;
    private final SqlExecutionRequirements executionRequirements = new SqlExecutionRequirements();
    private final FunctionFactoryCache functionFactoryCache;
    private final ObjList<Class<? extends FunctionFactory>> implicitConversions = new ObjList<>();
    private final Long256Impl long256Sink = new Long256Impl();
    private final IntList undefinedVariables = new IntList();
    private boolean cursorFunctionInstantiated;
    private int executionRequirementPosition = -1;

    public FunctionResolver(CairoConfiguration configuration, FunctionFactoryCache functionFactoryCache) {
        this.configuration = configuration;
        this.functionFactoryCache = functionFactoryCache;
    }

    @NotNull
    public static Function createColumn(
            int position,
            CharSequence name,
            RecordMetadata metadata
    ) throws SqlException {
        final int index = SqlUtil.getColumnIndexQuiet(metadata, name);

        if (index == -1) {
            throw SqlException.invalidColumn(position, name);
        }

        return createColumn(position, index, metadata);
    }

    @NotNull
    public static Function createColumn(int position, int index, RecordMetadata metadata) throws SqlException {
        final int columnType = metadata.getColumnType(index);
        if (ColumnType.tagOf(columnType) == ColumnType.RECORD) {
            return new RecordColumn(index, metadata.getMetadata(index));
        }
        return createColumn(position, index, columnType, ColumnType.isSymbol(columnType) && metadata.isSymbolTableStatic(index));
    }

    @NotNull
    public static Function createColumn(int position, int index, int columnType, boolean isSymbolTableStatic) throws SqlException {
        return switch (ColumnType.tagOf(columnType)) {
            case ColumnType.BOOLEAN -> BooleanColumn.newInstance(index);
            case ColumnType.BYTE -> ByteColumn.newInstance(index);
            case ColumnType.SHORT -> ShortColumn.newInstance(index);
            case ColumnType.CHAR -> new CharColumn(index);
            case ColumnType.INT -> IntColumn.newInstance(index);
            case ColumnType.LONG -> LongColumn.newInstance(index);
            case ColumnType.FLOAT -> FloatColumn.newInstance(index);
            case ColumnType.DOUBLE -> DoubleColumn.newInstance(index);
            case ColumnType.STRING ->
                // we cannot use a pooled StrColumn instance, because it is not thread-safe
                    new StrColumn(index);
            case ColumnType.VARCHAR, ColumnType.VARCHAR_SLICE ->
                // we cannot use a pooled VarcharColumn instance, because it is not thread-safe
                    new VarcharColumn(index);
            case ColumnType.SYMBOL -> new SymbolColumn(index, isSymbolTableStatic);
            case ColumnType.BINARY -> BinColumn.newInstance(index);
            case ColumnType.DATE -> DateColumn.newInstance(index);
            case ColumnType.TIMESTAMP -> TimestampColumn.newInstance(index, columnType);
            case ColumnType.GEOBYTE -> GeoByteColumn.newInstance(index, columnType);
            case ColumnType.GEOSHORT -> GeoShortColumn.newInstance(index, columnType);
            case ColumnType.GEOINT -> GeoIntColumn.newInstance(index, columnType);
            case ColumnType.GEOLONG -> GeoLongColumn.newInstance(index, columnType);
            case ColumnType.NULL -> NullConstant.NULL;
            case ColumnType.LONG256 -> Long256Column.newInstance(index);
            case ColumnType.LONG128 -> Long128Column.newInstance(index);
            case ColumnType.UUID -> UuidColumn.newInstance(index);
            case ColumnType.IPv4 -> IPv4Column.newInstance(index);
            case ColumnType.INTERVAL -> IntervalColumn.newInstance(index, columnType);
            case ColumnType.ARRAY -> new ArrayColumn(index, columnType);
            case ColumnType.DECIMAL8, ColumnType.DECIMAL16, ColumnType.DECIMAL32, ColumnType.DECIMAL64,
                 ColumnType.DECIMAL128, ColumnType.DECIMAL256 -> new DecimalColumn(index, columnType);
            default -> throw SqlException.position(position)
                    .put("unsupported column type ")
                    .put(ColumnType.nameOf(columnType));
        };
    }

    /**
     * Determines the appropriate timestamp type based on the string precision and year range.
     * If the string contains nanosecond precision (more than 6 digits after seconds) and
     * the year is within nano timestamp range (&lt; 2262), returns nano type;
     * otherwise returns the original signature type.
     *
     * @param timestampStr the timestamp string to analyze
     * @param sigArgType   the original signature argument type
     * @return adaptive timestamp type (nano if detected and within range, otherwise original)
     */
    public static int getAdaptiveTimestampType(CharSequence timestampStr, int sigArgType) {
        if (timestampStr == null || timestampStr.isEmpty()) {
            return FunctionFactoryDescriptor.toType(sigArgType);
        }

        // Extract year from timestamp string to check nano range
        if (isBeyondNanoRange(extractYearFromTimestamp(timestampStr))) {
            return FunctionFactoryDescriptor.toType(sigArgType);
        }

        // Look for fractional seconds part after last '.' or ':'
        int lastDot = -1;
        for (int i = timestampStr.length() - 1; i >= 0; i--) {
            char c = timestampStr.charAt(i);
            if (c == '.' || c == ':') {
                lastDot = i;
                break;
            }
            // Stop if we hit a space or non-digit (except for timezone indicators)
            if (c == ' ' || c == 'T' || c == '+' || c == '-') {
                break;
            }
        }

        if (lastDot >= 0 && lastDot < timestampStr.length() - 1) {
            // Count digits after the dot/colon until we hit non-digit
            int digitCount = 0;
            for (int i = lastDot + 1; i < timestampStr.length(); i++) {
                char c = timestampStr.charAt(i);
                if (c >= '0' && c <= '9') {
                    digitCount++;
                } else {
                    break; // Stop at timezone or other non-digit characters
                }
            }

            // If more than 6 digits (microsecond precision) and within nano range, use nanosecond type
            if (digitCount > 6) {
                return ColumnType.TIMESTAMP_NANO;
            }
        }

        return FunctionFactoryDescriptor.toType(sigArgType); // Use original signature type
    }

    /**
     * Whether a timestamp in the given year cannot be represented at nanosecond precision.
     */
    public static boolean isBeyondNanoRange(int year) {
        return year >= 2262;
    }

    /**
     * Rejects an aggregate passed as the argument at the given index; the error releases every argument.
     */
    public static void rejectAggregateArgument(ObjList<Function> args, int index, int position) throws SqlException {
        if (args.getQuick(index) instanceof GroupByFunction) {
            final SqlException ex = SqlException.position(position).put("Aggregate function cannot be passed as an argument");
            Misc.freeObjList(args, ex);
            throw ex;
        }
    }

    /**
     * The constant TIMESTAMP text converts to for a TIMESTAMP parameter: at nanosecond precision when the text has
     * it, otherwise at the parameter's precision.
     */
    public static Function timestampConstant(CharSequence text, int sigArgType) throws NumericException {
        final int adaptiveType = getAdaptiveTimestampType(text, sigArgType);
        return TimestampConstant.newInstance(ColumnType.getTimestampDriver(adaptiveType).parseFloorLiteral(text), adaptiveType);
    }

    /**
     * Wraps each runtime-constant argument of a call that is not itself runtime constant, so the maximal
     * runtime-constant subtree evaluates once per cursor, not per row. A call is runtime constant when every
     * argument is constant or runtime constant and at least one is runtime constant, so only the topmost node of
     * such a subtree is wrapped.
     */
    public static void wrapRuntimeConstants(ObjList<Function> args) {
        boolean allConstOrRuntimeConst = true;
        boolean anyRuntimeConst = false;
        for (int i = 0, n = args.size(); i < n; i++) {
            final Function arg = args.getQuick(i);
            final boolean isRuntimeConstant = arg != null && arg.isRuntimeConstant();
            if (arg == null || (!isRuntimeConstant && !arg.isConstant())) {
                allConstOrRuntimeConst = false;
            } else if (isRuntimeConstant) {
                // a function is never both constant and runtime constant (see Function.isRuntimeConstant)
                anyRuntimeConst = true;
            }
        }
        if (!(allConstOrRuntimeConst && anyRuntimeConst)) {
            for (int i = 0, n = args.size(); i < n; i++) {
                final Function arg = args.getQuick(i);
                if (RuntimeConstFunction.isFoldable(arg)) {
                    args.setQuick(i, RuntimeConstFunction.newInstance(arg));
                }
            }
        }
    }

    /**
     * Applies, in construction order, the checks constructing the overload would for a call the binder types
     * without constructing it: the admission checks, then the factory's checks of its arguments
     * ({@link FunctionFactory#isConstructionDeferrable}), then records the execution requirements. Returns false,
     * recording nothing, when the call must be constructed now. The caller keeps ownership of the arguments.
     */
    public boolean admitUnconstructed(
            FunctionFactoryDescriptor overload,
            int position,
            CharSequence name,
            @Transient ObjList<Function> args,
            @Transient IntList argPositions,
            SqlExecutionContext executionContext
    ) throws SqlException {
        final FunctionFactory factory = overload.getFactory();
        final SqlException rejection = rejectAdministrativeFunction(factory, position, name, executionContext);
        if (rejection != null) {
            throw rejection;
        }
        final boolean isDeferrable;
        try {
            isDeferrable = factory.isConstructionDeferrable(position, args, argPositions, configuration);
        } catch (SqlException | ImplicitCastException e) {
            throw e;
        } catch (Throwable e) {
            LOG.error().$("exception in function factory: ").$(e).$();
            throw SqlException.position(position).put("exception in function factory: ").put(e.getMessage());
        }
        if (!isDeferrable) {
            return false;
        }
        addExecutionRequirements(factory, position, name);
        return true;
    }

    @Override
    public void clear() {
        executionRequirements.clear();
        executionRequirementPosition = -1;
        cursorFunctionInstantiated = false;
    }

    /**
     * Applies the selected overload's argument coercions in place: the constant variadic check, the types of
     * untyped bind variables (a side effect on the bind variable service, in argument order), NULL substitution,
     * the conversion of constant text to TIMESTAMP and DATE, and implicit casts, recorded for
     * {@link #getImplicitConversion}. Releases the arguments on failure.
     */
    public void coerceArguments(
            FunctionFactoryDescriptor candidateDescriptor,
            int position,
            CharSequence name,
            @Nullable @Transient ObjList<Function> args,
            @Nullable @Transient IntList argPositions,
            SqlExecutionContext sqlExecutionContext
    ) throws SqlException {
        final int argCount = args == null ? 0 : args.size();
        implicitConversions.clear();
        implicitConversions.setPos(argCount);
        undefinedVariables.clear();
        // find all undefined args for the purpose of setting
        // their types when we find suitable candidate function
        for (int i = 0; i < argCount; i++) {
            if (args.getQuick(i).isUndefined()) {
                undefinedVariables.add(i);
            }
        }

        final FunctionFactory candidate = candidateDescriptor.getFactory();
        int candidateSigArgCount = candidateDescriptor.getSigArgCount();
        boolean candidateSigVarArgConst = false;
        if (candidateSigArgCount > 0) {
            final int lastSigArgTypeWithFlags = candidateDescriptor.getArgTypeWithFlags(candidateSigArgCount - 1);
            if (FunctionFactoryDescriptor.toTypeTag(lastSigArgTypeWithFlags) == ColumnType.VAR_ARG) {
                candidateSigArgCount--;
                candidateSigVarArgConst = FunctionFactoryDescriptor.isConstant(lastSigArgTypeWithFlags);
            }
        }

        try {
            if (candidateSigVarArgConst) {
                for (int k = candidateSigArgCount; k < argCount; k++) {
                    Function func = args.getQuick(k);
                    if (!(func.isConstant() || func.isRuntimeConstant())) {
                        assert argPositions != null;
                        final SqlException ex = SqlException.$(argPositions.getQuick(k), "constant expected");
                        Misc.freeObjList(args, ex);
                        throw ex;
                    }
                }
            }
            // resolve previously UNDEFINED function types
            for (int i = 0, n = undefinedVariables.size(); i < n; i++) {
                final int pos = undefinedVariables.getQuick(i);
                if (pos < candidateSigArgCount) {
                    // assign arguments based on the candidate function descriptor
                    int t = candidateDescriptor.getArgTypeWithFlags(pos);
                    final short sigArgType = FunctionFactoryDescriptor.toTypeTag(t);
                    final int argType;
                    if (FunctionFactoryDescriptor.isArray(t)) {
                        // allow varchar array only if the element type in signature is varchar
                        argType = ColumnType.encodeArrayTypeWithWeakDims(sigArgType, sigArgType != ColumnType.VARCHAR);
                    } else {
                        argType = sigArgType;
                    }
                    assert args != null;
                    args.getQuick(pos).assignType(argType, sqlExecutionContext.getBindVariableService());
                } else {
                    // in case of vararg it is possible that we have more undefined variables than args in the function descriptor,
                    // assign type to all remaining undefined variables based on the preference of the candidate function factory
                    assert argPositions != null;
                    int type = candidate.resolvePreferredVariadicType(argPositions.getQuick(pos), pos, args);
                    assert args != null;
                    args.getQuick(pos).assignType(type, sqlExecutionContext.getBindVariableService());
                }
            }

            for (int k = 0; k < candidateSigArgCount; k++) {
                assert args != null;
                final Function arg = args.getQuick(k);
                final int sigArgType = candidateDescriptor.getArgTypeWithFlags(k);
                final short sigArgTypeTag = FunctionFactoryDescriptor.toTypeTag(sigArgType);
                final short argTypeTag = ColumnType.tagOf(arg.getType());

                if (argTypeTag == ColumnType.DOUBLE && arg.isConstant() && Numbers.isNull(arg.getDouble(null))) {
                    // substitute NaNs with appropriate types
                    if (sigArgTypeTag == ColumnType.LONG) {
                        args.setQuick(k, LongConstant.NULL);
                    } else if (sigArgTypeTag == ColumnType.INT) {
                        args.setQuick(k, IntConstant.NULL);
                    }
                } else if ((argTypeTag == ColumnType.STRING || argTypeTag == ColumnType.SYMBOL || argTypeTag == ColumnType.VARCHAR) && arg.isConstant()) {
                    if (sigArgTypeTag == ColumnType.TIMESTAMP) {
                        final CharSequence timestampStr = arg.getStrA(null);
                        try {
                            args.set(k, timestampConstant(timestampStr, sigArgType));
                        } catch (NumericException e) {
                            assert argPositions != null;
                            throw SqlException.invalidDate(timestampStr, argPositions.getQuick(k));
                        }
                    } else if (sigArgTypeTag == ColumnType.DATE) {
                        assert argPositions != null;
                        int pos = argPositions.getQuick(k);
                        long millis = parseDate(arg.getStrA(null), pos);
                        args.set(k, DateConstant.newInstance(millis));
                    }
                } else if (argTypeTag == ColumnType.UUID && sigArgTypeTag == ColumnType.STRING) {
                    args.setQuick(k, new CastUuidToStrFunctionFactory.Func(arg));
                    implicitConversions.setQuick(k, CastUuidToStrFunctionFactory.class);
                } else if (argTypeTag == ColumnType.IPv4 && sigArgTypeTag == ColumnType.STRING) {
                    args.setQuick(k, new CastIPv4ToStrFunctionFactory.Func(arg));
                    implicitConversions.setQuick(k, CastIPv4ToStrFunctionFactory.class);
                } else if (argTypeTag == ColumnType.INTERVAL && sigArgTypeTag == ColumnType.STRING) {
                    args.setQuick(k, new CastIntervalToStrFunctionFactory.Func(arg));
                    implicitConversions.setQuick(k, CastIntervalToStrFunctionFactory.class);
                } else if (argTypeTag == ColumnType.INT && sigArgTypeTag == ColumnType.DECIMAL) {
                    assert argPositions != null;
                    args.setQuick(k, CastIntToDecimalFunctionFactory.newInstance(argPositions.getQuick(k), arg, sqlExecutionContext));
                    implicitConversions.setQuick(k, CastIntToDecimalFunctionFactory.class);
                } else if (argTypeTag == ColumnType.LONG && sigArgTypeTag == ColumnType.DECIMAL) {
                    assert argPositions != null;
                    args.setQuick(k, CastLongToDecimalFunctionFactory.newInstance(argPositions.getQuick(k), arg, sqlExecutionContext.getDecimal256()));
                    implicitConversions.setQuick(k, CastLongToDecimalFunctionFactory.class);
                } else if (argTypeTag == ColumnType.SHORT && sigArgTypeTag == ColumnType.DECIMAL) {
                    assert argPositions != null;
                    args.setQuick(k, CastShortToDecimalFunctionFactory.newInstance(argPositions.getQuick(k), arg, sqlExecutionContext));
                    implicitConversions.setQuick(k, CastShortToDecimalFunctionFactory.class);
                } else if (argTypeTag == ColumnType.BYTE && sigArgTypeTag == ColumnType.DECIMAL) {
                    assert argPositions != null;
                    args.setQuick(k, CastByteToDecimalFunctionFactory.newInstance(argPositions.getQuick(k), arg, sqlExecutionContext));
                    implicitConversions.setQuick(k, CastByteToDecimalFunctionFactory.class);
                }
            }
        } catch (Throwable th) {
            Misc.freeObjList(args, th);
            throw th;
        }
        // An untyped NULL literal as the value argument of a polymorphic window function (lead, min,
        // sum, nth_value, ...) is ambiguous: it ties across every typed variant (NULL to any type has
        // zero overload distance), so the winner - and the resulting behaviour - depends on classpath
        // scan order. Different winners give different observable behaviour: a clean rejection on one
        // platform, a "not yet implemented for NULL" factory error on another, or even silent acceptance
        // returning NULLs. Reject it deterministically here, before any factory runs, so the user gets
        // the same clear "cast it" error everywhere. Window functions with a single overload (e.g. ntile,
        // whose argument is a bucket count rather than a value) resolve deterministically and keep their
        // own argument validation.
        if (!sqlExecutionContext.getWindowContext().isEmpty() && candidate.isWindow() && argCount > 0
                && ColumnType.tagOf(args.getQuick(0).getType()) == ColumnType.NULL
                && countWindowOverloads(functionFactoryCache.getOverloadList(name)) > 1) {
            final SqlException ex = SqlException.$(position, "window function ").put(name).put(" does not support an untyped NULL argument; cast it to a concrete type, e.g. null::double");
            Misc.freeObjList(args, ex);
            throw ex;
        }
    }

    /**
     * The bind variable a {@code :name} or {@code $index} token refers to; an index parameter the service does not
     * define yet is untyped.
     */
    public Function createBindVariable(int position, CharSequence name, SqlExecutionContext executionContext) throws SqlException {
        if (name.charAt(0) != ':') {
            return parseIndexedParameter(position, name, executionContext);
        }
        return createNamedParameter(position, name, executionContext);
    }

    /**
     * The constant a SQL literal token spells, including the type constants of CAST targets.
     */
    public Function createConstant(int position, final CharSequence tok, SqlExecutionContext executionContext) throws SqlException {
        final int len = tok.length();

        if (isNullKeyword(tok) || isNanKeyword(tok)) {
            return NullConstant.NULL;
        }

        if (Chars.isQuoted(tok)) {
            return switch (len) {
                case 3 -> // this is 'x' - char
                        CharConstant.newInstance(tok.charAt(1));
                case 2 -> // this is '' - char
                        StrConstant.EMPTY;
                default -> new StrConstant(tok);
            };
        }

        // special case E'str' - we treat it like normal string for now
        if (len > 2 && tok.charAt(0) == 'E' && tok.charAt(1) == '\'' && tok.charAt(len - 1) == '\'') {
            return new StrConstant(Chars.toString(tok, 2, len - 1));
        }

        if (SqlKeywords.isTrueKeyword(tok)) {
            return BooleanConstant.TRUE;
        }

        if (SqlKeywords.isFalseKeyword(tok)) {
            return BooleanConstant.FALSE;
        }

        try {
            return IntConstant.newInstance(Numbers.parseInt(tok));
        } catch (NumericException ignore) {
        }

        try {
            return LongConstant.newInstance(Numbers.parseLong(tok));
        } catch (NumericException ignore) {
        }

        try {
            return DoubleConstant.newInstance(Numbers.parseDouble(tok));
        } catch (NumericException ignore) {
        }

        try {
            return FloatConstant.newInstance(Numbers.parseFloat(tok));
        } catch (NumericException ignore) {
        }

        // type constant for 'CAST' operation
        final int columnType = ColumnType.typeOf(tok);
        final short columnTag = ColumnType.tagOf(columnType);
        if (
                (columnTag >= ColumnType.BOOLEAN && columnTag <= ColumnType.BINARY)
                        || columnTag == ColumnType.REGCLASS
                        || columnTag == ColumnType.REGPROCEDURE
                        || columnTag == ColumnType.ARRAY_STRING
                        || columnTag == ColumnType.UUID
                        || columnTag == ColumnType.IPv4
                        || columnTag == ColumnType.VARCHAR
                        || columnTag == ColumnType.INTERVAL
                        || columnTag == ColumnType.ARRAY
        ) {
            return Constants.getTypeConstant(columnType);
        }

        // geohash type constant

        if (startsWithGeoHashKeyword(tok)) {
            return GeoHashTypeConstant.getInstanceByPrecision(
                    GeoHashUtil.parseGeoHashBits(position, 7, tok));
        }

        if (len > 1 && tok.charAt(0) == '#') {
            ConstantFunction geoConstant = GeoHashUtil.parseGeoHashConstant(position, tok, len);
            if (geoConstant != null) {
                return geoConstant;
            }
        }

        //region decimal
        if (len >= DECIMAL_KEYWORD_LENGTH && startsWithDecimalKeyword(tok)) {
            return createDecimalTypeConstant(tok, len, position);
        }

        if (len > 1 && (tok.charAt(len - 1) | 32) == 'm') {
            return DecimalUtil.parseDecimalConstant(position, executionContext, tok, -1, -1);
        }

        if (isNumericKeyword(tok)) {
            // We don't know the actual size of the decimal that will be sent,
            // we assume it's going to be enough but cannot be sure.
            return new DecimalTypeConstant(76, 38);
        }
        //endregion

        // long256
        if (Numbers.extractLong256(tok, long256Sink)) {
            return new Long256Constant(long256Sink); // values are copied from this sink
        }

        throw SqlException.position(position).put("invalid constant: ").put(tok);
    }

    /**
     * Constructs a previously selected overload without parsing or resolving an expression.
     * Argument types, required constants and implicit casts must already have been resolved.
     * The factory may modify both argument lists. Argument ownership is transferred to the
     * returned function on success, or released here on failure; callers must not close the
     * arguments separately. The argument list is cleared after a successful transfer.
     */
    public Function createFunction(
            FunctionFactoryDescriptor overload,
            int position,
            CharSequence name,
            @Transient ObjList<Function> args,
            @Transient IntList argPositions,
            SqlExecutionContext executionContext
    ) throws SqlException {
        final FunctionFactory factory = overload.getFactory();
        final SqlException rejection = rejectAdministrativeFunction(factory, position, name, executionContext);
        if (rejection != null) {
            Misc.freeObjList(args, rejection);
            throw rejection;
        }

        final int declaredType = ASSERTIONS_ENABLED ? declaredResultType(factory, args) : ColumnType.UNDEFINED;
        Function function;
        try {
            LOG.debug().$("call ").$safe(name)
                    .$(" -> ").$safe(factory.getSignature())
                    .$("[factory=").$(factory)
                    .I$();
            function = factory.newInstance(position, args, argPositions, configuration, executionContext);
        } catch (SqlException | ImplicitCastException e) {
            // Best-effort cleanup: keep closing args even if one close() throws, and fold any
            // close failure into the original error as suppressed instead of masking it.
            Misc.freeObjList(args, e);
            throw e;
        } catch (Throwable e) {
            LOG.error().$("exception in function factory: ").$(e).$();
            final SqlException ex = SqlException.position(position).put("exception in function factory: ").put(e.getMessage());
            Misc.freeObjList(args, ex);
            throw ex;
        }

        if (function == null) {
            LOG.error().$("NULL function")
                    .$(" [signature=").$safe(factory.getSignature())
                    .$(", class=").$safe(factory.getClass().getName())
                    .I$();
            final SqlException ex = SqlException.position(position).put("bad function factory (NULL), check log");
            Misc.freeObjList(args, ex);
            throw ex;
        } else if (!executionContext.allowNonDeterministicFunctions() && function.isNonDeterministic()) {
            // The same guard is armed for both a materialized view and a live view
            // SELECT; name the kind actually being compiled so the reject reads right.
            final SqlException exception = SqlException.nonDeterministicColumn(
                    position,
                    name,
                    executionContext.isLiveViewCompile() ? "live view" : "materialized view"
            );
            // Construction succeeded, so the function has taken ownership of args (see the args.clear()
            // below on the success path). Close the function itself - not just its argument list - so
            // any native resource it allocated beyond its arguments (e.g. an IN-value set) is released
            // instead of leaked. Closing the function also frees the args it owns, so do not free them
            // separately. Preserve the rejection exception if close() were to throw.
            if (args != null) {
                args.clear(); // newInstance() transferred argument ownership to function
            }
            Misc.free(function, exception);
            throw exception;
        }
        assert declaredType == ColumnType.UNDEFINED || declaredType == function.getType() : "function type differs from the result type its factory declares";
        addExecutionRequirements(factory, position, name);
        if (args != null) {
            args.clear(); // To enforce that args are not used after this point
        }
        if (ColumnType.isCursor(function.getType())) {
            cursorFunctionInstantiated = true;
        }
        return function;
    }

    /**
     * Consumes the input on success or failure; a null result leaves it with the caller.
     */
    public Function createImplicitCast(int position, Function function, int toType, SqlExecutionContext executionContext) throws SqlException {
        final Function cast;
        try {
            cast = createImplicitCastOrNull(position, function, toType, executionContext);
        } catch (Throwable th) {
            Misc.free(function, th);
            throw th;
        }
        if (cast instanceof ConstantFunction && cast != function) {
            try {
                function.close();
            } catch (Throwable th) {
                Misc.free(cast, th);
                throw th;
            }
        }
        return cast != null && cast.isConstant() ? functionToConstant(cast) : cast;
    }

    public int enterExecutionRequirementPosition(int position) {
        final int previousPosition = executionRequirementPosition;
        if (previousPosition < 0) {
            executionRequirementPosition = position;
        }
        return previousPosition;
    }

    public boolean findNoArgFunction(CharSequence name) {
        final ObjList<FunctionFactoryDescriptor> overload = functionFactoryCache.getOverloadList(name);
        if (overload != null) {
            for (int i = 0, n = overload.size(); i < n; i++) {
                if (overload.getQuick(i).getSigArgCount() == 0) {
                    return true;
                }
            }
        }
        return false;
    }

    public Function functionToConstant(Function function) {
        Function newFunction;
        try {
            newFunction = functionToConstant0(function);
        } catch (Throwable th) {
            Misc.free(function, th);
            throw th;
        }

        // Sometimes functionToConstant0 returns same instance as passed in parameter
        if (newFunction != function) {
            // and we want to close underlying function only in case it's different form returned newFunction
            try {
                function.close();
            } catch (Throwable th) {
                Misc.free(newFunction, th);
                throw th;
            }
        }
        return newFunction;
    }

    public CairoConfiguration getConfiguration() {
        return configuration;
    }

    public SqlExecutionRequirements getExecutionRequirements() {
        return executionRequirements;
    }

    public FunctionFactoryCache getFunctionFactoryCache() {
        return functionFactoryCache;
    }

    /**
     * The cast factory whose function the last {@link #coerceArguments} inserted at the argument index, or null.
     */
    public Class<? extends FunctionFactory> getImplicitConversion(int index) {
        return implicitConversions.getQuiet(index);
    }

    /**
     * Whether a function factory has produced a CURSOR-typed function since the last
     * {@link #resetCursorFunctionInstantiated()}. The flag is raised on the <em>instantiated</em>
     * function rather than on the factory or the name, because the same name can be either: the
     * {@code sleep} factory yields a cursor in one signature and a plain boolean in another, and only
     * the boolean one is legal in a WAL {@code UPDATE}. It is also raised wherever the function
     * stands - a FROM source, a projected column or a predicate operand all reach
     * {@code createFunction} - which is what makes the WAL {@code UPDATE} check that reads it
     * position-independent.
     * <p>
     * A sub-query written as {@code (SELECT ...)} does not raise it: that is an
     * {@link io.questdb.griffin.model.ExpressionNode#QUERY} node, not a factory call, and the tables it names are
     * visible in the model tree and checked there.
     *
     * @see SqlCompilerImpl#generateUpdate
     * @see #markCursorFunctionInstantiated()
     */
    public boolean isCursorFunctionInstantiated() {
        return cursorFunctionInstantiated;
    }

    /**
     * Raises the same flag {@link #isCursorFunctionInstantiated()} reports for a cursor the compiler
     * builds without going through a function factory. {@code SHOW} is the case that exists:
     * {@code TableFunctionSources} constructs the factory for it directly, so nothing here would ever
     * see it.
     * The invariant the flag stands for is "the compiler materialised a cursor for this statement",
     * not "a function factory was called", and this keeps the two construction paths on the same
     * side of it.
     */
    public void markCursorFunctionInstantiated() {
        cursorFunctionInstantiated = true;
    }

    public void resetCursorFunctionInstantiated() {
        cursorFunctionInstantiated = false;
    }

    /**
     * Resolves a CAST that needs no factory: one to the argument's own type returns the argument, and one of a
     * floating-point literal, spelled {@code literal}, to DECIMAL returns the exact DECIMAL constant. An untyped
     * bind variable CAST to a text, numeric, array or DECIMAL type takes a default type first, so it does not take
     * whatever the first CAST overload accepts; when that type is the target, the argument is returned. Returns null
     * when the call is not such a CAST. Argument ownership stays with the caller.
     */
    public Function resolveCast(
            CharSequence name,
            @Nullable ObjList<Function> args,
            @Nullable CharSequence literal,
            int literalPosition,
            SqlExecutionContext sqlExecutionContext
    ) throws SqlException {
        if (!SqlKeywords.isCastKeyword(name) || args == null || args.size() != 2 || !args.getQuick(1).isConstant()) {
            return null;
        }
        final Function arg0 = args.getQuick(0);
        final int fromType = arg0.getType();
        final int castToType = args.getQuick(1).getType();
        if (fromType == castToType) {
            return arg0;
        }

        // We manually handle the float/double to decimal cast here to avoid precision loss
        if (literal != null && ColumnType.isDecimal(castToType) && (fromType == ColumnType.DOUBLE || fromType == ColumnType.FLOAT)) {
            return DecimalUtil.parseDecimalConstant(literalPosition, sqlExecutionContext, literal,
                    ColumnType.getDecimalPrecision(castToType), ColumnType.getDecimalScale(castToType));
        }

        // If a bind variable of unknown type appears inside a cast expression, we should
        // assign a default type to it. Otherwise, since casting is a heavily overloaded
        // operation (can cast lots of things to a string/number), we'll end up picking
        // whatever happens to be the first cast function in the traversal order, and force
        // the bind variable to that type. This will then fail when an actual value is bound
        // to the variable, and it's most likely not that arbitrary type.
        if (ColumnType.isUndefined(fromType)) {
            final int assignType = switch (ColumnType.tagOf(castToType)) {
                case ColumnType.VARCHAR, ColumnType.STRING, ColumnType.CHAR -> ColumnType.STRING;
                case ColumnType.BYTE, ColumnType.SHORT, ColumnType.INT, ColumnType.LONG, ColumnType.FLOAT,
                     ColumnType.DOUBLE -> ColumnType.DOUBLE;
                case ColumnType.ARRAY, ColumnType.DECIMAL8, ColumnType.DECIMAL16, ColumnType.DECIMAL32,
                     ColumnType.DECIMAL64, ColumnType.DECIMAL128, ColumnType.DECIMAL256 -> castToType;
                default -> ColumnType.UNDEFINED;
            };
            if (assignType != ColumnType.UNDEFINED) {
                arg0.assignType(assignType, sqlExecutionContext.getBindVariableService());
                if (assignType == castToType) {
                    return arg0;
                }
            }
        }
        return null;
    }

    public void restoreExecutionRequirementPosition(int position) {
        executionRequirementPosition = position;
    }

    /**
     * Selects the overload of the named function the argument types and constness match best. When none matches
     * and an argument is a sub-query, which may be a scalar BOOLEAN sub-query in boolean context
     * (e.g. {@code a and (select b from x)}), wraps such arguments as BOOLEAN and returns null: the caller resolves
     * the call again, which cannot recur since no argument is a sub-query any more. Releases the arguments when it
     * reports that no overload matches.
     */
    public FunctionFactoryDescriptor selectOverload(
            CharSequence name,
            int position,
            @Nullable @Transient ObjList<Function> args,
            @Nullable @Transient IntList argPositions,
            SqlExecutionContext sqlExecutionContext
    ) throws SqlException {
        final ObjList<FunctionFactoryDescriptor> overload = functionFactoryCache.getOverloadList(name);
        if (overload == null) {
            throw invalidFunction(name, position, args);
        }

        final int argCount = args == null ? 0 : args.size();
        final boolean isWindowContext = !sqlExecutionContext.getWindowContext().isEmpty();
        argTypes.clear();
        argTraits.clear();
        for (int i = 0; i < argCount; i++) {
            final Function arg = args.getQuick(i);
            final int type = arg.getType();
            final boolean isConstant = arg.isConstant();
            argTypes.add(type);
            argTraits.add((isConstant ? OverloadResolver.ARG_CONSTANT : 0)
                    | (arg == CharTypeConstant.INSTANCE ? OverloadResolver.ARG_CHAR_TYPE : 0)
                    | (isConstant && ColumnType.tagOf(type) == ColumnType.STRING && arg.getStrLen(null) > 1 ? OverloadResolver.ARG_MULTI_CHAR : 0));
        }
        final FunctionFactoryDescriptor candidateDescriptor = OverloadResolver.resolve(
                overload, argTypes, argTraits, SqlKeywords.isCastKeyword(name), isWindowContext);
        if (candidateDescriptor != null) {
            return candidateDescriptor;
        }

        boolean coerced = false;
        for (int i = 0; i < argCount; i++) {
            assert argPositions != null;
            final Function wrapped = BooleanSubQueryFunction.maybeWrap(args.getQuick(i), argPositions.getQuick(i));
            if (wrapped != null) {
                args.setQuick(i, wrapped);
                coerced = true;
            }
        }
        if (coerced) {
            return null;
        }

        // find the best descriptor for a helpful error message
        FunctionFactoryDescriptor expectedDescriptor = null;
        if (overload.size() == 1) {
            expectedDescriptor = overload.getQuick(0);
        } else {
            // multiple overloads: filter by context (window vs group-by) to find the relevant one
            FunctionFactoryDescriptor contextMatch = null;
            int contextMatchCount = 0;
            for (int i = 0, n = overload.size(); i < n; i++) {
                FunctionFactoryDescriptor d = overload.getQuick(i);
                if (isWindowContext == d.getFactory().isWindow()) {
                    contextMatch = d;
                    contextMatchCount++;
                }
            }
            if (contextMatchCount == 1) {
                expectedDescriptor = contextMatch;
            }
        }
        throw invalidArgument(name, position, args, argPositions, expectedDescriptor);
    }

    private static int countWindowOverloads(ObjList<FunctionFactoryDescriptor> overload) {
        int count = 0;
        for (int i = 0, n = overload.size(); i < n; i++) {
            if (overload.getQuick(i).getFactory().isWindow()) {
                count++;
            }
        }
        return count;
    }

    /**
     * Creates a DecimalTypeConstant function from a token representing a decimal type specification.
     * <p>
     * This function parses tokens in the format DECIMAL_p or DECIMAL_p_s, where 'p' represents
     * the precision (total number of digits) and 's' represents the scale (number of decimal places).
     * The token format uses underscores as separators because parentheses are handled separately
     * by the expression parser.
     *
     * @param tok      the token containing the decimal type specification (e.g., "DECIMAL_10" or "DECIMAL_10_2")
     * @param len      the length of the token
     * @param position the position in the SQL query for error reporting
     * @return a DecimalTypeConstant containing the validated precision and scale
     * @throws SqlException if:
     *                      - precision is not between 1 and MAX_PRECISION
     *                      - scale is not between 0 and MAX_SCALE
     *                      - the token format is invalid or contains non-numeric values
     */
    private static Function createDecimalTypeConstant(CharSequence tok, int len, int position) throws SqlException {
        // A decimal type is of the format DECIMAL[_p[_s]], to this point we have only validated `DECIMAL_`.
        int precision;
        int scale = 0;
        if (tok instanceof GenericLexer.FloatingSequencePair) {
            CharSequence cs1 = ((GenericLexer.FloatingSequencePair) tok).cs1;
            precision = DecimalUtil.parsePrecision(position, cs1, 0, cs1.length());
        } else if (tok instanceof GenericLexer.FloatingSequenceTriple) {
            CharSequence cs1 = ((GenericLexer.FloatingSequenceTriple) tok).cs1;
            CharSequence cs2 = ((GenericLexer.FloatingSequenceTriple) tok).cs2;
            precision = DecimalUtil.parsePrecision(position, cs1, 0, cs1.length());
            scale = DecimalUtil.parseScale(position, cs2, 0, cs2.length());
        } else {
            // Slower path
            if (len == DECIMAL_KEYWORD_LENGTH) {
                precision = 18;
                scale = 3;
            } else {
                int sepIndex = Chars.indexOf(tok, 8, '_');
                precision = DecimalUtil.parsePrecision(position, tok, 8, sepIndex == -1 ? len : sepIndex);
                if (sepIndex != -1) {
                    scale = DecimalUtil.parseScale(position, tok, sepIndex + 1, len);
                }
            }
        }

        if (precision <= 0) {
            throw SqlException.position(position)
                    .put("Invalid decimal type. The precision (")
                    .put(precision)
                    .put(") must be greater than zero");
        }
        if (precision > Decimals.MAX_PRECISION) {
            throw SqlException.position(position)
                    .put("Invalid decimal type. The precision (")
                    .put(precision)
                    .put(") must be less than ")
                    .put(Decimals.MAX_PRECISION);
        }
        if (scale < 0) {
            throw SqlException.position(position)
                    .put("Invalid decimal type. The scale (")
                    .put(scale)
                    .put(") must be greater than or equal to zero");
        }
        if (scale > precision) {
            throw SqlException.position(position)
                    .put("Invalid decimal type. The precision (")
                    .put(precision)
                    .put(") must be greater than or equal to the scale (")
                    .put(scale)
                    .put(")");
        }

        return new DecimalTypeConstant(precision, scale);
    }

    /**
     * Extracts the year from a timestamp string.
     * Supports formats like "YYYY-MM-DD...", "YYYY/MM/DD...", etc.
     *
     * @param timestampStr the timestamp string
     * @return the year, or -1 if cannot be extracted
     */
    private static int extractYearFromTimestamp(CharSequence timestampStr) {
        if (timestampStr == null || timestampStr.length() < 4) {
            return -1;
        }

        // Look for the first 4 consecutive digits at the start
        int yearStart = 0;
        int digitCount = 0;

        for (int i = 0; i < timestampStr.length() && i < 10; i++) { // Limit search to first 10 chars
            char c = timestampStr.charAt(i);
            if (c >= '0' && c <= '9') {
                if (digitCount == 0) {
                    yearStart = i;
                }
                digitCount++;
                if (digitCount == 4) {
                    // Found 4 consecutive digits, extract as year
                    try {
                        return Numbers.parseInt(timestampStr, yearStart, yearStart + 4);
                    } catch (NumericException e) {
                        return -1;
                    }
                }
            } else {
                digitCount = 0; // Reset if non-digit found
            }
        }

        return -1; // Could not extract year
    }

    @NotNull
    private static BindVariableService getBindVariableService(SqlExecutionContext executionContext) throws SqlException {
        final BindVariableService bindVariableService = executionContext.getBindVariableService();
        if (bindVariableService == null) {
            throw SqlException.$(0, "bind variable service is not provided");
        }
        return bindVariableService;
    }

    private static void handleExpectedAndActual(@Transient IntList argPositions, SqlException ex, int i, int expectedType, int actualType) {
        ex.put(" expected: ").put(ColumnType.nameOf(expectedType));
        if (expectedType == actualType) {
            ex.put(" constant");
        }
        ex.put(", actual: ").put(ColumnType.nameOf(actualType));
        ex.setPosition(argPositions.getQuick(i));
    }

    private static SqlException invalidArgument(
            CharSequence name,
            int position,
            @Nullable ObjList<Function> args,
            @Transient IntList argPositions,
            FunctionFactoryDescriptor descriptor
    ) {
        SqlException ex = SqlException.position(position);
        if (descriptor != null) {
            if (args != null) {
                if (args.size() != descriptor.getSigArgCount()) {
                    ex.put("wrong number of arguments for function `").put(name)
                            .put("`; expected: ").put(descriptor.getSigArgCount())
                            .put(", provided: ").put(args.size());
                } else if (args.size() == 2) {
                    // Binary operator; we have overloads we could not use because argument types
                    // do not match somewhere. Throw type-specific exception, pointing out expression,
                    // which type does not match the operator's descriptor.
                    // This is typically works for boolean operators, such as "and" and "or" when
                    // their arguments are not boolean.
                    ex.put("expression type mismatch,");
                    for (int i = 0, n = descriptor.getSigArgCount(); i < n; i++) {
                        final int typeWithFlags = descriptor.getArgTypeWithFlags(i);
                        final int expectedType = FunctionFactoryDescriptor.toTypeTag(typeWithFlags);
                        final int actualType = args.getQuick(i).getType();
                        final boolean expectedConstant = FunctionFactoryDescriptor.isConstant(typeWithFlags);
                        final boolean actualConstant = args.getQuick(i).isConstant();

                        if (FunctionFactoryDescriptor.isArray(typeWithFlags)) {
                            // we expect arg to be a compatible array
                            if (ColumnType.isArray(actualType) && expectedType == ColumnType.decodeArrayElementType(actualType) && (!expectedConstant || actualConstant)) {
                                continue;
                            }
                            ex.put(" expected: ").put(ColumnType.nameOf(expectedType)).put("[]");
                            if (ColumnType.isArray(actualType) && expectedType == ColumnType.decodeArrayElementType(actualType)) {
                                ex.put(" constant");
                            }
                            ex.put(", actual: ").put(ColumnType.nameOf(actualType));
                            ex.setPosition(argPositions.getQuick(i));
                            break;
                        } else {
                            if (expectedType != actualType || (expectedConstant && !actualConstant)) {
                                handleExpectedAndActual(argPositions, ex, i, expectedType, actualType);
                                break;
                            }
                        }
                    }
                } else {
                    ex.put("argument type mismatch for function `").put(name).put('`');
                    for (int i = 0, n = descriptor.getSigArgCount(); i < n; i++) {
                        final int typeWithFlags = descriptor.getArgTypeWithFlags(i);
                        final int expectedType = FunctionFactoryDescriptor.toTypeTag(typeWithFlags);
                        final boolean expectedConstant = FunctionFactoryDescriptor.isConstant(typeWithFlags);
                        final int actualType = args.getQuick(i).getType();
                        final boolean actualConstant = args.getQuick(i).isConstant();

                        if (expectedType != actualType || (expectedConstant && !actualConstant)) {
                            ex.put(" at #").put(i + 1);
                            handleExpectedAndActual(argPositions, ex, i, expectedType, actualType);
                        }
                    }
                }
                Misc.freeObjList(args, ex);
                return ex;
            }

            ex.put("function `");
            ex.put(name);
            ex.put("` requires arguments: ");
            ex.put(name);
            ex.put('(');
            for (int i = 0, n = descriptor.getSigArgCount(); i < n; i++) {
                if (i > 0) {
                    ex.put(", ");
                }
                final int typeWithFlags = descriptor.getArgTypeWithFlags(i);
                ex.put(ColumnType.nameOf(FunctionFactoryDescriptor.toTypeTag(typeWithFlags)));
                if (FunctionFactoryDescriptor.isArray(typeWithFlags)) {
                    ex.put("[]");
                }
                if (FunctionFactoryDescriptor.isConstant(typeWithFlags)) {
                    ex.put(" constant");
                }
            }
            ex.put(')');
            return ex;
        }

        OperatorExpression op = OperatorExpression.getRegistry().getOperatorDefinition(name);
        if (op == null) {
            // function, not an operator, is not found
            if (args != null) {
                ex.put("there is no matching function `").put(name).put("` with the argument types: (");
                for (int i = 0, n = args.size(); i < n; i++) {
                    if (i > 0) {
                        ex.put(", ");
                    }
                    putArgType(args, i, ex);
                }
                ex.put(')');
            } else {
                ex.put("function `").put(name).put("` requires arguments");
            }
            Misc.freeObjList(args, ex);
            return ex;
        }

        if (args != null && args.size() == 2) {
            // binary operator not found
            // function, not an operator, is not found
            ex.put("there is no matching operator `").put(name).put("` with the argument types: ");
            putArgType(args, 0, ex);
            ex.put(' ');
            ex.put(name);
            ex.put(' ');
            putArgType(args, 1, ex);
            Misc.freeObjList(args, ex);
            return ex;
        }

        assert args != null;

        // Unary operator with the specific argument types not found.
        // function, not an operator, is not found
        ex.put("there is no matching operator `").put(name).put("` with the argument type: ");
        putArgType(args, 0, ex);
        Misc.freeObjList(args, ex);
        return ex;
    }

    private static SqlException invalidFunction(CharSequence name, int position, ObjList<Function> args) {
        if (isUnnestKeyword(name)) {
            final SqlException ex = SqlException.position(position)
                    .put("UNNEST cannot be used as an expression; use it in the FROM clause");
            Misc.freeObjList(args, ex);
            return ex;
        }
        SqlException ex = SqlException.position(position);
        ex.put("unknown function name");
        ex.put(": ");
        ex.put(name);
        ex.put('(');
        if (args != null) {
            for (int i = 0, n = args.size(); i < n; i++) {
                if (i > 0) {
                    ex.put(',');
                }
                ex.put(ColumnType.nameOf(args.getQuick(i).getType()));
            }
        }
        ex.put(')');
        Misc.freeObjList(args, ex);
        return ex;
    }

    private static long parseDate(CharSequence str, int position) throws SqlException {
        try {
            return MillisTimestampDriver.floor(str);
        } catch (NumericException e) {
            throw SqlException.invalidDate(str, position);
        }
    }

    private static void putArgType(ObjList<Function> args, int i, SqlException ex) {
        Function arg = args.getQuick(i);
        ex.put(ColumnType.nameOf(arg.getType()));
    }

    private static SqlException rejectAdministrativeFunction(
            FunctionFactory factory,
            int position,
            CharSequence name,
            SqlExecutionContext executionContext
    ) {
        if (executionContext.allowNonDeterministicFunctions()
                || (factory.getExecutionRequirements() & SqlExecutionRequirements.REQUIRES_ENTERPRISE_SECURITY_CONTEXT) == 0) {
            return null;
        }
        final CharSequence objectKind = executionContext.isLiveViewCompile() ? "live view" : "materialized view";
        return SqlException.position(position)
                .put("administrative function cannot be used in ")
                .put(objectKind)
                .put(": ")
                .put(name);
    }

    private void addExecutionRequirements(FunctionFactory factory, int position, CharSequence name) {
        executionRequirements.add(
                factory.getExecutionRequirements(),
                executionRequirementPosition > -1 ? executionRequirementPosition : position,
                name
        );
    }

    @Nullable
    private Function createImplicitCastOrNull(int position, Function function, int toType, SqlExecutionContext sqlExecutionContext) throws SqlException {
        int fromType = function.getType();
        switch (fromType) {
            case ColumnType.STRING:
            case ColumnType.SYMBOL:
                if (toType == ColumnType.UUID) {
                    return new CastStrToUuidFunctionFactory.Func(function);
                } else if (ColumnType.isTimestamp(toType)) {
                    return new CastStrToTimestampFunctionFactory.Func(function, toType);
                } else if (ColumnType.isArray(toType)) {
                    assert ColumnType.decodeArrayElementType(toType) == ColumnType.DOUBLE;
                    return new CastStrToDoubleArrayFunctionFactory.Func(function, toType);
                } else if (ColumnType.isGeoHash(toType)) {
                    return CastStrToGeoHashFunctionFactory.newInstance(position, toType, function);
                } else if (ColumnType.isDecimal(toType)) {
                    return CastStrToDecimalFunctionFactory.newInstance(sqlExecutionContext.getDecimal256(), position, toType, function);
                }
                break;
            case ColumnType.VARCHAR:
                if (toType == ColumnType.UUID) {
                    return new CastVarcharToUuidFunctionFactory.Func(function);
                } else if (ColumnType.isTimestamp(toType)) {
                    return new CastVarcharToTimestampFunctionFactory.Func(function, toType);
                } else if (ColumnType.isGeoHash(toType)) {
                    return CastVarcharToGeoHashFunctionFactory.newInstance(position, toType, function);
                } else if (ColumnType.isDecimal(toType)) {
                    return CastVarcharToDecimalFunctionFactory.newInstance(sqlExecutionContext.getDecimal256(), position, toType, function);
                }
                break;
            case ColumnType.UUID:
                if (toType == ColumnType.STRING) {
                    return new CastUuidToStrFunctionFactory.Func(function);
                } else if (toType == ColumnType.VARCHAR) {
                    return new CastUuidToVarcharFunctionFactory.Func(function);
                }
                break;
            case ColumnType.CHAR:
                if (toType == ColumnType.SYMBOL) {
                    return new CastCharToSymbolFunctionFactory.Func(function);
                } else if (ColumnType.isDecimal(toType)) {
                    return CastStrToDecimalFunctionFactory.newInstance(sqlExecutionContext.getDecimal256(), position, toType, function);
                }
                break;
            case ColumnType.IPv4:
                if (toType == ColumnType.STRING) {
                    return new CastIPv4ToStrFunctionFactory.Func(function);
                }
                if (toType == ColumnType.VARCHAR) {
                    return new CastIPv4ToVarcharFunctionFactory.Func(function);
                }
                break;
            default:
                if (ColumnType.isGeoHash(fromType)) {
                    int fromGeoBits = ColumnType.getGeoHashBits(fromType);
                    int toGeoBits = ColumnType.getGeoHashBits(toType);
                    if (ColumnType.isGeoHash(toType) && toGeoBits < fromGeoBits) {
                        return CastGeoHashToGeoHashFunctionFactory.newInstance(position, function, toType, fromType);
                    }
                }
                break;
        }
        if (ColumnType.isDecimal(toType)) {
            return DecimalUtil.getImplicitCastFunction(function, position, toType, sqlExecutionContext);
        }
        return null;
    }

    private Function createIndexParameter(int variableIndex, int position, SqlExecutionContext executionContext) throws SqlException {
        Function function = getBindVariableService(executionContext).getFunction(variableIndex);
        if (function == null) {
            // bind variable is undefined
            return new IndexedParameterLinkFunction(variableIndex, ColumnType.UNDEFINED, position);
        }
        return new IndexedParameterLinkFunction(variableIndex, function.getType(), position);
    }

    private Function createNamedParameter(int position, CharSequence name, SqlExecutionContext executionContext) throws SqlException {
        Function function = getBindVariableService(executionContext).getFunction(name);
        if (function == null) {
            throw SqlException.position(position).put("undefined bind variable: ").put(name);
        }
        return new NamedParameterLinkFunction(Chars.toString(name), function.getType());
    }

    private int declaredResultType(FunctionFactory factory, ObjList<Function> args) {
        argTypes.clear();
        for (int i = 0, n = args == null ? 0 : args.size(); i < n; i++) {
            argTypes.add(args.getQuick(i).getType());
        }
        return factory.getResultType(argTypes);
    }

    private Function functionToConstant0(Function function) {
        int type = function.getType();
        switch (ColumnType.tagOf(type)) {
            case ColumnType.INT:
                if (function instanceof IntConstant) {
                    return function;
                } else {
                    final int intConst = function.getInt(null);
                    return intConst == Numbers.INT_NULL ? IntConstant.NULL : IntConstant.newInstance(intConst);
                }
            case ColumnType.BOOLEAN:
                if (function instanceof BooleanConstant) {
                    return function;
                } else {
                    return BooleanConstant.of(function.getBool(null));
                }
            case ColumnType.BYTE:
                if (function instanceof ByteConstant) {
                    return function;
                } else {
                    return ByteConstant.newInstance(function.getByte(null));
                }
            case ColumnType.SHORT:
                if (function instanceof ShortConstant) {
                    return function;
                } else {
                    return ShortConstant.newInstance(function.getShort(null));
                }
            case ColumnType.CHAR:
                if (function instanceof CharConstant) {
                    return function;
                } else {
                    return CharConstant.newInstance(function.getChar(null));
                }
            case ColumnType.FLOAT:
                if (function instanceof FloatConstant) {
                    return function;
                } else {
                    return FloatConstant.newInstance(function.getFloat(null));
                }
            case ColumnType.DOUBLE:
                if (function instanceof DoubleConstant) {
                    return function;
                } else {
                    return DoubleConstant.newInstance(function.getDouble(null));
                }
            case ColumnType.LONG:
                if (function instanceof LongConstant) {
                    return function;
                } else {
                    return LongConstant.newInstance(function.getLong(null));
                }
            case ColumnType.LONG256:
                if (function instanceof Long256Constant) {
                    return function;
                } else {
                    return new Long256Constant(function.getLong256A(null));
                }
            case ColumnType.GEOBYTE:
                if (function instanceof GeoByteConstant) {
                    return function;
                } else {
                    return new GeoByteConstant(function.getGeoByte(null), type);
                }
            case ColumnType.GEOSHORT:
                if (function instanceof GeoShortConstant) {
                    return function;
                } else {
                    return new GeoShortConstant(function.getGeoShort(null), type);
                }
            case ColumnType.GEOINT:
                if (function instanceof GeoIntConstant) {
                    return function;
                } else {
                    return new GeoIntConstant(function.getGeoInt(null), type);
                }
            case ColumnType.GEOLONG:
                if (function instanceof GeoLongConstant) {
                    return function;
                } else {
                    return new GeoLongConstant(function.getGeoLong(null), type);
                }
            case ColumnType.DATE:
                if (function instanceof DateConstant) {
                    return function;
                } else {
                    return DateConstant.newInstance(function.getDate(null));
                }
            case ColumnType.STRING:
                if (function instanceof StrConstant) {
                    return function;
                } else {
                    return StrConstant.fromValue(function.getStrA(null));
                }
            case ColumnType.VARCHAR:
                if (function instanceof VarcharConstant) {
                    return function;
                } else {
                    return VarcharConstant.fromValue(function.getVarcharA(null));
                }
            case ColumnType.SYMBOL:
                if (function instanceof SymbolConstant) {
                    return function;
                }
                return SymbolConstant.fromValue(function.getSymbol(null));
            case ColumnType.TIMESTAMP:
                if (function instanceof TimestampConstant) {
                    return function;
                } else {
                    return TimestampConstant.newInstance(function.getTimestamp(null), type);
                }
            case ColumnType.UUID:
                if (function instanceof UuidConstant) {
                    return function;
                } else {
                    return new UuidConstant(function.getLong128Lo(null), function.getLong128Hi(null));
                }
            case ColumnType.IPv4:
                if (function instanceof IPv4Constant) {
                    return function;
                } else {
                    return IPv4Constant.newInstance(function.getIPv4(null));
                }
            case ColumnType.ARRAY:
                if (function instanceof ArrayConstant) {
                    return function;
                }
                ArrayView array = function.getArray(null);
                if (array instanceof FunctionArray) {
                    return new ArrayConstant((FunctionArray) array);
                }
                return function;
            case ColumnType.DECIMAL8:
                if (function instanceof Decimal8Constant) {
                    return function;
                } else {
                    return new Decimal8Constant(function.getDecimal8(null), type);
                }
            case ColumnType.DECIMAL16:
                if (function instanceof Decimal16Constant) {
                    return function;
                } else {
                    return new Decimal16Constant(function.getDecimal16(null), type);
                }
            case ColumnType.DECIMAL32:
                if (function instanceof Decimal32Constant) {
                    return function;
                } else {
                    return new Decimal32Constant(function.getDecimal32(null), type);
                }
            case ColumnType.DECIMAL64:
                if (function instanceof Decimal64Constant) {
                    return function;
                } else {
                    return new Decimal64Constant(function.getDecimal64(null), type);
                }
            case ColumnType.DECIMAL128:
                if (function instanceof Decimal128Constant) {
                    return function;
                } else {
                    Decimal128 d = Misc.getThreadLocalDecimal128();
                    function.getDecimal128(null, d);
                    return new Decimal128Constant(
                            d.getHigh(),
                            d.getLow(),
                            type
                    );
                }
            case ColumnType.DECIMAL256:
                if (function instanceof Decimal256Constant) {
                    return function;
                } else {
                    Decimal256 d = Misc.getThreadLocalDecimal256();
                    function.getDecimal256(null, d);
                    return new Decimal256Constant(
                            d.getHh(),
                            d.getHl(),
                            d.getLh(),
                            d.getLl(),
                            type
                    );
                }
            default:
                return function;
        }
    }

    private Function parseIndexedParameter(int position, CharSequence name, SqlExecutionContext executionContext) throws SqlException {
        // get variable index from token
        try {
            final int variableIndex = Numbers.parseInt(name, 1, name.length());
            if (variableIndex < 1) {
                throw SqlException.$(position, "invalid bind variable index [value=").put(variableIndex).put(']');
            }
            return createIndexParameter(variableIndex - 1, position, executionContext);
        } catch (NumericException e) {
            throw SqlException.$(position, "invalid bind variable index [value=").put(name).put(']');
        }
    }
}
