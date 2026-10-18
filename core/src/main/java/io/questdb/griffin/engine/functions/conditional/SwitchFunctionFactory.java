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

package io.questdb.griffin.engine.functions.conditional;

import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.ImplicitCastException;
import io.questdb.cairo.MillisTimestampDriver;
import io.questdb.cairo.TimestampDriver;
import io.questdb.cairo.arr.ArrayView;
import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.cairo.sql.StaticSymbolTable;
import io.questdb.cairo.sql.SymbolTable;
import io.questdb.cairo.sql.SymbolTableSource;
import io.questdb.griffin.FunctionFactory;
import io.questdb.griffin.FunctionResolver;
import io.questdb.griffin.PlanSink;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.engine.functions.IntFunction;
import io.questdb.griffin.engine.functions.MultiArgFunction;
import io.questdb.griffin.engine.functions.SymbolFunction;
import io.questdb.griffin.engine.functions.UnaryFunction;
import io.questdb.griffin.engine.functions.constants.ByteConstant;
import io.questdb.griffin.engine.functions.constants.Constants;
import io.questdb.griffin.engine.functions.constants.DateConstant;
import io.questdb.griffin.engine.functions.constants.IntConstant;
import io.questdb.griffin.engine.functions.constants.LongConstant;
import io.questdb.griffin.engine.functions.constants.ShortConstant;
import io.questdb.griffin.engine.functions.constants.TimestampConstant;
import io.questdb.griffin.engine.functions.memoization.MemoizerFunction;
import io.questdb.std.BinarySequence;
import io.questdb.std.CharSequenceObjHashMap;
import io.questdb.std.Chars;
import io.questdb.std.Decimal128;
import io.questdb.std.Decimal256;
import io.questdb.std.FiberLocal;
import io.questdb.std.Interval;
import io.questdb.std.IntList;
import io.questdb.std.IntObjHashMap;
import io.questdb.std.Long256;
import io.questdb.std.LongObjHashMap;
import io.questdb.std.Misc;
import io.questdb.std.Numbers;
import io.questdb.std.NumericException;
import io.questdb.std.ObjList;
import io.questdb.std.str.CharSink;
import io.questdb.std.str.StringSink;
import io.questdb.std.str.Utf8Sequence;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

public class SwitchFunctionFactory implements FunctionFactory {
    private static final IntMethod GET_BYTE = SwitchFunctionFactory::getByte;
    private static final IntMethod GET_CHAR = SwitchFunctionFactory::getChar;
    private static final LongMethod GET_DATE = SwitchFunctionFactory::getDate;
    private static final IntMethod GET_INT = SwitchFunctionFactory::getInt;
    private static final LongMethod GET_LONG = SwitchFunctionFactory::getLong;
    private static final IntMethod GET_SHORT = SwitchFunctionFactory::getShort;
    private static final FiberLocal<StringSink> tlKeyError = new FiberLocal<>(StringSink::new);

    @Override
    public int getResultType(IntList argTypes) {
        int n = argTypes.size();
        if (n < 2) {
            return ColumnType.UNDEFINED;
        }
        int returnType = -1;
        if (n % 2 == 0) {
            returnType = argTypes.getQuick(--n);
        }
        for (int i = 2; i < n; i += 2) {
            returnType = CaseCommon.getCommonTypeOrUndefined(returnType, argTypes.getQuick(i));
            if (returnType == ColumnType.UNDEFINED) {
                return ColumnType.UNDEFINED;
            }
        }
        return CaseCommon.getCaseFunctionType(returnType);
    }

    @Override
    public String getSignature() {
        return "switch(V)";
    }

    /**
     * Builds the switch with a resolver of its own for the = of its WHEN constants; {@link FunctionResolver} builds
     * it with {@link #newInstance(int, ObjList, IntList, CairoConfiguration, EqualityFactory, SqlExecutionContext)}.
     */
    @Override
    public Function newInstance(
            int position,
            ObjList<Function> args,
            IntList argPositions,
            CairoConfiguration configuration,
            SqlExecutionContext sqlExecutionContext
    ) throws SqlException {
        return newInstance(position, args, argPositions, configuration, new FunctionResolver(configuration, sqlExecutionContext.getCairoEngine().getFunctionFactoryCache()), sqlExecutionContext);
    }

    /**
     * Builds the switch. The key looks up the WHEN constants its type holds exactly; every other WHEN constant is
     * compared with the = of the key and the constant, in WHEN order, over the key evaluated once per row. When that
     * = does not resolve either, the switch reports the error of the key lookup.
     */
    public Function newInstance(
            int position,
            ObjList<Function> args,
            IntList argPositions,
            CairoConfiguration configuration,
            EqualityFactory equalityFactory,
            SqlExecutionContext sqlExecutionContext
    ) throws SqlException {
        if (args == null || args.size() < 2) {
            throw SqlException.$(position, "switch can be used with 2 or more arguments");
        }
        int n = args.size();
        final Function keyFunction = args.getQuick(0);
        final int keyType = keyFunction.getType();

        Function elseBranch;
        final int elseBranchPosition;
        int returnType = -1;
        if (n % 2 == 0) {
            elseBranch = args.getLast();
            elseBranchPosition = argPositions.getLast();
            returnType = elseBranch.getType();
            n--;
        } else {
            elseBranch = null;
            elseBranchPosition = -1;
        }

        EqualityPicker equalities = null;
        for (int i = 1; i < n; i += 2) {
            final Function constant = args.getQuick(i);
            if (!constant.isConstant()) {
                throw SqlException.$(argPositions.getQuick(i), "constant expected");
            }
            final int constantPosition = argPositions.getQuick(i);
            try {
                args.setQuick(i, getKey(keyType, constant, constantPosition, argPositions.getQuick(0), configuration, sqlExecutionContext));
            } catch (SqlException | ImplicitCastException keyError) {
                if (equalities == null) {
                    final MemoizerFunction memo = MemoizerFunction.newInstance(keyFunction);
                    if (memo == null) {
                        throw keyError;
                    }
                    equalities = new EqualityPicker(memo);
                }
                // the = error may reuse the key error's flyweight
                final int keyErrorPosition = keyError.getPosition();
                final StringSink keyErrorMessage = tlKeyError.get();
                keyErrorMessage.clear();
                keyErrorMessage.put(keyError.getFlyweightMessage());
                args.setQuick(i, null);
                try {
                    args.setQuick(i, equalities.newArm(equalityFactory, constant, argPositions.getQuick(0), constantPosition, i, sqlExecutionContext));
                } catch (SqlException | ImplicitCastException equalityError) {
                    throw SqlException.$(keyErrorPosition, keyErrorMessage);
                }
            }

            returnType = CaseCommon.getCommonType(returnType, args.getQuick(i + 1).getType(), argPositions.getQuick(i + 1), "CASE values cannot be bind variables");
        }

        for (int i = 2; i < n; i += 2) {
            args.setQuick(
                    i,
                    CaseCommon.getValueCastFunction(
                            args.getQuick(i),
                            argPositions.getQuick(i),
                            returnType,
                            configuration,
                            sqlExecutionContext
                    )
            );
        }

        if (elseBranch != null) {
            elseBranch = CaseCommon.getValueCastFunction(
                    elseBranch,
                    elseBranchPosition,
                    returnType,
                    configuration,
                    sqlExecutionContext
            );
        }

        if (equalities == null) {
            return getKeyedFunction(args, argPositions, position, n, keyFunction, returnType, elseBranch);
        }
        return getEqualityFunction(args, argPositions, position, n, equalities, returnType, elseBranch);
    }

    @Override
    public int resolvePreferredVariadicType(int sqlPos, int argPos, ObjList<Function> args) throws SqlException {
        if (argPos == 0) {
            throw SqlException.$(sqlPos, "bind variable is not supported here, please use column instead");
        } else {
            throw SqlException.$(sqlPos, "CASE values cannot be bind variables");
        }
    }

    private static byte getByte(Function function, Record record) {
        return function.getByte(record);
    }

    private static char getChar(Function function, Record record) {
        return function.getChar(record);
    }

    private static long getDate(Function function, Record record) {
        return function.getDate(record);
    }

    private static double getDouble(Function function, Record record) {
        return function.getDouble(record);
    }

    private static float getFloat(Function function, Record record) {
        return function.getFloat(record);
    }

    private static int getInt(Function function, Record record) {
        return function.getInt(record);
    }

    /**
     * The constant of the key's type the branch of an integer or temporal WHEN constant matches, or null when no
     * key value equals it: a narrowing that is not exact, e.g. 1_000 for a BYTE key, matches nothing, as with =.
     */
    private static Function getIntegralKey(int keyType, int constantType, long value) {
        final short keyTag = ColumnType.tagOf(keyType);
        if (value == Numbers.LONG_NULL) {
            return keyTag == ColumnType.BYTE || keyTag == ColumnType.SHORT ? null : Constants.getNullConstant(keyType);
        }
        if (isTemporal(keyType) && isTemporal(constantType) && keyType != constantType) {
            final TimestampDriver driver = keyType == ColumnType.DATE ? MillisTimestampDriver.INSTANCE : ColumnType.getTimestampDriver(keyType);
            if (!driver.isExactFrom(value, constantType)) {
                return null;
            }
            value = driver.ceilFrom(value, constantType);
        }
        return switch (keyTag) {
            case ColumnType.BYTE ->
                    value >= Byte.MIN_VALUE && value <= Byte.MAX_VALUE ? ByteConstant.newInstance((byte) value) : null;
            case ColumnType.SHORT ->
                    value >= Short.MIN_VALUE && value <= Short.MAX_VALUE ? ShortConstant.newInstance((short) value) : null;
            case ColumnType.INT ->
                    value > Integer.MIN_VALUE && value <= Integer.MAX_VALUE ? IntConstant.newInstance((int) value) : null;
            case ColumnType.LONG -> LongConstant.newInstance(value);
            case ColumnType.DATE -> DateConstant.newInstance(value);
            default -> TimestampConstant.newInstance(value, keyType);
        };
    }

    /**
     * The constant the switch looks the key up by for a WHEN constant, or null when no key value equals the WHEN
     * constant. Integer and temporal constants, and timestamp text for a temporal key, convert exactly, as = compares
     * them; a NULL matches no BOOLEAN, BYTE or SHORT key; any other constant takes the CASE value cast. Throws the
     * error of the lookup when the key type has none or the constant does not convert to it.
     */
    private static Function getKey(
            int keyType,
            Function constant,
            int position,
            int keyPosition,
            CairoConfiguration configuration,
            SqlExecutionContext sqlExecutionContext
    ) throws SqlException {
        final int constantType = constant.getType();
        if (isInconvertibleKey(constantType, keyType)) {
            throw SqlException.position(position)
                    .put("type mismatch [expected=").put(ColumnType.nameOf(keyType))
                    .put(", actual=").put(ColumnType.nameOf(constantType))
                    .put(']');
        }
        final Function key = convertKey(keyType, constant, position, configuration, sqlExecutionContext);
        if (!isKeyType(keyType)) {
            throw SqlException.$(keyPosition, "type ")
                    .put(ColumnType.nameOf(keyType))
                    .put(" is not supported in 'switch' type of 'case' statement");
        }
        if (key != null) {
            readKey(keyType, key);
        }
        return key;
    }

    private static Function convertKey(
            int keyType,
            Function constant,
            int position,
            CairoConfiguration configuration,
            SqlExecutionContext sqlExecutionContext
    ) throws SqlException {
        final int constantType = constant.getType();
        final short keyTag = ColumnType.tagOf(keyType);
        if (keyTag == ColumnType.BOOLEAN && ColumnType.isNull(constantType)) {
            return null;
        }
        if (isIntegral(keyTag) || isTemporal(keyType)) {
            if (ColumnType.isNull(constantType) || isIntegral(ColumnType.tagOf(constantType)) || isTemporal(constantType)) {
                return getIntegralKey(keyType, constantType, ColumnType.isNull(constantType) ? Numbers.LONG_NULL : constant.getLong(null));
            }
            if (isTemporal(keyType) && ColumnType.isVarcharOrString(constantType)) {
                try (Function timestamp = parseTimestamp(constant.getStrA(null))) {
                    if (timestamp != null) {
                        return getIntegralKey(keyType, timestamp.getType(), timestamp.getTimestamp(null));
                    }
                }
            } else if ((keyTag == ColumnType.BYTE || keyTag == ColumnType.SHORT) && isText(constantType) && constant.getStrA(null) == null) {
                return null;
            }
        }
        return CaseCommon.getCastFunction(constant, position, keyType, configuration, sqlExecutionContext);
    }

    private static long getLong(Function function, Record record) {
        return function.getLong(record);
    }

    private static short getShort(Function function, Record record) {
        return function.getShort(record);
    }

    private static CharSequence getString(Function function, Record record) {
        return function.getStrA(record);
    }

    private static boolean isInconvertibleKey(int keyArgType, int keyType) {
        if (!ColumnType.isConvertibleFrom(keyArgType, keyType)) {
            return true;
        }
        if (ColumnType.isToSameOrWider(keyArgType, keyType) || keyType < ColumnType.BYTE || keyType > ColumnType.LONG) {
            return false;
        }
        // the getter of an integral key type does not narrow a value other than text, e.g. DOUBLE has no getInt()
        return !ColumnType.isStringyType(keyArgType) && keyArgType != ColumnType.CHAR && !CaseCommon.hasCastFactory(keyArgType, keyType);
    }

    private static boolean isIntegral(short tag) {
        return tag == ColumnType.BYTE || tag == ColumnType.SHORT || tag == ColumnType.INT || tag == ColumnType.LONG;
    }

    private static boolean isKeyType(int keyType) {
        return switch (ColumnType.tagOf(keyType)) {
            case ColumnType.CHAR, ColumnType.INT, ColumnType.IPv4, ColumnType.BYTE, ColumnType.SHORT, ColumnType.LONG,
                 ColumnType.FLOAT,
                 ColumnType.DOUBLE, ColumnType.DATE, ColumnType.TIMESTAMP, ColumnType.BOOLEAN, ColumnType.STRING,
                 ColumnType.VARCHAR,
                 ColumnType.SYMBOL -> true;
            default -> false;
        };
    }

    private static boolean isTemporal(int type) {
        return type == ColumnType.DATE || ColumnType.isTimestamp(type);
    }

    private static boolean isText(int type) {
        return switch (ColumnType.tagOf(type)) {
            case ColumnType.CHAR, ColumnType.STRING, ColumnType.VARCHAR, ColumnType.SYMBOL -> true;
            default -> false;
        };
    }

    /**
     * The TIMESTAMP constant text converts to when = compares it with a TIMESTAMP or DATE, or null when the text
     * spells no timestamp.
     */
    private static @Nullable Function parseTimestamp(@Nullable CharSequence text) {
        try {
            return FunctionResolver.timestampConstant(text, ColumnType.TIMESTAMP_MICRO);
        } catch (NumericException e) {
            return null;
        }
    }

    private static void readKey(int keyType, Function key) {
        switch (ColumnType.tagOf(keyType)) {
            case ColumnType.CHAR -> key.getChar(null);
            case ColumnType.INT, ColumnType.IPv4 -> key.getInt(null);
            case ColumnType.BYTE -> key.getByte(null);
            case ColumnType.SHORT -> key.getShort(null);
            case ColumnType.LONG -> key.getLong(null);
            case ColumnType.FLOAT -> key.getFloat(null);
            case ColumnType.DOUBLE -> key.getDouble(null);
            case ColumnType.DATE -> key.getDate(null);
            case ColumnType.TIMESTAMP -> key.getTimestamp(null);
            case ColumnType.BOOLEAN -> key.getBool(null);
            default -> key.getStrA(null);
        }
    }

    private static void removeBranch(ObjList<Function> args, int keyIndex) {
        args.setQuick(keyIndex + 1, Misc.free(args.getQuick(keyIndex + 1)));
    }

    private Function getCharSequenceKeyedFunction(
            ObjList<Function> args,
            IntList argPositions,
            int position,
            int n,
            Function keyFunction,
            int valueType,
            Function elseBranch
    ) throws SqlException {
        final CharSequenceObjHashMap<Function> map = new CharSequenceObjHashMap<>();
        final ObjList<Function> argsToPoke = new ObjList<>();
        Function nullFunc = null;
        int nullValueIndex = -1;
        for (int i = 1; i < n; i += 2) {
            final Function fun = args.getQuick(i);
            final CharSequence key = SwitchFunctionFactory.getString(fun, null);
            if (key == null) {
                if (nullValueIndex >= 0) {
                    args.setQuick(nullValueIndex, null);
                    Misc.free(nullFunc);
                }
                nullValueIndex = i + 1;
                nullFunc = args.getQuick(nullValueIndex);
            } else {
                final int index = map.keyIndex(key);
                if (index < 0) {
                    throw SqlException.$(argPositions.getQuick(i), "duplicate branch");
                }
                map.putAt(index, key, args.getQuick(i + 1));
                argsToPoke.add(args.getQuick(i + 1));
            }
        }

        final Function elseB = getElseFunction(valueType, elseBranch);
        final CaseFunctionPicker picker;
        if (nullFunc == null) {
            picker = record -> {
                final CharSequence value = SwitchFunctionFactory.getString(keyFunction, record);
                if (value != null) {
                    final int index = map.keyIndex(value);
                    if (index < 0) {
                        return map.valueAtQuick(index);
                    }
                }
                return elseB;
            };
        } else {
            final Function nullFuncRef = nullFunc;
            picker = record -> {
                final CharSequence value = SwitchFunctionFactory.getString(keyFunction, record);
                if (value == null) {
                    return nullFuncRef;
                }
                final int index = map.keyIndex(value);
                if (index < 0) {
                    return map.valueAtQuick(index);
                }
                return elseB;
            };
            argsToPoke.add(nullFunc);
        }
        argsToPoke.add(elseB);
        argsToPoke.add(keyFunction);
        return CaseCommon.getCaseFunction(position, valueType, picker, argsToPoke);
    }

    private Function getDoubleKeyedFunction(
            ObjList<Function> args,
            IntList argPositions,
            int position,
            int n,
            Function keyFunction,
            int valueType,
            Function elseBranch
    ) throws SqlException {
        final LongObjHashMap<Function> map = new LongObjHashMap<>();
        final ObjList<Function> argsToPoke = new ObjList<>();
        Function zeroBranch = null;
        for (int i = 1; i < n; i += 2) {
            final Function fun = args.getQuick(i);
            final double value = getDouble(fun, null);
            final long key = Double.doubleToLongBits(value);
            final int index = map.keyIndex(key);
            if (index < 0) {
                throw SqlException.$(argPositions.getQuick(i), "duplicate branch");
            }
            map.putAt(index, key, args.getQuick(i + 1));
            argsToPoke.add(args.getQuick(i + 1));
            if (value == 0 && zeroBranch == null) {
                zeroBranch = args.getQuick(i + 1);
            }
        }
        // = finds 0.0 and -0.0 equal, so either zero takes the first zero branch
        if (zeroBranch != null) {
            map.put(Double.doubleToLongBits(0.0), zeroBranch);
            map.put(Double.doubleToLongBits(-0.0), zeroBranch);
        }

        final Function elseB = getElseFunction(valueType, elseBranch);
        final CaseFunctionPicker picker = record -> {
            final int index = map.keyIndex(Double.doubleToLongBits(getDouble(keyFunction, record)));
            if (index < 0) {
                return map.valueAtQuick(index);
            }
            return elseB;
        };
        argsToPoke.add(elseB);
        argsToPoke.add(keyFunction);

        return CaseCommon.getCaseFunction(position, valueType, picker, argsToPoke);
    }

    private Function getElseFunction(int valueType, Function elseBranch) {
        return elseBranch != null ? elseBranch : Constants.getNullConstant(valueType);
    }

    /**
     * The switch over WHEN constants some of which the key lookup does not hold. A switch of the WHEN ordinals over
     * the memoizer looks up the constants the key type holds.
     */
    private Function getEqualityFunction(
            ObjList<Function> args,
            IntList argPositions,
            int position,
            int n,
            EqualityPicker picker,
            int valueType,
            Function elseBranch
    ) throws SqlException {
        final int whenCount = n / 2;
        final ObjList<Function> ordinalArgs = new ObjList<>();
        final IntList ordinalPositions = new IntList();
        ordinalArgs.add(picker.memo);
        ordinalPositions.add(argPositions.getQuick(0));
        for (int i = 1, when = 0; i < n; i += 2, when++) {
            picker.whens.add(args.getQuick(i));
            picker.values.add(args.getQuick(i + 1));
            if (!picker.isArm(when)) {
                ordinalArgs.add(args.getQuick(i));
                ordinalArgs.add(IntConstant.newInstance(when));
                ordinalPositions.add(argPositions.getQuick(i));
                ordinalPositions.add(argPositions.getQuick(i + 1));
            }
        }
        final Function elseB = getElseFunction(valueType, elseBranch);
        if (ordinalArgs.size() > 1) {
            final Function elseOrdinal = IntConstant.newInstance(whenCount);
            ordinalArgs.add(elseOrdinal);
            ordinalPositions.add(position);
            final Function ordinals = getKeyedFunction(ordinalArgs, ordinalPositions, position, ordinalArgs.size() - 1, picker.memo, ColumnType.INT, elseOrdinal);
            picker.of(ordinals, ordinals, elseB);
        } else {
            picker.of(IntConstant.newInstance(whenCount), picker.memo, elseB);
        }
        final ObjList<Function> argsToPoke = new ObjList<>(1);
        argsToPoke.add(picker);
        return CaseCommon.getCaseFunction(position, valueType, picker, argsToPoke);
    }

    private Function getFloatKeyedFunction(
            ObjList<Function> args,
            IntList argPositions,
            int position,
            int n,
            Function keyFunction,
            int valueType,
            Function elseBranch
    ) throws SqlException {
        final IntObjHashMap<Function> map = new IntObjHashMap<>();
        final ObjList<Function> argsToPoke = new ObjList<>();
        Function zeroBranch = null;
        for (int i = 1; i < n; i += 2) {
            final Function fun = args.getQuick(i);
            final float value = getFloat(fun, null);
            final int key = Float.floatToIntBits(value);
            final int index = map.keyIndex(key);
            if (index < 0) {
                throw SqlException.$(argPositions.getQuick(i), "duplicate branch");
            }
            map.putAt(index, key, args.getQuick(i + 1));
            argsToPoke.add(args.getQuick(i + 1));
            if (value == 0 && zeroBranch == null) {
                zeroBranch = args.getQuick(i + 1);
            }
        }
        // = finds 0.0 and -0.0 equal, so either zero takes the first zero branch
        if (zeroBranch != null) {
            map.put(Float.floatToIntBits(0.0f), zeroBranch);
            map.put(Float.floatToIntBits(-0.0f), zeroBranch);
        }

        final Function elseB = getElseFunction(valueType, elseBranch);
        final CaseFunctionPicker picker = record -> {
            final int index = map.keyIndex(Float.floatToIntBits(getFloat(keyFunction, record)));
            if (index < 0) {
                return map.valueAtQuick(index);
            }
            return elseB;
        };

        argsToPoke.add(elseB);
        argsToPoke.add(keyFunction);

        return CaseCommon.getCaseFunction(position, valueType, picker, argsToPoke);
    }

    /**
     * The switch over a BOOLEAN key, which has at most a TRUE and a FALSE branch; a WHEN null branch matches no row.
     */
    private Function getIfElseFunction(
            ObjList<Function> args,
            IntList argPositions,
            int position,
            int n,
            Function keyFunction,
            int returnType,
            Function elseBranch
    ) throws SqlException {
        int first = -1;
        int second = -1;
        for (int i = 1; i < n; i += 2) {
            if (args.getQuick(i) == null) {
                removeBranch(args, i);
            } else if (first < 0) {
                first = i;
            } else if (second < 0) {
                second = i;
            } else {
                throw SqlException.$(argPositions.getQuick(i), "too many branches");
            }
        }

        final CaseFunctionPicker picker;
        final ObjList<Function> argsToPoke = new ObjList<>();
        argsToPoke.add(keyFunction);
        if (second > 0) {
            final boolean a = args.getQuick(first).getBool(null);
            final Function branchA = args.getQuick(first + 1);
            final boolean b = args.getQuick(second).getBool(null);
            final Function branchB = args.getQuick(second + 1);

            if (a == b) {
                throw SqlException.$(argPositions.getQuick(second), "duplicate branch");
            }

            if (a) {
                picker = record -> keyFunction.getBool(record) ? branchA : branchB;
            } else {
                picker = record -> keyFunction.getBool(record) ? branchB : branchA;
            }

            argsToPoke.add(branchA);
            argsToPoke.add(branchB);
            if (elseBranch != null) {
                args.setQuick(args.size() - 1, null);
                Misc.free(elseBranch);
            }
        } else {
            final Function elseB = getElseFunction(returnType, elseBranch);
            argsToPoke.add(elseB);
            if (first > 0) {
                final boolean value = args.getQuick(first).getBool(null);
                final Function branch = args.getQuick(first + 1);
                if (value) {
                    picker = record -> keyFunction.getBool(record) ? branch : elseB;
                } else {
                    picker = record -> keyFunction.getBool(record) ? elseB : branch;
                }
                argsToPoke.add(branch);
            } else {
                picker = record -> elseB;
            }
        }
        return CaseCommon.getCaseFunction(position, returnType, picker, argsToPoke);
    }

    private Function getIntKeyedFunction(
            ObjList<Function> args,
            IntList argPositions,
            int position,
            int n,
            Function keyFunction,
            int valueType,
            Function elseBranch,
            IntMethod intMethod
    ) throws SqlException {
        final IntObjHashMap<Function> map = new IntObjHashMap<>();
        final ObjList<Function> argsToPoke = new ObjList<>();
        for (int i = 1; i < n; i += 2) {
            final Function fun = args.getQuick(i);
            if (fun == null) {
                removeBranch(args, i);
                continue;
            }
            final int key = intMethod.getKey(fun, null);
            final int index = map.keyIndex(key);
            if (index < 0) {
                throw SqlException.$(argPositions.getQuick(i), "duplicate branch");
            }
            map.putAt(index, key, args.getQuick(i + 1));
            argsToPoke.add(args.getQuick(i + 1));
        }

        final Function elseB = getElseFunction(valueType, elseBranch);
        final CaseFunctionPicker picker = record -> {
            final int index = map.keyIndex(intMethod.getKey(keyFunction, record));
            if (index < 0) {
                return map.valueAtQuick(index);
            }
            return elseB;
        };

        argsToPoke.add(elseB);
        argsToPoke.add(keyFunction);

        return CaseCommon.getCaseFunction(position, valueType, picker, argsToPoke);
    }

    private Function getKeyedFunction(
            ObjList<Function> args,
            IntList argPositions,
            int position,
            int n,
            Function keyFunction,
            int returnType,
            Function elseBranch
    ) throws SqlException {
        return switch (ColumnType.tagOf(keyFunction.getType())) {
            case ColumnType.CHAR ->
                    getIntKeyedFunction(args, argPositions, position, n, keyFunction, returnType, elseBranch, GET_CHAR);
            case ColumnType.INT, ColumnType.IPv4 ->
                    getIntKeyedFunction(args, argPositions, position, n, keyFunction, returnType, elseBranch, GET_INT);
            case ColumnType.BYTE ->
                    getIntKeyedFunction(args, argPositions, position, n, keyFunction, returnType, elseBranch, GET_BYTE);
            case ColumnType.SHORT ->
                    getIntKeyedFunction(args, argPositions, position, n, keyFunction, returnType, elseBranch, GET_SHORT);
            case ColumnType.LONG ->
                    getLongKeyedFunction(args, argPositions, position, n, keyFunction, returnType, elseBranch, GET_LONG);
            case ColumnType.FLOAT ->
                    getFloatKeyedFunction(args, argPositions, position, n, keyFunction, returnType, elseBranch);
            case ColumnType.DOUBLE ->
                    getDoubleKeyedFunction(args, argPositions, position, n, keyFunction, returnType, elseBranch);
            case ColumnType.DATE ->
                    getLongKeyedFunction(args, argPositions, position, n, keyFunction, returnType, elseBranch, GET_DATE);
            case ColumnType.TIMESTAMP ->
                    getTimestampKeyedFunction(args, argPositions, position, n, keyFunction, returnType, elseBranch, keyFunction.getType());
            case ColumnType.BOOLEAN ->
                    getIfElseFunction(args, argPositions, position, n, keyFunction, returnType, elseBranch);
            case ColumnType.STRING,
                 ColumnType.VARCHAR -> // varchar is treated as char sequence, this works, but it's suboptimal
                    getCharSequenceKeyedFunction(args, argPositions, position, n, keyFunction, returnType, elseBranch);
            case ColumnType.SYMBOL -> {
                if (keyFunction instanceof SymbolFunction symFunc && symFunc.isSymbolTableStatic()) {
                    yield getSymbolKeyedFunction(args, argPositions, position, n, symFunc, returnType, elseBranch);
                }
                yield getCharSequenceKeyedFunction(args, argPositions, position, n, keyFunction, returnType, elseBranch);
            }
            default -> throw SqlException.
                    $(argPositions.getQuick(0), "type ")
                    .put(ColumnType.nameOf(keyFunction.getType()))
                    .put(" is not supported in 'switch' type of 'case' statement");
        };
    }

    private Function getLongKeyedFunction(
            ObjList<Function> args,
            IntList argPositions,
            int position,
            int n,
            Function keyFunction,
            int valueType,
            Function elseBranch,
            LongMethod longMethod
    ) throws SqlException {
        final LongObjHashMap<Function> map = new LongObjHashMap<>();
        final ObjList<Function> argsToPoke = new ObjList<>();
        for (int i = 1; i < n; i += 2) {
            final Function fun = args.getQuick(i);
            if (fun == null) {
                removeBranch(args, i);
                continue;
            }
            final long key = longMethod.getKey(fun, null);
            final int index = map.keyIndex(key);
            if (index < 0) {
                throw SqlException.$(argPositions.getQuick(i), "duplicate branch");
            }
            map.putAt(index, key, args.getQuick(i + 1));
            argsToPoke.add(args.getQuick(i + 1));
        }

        final Function elseB = getElseFunction(valueType, elseBranch);
        final CaseFunctionPicker picker = record -> {
            final int index = map.keyIndex(longMethod.getKey(keyFunction, record));
            if (index < 0) {
                return map.valueAtQuick(index);
            }
            return elseB;
        };
        argsToPoke.add(elseB);
        argsToPoke.add(keyFunction);

        return CaseCommon.getCaseFunction(position, valueType, picker, argsToPoke);
    }

    private Function getSymbolKeyedFunction(
            ObjList<Function> args,
            IntList argPositions,
            int position,
            int n,
            SymbolFunction keyFunction,
            int valueType,
            Function elseBranch
    ) throws SqlException {
        final ObjList<String> strKeys = new ObjList<>();
        final ObjList<Function> keyBranches = new ObjList<>();
        final ObjList<Function> argsToPoke = new ObjList<>();
        Function nullFunc = null;
        int nullValueIndex = -1;
        for (int i = 1; i < n; i += 2) {
            final Function fun = args.getQuick(i);
            final CharSequence key = getString(fun, null);
            if (key == null) {
                if (nullValueIndex >= 0) {
                    args.setQuick(nullValueIndex, null);
                    Misc.free(nullFunc);
                }
                nullValueIndex = i + 1;
                nullFunc = args.getQuick(nullValueIndex);
            } else {
                for (int j = 0, m = strKeys.size(); j < m; j++) {
                    if (Chars.equals(strKeys.getQuick(j), key)) {
                        throw SqlException.$(argPositions.getQuick(i), "duplicate branch");
                    }
                }
                strKeys.add(key.toString());
                keyBranches.add(args.getQuick(i + 1));
                argsToPoke.add(args.getQuick(i + 1));
            }
        }

        final Function elseB = getElseFunction(valueType, elseBranch);
        final int branchCount = strKeys.size();

        if (branchCount == 1
                && nullFunc == null
                && ColumnType.tagOf(valueType) == ColumnType.INT
                && keyBranches.getQuick(0).isConstant()
                && elseB.isConstant()
        ) {
            return new SymbolSwitchConstIntFunction(
                    keyFunction,
                    strKeys.getQuick(0),
                    keyBranches.getQuick(0).getInt(null),
                    elseB.getInt(null)
            );
        }

        final SymbolPicker picker;
        if (branchCount == 1) {
            picker = new SymbolSwitchSinglePicker(
                    keyFunction, strKeys.getQuick(0), keyBranches.getQuick(0), nullFunc, elseB
            );
        } else if (branchCount == 2) {
            picker = new SymbolSwitchDualPicker(
                    keyFunction,
                    strKeys.getQuick(0), keyBranches.getQuick(0),
                    strKeys.getQuick(1), keyBranches.getQuick(1),
                    nullFunc, elseB
            );
        } else {
            picker = new SymbolSwitchPicker(keyFunction, strKeys, keyBranches, nullFunc, elseB);
        }
        if (nullFunc != null) {
            argsToPoke.add(nullFunc);
        }
        argsToPoke.add(elseB);
        argsToPoke.add(keyFunction);
        // picker must be last so its init() runs after keyFunction is initialized
        argsToPoke.add(picker);
        return CaseCommon.getCaseFunction(position, valueType, picker, argsToPoke);
    }

    private Function getTimestampKeyedFunction(
            ObjList<Function> args,
            IntList argPositions,
            int position,
            int n,
            Function keyFunction,
            int valueType,
            Function elseBranch,
            int timestampType
    ) throws SqlException {
        final LongObjHashMap<Function> map = new LongObjHashMap<>();
        final ObjList<Function> argsToPoke = new ObjList<>();
        TimestampDriver driver = ColumnType.getTimestampDriver(timestampType);
        long key;
        for (int i = 1; i < n; i += 2) {
            final Function fun = args.getQuick(i);
            if (fun == null) {
                removeBranch(args, i);
                continue;
            }
            int funType = fun.getType();
            key = driver.from(fun.getTimestamp(null), ColumnType.getTimestampType(funType));
            final int index = map.keyIndex(key);
            if (index < 0) {
                throw SqlException.$(argPositions.getQuick(i), "duplicate branch");
            }
            map.putAt(index, key, args.getQuick(i + 1));
            argsToPoke.add(args.getQuick(i + 1));
        }

        final Function elseB = getElseFunction(valueType, elseBranch);
        final CaseFunctionPicker picker = record -> {
            final int index = map.keyIndex(keyFunction.getTimestamp(record));
            if (index < 0) {
                return map.valueAtQuick(index);
            }
            return elseB;
        };
        argsToPoke.add(elseB);
        argsToPoke.add(keyFunction);

        return CaseCommon.getCaseFunction(position, valueType, picker, argsToPoke);
    }

    @FunctionalInterface
    private interface IntMethod {
        int getKey(Function function, Record record);
    }

    @FunctionalInterface
    private interface LongMethod {
        long getKey(Function function, Record record);
    }

    /**
     * Base class for symbol CASE pickers that resolve string constants to int keys at
     * init time and compare by int at runtime. Extends IntFunction so the picker can
     * participate in the MultiArgFunction.init() lifecycle when added to the CaseFunction's
     * args list.
     */
    private abstract static class SymbolPicker extends IntFunction implements CaseFunctionPicker {
        protected final Function elseFunc;
        protected final SymbolFunction keyFunction;
        protected final Function nullFunc;

        SymbolPicker(SymbolFunction keyFunction, Function nullFunc, Function elseFunc) {
            this.keyFunction = keyFunction;
            this.nullFunc = nullFunc;
            this.elseFunc = elseFunc;
        }

        @Override
        public int getInt(Record rec) {
            throw new UnsupportedOperationException();
        }

        protected Function pickNull() {
            return nullFunc != null ? nullFunc : elseFunc;
        }

        protected void toPlanSwitchPrefix(PlanSink sink) {
            sink.val("switch(").val(keyFunction).val(',');
        }

        protected void toPlanSwitchSuffix(PlanSink sink) {
            if (nullFunc != null) {
                sink.val(",null,").val(nullFunc);
            }
            sink.val(',').val(elseFunc).val(')');
        }
    }

    /**
     * Inlined CASE for symbol-keyed single-branch switch with constant int results.
     * Compiles {@code CASE symbol WHEN 'value' THEN int_const ELSE int_const END}
     * into a direct int comparison without picker/CaseFunction virtual dispatch.
     * <p>
     * NULL key handling: {@code SymbolTable.VALUE_IS_NULL} (INT_NULL) never equals
     * a valid resolved key (>= 0) or VALUE_NOT_FOUND (-2), so NULL naturally maps
     * to the else value without an explicit check.
     */
    private static class SymbolSwitchConstIntFunction extends IntFunction implements UnaryFunction {
        // Raw int widening would turn INT_NULL into a value instead of a wider NULL.
        private final double doubleElseValue;
        private final double doubleThenValue;
        private final int elseValue;
        private final float floatElseValue;
        private final float floatThenValue;
        private final SymbolFunction keyFunction;
        private final long longElseValue;
        private final long longThenValue;
        private final String strKey;
        private final int thenValue;
        private int resolvedKey;

        SymbolSwitchConstIntFunction(
                SymbolFunction keyFunction,
                String strKey,
                int thenValue,
                int elseValue
        ) {
            this.keyFunction = keyFunction;
            this.strKey = strKey;
            this.thenValue = thenValue;
            this.elseValue = elseValue;
            this.longThenValue = Numbers.intToLong(thenValue);
            this.longElseValue = Numbers.intToLong(elseValue);
            this.doubleThenValue = Numbers.intToDouble(thenValue);
            this.doubleElseValue = Numbers.intToDouble(elseValue);
            this.floatThenValue = Numbers.intToFloat(thenValue);
            this.floatElseValue = Numbers.intToFloat(elseValue);
        }

        @Override
        public Function getArg() {
            return keyFunction;
        }

        @Override
        public long getDate(Record rec) {
            return keyFunction.getInt(rec) == resolvedKey ? longThenValue : longElseValue;
        }

        @Override
        public double getDouble(Record rec) {
            return keyFunction.getInt(rec) == resolvedKey ? doubleThenValue : doubleElseValue;
        }

        @Override
        public float getFloat(Record rec) {
            return keyFunction.getInt(rec) == resolvedKey ? floatThenValue : floatElseValue;
        }

        @Override
        public int getInt(Record rec) {
            return keyFunction.getInt(rec) == resolvedKey ? thenValue : elseValue;
        }

        @Override
        public long getLong(Record rec) {
            return keyFunction.getInt(rec) == resolvedKey ? longThenValue : longElseValue;
        }

        @Override
        public void init(SymbolTableSource symbolTableSource, SqlExecutionContext executionContext) throws SqlException {
            keyFunction.init(symbolTableSource, executionContext);
            final StaticSymbolTable symbolTable = keyFunction.getStaticSymbolTable();
            assert symbolTable != null;
            resolvedKey = symbolTable.keyOf(strKey);
        }

        @Override
        public void toPlan(PlanSink sink) {
            sink.val("switch(").val(keyFunction)
                    .val(",'").val(strKey).val("',").val(thenValue)
                    .val(',').val(elseValue).val(')');
        }
    }

    /**
     * Two-branch specialization: two direct int comparisons, no hash map.
     */
    private static class SymbolSwitchDualPicker extends SymbolPicker {
        private final Function branch1;
        private final Function branch2;
        private final String strKey1;
        private final String strKey2;
        private int resolvedKey1;
        private int resolvedKey2;

        SymbolSwitchDualPicker(
                SymbolFunction keyFunction,
                String strKey1, Function branch1,
                String strKey2, Function branch2,
                Function nullFunc,
                Function elseFunc
        ) {
            super(keyFunction, nullFunc, elseFunc);
            this.strKey1 = strKey1;
            this.branch1 = branch1;
            this.strKey2 = strKey2;
            this.branch2 = branch2;
        }

        @Override
        public void init(SymbolTableSource symbolTableSource, SqlExecutionContext executionContext) {
            final StaticSymbolTable symbolTable = keyFunction.getStaticSymbolTable();
            assert symbolTable != null;
            resolvedKey1 = symbolTable.keyOf(strKey1);
            resolvedKey2 = symbolTable.keyOf(strKey2);
        }

        @Override
        public @NotNull Function pick(Record record) {
            final int symbolKey = keyFunction.getInt(record);
            if (symbolKey == SymbolTable.VALUE_IS_NULL) {
                return pickNull();
            }
            if (symbolKey == resolvedKey1) {
                return branch1;
            }
            if (symbolKey == resolvedKey2) {
                return branch2;
            }
            return elseFunc;
        }

        @Override
        public void toPlan(PlanSink sink) {
            toPlanSwitchPrefix(sink);
            sink.val('\'').val(strKey1).val("',").val(branch1);
            sink.val(",'").val(strKey2).val("',").val(branch2);
            toPlanSwitchSuffix(sink);
        }
    }

    /**
     * General multi-branch picker: resolves keys into IntObjHashMap for O(1) lookup.
     */
    private static class SymbolSwitchPicker extends SymbolPicker {
        private final IntObjHashMap<Function> intMap = new IntObjHashMap<>();
        private final ObjList<Function> keyBranches;
        private final ObjList<String> strKeys;

        SymbolSwitchPicker(
                SymbolFunction keyFunction,
                ObjList<String> strKeys,
                ObjList<Function> keyBranches,
                Function nullFunc,
                Function elseFunc
        ) {
            super(keyFunction, nullFunc, elseFunc);
            this.strKeys = strKeys;
            this.keyBranches = keyBranches;
        }

        @Override
        public void init(SymbolTableSource symbolTableSource, SqlExecutionContext executionContext) {
            intMap.clear();
            final StaticSymbolTable symbolTable = keyFunction.getStaticSymbolTable();
            assert symbolTable != null;
            for (int i = 0, n = strKeys.size(); i < n; i++) {
                final int symbolKey = symbolTable.keyOf(strKeys.getQuick(i));
                if (symbolKey != SymbolTable.VALUE_NOT_FOUND) {
                    intMap.put(symbolKey, keyBranches.getQuick(i));
                }
            }
        }

        @Override
        public @NotNull Function pick(Record record) {
            final int symbolKey = keyFunction.getInt(record);
            if (symbolKey == SymbolTable.VALUE_IS_NULL) {
                return pickNull();
            }
            final int index = intMap.keyIndex(symbolKey);
            if (index < 0) {
                return intMap.valueAtQuick(index);
            }
            return elseFunc;
        }

        @Override
        public void toPlan(PlanSink sink) {
            toPlanSwitchPrefix(sink);
            for (int i = 0, n = strKeys.size(); i < n; i++) {
                if (i > 0) {
                    sink.val(',');
                }
                sink.val('\'').val(strKeys.getQuick(i)).val("',").val(keyBranches.getQuick(i));
            }
            toPlanSwitchSuffix(sink);
        }
    }

    /**
     * Single-branch specialization: one direct int comparison, no hash map.
     */
    private static class SymbolSwitchSinglePicker extends SymbolPicker {
        private final Function branch;
        private final String strKey;
        private int resolvedKey;

        SymbolSwitchSinglePicker(
                SymbolFunction keyFunction,
                String strKey,
                Function branch,
                Function nullFunc,
                Function elseFunc
        ) {
            super(keyFunction, nullFunc, elseFunc);
            this.strKey = strKey;
            this.branch = branch;
        }

        @Override
        public void init(SymbolTableSource symbolTableSource, SqlExecutionContext executionContext) {
            final StaticSymbolTable symbolTable = keyFunction.getStaticSymbolTable();
            assert symbolTable != null;
            resolvedKey = symbolTable.keyOf(strKey);
        }

        @Override
        public @NotNull Function pick(Record record) {
            final int symbolKey = keyFunction.getInt(record);
            if (symbolKey == SymbolTable.VALUE_IS_NULL) {
                return pickNull();
            }
            if (symbolKey == resolvedKey) {
                return branch;
            }
            return elseFunc;
        }

        @Override
        public void toPlan(PlanSink sink) {
            toPlanSwitchPrefix(sink);
            sink.val('\'').val(strKey).val("',").val(branch);
            toPlanSwitchSuffix(sink);
        }
    }

    /**
     * Constructs the = of two operands, as the parser resolves {@code left = right}. Operand ownership is
     * transferred to the returned function on success, or released on failure.
     */
    @FunctionalInterface
    public interface EqualityFactory {
        Function createEquality(Function left, int leftPosition, Function right, int rightPosition, SqlExecutionContext executionContext) throws SqlException;
    }

    /**
     * Picks the branch of the first WHEN, in WHEN order, whose = holds for the key or whose constant the key looks
     * up, the key read once per pick through the memoizer the switch owns. Owns the functions of the switch, which
     * it plans as {@code switch(key, when, value, ..., else)} with each = arm as its equality.
     */
    private static final class EqualityPicker extends IntFunction implements CaseFunctionPicker, MultiArgFunction {
        private final ObjList<Function> args = new ObjList<>();
        private final IntList armWhens = new IntList();
        private final ObjList<Function> arms = new ObjList<>();
        private final MemoizerFunction memo;
        private final ObjList<Function> values = new ObjList<>();
        // the key constant, the = arm or, for a constant no key value equals, null, of each WHEN
        private final ObjList<Function> whens = new ObjList<>();
        private Function elseFunction;
        private Function ordinals;

        private EqualityPicker(MemoizerFunction memo) {
            this.memo = memo;
        }

        @Override
        public ObjList<Function> args() {
            return args;
        }

        @Override
        public int getInt(Record rec) {
            throw new UnsupportedOperationException();
        }

        @Override
        public @NotNull Function pick(Record record) {
            memo.clearMemo();
            final int keyed = ordinals.getInt(record);
            for (int i = 0, n = arms.size(); i < n; i++) {
                final int when = armWhens.getQuick(i);
                if (when > keyed) {
                    break;
                }
                if (arms.getQuick(i).getBool(record)) {
                    return values.getQuick(when);
                }
            }
            return keyed < values.size() ? values.getQuick(keyed) : elseFunction;
        }

        @Override
        public void toPlan(PlanSink sink) {
            sink.val("switch(").val(memo.getArg());
            for (int i = 0, n = whens.size(); i < n; i++) {
                final Function when = whens.getQuick(i);
                if (when != null) {
                    sink.val(',').val(when).val(',').val(values.getQuick(i));
                }
            }
            sink.val(',').val(elseFunction).val(')');
        }

        private boolean isArm(int when) {
            return armWhens.contains(when);
        }

        private Function newArm(
                EqualityFactory equalityFactory,
                Function constant,
                int keyPosition,
                int constantPosition,
                int argIndex,
                SqlExecutionContext executionContext
        ) throws SqlException {
            final Function key = ColumnType.isSymbol(memo.getType()) ? new SymbolKeyView((SymbolFunction) memo) : new KeyView(memo);
            final Function arm = equalityFactory.createEquality(key, keyPosition, constant, constantPosition, executionContext);
            arms.add(arm);
            armWhens.add((argIndex - 1) / 2);
            return arm;
        }

        /**
         * Takes ownership of the values, the ELSE, the key and the = arms.
         *
         * @param ordinals the WHEN ordinal of the constant the key looks up, or the WHEN count when it looks up none
         * @param keyOwner the function that owns the memoizer: the key lookup, or the memoizer when there is none
         */
        private void of(Function ordinals, Function keyOwner, Function elseFunction) {
            this.ordinals = ordinals;
            this.elseFunction = elseFunction;
            args.addAll(values);
            args.add(elseFunction);
            args.add(keyOwner);
            args.addAll(arms);
        }
    }

    /**
     * The key of a switch read through the switch's memoizer, which owns, initializes and closes it.
     */
    private static final class KeyView implements Function {
        private final MemoizerFunction memo;

        private KeyView(MemoizerFunction memo) {
            this.memo = memo;
        }

        @Override
        public ArrayView getArray(Record rec) {
            return memo.getArray(rec);
        }

        @Override
        public BinarySequence getBin(Record rec) {
            return memo.getBin(rec);
        }

        @Override
        public long getBinLen(Record rec) {
            return memo.getBinLen(rec);
        }

        @Override
        public boolean getBool(Record rec) {
            return memo.getBool(rec);
        }

        @Override
        public byte getByte(Record rec) {
            return memo.getByte(rec);
        }

        @Override
        public char getChar(Record rec) {
            return memo.getChar(rec);
        }

        @Override
        public long getDate(Record rec) {
            return memo.getDate(rec);
        }

        @Override
        public void getDecimal128(Record rec, Decimal128 sink) {
            memo.getDecimal128(rec, sink);
        }

        @Override
        public short getDecimal16(Record rec) {
            return memo.getDecimal16(rec);
        }

        @Override
        public void getDecimal256(Record rec, Decimal256 sink) {
            memo.getDecimal256(rec, sink);
        }

        @Override
        public int getDecimal32(Record rec) {
            return memo.getDecimal32(rec);
        }

        @Override
        public long getDecimal64(Record rec) {
            return memo.getDecimal64(rec);
        }

        @Override
        public byte getDecimal8(Record rec) {
            return memo.getDecimal8(rec);
        }

        @Override
        public double getDouble(Record rec) {
            return memo.getDouble(rec);
        }

        @Override
        public float getFloat(Record rec) {
            return memo.getFloat(rec);
        }

        @Override
        public byte getGeoByte(Record rec) {
            return memo.getGeoByte(rec);
        }

        @Override
        public int getGeoInt(Record rec) {
            return memo.getGeoInt(rec);
        }

        @Override
        public long getGeoLong(Record rec) {
            return memo.getGeoLong(rec);
        }

        @Override
        public short getGeoShort(Record rec) {
            return memo.getGeoShort(rec);
        }

        @Override
        public int getIPv4(Record rec) {
            return memo.getIPv4(rec);
        }

        @Override
        public int getInt(Record rec) {
            return memo.getInt(rec);
        }

        @Override
        public Interval getInterval(Record rec) {
            return memo.getInterval(rec);
        }

        @Override
        public long getLong(Record rec) {
            return memo.getLong(rec);
        }

        @Override
        public long getLong128Hi(Record rec) {
            return memo.getLong128Hi(rec);
        }

        @Override
        public long getLong128Lo(Record rec) {
            return memo.getLong128Lo(rec);
        }

        @Override
        public void getLong256(Record rec, CharSink<?> sink) {
            memo.getLong256(rec, sink);
        }

        @Override
        public Long256 getLong256A(Record rec) {
            return memo.getLong256A(rec);
        }

        @Override
        public Long256 getLong256B(Record rec) {
            return memo.getLong256B(rec);
        }

        @Override
        public RecordCursorFactory getRecordCursorFactory() {
            return memo.getRecordCursorFactory();
        }

        @Override
        public short getShort(Record rec) {
            return memo.getShort(rec);
        }

        @Override
        public CharSequence getStrA(Record rec) {
            return memo.getStrA(rec);
        }

        @Override
        public CharSequence getStrB(Record rec) {
            return memo.getStrB(rec);
        }

        @Override
        public int getStrLen(Record rec) {
            return memo.getStrLen(rec);
        }

        @Override
        public CharSequence getSymbol(Record rec) {
            return memo.getSymbol(rec);
        }

        @Override
        public CharSequence getSymbolB(Record rec) {
            return memo.getSymbolB(rec);
        }

        @Override
        public long getTimestamp(Record rec) {
            return memo.getTimestamp(rec);
        }

        @Override
        public int getType() {
            return memo.getType();
        }

        @Override
        public Utf8Sequence getVarcharA(Record rec) {
            return memo.getVarcharA(rec);
        }

        @Override
        public Utf8Sequence getVarcharB(Record rec) {
            return memo.getVarcharB(rec);
        }

        @Override
        public int getVarcharSize(Record rec) {
            return memo.getVarcharSize(rec);
        }

        @Override
        public boolean isConstant() {
            return memo.isConstant();
        }

        @Override
        public void toPlan(PlanSink sink) {
            sink.val(memo.getArg());
        }
    }

    /**
     * The SYMBOL key of a switch read through the switch's memoizer, which owns, initializes and closes it.
     */
    private static final class SymbolKeyView extends SymbolFunction {
        private final SymbolFunction memo;

        private SymbolKeyView(SymbolFunction memo) {
            this.memo = memo;
        }

        @Override
        public int getInt(Record rec) {
            return memo.getInt(rec);
        }

        @Override
        public StaticSymbolTable getStaticSymbolTable() {
            return memo.getStaticSymbolTable();
        }

        @Override
        public CharSequence getSymbol(Record rec) {
            return memo.getSymbol(rec);
        }

        @Override
        public CharSequence getSymbolB(Record rec) {
            return memo.getSymbolB(rec);
        }

        @Override
        public boolean isConstant() {
            return memo.isConstant();
        }

        @Override
        public boolean isSymbolTableStatic() {
            return memo.isSymbolTableStatic();
        }

        @Override
        public SymbolTable newSymbolTable() {
            return memo.newSymbolTable();
        }

        @Override
        public void toPlan(PlanSink sink) {
            sink.val(((MemoizerFunction) memo).getArg());
        }

        @Override
        public CharSequence valueBOf(int key) {
            return memo.valueBOf(key);
        }

        @Override
        public CharSequence valueOf(int key) {
            return memo.valueOf(key);
        }
    }
}
