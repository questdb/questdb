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
import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.StaticSymbolTable;
import io.questdb.cairo.sql.SymbolTable;
import io.questdb.cairo.sql.SymbolTableSource;
import io.questdb.griffin.FunctionFactory;
import io.questdb.griffin.FunctionResolver;
import io.questdb.griffin.PlanSink;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.engine.functions.IntFunction;
import io.questdb.griffin.engine.functions.SymbolFunction;
import io.questdb.griffin.engine.functions.UnaryFunction;
import io.questdb.griffin.engine.functions.constants.ByteConstant;
import io.questdb.griffin.engine.functions.constants.Constants;
import io.questdb.griffin.engine.functions.constants.DateConstant;
import io.questdb.griffin.engine.functions.constants.IntConstant;
import io.questdb.griffin.engine.functions.constants.LongConstant;
import io.questdb.griffin.engine.functions.constants.ShortConstant;
import io.questdb.griffin.engine.functions.constants.TimestampConstant;
import io.questdb.std.CharSequenceObjHashMap;
import io.questdb.std.Chars;
import io.questdb.std.IntList;
import io.questdb.std.IntObjHashMap;
import io.questdb.std.LongObjHashMap;
import io.questdb.std.Misc;
import io.questdb.std.Numbers;
import io.questdb.std.NumericException;
import io.questdb.std.ObjList;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

public class SwitchFunctionFactory implements FunctionFactory {
    private static final IntMethod GET_BYTE = SwitchFunctionFactory::getByte;
    private static final IntMethod GET_CHAR = SwitchFunctionFactory::getChar;
    private static final LongMethod GET_DATE = SwitchFunctionFactory::getDate;
    private static final IntMethod GET_INT = SwitchFunctionFactory::getInt;
    private static final LongMethod GET_LONG = SwitchFunctionFactory::getLong;
    private static final IntMethod GET_SHORT = SwitchFunctionFactory::getShort;

    /**
     * Whether a WHEN constant of the switch over these arguments is one the switch does not look the key up by:
     * its type does not convert to the key type, or its value does not. The binder binds such a switch as the
     * CASE of key = constant instead, whose = decides the result or the error.
     */
    public static boolean requiresEquality(
            ObjList<Function> args,
            IntList argPositions,
            CairoConfiguration configuration,
            SqlExecutionContext sqlExecutionContext
    ) {
        final int keyType = args.getQuick(0).getType();
        if (!isKeyType(keyType)) {
            return true;
        }
        for (int i = 1, n = (args.size() & 1) == 0 ? args.size() - 1 : args.size(); i < n; i += 2) {
            final Function constant = args.getQuick(i);
            if (!constant.isConstant()) {
                continue;
            }
            if (isInconvertibleKey(constant.getType(), keyType)) {
                return true;
            }
            try {
                final Function key = getKey(keyType, constant, argPositions.getQuick(i), configuration, sqlExecutionContext);
                if (key != null) {
                    readKey(keyType, key);
                }
            } catch (SqlException | ImplicitCastException e) {
                return true;
            }
        }
        return false;
    }

    @Override
    public int getResultType(IntList argTypes) {
        int n = argTypes.size();
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

    @Override
    public Function newInstance(
            int position,
            ObjList<Function> args,
            IntList argPositions,
            CairoConfiguration configuration,
            SqlExecutionContext sqlExecutionContext
    ) throws SqlException {
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

        for (int i = 1; i < n; i += 2) {
            final Function keyFunc = args.getQuick(i);
            final int keyArgType = keyFunc.getType();
            if (!keyFunc.isConstant()) {
                throw SqlException.$(argPositions.getQuick(i), "constant expected");
            }

            if (isInconvertibleKey(keyArgType, keyType)) {
                throw SqlException.position(argPositions.getQuick(i))
                        .put("type mismatch [expected=").put(ColumnType.nameOf(keyType))
                        .put(", actual=").put(ColumnType.nameOf(keyArgType))
                        .put(']');
            }
            args.setQuick(i, getKey(keyType, keyFunc, argPositions.getQuick(i), configuration, sqlExecutionContext));

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

        return switch (ColumnType.tagOf(keyType)) {
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
                    getTimestampKeyedFunction(args, argPositions, position, n, keyFunction, returnType, elseBranch, keyType);
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
                    .put(ColumnType.nameOf(keyType))
                    .put(" is not supported in 'switch' type of 'case' statement");
        };
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
     * them; a NULL text matches no BYTE or SHORT key; any other constant takes the CASE value cast.
     */
    private static Function getKey(
            int keyType,
            Function constant,
            int position,
            CairoConfiguration configuration,
            SqlExecutionContext sqlExecutionContext
    ) throws SqlException {
        final int constantType = constant.getType();
        final short keyTag = ColumnType.tagOf(keyType);
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

    private Function getIfElseFunction(
            ObjList<Function> args,
            IntList argPositions,
            int position,
            int n,
            Function keyFunction,
            int returnType,
            Function elseBranch
    ) throws SqlException {
        final CaseFunctionPicker picker;
        final ObjList<Function> argsToPoke;
        if (n == 3) {
            boolean value = args.getQuick(1).getBool(null);
            final Function branch = args.getQuick(2);

            final Function elseB = getElseFunction(returnType, elseBranch);

            if (value) {
                picker = record -> keyFunction.getBool(record) ? branch : elseB;
            } else {
                picker = record -> keyFunction.getBool(record) ? elseB : branch;
            }

            argsToPoke = new ObjList<>();
            argsToPoke.add(keyFunction);
            argsToPoke.add(elseB);
            argsToPoke.add(branch);

        } else if (n == 5) {
            final boolean a = args.getQuick(1).getBool(null);
            final Function branchA = args.getQuick(2);
            final boolean b = args.getQuick(3).getBool(null);
            final Function branchB = args.getQuick(4);

            if (a && b || !a && !b) {
                throw SqlException.$(argPositions.getQuick(3), "duplicate branch");
            }

            if (a) {
                picker = record -> keyFunction.getBool(record) ? branchA : branchB;
            } else {
                picker = record -> keyFunction.getBool(record) ? branchB : branchA;
            }

            argsToPoke = new ObjList<>();
            argsToPoke.add(keyFunction);
            argsToPoke.add(branchA);
            argsToPoke.add(branchB);
            if (elseBranch != null) {
                args.setQuick(args.size() - 1, null);
                Misc.free(elseBranch);
            }
        } else {
            throw SqlException.$(argPositions.getQuick(5), "too many branches");
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
}
