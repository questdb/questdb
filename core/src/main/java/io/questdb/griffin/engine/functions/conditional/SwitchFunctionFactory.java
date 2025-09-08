/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2024 QuestDB
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
import io.questdb.cairo.TimestampDriver;
import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.Record;
import io.questdb.griffin.FunctionFactory;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.engine.functions.constants.Constants;
import io.questdb.std.CharSequenceObjHashMap;
import io.questdb.std.IntList;
import io.questdb.std.IntObjHashMap;
import io.questdb.std.LongObjHashMap;
import io.questdb.std.ObjList;

public class SwitchFunctionFactory implements FunctionFactory {
    private static final IntMethod GET_BYTE = SwitchFunctionFactory::getByte;
    private static final IntMethod GET_CHAR = SwitchFunctionFactory::getChar;
    private static final LongMethod GET_DATE = SwitchFunctionFactory::getDate;
    private static final IntMethod GET_INT = SwitchFunctionFactory::getInt;
    private static final LongMethod GET_LONG = SwitchFunctionFactory::getLong;
    private static final IntMethod GET_SHORT = SwitchFunctionFactory::getShort;

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

        // key function type must be defined, which makes
        // case $1
        /// when ...
        // an unsupported use case
        if (keyType == ColumnType.UNDEFINED) {
            throw SqlException.$(argPositions.getQuick(0), "bind variable is not supported here, please use column instead");
        }

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

            if (!ColumnType.isAssignableFrom(keyArgType, keyType)) {
                throw SqlException.position(argPositions.getQuick(i))
                        .put("type mismatch [expected=").put(ColumnType.nameOf(keyType))
                        .put(", actual=").put(ColumnType.nameOf(keyArgType))
                        .put(']');
            }

            // determine common return type
            returnType = CaseCommon.getCommonType(returnType, args.getQuick(i + 1).getType(), argPositions.getQuick(i + 1), "CASE values cannot be bind variables");
        }

        // another loop to create cast functions and replace current value function
        // start with 2 to avoid offsetting each function position
        for (int i = 2; i < n; i += 2) {
            args.setQuick(
                    i,
                    CaseCommon.getCastFunction(
                            args.getQuick(i),
                            argPositions.getQuick(i),
                            returnType,
                            configuration,
                            sqlExecutionContext
                    )
            );
        }

        // don't forget to cast the else branch function
        if (elseBranch != null) {
            elseBranch = CaseCommon.getCastFunction(
                    elseBranch,
                    elseBranchPosition,
                    returnType,
                    configuration,
                    sqlExecutionContext
            );
        }

        switch (ColumnType.tagOf(keyType)) {
            case ColumnType.CHAR:
                return getIntKeyedFunction(args, argPositions, position, n, keyFunction, returnType, elseBranch, GET_CHAR);
            case ColumnType.INT:
            case ColumnType.IPv4:
                return getIntKeyedFunction(args, argPositions, position, n, keyFunction, returnType, elseBranch, GET_INT);
            case ColumnType.BYTE:
                return getIntKeyedFunction(args, argPositions, position, n, keyFunction, returnType, elseBranch, GET_BYTE);
            case ColumnType.SHORT:
                return getIntKeyedFunction(args, argPositions, position, n, keyFunction, returnType, elseBranch, GET_SHORT);
            case ColumnType.LONG:
                return getLongKeyedFunction(args, argPositions, position, n, keyFunction, returnType, elseBranch, GET_LONG);
            case ColumnType.FLOAT:
                return getFloatKeyedFunction(args, argPositions, position, n, keyFunction, returnType, elseBranch);
            case ColumnType.DOUBLE:
                return getDoubleKeyedFunction(args, argPositions, position, n, keyFunction, returnType, elseBranch);
            case ColumnType.DATE:
                return getLongKeyedFunction(args, argPositions, position, n, keyFunction, returnType, elseBranch, GET_DATE);
            case ColumnType.TIMESTAMP:
                return getTimestampKeyedFunction(args, argPositions, position, n, keyFunction, returnType, elseBranch, keyType);
            case ColumnType.BOOLEAN:
                return getIfElseFunction(args, argPositions, position, n, keyFunction, returnType, elseBranch);
            case ColumnType.STRING:
            case ColumnType.SYMBOL:
            case ColumnType.VARCHAR: // varchar is treated as char sequence, this works, but it's suboptimal
                return getCharSequenceKeyedFunction(args, argPositions, position, n, keyFunction, returnType, elseBranch);
            default:
                throw SqlException.
                        $(argPositions.getQuick(0), "type ")
                        .put(ColumnType.nameOf(keyType))
                        .put(" is not supported in 'switch' type of 'case' statement");
        }
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

    private static long getLong(Function function, Record record) {
        return function.getLong(record);
    }

    private static short getShort(Function function, Record record) {
        return function.getShort(record);
    }

    private static CharSequence getString(Function function, Record record) {
        return function.getStrA(record);
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
        for (int i = 1; i < n; i += 2) {
            final Function fun = args.getQuick(i);
            final CharSequence key = SwitchFunctionFactory.getString(fun, null);
            if (key == null) {
                nullFunc = args.getQuick(i + 1);
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
        for (int i = 1; i < n; i += 2) {
            final Function fun = args.getQuick(i);
            final long key = Double.doubleToLongBits(getDouble(fun, null));
            final int index = map.keyIndex(key);
            if (index < 0) {
                throw SqlException.$(argPositions.getQuick(i), "duplicate branch");
            }
            map.putAt(index, key, args.getQuick(i + 1));
            argsToPoke.add(args.getQuick(i + 1));
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
        for (int i = 1; i < n; i += 2) {
            final Function fun = args.getQuick(i);
            final int key = Float.floatToIntBits(getFloat(fun, null));
            final int index = map.keyIndex(key);
            if (index < 0) {
                throw SqlException.$(argPositions.getQuick(i), "duplicate branch");
            }
            map.putAt(index, key, args.getQuick(i + 1));
            argsToPoke.add(args.getQuick(i + 1));
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
            // only one conditional branch
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
}
