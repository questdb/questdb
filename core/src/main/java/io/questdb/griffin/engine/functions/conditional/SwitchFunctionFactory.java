/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2020 QuestDB
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
import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.Record;
import io.questdb.griffin.FunctionFactory;
import io.questdb.griffin.SqlCompiler;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.engine.functions.constants.Constants;
import io.questdb.std.CharSequenceObjHashMap;
import io.questdb.std.IntObjHashMap;
import io.questdb.std.LongObjHashMap;
import io.questdb.std.ObjList;

public class SwitchFunctionFactory implements FunctionFactory {

    private static char getChar(Function function, Record record) {
        return function.getChar(record);
    }

    private static int getInt(Function function, Record record) {
        return function.getInt(record);
    }

    private static byte getByte(Function function, Record record) {
        return function.getByte(record);
    }

    private static short getShort(Function function, Record record) {
        return function.getShort(record);
    }

    private static int getFloat(Function function, Record record) {
        return Float.floatToIntBits(function.getFloat(record));
    }

    private static long getLong(Function function, Record record) {
        return function.getLong(record);
    }

    private static long getDate(Function function, Record record) {
        return function.getDate(record);
    }

    private static long getTimestamp(Function function, Record record) {
        return function.getTimestamp(record);
    }

    private static long getDouble(Function function, Record record) {
        return Double.doubleToLongBits(function.getDouble(record));
    }

    private static CharSequence getString(Function function, Record record) {
        return function.getStr(record);
    }

    private static CharSequence getSymbol(Function function, Record record) {
        return function.getSymbol(record);
    }

    @Override
    public String getSignature() {
        return "switch(V)";
    }

    @Override
    public Function newInstance(ObjList<Function> args, int position, CairoConfiguration configuration) throws SqlException {
        int n = args.size();

        final Function keyFunction = args.getQuick(0);
        final int keyType = keyFunction.getType();
        final int returnType = args.getQuick(2).getType();
        final Function elseBranch;
        if (n % 2 == 0) {
            elseBranch = args.getLast();
            n--;
        } else {
            elseBranch = null;
        }

        for (int i = 1; i < n; i += 2) {
            final Function keyFunc = args.getQuick(i);
            final int keyArgType = keyFunc.getType();
            if (!keyFunc.isConstant()) {
                throw SqlException.$(keyFunc.getPosition(), "constant expected");
            }

            if (!SqlCompiler.isAssignableFrom(keyType, keyArgType)) {
                throw SqlException.position(keyFunc.getPosition())
                        .put("type mismatch [expected=").put(ColumnType.nameOf(keyType))
                        .put(", actual=").put(ColumnType.nameOf(keyArgType))
                        .put(']');
            }

            final Function valueFunc = args.getQuick(i + 1);
            if (!SqlCompiler.isAssignableFrom(returnType, valueFunc.getType())) {
                throw SqlException.position(valueFunc.getPosition())
                        .put("type mismatch [expected=").put(ColumnType.nameOf(returnType))
                        .put(", actual=").put(ColumnType.nameOf(valueFunc.getType()))
                        .put(']');
            }
        }

        switch (keyType) {
            case ColumnType.CHAR:
                return getIntKeyedFunction(args, position, n, keyFunction, returnType, elseBranch, SwitchFunctionFactory::getChar);
            case ColumnType.INT:
                return getIntKeyedFunction(args, position, n, keyFunction, returnType, elseBranch, SwitchFunctionFactory::getInt);
            case ColumnType.BYTE:
                return getIntKeyedFunction(args, position, n, keyFunction, returnType, elseBranch, SwitchFunctionFactory::getByte);
            case ColumnType.SHORT:
                return getIntKeyedFunction(args, position, n, keyFunction, returnType, elseBranch, SwitchFunctionFactory::getShort);
            case ColumnType.FLOAT:
                return getIntKeyedFunction(args, position, n, keyFunction, returnType, elseBranch, SwitchFunctionFactory::getFloat);
            case ColumnType.LONG:
                return getLongKeyedFunction(args, position, n, keyFunction, returnType, elseBranch, SwitchFunctionFactory::getLong);
            case ColumnType.DATE:
                return getLongKeyedFunction(args, position, n, keyFunction, returnType, elseBranch, SwitchFunctionFactory::getDate);
            case ColumnType.TIMESTAMP:
                return getLongKeyedFunction(args, position, n, keyFunction, returnType, elseBranch, SwitchFunctionFactory::getTimestamp);
            case ColumnType.DOUBLE:
                return getLongKeyedFunction(args, position, n, keyFunction, returnType, elseBranch, SwitchFunctionFactory::getDouble);
            case ColumnType.BOOLEAN:
                return getIfElseFunction(args, position, n, keyFunction, returnType, elseBranch);
            case ColumnType.STRING:
                return getCharSequenceKeyedFunction(args, position, n, keyFunction, returnType, elseBranch, SwitchFunctionFactory::getString);
            case ColumnType.SYMBOL:
                return getCharSequenceKeyedFunction(args, position, n, keyFunction, returnType, elseBranch, SwitchFunctionFactory::getSymbol);
            default:
                return null;
        }
    }

    private Function getIntKeyedFunction(
            ObjList<Function> args,
            int position, int n,
            Function keyFunction,
            int valueType,
            Function elseBranch,
            IntMethod intMethod
    ) throws SqlException {
        final IntObjHashMap<Function> map = new IntObjHashMap<>();
        for (int i = 1; i < n; i += 2) {
            final Function fun = args.getQuick(i);
            final int key = intMethod.getKey(fun, null);
            final int index = map.keyIndex(key);
            if (index < 0) {
                throw SqlException.$(fun.getPosition(), "duplicate branch");
            }
            map.putAt(index, key, args.getQuick(i + 1));
        }

        final Function elseB = getElseFunction(valueType, elseBranch);
        final CaseFunctionPicker picker = record -> {
            final int index = map.keyIndex(intMethod.getKey(keyFunction, record));
            if (index < 0) {
                return map.valueAtQuick(index);
            }
            return elseB;
        };

        return CaseCommon.getCaseFunction(position, valueType, picker);
    }

    private Function getElseFunction(int valueType, Function elseBranch) {
        return elseBranch != null ? elseBranch : Constants.getNullConstant(valueType);
    }

    private Function getIfElseFunction(
            ObjList<Function> args,
            int position, int n,
            Function keyFunction,
            int returnType,
            Function elseBranch
    ) throws SqlException {
        final CaseFunctionPicker picker;
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
        } else if (n == 5) {
            final boolean a = args.getQuick(1).getBool(null);
            final Function branchA = args.getQuick(2);
            final boolean b = args.getQuick(3).getBool(null);
            final Function branchB = args.getQuick(4);

            if (a && b || !a && !b) {
                throw SqlException.$(args.getQuick(3).getPosition(), "duplicate branch");
            }

            if (elseBranch != null) {
                throw SqlException.$(elseBranch.getPosition(), "duplicate of boolean values");
            }

            if (a) {
                picker = record -> keyFunction.getBool(record) ? branchA : branchB;
            } else {
                picker = record -> keyFunction.getBool(record) ? branchB : branchA;
            }

        } else {
            throw SqlException.$(args.getQuick(5).getPosition(), "too many branches");
        }

        return CaseCommon.getCaseFunction(position, returnType, picker);
    }

    private Function getLongKeyedFunction(
            ObjList<Function> args,
            int position,
            int n,
            Function keyFunction,
            int valueType,
            Function elseBranch,
            LongMethod longMethod
    ) throws SqlException {
        final LongObjHashMap<Function> map = new LongObjHashMap<>();
        for (int i = 1; i < n; i += 2) {
            final Function fun = args.getQuick(i);
            final long key = longMethod.getKey(fun, null);
            final int index = map.keyIndex(key);
            if (index < 0) {
                throw SqlException.$(fun.getPosition(), "duplicate branch");
            }
            map.putAt(index, key, args.getQuick(i + 1));
        }

        final Function elseB = getElseFunction(valueType, elseBranch);
        final CaseFunctionPicker picker = record -> {
            final int index = map.keyIndex(longMethod.getKey(keyFunction, record));
            if (index < 0) {
                return map.valueAtQuick(index);
            }
            return elseB;
        };

        return CaseCommon.getCaseFunction(position, valueType, picker);
    }


    private Function getCharSequenceKeyedFunction(
            ObjList<Function> args,
            int position,
            int n,
            Function keyFunction,
            int valueType,
            Function elseBranch,
            CharSequenceMethod method
    ) throws SqlException {
        final CharSequenceObjHashMap<Function> map = new CharSequenceObjHashMap<>();
        for (int i = 1; i < n; i += 2) {
            final Function fun = args.getQuick(i);
            final CharSequence key = method.getKey(fun, null);
            final int index = map.keyIndex(key);
            if (index < 0) {
                throw SqlException.$(fun.getPosition(), "duplicate branch");
            }
            map.putAt(index, key, args.getQuick(i + 1));
        }

        final Function elseB = getElseFunction(valueType, elseBranch);
        final CaseFunctionPicker picker = record -> {
            final int index = map.keyIndex(method.getKey(keyFunction, record));
            if (index < 0) {
                return map.valueAtQuick(index);
            }
            return elseB;
        };

        return CaseCommon.getCaseFunction(position, valueType, picker);
    }

    @FunctionalInterface
    private interface IntMethod {
        int getKey(Function function, Record record);
    }

    @FunctionalInterface
    private interface LongMethod {
        long getKey(Function function, Record record);
    }

    @FunctionalInterface
    private interface CharSequenceMethod {
        CharSequence getKey(Function function, Record record);
    }
}
