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

package io.questdb.griffin.engine.functions.conditional;

import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.TimestampDriver;
import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.StaticSymbolTable;
import io.questdb.cairo.sql.SymbolTable;
import io.questdb.cairo.sql.SymbolTableSource;
import io.questdb.griffin.FunctionFactory;
import io.questdb.griffin.PlanSink;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.engine.functions.IntFunction;
import io.questdb.griffin.engine.functions.SymbolFunction;
import io.questdb.griffin.engine.functions.constants.Constants;
import io.questdb.std.CharSequenceObjHashMap;
import io.questdb.std.Chars;
import io.questdb.std.IntList;
import io.questdb.std.IntObjHashMap;
import io.questdb.std.LongObjHashMap;
import io.questdb.std.ObjList;
import org.jetbrains.annotations.NotNull;

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

            if (!ColumnType.isConvertibleFrom(keyArgType, keyType)) {
                throw SqlException.position(argPositions.getQuick(i))
                        .put("type mismatch [expected=").put(ColumnType.nameOf(keyType))
                        .put(", actual=").put(ColumnType.nameOf(keyArgType))
                        .put(']');
            }
            args.setQuick(
                    i,
                    CaseCommon.getCastFunction(
                            args.getQuick(i),
                            argPositions.getQuick(i),
                            keyType,
                            configuration,
                            sqlExecutionContext
                    )
            );

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
        for (int i = 1; i < n; i += 2) {
            final Function fun = args.getQuick(i);
            final CharSequence key = getString(fun, null);
            if (key == null) {
                nullFunc = args.getQuick(i + 1);
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
