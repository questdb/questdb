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

package io.questdb.griffin.engine.functions.json;

import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.CairoException;
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.sql.Function;
import io.questdb.griffin.FunctionFactory;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.std.IntList;
import io.questdb.std.ObjList;
import io.questdb.std.str.DirectUtf8Sink;
import org.jetbrains.annotations.NotNull;

public class JsonPathFunctionFactory implements FunctionFactory {
    private static final String FUNCTION_NAME = "json_path";
    private static final String SIGNATURE = FUNCTION_NAME + "(ØøV)";

    @Override
    public String getSignature() {
        return SIGNATURE;
    }

    @Override
    public Function newInstance(
            int position, ObjList<Function> args, IntList argPositions,
            CairoConfiguration configuration, SqlExecutionContext sqlExecutionContext
    ) {
        final Function json = args.getQuick(0);
        final Function path = args.getQuick(1);
        final int targetType = parseTargetType(args.getQuiet(2));
        final int onError = parseOnError(args.getQuiet(3));
        final Function defaultValueFn = args.getQuiet(4);
        final DirectUtf8Sink pointer = SupportingState.varcharConstantToJsonPointer(path);
        final int maxSize = configuration.getStrFunctionMaxBufferLength();
        final boolean strict = onError == JsonPathFunc.FAIL_ON_ERROR;
        final JsonPathFunc fn = buildFunction(json, path, targetType, pointer, maxSize, strict);
        applyDefault(fn, targetType, onError, defaultValueFn);
        return fn;
    }

    private static @NotNull JsonPathFunc buildFunction(
            Function json,
            Function path,
            int targetType,
            DirectUtf8Sink pointer,
            int maxSize,
            boolean strict) {
        switch (targetType) {
            case ColumnType.LONG:
            case ColumnType.DOUBLE:
            case ColumnType.BOOLEAN:
            case ColumnType.SHORT:
            case ColumnType.INT:
                return new JsonConstPathPrimitiveFunc(targetType, FUNCTION_NAME, json, path, pointer, strict);
            case ColumnType.VARCHAR:
                return new JsonPathVarcharFunc(FUNCTION_NAME, json, path, pointer, maxSize, false);
            default:
                throw new UnsupportedOperationException("nyi");
        }
    }

    private static void applyDefault(JsonPathFunc fn, int targetType, int onError, Function defaultValueFn) {
        if ((defaultValueFn == null) || (onError == JsonPathFunc.FAIL_ON_ERROR)) {
            return;
        }
        if (!defaultValueFn.isConstant()) {
            throw CairoException.nonCritical().put("default value must be constant");
        }
        switch (targetType) {
            case ColumnType.LONG:
                fn.setDefaultLong(defaultValueFn.getLong(null));
                break;
            case ColumnType.DOUBLE:
                fn.setDefaultDouble(defaultValueFn.getDouble(null));
                break;
            case ColumnType.BOOLEAN:
                fn.setDefaultBool(defaultValueFn.getBool(null));
                break;
            case ColumnType.SYMBOL:
                fn.setDefaultSymbol(defaultValueFn.getSymbol(null));
                break;
            case ColumnType.SHORT:
                fn.setDefaultShort(defaultValueFn.getShort(null));
                break;
            case ColumnType.INT:
                fn.setDefaultInt(defaultValueFn.getInt(null));
                break;
            case ColumnType.FLOAT:
                fn.setDefaultFloat(defaultValueFn.getFloat(null));
                break;
            case ColumnType.VARCHAR:
                fn.setDefaultVarchar(defaultValueFn.getVarcharA(null));
                break;
            default:
                // TODO: Better error messaging to use the name of the type, e.g. "DATE" instead of "7".
                throw CairoException.nonCritical().put("unsupported target type: ").put(targetType);
        }
    }

    private static int parseOnError(Function onErrorFn) {
        if (onErrorFn == null) {
            // If the behaviour is unspecified,
            // default to returning null or the default value.
            return JsonPathFunc.DEFAULT_VALUE_ON_ERROR;
        }
        if (!onErrorFn.isConstant()) {
            throw CairoException.nonCritical().put("onError must be constant");
        }
        // TODO: This isn't _really_ a int, it's supposed to be an enum constant.
        //       Valid values: 0 (FAIL_ON_ERROR), 1 (DEFAULT_VALUE_ON_ERROR)
        if (onErrorFn.getType() != ColumnType.INT) {
            throw CairoException.nonCritical().put("onError must be INT");
        }
        final int onError = onErrorFn.getInt(null);
        if (onError != JsonPathFunc.FAIL_ON_ERROR && onError != JsonPathFunc.DEFAULT_VALUE_ON_ERROR) {
            // TODO: Better error messaging to use the name of the enum constant, e.g. "FAIL_ON_ERROR" instead of "0".
            throw CairoException.nonCritical().put("unsupported onError value: ").put(onError);
        }
        return onError;
    }

    private static int parseTargetType(Function targetTypeFn) {
        if (targetTypeFn == null) {
            return ColumnType.VARCHAR;
        }
        if (!targetTypeFn.isConstant()) {
            throw CairoException.nonCritical().put("target type must be constant");
        }
        // TODO: This isn't _really_ a int, it's supposed to be a type constant.
        //       Make it so in the parser.
        if (targetTypeFn.getType() != ColumnType.INT) {
            throw CairoException.nonCritical().put("target type must be INT");
        }
        final int targetType = targetTypeFn.getInt(null);
        switch (targetType) {
            case ColumnType.LONG:
            case ColumnType.DOUBLE:
            case ColumnType.BOOLEAN:
            case ColumnType.SYMBOL:
            case ColumnType.SHORT:
            case ColumnType.INT:
            case ColumnType.FLOAT:
            case ColumnType.VARCHAR:
                return targetType;
            default:
                // TODO: Better error messaging to use the name of the type, e.g. "DATE" instead of "7".
                throw CairoException.nonCritical().put("unsupported target type: ").put(targetTypeFn.getInt(null));
        }
    }
}
