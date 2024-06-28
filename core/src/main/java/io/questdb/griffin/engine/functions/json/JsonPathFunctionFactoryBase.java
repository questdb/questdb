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
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.sql.Function;
import io.questdb.griffin.FunctionFactory;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.std.IntList;
import io.questdb.std.ObjList;
import io.questdb.std.str.DirectUtf8Sink;
import org.jetbrains.annotations.NotNull;

public abstract class JsonPathFunctionFactoryBase implements FunctionFactory {
    private String signature;

    public JsonPathFunctionFactoryBase() {
        signature = getFunctionName() + "(" + getArguments() + ")";
    }

    private static @NotNull JsonPathFunction buildFunction(
            String functionName,
            int position,
            Function json,
            Function path,
            int targetType,
            DirectUtf8Sink pointer,
            int maxSize,
            boolean strict) {
        switch (targetType) {
            case ColumnType.LONG:
            case ColumnType.DOUBLE:
            case ColumnType.FLOAT:
            case ColumnType.BOOLEAN:
            case ColumnType.SHORT:
            case ColumnType.INT:
                return new JsonPathPrimitiveFunction(functionName, position, targetType, json, path, pointer, strict);
            case ColumnType.VARCHAR:
                return new JsonPathVarcharFunction(functionName, position, json, path, pointer, maxSize, strict);
            default:
                throw new UnsupportedOperationException("nyi");  // TODO: complete remaining types
        }
    }

    private static final String FUNCTION_NAME = JsonPathFunction.DEFAULT_FUNCTION_NAME;
    private static final String SIGNATURE = FUNCTION_NAME + "(ØøiV)";

    private String getFunctionName() {
        return isStrict()
                ? JsonPathFunction.STRICT_FUNCTION_NAME
                : JsonPathFunction.DEFAULT_FUNCTION_NAME;
    }

    @Override
    public String getSignature() {
        return signature;
    }

    protected abstract String getArguments();

    /**
     * Function execution should fail on error, rather than return a default value.
     */
    protected abstract boolean isStrict();

    @Override
    public JsonPathFunction newInstance(
            int position, ObjList<Function> args, IntList argPositions,
            CairoConfiguration configuration, SqlExecutionContext sqlExecutionContext
    ) throws SqlException {
        final Function json = args.getQuick(0);
        final int maxSize = configuration.getStrFunctionMaxBufferLength();
        final Function path = args.getQuick(1);
        final DirectUtf8Sink pointer = JsonPathFunction.varcharConstantToJsonPointer(path);
        final int targetType = parseTargetType(position, args.getQuiet(2));
        return buildFunction(
                getFunctionName(),
                position,
                json,
                path,
                targetType,
                pointer,
                maxSize,
                isStrict()
        );
    }

    private int parseTargetType(int position, Function targetTypeFn) throws SqlException {
        if (targetTypeFn == null) {
            return ColumnType.VARCHAR;
        }
        if (!targetTypeFn.isConstant()) {
            throw SqlException.position(position).put("target type must be constant");
        }
        // TODO: This isn't _really_ a int, it's supposed to be a type constant.
        //       Make it so in the parser.
        if (targetTypeFn.getType() != ColumnType.INT) {
            throw SqlException.position(position).put("target type must be INT");
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
                throw SqlException.position(position).put("unsupported target type: ").put(targetType);
        }
    }
}
