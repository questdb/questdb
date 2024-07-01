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
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.std.IntList;
import io.questdb.std.ObjList;

public class JsonPathDefaultTypedFunctionFactory extends JsonPathFunctionFactoryBase {

    @Override
    public JsonPathFunction newInstance(
            int position, ObjList<Function> args, IntList argPositions,
            CairoConfiguration configuration, SqlExecutionContext sqlExecutionContext
    ) throws SqlException {
        final JsonPathFunction fn = super.newInstance(
                position,
                args,
                argPositions,
                configuration,
                sqlExecutionContext
        );

        // Supply an explicit default value in case of execution errors, if one was provided.
        final int targetType = fn.getType();
        final Function defaultValueFn = parseDefaultValueFunction(position, targetType, args);
        if (defaultValueFn != null) {
            fn.applyDefault(position, targetType, defaultValueFn);
        }
        return fn;
    }

    private static boolean isEqualOrWidening(int targetType, int defaultType) {
        switch (targetType) {
            case ColumnType.BOOLEAN:
                return defaultType == ColumnType.BOOLEAN;
            case ColumnType.DOUBLE:
                return defaultType == ColumnType.DOUBLE;
            case ColumnType.FLOAT:
                return (defaultType == ColumnType.FLOAT)
                        || (defaultType == ColumnType.DOUBLE);
            case ColumnType.INT:
                return (defaultType == ColumnType.INT)
                        || (defaultType == ColumnType.SHORT)
                        || (defaultType == ColumnType.BYTE)
                        || (defaultType == ColumnType.BOOLEAN);
            case ColumnType.LONG:
                return (defaultType == ColumnType.LONG)
                        || (defaultType == ColumnType.INT)
                        || (defaultType == ColumnType.SHORT)
                        || (defaultType == ColumnType.BYTE)
                        || (defaultType == ColumnType.BOOLEAN);
            case ColumnType.SHORT:
                return (defaultType == ColumnType.SHORT)
                        || (defaultType == ColumnType.BYTE)
                        || (defaultType == ColumnType.BOOLEAN);
            case ColumnType.VARCHAR:
                return (defaultType == ColumnType.VARCHAR)
                        || (defaultType == ColumnType.STRING)
                        || (defaultType == ColumnType.SYMBOL)
                        || (defaultType == ColumnType.CHAR);
            default:
                return false;
        }
    }

    private Function parseDefaultValueFunction(int position, int targetType, ObjList<Function> args) throws SqlException {
        if (args.size() > 4) {
            throw SqlException
                    .position(position)
                    .put("supplied ")
                    .put(args.size())
                    .put(" arguments to the json_path function, expected either 3 or 4");
        }
        final Function defaultValueFn = args.getQuiet(3);

        final boolean isNullDefault = (defaultValueFn == null) || (defaultValueFn.getType() == ColumnType.NULL);

        if (isNullDefault && !ColumnType.isNullable(targetType)) {
            final String message = (args.size() == 3)
                    ? "json_path's default value must be specified for the "
                    : "json_path's default value cannot be NULL for the ";
            throw SqlException
                    .position(position)
                    .put(message)
                    .put(ColumnType.nameOf(targetType))
                    .put(" target type");
        }

        if (isNullDefault) {
            return null;
        }

        if (!defaultValueFn.isConstant()) {
            throw SqlException.position(position).put("json_path's default value must be a constant");
        }

        if (!isEqualOrWidening(targetType, defaultValueFn.getType())) {
            throw SqlException
                    .position(position)
                    .put("json_path's default value cannot be of type ")
                    .put(ColumnType.nameOf(defaultValueFn.getType()))
                    .put(", expected a value compatible with ")
                    .put(ColumnType.nameOf(targetType));
        }

        return defaultValueFn;
    }

    @Override
    protected String getArguments() {
        return "ØØiV";
    }

    @Override
    protected boolean isStrict() {
        return false;
    }
}
