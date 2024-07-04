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
import io.questdb.std.Misc;
import io.questdb.std.ObjList;

public class JsonExtractPrimitiveFunctionFactory implements FunctionFactory {

    @Override
    public String getSignature() {
        return JsonExtractSupportingState.EXTRACT_FUNCTION_NAME + "(ØØi)";
    }

    @Override
    public Function newInstance(
            int position,
            ObjList<Function> args,
            IntList argPositions,
            CairoConfiguration configuration,
            SqlExecutionContext sqlExecutionContext
    ) throws SqlException {
        final Function json = args.getQuick(0);
        final Function path = args.getQuick(1);

        if (path.getType() != ColumnType.UNDEFINED && !path.isConstant() && !path.isRuntimeConstant()) {
            Misc.freeObjList(args);
            throw SqlException.$(argPositions.getQuick(1), "constant or bind variable expected");
        }

        final int targetType = parseTargetType(position, args.getQuick(2));
        return new JsonExtractPrimitiveFunction(
                argPositions.getQuick(0),
                targetType,
                json,
                path
        );
    }

    private static int parseTargetType(int position, Function targetTypeFn) throws SqlException {
        // this is internal undocumented function, which is triggered via SQL rewrite.
        // The invocation is triggered by calling `json_extract(json,path)::type`.
        // Therefore, we have to validate type input to provide user with actionable error message.
        if (targetTypeFn != null && targetTypeFn.isConstant()) {
            final int targetType = targetTypeFn.getInt(null);
            switch (targetType) {
                case ColumnType.BOOLEAN:
                case ColumnType.SHORT:
                case ColumnType.INT:
                case ColumnType.LONG:
                case ColumnType.FLOAT:
                case ColumnType.DOUBLE:
                case ColumnType.TIMESTAMP:
                case ColumnType.DATE:
                    return targetType;
            }
        }
        throw SqlException.position(position).put("please use json_extract(json,path)::type semantic");
    }
}
