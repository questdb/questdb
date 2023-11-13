/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2023 QuestDB
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

package io.questdb.griffin.engine.functions.catalogue;

import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.sql.Function;
import io.questdb.griffin.FunctionFactory;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.engine.functions.constants.StrConstant;
import io.questdb.std.IntList;
import io.questdb.std.IntObjHashMap;
import io.questdb.std.ObjList;


public class TypeOfFunctionFactory implements FunctionFactory {
    static final IntObjHashMap<Function> TYPE_NAMES;

    @Override
    public String getSignature() {
        return "typeOf(V)";
    }

    @Override
    public Function newInstance(
            int position,
            ObjList<Function> args,
            IntList argPositions,
            CairoConfiguration configuration,
            SqlExecutionContext sqlExecutionContext
    ) throws SqlException {
        if (args != null && args.size() == 1) {
            return TYPE_NAMES.get(args.getQuick(0).getType());
        }
        throw SqlException.$(position, "exactly one argument expected");
    }

    static {
        TYPE_NAMES = new IntObjHashMap<>();
        for (int type = ColumnType.BOOLEAN; type <= ColumnType.NULL; type++) {
            TYPE_NAMES.put(type, new StrConstant(ColumnType.nameOf(type)));
        }
        TYPE_NAMES.put(ColumnType.GEOBYTE, new StrConstant("null(GEOBYTE)"));
        TYPE_NAMES.put(ColumnType.GEOSHORT, new StrConstant("null(GEOSHORT)"));
        TYPE_NAMES.put(ColumnType.GEOINT, new StrConstant("null(GEOINT)"));
        TYPE_NAMES.put(ColumnType.GEOLONG, new StrConstant("null(GEOLONG)"));
        for (int b = 1; b <= ColumnType.GEOLONG_MAX_BITS; b++) {
            final int type = ColumnType.getGeoHashTypeWithBits(b);
            TYPE_NAMES.put(type, new StrConstant(ColumnType.nameOf(type)));
        }
    }
}
