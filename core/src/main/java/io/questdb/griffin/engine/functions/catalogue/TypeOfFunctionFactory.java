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

import static io.questdb.cairo.ColumnType.GEOLONG_MAX_BITS;

public class TypeOfFunctionFactory implements FunctionFactory {
    static final Function NULL = new StrConstant("NULL");
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
            final Function arg = args.getQuick(0);
            final int argType = arg.getType();
            return ColumnType.isNull(argType) ? NULL : TYPE_NAMES.get(arg.getType());
        }
        throw SqlException.$(position, "exactly one argument expected");
    }

    static {
        TYPE_NAMES = new IntObjHashMap<>();
        TYPE_NAMES.put(ColumnType.BOOLEAN, new StrConstant(ColumnType.nameOf(ColumnType.BOOLEAN)));
        TYPE_NAMES.put(ColumnType.BYTE, new StrConstant(ColumnType.nameOf(ColumnType.BYTE)));
        TYPE_NAMES.put(ColumnType.SHORT, new StrConstant(ColumnType.nameOf(ColumnType.SHORT)));
        TYPE_NAMES.put(ColumnType.CHAR, new StrConstant(ColumnType.nameOf(ColumnType.CHAR)));
        TYPE_NAMES.put(ColumnType.INT, new StrConstant(ColumnType.nameOf(ColumnType.INT)));
        TYPE_NAMES.put(ColumnType.LONG, new StrConstant(ColumnType.nameOf(ColumnType.LONG)));
        TYPE_NAMES.put(ColumnType.DATE, new StrConstant(ColumnType.nameOf(ColumnType.DATE)));
        TYPE_NAMES.put(ColumnType.TIMESTAMP, new StrConstant(ColumnType.nameOf(ColumnType.TIMESTAMP)));
        TYPE_NAMES.put(ColumnType.FLOAT, new StrConstant(ColumnType.nameOf(ColumnType.FLOAT)));
        TYPE_NAMES.put(ColumnType.DOUBLE, new StrConstant(ColumnType.nameOf(ColumnType.DOUBLE)));
        TYPE_NAMES.put(ColumnType.STRING, new StrConstant(ColumnType.nameOf(ColumnType.STRING)));
        TYPE_NAMES.put(ColumnType.SYMBOL, new StrConstant(ColumnType.nameOf(ColumnType.SYMBOL)));
        TYPE_NAMES.put(ColumnType.LONG256, new StrConstant(ColumnType.nameOf(ColumnType.LONG256)));
        TYPE_NAMES.put(ColumnType.BINARY, new StrConstant(ColumnType.nameOf(ColumnType.BINARY)));
        TYPE_NAMES.put(ColumnType.PARAMETER, new StrConstant(ColumnType.nameOf(ColumnType.PARAMETER)));
        TYPE_NAMES.put(ColumnType.CURSOR, new StrConstant(ColumnType.nameOf(ColumnType.CURSOR)));
        TYPE_NAMES.put(ColumnType.VAR_ARG, new StrConstant(ColumnType.nameOf(ColumnType.VAR_ARG)));
        TYPE_NAMES.put(ColumnType.UUID, new StrConstant(ColumnType.nameOf(ColumnType.UUID)));

        TYPE_NAMES.put(ColumnType.GEOBYTE, new StrConstant("null(GEOBYTE)"));
        TYPE_NAMES.put(ColumnType.GEOSHORT, new StrConstant("null(GEOSHORT)"));
        TYPE_NAMES.put(ColumnType.GEOINT, new StrConstant("null(GEOINT)"));
        TYPE_NAMES.put(ColumnType.GEOLONG, new StrConstant("null(GEOLONG)"));

        for (int b = 1; b <= GEOLONG_MAX_BITS; b++) {
            final int type = ColumnType.getGeoHashTypeWithBits(b);
            TYPE_NAMES.put(type, new StrConstant(ColumnType.nameOf(type)));
        }

        TYPE_NAMES.put(ColumnType.IPv4, new StrConstant(ColumnType.nameOf(ColumnType.IPv4)));
    }
}
