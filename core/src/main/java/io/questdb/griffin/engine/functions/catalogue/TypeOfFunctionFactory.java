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

package io.questdb.griffin.engine.functions.catalogue;

import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.sql.Function;
import io.questdb.griffin.FunctionFactory;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.engine.functions.constants.StrConstant;
import io.questdb.griffin.engine.functions.constants.VarcharConstant;
import io.questdb.std.IntList;
import io.questdb.std.IntObjHashMap;
import io.questdb.std.ObjList;

import static io.questdb.cairo.ColumnType.*;

public class TypeOfFunctionFactory implements FunctionFactory {
    private static final Function NULL = new StrConstant("NULL");
    private static final IntObjHashMap<Function> TYPE_NAMES = new IntObjHashMap<>();

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
            if (argType == UNDEFINED) {
                throw SqlException.$(position, "bind variables are not supported");
            }
            return isNull(argType) ? NULL : TYPE_NAMES.get(argType);
        }
        throw SqlException.$(position, "exactly one argument expected");
    }

    @Override
    public int resolvePreferredVariadicType(int sqlPos, int argPos, ObjList<Function> args) throws SqlException {
        throw SqlException.$(sqlPos, "bind variables are not supported");
    }

    static {
        TYPE_NAMES.put(BOOLEAN, new StrConstant(nameOf(BOOLEAN)));
        TYPE_NAMES.put(BYTE, new StrConstant(nameOf(BYTE)));
        TYPE_NAMES.put(SHORT, new StrConstant(nameOf(SHORT)));
        TYPE_NAMES.put(CHAR, new StrConstant(nameOf(CHAR)));
        TYPE_NAMES.put(INT, new StrConstant(nameOf(INT)));
        TYPE_NAMES.put(LONG, new StrConstant(nameOf(LONG)));
        TYPE_NAMES.put(DATE, new StrConstant(nameOf(DATE)));
        TYPE_NAMES.put(TIMESTAMP_MICRO, new StrConstant(nameOf(TIMESTAMP_MICRO)));
        TYPE_NAMES.put(TIMESTAMP_NANO, new StrConstant(nameOf(TIMESTAMP_NANO)));
        TYPE_NAMES.put(FLOAT, new StrConstant(nameOf(FLOAT)));
        TYPE_NAMES.put(DOUBLE, new StrConstant(nameOf(DOUBLE)));
        TYPE_NAMES.put(STRING, new StrConstant(nameOf(STRING)));
        TYPE_NAMES.put(SYMBOL, new StrConstant(nameOf(SYMBOL)));
        TYPE_NAMES.put(LONG256, new StrConstant(nameOf(LONG256)));
        TYPE_NAMES.put(BINARY, new StrConstant(nameOf(BINARY)));
        TYPE_NAMES.put(PARAMETER, new StrConstant(nameOf(PARAMETER)));
        TYPE_NAMES.put(CURSOR, new StrConstant(nameOf(CURSOR)));
        TYPE_NAMES.put(VAR_ARG, new StrConstant(nameOf(VAR_ARG)));
        TYPE_NAMES.put(UUID, new StrConstant(nameOf(UUID)));

        TYPE_NAMES.put(GEOBYTE, new StrConstant("null(GEOBYTE)"));
        TYPE_NAMES.put(GEOSHORT, new StrConstant("null(GEOSHORT)"));
        TYPE_NAMES.put(GEOINT, new StrConstant("null(GEOINT)"));
        TYPE_NAMES.put(GEOLONG, new StrConstant("null(GEOLONG)"));

        for (int b = 1; b <= GEOLONG_MAX_BITS; b++) {
            final int type = getGeoHashTypeWithBits(b);
            TYPE_NAMES.put(type, new StrConstant(nameOf(type)));
        }

        TYPE_NAMES.put(IPv4, new StrConstant(nameOf(IPv4)));
        TYPE_NAMES.put(VARCHAR, new VarcharConstant(nameOf(VARCHAR)));
        TYPE_NAMES.put(INTERVAL_RAW, new StrConstant(nameOf(INTERVAL)));
        TYPE_NAMES.put(INTERVAL_TIMESTAMP_MICRO, new StrConstant(nameOf(INTERVAL)));
        TYPE_NAMES.put(INTERVAL_TIMESTAMP_NANO, new StrConstant(nameOf(INTERVAL)));
    }
}
