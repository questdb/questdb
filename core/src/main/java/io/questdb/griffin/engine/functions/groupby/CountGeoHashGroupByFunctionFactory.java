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

package io.questdb.griffin.engine.functions.groupby;

import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.GeoHashes;
import io.questdb.cairo.sql.Function;
import io.questdb.griffin.FunctionFactory;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.std.IntList;
import io.questdb.std.ObjList;

public class CountGeoHashGroupByFunctionFactory implements FunctionFactory {
    @Override
    public String getSignature() {
        return "count(G)";
    }

    @Override
    public boolean isGroupBy() {
        return true;
    }

    @Override
    public Function newInstance(int position, ObjList<Function> args, IntList argPositions, CairoConfiguration configuration, SqlExecutionContext sqlExecutionContext)
            throws SqlException {
        Function arg = args.getQuick(0);
        int type = arg.getType();
        if (arg.isConstant()) {
            if (value(arg) == GeoHashes.NULL) {
                throw SqlException.$(argPositions.getQuick(0), "NULL is not allowed");
            }
            return new CountLongConstGroupByFunction();
        }


        switch (ColumnType.tagOf(type)) {
            case ColumnType.GEOBYTE:
                return new CountGeoHashGroupByFunctionByte(arg);
            case ColumnType.GEOSHORT:
                return new CountGeoHashGroupByFunctionShort(arg);
            case ColumnType.GEOINT:
                return new CountGeoHashGroupByFunctionInt(arg);
            default:
                return new CountGeoHashGroupByFunctionLong(arg);
        }
    }

    private long value(Function arg) {
        switch (ColumnType.tagOf(arg.getType())) {
            case ColumnType.GEOBYTE:
                return arg.getGeoByte(null);
            case ColumnType.GEOSHORT:
                return arg.getGeoShort(null);
            case ColumnType.GEOINT:
                return arg.getGeoInt(null);
            default:
                return arg.getGeoLong(null);
        }
    }
}

