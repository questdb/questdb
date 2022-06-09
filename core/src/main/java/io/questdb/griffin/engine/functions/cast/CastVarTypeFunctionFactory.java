/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2022 QuestDB
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

package io.questdb.griffin.engine.functions.cast;

import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.sql.Function;
import io.questdb.griffin.FunctionFactory;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.engine.functions.constants.Constants;
import io.questdb.griffin.engine.functions.constants.IntConstant;
import io.questdb.griffin.engine.functions.date.ToPgDateFunctionFactory;
import io.questdb.std.CharSequenceObjHashMap;
import io.questdb.std.IntList;
import io.questdb.std.ObjList;

import static io.questdb.cutlass.pgwire.PGOids.PG_CLASS_OID;
import static io.questdb.cutlass.pgwire.PGOids.PG_NAMESPACE_OID;

public class CastVarTypeFunctionFactory implements FunctionFactory {
    @Override
    public String getSignature() {
        return "cast(oV)";
    }

    @Override
    public Function newInstance(int position, ObjList<Function> args, IntList argPositions, CairoConfiguration configuration, SqlExecutionContext sqlExecutionContext) throws SqlException {
        if (args.size() == 2) {
            final Function arg = args.getQuick(0);
            int castFrom = arg.getType();
            int castTo = args.getQuick(1).getType();

            if (castFrom == ColumnType.NULL) {
                // todo: we should try to cast NULL to REGCLASS or NULL
                return Constants.getNullConstant(castTo);
            }

            if (castFrom == ColumnType.STRING && castTo == ColumnType.REGCLASS) {
                Function res = map.get(arg.getStr(null));
                if (res != null) {
                    return res;
                }
                throw SqlException.$(argPositions.getQuick(0), "unsupported class [name=").put(arg.getStr(null)).put(']');
            }

            if (castFrom == ColumnType.STRING && castTo == ColumnType.PGDATE) {
                return new ToPgDateFunctionFactory.ToPgDateFunction(arg);
            }

            throw SqlException
                    .$(argPositions.getQuick(0), "unsupported cast [from=").put(ColumnType.nameOf(castFrom))
                    .put(", to=").put(ColumnType.nameOf(castTo))
                    .put(']');
        }
        throw SqlException.$(position, "cast accepts 2 arguments");
    }

    private static final CharSequenceObjHashMap<Function> map = new CharSequenceObjHashMap<>();

    static {
        map.put("pg_namespace", new IntConstant(PG_NAMESPACE_OID));
        map.put("pg_class", new IntConstant(PG_CLASS_OID));
    }

}
