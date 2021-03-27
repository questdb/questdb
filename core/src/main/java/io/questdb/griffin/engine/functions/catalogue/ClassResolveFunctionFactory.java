/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2020 QuestDB
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
import io.questdb.griffin.SqlKeywords;
import io.questdb.griffin.engine.functions.constants.IntConstant;
import io.questdb.griffin.engine.functions.date.ToPgDateFunctionFactory;
import io.questdb.griffin.engine.functions.date.ToTimestampFunctionFactory;
import io.questdb.std.CharSequenceObjHashMap;
import io.questdb.std.ObjList;

import static io.questdb.cutlass.pgwire.PGOids.PG_CLASS_OID;
import static io.questdb.cutlass.pgwire.PGOids.PG_NAMESPACE_OID;

public class ClassResolveFunctionFactory implements FunctionFactory {
    private static final CharSequenceObjHashMap<IntConstant> map = new CharSequenceObjHashMap<>();

    @Override
    public String getSignature() {
        return "::(ss)";
    }

    @Override
    public Function newInstance(ObjList<Function> args, int position, CairoConfiguration configuration, SqlExecutionContext sqlExecutionContext) throws SqlException {
        final Function nameFunction = args.getQuick(0);
        final CharSequence type = args.getQuick(1).getStr(null);

        if (SqlKeywords.isRegclassKeyword(type)) {
            final IntConstant func = map.get(nameFunction.getStr(null));
            if (func != null) {
                return func;
            }
            throw SqlException.$(nameFunction.getPosition(), "unsupported class");
        }

        if (SqlKeywords.isTimestampKeyword(type)) {
            return new ToTimestampFunctionFactory.ToTimestampFunction(nameFunction.getPosition(), nameFunction);
        }

        if (SqlKeywords.isDateKeyword(type)) {
            return new ToPgDateFunctionFactory.ToPgDateFunction(nameFunction.getPosition(), nameFunction);
        }

        if (SqlKeywords.isTextArrayKeyword(type)) {
            return new StringToStringArrayFunction(nameFunction.getPosition(), nameFunction.getStr(null));
        }

        throw SqlException.$(args.getQuick(1).getPosition(), "unsupported type");
    }

    static {
        map.put("pg_namespace", new IntConstant(0, PG_NAMESPACE_OID));
        map.put("pg_class", new IntConstant(0, PG_CLASS_OID));
    }
}
