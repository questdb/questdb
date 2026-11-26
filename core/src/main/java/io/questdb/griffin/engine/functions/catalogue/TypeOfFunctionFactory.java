/*+*****************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2026 QuestDB
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
import io.questdb.cairo.ColumnTypeTag;
import io.questdb.cairo.sql.Function;
import io.questdb.griffin.FunctionFactory;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.engine.functions.constants.StrConstant;
import io.questdb.griffin.engine.functions.constants.VarcharConstant;
import io.questdb.std.IntList;
import io.questdb.std.Misc;
import io.questdb.std.ObjList;

import static io.questdb.cairo.ColumnType.*;

public class TypeOfFunctionFactory implements FunctionFactory {
    private static final Function NULL = new StrConstant("NULL");

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
            final Function result = typeName(argType, position);
            // the returned constant keeps no argument, so this branch owns it
            Misc.free(arg);
            return result;
        }
        throw SqlException.$(position, "exactly one argument expected");
    }

    @Override
    public int resolvePreferredVariadicType(int sqlPos, int argPos, ObjList<Function> args) throws SqlException {
        throw SqlException.$(sqlPos, "bind variables are not supported");
    }

    // the name of the argument's type, as the type driver gives it; a bare geohash tag, which
    // names no width, answers in its own form
    private static Function typeName(int argType, int position) throws SqlException {
        if (isNull(argType)) {
            return NULL;
        }
        final short tag = tagOf(argType);
        if (argType == tag && tag >= GEOBYTE && tag <= GEOLONG) {
            return new StrConstant("null(" + ColumnTypeTag.of(tag).name() + ")");
        }
        final String name = nameOf(argType);
        if (UNKNOWN_NAME.equals(name)) {
            throw SqlException.$(position, "typeOf: the argument's type has no name [type=").put(argType).put(']');
        }
        return argType == VARCHAR ? new VarcharConstant(name) : new StrConstant(name);
    }
}
