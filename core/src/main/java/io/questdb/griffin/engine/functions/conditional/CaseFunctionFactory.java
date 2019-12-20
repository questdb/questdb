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

package io.questdb.griffin.engine.functions.conditional;

import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.sql.Function;
import io.questdb.griffin.FunctionFactory;
import io.questdb.griffin.SqlCompiler;
import io.questdb.griffin.SqlException;
import io.questdb.std.ObjList;

public class CaseFunctionFactory implements FunctionFactory {
    private static ObjList<CaseFunctionConstructor> constructors = new ObjList<>();

    static {
        constructors.set(0, ColumnType.MAX, null);
        constructors.setQuick(ColumnType.SYMBOL, StrCaseFunction::new);
        constructors.setQuick(ColumnType.STRING, StrCaseFunction::new);
        constructors.setQuick(ColumnType.DOUBLE, DoubleCaseFunction::new);
        constructors.setQuick(ColumnType.FLOAT, FloatCaseFunction::new);
        constructors.setQuick(ColumnType.LONG, LongCaseFunction::new);
        constructors.setQuick(ColumnType.INT, IntCaseFunction::new);
        constructors.setQuick(ColumnType.SHORT, ShortCaseFunction::new);
        constructors.setQuick(ColumnType.BINARY, BinCaseFunction::new);
        constructors.setQuick(ColumnType.CHAR, CharCaseFunction::new);
        constructors.setQuick(ColumnType.BYTE, ByteCaseFunction::new);
        constructors.setQuick(ColumnType.BOOLEAN, BooleanCaseFunction::new);
        constructors.setQuick(ColumnType.DATE, DateCaseFunction::new);
        constructors.setQuick(ColumnType.TIMESTAMP, TimestampCaseFunction::new);
        constructors.setQuick(ColumnType.LONG256, Long256CaseFunction::new);
    }

    @Override
    public String getSignature() {
        return "case(V)";
    }

    @Override
    public Function newInstance(ObjList<Function> args, int position, CairoConfiguration configuration) throws SqlException {
        int n = args.size();
        int returnType = -1;
        final ObjList<Function> vars = new ObjList<>(n);

        Function elseBranch;
        if (n % 2 == 1) {
            elseBranch = args.getQuick(n - 1);
            n--;
        } else {
            elseBranch = null;
        }


        for (int i = 0; i < n; i += 2) {
            Function bool = args.getQuick(i);
            Function outcome = args.getQuick(i + 1);

            if (bool.getType() != ColumnType.BOOLEAN) {
                throw SqlException.position(bool.getPosition()).put("BOOLEAN expected, found ").put(ColumnType.nameOf(bool.getType()));
            }

            if (i == 0) {
                returnType = outcome.getType();
            } else if (!SqlCompiler.isAssignableFrom(returnType, outcome.getType())) {
                throw SqlException.position(outcome.getPosition()).put(ColumnType.nameOf(returnType)).put(" expected, found ").put(ColumnType.nameOf(outcome.getType()));
            }

            vars.add(bool);
            vars.add(outcome);
        }

        if (elseBranch != null && !SqlCompiler.isAssignableFrom(returnType, elseBranch.getType())) {
            throw SqlException.position(elseBranch.getPosition()).put(ColumnType.nameOf(returnType)).put(" expected, found ").put(ColumnType.nameOf(elseBranch.getType()));
        }

        final CaseFunctionConstructor constructor = constructors.getQuick(returnType);
        if (constructor == null) {
            throw SqlException.$(position, "not implemented for type '").put(ColumnType.nameOf(returnType)).put('\'');
        }

        return constructor.newInstance(position, vars, elseBranch);
    }

    @FunctionalInterface
    private interface CaseFunctionConstructor {
        Function newInstance(int position, ObjList<Function> vars, Function elseBranch);
    }
}
