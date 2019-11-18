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
import io.questdb.cairo.sql.Record;
import io.questdb.griffin.FunctionFactory;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.engine.functions.StrFunction;
import io.questdb.std.ObjList;

public class CaseFunctionFactory implements FunctionFactory {
    @Override
    public String getSignature() {
        return "case(V)";
    }

    @Override
    //todo: unit test this and add support for all types
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
            } else if (returnType != outcome.getType()) {
                throw SqlException.position(outcome.getPosition()).put(ColumnType.nameOf(returnType)).put(" expected, found ").put(ColumnType.nameOf(outcome.getType()));
            }

            vars.add(bool);
            vars.add(outcome);
        }

        switch (returnType) {
            case ColumnType.STRING:
                return new StrCaseFunction(position, vars, elseBranch);
            default:
                throw SqlException.$(position, "not implemented for type '").put(ColumnType.nameOf(returnType)).put('\'');

        }
    }

    private static class StrCaseFunction extends StrFunction {
        private final ObjList<Function> args;
        private final int argsLen;
        private final Function elseBranch;

        public StrCaseFunction(int position, ObjList<Function> args, Function elseBranch) {
            super(position);
            this.args = args;
            this.argsLen = args.size();
            this.elseBranch = elseBranch;
        }

        @Override
        public CharSequence getStr(Record rec) {
            for (int i = 0; i < argsLen; i += 2) {
                if (args.getQuick(i).getBool(rec)) {
                    return args.getQuick(i + 1).getStr(rec);
                }
            }
            return elseBranch == null ? null : elseBranch.getStr(rec);
        }

        @Override
        public CharSequence getStrB(Record rec) {
            for (int i = 0; i < argsLen; i += 2) {
                if (args.getQuick(i).getBool(rec)) {
                    return args.getQuick(i + 1).getStrB(rec);
                }
            }
            return elseBranch.getStrB(rec);
        }
    }
}
