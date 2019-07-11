/*******************************************************************************
 *    ___                  _   ____  ____
 *   / _ \ _   _  ___  ___| |_|  _ \| __ )
 *  | | | | | | |/ _ \/ __| __| | | |  _ \
 *  | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *   \__\_\\__,_|\___||___/\__|____/|____/
 *
 * Copyright (C) 2014-2019 Appsicle
 *
 * This program is free software: you can redistribute it and/or  modify
 * it under the terms of the GNU Affero General Public License, version 3,
 * as published by the Free Software Foundation.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 *
 ******************************************************************************/

package com.questdb.griffin.engine.functions.conditional;

import com.questdb.cairo.CairoConfiguration;
import com.questdb.cairo.ColumnType;
import com.questdb.cairo.sql.Function;
import com.questdb.cairo.sql.Record;
import com.questdb.griffin.FunctionFactory;
import com.questdb.griffin.SqlException;
import com.questdb.griffin.engine.functions.StrFunction;
import com.questdb.std.ObjList;

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
