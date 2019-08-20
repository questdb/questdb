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

package com.questdb.griffin.engine.functions.bool;

import com.questdb.cairo.CairoConfiguration;
import com.questdb.cairo.ColumnType;
import com.questdb.cairo.sql.Function;
import com.questdb.cairo.sql.Record;
import com.questdb.griffin.FunctionFactory;
import com.questdb.griffin.SqlException;
import com.questdb.griffin.engine.functions.BooleanFunction;
import com.questdb.griffin.engine.functions.UnaryFunction;
import com.questdb.griffin.engine.functions.constants.BooleanConstant;
import com.questdb.std.IntHashSet;
import com.questdb.std.ObjList;

public class InCharFunctionFactory implements FunctionFactory {
    @Override
    public String getSignature() {
        return "in(Av)";
    }

    @Override
    public Function newInstance(ObjList<Function> args, int position, CairoConfiguration configuration) throws SqlException {

        IntHashSet set = new IntHashSet();
        int n = args.size();

        if (n == 1) {
            return new BooleanConstant(position, false);
        }

        for (int i = 1; i < n; i++) {
            Function func = args.getQuick(i);
            if (func.getType() == ColumnType.CHAR) {
                set.add(func.getChar(null));
            } else {
                throw SqlException.$(func.getPosition(), "CHAR constant expected");
            }
        }
        Function var = args.getQuick(0);
        if (var.isConstant()) {
            return new BooleanConstant(position, set.contains(var.getChar(null)));
        }
        return new Func(position, var, set);
    }

    private static class Func extends BooleanFunction implements UnaryFunction {
        private final Function arg;
        private final IntHashSet set;

        public Func(int position, Function arg, IntHashSet set) {
            super(position);
            this.arg = arg;
            this.set = set;
        }

        @Override
        public Function getArg() {
            return arg;
        }

        @Override
        public boolean getBool(Record rec) {
            return set.contains(arg.getChar(rec));
        }
    }
}
