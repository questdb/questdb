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

package io.questdb.griffin.engine.functions.eq;

import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.Record;
import io.questdb.griffin.FunctionFactory;
import io.questdb.griffin.engine.functions.BinaryFunction;
import io.questdb.griffin.engine.functions.BooleanFunction;
import io.questdb.std.ObjList;

public class EqCharCharFunctionFactory implements FunctionFactory {
    @Override
    public String getSignature() {
        return "=(AA)";
    }

    @Override
    public Function newInstance(ObjList<Function> args, int position, CairoConfiguration configuration) {
        // there are optimisation opportunities
        // 1. when one of args is constant null comparison can boil down to checking
        //    length of non-constant (must be -1)
        // 2. when one of arguments is constant, save method call and use a field

        Function chrFunc1 = args.getQuick(0);
        Function chrFunc2 = args.getQuick(1);

        return new Func(position, chrFunc1, chrFunc2);
    }

    private static class Func extends BooleanFunction implements BinaryFunction {

        private final Function chrFunc1;
        private final Function chrFunc2;

        public Func(int position, Function chrFunc1, Function chrFunc2) {
            super(position);
            this.chrFunc1 = chrFunc1;
            this.chrFunc2 = chrFunc2;
        }

        @Override
        public Function getLeft() {
            return chrFunc1;
        }

        @Override
        public Function getRight() {
            return chrFunc2;
        }

        @Override
        public boolean getBool(Record rec) {
            return chrFunc1.getChar(rec) == chrFunc2.getChar(rec);
        }
    }
}
