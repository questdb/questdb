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

package com.questdb.griffin.engine.functions.lt;

import com.questdb.cairo.CairoConfiguration;
import com.questdb.cairo.sql.Function;
import com.questdb.cairo.sql.Record;
import com.questdb.griffin.FunctionFactory;
import com.questdb.griffin.engine.functions.BooleanFunction;
import com.questdb.griffin.engine.functions.UnaryFunction;
import com.questdb.griffin.engine.functions.constants.BooleanConstant;
import com.questdb.std.ObjList;

public class LtDoubleCVFunctionFactory implements FunctionFactory {

    @Override
    public String getSignature() {
        return "<(dD)";
    }

    @Override
    public Function newInstance(ObjList<Function> args, int position, CairoConfiguration configuration) {
        final double left = args.getQuick(0).getDouble(null);
        if (Double.isNaN(left)) {
            return new BooleanConstant(position, false);
        }
        return new FuncCV(position, left, args.getQuick(1));
    }

    private static class FuncCV extends BooleanFunction implements UnaryFunction {
        private final double left;
        private final Function right;

        public FuncCV(int position, double left, Function right) {
            super(position);
            this.left = left;
            this.right = right;
        }

        @Override
        public Function getArg() {
            return right;
        }

        @Override
        public boolean getBool(Record rec) {
            return left < right.getDouble(rec);
        }
    }
}
