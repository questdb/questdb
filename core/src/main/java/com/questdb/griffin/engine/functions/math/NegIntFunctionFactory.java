/*******************************************************************************
 *    ___                  _   ____  ____
 *   / _ \ _   _  ___  ___| |_|  _ \| __ )
 *  | | | | | | |/ _ \/ __| __| | | |  _ \
 *  | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *   \__\_\\__,_|\___||___/\__|____/|____/
 *
 * Copyright (C) 2014-2018 Appsicle
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

package com.questdb.griffin.engine.functions.math;

import com.questdb.cairo.CairoConfiguration;
import com.questdb.cairo.sql.Function;
import com.questdb.cairo.sql.Record;
import com.questdb.griffin.FunctionFactory;
import com.questdb.griffin.engine.functions.IntFunction;
import com.questdb.std.Numbers;
import com.questdb.std.ObjList;

public class NegIntFunctionFactory implements FunctionFactory {
    @Override
    public String getSignature() {
        return "-(I)";
    }

    @Override
    public Function newInstance(ObjList<Function> args, int position, CairoConfiguration configuration) {
        return new Func(position, args.getQuick(0));
    }

    private static class Func extends IntFunction {
        final Function func;

        public Func(int position, Function func) {
            super(position);
            this.func = func;
        }

        @Override
        public int getInt(Record rec) {
            int value = func.getInt(rec);
            if (value == Numbers.INT_NaN) {
                return Numbers.INT_NaN;
            }
            return -value;
        }

        @Override
        public boolean isConstant() {
            return func.isConstant();
        }
    }
}
