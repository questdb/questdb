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

package io.questdb.griffin.engine.functions.math;

import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.Record;
import io.questdb.griffin.FunctionFactory;
import io.questdb.griffin.engine.functions.ShortFunction;
import io.questdb.griffin.engine.functions.UnaryFunction;
import io.questdb.std.ObjList;

public class AbsShortFunctionFactory implements FunctionFactory {
    @Override
    public String getSignature() {
        return "abs(E)";
    }

    @Override
    public Function newInstance(ObjList<Function> args, int position, CairoConfiguration configuration) {
        return new AbsFunction(position, args.getQuick(0));
    }

    private static class AbsFunction extends ShortFunction implements UnaryFunction {
        final Function function;

        public AbsFunction(int position, Function function) {
            super(position);
            this.function = function;
        }

        @Override
        public Function getArg() {
            return function;
        }

        @Override
        public short getShort(Record rec) {
            short value = function.getShort(rec);
            return value < 0 ? (short) -value : value;
        }
    }
}
