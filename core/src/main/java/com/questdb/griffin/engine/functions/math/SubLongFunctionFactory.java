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

package com.questdb.griffin.engine.functions.math;

import com.questdb.cairo.CairoConfiguration;
import com.questdb.cairo.sql.Function;
import com.questdb.cairo.sql.Record;
import com.questdb.griffin.FunctionFactory;
import com.questdb.griffin.engine.functions.BinaryFunction;
import com.questdb.griffin.engine.functions.LongFunction;
import com.questdb.std.Numbers;
import com.questdb.std.ObjList;

public class SubLongFunctionFactory implements FunctionFactory {
    @Override
    public String getSignature() {
        return "-(LL)";
    }

    @Override
    public Function newInstance(ObjList<Function> args, int position, CairoConfiguration configuration1) {
        return new SubtractIntVVFunc(position, args.getQuick(0), args.getQuick(1));
    }

    private static class SubtractIntVVFunc extends LongFunction implements BinaryFunction {
        final Function left;
        final Function right;

        public SubtractIntVVFunc(int position, Function left, Function right) {
            super(position);
            this.left = left;
            this.right = right;
        }

        @Override
        public Function getLeft() {
            return left;
        }

        @Override
        public Function getRight() {
            return right;
        }

        @Override
        public long getLong(Record rec) {
            long l = left.getLong(rec);
            long r = right.getLong(rec);

            if (l == Numbers.LONG_NaN || r == Numbers.LONG_NaN) {
                return Numbers.LONG_NaN;
            }

            return l - r;
        }
    }
}
