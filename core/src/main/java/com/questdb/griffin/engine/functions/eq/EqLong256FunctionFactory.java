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

package com.questdb.griffin.engine.functions.eq;

import com.questdb.cairo.CairoConfiguration;
import com.questdb.cairo.sql.Function;
import com.questdb.cairo.sql.Record;
import com.questdb.griffin.FunctionFactory;
import com.questdb.griffin.SqlException;
import com.questdb.griffin.engine.functions.BooleanFunction;
import com.questdb.griffin.engine.functions.UnaryFunction;
import com.questdb.std.Long256;
import com.questdb.std.Numbers;
import com.questdb.std.NumericException;
import com.questdb.std.ObjList;

public class EqLong256FunctionFactory implements FunctionFactory {
    @Override
    public String getSignature() {
        return "=(Hs)";
    }

    @Override
    public Function newInstance(ObjList<Function> args, int position, CairoConfiguration configuration) throws SqlException {
        final CharSequence hexLong256 = args.getQuick(1).getStr(null);
        int len = hexLong256.length();
        int n;

        long long0 = 0;
        long long1 = 0;
        long long2 = 0;
        long long3 = 0;
        try {
            n = Math.max(0, len - 16);
            if (len > 0) {
                long0 = Numbers.parseHexLong(hexLong256, n, len);
                len = n;
            }

            n = Math.max(0, len - 16);
            if (len > 0) {
                long1 = Numbers.parseHexLong(hexLong256, n, len);
                len = n;
            }

            n = Math.max(0, len - 16);
            if (len > 0) {
                long2 = Numbers.parseHexLong(hexLong256, n, len);
                len = n;
            }

            n = Math.max(0, len - 16);
            if (len > 0) {
                long3 = Numbers.parseHexLong(hexLong256, n, len);
                len = n;
            }

            if (len > 2) {
                throw SqlException.position(args.getQuick(1).getPosition()).put("value is too long");
            }

            return new Func(position, args.getQuick(0), long0, long1, long2, long3);
        } catch (NumericException e) {
            throw SqlException.position(args.getQuick(1).getPosition()).put("invalid hex value for long256");
        }

    }

    private static class Func extends BooleanFunction implements UnaryFunction {
        private final Function arg;
        private final long long0;
        private final long long1;
        private final long long2;
        private final long long3;

        public Func(int position, Function arg, long long0, long long1, long long2, long long3) {
            super(position);
            this.arg = arg;
            this.long0 = long0;
            this.long1 = long1;
            this.long2 = long2;
            this.long3 = long3;
        }

        @Override
        public boolean getBool(Record rec) {
            final Long256 value = arg.getLong256A(rec);
            return value.getLong0() == long0 && value.getLong1() == long1 && value.getLong2() == long2 && value.getLong3() == long3;
        }

        @Override
        public Function getArg() {
            return arg;
        }
    }
}
