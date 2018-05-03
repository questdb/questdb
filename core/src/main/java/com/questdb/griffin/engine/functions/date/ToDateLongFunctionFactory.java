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

package com.questdb.griffin.engine.functions.date;

import com.questdb.cairo.CairoConfiguration;
import com.questdb.cairo.sql.Function;
import com.questdb.cairo.sql.Record;
import com.questdb.griffin.FunctionFactory;
import com.questdb.griffin.engine.functions.DateFunction;
import com.questdb.griffin.engine.functions.constants.DateConstant;
import com.questdb.std.ObjList;

public class ToDateLongFunctionFactory implements FunctionFactory {
    @Override
    public String getSignature() {
        return "to_date(L)";
    }

    @Override
    public Function newInstance(ObjList<Function> args, int position, CairoConfiguration configuration) {
        Function var = args.getQuick(0);
        if (var.isConstant()) {
            return new DateConstant(position, var.getLong(null));
        }
        return new Func(position, var);
    }

    private static class Func extends DateFunction {
        private final Function var;

        public Func(int position, Function var) {
            super(position);
            this.var = var;
        }

        @Override
        public long getDate(Record rec) {
            return var.getLong(rec);
        }
    }
}
