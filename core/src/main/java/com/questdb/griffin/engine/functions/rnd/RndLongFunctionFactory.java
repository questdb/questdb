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

package com.questdb.griffin.engine.functions.rnd;

import com.questdb.cairo.CairoConfiguration;
import com.questdb.cairo.sql.Function;
import com.questdb.cairo.sql.Record;
import com.questdb.griffin.FunctionFactory;
import com.questdb.griffin.engine.functions.LongFunction;
import com.questdb.griffin.engine.functions.StatelessFunction;
import com.questdb.std.ObjList;
import com.questdb.std.Rnd;

public class RndLongFunctionFactory implements FunctionFactory {

    @Override
    public String getSignature() {
        return "rnd_long()";
    }

    @Override
    public Function newInstance(ObjList<Function> args, int position, CairoConfiguration configuration) {
        return new RndFunction(position, configuration);
    }

    private static class RndFunction extends LongFunction implements StatelessFunction {

        private final Rnd rnd;

        public RndFunction(int position, CairoConfiguration configuration) {
            super(position);
            this.rnd = SharedRandom.getRandom(configuration);
        }

        @Override
        public long getLong(Record rec) {
            return rnd.nextLong();
        }
    }
}
