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

package com.questdb.griffin.engine.functions.date;

import com.questdb.cairo.CairoConfiguration;
import com.questdb.cairo.sql.Function;
import com.questdb.cairo.sql.Record;
import com.questdb.griffin.FunctionFactory;
import com.questdb.griffin.engine.functions.DateFunction;
import com.questdb.griffin.engine.functions.StatelessFunction;
import com.questdb.std.ObjList;
import com.questdb.std.time.MillisecondClock;

public class SysdateFunctionFactory implements FunctionFactory {
    @Override
    public String getSignature() {
        return "sysdate()";
    }

    @Override
    public Function newInstance(ObjList<Function> args, int position, CairoConfiguration configuration) {
        return new Func(position, configuration.getMillisecondClock());
    }

    private static class Func extends DateFunction implements StatelessFunction {

        private final MillisecondClock clock;

        public Func(int position, MillisecondClock clock) {
            super(position);
            this.clock = clock;
        }

        @Override
        public long getDate(Record rec) {
            return clock.getTicks();
        }
    }
}
