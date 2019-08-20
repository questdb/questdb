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
import com.questdb.cairo.sql.Record;
import com.questdb.griffin.engine.functions.StatelessFunction;
import com.questdb.griffin.engine.functions.StrFunction;
import com.questdb.std.Rnd;

class RndStrFunction extends StrFunction implements StatelessFunction {
    private final int lo;
    private final int range;
    private final int nullRate;
    private final Rnd rnd;

    public RndStrFunction(int position, int lo, int hi, int nullRate, CairoConfiguration configuration) {
        super(position);
        this.lo = lo;
        this.range = hi - lo + 1;
        this.rnd = SharedRandom.getRandom(configuration);
        this.nullRate = nullRate;
    }

    @Override
    public CharSequence getStr(Record rec) {
        if ((rnd.nextInt() % nullRate) == 1) {
            return null;
        }
        return rnd.nextChars(lo + rnd.nextPositiveInt() % range);
    }

    @Override
    public CharSequence getStrB(Record rec) {
        return getStr(rec);
    }
}
