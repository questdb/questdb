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

package com.questdb.griffin.engine.functions.constants;

import com.questdb.cairo.sql.Record;
import com.questdb.griffin.Function;
import com.questdb.griffin.engine.functions.BooleanFunction;

public class BooleanConstant extends BooleanFunction {

    public static final BooleanConstant TRUE = new BooleanConstant(true);
    public static final BooleanConstant FALSE = new BooleanConstant(false);
    private final boolean value;

    private BooleanConstant(boolean value) {
        this.value = value;
    }

    public static Function of(boolean value) {
        if (value) {
            return TRUE;
        } else {
            return FALSE;
        }
    }

    @Override
    public boolean getBool(Record rec) {
        return value;
    }

    @Override
    public boolean isConstant() {
        return true;
    }
}
