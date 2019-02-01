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

package com.questdb.griffin.engine.functions.constants;

import com.questdb.cairo.TableUtils;
import com.questdb.cairo.sql.Record;
import com.questdb.griffin.engine.functions.StrFunction;
import com.questdb.std.Chars;

public class StrConstant extends StrFunction implements ConstantFunction {
    private final String value;
    private final int length;

    public StrConstant(int position, CharSequence value) {
        super(position);
        if (value == null) {
            this.value = null;
            this.length = TableUtils.NULL_LEN;
        } else {
            if (Chars.startsWith(value, '\'')) {
                this.value = Chars.toString(value, 1, value.length() - 1);
            } else {
                this.value = Chars.toString(value);
            }
            this.length = this.value.length();
        }
    }

    @Override
    public CharSequence getStr(Record rec) {
        return value;
    }

    @Override
    public CharSequence getStrB(Record rec) {
        return value;
    }

    @Override
    public int getStrLen(Record rec) {
        return length;
    }
}
