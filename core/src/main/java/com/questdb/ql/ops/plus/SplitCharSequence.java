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

package com.questdb.ql.ops.plus;

import com.questdb.std.str.AbstractCharSequence;

public class SplitCharSequence extends AbstractCharSequence {
    private CharSequence lhs;
    private CharSequence rhs;
    private int rl;
    private int split;

    @Override
    public int length() {
        return split + rl;
    }

    @Override
    public char charAt(int index) {
        if (index < split) {
            return lhs.charAt(index);
        } else {
            return rhs.charAt(index - split);
        }
    }

    public CharSequence of(CharSequence lhs, CharSequence rhs) {
        this.lhs = lhs;
        this.rhs = rhs;
        this.rl = rhs == null ? 0 : rhs.length();
        this.split = lhs == null ? 0 : lhs.length();
        return this;
    }
}
