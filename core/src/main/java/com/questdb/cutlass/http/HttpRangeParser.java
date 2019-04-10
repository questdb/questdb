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

package com.questdb.cutlass.http;

import com.questdb.std.Chars;
import com.questdb.std.Numbers;
import com.questdb.std.NumericException;

public class HttpRangeParser {
    private static final String BYTES = "bytes=";
    private long lo;
    private long hi;

    public long getHi() {
        return hi;
    }

    public long getLo() {
        return lo;
    }

    public boolean of(CharSequence range) {
        if (!Chars.startsWith(range, BYTES)) {
            return false;
        }

        int n = Chars.indexOf(range, BYTES.length(), '-');

        if (n == -1) {
            return false;
        }

        try {
            this.lo = Numbers.parseLong(range, BYTES.length(), n);
            if (n == range.length() - 1) {
                this.hi = Long.MAX_VALUE;
            } else {
                this.hi = Numbers.parseLong(range, n + 1, range.length());
            }
            return true;
        } catch (NumericException e) {
            return false;
        }
    }
}
