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

package com.questdb.cutlass.pgwire.codecs.out;

import com.questdb.cutlass.pgwire.codecs.AbstractTypePrefixedHeader;
import com.questdb.std.Unsafe;

public class ParameterStatusMsg extends AbstractTypePrefixedHeader {
    public static int setParameterPair(long address, CharSequence name, CharSequence value) {
        setType(address, (byte) 'S');
        long p = address + AbstractTypePrefixedHeader.LEN;
        final long start = p;
        p = copyStringZ(p, name);
        p = copyStringZ(p, value);
        int len = (int) (p - start);
        setLen(address, len + AbstractTypePrefixedHeader.LEN - 1);
        return len + AbstractTypePrefixedHeader.LEN;
    }

    private static long copyStringZ(long p, CharSequence value) {
        for (int i = 0, m = value.length(); i < m; i++) {
            Unsafe.getUnsafe().putByte(p++, (byte) value.charAt(i));
        }
        Unsafe.getUnsafe().putByte(p, (byte) 0);
        return p + 1;
    }

}
