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

package com.questdb.cutlass.pgwire.codecs;

import com.questdb.std.Unsafe;

public abstract class AbstractTypePrefixedHeader {

    public static final int LEN = 5;

    public static int getLen(long address) {
        return NetworkByteOrderUtils.getInt(address + 1);
    }

    public static byte getType(long address) {
        return Unsafe.getUnsafe().getByte(address);
    }

    public static void setLen(long address, int len) {
        NetworkByteOrderUtils.putInt(address + 1, len);
    }

    public static void setType(long address, byte type) {
        Unsafe.getUnsafe().putByte(address, type);
    }
}
