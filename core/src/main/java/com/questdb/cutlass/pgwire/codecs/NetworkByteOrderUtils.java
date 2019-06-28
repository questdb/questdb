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

public class NetworkByteOrderUtils {
    public static int getInt(long address) {
        int b = Unsafe.getUnsafe().getByte(address) & 0xff;
        b = (b << 8) | Unsafe.getUnsafe().getByte(address + 1) & 0xff;
        b = (b << 8) | Unsafe.getUnsafe().getByte(address + 2) & 0xff;
        return (b << 8) | Unsafe.getUnsafe().getByte(address + 3) & 0xff;
    }

    public static void putInt(long address, int value) {
        Unsafe.getUnsafe().putByte(address, (byte) (value >>> 24));
        Unsafe.getUnsafe().putByte(address + 1, (byte) (value >>> 16));
        Unsafe.getUnsafe().putByte(address + 2, (byte) (value >>> 8));
        Unsafe.getUnsafe().putByte(address + 3, (byte) (value));
    }

    public static void putShort(long address, short value) {
        Unsafe.getUnsafe().putByte(address, (byte) (value >>> 8));
        Unsafe.getUnsafe().putByte(address + 1, (byte) (value));
    }
}
