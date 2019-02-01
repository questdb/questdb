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

package com.questdb.std;

public interface BinarySequence {

    byte byteAt(long index);

    /**
     * Copies bytes from this binary sequence to buffer.
     *
     * @param address target buffer address
     * @param start   offset in binary sequence to start copying from
     * @param length  number of bytes to copy
     */
    default void copyTo(long address, long start, long length) {
        final long n = Math.min(length() - start, length);
        for (long l = 0; l < n; l++) {
            Unsafe.getUnsafe().putByte(address + l, byteAt(start + l));
        }
    }

    long length();
}
