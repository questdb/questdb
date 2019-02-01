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

import com.questdb.std.str.DirectBytes;

public final class Hash {
    private Hash() {
    }

    /**
     * Restricts hashCode() of the underlying char sequence to be no greater than max.
     *
     * @param s   char sequence
     * @param max max value of hashCode()
     * @return power of 2 integer
     */
    public static int boundedHash(CharSequence s, int max) {
        return s == null ? -1 : (Chars.hashCode(s) & 0xFFFFFFF) & max;
    }

    public static int boundedHash(DirectBytes s, int max) {
        return s == null ? -1 : (Chars.hashCode(s) & 0xFFFFFFF) & max;
    }

    /**
     * Calculates positive integer hash of memory pointer using Java hashcode() algorithm.
     *
     * @param p   memory pointer
     * @param len memory length in bytes
     * @return hash code
     */
    public static int hashMem(long p, int len) {
        int hash = 0;
        long hi = p + len;
        while (hi - p > 3) {
            hash = (hash << 5) - hash + Unsafe.getUnsafe().getInt(p);
            p += 4;
        }

        while (p < hi) {
            hash = (hash << 5) - hash + Unsafe.getUnsafe().getByte(p++);
        }

        return hash < 0 ? -hash : hash;
    }
}
