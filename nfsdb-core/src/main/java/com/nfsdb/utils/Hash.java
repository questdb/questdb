/*
 *  _  _ ___ ___     _ _
 * | \| | __/ __| __| | |__
 * | .` | _|\__ \/ _` | '_ \
 * |_|\_|_| |___/\__,_|_.__/
 *
 * Copyright (c) 2014-2015. The NFSdb project and its contributors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.nfsdb.utils;

public final class Hash {
    private Hash() {
    }

    /**
     * Restricts hashCode() of the underlying char sequence to be no greater than max.
     *
     * @param s   char sequence
     * @param max max value of hashCode()
     * @return positive integer between 0 and max-1
     */
    public static int boundedHash(CharSequence s, int max) {
        return s == null ? -1 : (s.hashCode() & 0xFFFFFFF) % max;
    }

    /**
     * Calculates positive integer hash of memory pointer using Java hashcode() algorithm.
     *
     * @param address memory pointer
     * @param len     memory length in bytes
     * @return hash code
     */
    public static int hashMem(long address, int len) {
        int hash = 0;
        long end = address + len;
        while (end - address > 1) {
            hash = (hash << 5) - hash + Unsafe.getUnsafe().getChar(address);
            address += 2;
        }

        if (address < end) {
            hash = (hash << 5) - hash + Unsafe.getUnsafe().getByte(address);
        }

        return hash < 0 ? -hash : hash;
    }
}
