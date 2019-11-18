/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2020 QuestDB
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 *
 ******************************************************************************/

package io.questdb.std;

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

    /**
     * Calculates positive integer hash of memory pointer using Java hashcode() algorithm.
     *
     * @param p   memory pointer
     * @param len memory length in bytes
     * @return hash code
     */
    public static int hashMem(long p, int len) {
        int hash = 0;
        final long hi = p + len;
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
