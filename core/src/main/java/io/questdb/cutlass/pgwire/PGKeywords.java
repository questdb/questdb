/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2026 QuestDB
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

package io.questdb.cutlass.pgwire;

import io.questdb.std.Unsafe;

public class PGKeywords {
    public static boolean isOptions(long lpsz, long len) {
        if (len != 7) {
            return false;
        }
        long i = lpsz;
        return Unsafe.getUnsafe().getByte(i++) == 'o'
                && Unsafe.getUnsafe().getByte(i++) == 'p'
                && Unsafe.getUnsafe().getByte(i++) == 't'
                && Unsafe.getUnsafe().getByte(i++) == 'i'
                && Unsafe.getUnsafe().getByte(i++) == 'o'
                && Unsafe.getUnsafe().getByte(i++) == 'n'
                && Unsafe.getUnsafe().getByte(i) == 's';
    }

    public static boolean isUser(long lpsz, long len) {
        if (len != 4) {
            return false;
        }
        long i = lpsz;
        return Unsafe.getUnsafe().getByte(i++) == 'u'
                && Unsafe.getUnsafe().getByte(i++) == 's'
                && Unsafe.getUnsafe().getByte(i++) == 'e'
                && Unsafe.getUnsafe().getByte(i) == 'r';
    }

    //"-c statement_timeout="
    public static boolean startsWithTimeoutOption(long lpsz, long len) {
        if (len < 21) {
            return false;
        }
        long i = lpsz;
        return Unsafe.getUnsafe().getByte(i++) == '-'
                && Unsafe.getUnsafe().getByte(i++) == 'c'
                && Unsafe.getUnsafe().getByte(i++) == ' '
                && Unsafe.getUnsafe().getByte(i++) == 's'
                && Unsafe.getUnsafe().getByte(i++) == 't'
                && Unsafe.getUnsafe().getByte(i++) == 'a'
                && Unsafe.getUnsafe().getByte(i++) == 't'
                && Unsafe.getUnsafe().getByte(i++) == 'e'
                && Unsafe.getUnsafe().getByte(i++) == 'm'
                && Unsafe.getUnsafe().getByte(i++) == 'e'
                && Unsafe.getUnsafe().getByte(i++) == 'n'
                && Unsafe.getUnsafe().getByte(i++) == 't'
                && Unsafe.getUnsafe().getByte(i++) == '_'
                && Unsafe.getUnsafe().getByte(i++) == 't'
                && Unsafe.getUnsafe().getByte(i++) == 'i'
                && Unsafe.getUnsafe().getByte(i++) == 'm'
                && Unsafe.getUnsafe().getByte(i++) == 'e'
                && Unsafe.getUnsafe().getByte(i++) == 'o'
                && Unsafe.getUnsafe().getByte(i++) == 'u'
                && Unsafe.getUnsafe().getByte(i++) == 't'
                && Unsafe.getUnsafe().getByte(i) == '=';
    }
}
