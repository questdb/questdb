/*+*****************************************************************************
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
        return Unsafe.getByte(i++) == 'o'
                && Unsafe.getByte(i++) == 'p'
                && Unsafe.getByte(i++) == 't'
                && Unsafe.getByte(i++) == 'i'
                && Unsafe.getByte(i++) == 'o'
                && Unsafe.getByte(i++) == 'n'
                && Unsafe.getByte(i) == 's';
    }

    public static boolean isUser(long lpsz, long len) {
        if (len != 4) {
            return false;
        }
        long i = lpsz;
        return Unsafe.getByte(i++) == 'u'
                && Unsafe.getByte(i++) == 's'
                && Unsafe.getByte(i++) == 'e'
                && Unsafe.getByte(i) == 'r';
    }

    //"-c statement_timeout="
    public static boolean startsWithTimeoutOption(long lpsz, long len) {
        if (len < 21) {
            return false;
        }
        long i = lpsz;
        return Unsafe.getByte(i++) == '-'
                && Unsafe.getByte(i++) == 'c'
                && Unsafe.getByte(i++) == ' '
                && Unsafe.getByte(i++) == 's'
                && Unsafe.getByte(i++) == 't'
                && Unsafe.getByte(i++) == 'a'
                && Unsafe.getByte(i++) == 't'
                && Unsafe.getByte(i++) == 'e'
                && Unsafe.getByte(i++) == 'm'
                && Unsafe.getByte(i++) == 'e'
                && Unsafe.getByte(i++) == 'n'
                && Unsafe.getByte(i++) == 't'
                && Unsafe.getByte(i++) == '_'
                && Unsafe.getByte(i++) == 't'
                && Unsafe.getByte(i++) == 'i'
                && Unsafe.getByte(i++) == 'm'
                && Unsafe.getByte(i++) == 'e'
                && Unsafe.getByte(i++) == 'o'
                && Unsafe.getByte(i++) == 'u'
                && Unsafe.getByte(i++) == 't'
                && Unsafe.getByte(i) == '=';
    }
}
