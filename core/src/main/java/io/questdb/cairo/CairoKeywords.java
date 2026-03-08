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

package io.questdb.cairo;

import io.questdb.std.Unsafe;

public class CairoKeywords {
    public static boolean isDetachedDirMarker(long lpsz) {
        int len = length(lpsz);
        if (len < 10) {
            return false;
        }

        long i = lpsz + len - 9;
        return Unsafe.getUnsafe().getByte(i++) == '.'
                && Unsafe.getUnsafe().getByte(i++) == 'd'
                && Unsafe.getUnsafe().getByte(i++) == 'e'
                && Unsafe.getUnsafe().getByte(i++) == 't'
                && Unsafe.getUnsafe().getByte(i++) == 'a'
                && Unsafe.getUnsafe().getByte(i++) == 'c'
                && Unsafe.getUnsafe().getByte(i++) == 'h'
                && Unsafe.getUnsafe().getByte(i++) == 'e'
                && Unsafe.getUnsafe().getByte(i) == 'd';

    }

    public static boolean isLock(long lpsz) {
        int len = length(lpsz);
        if (len < 6) {
            return false;
        }

        // .lock
        long i = lpsz + len - 5;
        return Unsafe.getUnsafe().getByte(i++) == '.'
                && Unsafe.getUnsafe().getByte(i++) == 'l'
                && Unsafe.getUnsafe().getByte(i++) == 'o'
                && Unsafe.getUnsafe().getByte(i++) == 'c'
                && Unsafe.getUnsafe().getByte(i) == 'k';

    }

    public static boolean isMeta(long lpsz) {
        long i = lpsz;
        return Unsafe.getUnsafe().getByte(i++) == '_'
                && Unsafe.getUnsafe().getByte(i++) == 'm'
                && Unsafe.getUnsafe().getByte(i++) == 'e'
                && Unsafe.getUnsafe().getByte(i++) == 't'
                && Unsafe.getUnsafe().getByte(i) == 'a';
    }

    public static boolean isSeq(long lpsz) {
        long i = lpsz;
        return Unsafe.getUnsafe().getByte(i++) == 's'
                && Unsafe.getUnsafe().getByte(i++) == 'e'
                && Unsafe.getUnsafe().getByte(i) == 'q';
    }

    public static boolean isTxn(long lpsz) {
        long i = lpsz;
        return Unsafe.getUnsafe().getByte(i++) == '_'
                && Unsafe.getUnsafe().getByte(i++) == 't'
                && Unsafe.getUnsafe().getByte(i++) == 'x'
                && Unsafe.getUnsafe().getByte(i) == 'n';
    }

    public static boolean isTxnSeq(long lpsz) {
        long i = lpsz;
        return Unsafe.getUnsafe().getByte(i++) == 't'
                && Unsafe.getUnsafe().getByte(i++) == 'x'
                && Unsafe.getUnsafe().getByte(i++) == 'n'
                && Unsafe.getUnsafe().getByte(i++) == '_'
                && Unsafe.getUnsafe().getByte(i++) == 's'
                && Unsafe.getUnsafe().getByte(i++) == 'e'
                && Unsafe.getUnsafe().getByte(i) == 'q';

    }

    public static boolean isWal(long lpsz) {
        long i = lpsz;
        return Unsafe.getUnsafe().getByte(i++) == 'w'
                && Unsafe.getUnsafe().getByte(i++) == 'a'
                && Unsafe.getUnsafe().getByte(i) == 'l';
    }

    public static int length(long lpsz) {
        long p = lpsz;
        while (Unsafe.getUnsafe().getByte(p) != 0) {
            p++;
        }
        return (int) (p - lpsz);
    }
}
