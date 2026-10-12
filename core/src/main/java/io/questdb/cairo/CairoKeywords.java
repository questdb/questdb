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

package io.questdb.cairo;

import io.questdb.std.Unsafe;

public class CairoKeywords {
    public static boolean isDetachedDirMarker(long lpsz) {
        int len = length(lpsz);
        if (len < 10) {
            return false;
        }

        long i = lpsz + len - 9;
        return Unsafe.getByte(i++) == '.'
                && Unsafe.getByte(i++) == 'd'
                && Unsafe.getByte(i++) == 'e'
                && Unsafe.getByte(i++) == 't'
                && Unsafe.getByte(i++) == 'a'
                && Unsafe.getByte(i++) == 'c'
                && Unsafe.getByte(i++) == 'h'
                && Unsafe.getByte(i++) == 'e'
                && Unsafe.getByte(i) == 'd';

    }

    public static boolean isLiveViewCheckpoints(long lpsz) {
        // "_checkpoints" - the live view checkpoint directory
        // (LiveViewCheckpointLayout.CHECKPOINT_DIR_NAME)
        if (length(lpsz) != 12) {
            return false;
        }

        long i = lpsz;
        return Unsafe.getByte(i++) == '_'
                && Unsafe.getByte(i++) == 'c'
                && Unsafe.getByte(i++) == 'h'
                && Unsafe.getByte(i++) == 'e'
                && Unsafe.getByte(i++) == 'c'
                && Unsafe.getByte(i++) == 'k'
                && Unsafe.getByte(i++) == 'p'
                && Unsafe.getByte(i++) == 'o'
                && Unsafe.getByte(i++) == 'i'
                && Unsafe.getByte(i++) == 'n'
                && Unsafe.getByte(i++) == 't'
                && Unsafe.getByte(i) == 's';
    }

    public static boolean isLock(long lpsz) {
        int len = length(lpsz);
        if (len < 6) {
            return false;
        }

        // .lock
        long i = lpsz + len - 5;
        return Unsafe.getByte(i++) == '.'
                && Unsafe.getByte(i++) == 'l'
                && Unsafe.getByte(i++) == 'o'
                && Unsafe.getByte(i++) == 'c'
                && Unsafe.getByte(i) == 'k';

    }

    public static boolean isMeta(long lpsz) {
        long i = lpsz;
        return Unsafe.getByte(i++) == '_'
                && Unsafe.getByte(i++) == 'm'
                && Unsafe.getByte(i++) == 'e'
                && Unsafe.getByte(i++) == 't'
                && Unsafe.getByte(i) == 'a';
    }

    /**
     * Matches the {@code _rebase_new} / {@code _rebase_source} markers of ALTER TABLE ... REBASE WAL
     * by their shared {@code _rebase_} prefix.
     */
    public static boolean isRebaseMarker(long lpsz) {
        long i = lpsz;
        return Unsafe.getByte(i++) == '_'
                && Unsafe.getByte(i++) == 'r'
                && Unsafe.getByte(i++) == 'e'
                && Unsafe.getByte(i++) == 'b'
                && Unsafe.getByte(i++) == 'a'
                && Unsafe.getByte(i++) == 's'
                && Unsafe.getByte(i++) == 'e'
                && Unsafe.getByte(i) == '_';
    }

    public static boolean isSeq(long lpsz) {
        long i = lpsz;
        return Unsafe.getByte(i++) == 's'
                && Unsafe.getByte(i++) == 'e'
                && Unsafe.getByte(i) == 'q';
    }

    public static boolean isTxn(long lpsz) {
        long i = lpsz;
        return Unsafe.getByte(i++) == '_'
                && Unsafe.getByte(i++) == 't'
                && Unsafe.getByte(i++) == 'x'
                && Unsafe.getByte(i) == 'n';
    }

    public static boolean isTxnSeq(long lpsz) {
        long i = lpsz;
        return Unsafe.getByte(i++) == 't'
                && Unsafe.getByte(i++) == 'x'
                && Unsafe.getByte(i++) == 'n'
                && Unsafe.getByte(i++) == '_'
                && Unsafe.getByte(i++) == 's'
                && Unsafe.getByte(i++) == 'e'
                && Unsafe.getByte(i) == 'q';

    }

    public static boolean isWal(long lpsz) {
        long i = lpsz;
        return Unsafe.getByte(i++) == 'w'
                && Unsafe.getByte(i++) == 'a'
                && Unsafe.getByte(i) == 'l';
    }

    public static int length(long lpsz) {
        long p = lpsz;
        while (Unsafe.getByte(p) != 0) {
            p++;
        }
        return (int) (p - lpsz);
    }
}
