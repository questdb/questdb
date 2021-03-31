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

package io.questdb.cairo;

import io.questdb.std.LongHashSet;
import io.questdb.std.Os;
import io.questdb.std.str.Path;

public class TxnScoreboard {

    private static final LongHashSet open = new LongHashSet();
    private static final LongHashSet closed = new LongHashSet();

    public static int close(Path shmPath, long pTxnScoreboard) {
        assert assertClose(pTxnScoreboard);
        return close0(shmPath.address(), pTxnScoreboard);
    }

    public static int close(Path shmPath, CharSequence tableName, long pTxnScoreboard) {
        setShmName(shmPath, tableName);
        return close(shmPath, pTxnScoreboard);
    }

    public static long create(Path shmPath, CharSequence tableName) {
        setShmName(shmPath, tableName);
        long pTxnScoreboard = create0(shmPath.address());
        assert assertOpen(pTxnScoreboard);
        return pTxnScoreboard;
    }

    private static synchronized boolean assertClose(long pTxnScoreboard) {
        boolean result = open.contains(pTxnScoreboard) && closed.excludes(pTxnScoreboard);
        closed.add(pTxnScoreboard);
        open.remove(pTxnScoreboard);
        return result;
    }

    private static synchronized boolean assertOpen(long pTxnScoreboard) {
        int keyIndex = open.keyIndex(pTxnScoreboard);
        open.addAt(keyIndex, pTxnScoreboard);
        closed.remove(pTxnScoreboard);
        return keyIndex > -1;
    }

    private static void setShmName(Path shmPath, CharSequence name) {
        if (Os.type == Os.WINDOWS) {
            shmPath.of("Local\\").put(name).$();
        } else {
            shmPath.of("/").put(name).$();
        }
    }

    static native boolean acquire(long pTxnScoreboard, long txn);

    static native long release(long pTxnScoreboard, long txn);

    private static native long create0(long lpszName);

    static native long getCount(long pTxnScoreboard, long txn);

    private static native int close0(long lpszName, long pTxnScoreboard);
}
