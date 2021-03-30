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

import io.questdb.std.str.LPSZ;

public class TxnScoreboard {

    public static int close(LPSZ name, long pTxnScoreboard) {
        return close0(name.address(), pTxnScoreboard);
    }

    public static long create(LPSZ name) {
        return create0(name.address());
    }

    static native boolean acquire(long pTxnScoreboard, long txn);

    static native long release(long pTxnScoreboard, long txn);

    private static native long create0(long lpszName);

    static native long getMax(long pTxnScoreboard);

    static native long getMin(long pTxnScoreboard);

    static native long getCount(long pTxnScoreboard, long txn);

    static native int getScoreboardSize();

    static native void init(long pTxnScoreboard);

    private static native int close0(long lpszName, long pTxnScoreboard);
}
