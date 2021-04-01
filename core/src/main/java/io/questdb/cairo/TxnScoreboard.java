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

import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.std.Os;
import io.questdb.std.str.Path;

public class TxnScoreboard {

    private static final Log LOG = LogFactory.getLog(TxnScoreboard.class);

    public static long close(Path shmPath, long pTxnScoreboard) {
        LOG.info().$("close [p=").$(pTxnScoreboard).$(']').$();
        return close0(shmPath.address(), pTxnScoreboard);
    }

    public static long close(Path shmPath, CharSequence tableName, long pTxnScoreboard) {
        setShmName(shmPath, tableName);
        return close(shmPath, pTxnScoreboard);
    }

    public static long create(Path shmPath, CharSequence tableName) {
        setShmName(shmPath, tableName);
        return create0(shmPath.address());
    }

    public static long newRef(long pTxnScoreboard) {
        assert pTxnScoreboard > 0;
        return newRef0(pTxnScoreboard);
    }

    private static void setShmName(Path shmPath, CharSequence name) {
        if (Os.type == Os.WINDOWS) {
            shmPath.of("Local\\").put(name).$();
        } else {
            shmPath.of("/").put(name).$();
        }
    }

    static boolean acquire(long pTxnScoreboard, long txn) {
        assert pTxnScoreboard > 0;
        LOG.info().$("acquire [p=").$(pTxnScoreboard).$(", txn=").$(txn).$(']').$();
        return acquire0(pTxnScoreboard, txn);
    }

    private native static boolean acquire0(long pTxnScoreboard, long txn);

    static long release(long pTxnScoreboard, long txn) {
        assert pTxnScoreboard > 0;
        LOG.info().$("release  [p=").$(pTxnScoreboard).$(", txn=").$(txn).$(']').$();
        return release0(pTxnScoreboard, txn);
    }

    private native static long release0(long pTxnScoreboard, long txn);

    private native static long newRef0(long pTxnScoreboard);

    private static native long create0(long lpszName);

    static native long getCount(long pTxnScoreboard, long txn);

    private static native long close0(long lpszName, long pTxnScoreboard);
}
