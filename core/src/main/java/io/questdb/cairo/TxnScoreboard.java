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
import io.questdb.std.FilesFacade;
import io.questdb.std.MemoryTag;
import io.questdb.std.Numbers;
import io.questdb.std.Transient;
import io.questdb.std.str.LPSZ;
import io.questdb.std.str.Path;

import java.io.Closeable;

public class TxnScoreboard implements Closeable {

    private static final Log LOG = LogFactory.getLog(TxnScoreboard.class);

    private long fd ;
    private long mem;
    private final long size;
    private final FilesFacade ff;

    public TxnScoreboard(FilesFacade ff, @Transient Path root, int entryCount) {
        this.ff = ff;
        root.concat(TableUtils.TXN_SCOREBOARD_FILE_NAME).$();
        int pow2EntryCount = Numbers.ceilPow2(entryCount);
        this.size = TxnScoreboard.getScoreboardSize(pow2EntryCount);
        this.fd = openCleanRW(ff, root, this.size);

        // truncate is required to give file a size
        // allocate above does not seem to update file system's size entry
        ff.truncate(fd, this.size);
        try {
            this.mem = TableUtils.mapRW(ff, fd, this.size, MemoryTag.MMAP_DEFAULT);
            init(mem, pow2EntryCount);
        } catch (Throwable e) {
            ff.close(fd);
            throw e;
        }
    }

    public static native long getScoreboardSize(int entryCount);

    public static void releaseTxn(long pTxnScoreboard, long txn) {
        assert pTxnScoreboard > 0;
        LOG.debug().$("release  [p=").$(pTxnScoreboard).$(", txn=").$(txn).$(']').$();
        releaseTxn0(pTxnScoreboard, txn);
    }

    public void acquireTxn(long txn) {
        if (acquireTxn(mem, txn)) {
            return;
        }
        throw CairoException.instance(0).put("max txn-inflight limit reached [txn=").put(txn).put(", min=").put(getMin()).put(']');
    }

    @Override
    public void close() {
        if (mem != 0) {
            ff.munmap(mem, size, MemoryTag.MMAP_DEFAULT);
            mem = 0;
        }

        if (fd != -1) {
            ff.close(fd);
            fd = -1;
        }
    }

    public long getActiveReaderCount(long txn) {
        return getCount(mem, txn);
    }

    public long getMin() {
        return getMin(mem);
    }

    public boolean isTxnAvailable(long nameTxn) {
        return isTxnAvailable(mem, nameTxn);
    }

    public void releaseTxn(long txn) {
        releaseTxn(mem, txn);
    }

    private static boolean acquireTxn(long pTxnScoreboard, long txn) {
        assert pTxnScoreboard > 0;
        LOG.debug().$("acquire [p=").$(pTxnScoreboard).$(", txn=").$(txn).$(']').$();
        return acquireTxn0(pTxnScoreboard, txn);
    }

    private native static boolean acquireTxn0(long pTxnScoreboard, long txn);

    private native static long releaseTxn0(long pTxnScoreboard, long txn);

    private static native long getCount(long pTxnScoreboard, long txn);

    private static native long getMin(long pTxnScoreboard);

    private static native boolean isTxnAvailable(long pTxnScoreboard, long txn);

    private static native void init(long pTxnScoreboard, int entryCount);

    static long openCleanRW(FilesFacade ff, LPSZ path, long size) {
        final long fd = ff.openCleanRW(path, size);
        if (fd > -1) {
            LOG.debug().$("open clean [file=").$(path).$(", fd=").$(fd).$(']').$();
            return fd;
        }
        throw CairoException.instance(ff.errno()).put("could not open read-write with clean allocation [file=").put(path).put(']');
    }
}
