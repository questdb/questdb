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
import io.questdb.std.Files;
import io.questdb.std.FilesFacade;
import io.questdb.std.Numbers;
import io.questdb.std.Transient;
import io.questdb.std.str.Path;

import java.io.Closeable;
import java.util.concurrent.locks.LockSupport;

public class TxnScoreboard implements Closeable {

    private static final Log LOG = LogFactory.getLog(TxnScoreboard.class);
    private static final int WINDOWS_ACCESS_DENIED_ERRNO = 5;
    private static final int MAX_FILE_OPEN_RETRIES = 64;

    private final long fd;
    private final long mem;
    private final long size;
    private final FilesFacade ff;

    public TxnScoreboard(FilesFacade ff, @Transient Path root, int entryCount) {
        this.ff = ff;
        root.concat(TableUtils.TXN_SCOREBOARD_FILE_NAME).$();
        this.fd = openWithSafeCleanup(ff, root);
        int pow2EntryCount = Numbers.ceilPow2(entryCount);
        this.size = TxnScoreboard.getScoreboardSize(pow2EntryCount);
        if (!ff.allocate(fd, this.size)) {
            ff.close(fd);
            throw CairoException.instance(ff.errno()).put("not enough space on disk? [name=").put(root).put(", size=").put(this.size).put(']') ;
        }
        // truncate is required to give file a size
        // the allocate above does not seem to update file system's size entry
        ff.truncate(fd, this.size);
        this.mem = ff.mmap(fd, this.size, 0, Files.MAP_RW);
        if (mem == -1) {
            ff.close(fd);
            throw CairoException.instance(ff.errno())
                    .put("could not mmap column [fd=").put(fd)
                    .put(", size=").put(this.size)
                    .put(']');
        }
        init(mem, pow2EntryCount);
    }

    @Override
    public void close() {
        ff.munmap(mem, size);
        ff.close(fd);
    }

    public void releaseTxn(long txn) {
        releaseTxn(mem, txn);
    }

    public void acquireTxn(long txn) {
        if (acquireTxn(mem, txn)) {
            return;
        }
        throw CairoException.instance(0).put("max txn-inflight limit reached [txn=").put(txn).put(", min=").put(getMin()).put(']');
    }

    public long getMin() {
        return getMin(mem);
    }

    public boolean isTxnAvailable(long nameTxn) {
        return isTxnAvailable(mem, nameTxn);
    }

    public long getActiveReaderCount(long txn) {
        return getCount(mem, txn);
    }

    private static boolean acquireTxn(long pTxnScoreboard, long txn) {
        assert pTxnScoreboard > 0;
        LOG.debug().$("acquire [p=").$(pTxnScoreboard).$(", txn=").$(txn).$(']').$();
        return acquireTxn0(pTxnScoreboard, txn);
    }

    public static void releaseTxn(long pTxnScoreboard, long txn) {
        assert pTxnScoreboard > 0;
        LOG.debug().$("release  [p=").$(pTxnScoreboard).$(", txn=").$(txn).$(']').$();
        releaseTxn0(pTxnScoreboard, txn);
    }


    private static long openWithSafeCleanup(FilesFacade ff, Path path) {
        // If a reader does not release txn in the scoreboard
        // because of process termination
        // the txn is locked forever and eventually scoreboard fails with txn-inflight error.
        // To prevent it try to clean scoreboard on opening if noone has it opened yet.
        if (ff.fsLocksOpenedFiles()) {
            // On Windows simply attempt to delete the file
            // If file is in use, delete attempt fails but it's ok to ignore since it will happen in most of the cases
            if (ff.remove(path)) {
                LOG.debug().$("no usage detected and file truncate [file=").$(path).I$();
            }
            long fd = ff.openRW(path);
            if (fd > -1) {
                LOG.debug().$("open [file=").$(path).$(", fd=").$(fd).$(']').$();
                return fd;
            }

            return retryFileOpen(ff, path);
        } else {
            long fd = TableUtils.openRW(ff, path, LOG);
            // On Linux use flock and truncate to clean file once opened.
            // tryCleanExclusively will keep shared lock which releases on file close.
            int isTruncated = ff.tryExclusiveLockTruncate(fd);
            if (isTruncated > 0) {
                LOG.debug().$("no usage detected and file truncate [file=").$(path).$(", fd=").$(fd).$(']').$();
            }
            if (isTruncated == 0) {
                return fd;
            }

            ff.close(fd);
            throw CairoException.instance(ff.errno()).put("Could not lock [file=").put(path).put(']');
        }
    }

    private static long retryFileOpen(FilesFacade ff, Path path) {
        long fd;
        // Do few retries, deleting and opening the same file from different threads can result to errno 5.
        int i = 0;
        while (ff.errno() == WINDOWS_ACCESS_DENIED_ERRNO && i++ < MAX_FILE_OPEN_RETRIES) {
            fd = ff.openRW(path);
            if (fd > -1) {
                LOG.debug().$("open [file=").$(path).$(", fd=").$(fd).$(']').$();
                return fd;
            }
            LockSupport.parkNanos(1);
        }

        throw CairoException.instance(ff.errno()).put("could not open read-write [file=").put(path).put(']');
    }

    private native static boolean acquireTxn0(long pTxnScoreboard, long txn);

    private native static long releaseTxn0(long pTxnScoreboard, long txn);

    private static native long getCount(long pTxnScoreboard, long txn);

    private static native long getMin(long pTxnScoreboard);

    public static native long getScoreboardSize(int entryCount);

    private static native boolean isTxnAvailable(long pTxnScoreboard, long txn);

    private static native void init(long pTxnScoreboard, int entryCount);
}
