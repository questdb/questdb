/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2024 QuestDB
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
import org.jetbrains.annotations.TestOnly;

/**
 * mmapped file-based transaction scoreboard. Txn numbers are organized
 * in a ring buffer-like array of reader counters where each active slot
 * corresponds to a txn number. Hence, the "distance" between the minimum
 * and maximum active txn numbers is limited with the array size.
 * <p>
 * Supports multiple server instances pointed as the same DB root.
 * The downside is that long-running or leaked table readers lead
 * to max txn in-flight errors.
 */
public class TxnScoreboardV1 implements TxnScoreboard {
    private static final Log LOG = LogFactory.getLog(TxnScoreboard.class);
    private final FilesFacade ff;
    private final int pow2EntryCount;
    private final long size;
    private long fd = -1;
    private long mem;
    private TableToken tableToken;

    public TxnScoreboardV1(FilesFacade ff, int entryCount, TableToken tableToken) {
        this.ff = ff;
        this.pow2EntryCount = Numbers.ceilPow2(entryCount);
        this.size = TxnScoreboardV1.getScoreboardSize(pow2EntryCount);
        this.tableToken = tableToken;
    }

    public static native long getScoreboardSize(int entryCount);

    @Override
    public boolean acquireTxn(int id, long txn) {
        assert txn > -1;
        final long internalTxn = toInternalTxn(txn);
        final long response = acquireTxn(mem, internalTxn);
        if (response == 0) {
            // all good
            return true;
        }
        if (response == -1) {
            // retry
            return false;
        }
        final long min = fromInternalTxn(-response - 2);
        throw CairoException.critical(0).put("max txn-inflight limit reached [txn=").put(txn)
                .put(", min=").put(min)
                .put(", size=").put(pow2EntryCount)
                .put(']');
    }

    @Override
    public void close() {
        if (mem != 0) {
            ff.munmap(mem, size, MemoryTag.MMAP_DEFAULT);
            mem = 0;
        }

        if (ff.close(fd)) {
            LOG.debug().$("closed [fd=").$(fd).I$();
            fd = -1;
        }
    }

    @TestOnly
    public long getActiveReaderCount(long txn) {
        return getCount(mem, toInternalTxn(txn));
    }

    @Override
    public int getEntryCount() {
        return pow2EntryCount;
    }

    @TestOnly
    public long getMin() {
        final long min = getMin(mem);
        // min can be 0 on empty scoreboard, so we simply treat it as txn 0.
        if (min == 0) {
            return 0;
        }
        return fromInternalTxn(min);
    }

    @Override
    public TableToken getTableToken() {
        return tableToken;
    }

    @Override
    public boolean hasEarlierTxnLocks(long maxTxn) {
        try {
            if (acquireTxn(0, maxTxn)) {
                releaseTxn(0, maxTxn);
            }
            return getMin() < maxTxn;
        } catch (CairoException ex) {
            // Scoreboard can be over allocated, don't stall writing because of that.
            // Schedule async purge and continue
            LOG.critical().$("cannot lock last txn in scoreboard, partition purge will be scheduled [table=")
                    .$(tableToken)
                    .$(", error=").$safe(ex.getFlyweightMessage())
                    .$(", errno=").$(ex.getErrno()).I$();
            return getMin() < maxTxn;
        }
    }

    @Override
    public boolean incrementTxn(int id, long txn) {
        assert txn > -1;
        final long internalTxn = toInternalTxn(txn);
        return incrementTxnScoreboardMem(mem, internalTxn);
    }

    @Override
    public boolean isOutdated(long txn) {
        // Unknown, return false as default
        return true;
    }

    @Override
    public boolean isRangeAvailable(long fromTxn, long toTxn) {
        return isRangeAvailable0(mem, toInternalTxn(fromTxn), toInternalTxn(toTxn));
    }

    @Override
    public boolean isTxnAvailable(long txn) {
        return getCount(mem, toInternalTxn(txn)) <= 0;
    }

    public TxnScoreboard ofRW(TableToken tableToken, @Transient Path root) {
        this.tableToken = tableToken;
        int rootLen = root.size();
        root.concat(TableUtils.TXN_SCOREBOARD_FILE_NAME);
        this.fd = openCleanRW(ff, root.$(), size);

        // truncate is required to give the file a size
        // allocate above does not seem to update the file system's size entry
        try {
            if (ff.length(fd) != size) {
                ff.truncate(fd, size);
            }

            try {
                this.mem = TableUtils.mapRW(ff, fd, size, MemoryTag.MMAP_DEFAULT);
                init(mem, pow2EntryCount);
            } catch (Throwable e) {
                ff.close(fd);
                root.trimTo(rootLen);
                fd = -1;
                throw e;
            }
            return this;
        } catch (Throwable th) {
            ff.close(fd);
            throw th;
        }
    }

    @Override
    public long releaseTxn(int id, long txn) {
        long released = releaseTxn(mem, txn);
        assert released > -1 : "released count " + txn + " must be positive: " + (released + 1);
        return released;
    }

    private static long acquireTxn(long pTxnScoreboard, long txn) {
        assert pTxnScoreboard > 0;
        LOG.debug().$("acquire [p=").$(pTxnScoreboard).$(", txn=").$(fromInternalTxn(txn)).I$();
        return acquireTxn0(pTxnScoreboard, txn);
    }

    private native static long acquireTxn0(long pTxnScoreboard, long txn);

    /**
     * Reverts toInternalTxn() value.
     */
    private static long fromInternalTxn(long txn) {
        return txn - 1;
    }

    private static native long getCount(long pTxnScoreboard, long txn);

    private static native long getMin(long pTxnScoreboard);

    private native static boolean incrementTxn0(long pTxnScoreboard, long txn);

    private static boolean incrementTxnScoreboardMem(long pTxnScoreboard, long txn) {
        assert pTxnScoreboard > 0;
        LOG.debug().$("increment [p=").$(pTxnScoreboard).$(", txn=").$(fromInternalTxn(txn)).I$();
        return incrementTxn0(pTxnScoreboard, txn);
    }

    private static native void init(long pTxnScoreboard, int entryCount);

    private native static boolean isRangeAvailable0(long pTxnScoreboard, long txnFrom, long txnTo);

    private static long openCleanRW(FilesFacade ff, LPSZ path, long size) {
        final long fd = ff.openCleanRW(path, size);
        if (fd > -1) {
            LOG.debug().$("open clean [file=").$(path).$(", fd=").$(fd).I$();
            return fd;
        }
        throw CairoException.critical(ff.errno()).put("could not open read-write with clean allocation [file=").put(path).put(']');
    }

    private static long releaseTxn(long pTxnScoreboard, long txn) {
        assert pTxnScoreboard > 0;
        LOG.debug().$("release  [p=").$(pTxnScoreboard).$(", txn=").$(txn).I$();
        final long internalTxn = toInternalTxn(txn);
        return releaseTxn0(pTxnScoreboard, internalTxn);
    }

    private native static long releaseTxn0(long pTxnScoreboard, long txn);

    /**
     * Table readers use 0 txn as the empty table transaction number.
     * The scoreboard only supports txn > 0, so we have to patch the value
     * to avoid races in the scoreboard initialization.
     */
    private static long toInternalTxn(long txn) {
        return txn + 1;
    }
}
