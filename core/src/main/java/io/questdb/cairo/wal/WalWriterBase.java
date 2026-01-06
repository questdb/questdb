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

package io.questdb.cairo.wal;

import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.CairoException;
import io.questdb.cairo.TableToken;
import io.questdb.cairo.TableUtils;
import io.questdb.cairo.wal.seq.TableSequencerAPI;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.std.FilesFacade;
import io.questdb.std.str.Path;
import org.jetbrains.annotations.NotNull;

import static io.questdb.cairo.TableUtils.lockName;
import static io.questdb.cairo.wal.WalUtils.WAL_NAME_BASE;
import static io.questdb.cairo.wal.seq.TableSequencer.NO_TXN;

abstract class WalWriterBase implements AutoCloseable {
    private static final Log LOG = LogFactory.getLog(WalWriterBase.class);

    protected final CairoConfiguration configuration;
    protected final WalEventWriter events;
    protected final FilesFacade ff;
    protected final int mkDirMode;
    protected final Path path;
    protected final int pathRootSize;
    protected final int pathSize;
    protected final TableSequencerAPI sequencer;
    protected final WalDirectoryPolicy walDirectoryPolicy;
    protected final int walId;
    protected final String walName;
    protected boolean distressed;
    protected int lastSegmentTxn = -1;
    protected long lastSeqTxn = NO_TXN;
    protected boolean open;
    protected boolean rollSegmentOnNextRow = false;
    protected int segmentId = -1;
    protected long segmentLockFd = -1;
    protected TableToken tableToken;
    protected long walLockFd = -1;

    WalWriterBase(
            CairoConfiguration configuration,
            TableToken tableToken,
            TableSequencerAPI tableSequencerAPI,
            WalDirectoryPolicy walDirectoryPolicy
    ) {
        this.sequencer = tableSequencerAPI;
        this.configuration = configuration;
        this.walDirectoryPolicy = walDirectoryPolicy;
        this.tableToken = tableToken;

        mkDirMode = configuration.getMkDirMode();
        ff = configuration.getFilesFacade();
        walId = tableSequencerAPI.getNextWalId(tableToken);
        walName = WAL_NAME_BASE + walId;
        path = new Path();
        path.of(configuration.getDbRoot());
        pathRootSize = configuration.getDbLogName() == null ? path.size() : 0;
        path.concat(tableToken).concat(walName);
        pathSize = path.size();
        open = true;
        events = new WalEventWriter(configuration);
    }

    public @NotNull TableToken getTableToken() {
        return tableToken;
    }

    public int getWalId() {
        return walId;
    }

    public String getWalName() {
        return walName;
    }

    public boolean isDistressed() {
        return distressed;
    }

    public boolean isOpen() {
        return open;
    }

    public void rollSegment() {
        try {
            openNewSegment();
        } catch (Throwable e) {
            distressed = true;
            throw e;
        }
    }

    long acquireSegmentLock() {
        final int segmentPathLen = path.size();
        try {
            lockName(path);
            final long segmentLockFd = TableUtils.lock(ff, path.$());
            if (segmentLockFd == -1) {
                path.trimTo(segmentPathLen);
                throw CairoException.critical(ff.errno()).put("Cannot lock wal segment: ").put(path);
            }
            return segmentLockFd;
        } finally {
            path.trimTo(segmentPathLen);
        }
    }

    abstract boolean breachedRolloverSizeThreshold();

    void checkDistressed() {
        if (!distressed) {
            return;
        }
        throw CairoException.critical(0)
                .put("WAL writer is distressed and cannot be used any more [table=").put(tableToken.getTableName())
                .put(", wal=").put(walId).put(']');
    }

    abstract void commit();

    int createSegmentDir(int segmentId) {
        path.trimTo(pathSize);
        path.slash().put(segmentId);
        final int segmentPathLen = path.size();
        segmentLockFd = acquireSegmentLock();
        if (ff.mkdirs(path.slash(), mkDirMode) != 0) {
            throw CairoException.critical(ff.errno()).put("Cannot create WAL segment directory: ").put(path);
        }
        walDirectoryPolicy.initDirectory(path);
        path.trimTo(segmentPathLen);
        return segmentPathLen;
    }

    abstract long getSequencerTxn();

    boolean isTruncateFilesOnClose() {
        return walDirectoryPolicy.truncateFilesOnClose();
    }

    void lockWal() {
        try {
            lockName(path);
            walLockFd = TableUtils.lock(ff, path.$());
        } finally {
            path.trimTo(pathSize);
        }

        if (walLockFd == -1) {
            throw CairoException.critical(ff.errno()).put("cannot lock table: ").put(path.$());
        }
    }

    abstract void mayRollSegmentOnNextRow();

    void mkWalDir() {
        final int walDirLength = path.size();
        if (ff.mkdirs(path.slash(), mkDirMode) != 0) {
            throw CairoException.critical(ff.errno()).put("Cannot create WAL directory: ").put(path);
        }
        path.trimTo(walDirLength);
    }

    abstract void openNewSegment();

    void releaseSegmentLock(int segmentId, long segmentLockFd, long segmentTxn) {
        if (ff.close(segmentLockFd)) {
            // if events file has some transactions
            if (segmentTxn >= 0) {
                sequencer.notifySegmentClosed(tableToken, lastSeqTxn, walId, segmentId);
                LOG.debug().$("released segment lock [walId=").$(walId)
                        .$(", segmentId=").$(segmentId)
                        .$(", fd=").$(segmentLockFd)
                        .$(']').$();
            } else {
                path.trimTo(pathSize).slash().put(segmentId);
                walDirectoryPolicy.rollbackDirectory(path);
                path.trimTo(pathSize);
            }
        } else {
            LOG.error()
                    .$("cannot close segment lock fd [walId=").$(walId)
                    .$(", segmentId=").$(segmentId)
                    .$(", fd=").$(segmentLockFd)
                    .$(", errno=").$(ff.errno()).I$();
        }
    }

    void releaseWalLock() {
        if (ff.close(walLockFd)) {
            walLockFd = -1;
            LOG.debug().$("released WAL lock [walId=").$(walId)
                    .$(", fd=").$(walLockFd)
                    .$(']').$();
        } else {
            LOG.error()
                    .$("cannot close WAL lock fd [walId=").$(walId)
                    .$(", fd=").$(walLockFd)
                    .$(", errno=").$(ff.errno()).I$();
        }
    }

    abstract void rollback();
}
