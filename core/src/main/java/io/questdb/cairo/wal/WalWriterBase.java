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
import io.questdb.cairo.wal.seq.TableSequencerAPI;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.std.FilesFacade;
import io.questdb.std.str.Path;
import org.jetbrains.annotations.NotNull;

import static io.questdb.cairo.wal.WalUtils.WAL_NAME_BASE;
import static io.questdb.cairo.wal.seq.TableSequencer.NO_TXN;

abstract class WalWriterBase implements AutoCloseable {
    private static final Log LOG = LogFactory.getLog(WalWriterBase.class);

    final CairoConfiguration configuration;
    final WalEventWriter events;
    final FilesFacade ff;
    final int mkDirMode;
    final Path path;
    final int pathRootSize;
    final int pathSize;
    final TableSequencerAPI sequencer;
    final WalDirectoryPolicy walDirectoryPolicy;
    final int walId;
    final WalLocker walLocker;
    final String walName;
    boolean distressed;
    int lastSegmentTxn = -1;
    long lastSeqTxn = NO_TXN;
    int minSegmentLocked = -1;
    boolean open;
    boolean rollSegmentOnNextRow = false;
    int segmentId = -1;
    TableToken tableToken;

    WalWriterBase(
            CairoConfiguration configuration,
            TableToken tableToken,
            TableSequencerAPI tableSequencerAPI,
            WalDirectoryPolicy walDirectoryPolicy,
            WalLocker walLocker
    ) {
        this.sequencer = tableSequencerAPI;
        this.configuration = configuration;
        this.walDirectoryPolicy = walDirectoryPolicy;
        this.tableToken = tableToken;
        this.walLocker = walLocker;

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

    int createSegmentDir(int segmentId) {
        path.trimTo(pathSize);
        path.slash().put(segmentId);
        final int segmentPathLen = path.size();
        if (ff.mkdirs(path.slash(), mkDirMode) != 0) {
            throw CairoException.critical(ff.errno()).put("Cannot create WAL segment directory: ").put(path);
        }
        walDirectoryPolicy.initDirectory(path);
        path.trimTo(segmentPathLen);
        return segmentPathLen;
    }

    void lockWal() {
        walLocker.lockWriter(tableToken, walId, 0);
        minSegmentLocked = 0;
        LOG.debug().$("locked WAL [walId=").$(walId).I$();
    }

    void mkWalDir() {
        final int walDirLength = path.size();
        if (ff.mkdirs(path.slash(), mkDirMode) != 0) {
            throw CairoException.critical(ff.errno()).put("Cannot create WAL directory: ").put(path);
        }
        path.trimTo(walDirLength);
    }

    /**
     * Move the minimum segment lock forward.
     *
     * @return true if the lock was moved, false if the newMinSegmentId is not greater than the current minSegmentLocked
     */
    boolean moveMinSegmentLock(int newSegmentId) {
        if (newSegmentId <= minSegmentLocked) {
            return false;
        }

        walLocker.setWalSegmentMinId(tableToken, walId, newSegmentId);
        minSegmentLocked = newSegmentId;
        return true;
    }

    void notifySegmentClosure(long oldLastSegmentTxn, int segmentId) {
        if (oldLastSegmentTxn >= 0) {
            sequencer.notifySegmentClosed(tableToken, lastSeqTxn, walId, segmentId);
            LOG.debug().$("notified segment closed [walId=").$(walId)
                    .$(", segmentId=").$(segmentId)
                    .I$();
        } else {
            path.trimTo(pathSize).slash().put(segmentId);
            walDirectoryPolicy.rollbackDirectory(path);
            path.trimTo(pathSize);
        }
    }

    void releaseWalLock() {
        walLocker.unlockWriter(tableToken, walId);
        LOG.debug().$("released WAL lock [walId=").$(walId).I$();
    }
}
