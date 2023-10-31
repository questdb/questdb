/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2023 QuestDB
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

import io.questdb.cairo.*;
import io.questdb.cairo.wal.seq.TableSequencerAPI;
import io.questdb.cairo.wal.seq.TransactionLogCursor;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.mp.SimpleWaitingLock;
import io.questdb.mp.SynchronizedJob;
import io.questdb.std.*;
import io.questdb.std.datetime.microtime.MicrosecondClock;
import io.questdb.std.datetime.millitime.MillisecondClock;
import io.questdb.std.str.DirectUtf8StringZ;
import io.questdb.std.str.Path;
import io.questdb.std.str.StringSink;
import io.questdb.std.str.Utf8Sequence;

import java.io.Closeable;

public class WalPurgeJob extends SynchronizedJob implements Closeable {
    private static final Log LOG = LogFactory.getLog(WalPurgeJob.class);
    private final TableSequencerAPI.TableSequencerCallback broadSweepRef;
    private final long checkInterval;
    private final MicrosecondClock clock;
    private final CairoConfiguration configuration;
    private final CairoEngine engine;
    private final FilesFacade ff;
    private final Logic logic;
    private final MillisecondClock millisecondClock;
    private final IntHashSet onDiskWalIDSet = new IntHashSet();
    private final Path path = new Path();
    private final SimpleWaitingLock runLock = new SimpleWaitingLock();
    private final long spinLockTimeout;
    private final ObjHashSet<TableToken> tableTokenBucket = new ObjHashSet<>();
    private final TxReader txReader;
    private final WalDirectoryPolicy walDirectoryPolicy;
    private final DirectUtf8StringZ walName = new DirectUtf8StringZ();
    private long last = 0;
    private TableToken tableToken;

    public WalPurgeJob(CairoEngine engine, FilesFacade ff, MicrosecondClock clock) {
        this.engine = engine;
        this.ff = ff;
        this.clock = clock;
        this.checkInterval = engine.getConfiguration().getWalPurgeInterval() * 1000;
        this.millisecondClock = engine.getConfiguration().getMillisecondClock();
        this.spinLockTimeout = engine.getConfiguration().getSpinLockTimeout();
        this.txReader = new TxReader(ff);
        this.broadSweepRef = this::broadSweep;
        this.walDirectoryPolicy = engine.getWalDirectoryPolicy();

        // some code here assumes that WAL_NAME_BASE is "wal", this is to fail the tests if it is not
        //noinspection ConstantConditions
        assert WalUtils.WAL_NAME_BASE.equals("wal");
        configuration = engine.getConfiguration();

        logic = new Logic(new FsDeleter(), engine.getConfiguration().getWalPurgeWaitBeforeDelete());
    }

    public WalPurgeJob(CairoEngine engine) {
        this(engine, engine.getConfiguration().getFilesFacade(), engine.getConfiguration().getMicrosecondClock());
    }

    @Override
    public void close() {
        this.txReader.close();
        path.close();
    }

    /**
     * Delay the first run of this job by half a configured interval to
     * spread its work more evenly across other timer-based jobs with
     * similar cadences.
     */
    public void delayByHalfInterval() {
        this.last = clock.getTicks() - (checkInterval / 2);
    }

    public SimpleWaitingLock getRunLock() {
        return runLock;
    }

    /**
     * Validate equivalent of "^\d+$" regex.
     */
    private static boolean matchesSegmentName(Utf8Sequence name) {
        for (int i = 0, n = name.size(); i < n; i++) {
            final byte b = name.byteAt(i);
            if (b < '0' || b > '9') {
                return false;
            }
        }
        return true;
    }

    /**
     * Validate equivalent of "^wal\d+$" regex.
     */
    private static boolean matchesWalNamePattern(Utf8Sequence name) {
        final int len = name.size();
        if (len < (WalUtils.WAL_NAME_BASE.length() + 1)) {
            return false;
        }
        if (name.byteAt(0) != 'w' || name.byteAt(1) != 'a' || name.byteAt(2) != 'l') {
            return false;  // Not a "wal" prefix.
        }
        for (int i = 3; i < len; ++i) {
            final byte b = name.byteAt(i);
            if (b < '0' || b > '9') {
                return false; // Not a number.
            }
        }
        return true;
    }

    /**
     * Perform a broad sweep that searches for all tables that have closed
     * WAL segments across the database and deletes any which are no longer needed.
     */
    private void broadSweep() {
        engine.getTableSequencerAPI().forAllWalTables(tableTokenBucket, true, broadSweepRef);
    }

    private void broadSweep(int tableId, final TableToken tableToken, long lastTxn) {
        try {
            this.tableToken = tableToken;
            this.logic.reset(tableToken);
            onDiskWalIDSet.clear();

            boolean tableDropped = false;
            discoverWalSegments();
            if (logic.hasOnDiskSegments()) {

                try {
                    tableDropped = fetchSequencerPairs();
                } catch (Throwable th) {
                    logic.releaseLocks();
                    throw th;
                }
                // Any of the calls above may leave outstanding `discoveredWalIds` that are still on the filesystem
                // and don't have any active segments. Any unlocked walNNN directories may be deleted if they don't have
                // pending segments that are yet to be applied to the table.
                // Note that this also handles cases where a wal directory was created shortly before a crash and thus
                // never recorded and tracked by the sequencer for that table.
                logic.run();
            }

            if (tableDropped || (lastTxn < 0 && engine.isTableDropped(tableToken))) {
                if (logic.hasPendingTasks()) {
                    LOG.info().$("table is dropped, but has WALs containing segments with pending tasks ")
                            .$("[tableDir=").$(tableToken.getDirName()).I$();
                } else if (
                        TableUtils.exists(
                                ff,
                                Path.getThreadLocal(""),
                                configuration.getRoot(),
                                tableToken.getDirName()
                        ) != TableUtils.TABLE_EXISTS
                ) {
                    // Fully deregister the table
                    LOG.info().$("table is fully dropped [tableDir=").$(tableToken.getDirName()).I$();
                    Path pathToDelete = Path.getThreadLocal(configuration.getRoot()).concat(tableToken).$();
                    Path symLinkTarget = null;
                    if (ff.isSoftLink(path)) {
                        symLinkTarget = Path.getThreadLocal2("");
                        if (!ff.readLink(pathToDelete, symLinkTarget)) {
                            symLinkTarget = null;
                        }
                    }
                    boolean fullyDeleted = ff.rmdir(pathToDelete, false);
                    if (symLinkTarget != null) {
                        ff.rmdir(symLinkTarget, false);
                    }
                    TableUtils.lockName(pathToDelete);

                    // Sometimes on Windows sequencer files can be open at this point,
                    // wait for them to be closed before fully removing the token from name registry
                    // and marking table as fully deleted.
                    if (fullyDeleted) {
                        engine.removeTableToken(tableToken);
                    }
                } else {
                    LOG.info().$("table is not fully dropped, pinging WAL Apply job to delete table files [tableDir=").$(tableToken.getDirName()).I$();
                    // Ping ApplyWal2TableJob to clean up the table files
                    engine.notifyWalTxnCommitted(tableToken);
                }
            }
        } catch (CairoException ce) {
            LOG.error().$("broad sweep failed [table=").$(tableToken)
                    .$(", msg=").$((Throwable) ce)
                    .$(", errno=").$(ff.errno()).$(']').$();
        }
    }

    private void discoverWalSegments() {
        Path path = setTablePath(tableToken);
        long p = ff.findFirst(path);
        int rootPathLen = path.size();
        logic.sequencerHasPendingTasks(sequencerHasPendingTasks());
        if (p > 0) {
            try {
                do {
                    int type = ff.findType(p);
                    long pUtf8NameZ = ff.findName(p);

                    if (type == Files.DT_DIR && matchesWalNamePattern(walName.of(pUtf8NameZ))) {
                        try {
                            final int walId = Numbers.parseInt(walName, 3, walName.size());
                            onDiskWalIDSet.add(walId);
                            int walLockFd = TableUtils.lock(ff, setWalLockPath(tableToken, walId), false);
                            boolean walHasPendingTasks = false;

                            // Search for segments.
                            path.trimTo(rootPathLen).concat(pUtf8NameZ);
                            final int walPathLen = path.size();
                            final long sp = ff.findFirst(path.$());

                            try {
                                do {
                                    type = ff.findType(sp);
                                    pUtf8NameZ = ff.findName(sp);

                                    if (type == Files.DT_DIR && matchesSegmentName(walName.of(pUtf8NameZ))) {
                                        try {
                                            final int segmentId = Numbers.parseInt(walName);
                                            if ((segmentId < WalUtils.SEG_MIN_ID) || (segmentId > WalUtils.SEG_MAX_ID)) {
                                                throw NumericException.INSTANCE;
                                            }
                                            path.trimTo(walPathLen);
                                            final Path segmentPath = setSegmentLockPath(tableToken, walId, segmentId);
                                            int lockFd = TableUtils.lock(ff, segmentPath, false);
                                            if (lockFd > -1) {
                                                final boolean pendingTasks = segmentHasPendingTasks(walId, segmentId);
                                                if (pendingTasks) {
                                                    // Treat is as being locked.
                                                    ff.close(lockFd);
                                                    lockFd = -1;
                                                }
                                            }
                                            walHasPendingTasks |= lockFd < 0;
                                            logic.trackDiscoveredSegment(walId, segmentId, lockFd);
                                        } catch (NumericException ne) {
                                            // Non-Segment directory, ignore.
                                        }
                                    }
                                } while (ff.findNext(sp) > 0);
                            } finally {
                                ff.findClose(sp);
                            }
                            if (walLockFd > -1 && walHasPendingTasks) {
                                // WAL dir cannot be deleted, there are busy segments.
                                // Unlock it.
                                ff.close(walLockFd);
                                walLockFd = -1;
                            }
                            logic.trackDiscoveredWal(walId, walLockFd);
                        } catch (NumericException ne) {
                            // Non-WAL directory, ignore.
                        }
                    }
                } while (ff.findNext(p) > 0);
            } finally {
                ff.findClose(p);
            }
        }
    }

    private boolean fetchSequencerPairs() {
        setTxnPath(tableToken);
        if (!engine.isTableDropped(tableToken)) {
            try {
                try {
                    txReader.ofRO(path, PartitionBy.NONE);
                    TableUtils.safeReadTxn(txReader, millisecondClock, spinLockTimeout);
                } catch (CairoException ex) {
                    if (engine.isTableDropped(tableToken)) {
                        // This is ok, table dropped while we tried to read the txn
                        return false;
                    }
                    throw ex;
                }
                final long lastAppliedTxn = txReader.getSeqTxn();

                TableSequencerAPI tableSequencerAPI = engine.getTableSequencerAPI();
                try (TransactionLogCursor transactionLogCursor = tableSequencerAPI.getCursor(tableToken, lastAppliedTxn)) {
                    while (onDiskWalIDSet.size() > 0 && transactionLogCursor.hasNext()) {
                        int walId = transactionLogCursor.getWalId();
                        if (onDiskWalIDSet.remove(walId) != -1) {
                            int segmentId = transactionLogCursor.getSegmentId();
                            logic.trackNextToApplySegment(walId, segmentId);
                        }
                    }
                } catch (CairoException e) {
                    if (e.isTableDropped()) {
                        // there was a race, we lost
                        return true;
                    } else {
                        throw e;
                    }
                }
            } finally {
                txReader.close();
            }
        }
        return false;
        // If table is dropped, all wals can be deleted.
        // No need to do anything, all discovered segments / wals will be deleted
    }

    private boolean recursiveDelete(Path path) {
        if (!ff.rmdir(path, false) && !CairoException.errnoRemovePathDoesNotExist(ff.errno())) {
            LOG.debug()
                    .$("could not delete directory [path=").$(path)
                    .$(", errno=").$(ff.errno())
                    .I$();
            return false;
        }
        return true;
    }

    /**
     * Check if the segment directory has any outstanding ".pending" marker files in the ".pending" directory.
     */
    private boolean segmentHasPendingTasks(int walId, int segmentId) {
        return walDirectoryPolicy.isInUse(setSegmentPath(tableToken, walId, segmentId));
    }

    private boolean sequencerHasPendingTasks() {
        return walDirectoryPolicy.isInUse(path.of(configuration.getRoot()).concat(tableToken).concat(WalUtils.SEQ_DIR));
    }

    private Path setSegmentLockPath(TableToken tableName, int walId, int segmentId) {
        TableUtils.lockName(setSegmentPath(tableName, walId, segmentId));
        return path;
    }

    private Path setSegmentPath(TableToken tableName, int walId, int segmentId) {
        return path.of(configuration.getRoot())
                .concat(tableName).concat(WalUtils.WAL_NAME_BASE).put(walId).slash().put(segmentId);
    }

    private Path setTablePath(TableToken tableName) {
        return path.of(configuration.getRoot())
                .concat(tableName).$();
    }

    private void setTxnPath(TableToken tableName) {
        path.of(configuration.getRoot())
                .concat(tableName)
                .concat(TableUtils.TXN_FILE_NAME).$();
    }

    private Path setWalLockPath(TableToken tableName, int walId) {
        path.of(configuration.getRoot())
                .concat(tableName).concat(WalUtils.WAL_NAME_BASE).put(walId);
        TableUtils.lockName(path);
        return path;
    }

    private Path setWalPath(TableToken tableName, int walId) {
        return path.of(configuration.getRoot())
                .concat(tableName).concat(WalUtils.WAL_NAME_BASE).put(walId).$();
    }

    @Override
    protected boolean runSerially() {
        final long t = clock.getTicks();
        if (last + checkInterval < t) {
            last = t;
            if (runLock.tryLock()) {
                try {
                    broadSweep();
                } finally {
                    runLock.unlock();
                }
            } else {
                LOG.info().$("skipping, locked out").$();
            }
            return false;
        } else {
            return false;
        }
    }

    public interface Deleter {
        void deleteSegmentDirectory(int walId, int segmentId, int lockFd);

        void deleteWalDirectory(int walId, int lockFd);

        void unlock(int lockFd);
    }

    public static class Logic {
        private final StringSink debugBuffer = new StringSink();
        private final Deleter deleter;
        private final IntList discovered = new IntList();
        private final IntIntHashMap nextToApply = new IntIntHashMap();
        private final int waitBeforeDelete;
        private boolean sequencerPending;
        private TableToken tableToken;


        public Logic(Deleter deleter, int waitBeforeDelete) {
            this.deleter = deleter;
            this.waitBeforeDelete = waitBeforeDelete;
        }

        public void accumDebugState() {
            debugBuffer.clear();
            debugBuffer.put("table=").put(tableToken.getDirName())
                    .put(", discovered=[");

            for (int i = 0, n = getStateSize(); i < n; i++) {
                final int walId = getWalId(i);
                final int nextToApplyId = nextToApply.get(walId);
                final int segmentId = getSegmentId(i);
                final int lockId = getLockFd(i);
                if (isWalDir(segmentId)) {
                    debugBuffer.put("(wal").put(walId);
                } else {
                    debugBuffer.put('(').put(walId).put(',').put(segmentId);
                    if (segmentId == nextToApplyId) {
                        debugBuffer.put(":next");
                    }
                }

                if (lockId < 0) {
                    debugBuffer.put(":busy");
                }
                debugBuffer.put(')');

                if (i < n - 1) {
                    debugBuffer.put(',');
                }
            }

            debugBuffer.put(']');
        }

        public boolean hasOnDiskSegments() {
            return discovered.size() != 0;
        }

        public boolean hasPendingTasks() {
            if (sequencerPending) {
                return true;
            }
            for (int i = 0; i < getStateSize(); ++i) {
                if (getLockFd(i) < 0) {
                    return true;
                }
            }
            return false;
        }

        public void releaseLocks() {
            for (int i = 0, n = getStateSize(); i < n; ++i) {
                deleter.unlock(getLockFd(i));
            }
        }

        public void reset(TableToken tableToken) {
            this.tableToken = tableToken;
            nextToApply.clear();
            discovered.clear();
            sequencerPending = false;
        }

        public void run() {
            accumDebugState();
            int i = 0, n = getStateSize();
            if (n > 0 && waitBeforeDelete > 0) {
                Os.sleep(waitBeforeDelete);
            }

            try {
                for (; i < n; i++) {
                    int lockFd = getLockFd(i);
                    if (lockFd > -1) {
                        final int walId = getWalId(i);
                        final int segmentId = getSegmentId(i);
                        final int nextToApplySegmentId = nextToApply.get(walId);  // -1 if not found

                        // Delete a wal or segment directory only if:
                        //   * It has been fully applied to the table.
                        //   * Is not locked.
                        //   * None of its segments have pending tasks

                        if (isWalDir(segmentId)) {
                            final boolean walAlreadyApplied = nextToApplySegmentId == -1;
                            if (walAlreadyApplied) {
                                logDebugInfo();
                                deleter.deleteWalDirectory(walId, lockFd);
                                continue;
                            }
                        } else {
                            final boolean segmentAlreadyApplied = (nextToApplySegmentId == -1) || (nextToApplySegmentId > segmentId);
                            if (segmentAlreadyApplied) {
                                logDebugInfo();
                                deleter.deleteSegmentDirectory(walId, segmentId, lockFd);
                                continue;
                            }
                        }
                        deleter.unlock(lockFd);
                    }
                }
            } finally {
                for (; i < n; i++) {
                    deleter.unlock(getLockFd(i));
                }
            }
        }

        public void sequencerHasPendingTasks(boolean isPending) {
            sequencerPending = isPending;
        }

        public void trackDiscoveredSegment(int walId, int segmentId, int lockFd) {
            discovered.add(walId);
            discovered.add(segmentId);
            discovered.add(lockFd);
        }

        public void trackDiscoveredWal(int walId, int lockFd) {
            trackDiscoveredSegment(walId, WalUtils.SEG_NONE_ID, lockFd);
        }

        public void trackNextToApplySegment(int walId, int segmentId) {
            final int index = nextToApply.keyIndex(walId);
            if (index > -1) {  // not tracked yet
                nextToApply.putAt(index, walId, segmentId);
            }
        }

        private static boolean isWalDir(int segmentId) {
            return segmentId == WalUtils.SEG_NONE_ID;
        }

        private int getLockFd(int index) {
            return discovered.get(index * 3 + 2);
        }

        private int getSegmentId(int index) {
            return discovered.get(index * 3 + 1);
        }

        private int getStateSize() {
            return discovered.size() / 3;
        }

        private int getWalId(int index) {
            return discovered.get(index * 3);
        }

        private void logDebugInfo() {
            if (debugBuffer.length() > 0) {
                LOG.info().utf8(debugBuffer).$();
                debugBuffer.clear();
            }
        }
    }

    private class FsDeleter implements Deleter {
        @Override
        public void deleteSegmentDirectory(int walId, int segmentId, int lockFd) {
            LOG.debug().$("deleting WAL segment directory [table=").utf8(tableToken.getDirName())
                    .$(", walId=").$(walId)
                    .$(", segmentId=").$(segmentId).$(']').$();
            if (recursiveDelete(setSegmentPath(tableToken, walId, segmentId).$())) {
                ff.closeRemove(lockFd, setSegmentLockPath(tableToken, walId, segmentId));
            } else {
                ff.close(lockFd);
            }
        }

        @Override
        public void deleteWalDirectory(int walId, int lockFd) {
            LOG.debug().$("deleting WAL directory [table=").utf8(tableToken.getDirName())
                    .$(", walId=").$(walId).$(']').$();
            if (recursiveDelete(setWalPath(tableToken, walId))) {
                ff.closeRemove(lockFd, setWalLockPath(tableToken, walId));
            } else {
                ff.close(lockFd);
            }
        }

        @Override
        public void unlock(int lockFd) {
            if (lockFd > -1) {
                ff.close(lockFd);
            }
        }
    }
}
