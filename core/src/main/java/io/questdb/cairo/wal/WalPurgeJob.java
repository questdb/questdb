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

package io.questdb.cairo.wal;

import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.CairoEngine;
import io.questdb.cairo.CairoException;
import io.questdb.cairo.TableToken;
import io.questdb.cairo.TableUtils;
import io.questdb.cairo.TxReader;
import io.questdb.cairo.mv.MatViewState;
import io.questdb.cairo.sql.TableMetadata;
import io.questdb.cairo.wal.seq.TableSequencerAPI;
import io.questdb.cairo.wal.seq.TransactionLogCursor;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.log.LogRecord;
import io.questdb.mp.SimpleWaitingLock;
import io.questdb.mp.SynchronizedJob;
import io.questdb.std.Files;
import io.questdb.std.FilesFacade;
import io.questdb.std.IntHashSet;
import io.questdb.std.IntIntHashMap;
import io.questdb.std.LongList;
import io.questdb.std.Numbers;
import io.questdb.std.NumericException;
import io.questdb.std.ObjHashSet;
import io.questdb.std.ObjList;
import io.questdb.std.Os;
import io.questdb.std.datetime.Clock;
import io.questdb.std.datetime.millitime.MillisecondClock;
import io.questdb.std.str.DirectUtf8StringZ;
import io.questdb.std.str.LPSZ;
import io.questdb.std.str.Path;
import io.questdb.std.str.Utf8Sequence;

import java.io.Closeable;

public class WalPurgeJob extends SynchronizedJob implements Closeable {
    private static final Log LOG = LogFactory.getLog(WalPurgeJob.class);
    private final TableSequencerAPI.TableSequencerCallback broadSweepRef;
    private final long checkInterval;
    private final ObjList<TableToken> childViewSink = new ObjList<>();
    private final Clock clock;
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

    public WalPurgeJob(CairoEngine engine, FilesFacade ff, Clock clock) {
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
        txReader.close();
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
    private static boolean matchesNumberPattern(Utf8Sequence name) {
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
            discoverSequencerParts();

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
                    LOG.info().$("table is dropped, but has WALs containing segments with pending tasks [table=")
                            .$(tableToken).I$();
                } else if (
                        TableUtils.exists(
                                ff,
                                Path.getThreadLocal(""),
                                configuration.getDbRoot(),
                                tableToken.getDirName()
                        ) != TableUtils.TABLE_EXISTS
                ) {
                    // Fully deregister the table
                    Path pathToDelete = Path.getThreadLocal(configuration.getDbRoot()).concat(tableToken);
                    Path symLinkTarget = null;
                    if (ff.isSoftLink(path.$())) {
                        symLinkTarget = Path.getThreadLocal2("");
                        if (!ff.readLink(pathToDelete, symLinkTarget)) {
                            symLinkTarget = null;
                        }
                    }
                    boolean fullyDeleted = ff.rmdir(pathToDelete, false);
                    if (symLinkTarget != null) {
                        ff.rmdir(symLinkTarget, false);
                    }

                    // Sometimes on Windows sequencer files can be open at this point,
                    // wait for them to be closed before fully removing the token from name registry
                    // and marking table as fully deleted.
                    if (fullyDeleted) {
                        engine.removeTableToken(tableToken);
                        LOG.info().$("table is fully dropped [tableDir=").$(pathToDelete).I$();
                        TableUtils.lockName(pathToDelete);
                        ff.removeQuiet(pathToDelete.$());
                    } else {
                        LOG.info().$("could not fully remove table, some files left on the disk [tableDir=")
                                .$(pathToDelete).I$();
                    }
                } else {
                    LOG.info().$("table is not fully dropped, pinging WAL Apply job to delete table files [table=")
                            .$(tableToken).I$();
                    // Ping ApplyWal2TableJob to clean up the table files
                    engine.notifyWalTxnCommitted(tableToken);
                }
            }
        } catch (CairoException ce) {
            LOG.error().$("broad sweep failed [table=").$(tableToken)
                    .$(", msg=").$((Throwable) ce)
                    .$(", errno=").$(ff.errno())
                    .I$();
        }
    }

    private void discoverSequencerParts() {
        LPSZ path = setSeqPartPath(tableToken).$();
        if (ff.exists(path)) {
            long p = ff.findFirst(path);
            if (p > 0) {
                try {
                    do {
                        int type = ff.findType(p);
                        long pUtf8NameZ = ff.findName(p);

                        if (type == Files.DT_FILE && matchesNumberPattern(walName.of(pUtf8NameZ))) {
                            try {
                                final int partNo = Numbers.parseInt(walName);
                                logic.trackSeqPart(partNo);
                            } catch (NumericException ne) {
                                // Non-Part file directory, ignore.
                            }
                        }
                    } while (ff.findNext(p) > 0);
                } finally {
                    ff.findClose(p);
                }
            }
        }
    }

    private void discoverWalSegments() {
        Path path = setTablePath(tableToken);
        long p = ff.findFirst(path.$());
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
                            long walLockFd = TableUtils.lock(ff, setWalLockPath(tableToken, walId).$(), false);
                            boolean walHasPendingTasks = false;

                            // Search for segments.
                            path.trimTo(rootPathLen).concat(pUtf8NameZ);
                            final int walPathLen = path.size();
                            final long sp = ff.findFirst(path.$());
                            if (sp > 0) {
                                try {
                                    do {
                                        type = ff.findType(sp);
                                        pUtf8NameZ = ff.findName(sp);

                                        if (type == Files.DT_DIR && matchesNumberPattern(walName.of(pUtf8NameZ))) {
                                            try {
                                                final int segmentId = Numbers.parseInt(walName);
                                                if ((segmentId < WalUtils.SEG_MIN_ID) || (segmentId > WalUtils.SEG_MAX_ID)) {
                                                    throw NumericException.instance()
                                                            .position(0)
                                                            .put("segment id out of range [min=").put(WalUtils.SEG_MIN_ID)
                                                            .put(", max=").put(WalUtils.SEG_MAX_ID)
                                                            .put(", segmentId=").put(segmentId)
                                                            .put(']')
                                                            ;
                                                }
                                                path.trimTo(walPathLen);
                                                final Path segmentPath = setSegmentLockPath(tableToken, walId, segmentId);
                                                long lockFd = TableUtils.lock(ff, segmentPath.$(), false);
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
                try (TableMetadata tableMetadata = engine.getTableMetadata(tableToken)) {
                    txReader.ofRO(path.$(), tableMetadata.getTimestampType(), tableMetadata.getPartitionBy());
                    TableUtils.safeReadTxn(txReader, millisecondClock, spinLockTimeout);
                } catch (CairoException ex) {
                    if (engine.isTableDropped(tableToken)) {
                        // This is ok, table dropped while we tried to read the txn
                        return false;
                    }
                    throw ex;
                }
                final long safeToPurgeTxn = getSafeToPurgeUpToTxn(txReader.getSeqTxn());

                TableSequencerAPI tableSequencerAPI = engine.getTableSequencerAPI();
                try (TransactionLogCursor transactionLogCursor = tableSequencerAPI.getCursor(tableToken, safeToPurgeTxn)) {
                    int txnPartSize = transactionLogCursor.getPartitionSize();
                    long currentSeqPart = getCurrentSeqPart(safeToPurgeTxn, txnPartSize);
                    logic.trackCurrentSeqPart(currentSeqPart);
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

    // Segments that are considered safe to delete by PurgeJob may still be used by dependent materialized views.
    // This method is used to determine the safe txn to purge up to.
    private long getSafeToPurgeUpToTxn(long readerSeqTxn) {
        long safeToPurgeTxn = readerSeqTxn;
        childViewSink.clear();
        engine.getMatViewGraph().getDependentViews(tableToken, childViewSink);
        for (int v = 0, n = childViewSink.size(); v < n; v++) {
            final TableToken viewToken = childViewSink.get(v);
            final MatViewState state = engine.getMatViewStateStore().getViewState(viewToken);

            if (state != null && !state.isDropped()) {
                // The first incremental refresh reads full table, without having to read WAL txn intervals.
                // Don't purge WAL segments when the first incremental refresh is running on a mat view.
                // That's to avoid a race between this job and a mat view refresh job leading to refresh
                // with full base table scan executed multiple times. Namely, the first incremental refresh on
                // a view may be about to finish when the purge job checks the txn numbers. If so, the purge job
                // may delete WAL segments required for the second incremental refresh. Yet the refresh job is able
                // to recover from this by falling back to a full table scan, we don't want that to happen.

                final boolean invalid = state.isPendingInvalidation() || state.isInvalid();
                if (state.isLocked() && (state.getLastRefreshBaseTxn() == -1 || invalid)) {
                    // The first refresh must be running.
                    return 0;
                }

                if (!invalid) {
                    final long appliedToViewTxn = Math.max(state.getLastRefreshBaseTxn(), state.getRefreshIntervalsBaseTxn());
                    if (appliedToViewTxn > -1) {
                        // The incremental refresh have run many times for the view.
                        safeToPurgeTxn = Math.min(safeToPurgeTxn, appliedToViewTxn);
                    }
                }
            }
        }
        return safeToPurgeTxn;
    }

    private boolean recursiveDelete(Path path) {
        if (!ff.rmdir(path, false) && !Files.isErrnoFileDoesNotExist(ff.errno())) {
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
        return walDirectoryPolicy.isInUse(path.of(configuration.getDbRoot()).concat(tableToken).concat(WalUtils.SEQ_DIR));
    }

    private Path setSegmentLockPath(TableToken tableName, int walId, int segmentId) {
        TableUtils.lockName(setSegmentPath(tableName, walId, segmentId));
        return path;
    }

    private Path setSegmentPath(TableToken tableName, int walId, int segmentId) {
        return path.of(configuration.getDbRoot())
                .concat(tableName).concat(WalUtils.WAL_NAME_BASE).put(walId).slash().put(segmentId);
    }

    private Path setSeqPartPath(TableToken tableName) {
        return path.of(configuration.getDbRoot())
                .concat(tableName).concat(WalUtils.SEQ_DIR).concat(WalUtils.TXNLOG_PARTS_DIR);
    }

    private Path setTablePath(TableToken tableName) {
        return path.of(configuration.getDbRoot())
                .concat(tableName);
    }

    private void setTxnPath(TableToken tableName) {
        path.of(configuration.getDbRoot())
                .concat(tableName)
                .concat(TableUtils.TXN_FILE_NAME);
    }

    private Path setWalLockPath(TableToken tableName, int walId) {
        path.of(configuration.getDbRoot())
                .concat(tableName).concat(WalUtils.WAL_NAME_BASE).put(walId);
        TableUtils.lockName(path);
        return path;
    }

    private Path setWalPath(TableToken tableName, int walId) {
        return path.of(configuration.getDbRoot())
                .concat(tableName).concat(WalUtils.WAL_NAME_BASE).put(walId);
    }

    protected long getCurrentSeqPart(long lastAppliedTxn, int txnPartSize) {
        // There can be more advanced override which uses more parameters than this implementation.
        if (txnPartSize > 0) {
            // we don't want to purge the part where the last txn is in. Txn is 1-based.
            return (lastAppliedTxn - 1) / txnPartSize;
        }
        return Long.MAX_VALUE;
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
        }
        return false;
    }

    public interface Deleter {
        void deleteSegmentDirectory(int walId, int segmentId, long lockFd);

        void deleteSequencerPart(int seqPart);

        void deleteWalDirectory(int walId, long lockFd);

        void unlock(long lockFd);
    }

    public static class Logic {
        private final Deleter deleter;
        private final LongList discovered = new LongList();
        private final IntIntHashMap nextToApply = new IntIntHashMap();
        private final int waitBeforeDelete;
        private long currentSeqPart;
        private boolean logged;
        private boolean sequencerPending;
        private TableToken tableToken;


        public Logic(Deleter deleter, int waitBeforeDelete) {
            this.deleter = deleter;
            this.waitBeforeDelete = waitBeforeDelete;
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
            currentSeqPart = -1;
            logged = false;
        }

        public void run() {
            int i = 0, n = getStateSize();
            if (n > 0 && waitBeforeDelete > 0) {
                Os.sleep(waitBeforeDelete);
            }

            try {
                for (; i < n; i++) {
                    long lockFd = getLockFd(i);
                    final int walId = getWalId(i);
                    final int segmentId = getSegmentId(i);
                    final int nextToApplySegmentId = nextToApply.get(walId);  // -1 if not found
                    if (lockFd > -1) {
                        // Delete a wal or segment directory only if:
                        //   * It has been fully applied to the table.
                        //   * Is not locked.
                        //   * None of its segments have pending tasks.

                        if (isWalDir(segmentId, walId)) {
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
                    } else {
                        final int seqPart = getSeqPart(walId, segmentId); // -1 if not a seq part
                        if (seqPart > -1 && seqPart < currentSeqPart) {
                            logDebugInfo();
                            deleter.deleteSequencerPart(seqPart);
                        }
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

        public void trackCurrentSeqPart(long partNo) {
            currentSeqPart = partNo;
        }

        public void trackDiscoveredSegment(int walId, int segmentId, long lockFd) {
            discovered.add(walId);
            discovered.add(segmentId);
            discovered.add(lockFd);
        }

        public void trackDiscoveredWal(int walId, long lockFd) {
            trackDiscoveredSegment(walId, WalUtils.SEG_NONE_ID, lockFd);
        }

        public void trackNextToApplySegment(int walId, int segmentId) {
            final int index = nextToApply.keyIndex(walId);
            if (index > -1) {  // not tracked yet
                nextToApply.putAt(index, walId, segmentId);
            }
        }

        public void trackSeqPart(int part) {
            discovered.add(WalUtils.METADATA_WALID);
            discovered.add(part);
            discovered.add(-1);
        }

        private static boolean isWalDir(int segmentId, int walId) {
            return segmentId == WalUtils.SEG_NONE_ID && walId != WalUtils.METADATA_WALID;
        }

        private long getLockFd(int index) {
            return discovered.get(index * 3 + 2);
        }

        private int getSegmentId(int index) {
            return (int) discovered.get(index * 3 + 1);
        }

        private int getSeqPart(int walId, int segmentId) {
            return walId == WalUtils.METADATA_WALID ? segmentId : -1;
        }

        private int getStateSize() {
            return discovered.size() / 3;
        }

        private int getWalId(int index) {
            return (int) discovered.get(index * 3);
        }

        private void logDebugInfo() {
            if (!logged) {
                printDebugState();
                logged = true;
            }
        }

        private void printDebugState() {
            LogRecord log = LOG.info();

            try {
                log.$("table=").$(tableToken).$(", discovered=[");

                for (int i = 0, n = getStateSize(); i < n; i++) {
                    final int walId = getWalId(i);
                    final int segmentId = getSegmentId(i);
                    final long lockId = getLockFd(i);
                    final int partNo = getSeqPart(walId, segmentId);

                    if (partNo > -1) {
                        log.$("seqPart=").$(partNo);
                        if (partNo == currentSeqPart) {
                            log.$(":current");
                        }
                    } else {
                        if (isWalDir(segmentId, walId)) {
                            log.$("(wal").$(walId);
                        } else {
                            final int nextToApplyId = nextToApply.get(walId);
                            log.$('(').$(walId).$(',').$(segmentId);
                            if (segmentId == nextToApplyId) {
                                log.$(":next");
                            }
                        }

                        if (lockId < 0) {
                            log.$(":busy");
                        }
                        log.$(')');
                    }

                    if (i < n - 1) {
                        log.$(',');
                    }
                }
            } finally {
                log.I$();
            }
        }
    }

    private class FsDeleter implements Deleter {

        @Override
        public void deleteSegmentDirectory(int walId, int segmentId, long lockFd) {
            LOG.debug().$("deleting WAL segment directory [table=").$(tableToken)
                    .$(", walId=").$(walId)
                    .$(", segmentId=").$(segmentId)
                    .I$();
            if (recursiveDelete(setSegmentPath(tableToken, walId, segmentId))) {
                ff.closeRemove(lockFd, setSegmentLockPath(tableToken, walId, segmentId).$());
            } else {
                ff.close(lockFd);
            }
        }

        @Override
        public void deleteSequencerPart(int seqPart) {
            LOG.debug().$("deleting sequencer part [table=").$(tableToken)
                    .$(", part=").$(seqPart)
                    .I$();
            Path path = setSeqPartPath(tableToken).put(Files.SEPARATOR).put(seqPart);
            // If error removing, will be retried on next run.
            ff.removeQuiet(path.$());
        }

        @Override
        public void deleteWalDirectory(int walId, long lockFd) {
            LOG.debug().$("deleting WAL directory [table=").$(tableToken)
                    .$(", walId=").$(walId)
                    .I$();
            if (recursiveDelete(setWalPath(tableToken, walId))) {
                ff.closeRemove(lockFd, setWalLockPath(tableToken, walId).$());
            } else {
                ff.close(lockFd);
            }
        }

        @Override
        public void unlock(long lockFd) {
            if (lockFd > -1) {
                ff.close(lockFd);
            }
        }
    }
}
