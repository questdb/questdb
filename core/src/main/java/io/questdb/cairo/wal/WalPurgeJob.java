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
import io.questdb.std.IntList;
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
    private final WalLocker walLocker;
    private final DirectUtf8StringZ walName = new DirectUtf8StringZ();
    private long last = 0;
    private TableToken tableToken;

    public WalPurgeJob(CairoEngine engine, FilesFacade ff, Clock clock) {
        this.engine = engine;
        this.ff = ff;
        this.clock = clock;
        this.walLocker = engine.getWalLocker();
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
     * <p>
     * Note:
     * Purge uses three phases: (1) lock & discover, (2) query sequencer, (3) delete & unlock.
     * We cannot delete during discovery because delete decisions require sequencer state
     * (nextToApply position, materialized view dependencies) which we only learn in phase 2.
     * <p>
     * Locking all WALs before querying the sequencer ensures consistency:
     * - Exclusive lock (no writer): safe to delete entire WAL, no new txns possible;
     * new downloaders block until purge unlocks
     * - Shared lock (writer active): purge gets minSegmentId boundary; segments below are
     * finalized (no new txns will reference them), segments at/above belong to writer
     * <p>
     * In enterprise, the WAL downloader can add segments to unlocked WALs and commit txns.
     * By locking first, we ensure the sequencer returns a consistent "safe to delete" view.
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
            boolean hasPendingTasks;
            try {
                hasPendingTasks = discoverWalSegments();
                hasPendingTasks |= discoverSequencerParts();
            } catch (Throwable th) {
                logic.releaseLocks();
                throw th;
            }

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
                if (hasPendingTasks) {
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
                    if (!engine.lockWalWriters(tableToken)) {
                        // There are active WAL writers
                        // potentially opened after the table director is scanned above.
                        LOG.info().$("could not fully remove table, locked by active WAL writers [table=")
                                .$(tableToken).I$();
                        return;
                    }
                    try {
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
                    } finally {
                        // Unlock WAL writers, even if we removed the entry from the pool already
                        engine.unlockWalWriters(tableToken);
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

    /**
     * Discover all sequencer parts on disk for the current table.
     *
     * @return false if there aren't any pending tasks
     */
    private boolean discoverSequencerParts() {
        LPSZ path = setSeqPartPath(tableToken).$();
        boolean hasPendingTasks = false;
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
                                hasPendingTasks = true;
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
        return hasPendingTasks;
    }

    /**
     * Discover all WAL directories and their segments on disk for the current table.
     *
     * @return false if there aren't any pending tasks
     */
    private boolean discoverWalSegments() {
        Path path = setTablePath(tableToken);
        long p = ff.findFirst(path.$());
        int rootPathLen = path.size();
        boolean hasPendingTasks = sequencerHasPendingTasks();
        if (p > 0) {
            try {
                do {
                    int type = ff.findType(p);
                    long pUtf8NameZ = ff.findName(p);

                    if (type == Files.DT_DIR && matchesWalNamePattern(walName.of(pUtf8NameZ))) {
                        try {
                            final int walId = Numbers.parseInt(walName, 3, walName.size());
                            onDiskWalIDSet.add(walId);
                            int maxSegmentLocked = walLocker.lockPurge(tableToken, walId);

                            int trackerIdx = logic.trackDiscoveredWal(walId);
                            boolean walLocked = maxSegmentLocked == WalUtils.SEG_NONE_ID;

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
                                                boolean segmentLocked = segmentId <= maxSegmentLocked;
                                                if (segmentLocked) {
                                                    LOG.debug().$("locked segment [table=").$(tableToken)
                                                            .$(", walId=").$(walId)
                                                            .$(", segmentId=").$(segmentId)
                                                            .I$();

                                                    final boolean pendingTasks = segmentHasPendingTasks(walId, segmentId);
                                                    if (pendingTasks) {
                                                        // Treat is as being locked.
                                                        LOG.debug().$("unlocked segment [table=").$(tableToken)
                                                                .$(", walId=").$(walId)
                                                                .$(", segmentId=").$(segmentId)
                                                                .I$();
                                                        segmentLocked = false;
                                                    }
                                                }
                                                walLocked &= segmentLocked;
                                                logic.trackDiscoveredSegment(segmentId, segmentLocked);
                                            } catch (NumericException ne) {
                                                // Non-Segment directory, ignore.
                                            }
                                        }
                                    } while (ff.findNext(sp) > 0);
                                } finally {
                                    ff.findClose(sp);
                                }
                            }

                            logic.endWalTracking(trackerIdx, maxSegmentLocked, walLocked);
                            hasPendingTasks |= !walLocked;
                        } catch (NumericException ne) {
                            // Non-WAL directory, ignore.
                        }
                    }
                } while (ff.findNext(p) > 0);
            } finally {
                ff.findClose(p);
            }
        }
        return hasPendingTasks;
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
                LOG.debug().$("checking outstanding WAL transactions [table=").$(tableToken)
                        .$(", writerTxn=").$(safeToPurgeTxn)
                        .I$();

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

    private void recursiveDelete(Path path) {
        if (!ff.rmdir(path, false) && !Files.isErrnoFileDoesNotExist(ff.errno())) {
            LOG.debug()
                    .$("could not delete directory [path=").$(path)
                    .$(", errno=").$(ff.errno())
                    .I$();
        }
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
        void deleteSegmentDirectory(int walId, int segmentId);

        void deleteSequencerPart(int seqPart);

        void deleteWalDirectory(int walId);

        void unlock(int walId);
    }

    public static class Logic {
        private final Deleter deleter;
        // discovered stores the WAL and their segments and the sequencer parts
        // [ { walId, maxSegmentLocked, n segments, segmentId... | WalUtils.METADATA_WALID, seqPartNo } [, ...] ]
        private final IntList discovered = new IntList();
        private final IntIntHashMap nextToApply = new IntIntHashMap();
        private final int waitBeforeDelete;
        private long currentSeqPart;
        private boolean logged;
        private TableToken tableToken;

        public Logic(Deleter deleter, int waitBeforeDelete) {
            this.deleter = deleter;
            this.waitBeforeDelete = waitBeforeDelete;
        }

        public void endWalTracking(int idx, int maxSegmentLocked, boolean isLocked) {
            final int nSegments = (discovered.size() - idx - 2);
            discovered.setQuick(idx, isLocked ? maxSegmentLocked : -maxSegmentLocked - 1);
            discovered.setQuick(idx + 1, nSegments);
        }

        public boolean hasOnDiskSegments() {
            return discovered.size() != 0;
        }

        public void releaseLocks() {
            for (int i = 0, n = discovered.size(); i < n; ) {
                final int walId = discovered.get(i);
                if (walId != WalUtils.METADATA_WALID) {
                    // We've a valid WAL entry, unlock it
                    deleter.unlock(walId);
                    i += 3 + discovered.get(i + 2);
                } else {
                    // If walId is METADATA_WALID, it's a sequencer part, skip it
                    i += 2;
                }
            }
        }

        public void reset(TableToken tableToken) {
            this.tableToken = tableToken;
            nextToApply.clear();
            discovered.clear();
            currentSeqPart = -1;
            logged = false;
        }

        public void run() {
            int i = 0, n = discovered.size();
            if (n > 0 && waitBeforeDelete > 0) {
                Os.sleep(waitBeforeDelete);
            }

            try {
                while (i < n) {
                    final int walId = discovered.get(i);
                    final int maxSegmentLocked = discovered.get(i + 1);

                    final int seqPart = getSeqPart(walId, maxSegmentLocked); // -1 if not a seq part
                    if (seqPart > -1) {
                        if (seqPart < currentSeqPart) {
                            logDebugInfo();
                            deleter.deleteSequencerPart(seqPart);
                        }
                        // Move to next discovered entry, sequencer parts are composed of 2 ints (walId, seqPart)
                        i += 2;
                        continue;
                    }

                    final int nSegments = discovered.get(i + 2);
                    final int nextToApplySegmentId = nextToApply.get(walId);  // -1 if not found

                    if (maxSegmentLocked == WalUtils.SEG_NONE_ID && nextToApplySegmentId == -1) {
                        // No locked segments and no pending segments to apply, delete whole wal directory
                        logDebugInfo();
                        deleter.deleteWalDirectory(walId);
                    } else {
                        // Check segments individually
                        for (int s = 0; s < nSegments; s++) {
                            final int segmentId = discovered.get(i + 3 + s);
                            if (segmentId > -1) {
                                final boolean segmentAlreadyApplied = (nextToApplySegmentId == -1) || (nextToApplySegmentId > segmentId);
                                if (segmentAlreadyApplied) {
                                    logDebugInfo();
                                    deleter.deleteSegmentDirectory(walId, segmentId);
                                }
                            }
                        }
                    }
                    deleter.unlock(walId);
                    // Move to next discovered entry, wal entries are composed of 3 + nSegments ints:
                    // (walId, maxSegmentLocked, nSegments, segmentId...)
                    i += 3 + nSegments;
                }
            } finally {
                while (i < n) {
                    final int walId = discovered.get(i);
                    if (walId != WalUtils.METADATA_WALID) {
                        // We've a valid WAL entry, unlock it
                        deleter.unlock(walId);
                        i += 3 + discovered.get(i + 2);
                    } else {
                        // If walId is METADATA_WALID, it's a sequencer part, skip it
                        i += 2;
                    }
                }
            }
        }

        public void trackCurrentSeqPart(long partNo) {
            currentSeqPart = partNo;
        }

        public void trackDiscoveredSegment(int segmentId, boolean locked) {
            discovered.add(locked ? segmentId : -segmentId - 1);
        }

        public int trackDiscoveredWal(int walId) {
            discovered.add(walId);
            discovered.add(0); // placeholder for max segment locked
            discovered.add(0); // placeholder for number of segments discovered
            return discovered.size() - 2;
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
        }

        private int getSeqPart(int walId, int segmentId) {
            return walId == WalUtils.METADATA_WALID ? segmentId : -1;
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

                for (int i = 0, n = discovered.size(); i < n; ) {
                    final int walId = discovered.get(i);
                    final int maxSegmentLocked = discovered.get(i + 1);

                    final int partNo = getSeqPart(walId, maxSegmentLocked);
                    if (partNo > -1) {
                        log.$("seqPart=").$(partNo);
                        if (partNo == currentSeqPart) {
                            log.$(":current");
                        }
                        i += 2;
                        continue;
                    }

                    final int nSegments = discovered.get(i + 2);

                    log.$("(wal").$(walId);
                    if (maxSegmentLocked != WalUtils.SEG_NONE_ID) {
                        log.$(":busy");
                    }
                    log.$(')');

                    final int nextToApplyId = nextToApply.get(walId);
                    for (int s = 0; s < nSegments; s++) {
                        log.$(',');
                        final int segment = discovered.get(i + 3 + s);
                        final int segmentId = segment >= 0 ? segment : -segment - 1;
                        final boolean locked = segment >= 0;
                        log.$('(').$(walId).$(',').$(segmentId);
                        if (segmentId == nextToApplyId) {
                            log.$(":next");
                        }
                        if (!locked) {
                            log.$(":busy");
                        }
                        log.$(')');
                    }

                    i += 3 + nSegments;
                }
            } finally {
                log.I$();
            }
        }
    }

    private class FsDeleter implements Deleter {

        @Override
        public void deleteSegmentDirectory(int walId, int segmentId) {
            LOG.debug().$("deleting WAL segment directory [table=").$(tableToken)
                    .$(", walId=").$(walId)
                    .$(", segmentId=").$(segmentId)
                    .I$();
            recursiveDelete(setSegmentPath(tableToken, walId, segmentId));
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
        public void deleteWalDirectory(int walId) {
            LOG.debug().$("deleting WAL directory [table=").$(tableToken)
                    .$(", walId=").$(walId)
                    .I$();
            recursiveDelete(setWalPath(tableToken, walId));
        }

        @Override
        public void unlock(int walId) {
            walLocker.unlockPurge(tableToken, walId);
            LOG.debug().$("unlocked WAL [table=").$(tableToken)
                    .$(", walId=").$(walId)
                    .I$();
        }
    }
}
