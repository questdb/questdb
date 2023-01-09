/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2022 QuestDB
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
import io.questdb.std.str.NativeLPSZ;
import io.questdb.std.str.Path;
import io.questdb.std.str.StringSink;

import java.io.Closeable;
import java.util.PrimitiveIterator;

public class WalPurgeJob extends SynchronizedJob implements Closeable {
    private static final Log LOG = LogFactory.getLog(WalPurgeJob.class);
    private final TableSequencerAPI.RegisteredTable broadSweepIter;
    private final long checkInterval;
    private final MicrosecondClock clock;
    private final CairoConfiguration configuration;
    private final StringSink debugBuffer = new StringSink();
    private final IntHashSet discoveredWalIds = new IntHashSet();
    private final CairoEngine engine;
    private final FilesFacade ff;
    private final MillisecondClock millisecondClock;
    private final Path path = new Path();
    private final SimpleWaitingLock runLock = new SimpleWaitingLock();
    private final NativeLPSZ segmentName = new NativeLPSZ();
    private final long spinLockTimeout;
    private final ObjList<TableToken> tableTokenBucket = new ObjList<>();
    private final TxReader txReader;
    private final WalInfoDataFrame walInfoDataFrame = new WalInfoDataFrame();
    private final NativeLPSZ walName = new NativeLPSZ();
    private final IntHashSet walsInUse = new IntHashSet();
    private long last = 0;
    private TableToken tableToken;
    private final FindVisitor discoverWalDirectoriesIterFunc = this::discoverWalDirectoriesIter;
    private int walId;
    private final FindVisitor deleteClosedSegmentsIterFunc = this::deleteClosedSegmentsIter;
    private int walsLatestSegmentId;
    private final FindVisitor deleteUnreachableSegmentsIterFunc = this::deleteUnreachableSegmentsIter;

    public WalPurgeJob(CairoEngine engine, FilesFacade ff, MicrosecondClock clock) {
        this.engine = engine;
        this.ff = ff;
        this.clock = clock;
        this.checkInterval = engine.getConfiguration().getWalPurgeInterval() * 1000;
        this.millisecondClock = engine.getConfiguration().getMillisecondClock();
        this.spinLockTimeout = engine.getConfiguration().getSpinLockTimeout();
        this.txReader = new TxReader(ff);
        this.broadSweepIter = this::broadSweep;

        // some code here assumes that WAL_NAME_BASE is "wal", this is to fail the tests if it is not
        //noinspection ConstantConditions
        assert WalUtils.WAL_NAME_BASE.equals("wal");
        configuration = engine.getConfiguration();
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
    private static boolean matchesSegmentName(CharSequence name) {
        for (int i = 0, n = name.length(); i < n; i++) {
            char c = name.charAt(i);
            if (c < '0' || c > '9') {
                return false;
            }
        }
        return true;
    }

    /**
     * Validate equivalent of "^wal\d+$" regex.
     */
    private static boolean matchesWalNamePattern(CharSequence name) {
        final int len = name.length();
        if (len < (WalUtils.WAL_NAME_BASE.length() + 1)) {
            return false;
        }

        if (name.charAt(0) != 'w' || name.charAt(1) != 'a' || name.charAt(2) != 'l') {
            return false;  // Not a "wal" prefix.
        }

        for (int i = 3; i < len; ++i) {
            final char c = name.charAt(i);
            if (c < '0' || c > '9') {
                return false;  // Not a number.
            }
        }

        return true;
    }

    private void accumDebugState() {
        debugBuffer.clear();
        debugBuffer.put("table=").put(tableToken.getDirName())
                .put(", discoveredWalIds=[");
        for (PrimitiveIterator.OfInt it = discoveredWalIds.iterator(); it.hasNext(); ) {
            final int walId = it.nextInt();
            debugBuffer.put(walId);
            if (walsInUse.contains(walId)) {
                debugBuffer.put("(locked)");
            }
            if (it.hasNext()) {
                debugBuffer.put(',');
            }
        }
        debugBuffer.put("], walInfoDataFrame=[");
        for (int index = 0; index < walInfoDataFrame.size(); ++index) {
            debugBuffer.put('(').put(walInfoDataFrame.walIds.get(index)).put(',')
                    .put(walInfoDataFrame.segmentIds.get(index)).put(')');
            if (index < walInfoDataFrame.size() - 1) {
                debugBuffer.put(',');
            }
        }
        debugBuffer.put(']');
    }

    /**
     * Perform a broad sweep that searches for all tables that have closed
     * WAL segments across the database and deletes any which are no longer needed.
     */
    private void broadSweep() {
        engine.getTableSequencerAPI().forAllWalTables(tableTokenBucket, true, broadSweepIter);
    }

    private void broadSweep(int tableId, final TableToken tableToken, long lastTxn) {
        try {
            this.tableToken = tableToken;
            discoveredWalIds.clear();
            walsInUse.clear();
            walInfoDataFrame.clear();

            discoverWalDirectories();
            if (discoveredWalIds.size() != 0) {

                populateWalInfoDataFrame();
                accumDebugState();

                deleteUnreachableSegments();

                // Any of the calls above may leave outstanding `discoveredWalIds` that are still on the filesystem
                // and don't have any active segments. Any unlocked walNNN directories may be deleted if they don't have
                // pending segments that are yet to be applied to the table.
                // Note that this also handles cases where a wal directory was created shortly before a crash and thus
                // never recorded and tracked by the sequencer for that table.
                deleteOutstandingWalDirectories();
            }

            if (lastTxn < 0 && engine.isTableDropped(tableToken)) {
                // Delete sequencer files
                deleteTableSequencerFiles(tableToken);

                if (
                        TableUtils.exists(
                                ff,
                                Path.getThreadLocal(""),
                                configuration.getRoot(),
                                tableToken.getDirName()
                        ) != TableUtils.TABLE_EXISTS
                ) {
                    // Fully deregister the table
                    LOG.info().$("table is fully dropped [tableDir=").$(tableToken.getDirName()).I$();
                    engine.removeTableToken(tableToken);
                } else {
                    LOG.info().$("table is not fully dropped, pinging WAL Apply job to delete table files [tableDir=").$(tableToken.getDirName()).I$();
                    // Ping ApplyWal2TableJob to clean up the table files
                    engine.notifyWalTxnRepublisher();
                }
            }
        } catch (CairoException ce) {
            LOG.error().$("broad sweep failed [table=").$(tableToken)
                    .$(", msg=").$((Throwable) ce)
                    .$(", errno=").$(ff.errno()).$(']').$();
        }
    }

    private boolean couldObtainLock(Path path) {
        final int lockFd = TableUtils.lock(ff, path, false);
        if (lockFd != -1) {
            ff.close(lockFd);
            return true; // Could lock/unlock.
        }
        return false; // Could not obtain lock.
    }

    private void deleteClosedSegments() {
        setWalPath(tableToken, walId);
        ff.iterateDir(path, deleteClosedSegmentsIterFunc);
    }

    private void deleteClosedSegmentsIter(long pUtf8NameZ, int type) {
        if ((type == Files.DT_DIR) && matchesSegmentName(segmentName.of(pUtf8NameZ))) {
            int segmentId;
            try {
                segmentId = Numbers.parseInt(segmentName);
            } catch (NumericException _ne) {
                return; // Ignore non-segment directory.
            }
            if (couldObtainLock(setSegmentLockPath(tableToken, walId, segmentId))) {
                deleteSegmentDirectory(tableToken, walId, segmentId);
            }
        }
    }

    private boolean deleteFile(Path path) {
        if (!ff.remove(path)) {
            final int errno = ff.errno();
            if (errno != 2) {
                LOG.error().$("Could not delete file [path=").$(path)
                        .$(", errno=").$(errno).$(']').$();
                return false;
            }
        }
        return true;
    }

    private void deleteOutstandingWalDirectories() {
        for (PrimitiveIterator.OfInt it = discoveredWalIds.iterator(); it.hasNext(); ) {
            walId = it.nextInt();
            if (walsInUse.contains(walId)) {
                deleteClosedSegments();
            } else {
                deleteWalDirectory();
            }
        }
    }

    private void deleteSegmentDirectory(TableToken tableName, int walId, int segmentId) {
        mayLogDebugInfo();
        LOG.info().$("deleting WAL segment directory [table=").utf8(tableName.getDirName())
                .$(", walId=").$(walId)
                .$(", segmentId=").$(segmentId).$(']').$();
        if (deleteFile(setSegmentLockPath(tableName, walId, segmentId))) {
            recursiveDelete(setSegmentPath(tableName, walId, segmentId));
        }
    }

    private void deleteTableSequencerFiles(TableToken tableToken) {
        setTableSequencerPath(tableToken);
        LOG.info().$("table is dropped, deleting sequencer files [table=").utf8(tableToken.getDirName()).$(']').$();
        recursiveDelete(path);
    }

    private void deleteUnreachableSegments() {
        for (int index = 0; index < walInfoDataFrame.size(); ++index) {
            walId = walInfoDataFrame.walIds.get(index);
            walsLatestSegmentId = walInfoDataFrame.segmentIds.get(index);
            ff.iterateDir(setWalPath(tableToken, walId), deleteUnreachableSegmentsIterFunc);
        }
    }

    private void deleteUnreachableSegmentsIter(long pUtf8NameZ, int type) {
        if ((type == Files.DT_DIR) && matchesSegmentName(segmentName.of(pUtf8NameZ))) {
            int segmentId;
            try {
                segmentId = Numbers.parseInt(segmentName);
            } catch (NumericException _ne) {
                return; // Ignore non-segment directory.
            }
            if (segmentIsReapable(segmentId, walsLatestSegmentId)) {
                deleteSegmentDirectory(tableToken, walId, segmentId);
            }
        }
    }

    private void deleteWalDirectory() {
        mayLogDebugInfo();
        LOG.info().$("deleting WAL directory [table=").utf8(tableToken.getDirName())
                .$(", walId=").$(walId).$(']').$();
        if (deleteFile(setWalLockPath(tableToken, walId))) {
            recursiveDelete(setWalPath(tableToken, walId));
        }
    }

    private void discoverWalDirectories() {
        ff.iterateDir(setTablePath(tableToken), discoverWalDirectoriesIterFunc);
    }

    private void discoverWalDirectoriesIter(long pUtf8NameZ, int type) {
        if ((type == Files.DT_DIR) && matchesWalNamePattern(walName.of(pUtf8NameZ))) {
            // We just record the name for now in a set which we'll remove items to know when we're done.
            int walId;
            try {
                walId = Numbers.parseInt(walName, 3, walName.length());
            } catch (NumericException ne) {
                return;  // Ignore non-wal directory
            }
            discoveredWalIds.add(walId);
            if (walIsInUse(tableToken, walId)) {
                walsInUse.add(walId);
            }
        }
    }

    private void mayLogDebugInfo() {
        if (debugBuffer.length() > 0) {
            LOG.info().utf8(debugBuffer).$();
            debugBuffer.clear();
        }
    }

    private void populateWalInfoDataFrame() {
        setTxnPath(tableToken);
        if (!engine.isTableDropped(tableToken)) {
            try {
                txReader.ofRO(path, PartitionBy.NONE);
                TableUtils.safeReadTxn(txReader, millisecondClock, spinLockTimeout);
                final long lastAppliedTxn = txReader.getSeqTxn();

                TableSequencerAPI tableSequencerAPI = engine.getTableSequencerAPI();
                try (TransactionLogCursor transactionLogCursor = tableSequencerAPI.getCursor(tableToken, lastAppliedTxn)) {
                    while (transactionLogCursor.hasNext() && (discoveredWalIds.size() > 0)) {
                        final int walId = transactionLogCursor.getWalId();
                        if (discoveredWalIds.contains(walId)) {
                            walInfoDataFrame.add(walId, transactionLogCursor.getSegmentId());
                            discoveredWalIds.remove(walId);
                        }
                    }
                }
            } finally {
                txReader.close();
            }
        } else {
            // Table is dropped, all wals can be deleted.
            for (PrimitiveIterator.OfInt it = discoveredWalIds.iterator(); it.hasNext(); ) {
                walInfoDataFrame.add(it.nextInt(), Integer.MAX_VALUE);
            }
        }
    }

    private void recursiveDelete(Path path) {
        final int errno = ff.rmdir(path);
        if (errno > 0 && !CairoException.errnoRemovePathDoesNotExist(errno)) {
            LOG.error().$("could not delete directory [path=").utf8(path)
                    .$(", errno=").$(errno).$(']').$();
        }
    }

    private boolean segmentIsReapable(int segmentId, int walsLatestSegmentId) {
        return segmentId < walsLatestSegmentId;
    }

    private Path setSegmentLockPath(TableToken tableName, int walId, int segmentId) {
        path.of(configuration.getRoot())
                .concat(tableName).concat(WalUtils.WAL_NAME_BASE).put(walId).slash().put(segmentId);
        TableUtils.lockName(path);
        return path;
    }

    private Path setSegmentPath(TableToken tableName, int walId, int segmentId) {
        return path.of(configuration.getRoot())
                .concat(tableName).concat(WalUtils.WAL_NAME_BASE).put(walId).slash().put(segmentId).$();
    }

    private Path setTablePath(TableToken tableName) {
        return path.of(configuration.getRoot())
                .concat(tableName).$();
    }

    private void setTableSequencerPath(TableToken tableName) {
        path.of(configuration.getRoot())
                .concat(tableName).concat(WalUtils.SEQ_DIR).$();
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

    private boolean walIsInUse(TableToken tableName, int walId) {
        return !couldObtainLock(setWalLockPath(tableName, walId));
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

    /**
     * Table of columns grouping segment information. One row per walId.
     */
    private static class WalInfoDataFrame {
        public final IntList segmentIds = new IntList();
        public final IntList walIds = new IntList();

        public void add(int walId, int segmentId) {
            walIds.add(walId);
            segmentIds.add(segmentId);
        }

        public void clear() {
            walIds.clear();
            segmentIds.clear();
        }

        public int size() {
            return walIds.size();
        }
    }
}
