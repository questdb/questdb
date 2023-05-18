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
import io.questdb.std.str.NativeLPSZ;
import io.questdb.std.str.Path;
import io.questdb.std.str.StringSink;

import java.io.Closeable;

public class WalPurgeJob extends SynchronizedJob implements Closeable {
    private static final Log LOG = LogFactory.getLog(WalPurgeJob.class);
    private final TableSequencerAPI.TableSequencerCallback broadSweepRef;
    private final long checkInterval;
    private final MicrosecondClock clock;
    private final CairoConfiguration configuration;
    private final StringSink debugBuffer = new StringSink();
    private final CairoEngine engine;
    private final FilesFacade ff;
    private final NativeLPSZ fileName = new NativeLPSZ();
    private final IntHashSet lockedWalIDSet = new IntHashSet();
    private final MillisecondClock millisecondClock;
    private final LongList onDiskWalIDSegmentIDPairs = new LongList();
    private final IntHashSet onDiskWalIDSet = new IntHashSet();
    private final Path path = new Path();
    private final IntHashSet pendingWalIDSet = new IntHashSet();
    private final SimpleWaitingLock runLock = new SimpleWaitingLock();
    private final LongList sequencerWalIDSegmentIDPairs = new LongList();
    private final long spinLockTimeout;
    private final ObjHashSet<TableToken> tableTokenBucket = new ObjHashSet<>();
    private final TxReader txReader;
    private final NativeLPSZ walName = new NativeLPSZ();
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

    private static boolean decodeLocked(int segmentKey) {
        return (segmentKey & 1) == 1;
    }

    private static boolean decodePendingTasks(int segmentKey) {
        return (segmentKey & 2) == 2;
    }

    private static int decodeSegmentId(int segmentKey) {
        return segmentKey >> 2;
    }

    /**
     * Return encoded segment key for no segment.
     */
    private static int encodeNoSegment() {
        return encodeSegmentKey(WalUtils.SEG_NONE_ID, false, false);
    }

    /**
     * Return new encoded segment key.
     */
    private static int encodeSegmentKey(int segmentId, boolean pendingTasks, boolean locked) {
        return (segmentId << 2) + (pendingTasks ? 2 : 0) + (locked ? 1 : 0);
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
                .put(", discovered=[");

        for (int i = 0, n = onDiskWalIDSegmentIDPairs.size(); i < n; i++) {
            final long walSegment = onDiskWalIDSegmentIDPairs.getQuick(i);
            final int segmentKey = Numbers.decodeLowInt(walSegment);
            final int walId = Numbers.decodeHighInt(walSegment);
            final int segmentId = decodeSegmentId(segmentKey);
            final boolean pendingTasks = decodePendingTasks(segmentKey);
            final boolean locked = decodeLocked(segmentKey);
            if (segmentId <= WalUtils.SEG_MAX_ID) {
                debugBuffer.put('(').put(walId).put(',').put(segmentId);
                if (pendingTasks) {
                    debugBuffer.put(":tasks");
                }
                if (locked) {
                    debugBuffer.put(":locked");
                }
                debugBuffer.put(')');
            } else {
                debugBuffer.put("(wal").put(walId);
                if (lockedWalIDSet.contains(walId)) {
                    debugBuffer.put(":locked");
                }
                if (pendingWalIDSet.contains(walId)) {
                    debugBuffer.put(":tasks");
                }
                debugBuffer.put(')');
            }

            if (i < n - 1) {
                debugBuffer.put(',');
            }
        }

        debugBuffer.put("], nextToApply=[");
        for (int i = 0, n = sequencerWalIDSegmentIDPairs.size(); i < n; i++) {
            long walSegment = sequencerWalIDSegmentIDPairs.getQuick(i);
            int segmentId = Numbers.decodeLowInt(walSegment);
            int walId = Numbers.decodeHighInt(walSegment);

            debugBuffer.put('(').put(walId).put(',').put(segmentId).put(')');
            if (i < n - 1) {
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
        engine.getTableSequencerAPI().forAllWalTables(tableTokenBucket, true, broadSweepRef);
    }

    private void broadSweep(int tableId, final TableToken tableToken, long lastTxn) {
        try {
            this.tableToken = tableToken;
            onDiskWalIDSegmentIDPairs.clear();
            lockedWalIDSet.clear();
            onDiskWalIDSet.clear();
            pendingWalIDSet.clear();
            sequencerWalIDSegmentIDPairs.clear();

            boolean tableDropped = false;
            discoverWalSegments();
            if (onDiskWalIDSegmentIDPairs.size() != 0) {

                tableDropped = fetchSequencerPairs();
                accumDebugState();

                // Any of the calls above may leave outstanding `discoveredWalIds` that are still on the filesystem
                // and don't have any active segments. Any unlocked walNNN directories may be deleted if they don't have
                // pending segments that are yet to be applied to the table.
                // Note that this also handles cases where a wal directory was created shortly before a crash and thus
                // never recorded and tracked by the sequencer for that table.
                deleteOutstandingWalDirectories();
            }

            if (tableDropped || (lastTxn < 0 && engine.isTableDropped(tableToken))) {
                if (pendingWalIDSet.size() > 0) {
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
                    ff.rmdir(pathToDelete);
                    if (symLinkTarget != null) {
                        ff.rmdir(symLinkTarget);
                    }
                    TableUtils.lockName(pathToDelete);
                    ff.remove(pathToDelete);
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
        // Merge join discoveredWalSegments and nextToApplyWalSegments
        // Both are sorted by WalId and then SegmentId
        // Delete those segments which are lower than first committed segment
        // Keep locked segments and all segments higher than first locked segment
        int committedWalSegmentIndex = -1;
        int committedSegmentId = -1;
        int committedWalId = -1;
        int committedSegmentSize = sequencerWalIDSegmentIDPairs.size();

        for (int i = 0, n = onDiskWalIDSegmentIDPairs.size(); i < n; i++) {
            final long diskPair = onDiskWalIDSegmentIDPairs.get(i);
            final int walId = Numbers.decodeHighInt(diskPair);
            final int segmentKey = Numbers.decodeLowInt(diskPair);
            final int segmentId = decodeSegmentId(segmentKey);
            final boolean hasPendingTasks = decodePendingTasks(segmentKey);
            final boolean segmentLocked = decodeLocked(segmentKey);

            if (hasPendingTasks) {
                continue;
            }

            // If the current segment is locked, scroll to next WAL id.
            final int walIdLimit = segmentLocked ? walId + 1 : walId;
            while (committedWalId < walIdLimit && ++committedWalSegmentIndex < committedSegmentSize) {
                long sequencerPair = sequencerWalIDSegmentIDPairs.get(committedWalSegmentIndex);
                committedWalId = Numbers.decodeHighInt(sequencerPair);
                committedSegmentId = Numbers.decodeLowInt(sequencerPair);
            }

            if (walId == committedWalId) {
                // WAL is in use, there are committed transactions in Sequencer
                if (committedSegmentId > segmentId) {
                    // Segment already applied.
                    deleteSegmentDirectory(tableToken, walId, segmentId);
                }
            } else if (!segmentLocked) {
                // If the sequencer doesn't track any unapplied changes for this walId and the walId is unlocked,
                // then we can delete the whole WAL directory, unless any of its segments track pending tasks.
                if (!lockedWalIDSet.contains(walId) && !pendingWalIDSet.contains(walId)) {
                    deleteWalDirectory(walId);
                } else {
                    // WAL is locked but the segment in it is not in outstanding commits, delete it.
                    // Segment with ID of WalUtils.SEG_NONE_ID is the WAL directory itself: only delete it if it's unlocked.
                    if (segmentId != WalUtils.SEG_NONE_ID) {
                        deleteSegmentDirectory(tableToken, walId, segmentId);
                    }
                }
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

    private void deleteWalDirectory(int walId) {
        mayLogDebugInfo();
        LOG.info().$("deleting WAL directory [table=").utf8(tableToken.getDirName())
                .$(", walId=").$(walId).$(']').$();
        if (deleteFile(setWalLockPath(tableToken, walId))) {
            recursiveDelete(setWalPath(tableToken, walId));
        }
    }

    private void discoverWalSegments() {
        Path path = setTablePath(tableToken);
        long p = ff.findFirst(path);
        int rootPathLen = path.length();
        if (p > 0) {
            try {
                do {
                    int type = ff.findType(p);
                    long pUtf8NameZ = ff.findName(p);

                    if (type == Files.DT_DIR && matchesWalNamePattern(walName.of(pUtf8NameZ))) {
                        // We just record the name for now in a set which we'll remove items to know when we're done.
                        try {
                            int walId = Numbers.parseInt(walName, 3, walName.length());
                            if (walIsInUse(tableToken, walId)) {
                                lockedWalIDSet.add(walId);
                            }
                            onDiskWalIDSet.add(walId);
                            // This is to track WAL directory itself.
                            onDiskWalIDSegmentIDPairs.add(Numbers.encodeLowHighInts(encodeNoSegment(), walId));

                            // Search for segments.
                            path.trimTo(rootPathLen).concat(pUtf8NameZ);
                            int walPathLen = path.length();
                            long sp = ff.findFirst(path.$());

                            try {
                                do {
                                    type = ff.findType(sp);
                                    pUtf8NameZ = ff.findName(sp);

                                    if (type == Files.DT_DIR && matchesSegmentName(walName.of(pUtf8NameZ))) {
                                        try {
                                            int segmentId = Numbers.parseInt(walName);
                                            if ((segmentId < WalUtils.SEG_MIN_ID) || (segmentId > WalUtils.SEG_MAX_ID)) {
                                                throw NumericException.INSTANCE;
                                            }
                                            Path segmentPath = path.trimTo(walPathLen).slash().put(segmentId);
                                            TableUtils.lockName(segmentPath);
                                            boolean locked = !unlocked(segmentPath.$());
                                            boolean pendingTasks = segmentHasPendingTasks(walId, segmentId);
                                            if (pendingTasks) {
                                                pendingWalIDSet.add(walId);
                                            }
                                            final int segmentKey = encodeSegmentKey(segmentId, pendingTasks, locked);
                                            onDiskWalIDSegmentIDPairs.add(Numbers.encodeLowHighInts(segmentKey, walId));
                                        } catch (NumericException ne) {
                                            // Non-Segment directory, ignore.
                                        }
                                    }
                                } while (ff.findNext(sp) > 0);
                            } finally {
                                ff.findClose(sp);
                            }
                        } catch (NumericException ne) {
                            // Non-WAL directory, ignore.
                        }
                    }
                } while (ff.findNext(p) > 0);
                onDiskWalIDSegmentIDPairs.sort();
            } finally {
                ff.findClose(p);
            }
        }
    }

    private boolean fetchSequencerPairs() {
        setTxnPath(tableToken);
        if (!engine.isTableDropped(tableToken)) {
            try {
                txReader.ofRO(path, PartitionBy.NONE);
                TableUtils.safeReadTxn(txReader, millisecondClock, spinLockTimeout);
                final long lastAppliedTxn = txReader.getSeqTxn();

                TableSequencerAPI tableSequencerAPI = engine.getTableSequencerAPI();
                try (TransactionLogCursor transactionLogCursor = tableSequencerAPI.getCursor(tableToken, lastAppliedTxn)) {
                    while (onDiskWalIDSet.size() > 0 && transactionLogCursor.hasNext()) {
                        int walId = transactionLogCursor.getWalId();
                        if (onDiskWalIDSet.remove(walId) != -1) {
                            int segmentId = transactionLogCursor.getSegmentId();
                            sequencerWalIDSegmentIDPairs.add(Numbers.encodeLowHighInts(segmentId, walId));
                        }
                    }
                    sequencerWalIDSegmentIDPairs.sort();
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

    private void mayLogDebugInfo() {
        if (debugBuffer.length() > 0) {
            LOG.info().utf8(debugBuffer).$();
            debugBuffer.clear();
        }
    }

    private void recursiveDelete(Path path) {
        final int errno = ff.rmdir(path);
        if (errno > 0 && !CairoException.errnoRemovePathDoesNotExist(errno)) {
            LOG.error().$("could not delete directory [path=").utf8(path)
                    .$(", errno=").$(errno).$(']').$();
        }
    }

    /**
     * Check if the segment directory has any outstanding ".pending" marker files in a ".pending" directory.
     */
    private boolean segmentHasPendingTasks(int walId, int segmentId) {
        final Path pendingPath = setSegmentPendingPath(tableToken, walId, segmentId);
        final long p = ff.findFirst(pendingPath);
        if (p > 0) {
            try {
                do {
                    final int type = ff.findType(p);
                    final long pUtf8NameZ = ff.findName(p);
                    fileName.of(pUtf8NameZ);
                    if ((type == Files.DT_FILE) && Chars.endsWith(fileName, ".pending")) {
                        return true;
                    }
                } while (ff.findNext(p) > 0);
            } finally {
                ff.findClose(p);
            }
        }
        return false;
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

    private Path setSegmentPendingPath(TableToken tableName, int walId, int segmentId) {
        return path.of(configuration.getRoot())
                .concat(tableName).concat(WalUtils.WAL_NAME_BASE).put(walId).slash().put(segmentId).concat(".pending").slash$();
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

    private boolean unlocked(Path path) {
        final int lockFd = TableUtils.lock(ff, path, false);
        if (lockFd != -1) {
            ff.close(lockFd);
            return true; // Could lock/unlock.
        }
        return false; // Could not obtain lock.
    }

    private boolean walIsInUse(TableToken tableName, int walId) {
        return !unlocked(setWalLockPath(tableName, walId));
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
}
