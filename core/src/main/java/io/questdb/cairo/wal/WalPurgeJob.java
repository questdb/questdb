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
    private final CairoEngine engine;
    private final FilesFacade ff;
    private final NativeLPSZ fileName = new NativeLPSZ();
    private final Logic logic;
    private final MillisecondClock millisecondClock;
    private final IntHashSet onDiskWalIDSet = new IntHashSet();
    private final Path path = new Path();
    private final SimpleWaitingLock runLock = new SimpleWaitingLock();
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

        logic = new Logic(new FsDeleter());
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

                tableDropped = fetchSequencerPairs();

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
                    engine.notifyWalTxnCommitted(tableToken);
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
                        try {
                            final int walId = Numbers.parseInt(walName, 3, walName.length());
                            onDiskWalIDSet.add(walId);
                            final boolean walInUse = walIsInUse(tableToken, walId);
                            boolean walHasPendingTasks = false;

                            // Search for segments.
                            path.trimTo(rootPathLen).concat(pUtf8NameZ);
                            final int walPathLen = path.length();
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
                                            final Path segmentPath = path.trimTo(walPathLen).slash().put(segmentId);
                                            TableUtils.lockName(segmentPath);
                                            final boolean locked = !unlocked(segmentPath.$());
                                            final boolean pendingTasks = segmentHasPendingTasks(walId, segmentId);
                                            if (pendingTasks) {
                                                walHasPendingTasks = true;
                                            }
                                            logic.trackDiscoveredSegment(walId, segmentId, pendingTasks, locked);
                                        } catch (NumericException ne) {
                                            // Non-Segment directory, ignore.
                                        }
                                    }
                                } while (ff.findNext(sp) > 0);
                            } finally {
                                ff.findClose(sp);
                            }
                            logic.trackDiscoveredWal(walId, walHasPendingTasks, walInUse);
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
                txReader.ofRO(path, PartitionBy.NONE);
                TableUtils.safeReadTxn(txReader, millisecondClock, spinLockTimeout);
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

    public interface Deleter {
        void deleteSegmentDirectory(int walId, int segmentId);

        void deleteWalDirectory(int walId);
    }

    public static class Logic {
        private final StringSink debugBuffer = new StringSink();
        private final Deleter deleter;
        private final LongList discovered = new LongList();
        private final IntIntHashMap nextToApply = new IntIntHashMap();
        private final LongList nextToApplyKeys = new LongList();  // LongList rather than IntList because need .sort().
        private TableToken tableToken;

        public Logic(Deleter deleter) {
            this.deleter = deleter;
        }

        public void accumDebugState() {
            debugBuffer.clear();
            debugBuffer.put("table=").put(tableToken.getDirName())
                    .put(", discovered=[");

            for (int i = 0, n = discovered.size(); i < n; i++) {
                final long encoded = discovered.getQuick(i);
                final int walId = decodeWalId(encoded);
                final int segmentId = decodeSegmentId(encoded);
                final boolean pendingTasks = decodePendingTasks(encoded);
                final boolean locked = decodeLocked(encoded);
                if (isWalDir(segmentId)) {
                    debugBuffer.put("(wal").put(walId);
                } else {
                    debugBuffer.put('(').put(walId).put(',').put(segmentId);
                }

                if (pendingTasks) {
                    debugBuffer.put(":tasks");
                }
                if (locked) {
                    debugBuffer.put(":locked");
                }
                debugBuffer.put(')');

                if (i < n - 1) {
                    debugBuffer.put(',');
                }
            }

            debugBuffer.put("], nextToApply=[");
            for (int i = 0, n = nextToApplyKeys.size(); i < n; ++i) {
                final int walId = (int) nextToApplyKeys.getQuick(i);
                final int segmentId = nextToApply.get(walId);
                debugBuffer.put('(').put(walId).put(',').put(segmentId).put(')');
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
            for (int i = 0; i < discovered.size(); ++i) {
                if (decodePendingTasks(discovered.get(i))) {
                    return true;
                }
            }
            return false;
        }

        public void reset(TableToken tableToken) {
            this.tableToken = tableToken;
            nextToApply.clear();
            nextToApplyKeys.clear();
            discovered.clear();
        }

        public void run() {
            sortTracked();
            accumDebugState();

            for (int i = 0, n = discovered.size(); i < n; i++) {
                final long encoded = discovered.get(i);
                final int walId = decodeWalId(encoded);
                final int segmentId = decodeSegmentId(encoded);
                final boolean hasPendingTasks = decodePendingTasks(encoded);
                final boolean isLocked = decodeLocked(encoded);
                final int nextToApplySegmentId = nextToApply.get(walId);  // -1 if not found

                // Delete a wal or segment directory only if:
                //   * It has been fully applied to the table.
                //   * Is not locked.
                //   * None of its segments have pending tasks
                if (isWalDir(segmentId)) {
                    final boolean walAlreadyApplied = nextToApplySegmentId == -1;
                    if (walAlreadyApplied && !isLocked && !hasPendingTasks) {
                        logDebugInfo();
                        deleter.deleteWalDirectory(walId);
                    }
                } else {
                    final boolean segmentAlreadyApplied = (nextToApplySegmentId == -1) || (nextToApplySegmentId > segmentId);
                    if (segmentAlreadyApplied && !isLocked && !hasPendingTasks) {
                        logDebugInfo();
                        deleter.deleteSegmentDirectory(walId, segmentId);
                    }
                }
            }
        }

        public void trackDiscoveredSegment(int walId, int segmentId, boolean pendingTasks, boolean locked) {
            discovered.add(encodeDiscovered(walId, segmentId, pendingTasks, locked));
        }

        public void trackDiscoveredWal(int walId, boolean pendingTasks, boolean locked) {
            discovered.add(encodeDiscovered(walId, WalUtils.SEG_NONE_ID, pendingTasks, locked));
        }

        public void trackNextToApplySegment(int walId, int segmentId) {
            final int index = nextToApply.keyIndex(walId);
            if (index > -1) {  // not tracked yet
                nextToApply.putAt(index, walId, segmentId);
                nextToApplyKeys.add(walId);
            }
        }

        private static boolean decodeLocked(long encoded) {
            final int segmentKey = Numbers.decodeLowInt(encoded);
            return (segmentKey & 1) == 1;
        }

        private static boolean decodePendingTasks(long encoded) {
            final int segmentKey = Numbers.decodeLowInt(encoded);
            return (segmentKey & 2) == 2;
        }

        private static int decodeSegmentId(long encoded) {
            final int segmentKey = Numbers.decodeLowInt(encoded);
            return segmentKey >> 2;
        }

        private static int decodeWalId(long encoded) {
            return Numbers.decodeHighInt(encoded);
        }

        private static long encodeDiscovered(int walId, int segmentId, boolean pendingTasks, boolean locked) {
            final int segmentKey = (segmentId << 2) + (pendingTasks ? 2 : 0) + (locked ? 1 : 0);
            return Numbers.encodeLowHighInts(segmentKey, walId);
        }

        private static boolean isWalDir(int segmentId) {
            return segmentId == WalUtils.SEG_NONE_ID;
        }

        private void logDebugInfo() {
            if (debugBuffer.length() > 0) {
                LOG.info().utf8(debugBuffer).$();
                debugBuffer.clear();
            }
        }

        private void sortTracked() {
            discovered.sort();
            nextToApplyKeys.sort();
        }
    }

    private class FsDeleter implements Deleter {
        @Override
        public void deleteSegmentDirectory(int walId, int segmentId) {
            LOG.info().$("deleting WAL segment directory [table=").utf8(tableToken.getDirName())
                    .$(", walId=").$(walId)
                    .$(", segmentId=").$(segmentId).$(']').$();
            if (deleteFile(setSegmentLockPath(tableToken, walId, segmentId))) {
                recursiveDelete(setSegmentPath(tableToken, walId, segmentId));
            }
        }

        @Override
        public void deleteWalDirectory(int walId) {
            LOG.info().$("deleting WAL directory [table=").utf8(tableToken.getDirName())
                    .$(", walId=").$(walId).$(']').$();
            if (deleteFile(setWalLockPath(tableToken, walId))) {
                recursiveDelete(setWalPath(tableToken, walId));
            }
        }
    }
}
