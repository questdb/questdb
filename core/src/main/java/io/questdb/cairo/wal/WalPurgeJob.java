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
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.mp.SynchronizedJob;
import io.questdb.std.*;
import io.questdb.std.datetime.microtime.MicrosecondClock;
import io.questdb.std.str.NativeLPSZ;
import io.questdb.std.str.Path;
import io.questdb.std.str.StringSink;

import java.io.Closeable;
import java.util.PrimitiveIterator;

public class WalPurgeJob extends SynchronizedJob implements Closeable {

    /** Table of columns grouping segment information. One row per walId. */
    private static class WalInfoDataFrame {
        public IntList walIds = new IntList();
        public IntList segmentIds = new IntList();

        public void clear() {
            walIds.clear();
            segmentIds.clear();
        }

        public int size() {
            return walIds.size();
        }

        public void add(int walId, int segmentId) {
            walIds.add(walId);
            segmentIds.add(segmentId);
        }
    }

    private final FindVisitor discoverWalDirectoriesIterFunc = this::discoverWalDirectoriesIter;
    private final FindVisitor deleteClosedSegmentsIterFunc = this::deleteClosedSegmentsIter;
    private final FindVisitor deleteUnreachableSegmentsIterFunc = this::deleteUnreachableSegmentsIter;
    private final FindVisitor broadSweepIterFunc = this::broadSweepIter;
    private static final Log LOG = LogFactory.getLog(WalPurgeJob.class);
    private final CairoEngine engine;
    private final FilesFacade ff;
    private final MicrosecondClock clock;
    private final long checkInterval;
    private long last = 0;
    private final TxReader txReader;
    private final Path path = new Path();
    private final NativeLPSZ tableName = new NativeLPSZ();
    private final NativeLPSZ walName = new NativeLPSZ();
    private final NativeLPSZ segmentName = new NativeLPSZ();
    private final IntHashSet discoveredWalIds = new IntHashSet();
    private final IntHashSet walsInUse = new IntHashSet();
    private final WalInfoDataFrame walInfoDataFrame = new WalInfoDataFrame();

    private int walId;
    private int walsLatestSegmentId;

    private final StringSink debugBuffer = new StringSink();

    public WalPurgeJob(CairoEngine engine, FilesFacade ff, MicrosecondClock clock) {
        this.engine = engine;
        this.ff = ff;
        this.clock = clock;
        this.checkInterval = engine.getConfiguration().getWalPurgeInterval() * 1000;
        this.txReader = new TxReader(ff);

        assert WalUtils.WAL_NAME_BASE.equals("wal");
    }

    public WalPurgeJob(CairoEngine engine) {
        this(engine, engine.getConfiguration().getFilesFacade(), engine.getConfiguration().getMicrosecondClock());
    }

    /**
     * Delay the first run of this job by half a configured interval to
     * spread its work more evenly across other timer-based jobs with
     * similar cadences.
     */
    public void delayByHalfInterval() {
        this.last = clock.getTicks() - (checkInterval / 2);
    }

    /** Validate equivalent of "^wal\d+$" regex. */
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

    /** Validate equivalent of "^\d+$" regex. */
    private static boolean matchesSegmentName(CharSequence name) {
        for (int i = 0, n = name.length(); i < n; i++) {
            char c = name.charAt(i);
            if (c < '0' || c > '9') {
                return false;
            }
        }
        return true;
    }

    private Path setTablePath(CharSequence tableName) {
        return path.of(engine.getConfiguration().getRoot())
                .concat(tableName).$();
    }

    private Path setSeqTxnPath(CharSequence tableName) {
        return path.of(engine.getConfiguration().getRoot())
                .concat(tableName)
                .concat(Sequencer.SEQ_DIR).$();
    }

    private Path setTxnPath(CharSequence tableName) {
        return path.of(engine.getConfiguration().getRoot())
                .concat(tableName)
                .concat(TableUtils.TXN_FILE_NAME).$();
    }

    private Path setWalPath(CharSequence tableName, int walId) {
        return path.of(engine.getConfiguration().getRoot())
                .concat(tableName).concat(WalUtils.WAL_NAME_BASE).put(walId).$();
    }

    private Path setWalLockPath(CharSequence tableName, int walId) {
        path.of(engine.getConfiguration().getRoot())
                .concat(tableName).concat(WalUtils.WAL_NAME_BASE).put(walId);
        TableUtils.lockName(path);
        return path;
    }

    private Path setSegmentPath(CharSequence tableName, int walId, int segmentId) {
        return path.of(engine.getConfiguration().getRoot())
                .concat(tableName).concat(WalUtils.WAL_NAME_BASE).put(walId).slash().put(segmentId).$();
    }

    private Path setSegmentLockPath(CharSequence tableName, int walId, int segmentId) {
        path.of(engine.getConfiguration().getRoot())
                .concat(tableName).concat(WalUtils.WAL_NAME_BASE).put(walId).slash().put(segmentId);
        TableUtils.lockName(path);
        return path;
    }

    private boolean couldObtainLock(Path path) {
        final long lockFd = TableUtils.lock(ff, path, false);
        if (lockFd != -1L) {
            ff.close(lockFd);
            return true;  // Could lock/unlock.
        }
        return false;  // Could not obtain lock.
    }

    private boolean walIsInUse(CharSequence tableName, int walId) {
        return !couldObtainLock(setWalLockPath(tableName, walId));
    }

    private void discoverWalDirectoriesIter(long pUtf8NameZ, int type) {
        if ((type == Files.DT_DIR) && matchesWalNamePattern(walName.of(pUtf8NameZ))) {
            // We just record the name for now in a set which we'll remove items to know when we're done.
            int walId = 0;
            try {
                walId = Numbers.parseInt(walName, 3, walName.length());
            } catch (NumericException ne) {
                return;  // Ignore non-wal directory
            }
            discoveredWalIds.add(walId);
            if (walIsInUse(tableName, walId)) {
                walsInUse.add(walId);
            }
        }
    }

    private void discoverWalDirectories() {
        ff.iterateDir(setTablePath(tableName), discoverWalDirectoriesIterFunc);
    }

    private void populateWalInfoDataFrame() {
        setTxnPath(tableName);
        txReader.ofRO(path, PartitionBy.NONE);
        try {
            final long lastAppliedTxn = txReader.unsafeReadVersion();

            TableRegistry tableRegistry = engine.getTableRegistry();
            try (SequencerCursor sequencerCursor = tableRegistry.getCursor(tableName, lastAppliedTxn)) {
                while (sequencerCursor.hasNext() && (discoveredWalIds.size() > 0)) {
                    final int walId = sequencerCursor.getWalId();
                    if (discoveredWalIds.contains(walId)) {
                        walInfoDataFrame.add(walId, sequencerCursor.getSegmentId());
                        discoveredWalIds.remove(walId);
                    }
                }
            }
        }
        finally {
            txReader.close();
        }
    }

    private void recursiveDelete(Path path) {
        final int errno = ff.rmdir(path);
        if ((errno != 0) && ((errno != 2)))  {
            LOG.error().$("Could not delete directory [path=").$(path)
                    .$(", errno=").$(errno).$(']').$();
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

    private void mayLogDebugInfo() {
        if (debugBuffer.length() > 0) {
            LOG.info().$(debugBuffer).$();
            debugBuffer.clear();
        }
    }

    private void deleteWalDirectory() {
        mayLogDebugInfo();
        LOG.info().$("deleting WAL directory [table=").$(tableName)
                .$(", walId=").$(walId).$(']').$();
        if (deleteFile(setWalLockPath(tableName, walId))) {
            recursiveDelete(setWalPath(tableName, walId));
        }
    }

    private void deleteSegmentDirectory(CharSequence tableName, int walId, int segmentId) {
        mayLogDebugInfo();
        LOG.info().$("deleting WAL segment directory [table=").$(tableName)
                .$(", walId=").$(walId)
                .$(", segmentId=").$(segmentId).$(']').$();
        if (deleteFile(setSegmentLockPath(tableName, walId, segmentId))) {
            recursiveDelete(setSegmentPath(tableName, walId, segmentId));
        }
    }

    private void deleteClosedSegmentsIter(long pUtf8NameZ, int type) {
        if ((type == Files.DT_DIR) && matchesSegmentName(segmentName.of(pUtf8NameZ))) {
            int segmentId = 0;
            try {
                segmentId = Numbers.parseInt(segmentName);
            } catch (NumericException _ne) {
                return; // Ignore non-segment directory.
            }
            if (couldObtainLock(setSegmentLockPath(tableName, walId, segmentId))) {
                deleteSegmentDirectory(tableName, walId, segmentId);
            }
        }
    }

    private void deleteClosedSegments() {
        setWalPath(tableName, walId);
        ff.iterateDir(path, deleteClosedSegmentsIterFunc);
    }

    private void deleteOutstandingWalDirectories() {
        for (PrimitiveIterator.OfInt it = discoveredWalIds.iterator(); it.hasNext(); ) {
            walId = it.nextInt();
            if (walsInUse.contains(walId)) {
                deleteClosedSegments();
            }
            else {
                deleteWalDirectory();
            }
        }
    }

    private boolean segmentIsReapable(int segmentId, int walsLatestSegmentId) {
        return segmentId < walsLatestSegmentId;
    }

    private void deleteUnreachableSegmentsIter(long pUtf8NameZ, int type) {
        if ((type == Files.DT_DIR) && matchesSegmentName(segmentName.of(pUtf8NameZ))) {
            int segmentId = 0;
            try {
                segmentId = Numbers.parseInt(segmentName);
            }
            catch (NumericException _ne) {
                return; // Ignore non-segment directory.
            }
            if (segmentIsReapable(segmentId, walsLatestSegmentId)) {
                deleteSegmentDirectory(tableName, walId, segmentId);
            }
        }
    }

    private void deleteUnreachableSegments() {
        for (int index = 0; index < walInfoDataFrame.size(); ++index) {
            walId = walInfoDataFrame.walIds.get(index);
            walsLatestSegmentId = walInfoDataFrame.segmentIds.get(index);
            ff.iterateDir(setWalPath(tableName, walId), deleteUnreachableSegmentsIterFunc);
        }
    }

    private void accumDebugState() {
        debugBuffer.clear();
        debugBuffer.put("table=").put(tableName)
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

    private boolean isWalTable(CharSequence tableName) {
        final int tableNameLen = tableName.length();
        final boolean result = ff.exists(setSeqTxnPath(tableName));
        path.trimTo(tableNameLen).$();
        return result;
    }

    private void broadSweepIter(long pUtf8NameZ, int type) {
        if ((type == Files.DT_DIR) && isWalTable(tableName.of(pUtf8NameZ))) {
            try {
                discoveredWalIds.clear();
                walsInUse.clear();
                walInfoDataFrame.clear();

                discoverWalDirectories();
                if (discoveredWalIds.size() == 0) {
                    return;
                }

                populateWalInfoDataFrame();
                accumDebugState();

                deleteUnreachableSegments();

                // Any of the calls above may leave outstanding `discoveredWalIds` that are still on the filesystem
                // and don't have any active segments. Any unlocked walNNN directories may be deleted if they don't have
                // pending segments that are yet to be applied to the table.
                // Note that this also handles cases where a wal directory was created shortly before a crash and thus
                // never recorded and tracked by the sequencer for that table.
                deleteOutstandingWalDirectories();
            } catch (CairoException ce) {
                LOG.error().$("broad sweep failed [table=").$(tableName)
                        .$(", msg=").$((Throwable) ce)
                        .$(", errno=").$(ff.errno()).$(']').$();
            }
        }
    }

    /**
     * Perform a broad sweep that searches for all tables that have closed
     * WAL segments across the database and deletes any which are no longer needed.
     */
    private void broadSweep() {
        path.of(engine.getConfiguration().getRoot()).slash$();
        ff.iterateDir(path, broadSweepIterFunc);
    }

    @Override
    protected boolean runSerially() {
        long t = clock.getTicks();
        if (last + checkInterval < t) {
            last = t;
            broadSweep();
        }
        return false;
    }

    @Override
    public void close() {
        this.txReader.close();
        path.close();
    }
}
