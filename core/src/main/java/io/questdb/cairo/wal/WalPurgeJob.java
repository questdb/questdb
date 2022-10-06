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
import io.questdb.std.str.NativeLPSZ;
import io.questdb.std.str.Path;

import java.io.Closeable;
import java.util.PrimitiveIterator;

// TODO [amunra]: Should this just be a sync job?
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

    // TODO [amunra]: Sprinkle some logging to track what got deleted and why.
    private static final Log LOG = LogFactory.getLog(WalPurgeJob.class);

    private CairoEngine engine;
    private FilesFacade ff;
    private TxReader txReader;
    private Path path = new Path();
    private NativeLPSZ walName = new NativeLPSZ();
    private NativeLPSZ segmentName = new NativeLPSZ();  // TODO [amunra]: Can we have a single "name" instead of "walName" and "segmentName"?
    private final IntHashSet discoveredWalIds = new IntHashSet();
    private final IntHashSet walsInUse = new IntHashSet();
    private WalInfoDataFrame walInfoDataFrame = new WalInfoDataFrame();

    private int walId;
    private int walsLatestSegmentId;

    private CharSequence tableName;

    private boolean anySegmentsKept = false;

    public WalPurgeJob(CairoEngine engine) {
        this.engine = engine;
        this.ff = engine.getConfiguration().getFilesFacade();
        this.txReader = new TxReader(ff);
    }

    /** Validate equivalent of "^wal\d+$" regex. */
    private static boolean matchesWalNamePattern(CharSequence name) {
        final int len = name.length();
        if (len < 4) {
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

    private Path setTxnPath(CharSequence tableName) {
        return path.of(engine.getConfiguration().getRoot())
                .concat(tableName)
                .concat(TableUtils.TXN_FILE_NAME).$();
    }

    private Path setWalPath(CharSequence tableName, int walId) {
        return path.of(engine.getConfiguration().getRoot())
                .concat(tableName).concat("wal").put(walId).$();
    }

    private Path setWalLockPath(CharSequence tableName, int walId) {
        path.of(engine.getConfiguration().getRoot())
                .concat(tableName).concat("wal").put(walId);
        TableUtils.lockName(path);
        return path;
    }

    private Path setSegmentPath(CharSequence tableName, int walId, int segmentId) {
        return path.of(engine.getConfiguration().getRoot())
                .concat(tableName).concat("wal").put(walId).slash().put(segmentId).$();
    }

    private Path setSegmentLockPath(CharSequence tableName, int walId, int segmentId) {
        path.of(engine.getConfiguration().getRoot())
                .concat(tableName).concat("wal").put(walId).slash().put(segmentId);
        TableUtils.lockName(path);
        return path;
    }

    private boolean couldObtainLock(Path path) {
        long lockFd = TableUtils.lock(ff, path, false);
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
            } catch (NumericException _ne) {
                return;  // Ignore non-wal directory
            }
            discoveredWalIds.add(walId);
            if (walIsInUse(tableName, walId)) {
                walsInUse.add(walId);
            }
        }
    }

    private void discoverWalDirectories() {
        ff.iterateDir(setTablePath(tableName), this::discoverWalDirectoriesIter);
    }

    private void populateWalInfoTable() {
        setTxnPath(tableName);
        txReader.ofRO(path, PartitionBy.NONE);  // TODO [amunra]: Does the partitioning param matter here? - I guess not
        final long lastAppliedTxn = txReader.unsafeReadVersion();

        TableRegistry tableRegistry = engine.getTableRegistry();
        try (SequencerCursor sequencerCursor = tableRegistry.getCursor(tableName, lastAppliedTxn)) {
            while (sequencerCursor.hasNext() && (discoveredWalIds.size() > 0)) {
                final int walId = sequencerCursor.getWalId();
                if (discoveredWalIds.contains(walId)) {
                    walInfoDataFrame.add(walId,
                            sequencerCursor.getSegmentId(),

                            // TODO [amunra]: Do we need these two fields?
                            sequencerCursor.getSegmentTxn(),
                            sequencerCursor.getTxn());
                    discoveredWalIds.remove(walId);
                }
            }
        }
    }

    private void silentRecursiveDelete(Path path) {
        // TODO [amunra]: Is this a recursive delete? Are errors suppressed?
        ff.rmdir(path);
    }

    private void deleteWalDirectory() {
        silentRecursiveDelete(setWalPath(tableName, walId));
    }

    private void deleteSegmentDirectory(CharSequence tableName, int walId, int segmentId) {
        silentRecursiveDelete(setSegmentPath(tableName, walId, segmentId));
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
        ff.iterateDir(path, this::deleteClosedSegmentsIter);
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
            else {
                anySegmentsKept = true;
            }
        }
    }

    private void deleteUnreachableSegments() {
        for (int index = 0; index < walInfoDataFrame.size(); ++index) {
            walId = walInfoDataFrame.walIds.get(index);
            walsLatestSegmentId = walInfoDataFrame.segmentIds.get(index);
            anySegmentsKept = false;
            ff.iterateDir(setWalPath(tableName, walId), this::deleteUnreachableSegmentsIter);

            // If all known segments were deleted, then this whole WAL directory is candidate for deletion.
            // We add it for clean-up by `deleteOutstandingWalDirectories`.
            if (!anySegmentsKept) {
                discoveredWalIds.add(walId);
            }
        }
    }

    private void broadSweepIter(int _tableId, CharSequence tableName, long _lastTxn) {
        this.tableName = tableName;

        discoveredWalIds.clear();
        walsInUse.clear();
        walInfoDataFrame.clear();

        discoverWalDirectories();
        populateWalInfoTable();
        deleteUnreachableSegments();

        // Any of the calls above may leave outstanding `discoveredWalIds` that are still on the filesystem
        // and don't have any active segments. The walNNN directories themselves may be deleted if they don't have
        // an associated open WalWriter.
        // Note that this also handles cases where a wal directory was created shortly before a crash and thus
        // never recorded and tracked by the sequencer for that table.
        deleteOutstandingWalDirectories();
    }

    /**
     * Perform a broad sweep that searches for all tables that have closed
     * WAL segments across the database and deletes any which are no longer needed.
     */
    private void broadSweep() {
        engine.getTableRegistry().forAllWalTables(this::broadSweepIter);
    }

    @Override
    protected boolean runSerially() {
        broadSweep();
        return false;
    }

    @Override
    public void close() {
        this.txReader.close();
        path.close();
    }
}
