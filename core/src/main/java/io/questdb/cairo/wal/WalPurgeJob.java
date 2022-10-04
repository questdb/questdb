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

import io.questdb.cairo.CairoEngine;
import io.questdb.cairo.PartitionBy;
import io.questdb.cairo.TxReader;
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
    private static class WalInfoTable {
        public IntList walIds = new IntList();
        public IntList segmentIds = new IntList();
        public LongList segmentTxns = new LongList();
        public LongList txns = new LongList();

        public void clear() {
            walIds.clear();
            segmentIds.clear();
            segmentTxns.clear();
            txns.clear();
        }

        public int size() {
            return walIds.size();
        }

        public void add(int walId, int segmentId, long segmentTxn, long txn) {
            walIds.add(walId);
            segmentIds.add(segmentId);
            segmentTxns.add(segmentTxn);
            txns.add(txn);
        }
    }

    // TODO [amunra]: Sprinkle some logging to track what got deleted and why.
    private static final Log LOG = LogFactory.getLog(WalPurgeJob.class);

    private CairoEngine engine;
    private Path path = new Path();
    private NativeLPSZ walName = new NativeLPSZ();
    private NativeLPSZ segmentName = new NativeLPSZ();
    private final IntHashSet discoveredWalIds = new IntHashSet();
    private WalInfoTable walInfoTable = new WalInfoTable();
    private TxReader txReader;

    public WalPurgeJob(CairoEngine engine) {
        this.engine = engine;
        this.txReader = new TxReader(engine.getConfiguration().getFilesFacade());
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
        path.of(engine.getConfiguration().getRoot())
                .concat(tableName).$();
        return path;
    }

    private Path setWalPath(CharSequence tableName, int walId) {
        path.of(engine.getConfiguration().getRoot())
                .concat(tableName).concat("wal").put(walId).$();
        return path;
    }

    private Path setSegmentPath(CharSequence tableName, int walId, int segmentId) {
        path.of(engine.getConfiguration().getRoot())
                .concat(tableName).concat("wal").put(walId).slash().put(segmentId).$();
        return path;
    }

    private void discoverWalDirectories(CharSequence tableName, IntHashSet discoveredWalIds) {
        FilesFacadeImpl.INSTANCE.iterateDir(setTablePath(tableName), (pUtf8NameZ, type) -> {
            if ((type == Files.DT_DIR) && matchesWalNamePattern(walName.of(pUtf8NameZ))) {
                // We just record the name for now in a set which we'll remove items to know when we're done.
                int walId = 0;
                try {
                    walId = Numbers.parseInt(walName, 3, walName.length());
                } catch (NumericException _ne) {
                    return;  // Ignore non-wal directory
                }
                discoveredWalIds.add(walId);
            }
        });
    }

    private void extractSegmentInfoForEachWal(CharSequence tableName, TableRegistry tableRegistry, IntHashSet fsWalIds, WalInfoTable walInfoTable) {
        // TODO: How do I get the path to the TXN file?
        txReader.ofRO(path, PartitionBy.NONE);  // TODO [amunra]: Does the partitioning param matter here?
        final long lastAppliedTxn = txReader.unsafeReadVersion();

        try (SequencerCursor sequencerCursor = tableRegistry.getCursor(tableName, lastAppliedTxn)) {
            while (sequencerCursor.hasNext() && (fsWalIds.size() > 0)) {
                final int walId = sequencerCursor.getWalId();
                if (fsWalIds.contains(walId)) {
                    walInfoTable.add(walId,
                            sequencerCursor.getSegmentId(),

                            // TODO [amunra]: Do we need these two fields?
                            sequencerCursor.getSegmentTxn(),
                            sequencerCursor.getTxn());
                    fsWalIds.remove(walId);
                }
            }
        }
    }

    private void silentRecursiveDelete(Path path) {
        // TODO [amunra]: Is this a recursive delete? Are errors suppressed?
        FilesFacadeImpl.INSTANCE.rmdir(path);
    }

    private void deleteWalDirectory(CharSequence tableName, int walId) {
        silentRecursiveDelete(setWalPath(tableName, walId));
    }

    private void deleteOutstandingWalDirectories(CharSequence tableName, IntHashSet discoveredWalIds) {
        for (PrimitiveIterator.OfInt it = discoveredWalIds.iterator(); it.hasNext(); ) {
            deleteWalDirectory(tableName, it.nextInt());
        }
    }

    private boolean segmentIsReapable(CharSequence tableName, int walId, int segmentId, int walsLatestSegmentId) {
        return segmentId < walsLatestSegmentId;
    }

    private void deleteClosedSegment(CharSequence tableName, int walId, int segmentId) {
        silentRecursiveDelete(setSegmentPath(tableName, walId, segmentId));
    }

    private void deleteUnreachableSegments(CharSequence tableName, WalInfoTable walInfoTable) {
        for (int i = 0; i < walInfoTable.size(); ++i) {
            final int index = i;
            final int walId = walInfoTable.walIds.get(index);
            FilesFacadeImpl.INSTANCE.iterateDir(setWalPath(tableName, walId), (pUtf8NameZ, type) -> {
                if ((type == Files.DT_DIR) && matchesSegmentName(segmentName.of(pUtf8NameZ))) {
                    int segmentId = 0;
                    try {
                        segmentId = Numbers.parseInt(segmentName);
                    }
                    catch (NumericException _ne) {
                        return; // Ignore non-segment directory.
                    }
                    final int walsLatestSegmentId = walInfoTable.segmentIds.get(index);
                    if (segmentIsReapable(tableName, walId, segmentId, walsLatestSegmentId)) {
                        deleteClosedSegment(tableName, walId, segmentId);
                    }
                }
            });
        }
        // setSegmentPath(tableName, walId, segmentId);
    }

    /**
     * Perform a broad sweep that searches for all tables that have closed
     * WAL segments across the database and deletes any which are no longer needed.
     */
    private void broadSweep() {
        TableRegistry tableRegistry = engine.getTableRegistry();
        tableRegistry.forAllWalTables((tableId, tableName, lastTxn) -> {
            discoveredWalIds.clear();
            walInfoTable.clear();

            discoverWalDirectories(tableName, discoveredWalIds);

            extractSegmentInfoForEachWal(tableName, tableRegistry, discoveredWalIds, walInfoTable);

            // Call to `extractSegmentInfoForEachWal` populated `walInfoTable`.
            deleteUnreachableSegments(tableName, walInfoTable);

            // Calls to `extractSegmentInfoForEachWal` and `deleteUnreachableSegments` possibly leave outstanding
            // `discoveredWalIds` that are still on the filesystem and don't have any active segments.
            // The walNNN directories themselves can be removed.
            // Note that this also handles cases where a wal directory was created shortly before a crash and thus
            // never recorded and tracked by the sequencer for that table.
            deleteOutstandingWalDirectories(tableName, discoveredWalIds);
        });
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
