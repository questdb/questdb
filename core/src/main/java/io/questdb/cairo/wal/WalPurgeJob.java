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
import io.questdb.mp.AbstractQueueConsumerJob;
import io.questdb.std.*;
import io.questdb.std.str.NativeLPSZ;
import io.questdb.std.str.Path;
import io.questdb.tasks.WalPurgeNotificationTask;
import java.io.Closeable;

public class WalPurgeJob extends AbstractQueueConsumerJob<WalPurgeNotificationTask> implements Closeable {

    private static final Log LOG = LogFactory.getLog(WalPurgeJob.class);

    private CairoEngine engine;
    private Path path = new Path();
    private NativeLPSZ walName = new NativeLPSZ();
    private NativeLPSZ segmentName = new NativeLPSZ();

    // Key: WalId (e.g. 1 for "wal1") to index in lists below.
    private CharSequenceIntHashMap walInfo = new CharSequenceIntHashMap();

    private IntList segmentIds = new IntList();

    // TODO: Remove these?
    private LongList tableTxns = new LongList();
    private LongList segmentTxns = new LongList();


    private final IntHashSet outstandingLastTxnWals = new IntHashSet();

    private final IntList allWalIds = new IntList();

    private final IntHashSet seenWalIds = new IntHashSet();  // TODO [amunra]: Merge logic with `outstandingLastTxnWals`.

    private TxReader txReader;



    public WalPurgeJob(CairoEngine engine) {
        super(
                engine.getMessageBus().getWalPurgeNotificationQueue(),
                engine.getMessageBus().getWalPurgeNotificationSubSequence());
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

    private void setTablePath(Path path, CharSequence tableName) {
        path.of(engine.getConfiguration().getRoot())
                .concat(tableName).$();
    }

    private void setWalPath(Path path, CharSequence tableName, int walId) {
        path.of(engine.getConfiguration().getRoot())
                .concat(tableName).concat("wal").put(walId).$();
    }

    private void setSegmentPath(Path path, CharSequence tableName, int walId, int segmentId) {
        path.of(engine.getConfiguration().getRoot())
                .concat(tableName).concat("wal").put(walId).slash().put(segmentId).$();
    }

    private void broadSweepWal(int tableId, CharSequence tableName, int walId, long lastTxn) {
        setWalPath(path, tableName, walId);
        FilesFacadeImpl.INSTANCE.iterateDir(path, (pUtf8NameZ, type) -> {   // TODO [amunra]: Can I use the same `path` object here or is the memory borrowed by another `iterateDir`?
            if ((type == Files.DT_DIR) && matchesSegmentName(segmentName.of(pUtf8NameZ))) {
                // Read first long from transaction file or Transaction Reader.
                // engine.getWalReader(allowAll, tableName, walName, segmentCount, -1);
            }
        });
    }

    private void deleteWalDirectory(CharSequence tableName, int walId) {
        setWalPath(path, tableName, walId);
        FilesFacadeImpl.INSTANCE.rmdir(path);  // TODO: Recursive delete?
    }

    private void deleteUnreachableWals(CharSequence tableName, IntHashSet seenWalIds) {
        // We scan for WAL directories that we didn't already discover by looking through the sequencer cursor.
        setTablePath(path, tableName);
        FilesFacadeImpl.INSTANCE.iterateDir(path, (pUtf8NameZ, type) -> {
            if ((type == Files.DT_DIR) && matchesWalNamePattern(walName.of(pUtf8NameZ))) {
                // We just record the name for now in a set which we'll remove items to know when we're done.
                int walId = 0;
                try {
                    walId = Numbers.parseInt(walName, 3, walName.length());
                } catch (NumericException e) {
                    return;  // Ignore non-wal directory
                }
                if (!seenWalIds.contains(walId)) {
                    deleteWalDirectory(tableName, walId);
                }
            }
        });
    }

    private void deleteUnreachableSegments(CharSequence tableName, int walId, int segmentId) {
        setSegmentPath(path, tableName, walId, segmentId);
    }

    /**
     * Perform a broad sweep that searches for all tables that have closed
     * WAL segments across the database and deletes any which are no longer needed.
     */
    public void broadSweep() {
        // Clean-up at startup.
        TableRegistry tableRegistry = engine.getTableRegistry();
        tableRegistry.forAllWalTables((tableId, tableName, lastTxn) -> {
            segmentIds.clear();
            segmentTxns.clear();
            tableTxns.clear();
            walInfo.clear();
            allWalIds.clear();
            seenWalIds.clear();

            // TODO: How do I get the path to the TXN file?
            txReader.ofRO(path, PartitionBy.NONE);  // TODO [amunra]: Does the partitioning param matter here?
            final long lastAppliedTxn = txReader.unsafeReadVersion();

            try (SequencerCursor sequencerCursor = tableRegistry.getCursor(tableName, lastAppliedTxn)) {
                while (sequencerCursor.hasNext() && (outstandingLastTxnWals.size() > 0)) {
                    final int walId = sequencerCursor.getWalId();
                    final int segmentId = sequencerCursor.getSegmentId();

                    // We probably don't need these two.
                    final long segmentTxn = sequencerCursor.getSegmentTxn();
                    final long tableTxn = sequencerCursor.getTxn();

                    if (!seenWalIds.contains(walId)) {
                        final int index = segmentIds.size();
                        segmentIds.add(segmentId);
                        segmentTxns.add(segmentTxn);

                        // TODO: Remove these?
                        tableTxns.add(tableTxn);
                        walInfo.put(walName, index);

                        seenWalIds.add(walId);
                    }
                }
            }

            deleteUnreachableWals(tableName, seenWalIds);
            deleteUnreachableSegments(tableName, walInfo);
        });
    }

    @Override
    public void close() {
        this.txReader.close();
        path.close();
    }

    @Override
    protected boolean doRun(int workerId, long cursor) {
        // TODO [amunra]: Do we even needs queues? Should this be a sync job instead?
        return false;
    }
}
