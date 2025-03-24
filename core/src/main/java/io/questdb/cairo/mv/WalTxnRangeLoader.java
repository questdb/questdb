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

package io.questdb.cairo.mv;

import io.questdb.cairo.CairoEngine;
import io.questdb.cairo.CairoException;
import io.questdb.cairo.TableToken;
import io.questdb.cairo.wal.WalEventCursor;
import io.questdb.cairo.wal.WalEventReader;
import io.questdb.cairo.wal.WalTxnType;
import io.questdb.cairo.wal.seq.TransactionLogCursor;
import io.questdb.std.FilesFacade;
import io.questdb.std.IntList;
import io.questdb.std.str.Path;

import static io.questdb.cairo.wal.WalUtils.WAL_NAME_BASE;
import static io.questdb.cairo.wal.WalUtils.WAL_SEQUENCER_FORMAT_VERSION_V1;

public class WalTxnRangeLoader {
    private final IntList txnDetails = new IntList();
    private final WalEventReader walEventReader;
    private long maxTimestamp;
    private long minTimestamp;

    public WalTxnRangeLoader(FilesFacade ff) {
        walEventReader = new WalEventReader(ff);
    }

    public long getMaxTimestamp() {
        return maxTimestamp;
    }

    public long getMinTimestamp() {
        return minTimestamp;
    }

    public void load(CairoEngine engine, Path tempPath, TableToken tableToken, long txnLo, long txnHi) {
        try (TransactionLogCursor transactionLogCursor = engine.getTableSequencerAPI().getCursor(tableToken, txnLo)) {
            if (transactionLogCursor.getVersion() == WAL_SEQUENCER_FORMAT_VERSION_V1) {
                tempPath.of(engine.getConfiguration().getDbRoot()).concat(tableToken);
                int rootLen = tempPath.size();
                loadTransactionDetailsV1(tempPath, transactionLogCursor, rootLen, txnLo, txnHi);
            } else {
                loadTransactionDetailsV2(transactionLogCursor, txnLo, txnHi);
            }
        }
    }

    private static WalEventCursor openWalEFile(Path tempPath, WalEventReader eventReader, int segmentTxn) {
        WalEventCursor walEventCursor;
        try {
            walEventCursor = eventReader.of(tempPath, segmentTxn);
        } catch (CairoException ex) {
            throw CairoException.critical(ex.getErrno()).put("cannot read WAL even file:").put(tempPath)
                    .put(", ").put(ex.getFlyweightMessage());
        }
        return walEventCursor;
    }

    private void loadTransactionDetailsV1(Path tempPath, TransactionLogCursor transactionLogCursor, int rootLen, long txnLo, long txnHi) {
        txnDetails.clear();
        txnDetails.allocate((int) ((txnHi - txnLo) * 3));

        while (txnLo++ < txnHi && transactionLogCursor.hasNext()) {
            final int walId = transactionLogCursor.getWalId();
            if (walId > 0) {
                txnDetails.add(transactionLogCursor.getWalId());
                txnDetails.add(transactionLogCursor.getSegmentId());
                txnDetails.add(transactionLogCursor.getSegmentTxn());
            }
        }

        int lastWalId = -1;
        int lastSegmentId = -1;
        int lastSegmentTxn = -2;
        WalEventCursor walEventCursor = null;
        minTimestamp = Long.MAX_VALUE;
        maxTimestamp = Long.MIN_VALUE;

        txnDetails.sortGroups(3);
        try (WalEventReader eventReader = walEventReader) {
            for (int i = 0, size = txnDetails.size() / 3; i < size; i++) {
                int walId = txnDetails.get(3 * i);
                int segmentId = txnDetails.get(3 * i + 1);
                int segmentTxn = txnDetails.get(3 * i + 2);

                if (lastWalId == walId && segmentId == lastSegmentId) {
                    assert segmentTxn > lastSegmentTxn;
                    while (lastSegmentTxn++ < segmentTxn && walEventCursor.hasNext()) {
                        // Skip uncommitted yet transactions
                    }
                    if (lastSegmentTxn != segmentTxn) {
                        walEventCursor = openWalEFile(tempPath, eventReader, segmentTxn);
                        lastSegmentTxn = segmentTxn;
                    }
                } else {
                    tempPath.trimTo(rootLen).concat(WAL_NAME_BASE).put(walId).slash().put(segmentId);
                    walEventCursor = openWalEFile(tempPath, eventReader, segmentTxn);
                    lastWalId = walId;
                    lastSegmentId = segmentId;
                    lastSegmentTxn = segmentTxn;
                }

                if (!WalTxnType.isDataType(walEventCursor.getType())) {
                    // Skip non-inserts
                    continue;
                }

                minTimestamp = Math.min(minTimestamp, walEventCursor.getDataInfo().getMinTimestamp());
                maxTimestamp = Math.max(maxTimestamp, walEventCursor.getDataInfo().getMaxTimestamp());
            }
        }
        txnDetails.clear();
    }

    private void loadTransactionDetailsV2(TransactionLogCursor transactionLogCursor, long txnLo, long txnHi) {
        minTimestamp = Long.MAX_VALUE;
        maxTimestamp = Long.MIN_VALUE;

        while (txnLo++ < txnHi && transactionLogCursor.hasNext()) {
            if (transactionLogCursor.getTxnRowCount() > 0) {
                minTimestamp = Math.min(minTimestamp, transactionLogCursor.getTxnMinTimestamp());
                maxTimestamp = Math.max(maxTimestamp, transactionLogCursor.getTxnMaxTimestamp());
            }
        }
    }
}
