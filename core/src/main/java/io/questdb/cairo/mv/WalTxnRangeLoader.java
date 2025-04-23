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
import io.questdb.cairo.TableToken;
import io.questdb.cairo.wal.WalEventCursor;
import io.questdb.cairo.wal.WalEventReader;
import io.questdb.cairo.wal.WalTxnDetails;
import io.questdb.cairo.wal.WalTxnType;
import io.questdb.cairo.wal.seq.TransactionLogCursor;
import io.questdb.griffin.model.IntervalUtils;
import io.questdb.std.DirectLongList;
import io.questdb.std.FilesFacade;
import io.questdb.std.LongList;
import io.questdb.std.MemoryTag;
import io.questdb.std.Misc;
import io.questdb.std.Numbers;
import io.questdb.std.QuietCloseable;
import io.questdb.std.Vect;
import io.questdb.std.str.Path;
import org.jetbrains.annotations.NotNull;

import static io.questdb.cairo.wal.WalUtils.*;

public class WalTxnRangeLoader implements QuietCloseable {
    private final WalEventReader walEventReader;
    private long maxTimestamp;
    private long minTimestamp;
    private DirectLongList txnDetails = new DirectLongList(10 * 4L, MemoryTag.NATIVE_TABLE_READER);

    public WalTxnRangeLoader(FilesFacade ff) {
        walEventReader = new WalEventReader(ff);
    }

    @Override
    public void close() {
        txnDetails = Misc.free(txnDetails);
    }

    public long getMaxTimestamp() {
        return maxTimestamp;
    }

    public long getMinTimestamp() {
        return minTimestamp;
    }

    public void load(
            @NotNull CairoEngine engine,
            @NotNull Path tempPath,
            @NotNull TableToken tableToken,
            @NotNull LongList intervals,
            long txnLo,
            long txnHi
    ) {
        try (TransactionLogCursor transactionLogCursor = engine.getTableSequencerAPI().getCursor(tableToken, txnLo)) {
            if (transactionLogCursor.getVersion() == WAL_SEQUENCER_FORMAT_VERSION_V1) {
                tempPath.of(engine.getConfiguration().getDbRoot()).concat(tableToken);
                int rootLen = tempPath.size();
                loadTransactionDetailsV1(tempPath, rootLen, transactionLogCursor, intervals, txnLo, txnHi);
            } else {
                loadTransactionDetailsV2(transactionLogCursor, intervals, txnLo, txnHi);
            }
        }
    }

    private void loadTransactionDetailsV1(Path tempPath, int rootLen, TransactionLogCursor transactionLogCursor, LongList intervals, long txnLo, long txnHi) {
        txnDetails.clear();

        try (WalEventReader eventReader = walEventReader) {
            final int maxLoadTxnCount = (int) (txnHi - txnLo);
            int txnsToLoad = (int) Math.min(maxLoadTxnCount, transactionLogCursor.getMaxTxn() - txnLo + 1);
            if (txnsToLoad > 0) {
                txnDetails.setCapacity(txnsToLoad * 4L);

                // Load the map of outstanding WAL transactions to load necessary details from WAL-E files efficiently.
                long max = Long.MIN_VALUE, min = Long.MAX_VALUE;
                int txn;
                for (txn = 0; txn < txnsToLoad && transactionLogCursor.hasNext(); txn++) {
                    long long1 = Numbers.encodeLowHighInts(transactionLogCursor.getSegmentId(), transactionLogCursor.getWalId() - MIN_WAL_ID);
                    max = Math.max(max, long1);
                    min = Math.min(min, long1);
                    txnDetails.add(long1);
                    txnDetails.add(Numbers.encodeLowHighInts(transactionLogCursor.getSegmentTxn(), txn));
                }
                txnsToLoad = txn;

                // We specify min as 0, so we expect the highest bit to be 0
                Vect.radixSortLongIndexAscChecked(
                        txnDetails.getAddress(),
                        txnsToLoad,
                        txnDetails.getAddress() + txnsToLoad * 2L * Long.BYTES,
                        min,
                        max
                );

                int lastWalId = -1;
                int lastSegmentId = -1;
                int lastSegmentTxn = -2;

                WalEventCursor walEventCursor = null;
                minTimestamp = Long.MAX_VALUE;
                maxTimestamp = Long.MIN_VALUE;

                for (int i = 0; i < txnsToLoad; i++) {
                    long long1 = txnDetails.get(2L * i);
                    long long2 = txnDetails.get(2L * i + 1);

                    long seqTxn = Numbers.decodeHighInt(long2) + txnLo;
                    int walId = Numbers.decodeHighInt(long1) + MIN_WAL_ID;
                    int segmentId = Numbers.decodeLowInt(long1);
                    int segmentTxn = Numbers.decodeLowInt(long2);

                    if (walId > 0) {
                        if (lastWalId == walId && segmentId == lastSegmentId) {
                            assert segmentTxn > lastSegmentTxn;
                            while (lastSegmentTxn < segmentTxn && walEventCursor.hasNext()) {
                                // Skip uncommitted transactions
                                lastSegmentTxn++;
                            }
                            if (lastSegmentTxn != segmentTxn) {
                                walEventCursor = WalTxnDetails.openWalEFile(tempPath, eventReader, segmentTxn, seqTxn);
                                lastSegmentTxn = segmentTxn;
                            }
                        } else {
                            tempPath.trimTo(rootLen).concat(WAL_NAME_BASE).put(walId).slash().put(segmentId);
                            walEventCursor = WalTxnDetails.openWalEFile(tempPath, eventReader, segmentTxn, seqTxn);
                            lastWalId = walId;
                            lastSegmentId = segmentId;
                            lastSegmentTxn = segmentTxn;
                        }

                        if (!WalTxnType.isDataType(walEventCursor.getType())) {
                            // Skip non-inserts
                            continue;
                        }

                        final WalEventCursor.DataInfo dataInfo = walEventCursor.getDataInfo();
                        intervals.add(dataInfo.getMinTimestamp(), dataInfo.getMaxTimestamp());
                        IntervalUtils.unionInPlace(intervals, intervals.size() - 2);
                    }
                }

                if (intervals.size() > 0) {
                    minTimestamp = intervals.getQuick(0);
                    maxTimestamp = intervals.getQuick(intervals.size() - 1);
                }
            }
        } finally {
            txnDetails.resetCapacity();
        }
    }

    private void loadTransactionDetailsV2(TransactionLogCursor transactionLogCursor, LongList intervals, long txnLo, long txnHi) {
        minTimestamp = Long.MAX_VALUE;
        maxTimestamp = Long.MIN_VALUE;

        while (txnLo++ < txnHi && transactionLogCursor.hasNext()) {
            if (transactionLogCursor.getTxnRowCount() > 0) {
                intervals.add(transactionLogCursor.getTxnMinTimestamp(), transactionLogCursor.getTxnMaxTimestamp());
                IntervalUtils.unionInPlace(intervals, intervals.size() - 2);
            }
        }

        if (intervals.size() > 0) {
            minTimestamp = intervals.getQuick(0);
            maxTimestamp = intervals.getQuick(intervals.size() - 1);
        }
    }
}
