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

package io.questdb.cairo.mv;

import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.CairoEngine;
import io.questdb.cairo.TableToken;
import io.questdb.cairo.wal.WalEventCursor;
import io.questdb.cairo.wal.WalEventReader;
import io.questdb.cairo.wal.WalTxnDetails;
import io.questdb.cairo.wal.WalTxnType;
import io.questdb.cairo.wal.seq.TransactionLogCursor;
import io.questdb.griffin.model.IntervalUtils;
import io.questdb.std.DirectLongList;
import io.questdb.std.LongList;
import io.questdb.std.MemoryTag;
import io.questdb.std.Misc;
import io.questdb.std.Numbers;
import io.questdb.std.QuietCloseable;
import io.questdb.std.str.Path;
import org.jetbrains.annotations.NotNull;

import static io.questdb.cairo.wal.WalUtils.*;

public class WalTxnRangeLoader implements QuietCloseable {
    private final WalEventReader walEventReader;
    private long maxTimestamp;
    private long minTimestamp;
    private DirectLongList txnDetails = new DirectLongList(10 * 4L, MemoryTag.NATIVE_TABLE_READER);

    public WalTxnRangeLoader(CairoConfiguration configuration) {
        walEventReader = new WalEventReader(configuration);
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
            // We need to load replace range timestamps from WAL-E files, even when we can read min/max timestamps
            // from the transaction log when it's in V2 format.
            tempPath.of(engine.getConfiguration().getDbRoot()).concat(tableToken);
            int rootLen = tempPath.size();
            loadTransactionDetailsFromWalE(tempPath, rootLen, transactionLogCursor, intervals, txnLo, txnHi);
        }
    }

    private void loadTransactionDetailsFromWalE(
            Path tempPath,
            int rootLen,
            TransactionLogCursor transactionLogCursor,
            LongList intervals,
            long txnLo,
            long txnHi
    ) {
        txnDetails.clear();

        minTimestamp = Long.MAX_VALUE;
        maxTimestamp = Long.MIN_VALUE;

        try (WalEventReader eventReader = walEventReader) {
            final int maxLoadTxnCount = (int) (txnHi - txnLo);
            int txnsToLoad = (int) Math.min(maxLoadTxnCount, transactionLogCursor.getMaxTxn() - txnLo + 1);
            if (txnsToLoad > 0) {
                txnsToLoad = WalTxnDetails.loadTxns(transactionLogCursor, txnsToLoad, txnDetails);

                int lastWalId = -1;
                int lastSegmentId = -1;
                int lastSegmentTxn = -2;

                WalEventCursor walEventCursor = null;
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
                        long minTimestamp1 = dataInfo.getMinTimestamp();
                        long maxTimestamp1 = dataInfo.getMaxTimestamp();
                        if (dataInfo.getDedupMode() == WAL_DEDUP_MODE_REPLACE_RANGE) {
                            minTimestamp1 = dataInfo.getReplaceRangeTsLow();
                            // Replace range high is exclusive, so we subtract 1 to make it maximum inclusive.
                            maxTimestamp1 = dataInfo.getReplaceRangeTsHi() - 1;
                        }
                        intervals.add(minTimestamp1, maxTimestamp1);
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
}
