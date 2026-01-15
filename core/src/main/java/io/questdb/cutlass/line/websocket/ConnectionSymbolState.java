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

package io.questdb.cutlass.line.websocket;

import io.questdb.std.IntList;
import io.questdb.std.LongList;

/**
 * Tracks per-connection symbol dictionary watermark state for delta encoding.
 * <p>
 * This class maintains the relationship between batches sent and the maximum
 * symbol ID used in each batch. When batches are acknowledged (via cumulative ACK),
 * the watermark advances to the highest confirmed symbol ID.
 * <p>
 * The watermark (confirmedMaxId) represents the highest symbol ID that the server
 * has definitively received. Subsequent batches only need to send symbols with
 * IDs greater than confirmedMaxId.
 * <p>
 * Thread safety: This class is NOT thread-safe. External synchronization is required
 * if accessed from multiple threads.
 */
public class ConnectionSymbolState {

    // Highest symbol ID that the server has confirmed it received
    private int confirmedMaxId = -1;

    // Parallel lists: pendingSequences[i] has maxId of pendingMaxIds[i]
    // Sequences are stored in insertion order (typically ascending)
    private final LongList pendingSequences;
    private final IntList pendingMaxIds;

    public ConnectionSymbolState() {
        this(64);
    }

    public ConnectionSymbolState(int initialCapacity) {
        this.pendingSequences = new LongList(initialCapacity);
        this.pendingMaxIds = new IntList(initialCapacity);
    }

    /**
     * Returns the highest symbol ID that the server has confirmed receiving.
     * Returns -1 for a fresh connection (no confirmations yet).
     */
    public int getConfirmedMaxId() {
        return confirmedMaxId;
    }

    /**
     * Records that a batch was sent with symbols up to maxIdUsed.
     *
     * @param batchSequence the sequence number of the batch
     * @param maxIdUsed     the highest symbol ID used in this batch, or -1 if no symbols
     */
    public void onBatchSent(long batchSequence, int maxIdUsed) {
        pendingSequences.add(batchSequence);
        pendingMaxIds.add(maxIdUsed);
    }

    /**
     * Handles a cumulative acknowledgment up to the given sequence.
     * Advances the confirmed watermark and removes acknowledged batches.
     *
     * @param ackedSequence the cumulative ACK sequence (all batches <= this are acknowledged)
     */
    public void onBatchesAcked(long ackedSequence) {
        int n = pendingSequences.size();
        if (n == 0) {
            return;
        }

        int maxAckedId = confirmedMaxId;
        int writeIdx = 0;

        // Single pass: compute max acked ID and compact remaining entries
        for (int i = 0; i < n; i++) {
            long seq = pendingSequences.getQuick(i);
            if (seq <= ackedSequence) {
                // This batch is acknowledged
                int maxId = pendingMaxIds.getQuick(i);
                if (maxId > maxAckedId) {
                    maxAckedId = maxId;
                }
            } else {
                // Keep this batch - it's still pending
                if (writeIdx != i) {
                    pendingSequences.setQuick(writeIdx, seq);
                    pendingMaxIds.setQuick(writeIdx, pendingMaxIds.getQuick(i));
                }
                writeIdx++;
            }
        }

        // Truncate lists to remove acknowledged entries
        pendingSequences.setPos(writeIdx);
        pendingMaxIds.setPos(writeIdx);

        confirmedMaxId = maxAckedId;
    }

    /**
     * Handles a batch failure.
     * Removes the batch from pending tracking without advancing the watermark.
     *
     * @param failedSequence the sequence number of the failed batch
     */
    public void onBatchFailed(long failedSequence) {
        int n = pendingSequences.size();
        for (int i = 0; i < n; i++) {
            if (pendingSequences.getQuick(i) == failedSequence) {
                // Remove by swapping with last element
                int last = n - 1;
                if (i != last) {
                    pendingSequences.setQuick(i, pendingSequences.getQuick(last));
                    pendingMaxIds.setQuick(i, pendingMaxIds.getQuick(last));
                }
                pendingSequences.setPos(last);
                pendingMaxIds.setPos(last);
                return;
            }
        }
    }

    /**
     * Resets the state for a new connection.
     * Call this when establishing a new connection.
     */
    public void reset() {
        confirmedMaxId = -1;
        pendingSequences.clear();
        pendingMaxIds.clear();
    }

    /**
     * Returns the number of batches pending acknowledgment.
     */
    public int getPendingBatchCount() {
        return pendingSequences.size();
    }

    /**
     * Checks if there are any pending batches.
     */
    public boolean isEmpty() {
        return pendingSequences.size() == 0;
    }

    /**
     * Gets the max symbol ID used in a specific pending batch.
     *
     * @param batchSequence the batch sequence
     * @return the max symbol ID for that batch, or -1 if not found
     */
    public int getMaxIdForBatch(long batchSequence) {
        int n = pendingSequences.size();
        for (int i = 0; i < n; i++) {
            if (pendingSequences.getQuick(i) == batchSequence) {
                return pendingMaxIds.getQuick(i);
            }
        }
        return -1;
    }
}
