/*+*****************************************************************************
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

package io.questdb.cairo.wal.seq;

import io.questdb.cairo.CairoException;
import io.questdb.cairo.TableWriteMode;
import io.questdb.cairo.wal.WalUtils;

/**
 * Local sequencer coordinates name WAL events before the boundary and Rust event records after it.
 * The uploader translates post-boundary addresses to event-pack identities and offsets.
 */
final class PackTransport {
    static final int MIN_EVENT_OFFSET8 = 4;

    private PackTransport() {
    }

    static void checkAppend(long packStartTxn, long txn, int walId, int segmentId, int segmentTxn) {
        if (!isPacked(txn, packStartTxn, walId)) {
            throw CairoException.critical(0).put("pack transport is not active [txn=").put(txn)
                    .put(", packStartTxn=").put(packStartTxn).put(']');
        }
        checkEventCoordinates(walId, segmentId, segmentTxn);
    }

    static void checkBoundary(long current, long requested, long lastTxn) {
        if (requested <= 0 || current != 0 && current != requested || current == 0 && requested != lastTxn + 1) {
            throw CairoException.critical(0).put("invalid pack boundary [current=").put(current)
                    .put(", requested=").put(requested).put(", lastTxn=").put(lastTxn).put(']');
        }
    }

    /** Reject a corrupt persisted boundary before it can silently select legacy coordinate decoding. */
    static void checkPackStartTxn(long packStartTxn) {
        if (packStartTxn < 0) {
            throw CairoException.critical(0).put("invalid pack boundary [packStartTxn=").put(packStartTxn).put(']');
        }
    }

    static boolean isPacked(long txn, long packStartTxn, int walId) {
        return packStartTxn > 0 && txn >= packStartTxn
                && walId != WalUtils.DROP_TABLE_WAL_ID && walId != WalUtils.DELTA_TABLE_SWITCH_WAL_ID;
    }

    /** Validates a record using the boundary belonging to its table's retained history. */
    static void validateRecord(long txn, long packStartTxn, int walId, int segmentId, int segmentTxn) {
        // Controls retain their meaning even when their transaction follows the pack boundary.
        if (walId == WalUtils.DELTA_TABLE_SWITCH_WAL_ID) {
            if (segmentId != WalUtils.DELTA_TABLE_SWITCH_VERSION || segmentTxn != TableWriteMode.DELTA_RUNS) {
                throw CairoException.critical(0).put("invalid Delta table switch record");
            }
        } else if (isPacked(txn, packStartTxn, walId)) {
            checkEventCoordinates(walId, segmentId, segmentTxn);
        } else {
            WalUtils.checkWalId(walId);
        }
    }

    /** Callers exclude DROP/SWITCH first; local records follow a 32-byte header in 8-byte units. */
    private static void checkEventCoordinates(int walId, int segmentId, int segmentTxn) {
        if (Integer.toUnsignedLong(segmentTxn) < MIN_EVENT_OFFSET8) {
            throw CairoException.critical(0).put("invalid local event coordinates [walId=").put(walId)
                    .put(", segmentId=").put(segmentId).put(", offset8=").put(Integer.toUnsignedLong(segmentTxn)).put(']');
        }
    }
}
