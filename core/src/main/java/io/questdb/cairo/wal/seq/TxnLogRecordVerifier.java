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

package io.questdb.cairo.wal.seq;

import io.questdb.cairo.CairoException;
import io.questdb.cairo.TableUtils;

/**
 * Decides whether a V2 sequencer txnlog record is intact or torn, from the CRC in its reserved slot.
 */
public final class TxnLogRecordVerifier {

    private TxnLogRecordVerifier() {
    }

    /**
     * Throws when the record is torn; returns silently when it is intact. The caller reads a record with a zero
     * CRC unverified and calls in only for a non-zero one.
     *
     * @param txn            the 1-based txn being read
     * @param recordBaseAddr address of the record
     * @param bodySize       bytes covered by the CRC
     * @param storedCrc      the CRC as stored in the reserved slot, never 0
     * @param txnOffset      record offset, for the error message
     */
    public static void verify(
            long txn,
            long recordBaseAddr,
            long bodySize,
            long storedCrc,
            long txnOffset
    ) {
        final long actual = TableUtils.calculateCvAreaChecksum(recordBaseAddr, bodySize);
        if (actual != storedCrc) {
            throw CairoException.critical(CairoException.METADATA_VALIDATION)
                    .put("torn sequencer txnlog record [txn=").put(txn)
                    .put(", txnOffset=").put(txnOffset)
                    .put(", expected=").put(storedCrc)
                    .put(", actual=").put(actual)
                    .put(']');
        }
    }
}
