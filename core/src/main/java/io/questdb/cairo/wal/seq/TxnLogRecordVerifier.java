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
     * Throws when the record is torn or absent; returns silently when it is intact.
     * <p>
     * The caller decides whether a record was checksummed at all and only calls in when it was, because
     * the reserved slot is non-zero. A legacy record written before the CRC existed therefore never reaches here, so a zero CRC that
     * does reach here is not legacy -- it is a checksum that was written and has since gone, which is
     * an absent or torn record rather than a backward-compatible read.
     *
     * @param txn            the 1-based txn being read
     * @param recordBaseAddr address of the record
     * @param bodySize       bytes covered by the CRC
     * @param storedCrc      the CRC as stored in the reserved slot, 0 when absent
     * @param txnOffset      record offset, for the error message
     */
    public static void verify(
            long txn,
            long recordBaseAddr,
            long bodySize,
            long storedCrc,
            long txnOffset
    ) {
        // The CALLER decides whether this record was checksummed at all, and only calls in when it was. So a
        // zero reaching here is not a legacy record, it is a checksum that was written and has since gone.
        if (storedCrc == 0L) {
            throw CairoException.critical(CairoException.METADATA_VALIDATION)
                    .put("absent/torn sequencer txnlog record [txn=").put(txn)
                    .put(", txnOffset=").put(txnOffset)
                    .put(']');
        }
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
