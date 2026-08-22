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
import io.questdb.std.Unsafe;

import static io.questdb.cairo.wal.seq.TableTransactionLogFile.TX_LOG_WAL_ID_OFFSET;

/**
 * The one place that decides whether a sequencer txnlog record is intact, legacy, or torn.
 * <p>
 * V1 and V2 store the CRC in different places -- V2 in a reserved trailing slot inside the record, V1
 * in the additive {@code _txnlog.c} sidecar, because its records have no spare bytes -- but the
 * classification must be identical, so both hand their stored value here rather than re-deriving the
 * rules. Divergence between the two would mean the same torn record is fatal on one format and
 * invisible on the other.
 */
public final class TxnLogRecordVerifier {

    private TxnLogRecordVerifier() {
    }

    /**
     * Throws when the record is torn or absent; returns silently when it is intact or legitimately
     * legacy.
     * <p>
     * {@code calculateCvAreaChecksum} never returns 0, so {@code storedCrc == 0} means "no CRC written
     * here" -- but that has TWO possible causes the CRC alone cannot separate:
     * <ol>
     *   <li>a genuine LEGACY record written before the CRC existed. Its body is fully populated, so it
     *       is read unverified for backward compatibility;</li>
     *   <li>an ABSENT record: a slot never written back to the device that the cursor reached because
     *       the header MAX_TXN was made durable AHEAD of this record. The ordered flush normally
     *       prevents it, so it is only reachable on a device that reorders those flushes across a
     *       crash -- narrow, but not provably impossible. Returning here would silently inject a
     *       garbage all-zero txn.</li>
     * </ol>
     * Two cheap, independent discriminators separate them. The capability watermark
     * ({@code firstCoveredTxn}) says a CRC was guaranteed at or beyond it. And no legitimate record --
     * legacy or current -- ever carries {@code walId == 0}: real writers use {@code walId >= 1},
     * {@code STRUCTURAL_CHANGE_WAL_ID = -1}, {@code DROP_TABLE_WAL_ID = -2}. So a zero CRC together
     * with either signal is definitionally absent/torn.
     *
     * @param txn             the 1-based txn being read
     * @param recordBaseAddr  address of the record
     * @param bodySize        bytes covered by the CRC
     * @param storedCrc       the CRC as stored (V2: reserved slot; V1: sidecar), 0 when absent
     * @param firstCoveredTxn capability watermark; records below it predate the CRC
     * @param txnOffset       record offset, for the error message
     */
    public static void verify(
            long txn,
            long recordBaseAddr,
            long bodySize,
            long storedCrc,
            long firstCoveredTxn,
            long txnOffset
    ) {
        if (storedCrc == 0L) {
            if (txn >= firstCoveredTxn || Unsafe.getInt(recordBaseAddr + TX_LOG_WAL_ID_OFFSET) == 0) {
                throw CairoException.critical(CairoException.METADATA_VALIDATION)
                        .put("absent/torn sequencer txnlog record beyond the durable frontier [txn=").put(txn)
                        .put(", txnOffset=").put(txnOffset)
                        .put(']');
            }
            return; // legacy record without CRC — read unverified for backward compatibility
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
