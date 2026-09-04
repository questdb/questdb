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

package io.questdb.cairo;

/**
 * The one way this codebase expresses "checksummed area present / absent / torn".
 * <p>
 * Four hand-rolled variants of this existed: {@code _cv} and {@link SnapshotMarker} gate presence on
 * a 64-bit MAGIC, {@code _event.c} on a feature version in the WAL-e version word, and {@code _txn}
 * on a bare {@code stored == 0} sentinel. The bare sentinel is the weak one: it cannot tell a legacy
 * record from one whose checksum slot was zeroed by a torn write, which is exactly the state a
 * partial page write leaves behind.
 * <p>
 * Presence is gated on a MAGIC rather than on a zero check or an EOF guard because a legacy
 * page-rounded file frequently has NON-ZERO adjacent data sitting where the trailer would be -- see
 * the reasoning on {@link TableUtils#CV_CHECKSUM_MAGIC}. A 64-bit magic makes a false "present"
 * ~2^-64.
 * <p>
 * Capability separates "no trailer here" from "no trailer here and there should have been". A file
 * records a capability magic and a watermark; at or beyond that watermark a trailer is guaranteed,
 * so its absence is torn. Below it, absence is simply legacy. This mirrors
 * {@code TableTransactionLogV2}'s {@code CHECKSUM_CAPABILITY_MAGIC} / {@code checksumFromTxn}, which
 * is where the pattern was first worked out.
 * <p>
 * Pure functions only: no I/O and no state, so every classification is unit-testable without a file.
 */
public final class ChecksumTrailer {

    /**
     * A trailer is present and matches the area.
     */
    public static final int PRESENT_OK = 0;
    /**
     * No trailer here. Legacy unless a capability says otherwise.
     */
    public static final int ABSENT = 1;
    /**
     * A trailer is present and does not match, or was guaranteed and is missing. Torn.
     */
    public static final int MISMATCH = 2;

    /**
     * On-disk size of the trailer: 8-byte MAGIC + 8-byte checksum.
     */
    public static final int TRAILER_SIZE = TableUtils.CV_CHECKSUM_TRAILER_SIZE;

    private ChecksumTrailer() {
    }

    /**
     * Promotes ABSENT to MISMATCH when the capability says a trailer was guaranteed. PRESENT_OK and
     * MISMATCH pass through: a matching checksum is fine whether or not it was mandatory, and a
     * mismatch is already the worst verdict.
     */
    public static int applyCapability(int classification, boolean covered) {
        if (classification == ABSENT && covered) {
            return MISMATCH;
        }
        return classification;
    }

    /**
     * Classifies a trailer read from disk. {@code storedMagic} / {@code storedChecksum} are the raw
     * 16 bytes; {@code areaBaseAddr} / {@code areaSize} describe the covered region.
     */
    public static int classify(
            long storedMagic,
            long storedChecksum,
            long areaBaseAddr,
            long areaSize,
            long expectedMagic
    ) {
        if (storedMagic != expectedMagic) {
            // No magic => no trailer was written here. Whatever these bytes are, they are not a
            // checksum, so this is never evidence of corruption on its own.
            return ABSENT;
        }
        return storedChecksum == TableUtils.calculateCvAreaChecksum(areaBaseAddr, areaSize)
                ? PRESENT_OK
                : MISMATCH;
    }

    /**
     * True when {@code recordId} is at or beyond the file's capability watermark, i.e. a trailer was
     * guaranteed to have been written for it. False when the file predates the capability entirely,
     * so nothing in it is ever judged as "should have had a trailer".
     */
    public static boolean isCovered(
            long storedCapabilityMagic,
            long expectedCapabilityMagic,
            long watermark,
            long recordId
    ) {
        return storedCapabilityMagic == expectedCapabilityMagic && recordId >= watermark;
    }
}
