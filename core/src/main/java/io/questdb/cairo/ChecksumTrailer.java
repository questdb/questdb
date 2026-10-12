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
 * Classifies a 16-byte checksum trailer (a magic, then a checksum) as present and matching, absent, or
 * torn. The {@code _cv} file keeps one after each area.
 * <p>
 * Presence is decided by a 64-bit magic rather than a zero check or an end-of-file guard: a page-rounded
 * file written by a released version often holds non-zero data where the trailer would be (see
 * {@link TableUtils#CV_CHECKSUM_MAGIC}).
 * <p>
 * A file-level watermark does not tell a missing trailer from one that should have been written: an older
 * binary can neither see nor withdraw such a promise, so a table it writes to would read back as damaged.
 * Where the distinction is needed, it is made per record by stamping the checksum with the record it
 * describes (see {@link TableUtils#TX_OFFSET_BODY_CHECKSUM_STAMP_32}).
 * <p>
 * Pure functions only: no I/O and no state.
 */
public final class ChecksumTrailer {

    /**
     * A trailer is present and matches the area.
     */
    public static final int PRESENT_OK = 0;
    /**
     * No trailer here: the area is unverified.
     */
    public static final int ABSENT = 1;
    /**
     * A trailer is present and does not match the area: torn.
     */
    public static final int MISMATCH = 2;

    private ChecksumTrailer() {
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
            // No magic: no trailer was written here, and these bytes are not evidence of corruption.
            return ABSENT;
        }
        return storedChecksum == TableUtils.calculateCvAreaChecksum(areaBaseAddr, areaSize)
                ? PRESENT_OK
                : MISMATCH;
    }

}
