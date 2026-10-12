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

package io.questdb.cairo.idx;

import io.questdb.cairo.arr.ArrayView;
import io.questdb.cairo.sql.RowCursor;
import io.questdb.std.BinarySequence;
import io.questdb.std.str.Utf8Sequence;

public interface CoveringRowCursor extends RowCursor {

    ArrayView getCoveredArray(int includeIdx, int columnType);

    /**
     * The decoded values of fixed-width covered column {@code includeIdx} for the block the current
     * row reads from, one value of {@code ColumnType.sizeOf(type)} bytes per ordinal (see
     * {@link #getCoveredBlockOrdinal()}): a dense generation's key block, decoded, or a sparse
     * generation's raw values. 0 when they are not available in bulk, such as when the column has no
     * values for the key. The address is valid until the cursor moves to another block (see
     * {@link #getCoveredBlockId(int)}) or closes.
     */
    default long getCoveredBlockAddress(int includeIdx) {
        return 0;
    }

    /**
     * An identity of the block the current row of column {@code includeIdx} reads from, without
     * decoding it: two rows with the same id and consecutive ordinals read consecutive values of one
     * block. 0 when the column cannot be read in bulk, see {@link #getCoveredBlockAddress(int)}.
     */
    default long getCoveredBlockId(int includeIdx) {
        return 0;
    }

    /**
     * The current row's ordinal in its block, see {@link #getCoveredBlockAddress(int)}.
     */
    default int getCoveredBlockOrdinal() {
        return -1;
    }

    /**
     * The ordinal, in the current row's block (see {@link #getCoveredBlockAddress(int)}), of the
     * first of the rows {@link #peekCoveredRun(int)} counted.
     */
    default int getCoveredRunOrdinal() {
        return -1;
    }

    /**
     * Counts, without moving the cursor, the next rows that read consecutive values of the current
     * row's block, from {@link #getCoveredRunOrdinal()} on: the rows the next {@code hasNext()}
     * calls would return, which a bulk reader can take from the block addresses at once and then
     * pass with {@link #skipCoveredRun(int)}. 0 when there are none, or the cursor cannot tell
     * without decoding: the bulk reader then carries on with {@code hasNext()}.
     *
     * @param maxRows the most rows to count, at least 1
     */
    default int peekCoveredRun(int maxRows) {
        return 0;
    }

    /**
     * Moves the cursor past the first {@code rows} rows {@link #peekCoveredRun(int)} counted, as
     * that many {@code hasNext()} and {@code next()} calls would: the current row is the last of
     * them.
     */
    default void skipCoveredRun(int rows) {
        throw new UnsupportedOperationException();
    }

    BinarySequence getCoveredBin(int includeIdx);

    long getCoveredBinLen(int includeIdx);

    byte getCoveredByte(int includeIdx);

    double getCoveredDouble(int includeIdx);

    float getCoveredFloat(int includeIdx);

    int getCoveredInt(int includeIdx);

    long getCoveredLong(int includeIdx);

    long getCoveredLong128Hi(int includeIdx);

    long getCoveredLong128Lo(int includeIdx);

    long getCoveredLong256_0(int includeIdx);

    long getCoveredLong256_1(int includeIdx);

    long getCoveredLong256_2(int includeIdx);

    long getCoveredLong256_3(int includeIdx);

    short getCoveredShort(int includeIdx);

    CharSequence getCoveredStrA(int includeIdx);

    CharSequence getCoveredStrB(int includeIdx);

    Utf8Sequence getCoveredVarcharA(int includeIdx);

    Utf8Sequence getCoveredVarcharB(int includeIdx);

    boolean isCoveredAvailable(int includeIdx);

    long seekToLast();
}
