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

package io.questdb.cairo.idx;

import io.questdb.cairo.arr.ArrayView;
import io.questdb.cairo.sql.RowCursor;
import io.questdb.std.BinarySequence;
import io.questdb.std.str.Utf8Sequence;

/**
 * Extension of RowCursor that provides access to covered column values
 * stored in sidecar files alongside the posting list.
 */
public interface CoveringRowCursor extends RowCursor {

    default ArrayView getCoveredArray(int includeIdx, int columnType) {
        return null;
    }

    default BinarySequence getCoveredBin(int includeIdx) {
        return null;
    }

    default long getCoveredBinLen(int includeIdx) {
        return -1;
    }

    byte getCoveredByte(int includeIdx);

    double getCoveredDouble(int includeIdx);

    float getCoveredFloat(int includeIdx);

    int getCoveredInt(int includeIdx);

    long getCoveredLong(int includeIdx);

    default long getCoveredLong128Lo(int includeIdx) {
        return Long.MIN_VALUE;
    }

    default long getCoveredLong128Hi(int includeIdx) {
        return Long.MIN_VALUE;
    }

    default long getCoveredLong256_0(int includeIdx) {
        return Long.MIN_VALUE;
    }

    default long getCoveredLong256_1(int includeIdx) {
        return Long.MIN_VALUE;
    }

    default long getCoveredLong256_2(int includeIdx) {
        return Long.MIN_VALUE;
    }

    default long getCoveredLong256_3(int includeIdx) {
        return Long.MIN_VALUE;
    }

    short getCoveredShort(int includeIdx);

    default CharSequence getCoveredStrA(int includeIdx) {
        return null;
    }

    default CharSequence getCoveredStrB(int includeIdx) {
        return null;
    }

    default Utf8Sequence getCoveredVarcharA(int includeIdx) {
        return null;
    }

    default Utf8Sequence getCoveredVarcharB(int includeIdx) {
        return null;
    }

    /**
     * Returns the number of covered columns (INCLUDE list size).
     */
    default int getCoveredColumnCount() {
        return 0;
    }

    /**
     * Returns the column type for a covered column by include index.
     */
    default int getCoveredColumnType(int includeIdx) {
        return -1;
    }

    /**
     * Returns the total value count for the current key across all gens.
     * Returns -1 if unknown or covering data is unavailable.
     */
    default int getCoveredValueCount() {
        return -1;
    }

    boolean hasCovering();

    /**
     * Bulk-decodes all covered column values for the current key into native memory.
     * For each include column, writes values contiguously starting at the given address.
     * The caller must ensure each buffer has room for getCoveredValueCount() elements.
     * Variable-size columns (VARCHAR, STRING) are not supported; their buffer is untouched.
     *
     * @param outputAddrs array of native addresses, one per include column
     * @return number of values written, or -1 if bulk decode is unavailable
     */
    default int decodeCoveredColumnsToAddr(long[] outputAddrs) {
        return -1;
    }

    /**
     * Positions the cursor at the last row, returning its row ID.
     * After this call, getCoveredXxx() returns values for the last row.
     * Returns -1 if the cursor has no rows.
     */
    long seekToLast();

    /**
     * Iterates backward from the last row, checking the filter on covered
     * values via the record, and returns the first matching row ID (the
     * latest matching row). Returns -1 if no rows match.
     */
    default long seekToLastMatching(io.questdb.cairo.sql.Function filter, io.questdb.cairo.sql.Record record) {
        return seekToLast();
    }
}
