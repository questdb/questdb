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

package io.questdb.griffin.engine.table;

import io.questdb.cairo.sql.SqlExecutionCircuitBreaker;
import io.questdb.std.DirectLongList;
import io.questdb.std.Numbers;
import io.questdb.std.Unsafe;

/**
 * Strategy interface for SUBSAMPLE downsampling algorithms.
 * <p>
 * Implementations receive a native buffer of (timestamp, value) entries and
 * write selected buffer indices to the output list. The buffer layout per
 * entry is: [timestamp: long (8)][value: 8 bytes] = 16 bytes. The value slot
 * is dual-lane: it holds a raw {@code long} for integral value columns
 * (INT/LONG/SHORT/BYTE) and a {@code double} for floating-point ones
 * (FLOAT/DOUBLE). The writer passes {@code hasIntegralValues} into
 * {@link #select} so implementations read the slot through the matching
 * accessor; a raw long keeps LONG values exact over the full 64-bit range,
 * where narrowing to double collapses values beyond 2^53.
 * <p>
 * There is deliberately no stored ordinal/rowId field: an entry's ordinal IS
 * its buffer index, which every caller already holds, so storing it cost 8
 * bytes per input row and was never read back. Keep the stride a power of two
 * - it turns {@code index * ENTRY_SIZE} into a shift and packs exactly 4
 * entries per 64-byte cache line instead of straddling lines.
 * <h2>NULL contract</h2>
 * This is the single authoritative statement of what the buffer may contain;
 * implementations must not restate or re-derive it.
 * <p>
 * The writer screens every row before appending, so <b>the buffer never holds a
 * NULL value in either lane</b>:
 * <ul>
 *   <li>integral lane: the tag-specific sentinel ({@code LONG_NULL} for LONG,
 *       {@code INT_NULL} for INT) is dropped; SHORT/BYTE have no sentinel.</li>
 *   <li>floating lane: any value for which {@link Numbers#isNull(double)} holds
 *       is dropped. QuestDB defines a NULL double as <b>non-finite</b>, so this
 *       covers NaN <b>and both infinities</b> - a projected expression such as
 *       {@code p * 1e308} can overflow to +/-Inf, and an infinity left in the
 *       buffer would win a bucket's min/max and be rendered back to the client
 *       as {@code null}.</li>
 * </ul>
 * Consequently implementations may compare values with plain {@code <} / {@code >}
 * and need no non-finite guards of their own. Any new screening site must use
 * {@link Numbers#isNull(double)} (never {@code Double.isNaN}, which admits the
 * infinities) so all algorithms agree on what NULL means in the same column.
 */
public interface SubsampleAlgorithm {
    int ENTRY_SIZE = 16;

    /**
     * Select representative points from the buffer and add their indices
     * to {@code selectedIndices}. The list is NOT cleared before this call -
     * implementations must call {@code selectedIndices.clear()} if needed.
     *
     * @param buffer            native memory buffer of entries
     * @param bufferSize        number of entries in the buffer
     * @param targetPoints      desired number of output points
     * @param hasIntegralValues true when the value slots hold raw longs
     *                          (integral value column), false when they hold
     *                          doubles (floating-point value column)
     * @param selectedIndices   output list to add selected buffer indices to
     * @param circuitBreaker    for query cancellation during processing
     */
    void select(long buffer, int bufferSize, int targetPoints, boolean hasIntegralValues,
                DirectLongList selectedIndices, SqlExecutionCircuitBreaker circuitBreaker);

    /**
     * Computes the unsigned offset floor(span * bucket / numBuckets) from the
     * precomputed unsigned quotient and remainder of span / numBuckets.
     * For 0 &lt;= bucket &lt;= numBuckets, remainder * bucket fits in a signed long
     * because numBuckets is a positive int. The quotient product and sum may
     * wrap: adding this unsigned offset to the signed minimum timestamp gives
     * the exact signed bucket boundary via modular arithmetic.
     * <p>
     * Exact integer boundaries keep full resolution at any epoch. Double math
     * does not: the double ulp near a 2024 nanosecond epoch (~1.7e18) is 256,
     * so absolute-epoch doubles quantize TIMESTAMP_NS boundaries to 256ns
     * steps and can collapse sub-256ns bucket spans to zero.
     */
    static long bucketOffset(long quotient, long remainder, int bucket, int numBuckets) {
        return quotient * bucket + remainder * bucket / numBuckets;
    }

    /**
     * Read timestamp from buffer entry at the given index.
     */
    static long getTimestamp(long buffer, long index) {
        return Unsafe.getUnsafe().getLong(buffer + index * ENTRY_SIZE);
    }

    /**
     * Read a raw long value from buffer entry at the given index. Valid only
     * when the writer buffered an integral value column
     * ({@code hasIntegralValues == true}).
     */
    static long getLongValue(long buffer, long index) {
        return Unsafe.getUnsafe().getLong(buffer + index * ENTRY_SIZE + 8);
    }

    /**
     * Read a double value from buffer entry at the given index. Valid only
     * when the writer buffered a floating-point value column
     * ({@code hasIntegralValues == false}).
     */
    static double getValue(long buffer, long index) {
        return Unsafe.getUnsafe().getDouble(buffer + index * ENTRY_SIZE + 8);
    }

    /**
     * Reads the value slot as a double regardless of lane, for callers whose
     * comparisons are inherently floating-point (LTTB's triangle math). The
     * integral lane is widened, which may collapse LONG magnitudes beyond 2^53
     * - acceptable where the result only ranks candidates, never where it
     * decides an exact extremum.
     */
    static double getValueAsDouble(long buffer, long index, boolean hasIntegralValues) {
        return hasIntegralValues ? (double) getLongValue(buffer, index) : getValue(buffer, index);
    }

    /**
     * Appends the two given buffer indices to {@code out} in ascending index
     * order, collapsing them to a single entry when they coincide (the bucket's
     * min and max are the same row). Keeps every algorithm's output strictly
     * ascending, which is the precondition the window-function pass2 walk and
     * LTTB's candidate list both rely on.
     */
    static void emitAscendingPair(DirectLongList out, int a, int b) {
        if (a == b) {
            out.add(a);
        } else if (a < b) {
            out.add(a);
            out.add(b);
        } else {
            out.add(b);
            out.add(a);
        }
    }
}
